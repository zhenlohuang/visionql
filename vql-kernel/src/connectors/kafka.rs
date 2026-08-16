use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, StringArray, StructArray, new_empty_array};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::json::writer::{LineDelimited, WriterBuilder};
use arrow::record_batch::RecordBatch;
use futures::stream::{FuturesUnordered, StreamExt};
use krafka::producer::{Acks, Producer};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::catalog::KafkaSinkConfig;
use crate::types::is_image_field;
use crate::{ErrorCode, Result, SecretProviderRef, VqlError};

const IMAGE_KAFKA_FIELDS: [&str; 7] = [
    "uri", "locator", "pts_ms", "frame_id", "encoding", "width", "height",
];

pub(crate) struct KafkaSink {
    name: String,
    config: KafkaSinkConfig,
    secret_provider: Option<SecretProviderRef>,
    state: Mutex<KafkaSinkState>,
    delivery_slots: Arc<Semaphore>,
}

#[derive(Default)]
struct KafkaSinkState {
    producer: Option<Arc<Producer>>,
    active_executions: usize,
}

impl KafkaSink {
    pub(crate) fn new(
        name: String,
        config: KafkaSinkConfig,
        secret_provider: Option<SecretProviderRef>,
    ) -> Self {
        let delivery_slots = Arc::new(Semaphore::new(config.buffer_capacity));
        Self {
            name,
            config,
            secret_provider,
            state: Mutex::new(KafkaSinkState::default()),
            delivery_slots,
        }
    }

    pub(crate) async fn write_batch(
        &self,
        batch: &RecordBatch,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let batch = sanitize_images(batch)?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let producer = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(VqlError::new(ErrorCode::QueryCancelled, "query cancelled"));
            }
            producer = self.producer() => producer?,
        };
        let mut deliveries = FuturesUnordered::new();
        for row in 0..batch.num_rows() {
            let mut permit_wait = Box::pin(Arc::clone(&self.delivery_slots).acquire_owned());
            let permit = loop {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        return Err(VqlError::new(ErrorCode::QueryCancelled, "query cancelled"));
                    }
                    permit = &mut permit_wait => {
                        break permit.map_err(|_| VqlError::new(
                            ErrorCode::Internal,
                            "Kafka Sink delivery semaphore was closed",
                        ))?;
                    }
                    delivery = deliveries.next(), if !deliveries.is_empty() => {
                        if let Some(delivery) = delivery {
                            delivery?;
                        }
                    }
                }
            };
            let payload = json_message(&batch, row)?;
            deliveries.push(deliver(
                Arc::clone(&producer),
                self.config.topic.clone(),
                payload,
                permit,
            ));
        }
        while !deliveries.is_empty() {
            await_delivery(&mut deliveries, cancellation).await?;
        }
        Ok(())
    }

    async fn producer(&self) -> Result<Arc<Producer>> {
        let mut state = self.state.lock().await;
        if let Some(existing) = state.producer.as_ref()
            && !existing.is_closed()
        {
            return Ok(Arc::clone(existing));
        }

        let mut builder = Producer::builder()
            .bootstrap_servers(self.config.bootstrap_servers.clone())
            .client_id(format!("visionql-{}", self.name))
            .acks(Acks::All)
            .connect_timeout(Duration::from_millis(self.config.delivery_timeout_ms))
            .request_timeout(Duration::from_millis(self.config.delivery_timeout_ms))
            .delivery_timeout(Duration::from_millis(self.config.delivery_timeout_ms))
            .retries(0)
            .max_in_flight(self.config.buffer_capacity)
            .idempotent(false);
        if let Some(reference) = self.config.credential_ref.as_deref() {
            let provider = self.secret_provider.as_ref().ok_or_else(|| {
                VqlError::new(
                    ErrorCode::Execution,
                    format!(
                        "Kafka Sink '{}' requires a SecretProvider for credential_ref",
                        self.name
                    ),
                )
            })?;
            let authentication =
                provider
                    .resolve_kafka_authentication(reference)
                    .map_err(|error| {
                        VqlError::new(
                            ErrorCode::Execution,
                            format!(
                                "failed to resolve credential_ref for Kafka Sink '{}'",
                                self.name
                            ),
                        )
                        .with_source(error)
                    })?;
            builder = builder.auth(authentication.into_auth_config());
        }
        let initialized = builder.build().await.map(Arc::new).map_err(|error| {
            VqlError::new(
                ErrorCode::Execution,
                format!("failed to connect Kafka producer for Sink '{}'", self.name),
            )
            .with_source(error)
        })?;
        state.producer = Some(Arc::clone(&initialized));
        Ok(initialized)
    }

    pub(crate) async fn begin_execution(&self) {
        self.state.lock().await.active_executions += 1;
    }

    pub(crate) async fn finish_execution(&self) -> Result<()> {
        let producer = {
            let mut state = self.state.lock().await;
            if state.active_executions == 0 {
                return Err(VqlError::new(
                    ErrorCode::Internal,
                    format!(
                        "Kafka Sink '{}' execution lifecycle is unbalanced",
                        self.name
                    ),
                ));
            }
            state.active_executions -= 1;
            if state.active_executions == 0 {
                state.producer.take()
            } else {
                None
            }
        };
        let Some(producer) = producer else {
            return Ok(());
        };
        producer
            .close_with_timeout(Duration::from_millis(self.config.delivery_timeout_ms))
            .await
            .map_err(|error| {
                VqlError::new(
                    ErrorCode::Execution,
                    format!("failed to close Kafka producer for Sink '{}'", self.name),
                )
                .with_source(error)
            })
    }
}

impl Debug for KafkaSink {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KafkaSink")
            .field("name", &self.name)
            .field("topic", &self.config.topic)
            .finish_non_exhaustive()
    }
}

async fn deliver(
    producer: Arc<Producer>,
    topic: String,
    payload: Vec<u8>,
    _permit: OwnedSemaphorePermit,
) -> Result<()> {
    producer
        .send(&topic, None, &payload)
        .await
        .map(|_| ())
        .map_err(|error| {
            VqlError::new(
                ErrorCode::Execution,
                format!("Kafka delivery to topic '{topic}' failed: {error}"),
            )
            .with_source(error)
        })
}

async fn await_delivery<F>(
    deliveries: &mut FuturesUnordered<F>,
    cancellation: &CancellationToken,
) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    tokio::select! {
        _ = cancellation.cancelled() => {
            Err(VqlError::new(ErrorCode::QueryCancelled, "query cancelled"))
        }
        delivery = deliveries.next() => {
            match delivery {
                Some(Ok(())) => Ok(()),
                Some(Err(error)) => Err(error),
                None => Ok(()),
            }
        }
    }
}

pub(crate) fn validate_kafka_schema(schema: &SchemaRef) -> Result<()> {
    let mut names = HashSet::new();
    for field in schema.fields() {
        if !names.insert(field.name()) {
            return Err(VqlError::new(
                ErrorCode::InvalidOption,
                format!(
                    "Kafka JSON output contains duplicate column name '{}'",
                    field.name()
                ),
            ));
        }
        validate_field(field, field.name(), true)?;
    }
    let columns = schema
        .fields()
        .iter()
        .map(|field| new_empty_array(field.data_type()))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::clone(schema), columns).map_err(|error| {
        VqlError::new(
            ErrorCode::InvalidOption,
            "Kafka JSON output schema is invalid",
        )
        .with_source(error)
    })?;
    json_messages(&batch).map(|_| ()).map_err(|error| {
        VqlError::new(
            ErrorCode::InvalidOption,
            format!("Kafka JSON output schema is unsupported: {}", error.message),
        )
    })
}

fn validate_field(field: &Field, path: &str, allow_image: bool) -> Result<()> {
    if allow_image && is_image_field(field) {
        return Ok(());
    }
    match field.data_type() {
        DataType::Binary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary => Err(VqlError::new(
            ErrorCode::InvalidOption,
            format!("Kafka JSON output field '{path}' is binary; encode it explicitly as text"),
        )),
        DataType::List(child)
        | DataType::ListView(child)
        | DataType::FixedSizeList(child, _)
        | DataType::LargeList(child)
        | DataType::LargeListView(child) => validate_field(child, &format!("{path}[]"), false),
        DataType::Struct(fields) => {
            for child in fields {
                validate_field(child, &format!("{path}.{}", child.name()), false)?;
            }
            Ok(())
        }
        DataType::Map(entries, _) => validate_field(entries, &format!("{path}[]"), false),
        DataType::Dictionary(_, value) => validate_field(
            &Field::new("dictionary_value", value.as_ref().clone(), true),
            path,
            false,
        ),
        DataType::Union(_, _) | DataType::RunEndEncoded(_, _) => Err(VqlError::new(
            ErrorCode::InvalidOption,
            format!(
                "Kafka JSON output field '{path}' has unsupported type {}",
                field.data_type()
            ),
        )),
        _ => Ok(()),
    }
}

fn json_messages(batch: &RecordBatch) -> Result<Vec<Vec<u8>>> {
    let batch = sanitize_images(batch)?;
    if batch.num_rows() == 0 {
        encode_json_batch(&batch)?;
        return Ok(Vec::new());
    }
    (0..batch.num_rows())
        .map(|row| json_message(&batch, row))
        .collect()
}

fn json_message(batch: &RecordBatch, row: usize) -> Result<Vec<u8>> {
    let mut output = encode_json_batch(&batch.slice(row, 1))?;
    if output.last() == Some(&b'\n') {
        output.pop();
    }
    Ok(output)
}

fn encode_json_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, LineDelimited>(&mut output);
    writer.write(batch).map_err(|error| {
        VqlError::new(
            ErrorCode::Execution,
            "failed to encode a Kafka Sink batch as JSON",
        )
        .with_source(error)
    })?;
    writer.finish().map_err(|error| {
        VqlError::new(
            ErrorCode::Execution,
            "failed to finish Kafka Sink JSON encoding",
        )
        .with_source(error)
    })?;
    drop(writer);
    Ok(output)
}

fn sanitize_images(batch: &RecordBatch) -> Result<RecordBatch> {
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if is_image_field(field) {
            let (field, column) = sanitize_image(field, column)?;
            fields.push(Arc::new(field));
            columns.push(column);
        } else {
            fields.push(Arc::clone(field));
            columns.push(Arc::clone(column));
        }
    }
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        )),
        columns,
    )
    .map_err(|error| {
        VqlError::new(
            ErrorCode::Execution,
            "failed to sanitize IMAGE values for Kafka JSON",
        )
        .with_source(error)
    })
}

fn sanitize_image(field: &Field, column: &ArrayRef) -> Result<(Field, ArrayRef)> {
    let images = column
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            VqlError::new(
                ErrorCode::Execution,
                format!("IMAGE field '{}' is not a StructArray", field.name()),
            )
        })?;
    let source_fields = match field.data_type() {
        DataType::Struct(fields) => fields,
        _ => unreachable!("IMAGE storage is always a struct"),
    };
    let mut safe_fields = Vec::with_capacity(IMAGE_KAFKA_FIELDS.len());
    let mut safe_columns = Vec::with_capacity(IMAGE_KAFKA_FIELDS.len());
    for name in IMAGE_KAFKA_FIELDS {
        let (index, source_field) = source_fields.find(name).ok_or_else(|| {
            VqlError::new(
                ErrorCode::Internal,
                format!("IMAGE storage is missing field '{name}'"),
            )
        })?;
        safe_fields.push(Arc::clone(source_field));
        let source = Arc::clone(images.column(index));
        safe_columns.push(if name == "uri" {
            sanitize_uri_array(&source)?
        } else {
            source
        });
    }
    let safe_fields = Fields::from(safe_fields);
    let safe_array = StructArray::new(safe_fields.clone(), safe_columns, images.nulls().cloned());
    Ok((
        Field::new(
            field.name(),
            DataType::Struct(safe_fields),
            field.is_nullable(),
        ),
        Arc::new(safe_array),
    ))
}

fn sanitize_uri_array(array: &ArrayRef) -> Result<ArrayRef> {
    let values = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| VqlError::new(ErrorCode::Internal, "IMAGE uri field is not Utf8"))?;
    Ok(Arc::new(StringArray::from_iter(
        values.iter().map(|value| value.map(sanitize_uri)),
    )))
}

fn sanitize_uri(value: &str) -> String {
    let Ok(mut uri) = url::Url::parse(value) else {
        return value.to_owned();
    };
    let _ = uri.set_username("");
    let _ = uri.set_password(None);
    uri.set_query(None);
    uri.set_fragment(None);
    uri.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ImageRef, ImageRefBuilder, image_field};
    use arrow::array::{
        BinaryArray, BooleanArray, Float64Array, Int64Array, TimestampMillisecondArray,
    };

    fn kafka_config(buffer_capacity: usize) -> KafkaSinkConfig {
        KafkaSinkConfig {
            bootstrap_servers: "127.0.0.1:9092".to_owned(),
            topic: "events".to_owned(),
            credential_ref: None,
            delivery_timeout_ms: 30_000,
            buffer_capacity,
        }
    }

    #[test]
    fn json_wire_format_preserves_names_and_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("answer", DataType::Int64, false),
            Field::new("note", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![42, 7])),
                Arc::new(StringArray::from(vec![None, Some("seven")])),
            ],
        )
        .unwrap();

        let messages = json_messages(&batch).unwrap();

        assert_eq!(
            messages,
            vec![
                br#"{"answer":42,"note":null}"#.to_vec(),
                br#"{"answer":7,"note":"seven"}"#.to_vec(),
            ]
        );
    }

    #[test]
    fn json_wire_format_fixes_boolean_float_and_timestamp_rules() {
        let timestamp = TimestampMillisecondArray::from(vec![Some(1_700_000_000_123), None]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("active", DataType::Boolean, false),
            Field::new("score", DataType::Float64, false),
            Field::new("window_start", timestamp.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(Float64Array::from(vec![1.25, f64::NAN])),
                Arc::new(timestamp),
            ],
        )
        .unwrap();

        assert_eq!(
            json_messages(&batch).unwrap(),
            vec![
                br#"{"active":true,"score":1.25,"window_start":"2023-11-14T22:13:20.123"}"#
                    .to_vec(),
                br#"{"active":false,"score":null,"window_start":null}"#.to_vec(),
            ]
        );
    }

    #[test]
    fn image_json_excludes_pixels_and_process_local_fields() {
        let mut builder = ImageRefBuilder::with_capacity(1);
        builder.append(ImageRef {
            uri: Some("rtsp://user:secret@camera/live?token=secret#frame".to_owned()),
            locator: Some("vql://media/v1/3/frame.jpg".to_owned()),
            pts_ms: Some(1250),
            frame_id: Some(9),
            encoded: Some(vec![1, 2, 3]),
            encoding: Some("jpeg".to_owned()),
            width: Some(1920),
            height: Some(1080),
            buffer_id: Some(7),
            buffer_slot: Some(2),
        });
        let schema = Arc::new(Schema::new(vec![image_field("frame", false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(builder.finish())]).unwrap();

        let messages = json_messages(&batch).unwrap();

        assert_eq!(messages.len(), 1);
        let value: serde_json::Value = serde_json::from_slice(&messages[0]).unwrap();
        assert_eq!(value["frame"]["uri"], "rtsp://camera/live");
        assert_eq!(value["frame"]["locator"], "vql://media/v1/3/frame.jpg");
        assert_eq!(value["frame"]["pts_ms"], 1250);
        assert_eq!(value["frame"]["frame_id"], 9);
        assert_eq!(value["frame"]["encoding"], "jpeg");
        assert_eq!(value["frame"]["width"], 1920);
        assert_eq!(value["frame"]["height"], 1080);
        assert!(value["frame"].get("encoded").is_none());
        assert!(value["frame"].get("buffer_id").is_none());
        assert!(value["frame"].get("buffer_slot").is_none());
    }

    #[test]
    fn raw_binary_requires_explicit_text_encoding() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Binary,
            false,
        )]));
        let error = validate_kafka_schema(&schema).unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidOption);
        assert!(error.message.contains("encode it explicitly as text"));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(BinaryArray::from(vec![b"data".as_slice()]))],
        )
        .unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn nested_image_is_rejected_instead_of_leaking_pixels() {
        let nested_image = Arc::new(image_field("frame", true));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "wrapper",
            DataType::Struct(Fields::from(vec![nested_image])),
            true,
        )]));

        let error = validate_kafka_schema(&schema).unwrap_err();

        assert_eq!(error.code, ErrorCode::InvalidOption);
        assert!(error.message.contains("wrapper.frame.encoded"));
    }

    #[test]
    fn image_shaped_struct_without_logical_metadata_is_not_sanitized() {
        let image_storage = image_field("image", false).data_type().clone();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ordinary",
            image_storage,
            false,
        )]));

        let error = validate_kafka_schema(&schema).unwrap_err();

        assert_eq!(error.code, ErrorCode::InvalidOption);
        assert!(error.message.contains("ordinary.encoded"));
    }

    #[test]
    fn delivery_capacity_is_shared_by_all_writers() {
        let sink = KafkaSink::new("events".to_owned(), kafka_config(2), None);
        let first = Arc::clone(&sink.delivery_slots)
            .try_acquire_owned()
            .unwrap();
        let _second = Arc::clone(&sink.delivery_slots)
            .try_acquire_owned()
            .unwrap();

        assert!(
            Arc::clone(&sink.delivery_slots)
                .try_acquire_owned()
                .is_err()
        );
        drop(first);
        assert!(Arc::clone(&sink.delivery_slots).try_acquire_owned().is_ok());
    }

    #[tokio::test]
    async fn producer_lifecycle_waits_for_the_last_query_execution() {
        let sink = KafkaSink::new("events".to_owned(), kafka_config(2), None);
        sink.begin_execution().await;
        sink.begin_execution().await;

        sink.finish_execution().await.unwrap();
        assert_eq!(sink.state.lock().await.active_executions, 1);

        sink.finish_execution().await.unwrap();
        assert_eq!(sink.state.lock().await.active_executions, 0);
        assert_eq!(
            sink.finish_execution().await.unwrap_err().code,
            ErrorCode::Internal
        );
    }
}
