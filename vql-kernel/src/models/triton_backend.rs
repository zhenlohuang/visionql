use std::collections::BTreeMap;
use std::io::{Cursor, Seek, SeekFrom, Write};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use image::{DynamicImage, ImageFormat};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::backend::ModelBackend;
use super::postprocess::canonical_detection_output;
use super::registry::{
    RuntimeFactory, RuntimeResolution, deserialize_model_options, invalid_option,
};
use crate::catalog::{ModelDef, ModelType, ResolvedExecutionSpec, ResolvedModelDef, RuntimeSpec};
use crate::resources::{QueryBudget, QueryReservation};
use crate::{ErrorCode, Result, VqlError};

const SUPPORTED_TYPES: &[ModelType] = &[ModelType::ObjectDetection];
const CONTRACT_INPUT: &str = "image";
const CONTRACT_OUTPUT: &str = "detections";

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TritonOptions {
    model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    version: Option<String>,
}

impl TritonOptions {
    fn parse(options: &BTreeMap<String, serde_json::Value>) -> Result<Self> {
        let options: Self = deserialize_model_options("TRITON_INFERENCE_SERVER", options)?;
        validate_path_segment("WITH.model", &options.model)?;
        if let Some(version) = &options.version {
            validate_path_segment("WITH.version", version)?;
        }
        Ok(options)
    }

    fn runtime_spec(&self) -> RuntimeSpec {
        let mut options = BTreeMap::from([("model".to_owned(), serde_json::json!(self.model))]);
        if let Some(version) = &self.version {
            options.insert("version".to_owned(), serde_json::json!(version));
        }
        RuntimeSpec {
            kind: "triton-inference-server".to_owned(),
            protocol: Some("kserve_v2_http".to_owned()),
            options,
        }
    }
}

#[derive(Debug)]
pub(super) struct TritonRuntimeFactory;

#[async_trait]
impl RuntimeFactory for TritonRuntimeFactory {
    fn kind(&self) -> &str {
        "triton-inference-server"
    }

    fn supported_types(&self) -> &[ModelType] {
        SUPPORTED_TYPES
    }

    fn validate_declaration(&self, model: &ModelDef) -> Result<()> {
        validate_endpoint(&model.source)?;
        TritonOptions::parse(&model.options).map(|_| ())
    }

    async fn resolve(
        &self,
        model: &ModelDef,
        _cache_dir: &std::path::Path,
        cancel: CancellationToken,
    ) -> Result<RuntimeResolution> {
        self.validate_declaration(model)?;
        let options = TritonOptions::parse(&model.options)?;
        let backend =
            TritonServiceBackend::new(&model.source, &options.model, options.version.as_deref())?;
        tokio::select! {
            _ = cancel.cancelled() => {
                return Err(VqlError::new(ErrorCode::QueryCancelled, "model resolve cancelled"));
            }
            result = backend.validate_metadata() => result?,
        }
        Ok(RuntimeResolution {
            resolved_source: model.source.clone(),
            artifact_hash: None,
            execution: ResolvedExecutionSpec::Service {
                runtime: options.runtime_spec(),
            },
            volatile: options.version.is_none(),
        })
    }

    fn build_service(
        &self,
        model: &ResolvedModelDef,
        runtime: &RuntimeSpec,
    ) -> Result<Arc<dyn ModelBackend>> {
        let options = runtime_options(runtime)?;
        Ok(Arc::new(TritonServiceBackend::new_resolved(
            &model.resolved_source,
            &options.model,
            options.version.as_deref(),
        )?))
    }
}

fn runtime_options(spec: &RuntimeSpec) -> Result<TritonOptions> {
    if spec.protocol.as_deref() != Some("kserve_v2_http") {
        return Err(VqlError::new(
            ErrorCode::Internal,
            "resolved Triton Runtime has an invalid protocol",
        ));
    }
    TritonOptions::parse(&spec.options)
}

fn validate_endpoint(endpoint: &str) -> Result<()> {
    let url = reqwest::Url::parse(endpoint).map_err(|error| {
        VqlError::new(
            ErrorCode::InvalidLocation,
            "TRITON_INFERENCE_SERVER FROM must be an absolute HTTP(S) URL",
        )
        .with_source(error)
    })?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return invalid_option(
            "FROM",
            "TRITON_INFERENCE_SERVER requires an absolute HTTP(S) URL",
        );
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return invalid_option(
            "FROM",
            "credentials, query parameters, and fragments are not allowed",
        );
    }
    Ok(())
}

fn validate_path_segment(name: &str, value: &str) -> Result<()> {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        Ok(())
    } else {
        invalid_option(
            name,
            "must contain only ASCII letters, digits, '.', '-', or '_'",
        )
    }
}

#[derive(Debug)]
struct TritonServiceBackend {
    metadata_url: String,
    infer_url: String,
    client: reqwest::Client,
    metadata_validated: OnceLock<()>,
}

impl TritonServiceBackend {
    fn new(endpoint: &str, model: &str, version: Option<&str>) -> Result<Self> {
        Self::with_timeout(endpoint, model, version, Duration::from_secs(30), false)
    }

    fn new_resolved(endpoint: &str, model: &str, version: Option<&str>) -> Result<Self> {
        Self::with_timeout(endpoint, model, version, Duration::from_secs(30), true)
    }

    fn with_timeout(
        endpoint: &str,
        model: &str,
        version: Option<&str>,
        timeout: Duration,
        metadata_validated: bool,
    ) -> Result<Self> {
        validate_endpoint(endpoint)?;
        validate_path_segment("WITH.model", model)?;
        if let Some(version) = version {
            validate_path_segment("WITH.version", version)?;
        }
        let endpoint = endpoint.trim_end_matches('/');
        let metadata_url = if let Some(version) = version {
            format!("{endpoint}/v2/models/{model}/versions/{version}")
        } else {
            format!("{endpoint}/v2/models/{model}")
        };
        let infer_url = format!("{metadata_url}/infer");
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|error| {
                VqlError::new(
                    ErrorCode::Execution,
                    "failed to build Triton Inference Server client",
                )
                .with_source(error)
            })?;
        let validated = OnceLock::new();
        if metadata_validated {
            let _ = validated.set(());
        }
        Ok(Self {
            metadata_url,
            infer_url,
            client,
            metadata_validated: validated,
        })
    }

    async fn validate_metadata(&self) -> Result<()> {
        if self.metadata_validated.get().is_some() {
            return Ok(());
        }
        let metadata = self
            .client
            .get(&self.metadata_url)
            .send()
            .await
            .map_err(|error| {
                VqlError::new(
                    ErrorCode::Execution,
                    "Triton Inference Server metadata request failed",
                )
                .with_source(error)
            })?
            .error_for_status()
            .map_err(|error| {
                VqlError::new(
                    ErrorCode::Execution,
                    "Triton Inference Server metadata request returned an error status",
                )
                .with_source(error)
            })?
            .json::<ModelMetadata>()
            .await
            .map_err(|error| {
                VqlError::new(
                    ErrorCode::Execution,
                    "Triton KServe V2 model metadata is invalid",
                )
                .with_source(error)
            })?;
        validate_contract_tensor("input", &metadata.inputs, CONTRACT_INPUT)?;
        validate_contract_tensor("output", &metadata.outputs, CONTRACT_OUTPUT)?;
        let _ = self.metadata_validated.set(());
        Ok(())
    }

    async fn infer_inner(
        &self,
        images: Vec<DynamicImage>,
        budget: &QueryBudget,
    ) -> Result<arrow::array::ArrayRef> {
        self.validate_metadata().await?;
        let mut payload_reservations = Vec::new();
        let data = images
            .into_iter()
            .map(|image| encode_image(image, budget, &mut payload_reservations))
            .collect::<Result<Vec<_>>>()?;
        let request = InferRequest {
            inputs: vec![InferInput {
                name: CONTRACT_INPUT,
                shape: vec![i64::try_from(data.len()).map_err(|_| {
                    VqlError::new(ErrorCode::Execution, "model batch is too large")
                })?],
                datatype: "BYTES",
                data,
            }],
            outputs: vec![RequestedOutput {
                name: CONTRACT_OUTPUT,
            }],
        };
        let mut payload = BudgetedCursor::new(budget, crate::QueryResource::TritonPayload)?;
        if let Err(error) = serde_json::to_writer(&mut payload, &request) {
            if let Some(error) = payload.take_reservation_error() {
                return Err(error);
            }
            return Err(VqlError::new(
                ErrorCode::Execution,
                "failed to serialize Triton inference request",
            )
            .with_source(error));
        }
        let (payload, payload_reservation) = payload.into_parts();
        payload_reservations.push(payload_reservation);
        let mut response = self
            .client
            .post(&self.infer_url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(payload)
            .send()
            .await
            .map_err(|error| {
                VqlError::new(
                    ErrorCode::Execution,
                    "Triton Inference Server inference request failed",
                )
                .with_source(error)
            })?
            .error_for_status()
            .map_err(|error| {
                VqlError::new(
                    ErrorCode::Execution,
                    "Triton Inference Server inference returned an error status",
                )
                .with_source(error)
            })?;
        let mut response_bytes = Vec::new();
        let mut response_reservation = budget.reserve(crate::QueryResource::TritonPayload, 0)?;
        while let Some(chunk) = response.chunk().await.map_err(|error| {
            VqlError::new(ErrorCode::Execution, "Triton KServe V2 response is invalid")
                .with_source(error)
        })? {
            response_reservation.try_grow(chunk.len())?;
            response_bytes.extend_from_slice(&chunk);
        }
        let response =
            serde_json::from_slice::<InferResponse>(&response_bytes).map_err(|error| {
                VqlError::new(ErrorCode::Execution, "Triton KServe V2 response is invalid")
                    .with_source(error)
            })?;
        let output = response
            .outputs
            .into_iter()
            .find(|output| output.name == CONTRACT_OUTPUT)
            .ok_or_else(|| {
                VqlError::new(
                    ErrorCode::Execution,
                    "Triton response does not contain canonical 'detections' output",
                )
            })?;
        if output.datatype != "BYTES" || output.shape != [request.inputs[0].shape[0]] {
            return Err(VqlError::new(
                ErrorCode::Execution,
                "Triton canonical 'detections' output must be BYTES with shape [batch]",
            ));
        }
        let rows = output
            .data
            .into_iter()
            .map(parse_detection_row)
            .collect::<Result<Vec<_>>>()?;
        Ok(canonical_detection_output(rows))
    }
}

fn encode_image(
    image: DynamicImage,
    budget: &QueryBudget,
    reservations: &mut Vec<QueryReservation>,
) -> Result<String> {
    let mut bytes = BudgetedCursor::new(budget, crate::QueryResource::TritonPayload)?;
    if let Err(error) = image.write_to(&mut bytes, ImageFormat::Png) {
        if let Some(error) = bytes.take_reservation_error() {
            return Err(error);
        }
        return Err(VqlError::new(
            ErrorCode::Execution,
            "failed to encode IMAGE for Triton Inference Server",
        )
        .with_source(error));
    }
    let (bytes, compressed_reservation) = bytes.into_parts();
    reservations.push(compressed_reservation);
    let encoded_length = base64::encoded_len(bytes.len(), true)
        .and_then(|length| length.checked_add(4))
        .ok_or_else(|| {
            VqlError::new(
                ErrorCode::ResourceExhausted,
                "Triton IMAGE payload exceeds platform limits",
            )
        })?;
    reservations.push(budget.reserve(crate::QueryResource::TritonPayload, encoded_length)?);
    let mut encoded = String::with_capacity(encoded_length);
    encoded.push_str("b64:");
    base64::engine::general_purpose::STANDARD.encode_string(bytes, &mut encoded);
    Ok(encoded)
}

fn parse_detection_row(value: String) -> Result<Vec<(String, f32, [f32; 4])>> {
    let detections = serde_json::from_str::<Vec<ServiceDetection>>(&value).map_err(|error| {
        VqlError::new(
            ErrorCode::Execution,
            "Triton canonical detection row is invalid",
        )
        .with_source(error)
    })?;
    detections
        .into_iter()
        .map(|detection| {
            if detection.label.is_empty() {
                return Err(VqlError::new(
                    ErrorCode::Execution,
                    "Triton detection label must not be empty",
                ));
            }
            if !detection.confidence.is_finite()
                || !(0.0..=1.0).contains(&detection.confidence)
                || detection
                    .bbox
                    .iter()
                    .any(|value| !value.is_finite() || !(0.0..=1.0).contains(value))
            {
                return Err(VqlError::new(
                    ErrorCode::Execution,
                    "Triton detections must use finite normalized confidence and BOX2D values",
                ));
            }
            Ok((detection.label, detection.confidence, detection.bbox))
        })
        .collect()
}

#[async_trait]
impl ModelBackend for TritonServiceBackend {
    async fn infer(
        &self,
        images: Vec<DynamicImage>,
        cancel: CancellationToken,
        budget: &QueryBudget,
    ) -> Result<arrow::array::ArrayRef> {
        tokio::select! {
            _ = cancel.cancelled() => Err(VqlError::new(ErrorCode::QueryCancelled, "query cancelled")),
            result = self.infer_inner(images, budget) => result,
        }
    }
}

struct BudgetedCursor {
    inner: Cursor<Vec<u8>>,
    reservation: QueryReservation,
    reservation_error: Option<VqlError>,
}

impl BudgetedCursor {
    fn new(budget: &QueryBudget, resource: crate::QueryResource) -> Result<Self> {
        Ok(Self {
            inner: Cursor::new(Vec::new()),
            reservation: budget.reserve(resource, 0)?,
            reservation_error: None,
        })
    }

    fn into_parts(self) -> (Vec<u8>, QueryReservation) {
        (self.inner.into_inner(), self.reservation)
    }

    fn take_reservation_error(&mut self) -> Option<VqlError> {
        self.reservation_error.take()
    }
}

impl Write for BudgetedCursor {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let end = usize::try_from(self.inner.position())
            .ok()
            .and_then(|position| position.checked_add(bytes.len()))
            .ok_or_else(|| std::io::Error::other("buffer size exceeds platform limits"))?;
        if end > self.reservation.size()
            && let Err(error) = self.reservation.try_resize(end)
        {
            let message = error.to_string();
            self.reservation_error = Some(error);
            return Err(std::io::Error::other(message));
        }
        self.inner.write(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl Seek for BudgetedCursor {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(position)
    }
}

#[derive(Debug, Deserialize)]
struct ModelMetadata {
    inputs: Vec<TensorMetadata>,
    outputs: Vec<TensorMetadata>,
}

#[derive(Debug, Deserialize)]
struct TensorMetadata {
    name: String,
    datatype: String,
    shape: Vec<i64>,
}

fn validate_contract_tensor(role: &str, tensors: &[TensorMetadata], name: &str) -> Result<()> {
    let tensor = tensors
        .iter()
        .find(|tensor| tensor.name == name)
        .ok_or_else(|| {
            VqlError::new(
                ErrorCode::Execution,
                format!("Triton model does not expose canonical {role} '{name}'"),
            )
        })?;
    if tensor.datatype != "BYTES" || tensor.shape != [-1] {
        return Err(VqlError::new(
            ErrorCode::Execution,
            format!("Triton canonical {role} '{name}' must be BYTES with shape [-1]"),
        ));
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct InferRequest {
    inputs: Vec<InferInput>,
    outputs: Vec<RequestedOutput>,
}

#[derive(Debug, Serialize)]
struct InferInput {
    name: &'static str,
    shape: Vec<i64>,
    datatype: &'static str,
    data: Vec<String>,
}

#[derive(Debug, Serialize)]
struct RequestedOutput {
    name: &'static str,
}

#[derive(Debug, Deserialize)]
struct InferResponse {
    outputs: Vec<InferOutput>,
}

#[derive(Debug, Deserialize)]
struct InferOutput {
    name: String,
    shape: Vec<i64>,
    datatype: String,
    data: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ServiceDetection {
    label: String,
    confidence: f32,
    bbox: [f32; 4],
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ListArray};
    use image::DynamicImage;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    fn metadata_body() -> &'static str {
        r#"{"inputs":[{"name":"image","datatype":"BYTES","shape":[-1]}],"outputs":[{"name":"detections","datatype":"BYTES","shape":[-1]}]}"#
    }

    fn query_budget() -> QueryBudget {
        QueryBudget::new(16 * 1024 * 1024)
    }

    #[test]
    fn triton_payload_limit_fails_before_buffer_growth() {
        let budget = QueryBudget::new(1);
        let probe = budget.clone();
        let mut reservations = Vec::new();

        let error =
            encode_image(DynamicImage::new_rgb8(2, 2), &budget, &mut reservations).unwrap_err();

        assert_eq!(error.code, ErrorCode::ResourceExhausted);
        assert!(reservations.is_empty());
        assert!(
            probe
                .reserve(crate::QueryResource::TritonPayload, 1)
                .is_ok()
        );
    }

    #[tokio::test]
    async fn triton_service_uses_the_canonical_object_detection_contract() {
        let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
            return;
        };
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let mut buffer = [0_u8; 16 * 1024];
            let (mut stream, _) = listener.accept().unwrap();
            let count = stream.read(&mut buffer).unwrap();
            assert!(
                String::from_utf8_lossy(&buffer[..count])
                    .contains("GET /v2/models/detector/versions/1")
            );
            write_response(&mut stream, metadata_body());

            let (mut stream, _) = listener.accept().unwrap();
            let count = stream.read(&mut buffer).unwrap();
            let request = String::from_utf8_lossy(&buffer[..count]);
            assert!(request.contains("POST /v2/models/detector/versions/1/infer"));
            assert!(request.contains("\"name\":\"image\""));
            assert!(request.contains("b64:"));
            write_response(
                &mut stream,
                r#"{"outputs":[{"name":"detections","shape":[1],"datatype":"BYTES","data":["[{\"label\":\"person\",\"confidence\":0.9,\"bbox\":[0.1,0.2,0.3,0.4]}]"]}]}"#,
            );
        });
        let backend =
            TritonServiceBackend::new(&format!("http://{address}"), "detector", Some("1")).unwrap();
        let budget = query_budget();
        let output = backend
            .infer(
                vec![DynamicImage::new_rgb8(2, 2)],
                CancellationToken::new(),
                &budget,
            )
            .await
            .unwrap();
        assert_eq!(
            output.as_any().downcast_ref::<ListArray>().unwrap().len(),
            1
        );
        server.join().unwrap();
    }

    #[tokio::test]
    async fn triton_rejects_a_raw_tensor_model() {
        let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
            return;
        };
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0_u8; 4096];
            let _ = stream.read(&mut buffer);
            write_response(
                &mut stream,
                r#"{"inputs":[{"name":"images","datatype":"FP32","shape":[-1,3,640,640]}],"outputs":[{"name":"output0","datatype":"FP32","shape":[-1,-1,6]}]}"#,
            );
        });
        let backend =
            TritonServiceBackend::new(&format!("http://{address}"), "detector", None).unwrap();
        let error = backend.validate_metadata().await.unwrap_err();
        assert!(error.message.contains("canonical input 'image'"));
        server.join().unwrap();
    }

    #[test]
    fn triton_rejects_embedded_processor_options() {
        let options = BTreeMap::from([
            ("model".to_owned(), serde_json::json!("detector")),
            (
                "output".to_owned(),
                serde_json::json!({"format": "yolo_e2e"}),
            ),
        ]);

        let error = TritonOptions::parse(&options).unwrap_err();

        assert_eq!(error.code, ErrorCode::InvalidOption);
        assert!(error.message.contains("TRITON_INFERENCE_SERVER"));
        assert!(error.message.contains("output"));
    }

    fn write_response(stream: &mut std::net::TcpStream, body: &str) {
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .unwrap();
    }
}
