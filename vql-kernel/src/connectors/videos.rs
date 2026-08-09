use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Array, Int32Array, Int64Array, StringArray, TimestampMillisecondArray,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::ScalarValue;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;

use crate::media::{FrameInfo, MediaRuntime, TimeRange, VideoMetadata};
use crate::types::{ImageRef, ImageRefBuilder, image_field, make_locator};
use crate::{ErrorCode, Result, VqlError};

const BATCH_SIZE: usize = 1024;

pub(crate) fn videos_schema(synthetic_event_time: bool) -> SchemaRef {
    let mut ts = Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        false,
    );
    if synthetic_event_time {
        ts = ts.with_metadata(std::collections::HashMap::from([(
            "visionql.synthetic_event_time".to_owned(),
            "true".to_owned(),
        )]));
    }
    Arc::new(Schema::new(vec![
        Field::new("uri", DataType::Utf8, false),
        ts,
        Field::new("pts_ms", DataType::Int64, false),
        Field::new("frame_id", DataType::UInt64, false),
        image_field("frame", false),
        Field::new("duration", DataType::Float64, true),
        Field::new("fps", DataType::Float64, true),
        Field::new("width", DataType::Int32, true),
        Field::new("height", DataType::Int32, true),
        Field::new("codec", DataType::Utf8, true),
    ]))
}

#[derive(Debug)]
pub(crate) struct VideosTableProvider {
    root: PathBuf,
    table_revision: i64,
    recursive: bool,
    fps: f64,
    start_time_ms: Option<i64>,
    schema: SchemaRef,
    media: Arc<MediaRuntime>,
}

impl VideosTableProvider {
    pub(crate) fn try_new(
        root: impl Into<PathBuf>,
        table_revision: i64,
        recursive: bool,
        fps: Option<f64>,
        start_time_ms: Option<i64>,
        media: Arc<MediaRuntime>,
    ) -> Result<Self> {
        let root = root.into();
        if !root.is_dir() {
            return Err(VqlError::new(
                ErrorCode::InvalidLocation,
                format!(
                    "video location is not a readable directory: {}",
                    root.display()
                ),
            ));
        }
        let fps = fps.unwrap_or(1.0);
        if !fps.is_finite() || fps <= 0.0 || fps > 120.0 {
            return Err(VqlError::new(
                ErrorCode::InvalidOption,
                "video fps must be greater than 0 and at most 120",
            ));
        }
        Ok(Self {
            root,
            table_revision,
            recursive,
            fps,
            start_time_ms,
            schema: videos_schema(start_time_ms.is_none()),
            media,
        })
    }
}

#[async_trait]
impl TableProvider for VideosTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if time_bound(filter, self.start_time_ms.unwrap_or(0)).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let range = filters.iter().fold(TimeRange::default(), |range, filter| {
            apply_time_bound(range, filter, self.start_time_ms.unwrap_or(0))
        });
        Ok(Arc::new(VideosExec::new(
            self.root.clone(),
            self.table_revision,
            self.recursive,
            self.fps,
            self.start_time_ms,
            Arc::clone(&self.schema),
            projection.cloned(),
            limit,
            range,
            Arc::clone(&self.media),
        )))
    }
}

struct VideosExec {
    root: PathBuf,
    table_revision: i64,
    recursive: bool,
    fps: f64,
    start_time_ms: Option<i64>,
    source_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    range: TimeRange,
    media: Arc<MediaRuntime>,
    properties: Arc<PlanProperties>,
}

impl VideosExec {
    #[allow(clippy::too_many_arguments)]
    fn new(
        root: PathBuf,
        table_revision: i64,
        recursive: bool,
        fps: f64,
        start_time_ms: Option<i64>,
        source_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        range: TimeRange,
        media: Arc<MediaRuntime>,
    ) -> Self {
        let output_schema = projected_schema(&source_schema, projection.as_deref());
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            root,
            table_revision,
            recursive,
            fps,
            start_time_ms,
            source_schema,
            projection,
            limit,
            range,
            media,
            properties,
        }
    }

    fn output_schema(&self) -> SchemaRef {
        projected_schema(&self.source_schema, self.projection.as_deref())
    }
}

impl Debug for VideosExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VideosExec")
            .field("root", &self.root)
            .field("fps", &self.fps)
            .field("range", &self.range)
            .field("projection", &self.projection)
            .finish()
    }
}

impl DisplayAs for VideosExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "VideosExec: root={}, fps={}, range={:?}, projection={:?}",
            self.root.display(),
            self.fps,
            self.range,
            self.projection
        )
    }
}

impl ExecutionPlan for VideosExec {
    fn name(&self) -> &str {
        "VideosExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "VideosExec is a leaf and cannot accept children".to_owned(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "VideosExec has one partition, got {partition}"
            )));
        }
        let root = self.root.clone();
        let table_revision = self.table_revision;
        let recursive = self.recursive;
        let fps = self.fps;
        let start_time_ms = self.start_time_ms;
        let source_schema = Arc::clone(&self.source_schema);
        let output_schema = self.output_schema();
        let stream_schema = Arc::clone(&output_schema);
        let projection = self.projection.clone();
        let limit = self.limit;
        let range = self.range;
        let media = Arc::clone(&self.media);

        let stream = async_stream::try_stream! {
            let store = LocalFileSystem::new_with_prefix(&root)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            let mut listed = store.list(None);
            let mut objects = Vec::new();
            while let Some(meta) = listed.next().await {
                let meta = meta.map_err(|error| DataFusionError::External(Box::new(error)))?;
                let relative = meta.location.as_ref();
                if is_supported_video(relative) && (recursive || !relative.contains('/')) {
                    objects.push(meta);
                }
            }
            objects.sort_by(|left, right| left.location.cmp(&right.location));
            let needs_probe = projection
                .as_deref()
                .is_none_or(|indices| indices.iter().any(|index| matches!(index, 4..=9)));
            let mut rows = Vec::new();
            'files: for object in objects {
                let relative = object.location.as_ref();
                let path = root.join(relative);
                let metadata = if needs_probe {
                    Some(
                        media
                            .probe(&path)
                            .map_err(|error| DataFusionError::External(Box::new(error)))?,
                    )
                } else {
                    None
                };
                let frames = media
                    .sampled_frames(&path, range, fps)
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                for frame in frames {
                    rows.push(VideoRow {
                        relative: relative.to_owned(),
                        path: path.clone(),
                        frame,
                        metadata: metadata.clone(),
                    });
                    if limit.is_some_and(|limit| rows.len() >= limit) {
                        break 'files;
                    }
                }
            }
            if rows.is_empty() {
                yield build_batch(
                    table_revision,
                    start_time_ms,
                    &source_schema,
                    &output_schema,
                    projection.as_deref(),
                    &[],
                )?;
            }
            for chunk in rows.chunks(BATCH_SIZE) {
                yield build_batch(
                    table_revision,
                    start_time_ms,
                    &source_schema,
                    &output_schema,
                    projection.as_deref(),
                    chunk,
                )?;
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }
}

#[derive(Debug, Clone)]
struct VideoRow {
    relative: String,
    path: PathBuf,
    frame: FrameInfo,
    metadata: Option<VideoMetadata>,
}

fn build_batch(
    table_revision: i64,
    start_time_ms: Option<i64>,
    source_schema: &SchemaRef,
    output_schema: &SchemaRef,
    projection: Option<&[usize]>,
    rows: &[VideoRow],
) -> DataFusionResult<RecordBatch> {
    let mut images = ImageRefBuilder::with_capacity(rows.len());
    for row in rows {
        images.append(ImageRef::referenced(
            row.path.to_string_lossy(),
            make_locator(table_revision, &row.relative, Some(row.frame.pts_ms)),
            row.metadata.as_ref().map(|metadata| metadata.width),
            row.metadata.as_ref().map(|metadata| metadata.height),
        ));
    }
    let all_columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.path.to_string_lossy().into_owned())
                .collect::<Vec<_>>(),
        )),
        Arc::new(
            TimestampMillisecondArray::from(
                rows.iter()
                    .map(|row| start_time_ms.unwrap_or(0) + row.frame.pts_ms)
                    .collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        ),
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.frame.pts_ms).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter()
                .map(|row| row.frame.frame_id)
                .collect::<Vec<_>>(),
        )),
        Arc::new(images.finish()),
        Arc::new(Float64Array::from(
            rows.iter()
                .map(|row| {
                    row.metadata
                        .as_ref()
                        .map(|metadata| metadata.duration_ms as f64 / 1_000.0)
                })
                .collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter()
                .map(|row| {
                    row.metadata
                        .as_ref()
                        .and_then(|metadata| metadata.source_fps)
                })
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int32Array::from(
            rows.iter()
                .map(|row| row.metadata.as_ref().map(|metadata| metadata.width))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int32Array::from(
            rows.iter()
                .map(|row| row.metadata.as_ref().map(|metadata| metadata.height))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| {
                    row.metadata
                        .as_ref()
                        .map(|metadata| metadata.codec.as_str())
                })
                .collect::<Vec<_>>(),
        )),
    ];
    debug_assert_eq!(source_schema.fields().len(), 10);
    let columns = match projection {
        Some(indices) => indices
            .iter()
            .map(|index| Arc::clone(&all_columns[*index]))
            .collect::<Vec<_>>(),
        None => all_columns,
    };
    if columns.is_empty() {
        RecordBatch::try_new_with_options(
            Arc::clone(output_schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(rows.len())),
        )
        .map_err(DataFusionError::from)
    } else {
        RecordBatch::try_new(Arc::clone(output_schema), columns).map_err(DataFusionError::from)
    }
}

fn projected_schema(schema: &SchemaRef, projection: Option<&[usize]>) -> SchemaRef {
    projection.map_or_else(
        || Arc::clone(schema),
        |indices| {
            Arc::new(Schema::new(
                indices
                    .iter()
                    .map(|index| schema.field(*index).clone())
                    .collect::<Vec<_>>(),
            ))
        },
    )
}

fn is_supported_video(relative: &str) -> bool {
    Path::new(relative)
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "mp4" | "mov" | "mkv" | "avi" | "webm" | "m4v"
            )
        })
}

fn apply_time_bound(range: TimeRange, expr: &Expr, base_ms: i64) -> TimeRange {
    let Some((operator, value)) = time_bound(expr, base_ms) else {
        return range;
    };
    match operator {
        Operator::Gt => TimeRange {
            start_ms: Some(
                range
                    .start_ms
                    .unwrap_or(i64::MIN)
                    .max(value.saturating_add(1)),
            ),
            ..range
        },
        Operator::GtEq | Operator::Eq => TimeRange {
            start_ms: Some(range.start_ms.unwrap_or(i64::MIN).max(value)),
            ..range
        },
        Operator::Lt => TimeRange {
            end_ms: Some(range.end_ms.unwrap_or(i64::MAX).min(value)),
            ..range
        },
        Operator::LtEq => TimeRange {
            end_ms: Some(
                range
                    .end_ms
                    .unwrap_or(i64::MAX)
                    .min(value.saturating_add(1)),
            ),
            ..range
        },
        _ => range,
    }
}

fn time_bound(expr: &Expr, base_ms: i64) -> Option<(Operator, i64)> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };
    if let (Expr::Column(column), Expr::Literal(value, _)) = (&*binary.left, &*binary.right)
        && column.name == "ts"
    {
        return scalar_timestamp_ms(value).map(|value| (binary.op, value - base_ms));
    }
    if let (Expr::Literal(value, _), Expr::Column(column)) = (&*binary.left, &*binary.right)
        && column.name == "ts"
    {
        let operator = match binary.op {
            Operator::Gt => Operator::Lt,
            Operator::GtEq => Operator::LtEq,
            Operator::Lt => Operator::Gt,
            Operator::LtEq => Operator::GtEq,
            operator => operator,
        };
        return scalar_timestamp_ms(value).map(|value| (operator, value - base_ms));
    }
    None
}

fn scalar_timestamp_ms(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::TimestampSecond(Some(value), _) => Some(value.saturating_mul(1_000)),
        ScalarValue::TimestampMillisecond(Some(value), _) => Some(*value),
        ScalarValue::TimestampMicrosecond(Some(value), _) => Some(value / 1_000),
        ScalarValue::TimestampNanosecond(Some(value), _) => Some(value / 1_000_000),
        _ => None,
    }
}
