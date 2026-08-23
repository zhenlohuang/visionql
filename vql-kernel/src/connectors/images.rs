use crate::types::{ImageRef, ImageRefBuilder, make_locator};
use arrow::array::{ArrayRef, Int32Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::{ObjectMeta, ObjectStore};
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const BATCH_SIZE: usize = 1024;

pub(crate) use vql_catalog::images_schema;

#[derive(Debug, Default)]
pub(crate) struct ImagesScanMetrics {
    dimension_probes: AtomicUsize,
}

impl ImagesScanMetrics {
    #[cfg(test)]
    pub(crate) fn dimension_probes(&self) -> usize {
        self.dimension_probes.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub(crate) struct ImagesTableProvider {
    root: PathBuf,
    table_generation: i64,
    recursive: bool,
    schema: SchemaRef,
    metrics: Arc<ImagesScanMetrics>,
}

impl ImagesTableProvider {
    pub(crate) fn try_new(
        root: impl Into<PathBuf>,
        table_generation: i64,
        recursive: bool,
    ) -> crate::Result<Self> {
        let root = root.into();
        if !root.is_dir() {
            return Err(crate::VqlError::new(
                crate::ErrorCode::InvalidLocation,
                format!(
                    "image location is not a readable directory: {}",
                    root.display()
                ),
            ));
        }
        Ok(Self {
            root,
            table_generation,
            recursive,
            schema: images_schema(),
            metrics: Arc::new(ImagesScanMetrics::default()),
        })
    }

    #[cfg(test)]
    pub(crate) fn metrics(&self) -> Arc<ImagesScanMetrics> {
        Arc::clone(&self.metrics)
    }
}

#[async_trait]
impl TableProvider for ImagesTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ImagesExec::new(
            self.root.clone(),
            self.table_generation,
            self.recursive,
            Arc::clone(&self.schema),
            projection.cloned(),
            limit,
            Arc::clone(&self.metrics),
        )))
    }
}

struct ImagesExec {
    root: PathBuf,
    table_generation: i64,
    recursive: bool,
    source_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    metrics: Arc<ImagesScanMetrics>,
    properties: Arc<PlanProperties>,
}

impl ImagesExec {
    fn new(
        root: PathBuf,
        table_generation: i64,
        recursive: bool,
        source_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        metrics: Arc<ImagesScanMetrics>,
    ) -> Self {
        let schema = projected_schema(&source_schema, projection.as_deref());
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            root,
            table_generation,
            recursive,
            source_schema,
            projection,
            limit,
            metrics,
            properties,
        }
    }

    fn output_schema(&self) -> SchemaRef {
        projected_schema(&self.source_schema, self.projection.as_deref())
    }
}

impl Debug for ImagesExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImagesExec")
            .field("root", &self.root)
            .field("recursive", &self.recursive)
            .field("projection", &self.projection)
            .field("limit", &self.limit)
            .finish()
    }
}

impl DisplayAs for ImagesExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        let projection = self
            .projection
            .as_deref()
            .map(|indices| {
                indices
                    .iter()
                    .map(|index| self.source_schema.field(*index).name().as_str())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| {
                self.source_schema
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect()
            });
        write!(
            f,
            "ImagesExec: root={}, recursive={}, projection={:?}",
            self.root.display(),
            self.recursive,
            projection
        )
    }
}

impl ExecutionPlan for ImagesExec {
    fn name(&self) -> &str {
        "ImagesExec"
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
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "ImagesExec is a leaf and cannot accept children".to_owned(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "ImagesExec has one partition, got {partition}"
            )));
        }

        let root = self.root.clone();
        let table_generation = self.table_generation;
        let recursive = self.recursive;
        let source_schema = Arc::clone(&self.source_schema);
        let output_schema = self.output_schema();
        let projection = self.projection.clone();
        let limit = self.limit;
        let metrics = Arc::clone(&self.metrics);
        let stream_schema = Arc::clone(&output_schema);

        let stream = async_stream::try_stream! {
            let store = LocalFileSystem::new_with_prefix(&root)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            let mut listed = store.list(None);
            let mut objects = Vec::new();
            while let Some(meta) = listed.next().await {
                let meta = meta.map_err(|error| DataFusionError::External(Box::new(error)))?;
                let relative = meta.location.as_ref();
                if is_supported_image(relative) && (recursive || !relative.contains('/')) {
                    objects.push(meta);
                }
            }
            objects.sort_by(|left, right| left.location.cmp(&right.location));
            if let Some(limit) = limit {
                objects.truncate(limit);
            }
            if objects.is_empty() {
                yield build_batch(
                    &root,
                    table_generation,
                    &source_schema,
                    &output_schema,
                    projection.as_deref(),
                    &[],
                    &metrics,
                )?;
            }
            for chunk in objects.chunks(BATCH_SIZE) {
                yield build_batch(
                    &root,
                    table_generation,
                    &source_schema,
                    &output_schema,
                    projection.as_deref(),
                    chunk,
                    &metrics,
                )?;
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }
}

fn projected_schema(schema: &SchemaRef, projection: Option<&[usize]>) -> SchemaRef {
    match projection {
        Some(indices) => Arc::new(Schema::new(
            indices
                .iter()
                .map(|index| schema.field(*index).clone())
                .collect::<Vec<_>>(),
        )),
        None => Arc::clone(schema),
    }
}

fn is_supported_image(relative: &str) -> bool {
    Path::new(relative)
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "jpg" | "jpeg" | "png"
            )
        })
}

fn build_batch(
    root: &Path,
    table_generation: i64,
    source_schema: &SchemaRef,
    output_schema: &SchemaRef,
    projection: Option<&[usize]>,
    objects: &[ObjectMeta],
    metrics: &ImagesScanMetrics,
) -> DataFusionResult<RecordBatch> {
    let need_dimensions = projection
        .map(|indices| indices.iter().any(|index| matches!(index, 1..=3)))
        .unwrap_or(true);
    let mut uris = Vec::with_capacity(objects.len());
    let mut images = ImageRefBuilder::with_capacity(objects.len());
    let mut widths = Vec::with_capacity(objects.len());
    let mut heights = Vec::with_capacity(objects.len());
    let mut captured_at = Vec::with_capacity(objects.len());

    for object in objects {
        let relative = object.location.as_ref();
        let path = root.join(relative);
        let uri = path.to_string_lossy().into_owned();
        let dimensions = if need_dimensions {
            metrics.dimension_probes.fetch_add(1, Ordering::Relaxed);
            image::image_dimensions(&path)
                .ok()
                .and_then(|(width, height)| {
                    Some((i32::try_from(width).ok()?, i32::try_from(height).ok()?))
                })
        } else {
            None
        };
        let (width, height) = dimensions.unzip();
        images.append(ImageRef::referenced(
            uri.clone(),
            make_locator(table_generation, relative, None),
            width,
            height,
        ));
        uris.push(uri);
        widths.push(width);
        heights.push(height);
        captured_at.push(Some(object.last_modified.timestamp_millis()));
    }

    let all_columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(uris)),
        Arc::new(images.finish()),
        Arc::new(Int32Array::from(widths)),
        Arc::new(Int32Array::from(heights)),
        Arc::new(TimestampMillisecondArray::from(captured_at).with_timezone("UTC")),
    ];
    let columns = match projection {
        Some(indices) => indices
            .iter()
            .map(|index| Arc::clone(&all_columns[*index]))
            .collect(),
        None => all_columns,
    };
    debug_assert_eq!(source_schema.fields().len(), 5);
    if columns.is_empty() {
        RecordBatch::try_new_with_options(
            Arc::clone(output_schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(objects.len())),
        )
        .map_err(DataFusionError::from)
    } else {
        RecordBatch::try_new(Arc::clone(output_schema), columns).map_err(DataFusionError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use image::{Rgb, RgbImage};
    use tempfile::tempdir;

    #[test]
    fn custom_provider_compiles_and_honors_projection() {
        let temp = tempdir().unwrap();
        RgbImage::from_pixel(8, 4, Rgb([1, 2, 3]))
            .save(temp.path().join("a.png"))
            .unwrap();
        let provider = Arc::new(ImagesTableProvider::try_new(temp.path(), 1, false).unwrap());
        let metrics = provider.metrics();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let context = SessionContext::new_with_config(SessionConfig::new());
            context.register_table("photos", provider).unwrap();
            let batches = context
                .sql("SELECT uri FROM photos")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        });
        assert_eq!(metrics.dimension_probes(), 0);
    }
}
