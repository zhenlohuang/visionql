use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::array::{Array, ArrayRef, BinaryArray, StructArray};
use arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use tokio_util::sync::CancellationToken;

use super::BoundInferenceParams;
use super::backend::{MockBackend, ModelBackend};
use super::pipeline::BatchingOwner;
use super::postprocess::{
    filter_and_scatter_detections, mock_detection_output, mock_primary_label,
};
use super::registry::PipelineRegistry;
use super::scheduler::ModelScheduler;
use crate::catalog::{
    CatalogStore, ModelType, ResolvedExecutionSpec, ResolvedModelDef, TableProviderKind,
};
use crate::media::{DecodedFrame, MediaRuntime};
use crate::types::parse_locator;
use crate::{ErrorCode, Result, VqlError};

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ModelCounters {
    pub(crate) inference_rows: u64,
    pub(crate) inference_batches: u64,
    pub(crate) inference_errors: u64,
}

#[derive(Debug)]
pub(crate) struct ModelRuntime {
    catalog: Arc<CatalogStore>,
    media: Arc<MediaRuntime>,
    registry: Arc<PipelineRegistry>,
    schedulers: Mutex<HashMap<String, Arc<ModelScheduler>>>,
    inference_rows: AtomicU64,
    inference_batches: AtomicU64,
    inference_errors: AtomicU64,
    samples: Mutex<Vec<(u64, u64)>>,
}

impl ModelRuntime {
    pub(crate) fn new(
        catalog: Arc<CatalogStore>,
        media: Arc<MediaRuntime>,
        registry: Arc<PipelineRegistry>,
    ) -> Self {
        Self {
            catalog,
            media,
            registry,
            schedulers: Mutex::new(HashMap::new()),
            inference_rows: AtomicU64::new(0),
            inference_batches: AtomicU64::new(0),
            inference_errors: AtomicU64::new(0),
            samples: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn sample_count(&self) -> usize {
        self.samples.lock().map_or(0, |samples| samples.len())
    }

    pub(crate) fn samples_since(&self, start: usize) -> Vec<(u64, u64)> {
        self.samples
            .lock()
            .map(|samples| samples.get(start..).unwrap_or_default().to_vec())
            .unwrap_or_default()
    }

    pub(crate) fn counters(&self) -> ModelCounters {
        ModelCounters {
            inference_rows: self.inference_rows.load(Ordering::Relaxed),
            inference_batches: self.inference_batches.load(Ordering::Relaxed),
            inference_errors: self.inference_errors.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn evict_stale(&self) -> Result<()> {
        let live_fingerprints = self.live_model_fingerprints()?;
        self.schedulers
            .lock()
            .map_err(|_| VqlError::new(ErrorCode::Internal, "model scheduler cache was poisoned"))?
            .retain(|fingerprint, _| live_fingerprints.contains(fingerprint));
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn cached_pipeline_count(&self) -> usize {
        self.schedulers.lock().map_or(0, |cache| cache.len())
    }

    async fn scheduler(&self, model: &ResolvedModelDef) -> Result<Arc<ModelScheduler>> {
        let live_fingerprints = self.live_model_fingerprints()?;
        let cacheable = live_fingerprints.contains(&model.semantic_fingerprint);
        let key = model.semantic_fingerprint.clone();
        {
            let mut schedulers = self.schedulers.lock().map_err(|_| {
                VqlError::new(ErrorCode::Internal, "model scheduler cache was poisoned")
            })?;
            schedulers.retain(|fingerprint, _| live_fingerprints.contains(fingerprint));
            if let Some(scheduler) = schedulers.get(&key) {
                return Ok(Arc::clone(scheduler));
            }
        }

        let (backend, batching_owner): (Arc<dyn ModelBackend>, BatchingOwner) =
            if model.source.starts_with("mock://") {
                let ResolvedExecutionSpec::Embedded { post_processor, .. } = &model.execution
                else {
                    return Err(VqlError::new(
                        ErrorCode::Internal,
                        "mock Models require an embedded Runtime",
                    ));
                };
                (
                    Arc::new(MockBackend::new(mock_primary_label(post_processor)?)),
                    BatchingOwner::VisionQl,
                )
            } else {
                let registry = Arc::clone(&self.registry);
                let model = model.clone();
                tokio::task::spawn_blocking(move || registry.compile_backend(&model))
                    .await
                    .map_err(|error| {
                        VqlError::new(ErrorCode::Execution, "model Runtime build task failed")
                            .with_source(error)
                    })??
            };
        let scheduler = Arc::new(ModelScheduler::new(backend, batching_owner));
        if cacheable {
            let mut schedulers = self.schedulers.lock().map_err(|_| {
                VqlError::new(ErrorCode::Internal, "model scheduler cache was poisoned")
            })?;
            if let Some(existing) = schedulers.get(&key) {
                return Ok(Arc::clone(existing));
            }
            schedulers.insert(key, Arc::clone(&scheduler));
        }
        Ok(scheduler)
    }

    fn live_model_fingerprints(&self) -> Result<HashSet<String>> {
        Ok(self
            .catalog
            .snapshot()?
            .models()
            .filter_map(|(_, model)| {
                model
                    .definition
                    .resolved
                    .as_ref()
                    .map(|resolved| resolved.semantic_fingerprint.clone())
            })
            .collect())
    }

    fn decode_image(&self, images: &StructArray, row: usize) -> Result<image::DynamicImage> {
        let encoded = images
            .column(4)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| VqlError::new(ErrorCode::Internal, "IMAGE encoded field is invalid"))?;
        if !encoded.is_null(row) {
            return image::load_from_memory(encoded.value(row)).map_err(|error| {
                VqlError::new(ErrorCode::Execution, "IMAGE encoded bytes are invalid")
                    .with_source(error)
            });
        }
        let buffer_ids = images
            .column(8)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| {
                VqlError::new(ErrorCode::Internal, "IMAGE buffer_id field is invalid")
            })?;
        let buffer_slots = images
            .column(9)
            .as_any()
            .downcast_ref::<arrow::array::UInt32Array>()
            .ok_or_else(|| {
                VqlError::new(ErrorCode::Internal, "IMAGE buffer_slot field is invalid")
            })?;
        if !buffer_ids.is_null(row) && !buffer_slots.is_null(row) {
            return decoded_image(
                self.media
                    .resolve_buffered_frame(buffer_ids.value(row), buffer_slots.value(row))?,
            );
        }
        let locators = images
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| VqlError::new(ErrorCode::Internal, "IMAGE locator field is invalid"))?;
        if locators.is_null(row) {
            return Err(VqlError::new(
                ErrorCode::Execution,
                "IMAGE has neither encoded bytes, a frame buffer slot, nor a locator",
            ));
        }
        let locator = parse_locator(locators.value(row))?;
        let table = self.catalog.table_at_revision(locator.table_revision)?;
        let path = safe_path(&table.location, &locator.relative_path)?;
        match table.provider {
            TableProviderKind::Images => image::open(path).map_err(|error| {
                VqlError::new(ErrorCode::Execution, "failed to decode model IMAGE input")
                    .with_source(error)
            }),
            TableProviderKind::Videos => decoded_image(
                self.media
                    .decode_frame(&path, locator.pts_ms.unwrap_or_default())?,
            ),
        }
    }

    pub(crate) async fn infer(
        &self,
        model: &ResolvedModelDef,
        invocation: &BoundInferenceParams,
        images: &StructArray,
        fail_on_error: bool,
        cancel: CancellationToken,
    ) -> Result<ArrayRef> {
        let mut decoded = Vec::new();
        let mut positions = Vec::new();
        for row in 0..images.len() {
            if images.is_null(row) {
                continue;
            }
            let decoded_image = if model.source.starts_with("mock://") {
                Ok(image::DynamicImage::new_rgb8(1, 1))
            } else {
                self.decode_image(images, row)
            };
            match decoded_image {
                Ok(image) => {
                    positions.push(row);
                    decoded.push(image);
                }
                Err(error) if fail_on_error => return Err(error),
                Err(_) => {
                    self.inference_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        if decoded.is_empty() {
            return filter_and_scatter_detections(
                &mock_detection_output("object", 0),
                &[],
                images.len(),
                invocation,
            );
        }
        let count = decoded.len();
        let scheduler = self.scheduler(model).await?;
        let started = Instant::now();
        let result = scheduler.infer_with_cancel(decoded, cancel).await;
        if let Ok(mut samples) = self.samples.lock() {
            samples.push((started.elapsed().as_micros() as u64, count as u64));
        }
        match result {
            Ok(results) => {
                let output =
                    filter_and_scatter_detections(&results, &positions, images.len(), invocation)?;
                self.inference_rows
                    .fetch_add(count as u64, Ordering::Relaxed);
                self.inference_batches.fetch_add(1, Ordering::Relaxed);
                Ok(output)
            }
            Err(error) if fail_on_error => Err(error),
            Err(_) => {
                self.inference_errors
                    .fetch_add(count as u64, Ordering::Relaxed);
                filter_and_scatter_detections(
                    &mock_detection_output("object", 0),
                    &[],
                    images.len(),
                    invocation,
                )
            }
        }
    }
}

#[derive(Debug)]
struct ImageDetection(Signature);

impl PartialEq for ImageDetection {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for ImageDetection {}

impl Hash for ImageDetection {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "image_detection".hash(state);
    }
}

impl ScalarUDFImpl for ImageDetection {
    fn name(&self) -> &str {
        "image_detection"
    }

    fn signature(&self) -> &Signature {
        &self.0
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(ModelType::ObjectDetection.canonical_output_type())
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        exec_err!("IMAGE_DETECTION reached execution without InferenceNode extraction")
    }
}

pub(crate) fn image_detection() -> ScalarUDF {
    ScalarUDF::new_from_impl(ImageDetection(
        Signature::one_of(
            vec![
                TypeSignature::Any(2),
                TypeSignature::Any(3),
                TypeSignature::Any(4),
            ],
            Volatility::Volatile,
        )
        .with_parameter_names(vec![
            "model".to_owned(),
            "image".to_owned(),
            "classes".to_owned(),
            "min_confidence".to_owned(),
        ])
        .expect("IMAGE_DETECTION parameter names match its signatures"),
    ))
}

fn safe_path(root: &str, relative: &str) -> Result<PathBuf> {
    let root = Path::new(root).canonicalize()?;
    let path = root.join(relative).canonicalize()?;
    if !path.starts_with(root) {
        return Err(VqlError::new(
            ErrorCode::InvalidLocation,
            "media locator escapes its table location",
        ));
    }
    Ok(path)
}

fn decoded_image(frame: DecodedFrame) -> Result<image::DynamicImage> {
    image::RgbImage::from_raw(frame.width, frame.height, frame.rgb)
        .map(image::DynamicImage::ImageRgb8)
        .ok_or_else(|| VqlError::new(ErrorCode::Execution, "invalid decoded RGB frame"))
}
