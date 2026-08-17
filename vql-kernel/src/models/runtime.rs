use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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
use super::scheduler::{InferenceReservations, ModelScheduler};
use crate::catalog::{
    CatalogStore, ModelType, ResolvedExecutionSpec, ResolvedModelDef, TableProviderKind,
};
use crate::media::{DecodedFrame, MediaRuntime};
use crate::resources::QueryBudget;
use crate::types::parse_locator;
use crate::{ErrorCode, Result, VqlError};

#[derive(Debug)]
pub(crate) struct ModelRuntime {
    catalog: Arc<CatalogStore>,
    media: Arc<MediaRuntime>,
    registry: Arc<PipelineRegistry>,
    schedulers: Mutex<HashMap<String, Arc<ModelScheduler>>>,
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

    fn decode_image(
        &self,
        images: &StructArray,
        row: usize,
        budget: &QueryBudget,
    ) -> Result<(image::DynamicImage, crate::resources::QueryReservation)> {
        let encoded = images
            .column(4)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| VqlError::new(ErrorCode::Internal, "IMAGE encoded field is invalid"))?;
        if !encoded.is_null(row) {
            let bytes = encoded.value(row);
            let (width, height) = image::ImageReader::new(std::io::Cursor::new(bytes))
                .with_guessed_format()
                .map_err(|error| {
                    VqlError::new(ErrorCode::Execution, "model IMAGE format is invalid")
                        .with_source(error)
                })?
                .into_dimensions()
                .map_err(|error| {
                    VqlError::new(ErrorCode::Execution, "model IMAGE dimensions are invalid")
                        .with_source(error)
                })?;
            let reservation = reserve_decoded_image(budget, width, height, 4)?;
            let image = image::load_from_memory(bytes).map_err(|error| {
                VqlError::new(ErrorCode::Execution, "IMAGE encoded bytes are invalid")
                    .with_source(error)
            })?;
            return Ok((image, reservation));
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
            let buffer_id = buffer_ids.value(row);
            let buffer_slot = buffer_slots.value(row);
            let reservation = budget.reserve(
                crate::QueryResource::Media,
                self.media.buffered_frame_bytes(buffer_id, buffer_slot)?,
            )?;
            let image = decoded_image(self.media.resolve_buffered_frame(buffer_id, buffer_slot)?)?;
            return Ok((image, reservation));
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
            TableProviderKind::Images => {
                let (width, height) = image::image_dimensions(&path).map_err(|error| {
                    VqlError::new(ErrorCode::Execution, "failed to inspect model IMAGE input")
                        .with_source(error)
                })?;
                let reservation = reserve_decoded_image(budget, width, height, 4)?;
                let image = image::open(path).map_err(|error| {
                    VqlError::new(ErrorCode::Execution, "failed to decode model IMAGE input")
                        .with_source(error)
                })?;
                Ok((image, reservation))
            }
            TableProviderKind::Videos => {
                let metadata = self.media.probe(&path)?;
                let width = u32::try_from(metadata.width).map_err(|_| {
                    VqlError::new(ErrorCode::Execution, "video width must be non-negative")
                })?;
                let height = u32::try_from(metadata.height).map_err(|_| {
                    VqlError::new(ErrorCode::Execution, "video height must be non-negative")
                })?;
                let reservation = reserve_decoded_image(budget, width, height, 3)?;
                let image = decoded_image(
                    self.media
                        .decode_frame(&path, locator.pts_ms.unwrap_or_default())?,
                )?;
                Ok((image, reservation))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn infer(
        &self,
        model: &ResolvedModelDef,
        invocation: &BoundInferenceParams,
        images: &StructArray,
        fail_on_error: bool,
        cancel: CancellationToken,
        budget: &QueryBudget,
        metrics: Arc<crate::session::QueryMetrics>,
    ) -> Result<ArrayRef> {
        let mut decoded = Vec::new();
        let mut positions = Vec::new();
        let mut reservations = Vec::new();
        let buffer_ids = images
            .column(8)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| {
                VqlError::new(ErrorCode::Internal, "IMAGE buffer_id field is invalid")
            })?;
        for row in 0..images.len() {
            if images.is_null(row) {
                continue;
            }
            let decoded_image = if model.source.starts_with("mock://") {
                budget
                    .reserve(crate::QueryResource::Media, 3)
                    .map(|reservation| (image::DynamicImage::new_rgb8(1, 1), reservation))
            } else {
                self.decode_image(images, row, budget)
            };
            match decoded_image {
                Ok((image, reservation)) => {
                    if !model.source.starts_with("mock://") && buffer_ids.is_null(row) {
                        metrics.add_decode_frame();
                    }
                    positions.push(row);
                    decoded.push(image);
                    reservations.push(reservation);
                }
                Err(error) if fail_on_error || error.code == ErrorCode::ResourceExhausted => {
                    return Err(error);
                }
                Err(_) => {
                    metrics.add_error_rows(1);
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
        let queue_bytes = decoded
            .capacity()
            .saturating_mul(std::mem::size_of::<image::DynamicImage>());
        let queue_reservation = budget.reserve(crate::QueryResource::ModelQueue, queue_bytes)?;
        reservations.push(queue_reservation);
        let reservations = InferenceReservations::new(reservations);
        let scheduler = self.scheduler(model).await?;
        let result = scheduler
            .infer_with_metrics(
                decoded,
                cancel,
                budget.clone(),
                Arc::clone(&metrics),
                reservations,
            )
            .await;
        match result {
            Ok(results) => {
                let output =
                    filter_and_scatter_detections(&results, &positions, images.len(), invocation)?;
                Ok(output)
            }
            Err(error) if fail_on_error || error.code == ErrorCode::ResourceExhausted => Err(error),
            Err(_) => {
                metrics.add_error_rows(count);
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

fn reserve_decoded_image(
    budget: &QueryBudget,
    width: u32,
    height: u32,
    bytes_per_pixel: usize,
) -> Result<crate::resources::QueryReservation> {
    let bytes = (width as usize)
        .checked_mul(height as usize)
        .and_then(|value| value.checked_mul(bytes_per_pixel))
        .ok_or_else(|| {
            VqlError::new(
                ErrorCode::ResourceExhausted,
                "decoded model IMAGE size exceeds platform limits",
            )
        })?;
    budget.reserve(crate::QueryResource::Media, bytes)
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
