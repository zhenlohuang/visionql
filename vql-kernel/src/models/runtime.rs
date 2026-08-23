use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, StructArray, UInt32Array, new_null_array,
};
use arrow::compute::kernels::zip::zip;
use arrow::compute::take;
use arrow::datatypes::DataType;
use arrow::datatypes::FieldRef;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use tokio_util::sync::CancellationToken;

use super::BoundInferenceParams;
use super::backend::{MockBackend, ModelBackend};
use super::ort_backend::GenericOrtRuntime;
use super::pipeline::BatchingOwner;
use super::postprocess::{
    empty_classification_output, filter_and_scatter_classifications, filter_and_scatter_detections,
    mock_detection_output, mock_primary_label,
};
use super::preprocess::ImageTensorFactory;
use super::registry::PipelineRegistry;
use super::registry::PreProcessorFactory;
use super::scheduler::{InferenceReservations, ModelScheduler};
use crate::catalog::{
    CatalogStore, ModelDef, ModelInterface, ModelType, ModelVersion, ResolvedExecutionSpec,
    ResolvedModelDef, ResolvedModelSpec, TableProvider,
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
    generic_sessions: Mutex<HashMap<String, Arc<GenericOrtRuntime>>>,
    builtin_fingerprints: Mutex<HashSet<String>>,
    resolution_jobs: Mutex<HashMap<String, Arc<ResolutionJob>>>,
}

#[derive(Debug, Clone)]
struct SharedResolutionError {
    code: ErrorCode,
    message: String,
    target_version: Option<String>,
}

impl From<VqlError> for SharedResolutionError {
    fn from(error: VqlError) -> Self {
        Self {
            code: error.code,
            message: error.message,
            target_version: error.target_version,
        }
    }
}

impl SharedResolutionError {
    fn into_error(self) -> VqlError {
        let mut error = VqlError::new(self.code, self.message);
        error.target_version = self.target_version;
        error
    }
}

#[derive(Debug)]
struct ResolutionJob {
    waiters: AtomicUsize,
    cancel: CancellationToken,
    result: Mutex<Option<std::result::Result<ResolvedModelSpec, SharedResolutionError>>>,
    notify: tokio::sync::Notify,
}

impl ResolutionJob {
    fn new() -> Self {
        Self {
            waiters: AtomicUsize::new(0),
            cancel: CancellationToken::new(),
            result: Mutex::new(None),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn complete(&self, result: Result<ResolvedModelSpec>) {
        if let Ok(mut slot) = self.result.lock() {
            *slot = Some(result.map_err(SharedResolutionError::from));
        }
        self.notify.notify_waiters();
    }

    fn outcome(
        &self,
    ) -> Result<Option<std::result::Result<ResolvedModelSpec, SharedResolutionError>>> {
        self.result
            .lock()
            .map(|result| result.clone())
            .map_err(|_| {
                VqlError::new(
                    ErrorCode::Internal,
                    "model resolution job result was poisoned",
                )
            })
    }

    fn is_complete(&self) -> Result<bool> {
        self.result
            .lock()
            .map(|result| result.is_some())
            .map_err(|_| {
                VqlError::new(
                    ErrorCode::Internal,
                    "model resolution job result was poisoned",
                )
            })
    }

    async fn wait(&self, waiter_cancel: CancellationToken) -> Result<ResolvedModelSpec> {
        self.waiters.fetch_add(1, Ordering::AcqRel);
        let result = loop {
            let notified = self.notify.notified();
            if let Some(result) = self.outcome()? {
                break result.map_err(SharedResolutionError::into_error);
            }
            tokio::select! {
                _ = waiter_cancel.cancelled() => {
                    break Err(VqlError::new(
                        ErrorCode::QueryCancelled,
                        "model resolve cancelled",
                    ));
                }
                _ = notified => {}
            }
        };
        let previous = self.waiters.fetch_sub(1, Ordering::AcqRel);
        if previous == 1 && self.result.lock().is_ok_and(|result| result.is_none()) {
            self.cancel.cancel();
        }
        result
    }
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
            generic_sessions: Mutex::new(HashMap::new()),
            builtin_fingerprints: Mutex::new(HashSet::new()),
            resolution_jobs: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn register_builtin(&self, model: &ResolvedModelDef) -> Result<()> {
        self.builtin_fingerprints
            .lock()
            .map_err(|_| {
                VqlError::new(
                    ErrorCode::Internal,
                    "built-in model fingerprint cache was poisoned",
                )
            })?
            .insert(model.semantic_fingerprint.clone());
        Ok(())
    }

    pub(crate) async fn resolve_version(
        &self,
        interface: &ModelInterface,
        version: &ModelVersion,
        cache_dir: &Path,
        waiter_cancel: CancellationToken,
    ) -> Result<ResolvedModelSpec> {
        let (job, created) = {
            let mut jobs = self.resolution_jobs.lock().map_err(|_| {
                VqlError::new(
                    ErrorCode::Internal,
                    "model resolution job cache was poisoned",
                )
            })?;
            if let Some(job) = jobs.get(&version.declaration_fingerprint)
                && !job.is_complete()?
            {
                (Arc::clone(job), false)
            } else {
                let replacement = Arc::new(ResolutionJob::new());
                jobs.insert(
                    version.declaration_fingerprint.clone(),
                    Arc::clone(&replacement),
                );
                (replacement, true)
            }
        };
        if created {
            let registry = Arc::clone(&self.registry);
            let interface = interface.clone();
            let version = version.clone();
            let cache_dir = cache_dir.to_path_buf();
            let job_for_task = Arc::clone(&job);
            tokio::spawn(async move {
                let result = registry
                    .resolve_version(
                        &interface,
                        &version,
                        &cache_dir,
                        job_for_task.cancel.clone(),
                    )
                    .await;
                job_for_task.complete(result);
            });
        }
        let result = job.wait(waiter_cancel).await;
        let remove = job.waiters.load(Ordering::Acquire) == 0
            && (job.is_complete()? || job.cancel.is_cancelled());
        if remove {
            let mut jobs = self.resolution_jobs.lock().map_err(|_| {
                VqlError::new(
                    ErrorCode::Internal,
                    "model resolution job cache was poisoned",
                )
            })?;
            if jobs
                .get(&version.declaration_fingerprint)
                .is_some_and(|current| Arc::ptr_eq(current, &job))
            {
                jobs.remove(&version.declaration_fingerprint);
            }
        }
        result
    }

    pub(crate) fn evict_stale(&self) -> Result<()> {
        let live_fingerprints = self.live_model_fingerprints()?;
        self.schedulers
            .lock()
            .map_err(|_| VqlError::new(ErrorCode::Internal, "model scheduler cache was poisoned"))?
            .retain(|fingerprint, _| live_fingerprints.contains(fingerprint));
        self.generic_sessions
            .lock()
            .map_err(|_| {
                VqlError::new(
                    ErrorCode::Internal,
                    "generic model session cache was poisoned",
                )
            })?
            .retain(|fingerprint, _| live_fingerprints.contains(fingerprint));
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn cached_pipeline_count(&self) -> usize {
        self.schedulers.lock().map_or(0, |cache| cache.len())
    }

    #[cfg(test)]
    pub(crate) fn active_resolution_job_count(&self) -> usize {
        self.resolution_jobs.lock().map_or(0, |jobs| jobs.len())
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

    async fn generic_session(&self, model: &ResolvedModelDef) -> Result<Arc<GenericOrtRuntime>> {
        let live_fingerprints = self.live_model_fingerprints()?;
        let cacheable = live_fingerprints.contains(&model.semantic_fingerprint);
        let key = model.semantic_fingerprint.clone();
        {
            let mut sessions = self.generic_sessions.lock().map_err(|_| {
                VqlError::new(
                    ErrorCode::Internal,
                    "generic model session cache was poisoned",
                )
            })?;
            sessions.retain(|fingerprint, _| live_fingerprints.contains(fingerprint));
            if let Some(session) = sessions.get(&key) {
                return Ok(Arc::clone(session));
            }
        }
        let model = model.clone();
        let session = Arc::new(
            tokio::task::spawn_blocking(move || GenericOrtRuntime::new(&model))
                .await
                .map_err(|error| {
                    VqlError::new(
                        ErrorCode::Execution,
                        "generic ONNX Runtime build task failed",
                    )
                    .with_source(error)
                })??,
        );
        if cacheable {
            let mut sessions = self.generic_sessions.lock().map_err(|_| {
                VqlError::new(
                    ErrorCode::Internal,
                    "generic model session cache was poisoned",
                )
            })?;
            if let Some(existing) = sessions.get(&key) {
                return Ok(Arc::clone(existing));
            }
            sessions.insert(key, Arc::clone(&session));
        }
        Ok(session)
    }

    fn live_model_fingerprints(&self) -> Result<HashSet<String>> {
        let mut fingerprints = self
            .catalog
            .snapshot()?
            .models()
            .flat_map(|(_, model)| model.definition.versions.iter())
            .filter_map(|version| {
                version
                    .resolved
                    .as_ref()
                    .map(|resolved| resolved.semantic_fingerprint.clone())
            })
            .collect::<HashSet<_>>();
        fingerprints.extend(
            self.builtin_fingerprints
                .lock()
                .map_err(|_| {
                    VqlError::new(
                        ErrorCode::Internal,
                        "built-in model fingerprint cache was poisoned",
                    )
                })?
                .iter()
                .cloned(),
        );
        Ok(fingerprints)
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
        let table = self.catalog.table_at_generation(locator.table_generation)?;
        match &table.provider {
            TableProvider::Images { location, .. } => {
                let path = safe_path(location, &locator.relative_path)?;
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
            TableProvider::Videos { location, .. } => {
                let path = safe_path(location, &locator.relative_path)?;
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
            TableProvider::Rtsp(_) | TableProvider::Kafka(_) | TableProvider::External { .. } => {
                Err(VqlError::new(
                    ErrorCode::Catalog,
                    "model IMAGE locator references a table provider without local media",
                ))
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
    ) -> Result<ArrayRef> {
        let model_type = model.interface.capability.ok_or_else(|| {
            VqlError::new(
                ErrorCode::Internal,
                "capability inference requires a capability Model",
            )
        })?;
        let mut decoded = Vec::new();
        let mut positions = Vec::new();
        let mut reservations = Vec::new();
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
                    positions.push(row);
                    decoded.push(image);
                    reservations.push(reservation);
                }
                Err(error) if fail_on_error || error.code == ErrorCode::ResourceExhausted => {
                    return Err(error);
                }
                Err(_) => {}
            }
        }
        if decoded.is_empty() {
            return filter_and_scatter_capability(
                model_type,
                &empty_capability_output(model_type),
                &[],
                images.len(),
                invocation,
            );
        }
        let queue_bytes = decoded
            .capacity()
            .saturating_mul(std::mem::size_of::<image::DynamicImage>());
        let queue_reservation = budget.reserve(crate::QueryResource::ModelQueue, queue_bytes)?;
        reservations.push(queue_reservation);
        let reservations = InferenceReservations::new(reservations);
        let scheduler = self.scheduler(model).await?;
        let result = scheduler
            .infer(decoded, cancel, budget.clone(), reservations)
            .await;
        match result {
            Ok(results) => filter_and_scatter_capability(
                model_type,
                &results,
                &positions,
                images.len(),
                invocation,
            ),
            Err(error) if fail_on_error || error.code == ErrorCode::ResourceExhausted => Err(error),
            Err(_) => filter_and_scatter_capability(
                model_type,
                &empty_capability_output(model_type),
                &[],
                images.len(),
                invocation,
            ),
        }
    }

    pub(crate) async fn infer_generic(
        &self,
        model: &ResolvedModelDef,
        inputs: &[ArrayRef],
        rows: usize,
        fail_on_error: bool,
        cancel: CancellationToken,
        budget: &QueryBudget,
    ) -> Result<ArrayRef> {
        let ResolvedExecutionSpec::Generic {
            inputs: input_specs,
            outputs: output_specs,
            ..
        } = &model.execution
        else {
            return Err(VqlError::new(
                ErrorCode::Internal,
                "generic Model has a non-generic resolved execution contract",
            ));
        };
        if !model.source.starts_with("mock://") {
            if inputs.len() != input_specs.len() {
                return Err(VqlError::new(
                    ErrorCode::Internal,
                    "generic Model input count differs from its resolved contract",
                ));
            }
        } else {
            let output_type = crate::models::interface_output_field("", &model.interface, true)?
                .data_type()
                .clone();
            let nulls = new_null_array(&output_type, rows);
            let Some(first) = inputs
                .first()
                .filter(|value| value.data_type() == &output_type)
            else {
                return Ok(nulls);
            };
            let valid = BooleanArray::from(
                (0..rows)
                    .map(|row| inputs.iter().all(|input| input.is_valid(row)))
                    .collect::<Vec<_>>(),
            );
            return zip(&valid, first, &nulls).map_err(|error| {
                VqlError::new(
                    ErrorCode::Execution,
                    "failed to align generic Model NULL rows",
                )
                .with_source(error)
            });
        }

        let mut valid = (0..rows)
            .map(|row| inputs.iter().all(|input| input.is_valid(row)))
            .collect::<Vec<_>>();
        let mut decoded_inputs = (0..inputs.len())
            .map(|_| None)
            .collect::<Vec<Option<Vec<Option<image::DynamicImage>>>>>();
        let mut reservations = Vec::new();
        for (index, parameter) in model.interface.parameters.iter().enumerate() {
            if !parameter.data_type.eq_ignore_ascii_case("IMAGE") {
                continue;
            }
            let images = inputs[index]
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    VqlError::new(
                        ErrorCode::Execution,
                        format!("generic Model parameter '{}' expects IMAGE", parameter.name),
                    )
                })?;
            let mut decoded = (0..rows).map(|_| None).collect::<Vec<_>>();
            for row in 0..rows {
                if !valid[row] {
                    continue;
                }
                if cancel.is_cancelled() {
                    return Err(VqlError::new(ErrorCode::QueryCancelled, "query cancelled"));
                }
                match self.decode_image(images, row, budget) {
                    Ok((image, reservation)) => {
                        decoded[row] = Some(image);
                        reservations.push(reservation);
                    }
                    Err(error) if fail_on_error || error.code == ErrorCode::ResourceExhausted => {
                        return Err(error);
                    }
                    Err(_) => valid[row] = false,
                }
            }
            decoded_inputs[index] = Some(decoded);
        }

        let positions = valid
            .iter()
            .enumerate()
            .filter_map(|(row, valid)| (*valid).then_some(row))
            .collect::<Vec<_>>();
        let output_type = crate::models::interface_output_field("", &model.interface, true)?
            .data_type()
            .clone();
        if positions.is_empty() {
            return Ok(new_null_array(&output_type, rows));
        }
        let take_indices = UInt32Array::from(
            positions
                .iter()
                .map(|row| u32::try_from(*row).expect("Arrow row index fits u32"))
                .collect::<Vec<_>>(),
        );
        let mut runtime_inputs = Vec::with_capacity(inputs.len());
        for (index, (array, spec)) in inputs.iter().zip(input_specs).enumerate() {
            let compact = if let Some(decoded) = decoded_inputs[index].as_mut() {
                let processor = spec.processor.as_ref().ok_or_else(|| {
                    VqlError::new(
                        ErrorCode::Internal,
                        format!("generic IMAGE input '{}' has no processor", spec.name),
                    )
                })?;
                let images = positions
                    .iter()
                    .map(|row| {
                        decoded[*row].take().ok_or_else(|| {
                            VqlError::new(
                                ErrorCode::Internal,
                                "generic IMAGE compaction lost a decoded row",
                            )
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let processor = ImageTensorFactory.build(processor)?;
                let output_bytes = processor.output_bytes(images.len())?;
                reservations.push(budget.reserve(crate::QueryResource::ModelTensor, output_bytes)?);
                processor.process(&images)?.input.into_array()
            } else {
                take(array.as_ref(), &take_indices, None).map_err(|error| {
                    VqlError::new(
                        ErrorCode::Execution,
                        format!("failed to compact generic Model input '{}'", spec.name),
                    )
                    .with_source(error)
                })?
            };
            runtime_inputs.push((spec.clone(), compact));
        }

        let session = self.generic_session(model).await?;
        let outputs = output_specs.clone();
        let task = tokio::task::spawn_blocking(move || session.run(&runtime_inputs, &outputs));
        let compact_outputs = tokio::select! {
            _ = cancel.cancelled() => {
                return Err(VqlError::new(ErrorCode::QueryCancelled, "query cancelled"));
            }
            result = task => result.map_err(|error| {
                VqlError::new(ErrorCode::Execution, "generic ONNX inference task failed")
                    .with_source(error)
            })??,
        };
        let compact: ArrayRef = match &output_type {
            DataType::Struct(fields) => {
                Arc::new(StructArray::new(fields.clone(), compact_outputs, None))
            }
            _ => compact_outputs.into_iter().next().ok_or_else(|| {
                VqlError::new(ErrorCode::Internal, "generic ONNX returned no outputs")
            })?,
        };
        let mut next = 0_u32;
        let scatter_indices = UInt32Array::from(
            valid
                .into_iter()
                .map(|valid| {
                    valid.then(|| {
                        let index = next;
                        next += 1;
                        index
                    })
                })
                .collect::<Vec<_>>(),
        );
        take(compact.as_ref(), &scatter_indices, None).map_err(|error| {
            VqlError::new(
                ErrorCode::Execution,
                "failed to restore generic Model NULL row alignment",
            )
            .with_source(error)
        })
    }
}

fn empty_capability_output(model_type: ModelType) -> ArrayRef {
    match model_type {
        ModelType::ObjectDetection => mock_detection_output("object", 0),
        ModelType::ImageClassification => empty_classification_output(),
    }
}

fn filter_and_scatter_capability(
    model_type: ModelType,
    input: &ArrayRef,
    positions: &[usize],
    total_rows: usize,
    invocation: &BoundInferenceParams,
) -> Result<ArrayRef> {
    match model_type {
        ModelType::ObjectDetection => {
            filter_and_scatter_detections(input, positions, total_rows, invocation)
        }
        ModelType::ImageClassification => {
            filter_and_scatter_classifications(input, positions, total_rows, invocation)
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

struct ModelMarker {
    name: String,
    signature: Signature,
    return_type: DataType,
    return_field: FieldRef,
}

impl std::fmt::Debug for ModelMarker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ModelMarker")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl PartialEq for ModelMarker {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for ModelMarker {}

impl Hash for ModelMarker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for ModelMarker {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn return_field_from_args(
        &self,
        _args: ReturnFieldArgs,
    ) -> datafusion::common::Result<FieldRef> {
        Ok(Arc::clone(&self.return_field))
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        exec_err!(
            "model '{}' reached scalar execution without InferenceNode extraction",
            self.name
        )
    }
}

pub(crate) fn model_marker(model: &ModelDef) -> crate::Result<ScalarUDF> {
    let argument_count =
        model.interface.parameters.len() + model.interface.semantic_arguments.len() + 1;
    let mut parameter_names = model
        .interface
        .arguments()
        .map(|parameter| parameter.name.clone())
        .collect::<Vec<_>>();
    parameter_names.push("version".to_owned());
    let signature = Signature::one_of(
        vec![TypeSignature::Any(argument_count)],
        Volatility::Volatile,
    )
    .with_parameter_names(parameter_names)
    .map_err(|error| {
        VqlError::new(
            ErrorCode::Internal,
            "invalid persisted Model marker signature",
        )
        .with_source(error)
    })?;
    let return_field = crate::models::interface_output_field("", &model.interface, true)?;
    Ok(ScalarUDF::new_from_impl(ModelMarker {
        name: model.name.clone(),
        signature,
        return_type: return_field.data_type().clone(),
        return_field,
    }))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shared_resolution_cancels_only_after_the_last_waiter_leaves() {
        let job = Arc::new(ResolutionJob::new());
        let first_cancel = CancellationToken::new();
        let second_cancel = CancellationToken::new();
        let first_job = Arc::clone(&job);
        let first_token = first_cancel.clone();
        let first = tokio::spawn(async move { first_job.wait(first_token).await });
        let second_job = Arc::clone(&job);
        let second_token = second_cancel.clone();
        let second = tokio::spawn(async move { second_job.wait(second_token).await });
        while job.waiters.load(Ordering::Acquire) != 2 {
            tokio::task::yield_now().await;
        }

        first_cancel.cancel();
        assert_eq!(
            first.await.unwrap().unwrap_err().code,
            ErrorCode::QueryCancelled
        );
        assert!(!job.cancel.is_cancelled());

        second_cancel.cancel();
        assert_eq!(
            second.await.unwrap().unwrap_err().code,
            ErrorCode::QueryCancelled
        );
        assert!(job.cancel.is_cancelled());
    }
}
