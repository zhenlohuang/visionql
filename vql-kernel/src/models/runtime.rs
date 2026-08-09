use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;

use arrow::array::{Array, BinaryArray, StructArray};
use arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use tokio_util::sync::CancellationToken;

use super::backend::{EndpointBackend, MockBackend, ModelBackend};
use super::ort_backend::OrtBackend;
use super::scheduler::ModelScheduler;
use super::{Detection, detections_type, effective_model_params, semantic_fingerprint};
use crate::catalog::{CatalogStore, FunctionDef, ModelDef, ModelParams, TableProviderKind};
use crate::media::{DecodedFrame, MediaRuntime};
use crate::types::{image_storage_fields, parse_locator};
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
    schedulers: Mutex<HashMap<String, Arc<ModelScheduler>>>,
    inference_rows: AtomicU64,
    inference_batches: AtomicU64,
    inference_errors: AtomicU64,
    samples: Mutex<Vec<(u64, u64)>>,
}

impl ModelRuntime {
    pub(crate) fn new(catalog: Arc<CatalogStore>, media: Arc<MediaRuntime>) -> Self {
        Self {
            catalog,
            media,
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

    fn scheduler(&self, model: &ModelDef, params: &ModelParams) -> Result<Arc<ModelScheduler>> {
        let mut schedulers = self.schedulers.lock().map_err(|_| {
            VqlError::new(ErrorCode::Internal, "model scheduler cache was poisoned")
        })?;
        let key = format!(
            "{}:{}",
            model.semantic_fingerprint,
            semantic_fingerprint(params)
        );
        if let Some(scheduler) = schedulers.get(&key) {
            return Ok(Arc::clone(scheduler));
        }
        let backend: Arc<dyn ModelBackend> = if model.source.starts_with("mock://") {
            Arc::new(MockBackend::new(
                params
                    .labels
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "object".to_owned()),
            ))
        } else if model.source.starts_with("endpoint://") {
            Arc::new(EndpointBackend::new(&model.source)?)
        } else {
            Arc::new(OrtBackend::new(
                Path::new(&model.resolved_source),
                params.clone(),
            )?)
        };
        let scheduler = Arc::new(ModelScheduler::new(
            backend,
            16,
            Duration::from_millis(5),
            64,
        ));
        schedulers.insert(key, Arc::clone(&scheduler));
        Ok(scheduler)
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
        let locators = images
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| VqlError::new(ErrorCode::Internal, "IMAGE locator field is invalid"))?;
        if locators.is_null(row) {
            return Err(VqlError::new(
                ErrorCode::Execution,
                "IMAGE has neither encoded bytes nor a locator",
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

    pub(crate) fn infer(
        &self,
        function: &FunctionDef,
        model: &ModelDef,
        images: &StructArray,
        fail_on_error: bool,
        cancel: CancellationToken,
    ) -> Result<Vec<Option<Vec<Detection>>>> {
        let params = effective_model_params(&model.params, &function.bindings)?;
        let mut decoded = Vec::new();
        let mut positions = Vec::new();
        let mut output = vec![None; images.len()];
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
            return Ok(output);
        }
        let count = decoded.len();
        let scheduler = self.scheduler(model, &params)?;
        let started = Instant::now();
        let result = scheduler.infer_with_cancel(decoded, cancel);
        if let Ok(mut samples) = self.samples.lock() {
            samples.push((started.elapsed().as_micros() as u64, count as u64));
        }
        match result {
            Ok(results) => {
                for (position, detections) in positions.into_iter().zip(results) {
                    output[position] = Some(
                        detections
                            .into_iter()
                            .filter(|detection| {
                                detection.confidence >= params.min_confidence
                                    && params
                                        .classes
                                        .as_ref()
                                        .is_none_or(|classes| classes.contains(&detection.label))
                            })
                            .collect(),
                    );
                }
                self.inference_rows
                    .fetch_add(count as u64, Ordering::Relaxed);
                self.inference_batches.fetch_add(1, Ordering::Relaxed);
                Ok(output)
            }
            Err(error) if fail_on_error => Err(error),
            Err(_) => {
                self.inference_errors
                    .fetch_add(count as u64, Ordering::Relaxed);
                Ok(output)
            }
        }
    }
}

#[derive(Debug)]
struct ModelFunction {
    signature: Signature,
    function: FunctionDef,
    model: ModelDef,
}

impl PartialEq for ModelFunction {
    fn eq(&self, other: &Self) -> bool {
        self.function.semantic_fingerprint == other.function.semantic_fingerprint
            && self.model.semantic_fingerprint == other.model.semantic_fingerprint
    }
}

impl Eq for ModelFunction {}

impl Hash for ModelFunction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.function.semantic_fingerprint.hash(state);
        self.model.semantic_fingerprint.hash(state);
    }
}

impl ScalarUDFImpl for ModelFunction {
    fn name(&self) -> &str {
        &self.function.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(detections_type())
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        exec_err!(
            "model function '{}' reached execution without InferenceNode extraction",
            self.function.name
        )
    }
}

pub(crate) fn model_function_udf(function: FunctionDef, model: ModelDef) -> ScalarUDF {
    ScalarUDF::new_from_impl(ModelFunction {
        signature: Signature::one_of(
            vec![TypeSignature::Exact(vec![DataType::Struct(
                image_storage_fields(),
            )])],
            if model.volatile {
                Volatility::Volatile
            } else {
                Volatility::Immutable
            },
        ),
        function,
        model,
    })
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
