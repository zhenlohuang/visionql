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

use super::backend::{MockBackend, ModelBackend};
use super::ort_backend::OrtRuntime;
use super::pipeline::{
    BatchingOwner, CompiledPipeline, ImageTensorPreProcessor, RuntimeSession, YoloPostProcessor,
};
use super::scheduler::ModelScheduler;
use super::triton_backend::TritonRuntime;
use super::{BoundInferenceParams, Detection, ModelParams, compile_model_params, detections_type};
use crate::catalog::{CatalogStore, ModelDef, TableProviderKind};
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
        let key = model.semantic_fingerprint.clone();
        if let Some(scheduler) = schedulers.get(&key) {
            return Ok(Arc::clone(scheduler));
        }
        let (backend, batching_owner): (Arc<dyn ModelBackend>, BatchingOwner) =
            if model.source.starts_with("mock://") {
                (
                    Arc::new(MockBackend::new(
                        params
                            .labels
                            .first()
                            .cloned()
                            .unwrap_or_else(|| "object".to_owned()),
                    )),
                    BatchingOwner::VisionQl,
                )
            } else {
                let runtime: Arc<dyn RuntimeSession> = if model.runtime.kind == "triton" {
                    let model_name = model.runtime.options["model_name"]
                        .as_str()
                        .expect("validated Triton model name");
                    let model_version = model
                        .runtime
                        .options
                        .get("model_version")
                        .and_then(serde_json::Value::as_str);
                    Arc::new(TritonRuntime::new(
                        &model.source,
                        model_name,
                        model_version,
                    )?)
                } else if model.runtime.kind == "onnxruntime" {
                    Arc::new(OrtRuntime::new(Path::new(&model.resolved_source))?)
                } else {
                    return Err(VqlError::new(
                        ErrorCode::InvalidOption,
                        format!("unsupported Runtime '{}'", model.runtime.kind),
                    ));
                };
                let batching_owner = runtime.batching_owner();
                let pipeline = CompiledPipeline::new(
                    Arc::new(ImageTensorPreProcessor::new(params.clone())),
                    runtime,
                    Arc::new(YoloPostProcessor::new(params.clone())),
                );
                (Arc::new(pipeline), batching_owner)
            };
        let scheduler = Arc::new(ModelScheduler::new(
            backend,
            16,
            match batching_owner {
                BatchingOwner::VisionQl => Duration::from_millis(5),
                BatchingOwner::Service => Duration::ZERO,
            },
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
        model: &ModelDef,
        invocation: &BoundInferenceParams,
        images: &StructArray,
        fail_on_error: bool,
        cancel: CancellationToken,
    ) -> Result<Vec<Option<Vec<Detection>>>> {
        let params = compile_model_params(model)?;
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
                                detection.confidence >= invocation.min_confidence
                                    && invocation
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
struct DetectObjects(Signature);

impl PartialEq for DetectObjects {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for DetectObjects {}

impl Hash for DetectObjects {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "detect_objects".hash(state);
    }
}

impl ScalarUDFImpl for DetectObjects {
    fn name(&self) -> &str {
        "detect_objects"
    }

    fn signature(&self) -> &Signature {
        &self.0
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(detections_type())
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        exec_err!("DETECT_OBJECTS reached execution without InferenceNode extraction")
    }
}

pub(crate) fn detect_objects_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(DetectObjects(
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
        .expect("DETECT_OBJECTS parameter names match its signatures"),
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
