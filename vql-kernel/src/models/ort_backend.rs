use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType;
use async_trait::async_trait;
use ort::session::Session;
use ort::value::{Outlet, TensorElementType, TensorRef, ValueType};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use super::pipeline::{
    BatchingOwner, RuntimeRequestBatch, RuntimeResponseBatch, RuntimeSession, TensorBatch,
    TensorContract,
};
use super::postprocess::YoloPostProcessorFactory;
use super::preprocess::ImageTensorFactory;
use super::registry::{
    PostProcessorFactory, PreProcessorFactory, RuntimeFactory, RuntimeResolution,
    deserialize_model_options, invalid_option,
};
use super::resolver::{resolve_onnx_source, validate_onnx_source};
use crate::catalog::{
    ModelDef, ModelType, ProcessorSpec, ResolvedExecutionSpec, ResolvedModelDef, RuntimeSpec,
};
use crate::{ErrorCode, Result, VqlError};

const SUPPORTED_TYPES: &[ModelType] = &[ModelType::ObjectDetection];

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct OrtOptions {
    #[serde(default)]
    sha256: Option<String>,
    #[serde(default)]
    input: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    output: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug)]
pub(super) struct OrtRuntimeFactory;

#[async_trait]
impl RuntimeFactory for OrtRuntimeFactory {
    fn kind(&self) -> &str {
        "onnx-runtime"
    }

    fn supported_types(&self) -> &[ModelType] {
        SUPPORTED_TYPES
    }

    fn validate_declaration(&self, model: &ModelDef) -> Result<()> {
        let options = parse_options(&model.options)?;
        validate_onnx_source(&model.source, options.sha256.as_deref())?;
        ImageTensorFactory
            .validate(&options.pre_processor)
            .map_err(remap_onnx_option_error)?;
        match options.post_processor.kind.as_str() {
            "vision.yolo_e2e@1" => {
                YoloPostProcessorFactory::end_to_end().validate(&options.post_processor)
            }
            "vision.yolo_raw@1" => {
                YoloPostProcessorFactory::raw().validate(&options.post_processor)
            }
            "vision.xywh_normalized@1" => {
                YoloPostProcessorFactory::xywh_normalized().validate(&options.post_processor)
            }
            _ => Err(VqlError::new(
                ErrorCode::Internal,
                "ONNX Runtime selected an unknown PostProcessor",
            )),
        }
        .map_err(remap_onnx_option_error)
    }

    async fn resolve(
        &self,
        model: &ModelDef,
        cache_dir: &Path,
        cancel: CancellationToken,
    ) -> Result<RuntimeResolution> {
        let options = parse_options(&model.options)?;
        let source = model.source.clone();
        let cache_dir = cache_dir.to_path_buf();
        let expected_sha256 = options.sha256.clone();
        let resolved = tokio::task::spawn_blocking(move || {
            resolve_onnx_source(&source, &cache_dir, expected_sha256.as_deref(), &cancel)
        })
        .await
        .map_err(|error| {
            VqlError::new(ErrorCode::Execution, "ONNX model resolve task failed").with_source(error)
        })??;
        Ok(RuntimeResolution {
            resolved_source: resolved.resolved_source,
            artifact_hash: resolved.artifact_hash,
            execution: ResolvedExecutionSpec::Embedded {
                runtime: RuntimeSpec {
                    kind: self.kind().to_owned(),
                    protocol: None,
                    options: BTreeMap::new(),
                },
                pre_processor: options.pre_processor,
                post_processor: options.post_processor,
            },
            volatile: false,
        })
    }

    fn build_embedded(
        &self,
        model: &ResolvedModelDef,
        _runtime: &RuntimeSpec,
        input: &TensorContract,
        output: &TensorContract,
    ) -> Result<Arc<dyn RuntimeSession>> {
        Ok(Arc::new(OrtRuntime::new(
            Path::new(&model.resolved_source),
            input.clone(),
            output.clone(),
        )?))
    }
}

struct ResolvedOrtOptions {
    sha256: Option<String>,
    pre_processor: ProcessorSpec,
    post_processor: ProcessorSpec,
}

fn parse_options(options: &BTreeMap<String, serde_json::Value>) -> Result<ResolvedOrtOptions> {
    let options: OrtOptions = deserialize_model_options("ONNX_RUNTIME", options)?;
    let mut input = options.input;
    rename_option(&mut input, "name", "input_name")?;
    let mut output = options.output;
    rename_option(&mut output, "name", "output_name")?;
    let format = output
        .remove("format")
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.is_empty())
                .map(|value| value.to_ascii_lowercase())
                .ok_or_else(|| {
                    VqlError::new(
                        ErrorCode::InvalidOption,
                        "invalid Model option 'WITH.output.format': must be a non-empty string",
                    )
                })
        })
        .transpose()?
        .unwrap_or_else(|| "yolo_e2e".to_owned());
    let post_processor_kind = match format.as_str() {
        "yolo_e2e" => "vision.yolo_e2e@1",
        "yolo_raw" => "vision.yolo_raw@1",
        "xywh_normalized" => "vision.xywh_normalized@1",
        _ => {
            return invalid_option(
                "WITH.output.format",
                "must be 'yolo_e2e', 'yolo_raw', or 'xywh_normalized'",
            );
        }
    };
    Ok(ResolvedOrtOptions {
        sha256: options.sha256,
        pre_processor: ProcessorSpec {
            kind: "vision.image_tensor@1".to_owned(),
            options: input,
        },
        post_processor: ProcessorSpec {
            kind: post_processor_kind.to_owned(),
            options: output,
        },
    })
}

fn rename_option(
    options: &mut BTreeMap<String, serde_json::Value>,
    from: &str,
    to: &str,
) -> Result<()> {
    if let Some(value) = options.remove(from)
        && options.insert(to.to_owned(), value).is_some()
    {
        return invalid_option(format!("WITH.{from}"), format!("conflicts with '{to}'"));
    }
    Ok(())
}

fn remap_onnx_option_error(mut error: VqlError) -> VqlError {
    if error.code == ErrorCode::InvalidOption {
        error.message = error
            .message
            .replace("pre_processor.options", "WITH.input")
            .replace("post_processor.options", "WITH.output");
    }
    error
}

pub(super) struct OrtRuntime {
    session: Arc<Mutex<Session>>,
    input_contract: TensorContract,
    output_contract: TensorContract,
}

impl std::fmt::Debug for OrtRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OrtRuntime")
            .field("input_contract", &self.input_contract)
            .field("output_contract", &self.output_contract)
            .finish_non_exhaustive()
    }
}

impl OrtRuntime {
    fn new(
        path: &Path,
        input_contract: TensorContract,
        output_contract: TensorContract,
    ) -> Result<Self> {
        // CoreML's NeuralNetwork format binds a dynamic batch dimension to 1, so any batched
        // call fails at predict time. Restricting the EP to static shapes keeps CoreML for
        // fixed-shape models — the only case where it also outperforms the CPU provider — and
        // falls back to CPU for the dynamic-batch exports VisionQL requires.
        #[cfg(target_os = "macos")]
        let coreml = Session::builder()
            .ok()
            .and_then(|builder| {
                builder
                    .with_execution_providers([ort::ep::CoreML::default()
                        .with_static_input_shapes(true)
                        .build()])
                    .ok()
            })
            .and_then(|mut builder| builder.commit_from_file(path).ok());
        #[cfg(not(target_os = "macos"))]
        let coreml: Option<Session> = None;

        let session = match coreml {
            Some(session) => session,
            None => Session::builder()
                .and_then(|mut builder| builder.commit_from_file(path))
                .map_err(|error| {
                    VqlError::new(
                        ErrorCode::Execution,
                        format!("failed to load ONNX model '{}': {error}", path.display()),
                    )
                })?,
        };
        validate_outlet("input", session.inputs(), &input_contract)?;
        validate_outlet("output", session.outputs(), &output_contract)?;
        Ok(Self {
            session: Arc::new(Mutex::new(session)),
            input_contract,
            output_contract,
        })
    }
}

fn validate_outlet(role: &str, outlets: &[Outlet], contract: &TensorContract) -> Result<()> {
    let outlet = outlets
        .iter()
        .find(|outlet| outlet.name() == contract.name)
        .ok_or_else(|| {
            VqlError::new(
                ErrorCode::Execution,
                format!(
                    "ONNX model does not declare {role} tensor '{}'",
                    contract.name
                ),
            )
        })?;
    let ValueType::Tensor { ty, shape, .. } = outlet.dtype() else {
        return Err(VqlError::new(
            ErrorCode::Execution,
            format!("ONNX {role} tensor '{}' is not a tensor", contract.name),
        ));
    };
    let expected_type = match &contract.dtype {
        DataType::Float32 => TensorElementType::Float32,
        unsupported => {
            return Err(VqlError::new(
                ErrorCode::Execution,
                format!(
                    "ONNX {role} tensor '{}' uses unsupported dtype {unsupported}",
                    contract.name
                ),
            ));
        }
    };
    if ty != &expected_type {
        return Err(VqlError::new(
            ErrorCode::Execution,
            format!(
                "ONNX {role} tensor '{}' has dtype {ty:?}, expected {}",
                contract.name, contract.dtype
            ),
        ));
    }
    if shape.len() != contract.shape.len()
        || shape
            .iter()
            .zip(&contract.shape)
            .any(|(declared, expected)| *declared >= 0 && *expected >= 0 && declared != expected)
    {
        return Err(VqlError::new(
            ErrorCode::Execution,
            format!(
                "ONNX {role} tensor '{}' shape {:?} is incompatible with contract {:?}",
                contract.name, shape, contract.shape
            ),
        ));
    }
    Ok(())
}

#[async_trait]
impl RuntimeSession for OrtRuntime {
    fn kind(&self) -> &str {
        "onnx-runtime"
    }

    fn input_contract(&self) -> &TensorContract {
        &self.input_contract
    }

    fn output_contract(&self) -> &TensorContract {
        &self.output_contract
    }

    fn batching_owner(&self) -> BatchingOwner {
        BatchingOwner::VisionQl
    }

    async fn infer(
        &self,
        batch: RuntimeRequestBatch,
        cancel: CancellationToken,
        budget: crate::resources::QueryBudget,
    ) -> Result<RuntimeResponseBatch> {
        self.input_contract
            .validate_batch("ONNX input", &batch.input)?;
        let session = Arc::clone(&self.session);
        let output_contract = self.output_contract.clone();
        let task = tokio::task::spawn_blocking(move || {
            run_session(session, batch, output_contract, budget)
        });
        tokio::select! {
            _ = cancel.cancelled() => Err(VqlError::new(ErrorCode::QueryCancelled, "query cancelled")),
            result = task => result.map_err(|error| {
                VqlError::new(ErrorCode::Execution, "ONNX inference task failed").with_source(error)
            })?,
        }
    }
}

fn run_session(
    session: Arc<Mutex<Session>>,
    batch: RuntimeRequestBatch,
    output_contract: TensorContract,
    budget: crate::resources::QueryBudget,
) -> Result<RuntimeResponseBatch> {
    let input_name = batch.input.name().to_owned();
    let shape = batch
        .input
        .shape()
        .iter()
        .map(|value| {
            usize::try_from(*value).map_err(|_| {
                VqlError::new(
                    ErrorCode::Execution,
                    "ONNX input shape must be non-negative",
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let tensor = TensorRef::from_array_view((shape, batch.input.as_f32("ONNX input")?)).map_err(
        |error| {
            VqlError::new(ErrorCode::Execution, "failed to build ONNX input tensor")
                .with_source(error)
        },
    )?;
    let mut session = session
        .lock()
        .map_err(|_| VqlError::new(ErrorCode::Internal, "ONNX session lock was poisoned"))?;
    let outputs = session
        .run(ort::inputs![input_name.as_str() => tensor])
        .map_err(|error| {
            VqlError::new(ErrorCode::Execution, "ONNX inference failed").with_source(error)
        })?;
    let mut response = BTreeMap::new();
    let mut reservations = Vec::new();
    for output_name in batch.output_names {
        let output = outputs.get(&output_name).ok_or_else(|| {
            VqlError::new(
                ErrorCode::Execution,
                format!("ONNX output '{output_name}' is missing"),
            )
        })?;
        let (shape, values) = output.try_extract_tensor::<f32>().map_err(|error| {
            VqlError::new(ErrorCode::Execution, "ONNX output must be float32").with_source(error)
        })?;
        reservations.push(budget.reserve(
            crate::QueryResource::ModelTensor,
            values.len().saturating_mul(std::mem::size_of::<f32>()),
        )?);
        let tensor =
            TensorBatch::from_f32(output_name.clone(), shape.to_vec(), values.to_vec(), None)?;
        output_contract.validate_batch("ONNX output", &tensor)?;
        response.insert(output_name, tensor);
    }
    Ok(RuntimeResponseBatch {
        outputs: response,
        _reservations: reservations,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ort::value::{Shape, SymbolicDimensions};

    fn outlet(name: &str, shape: &[i64]) -> Outlet {
        Outlet::new(
            name,
            ValueType::Tensor {
                ty: TensorElementType::Float32,
                shape: Shape::new(shape.iter().copied()),
                dimension_symbols: SymbolicDimensions::new(
                    shape.iter().map(|_| String::new()).collect::<Vec<_>>(),
                ),
            },
        )
    }

    #[test]
    fn contract_validation_names_a_missing_onnx_tensor() {
        let error = validate_outlet(
            "input",
            &[outlet("pixels", &[-1, 3, 640, 640])],
            &TensorContract {
                name: "images".to_owned(),
                dtype: DataType::Float32,
                shape: vec![-1, 3, 640, 640],
            },
        )
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::Execution);
        assert!(error.message.contains("images"));
    }

    #[test]
    fn contract_validation_names_an_incompatible_onnx_resolution() {
        let error = validate_outlet(
            "input",
            &[outlet("images", &[-1, 3, 320, 320])],
            &TensorContract {
                name: "images".to_owned(),
                dtype: DataType::Float32,
                shape: vec![-1, 3, 640, 640],
            },
        )
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::Execution);
        assert!(error.message.contains("images"));
        assert!(error.message.contains("640"));
    }

    #[test]
    fn declaration_validates_runtime_scoped_processor_options_locally() {
        let model = ModelDef {
            name: "detector".to_owned(),
            model_type: ModelType::ObjectDetection,
            source: "mock://person".to_owned(),
            runtime_kind: "onnx-runtime".to_owned(),
            options: BTreeMap::from([("input".to_owned(), serde_json::json!({"width": 0}))]),
            declaration_fingerprint: "declaration".to_owned(),
            resolved: None,
        };

        let error = OrtRuntimeFactory.validate_declaration(&model).unwrap_err();

        assert_eq!(error.code, ErrorCode::InvalidOption);
        assert!(error.message.contains("WITH.input.width"));
    }
}
