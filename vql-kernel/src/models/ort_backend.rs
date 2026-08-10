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
use super::registry::{RuntimeFactory, deserialize_runtime_options, invalid_option};
use crate::catalog::{ModelDef, ModelType, RuntimeSpec};
use crate::{ErrorCode, Result, VqlError};

const SUPPORTED_TYPES: &[ModelType] = &[ModelType::ObjectDetection];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OrtOptions {}

#[derive(Debug)]
pub(super) struct OrtRuntimeFactory;

impl RuntimeFactory for OrtRuntimeFactory {
    fn kind(&self) -> &str {
        "onnxruntime"
    }

    fn supported_types(&self) -> &[ModelType] {
        SUPPORTED_TYPES
    }

    fn validate(&self, source: &str, spec: &RuntimeSpec) -> Result<()> {
        if spec.protocol.is_some() {
            return invalid_option(
                "runtime.protocol",
                "onnxruntime does not use a wire protocol",
            );
        }
        if source.starts_with("endpoint://") {
            return invalid_option(
                "runtime.kind",
                "onnxruntime requires a local or cached ONNX artifact",
            );
        }
        if !spec.options.is_empty() {
            return invalid_option(
                "runtime",
                "onnxruntime does not accept binding options in v0.1",
            );
        }
        let _: OrtOptions = deserialize_runtime_options(&spec.options)?;
        if !source.starts_with("mock://") && !is_onnx_source(source) {
            return invalid_option(
                "source",
                "onnxruntime requires an ONNX artifact with a .onnx suffix",
            );
        }
        Ok(())
    }

    fn build(
        &self,
        model: &ModelDef,
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

fn is_onnx_source(source: &str) -> bool {
    if let Some(spec) = source.strip_prefix("hf://") {
        let path = spec.split('/').collect::<Vec<_>>();
        return path.len() == 2
            || path.get(2..).is_some_and(|components| {
                components.join("/").to_ascii_lowercase().ends_with(".onnx")
            });
    }
    Path::new(source.strip_prefix("file://").unwrap_or(source))
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("onnx"))
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
        #[cfg(target_os = "macos")]
        let coreml = Session::builder()
            .ok()
            .and_then(|builder| {
                builder
                    .with_execution_providers([ort::ep::CoreML::default().build()])
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
        "onnxruntime"
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
    ) -> Result<RuntimeResponseBatch> {
        self.input_contract
            .validate_batch("ONNX input", &batch.input)?;
        let session = Arc::clone(&self.session);
        let output_contract = self.output_contract.clone();
        let task =
            tokio::task::spawn_blocking(move || run_session(session, batch, output_contract));
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
        let tensor =
            TensorBatch::from_f32(output_name.clone(), shape.to_vec(), values.to_vec(), None)?;
        output_contract.validate_batch("ONNX output", &tensor)?;
        response.insert(output_name, tensor);
    }
    Ok(RuntimeResponseBatch { outputs: response })
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
}
