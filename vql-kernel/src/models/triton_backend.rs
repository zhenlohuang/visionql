use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::pipeline::{
    BatchingOwner, RuntimeRequestBatch, RuntimeResponseBatch, RuntimeSession, TensorBatch,
    TensorContract, kserve_datatype,
};
use super::registry::{RuntimeFactory, deserialize_runtime_options, invalid_option};
use crate::catalog::{ModelDef, ModelType, RuntimeSpec};
use crate::{ErrorCode, Result, VqlError};

const SUPPORTED_TYPES: &[ModelType] = &[ModelType::ObjectDetection];

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct TritonOptions {
    model_name: String,
    #[serde(default)]
    model_version: Option<String>,
}

impl TritonOptions {
    fn parse(spec: &RuntimeSpec) -> Result<Self> {
        let model_name = spec.options.get("model_name").ok_or_else(|| {
            VqlError::new(
                ErrorCode::InvalidOption,
                "invalid Model option 'runtime.model_name': is required for triton",
            )
        })?;
        if model_name.as_str().is_none_or(|value| value.is_empty()) {
            return invalid_option("runtime.model_name", "must be a non-empty string");
        }
        if let Some(version) = spec.options.get("model_version")
            && version.as_str().is_none_or(|value| value.is_empty())
        {
            return invalid_option("runtime.model_version", "must be a non-empty string");
        }
        let options: Self = deserialize_runtime_options(&spec.options)?;
        validate_path_segment("runtime.model_name", &options.model_name)?;
        if let Some(version) = &options.model_version {
            validate_path_segment("runtime.model_version", version)?;
        }
        Ok(options)
    }
}

#[derive(Debug)]
pub(super) struct TritonRuntimeFactory;

impl RuntimeFactory for TritonRuntimeFactory {
    fn kind(&self) -> &str {
        "triton"
    }

    fn supported_types(&self) -> &[ModelType] {
        SUPPORTED_TYPES
    }

    fn validate(&self, source: &str, spec: &RuntimeSpec) -> Result<()> {
        if !source.starts_with("endpoint://") {
            return invalid_option("runtime.kind", "triton requires endpoint:// source");
        }
        let endpoint = source.trim_start_matches("endpoint://");
        let url = reqwest::Url::parse(endpoint).map_err(|error| {
            VqlError::new(
                ErrorCode::InvalidOption,
                "invalid Model option 'source': Triton endpoint must be an absolute HTTP(S) URL",
            )
            .with_source(error)
        })?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return invalid_option("source", "Triton endpoint must be an absolute HTTP(S) URL");
        }
        if spec.protocol.as_deref() == Some("kserve_v2_grpc") {
            return Err(VqlError::feature(
                "Triton kserve_v2_grpc is not available",
                "未排期",
            ));
        }
        if spec.protocol.as_deref().unwrap_or("kserve_v2_http") != "kserve_v2_http" {
            return invalid_option(
                "runtime.protocol",
                "triton currently supports kserve_v2_http",
            );
        }
        TritonOptions::parse(spec).map(|_| ())
    }

    fn build(
        &self,
        model: &ModelDef,
        input: &TensorContract,
        output: &TensorContract,
    ) -> Result<Arc<dyn RuntimeSession>> {
        let options = TritonOptions::parse(&model.runtime)?;
        Ok(Arc::new(TritonRuntime::new(
            &model.source,
            &options.model_name,
            options.model_version.as_deref(),
            input.clone(),
            output.clone(),
        )?))
    }
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
pub(super) struct TritonRuntime {
    metadata_url: String,
    infer_url: String,
    client: reqwest::Client,
    metadata_validated: OnceLock<()>,
    input_contract: TensorContract,
    output_contract: TensorContract,
}

impl TritonRuntime {
    fn new(
        source: &str,
        model_name: &str,
        model_version: Option<&str>,
        input_contract: TensorContract,
        output_contract: TensorContract,
    ) -> Result<Self> {
        Self::with_timeout(
            source,
            model_name,
            model_version,
            Duration::from_secs(30),
            input_contract,
            output_contract,
        )
    }

    fn with_timeout(
        source: &str,
        model_name: &str,
        model_version: Option<&str>,
        timeout: Duration,
        input_contract: TensorContract,
        output_contract: TensorContract,
    ) -> Result<Self> {
        let endpoint = source.strip_prefix("endpoint://").unwrap_or(source);
        if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
            return Err(VqlError::new(
                ErrorCode::InvalidOption,
                "Triton endpoint source must contain an http:// or https:// URL",
            ));
        }
        let endpoint = endpoint.trim_end_matches('/');
        let metadata_url = if let Some(version) = model_version {
            format!("{endpoint}/v2/models/{model_name}/versions/{version}")
        } else {
            format!("{endpoint}/v2/models/{model_name}")
        };
        let infer_url = format!("{metadata_url}/infer");
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|error| {
                VqlError::new(ErrorCode::Execution, "failed to build Triton client")
                    .with_source(error)
            })?;
        Ok(Self {
            metadata_url,
            infer_url,
            client,
            metadata_validated: OnceLock::new(),
            input_contract,
            output_contract,
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
                VqlError::new(ErrorCode::Execution, "Triton metadata request failed")
                    .with_source(error)
            })?
            .error_for_status()
            .map_err(|error| {
                VqlError::new(
                    ErrorCode::Execution,
                    "Triton metadata request returned an error status",
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
        validate_metadata_tensor("input", &metadata.inputs, &self.input_contract)?;
        validate_metadata_tensor("output", &metadata.outputs, &self.output_contract)?;
        let _ = self.metadata_validated.set(());
        Ok(())
    }

    async fn infer_inner(&self, batch: RuntimeRequestBatch) -> Result<RuntimeResponseBatch> {
        self.input_contract
            .validate_batch("Triton input", &batch.input)?;
        self.validate_metadata().await?;
        let datatype = kserve_datatype(batch.input.value_type())?;
        let request = InferRequest {
            inputs: vec![InferInput {
                name: batch.input.name().to_owned(),
                shape: batch.input.shape(),
                datatype,
                data: batch.input.as_f32("Triton input")?,
            }],
            outputs: batch
                .output_names
                .iter()
                .cloned()
                .map(|name| RequestedOutput { name })
                .collect(),
        };
        let response = self
            .client
            .post(&self.infer_url)
            .json(&request)
            .send()
            .await
            .map_err(|error| {
                VqlError::new(ErrorCode::Execution, "Triton inference request failed")
                    .with_source(error)
            })?
            .error_for_status()
            .map_err(|error| {
                VqlError::new(
                    ErrorCode::Execution,
                    "Triton inference returned an error status",
                )
                .with_source(error)
            })?
            .json::<InferResponse>()
            .await
            .map_err(|error| {
                VqlError::new(ErrorCode::Execution, "Triton KServe V2 response is invalid")
                    .with_source(error)
            })?;

        let mut outputs = BTreeMap::new();
        for output in response.outputs {
            if output.datatype != "FP32" {
                return Err(VqlError::new(
                    ErrorCode::Execution,
                    format!(
                        "Triton output '{}' has unsupported datatype '{}'",
                        output.name, output.datatype
                    ),
                ));
            }
            let name = output.name;
            let tensor = TensorBatch::from_f32(name.clone(), output.shape, output.data, None)?;
            self.output_contract
                .validate_batch("Triton output", &tensor)?;
            if outputs.insert(name.clone(), tensor).is_some() {
                return Err(VqlError::new(
                    ErrorCode::Execution,
                    format!("Triton returned duplicate output '{name}'"),
                ));
            }
        }
        Ok(RuntimeResponseBatch { outputs })
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

fn validate_metadata_tensor(
    role: &str,
    tensors: &[TensorMetadata],
    contract: &TensorContract,
) -> Result<()> {
    let tensor = tensors
        .iter()
        .find(|tensor| tensor.name == contract.name)
        .ok_or_else(|| {
            VqlError::new(
                ErrorCode::Execution,
                format!(
                    "Triton model metadata does not declare {role} '{}'",
                    contract.name
                ),
            )
        })?;
    if tensor.datatype != kserve_datatype(&contract.dtype)? {
        return Err(VqlError::new(
            ErrorCode::Execution,
            format!(
                "Triton {role} '{}' has unsupported datatype '{}'",
                tensor.name, tensor.datatype
            ),
        ));
    }
    if tensor.shape.len() != contract.shape.len()
        || tensor
            .shape
            .iter()
            .zip(&contract.shape)
            .any(|(declared, expected)| *declared >= 0 && *expected >= 0 && declared != expected)
    {
        return Err(VqlError::new(
            ErrorCode::Execution,
            format!(
                "Triton {role} '{}' shape {:?} is incompatible with contract {:?}",
                tensor.name, tensor.shape, contract.shape
            ),
        ));
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct InferRequest<'a> {
    inputs: Vec<InferInput<'a>>,
    outputs: Vec<RequestedOutput>,
}

#[derive(Debug, Serialize)]
struct InferInput<'a> {
    name: String,
    shape: Vec<i64>,
    datatype: &'static str,
    data: &'a [f32],
}

#[derive(Debug, Serialize)]
struct RequestedOutput {
    name: String,
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
    data: Vec<f32>,
}

#[async_trait]
impl RuntimeSession for TritonRuntime {
    fn kind(&self) -> &str {
        "triton"
    }

    fn input_contract(&self) -> &TensorContract {
        &self.input_contract
    }

    fn output_contract(&self) -> &TensorContract {
        &self.output_contract
    }

    fn batching_owner(&self) -> BatchingOwner {
        BatchingOwner::Service
    }

    async fn infer(
        &self,
        batch: RuntimeRequestBatch,
        cancel: CancellationToken,
    ) -> Result<RuntimeResponseBatch> {
        tokio::select! {
            _ = cancel.cancelled() => Err(VqlError::new(ErrorCode::QueryCancelled, "query cancelled")),
            result = self.infer_inner(batch) => result,
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    fn contracts() -> (TensorContract, TensorContract) {
        (
            TensorContract {
                name: "images".to_owned(),
                dtype: DataType::Float32,
                shape: vec![-1, 3, 1, 1],
            },
            TensorContract {
                name: "output0".to_owned(),
                dtype: DataType::Float32,
                shape: vec![-1, -1, 6],
            },
        )
    }

    fn request() -> RuntimeRequestBatch {
        RuntimeRequestBatch {
            input: TensorBatch::from_f32(
                "images",
                vec![1, 3, 1, 1],
                vec![0.0, 0.0, 0.0],
                Some(vec!["C".to_owned(), "H".to_owned(), "W".to_owned()]),
            )
            .unwrap(),
            output_names: vec!["output0".to_owned()],
        }
    }

    #[tokio::test]
    async fn triton_runtime_uses_kserve_v2_http_contract() {
        let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
            return;
        };
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0_u8; 8192];
            let count = stream.read(&mut buffer).unwrap();
            let request = String::from_utf8_lossy(&buffer[..count]);
            assert!(request.contains("GET /v2/models/yolo/versions/1"));
            let body = r#"{"inputs":[{"name":"images","datatype":"FP32","shape":[-1,3,1,1]}],"outputs":[{"name":"output0","datatype":"FP32","shape":[-1,-1,6]}]}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(), body
            )
            .unwrap();

            let (mut stream, _) = listener.accept().unwrap();
            let count = stream.read(&mut buffer).unwrap();
            let request = String::from_utf8_lossy(&buffer[..count]);
            assert!(request.contains("POST /v2/models/yolo/versions/1/infer"));
            assert!(request.contains("\"datatype\":\"FP32\""));
            let body = r#"{"outputs":[{"name":"output0","shape":[1,1,6],"datatype":"FP32","data":[0.0,0.0,1.0,1.0,0.9,0.0]}]}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(), body
            )
            .unwrap();
        });
        let (input, output) = contracts();
        let runtime = TritonRuntime::new(
            &format!("endpoint://http://{address}"),
            "yolo",
            Some("1"),
            input,
            output,
        )
        .unwrap();
        let output = runtime
            .infer(request(), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(output.outputs["output0"].shape(), vec![1, 1, 6]);
        server.join().unwrap();
    }

    #[tokio::test]
    async fn triton_runtime_enforces_timeout() {
        let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
            return;
        };
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0_u8; 8192];
            let _ = stream.read(&mut buffer);
            std::thread::sleep(Duration::from_millis(100));
        });
        let (input, output) = contracts();
        let runtime = TritonRuntime::with_timeout(
            &format!("endpoint://http://{address}"),
            "yolo",
            None,
            Duration::from_millis(20),
            input,
            output,
        )
        .unwrap();
        let error = runtime
            .infer(request(), CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::Execution);
        assert!(error.message.contains("request failed"));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn triton_runtime_aborts_a_pending_request_on_cancellation() {
        let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
            return;
        };
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0_u8; 8192];
            let _ = stream.read(&mut buffer);
            let body = r#"{"inputs":[{"name":"images","datatype":"FP32","shape":[-1,3,1,1]}],"outputs":[{"name":"output0","datatype":"FP32","shape":[-1,-1,6]}]}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(), body
            )
            .unwrap();

            let (mut stream, _) = listener.accept().unwrap();
            let _ = stream.read(&mut buffer);
            std::thread::sleep(Duration::from_millis(200));
        });
        let (input, output) = contracts();
        let runtime = TritonRuntime::new(
            &format!("endpoint://http://{address}"),
            "yolo",
            None,
            input,
            output,
        )
        .unwrap();
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            trigger.cancel();
        });
        let started = std::time::Instant::now();
        let error = runtime.infer(request(), cancel).await.unwrap_err();
        assert_eq!(error.code, ErrorCode::QueryCancelled);
        assert!(started.elapsed() < Duration::from_millis(150));
        server.join().unwrap();
    }
}
