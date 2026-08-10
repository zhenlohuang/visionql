use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::pipeline::{
    BatchingOwner, RuntimeRequestBatch, RuntimeResponseBatch, RuntimeSession, TensorBatch,
};
use crate::{ErrorCode, Result, VqlError};

#[derive(Debug)]
pub(super) struct TritonRuntime {
    metadata_url: String,
    infer_url: String,
    timeout: Duration,
    client: OnceLock<reqwest::blocking::Client>,
    metadata_validated: OnceLock<()>,
}

impl TritonRuntime {
    pub(super) fn new(source: &str, model_name: &str, model_version: Option<&str>) -> Result<Self> {
        Self::with_timeout(source, model_name, model_version, Duration::from_secs(30))
    }

    fn with_timeout(
        source: &str,
        model_name: &str,
        model_version: Option<&str>,
        timeout: Duration,
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
        Ok(Self {
            metadata_url,
            infer_url,
            timeout,
            client: OnceLock::new(),
            metadata_validated: OnceLock::new(),
        })
    }

    fn client(&self) -> Result<&reqwest::blocking::Client> {
        if let Some(client) = self.client.get() {
            return Ok(client);
        }
        let client = reqwest::blocking::Client::builder()
            .timeout(self.timeout)
            .build()
            .map_err(|error| {
                VqlError::new(ErrorCode::Execution, "failed to build Triton client")
                    .with_source(error)
            })?;
        let _ = self.client.set(client);
        self.client.get().ok_or_else(|| {
            VqlError::new(ErrorCode::Internal, "Triton client initialization failed")
        })
    }

    fn validate_metadata(&self, batch: &RuntimeRequestBatch) -> Result<()> {
        if self.metadata_validated.get().is_some() {
            return Ok(());
        }
        let metadata = self
            .client()?
            .get(&self.metadata_url)
            .send()
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
            .map_err(|error| {
                VqlError::new(
                    ErrorCode::Execution,
                    "Triton KServe V2 model metadata is invalid",
                )
                .with_source(error)
            })?;
        let input = metadata
            .inputs
            .iter()
            .find(|input| input.name == batch.input.name)
            .ok_or_else(|| {
                VqlError::new(
                    ErrorCode::Execution,
                    format!(
                        "Triton model metadata does not declare input '{}'",
                        batch.input.name
                    ),
                )
            })?;
        validate_tensor_contract("input", input, Some(&batch.input.shape))?;
        for output_name in &batch.output_names {
            let output = metadata
                .outputs
                .iter()
                .find(|output| output.name == *output_name)
                .ok_or_else(|| {
                    VqlError::new(
                        ErrorCode::Execution,
                        format!("Triton model metadata does not declare output '{output_name}'"),
                    )
                })?;
            validate_tensor_contract("output", output, None)?;
        }
        let _ = self.metadata_validated.set(());
        Ok(())
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

fn validate_tensor_contract(
    role: &str,
    tensor: &TensorMetadata,
    request_shape: Option<&[i64]>,
) -> Result<()> {
    if tensor.datatype != "FP32" {
        return Err(VqlError::new(
            ErrorCode::Execution,
            format!(
                "Triton {role} '{}' has unsupported datatype '{}'",
                tensor.name, tensor.datatype
            ),
        ));
    }
    if let Some(request_shape) = request_shape
        && (tensor.shape.len() != request_shape.len()
            || tensor
                .shape
                .iter()
                .zip(request_shape)
                .any(|(declared, requested)| *declared >= 0 && declared != requested))
    {
        return Err(VqlError::new(
            ErrorCode::Execution,
            format!(
                "Triton {role} '{}' shape {:?} is incompatible with request shape {:?}",
                tensor.name, tensor.shape, request_shape
            ),
        ));
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct InferRequest {
    inputs: Vec<InferInput>,
    outputs: Vec<RequestedOutput>,
}

#[derive(Debug, Serialize)]
struct InferInput {
    name: String,
    shape: Vec<i64>,
    datatype: &'static str,
    data: Vec<f32>,
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

impl RuntimeSession for TritonRuntime {
    fn kind(&self) -> &str {
        "triton"
    }

    fn batching_owner(&self) -> BatchingOwner {
        BatchingOwner::Service
    }

    fn infer(&self, batch: RuntimeRequestBatch) -> Result<RuntimeResponseBatch> {
        self.validate_metadata(&batch)?;
        let request = InferRequest {
            inputs: vec![InferInput {
                name: batch.input.name,
                shape: batch.input.shape,
                datatype: "FP32",
                data: batch.input.values,
            }],
            outputs: batch
                .output_names
                .into_iter()
                .map(|name| RequestedOutput { name })
                .collect(),
        };
        let response = self
            .client()?
            .post(&self.infer_url)
            .json(&request)
            .send()
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
            if outputs
                .insert(
                    name.clone(),
                    TensorBatch {
                        name: name.clone(),
                        shape: output.shape,
                        values: output.data,
                    },
                )
                .is_some()
            {
                return Err(VqlError::new(
                    ErrorCode::Execution,
                    format!("Triton returned duplicate output '{name}'"),
                ));
            }
        }
        Ok(RuntimeResponseBatch { outputs })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    fn request() -> RuntimeRequestBatch {
        RuntimeRequestBatch {
            input: TensorBatch {
                name: "images".to_owned(),
                shape: vec![1, 3, 1, 1],
                values: vec![0.0, 0.0, 0.0],
            },
            output_names: vec!["output0".to_owned()],
        }
    }

    #[test]
    fn triton_runtime_uses_kserve_v2_http_contract() {
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
            let mut buffer = [0_u8; 8192];
            let count = stream.read(&mut buffer).unwrap();
            let request = String::from_utf8_lossy(&buffer[..count]);
            assert!(request.contains("POST /v2/models/yolo/versions/1/infer"));
            assert!(request.contains("\"datatype\":\"FP32\""));
            assert!(request.contains("\"name\":\"images\""));
            let body = r#"{"outputs":[{"name":"output0","shape":[1,1,6],"datatype":"FP32","data":[0.0,0.0,1.0,1.0,0.9,0.0]}]}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(), body
            )
            .unwrap();
        });
        let runtime =
            TritonRuntime::new(&format!("endpoint://http://{address}"), "yolo", Some("1")).unwrap();

        let output = runtime.infer(request()).unwrap();

        assert_eq!(output.outputs["output0"].shape, vec![1, 1, 6]);
        server.join().unwrap();
    }

    #[test]
    fn triton_runtime_enforces_timeout() {
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
        let runtime = TritonRuntime::with_timeout(
            &format!("endpoint://http://{address}"),
            "yolo",
            None,
            Duration::from_millis(20),
        )
        .unwrap();

        let error = runtime.infer(request()).unwrap_err();

        assert_eq!(error.code, ErrorCode::Execution);
        assert!(error.message.contains("request failed"));
        server.join().unwrap();
    }
}
