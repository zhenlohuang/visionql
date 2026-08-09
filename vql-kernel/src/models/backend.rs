use image::DynamicImage;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

use super::Detection;
use crate::Result;

pub(crate) trait ModelBackend: Send + Sync + std::fmt::Debug {
    fn infer(&self, images: Vec<DynamicImage>) -> Result<Vec<Vec<Detection>>>;
}

#[derive(Debug)]
pub(crate) struct EndpointBackend {
    url: String,
    timeout: std::time::Duration,
    client: OnceLock<reqwest::blocking::Client>,
}

impl EndpointBackend {
    pub(crate) fn new(source: &str) -> Result<Self> {
        Self::with_timeout(source, std::time::Duration::from_secs(30))
    }

    fn with_timeout(source: &str, timeout: std::time::Duration) -> Result<Self> {
        let url = source.strip_prefix("endpoint://").unwrap_or(source);
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err(crate::VqlError::new(
                crate::ErrorCode::InvalidOption,
                "endpoint source must contain an http:// or https:// URL",
            ));
        }
        Ok(Self {
            url: url.to_owned(),
            timeout,
            client: OnceLock::new(),
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
                crate::VqlError::new(
                    crate::ErrorCode::Execution,
                    "failed to build endpoint client",
                )
                .with_source(error)
            })?;
        let _ = self.client.set(client);
        self.client.get().ok_or_else(|| {
            crate::VqlError::new(
                crate::ErrorCode::Internal,
                "endpoint client initialization failed",
            )
        })
    }
}

#[derive(Serialize)]
struct EndpointRequest {
    images: Vec<String>,
}

#[derive(Deserialize)]
struct EndpointResponse {
    detections: Vec<Vec<EndpointDetection>>,
}

#[derive(Deserialize)]
struct EndpointDetection {
    label: String,
    confidence: f32,
    #[serde(rename = "box")]
    coordinates: [f32; 4],
}

impl ModelBackend for EndpointBackend {
    fn infer(&self, images: Vec<DynamicImage>) -> Result<Vec<Vec<Detection>>> {
        use base64::Engine as _;
        let input_count = images.len();
        let mut encoded = Vec::with_capacity(images.len());
        for image in images {
            let mut bytes = Vec::new();
            image::codecs::jpeg::JpegEncoder::new_with_quality(&mut bytes, 90)
                .encode_image(&image)
                .map_err(|error| {
                    crate::VqlError::new(
                        crate::ErrorCode::Execution,
                        "failed to encode endpoint input",
                    )
                    .with_source(error)
                })?;
            encoded.push(base64::engine::general_purpose::STANDARD.encode(bytes));
        }
        let response = self
            .client()?
            .post(&self.url)
            .json(&EndpointRequest { images: encoded })
            .send()
            .map_err(|error| {
                crate::VqlError::new(
                    crate::ErrorCode::Execution,
                    "endpoint inference request failed",
                )
                .with_source(error)
            })?
            .error_for_status()
            .map_err(|error| {
                crate::VqlError::new(
                    crate::ErrorCode::Execution,
                    "endpoint inference returned an error status",
                )
                .with_source(error)
            })?
            .json::<EndpointResponse>()
            .map_err(|error| {
                crate::VqlError::new(
                    crate::ErrorCode::Execution,
                    "endpoint inference response is invalid",
                )
                .with_source(error)
            })?;
        if response.detections.len() != input_count {
            return Err(crate::VqlError::new(
                crate::ErrorCode::Execution,
                format!(
                    "endpoint returned {} rows for {input_count} inputs",
                    response.detections.len()
                ),
            ));
        }
        Ok(response
            .detections
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|value| Detection {
                        label: value.label,
                        confidence: value.confidence,
                        x: value.coordinates[0],
                        y: value.coordinates[1],
                        w: value.coordinates[2],
                        h: value.coordinates[3],
                    })
                    .collect()
            })
            .collect())
    }
}

#[derive(Debug)]
pub(crate) struct MockBackend {
    label: String,
}

impl MockBackend {
    pub(crate) fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
        }
    }
}

impl ModelBackend for MockBackend {
    fn infer(&self, images: Vec<DynamicImage>) -> Result<Vec<Vec<Detection>>> {
        Ok(images
            .into_iter()
            .map(|image| {
                let confidence = if image.width() > 0 && image.height() > 0 {
                    0.9
                } else {
                    0.0
                };
                vec![Detection {
                    label: self.label.clone(),
                    confidence,
                    x: 0.25,
                    y: 0.25,
                    w: 0.5,
                    h: 0.5,
                }]
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    #[test]
    fn endpoint_backend_defers_blocking_client_initialization() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let backend = EndpointBackend::new("endpoint://http://127.0.0.1:9/infer").unwrap();
            assert!(backend.client.get().is_none());
        });
    }

    #[test]
    fn endpoint_backend_uses_bounded_json_contract() {
        let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
            // Some hermetic test sandboxes prohibit loopback listeners.
            return;
        };
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0_u8; 8192];
            let count = stream.read(&mut buffer).unwrap();
            let request = String::from_utf8_lossy(&buffer[..count]);
            assert!(request.contains("POST /infer"));
            let body =
                r#"{"detections":[[{"label":"person","confidence":0.8,"box":[0.1,0.2,0.3,0.4]}]]}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(), body
            )
            .unwrap();
        });
        let backend = EndpointBackend::new(&format!("endpoint://http://{address}/infer")).unwrap();
        let output = backend.infer(vec![DynamicImage::new_rgb8(4, 4)]).unwrap();
        assert_eq!(output[0][0].label, "person");
        assert_eq!(output[0][0].w, 0.3);
        server.join().unwrap();
    }

    #[test]
    fn endpoint_backend_enforces_timeout() {
        let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
            return;
        };
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0_u8; 8192];
            let _ = stream.read(&mut buffer);
            std::thread::sleep(std::time::Duration::from_millis(100));
        });
        let backend = EndpointBackend::with_timeout(
            &format!("endpoint://http://{address}/infer"),
            std::time::Duration::from_millis(20),
        )
        .unwrap();

        let error = backend
            .infer(vec![DynamicImage::new_rgb8(4, 4)])
            .unwrap_err();

        assert_eq!(error.code, crate::ErrorCode::Execution);
        assert!(error.message.contains("request failed"));
        server.join().unwrap();
    }
}
