use std::io::Cursor;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::{ErrorCode, Result, VqlError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObjectKind {
    Table,
    Stream,
    Model,
    Function,
    Sink,
}

impl ObjectKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Stream => "stream",
            Self::Model => "model",
            Self::Function => "function",
            Self::Sink => "sink",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub(crate) enum TableProviderKind {
    Images,
    Videos,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct TableDef {
    pub(crate) name: String,
    pub(crate) provider: TableProviderKind,
    pub(crate) location: String,
    #[serde(default)]
    pub(crate) recursive: bool,
    #[serde(default)]
    pub(crate) fps: Option<f64>,
    #[serde(default)]
    pub(crate) start_time_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EventTimePolicy {
    CaptureTime,
    IngestTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RtspTransport {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StreamDef {
    pub(crate) name: String,
    pub(crate) endpoint: String,
    pub(crate) fps: f64,
    pub(crate) event_time: EventTimePolicy,
    pub(crate) watermark_delay_ms: i64,
    pub(crate) transport: RtspTransport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum ModelType {
    ObjectDetection,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct RuntimeSpec {
    pub(crate) kind: String,
    #[serde(default)]
    pub(crate) protocol: Option<String>,
    #[serde(default)]
    pub(crate) options: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ProcessorSpec {
    pub(crate) kind: String,
    #[serde(default)]
    pub(crate) options: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ModelDef {
    pub(crate) name: String,
    pub(crate) model_type: ModelType,
    pub(crate) source: String,
    pub(crate) runtime_kind: String,
    #[serde(default)]
    pub(crate) options: BTreeMap<String, serde_json::Value>,
    pub(crate) declaration_fingerprint: String,
    #[serde(default)]
    pub(crate) resolved: Option<ResolvedModelSpec>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ResolvedModelSpec {
    pub(crate) resolved_source: String,
    pub(crate) artifact_hash: Option<String>,
    pub(crate) execution: ResolvedExecutionSpec,
    pub(crate) semantic_fingerprint: String,
    #[serde(default)]
    pub(crate) volatile: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub(crate) enum ResolvedExecutionSpec {
    Embedded {
        runtime: RuntimeSpec,
        pre_processor: ProcessorSpec,
        post_processor: ProcessorSpec,
    },
    Service {
        runtime: RuntimeSpec,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedModelDef {
    pub(crate) name: String,
    pub(crate) model_type: ModelType,
    pub(crate) source: String,
    pub(crate) resolved_source: String,
    pub(crate) artifact_hash: Option<String>,
    pub(crate) execution: ResolvedExecutionSpec,
    pub(crate) semantic_fingerprint: String,
    pub(crate) volatile: bool,
}

impl ModelDef {
    pub(crate) fn resolved_definition(&self) -> Option<ResolvedModelDef> {
        let resolved = self.resolved.as_ref()?;
        Some(ResolvedModelDef {
            name: self.name.clone(),
            model_type: self.model_type,
            source: self.source.clone(),
            resolved_source: resolved.resolved_source.clone(),
            artifact_hash: resolved.artifact_hash.clone(),
            execution: resolved.execution.clone(),
            semantic_fingerprint: resolved.semantic_fingerprint.clone(),
            volatile: resolved.volatile,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum FunctionImplementation {
    Python { entry: String },
    SqlMacro { expression: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct FunctionDef {
    pub(crate) name: String,
    pub(crate) implementation: FunctionImplementation,
    pub(crate) parameters: Vec<(String, String)>,
    pub(crate) return_type: String,
    pub(crate) semantic_fingerprint: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SinkKind {
    Console,
    Kafka,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct KafkaSinkConfig {
    pub(crate) bootstrap_servers: String,
    pub(crate) topic: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) credential_ref: Option<String>,
    pub(crate) delivery_timeout_ms: u64,
    pub(crate) buffer_capacity: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct SinkDef {
    pub(crate) name: String,
    pub(crate) kind: SinkKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) kafka: Option<KafkaSinkConfig>,
}

pub(crate) fn encode_schema(schema: &SchemaRef) -> Result<Vec<u8>> {
    let writer = StreamWriter::try_new(Vec::new(), schema).map_err(|error| {
        VqlError::new(ErrorCode::Catalog, "failed to encode Arrow table schema").with_source(error)
    })?;
    writer.into_inner().map_err(|error| {
        VqlError::new(ErrorCode::Catalog, "failed to finish Arrow schema stream").with_source(error)
    })
}

pub(crate) fn decode_schema(bytes: &[u8]) -> Result<SchemaRef> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(|error| {
        VqlError::new(
            ErrorCode::Catalog,
            "catalog contains an invalid Arrow schema",
        )
        .with_source(error)
    })?;
    Ok(Arc::new(reader.schema().as_ref().clone()))
}
