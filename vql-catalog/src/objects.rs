use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use serde::{Deserialize, Serialize};

use crate::{CatalogError, CatalogErrorCode, Result};

const IMAGE_EXTENSION_NAME: &str = "vql.image";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectKind {
    Table,
    Model,
    Function,
}

impl ObjectKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Model => "model",
            Self::Function => "function",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SecurableMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub properties: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogInfo {
    pub name: String,
    #[serde(flatten)]
    pub metadata: SecurableMetadata,
    pub id: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaInfo {
    pub catalog_name: String,
    pub name: String,
    #[serde(flatten)]
    pub metadata: SecurableMetadata,
    pub id: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TableProviderKind {
    Images,
    Videos,
    Rtsp,
    Kafka,
    External,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventTimePolicy {
    CaptureTime,
    IngestTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RtspTransport {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RtspTableConfig {
    #[serde(default)]
    pub name: String,
    pub endpoint: String,
    pub fps: f64,
    pub event_time: EventTimePolicy,
    pub watermark_delay_ms: i64,
    pub transport: RtspTransport,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KafkaTableConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_ref: Option<String>,
    pub delivery_timeout_ms: u64,
    pub buffer_capacity: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TableProvider {
    Images {
        location: String,
        #[serde(default)]
        recursive: bool,
    },
    Videos {
        location: String,
        #[serde(default)]
        recursive: bool,
        #[serde(default)]
        fps: Option<f64>,
        #[serde(default)]
        start_time_ms: Option<i64>,
    },
    Rtsp(RtspTableConfig),
    Kafka(KafkaTableConfig),
    External {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        data_source_format: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        storage_location: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableCapabilities {
    pub readable: bool,
    pub writable: bool,
    pub bounded: bool,
    pub durable: bool,
}

impl TableProvider {
    pub const fn kind(&self) -> TableProviderKind {
        match self {
            Self::Images { .. } => TableProviderKind::Images,
            Self::Videos { .. } => TableProviderKind::Videos,
            Self::Rtsp(_) => TableProviderKind::Rtsp,
            Self::Kafka(_) => TableProviderKind::Kafka,
            Self::External { .. } => TableProviderKind::External,
        }
    }

    pub const fn capabilities(&self) -> TableCapabilities {
        match self {
            Self::Images { .. } | Self::Videos { .. } => TableCapabilities {
                readable: true,
                writable: false,
                bounded: true,
                durable: true,
            },
            Self::Rtsp(_) => TableCapabilities {
                readable: true,
                writable: false,
                bounded: false,
                durable: false,
            },
            Self::Kafka(_) => TableCapabilities {
                readable: false,
                writable: true,
                bounded: false,
                durable: false,
            },
            Self::External { .. } => TableCapabilities {
                readable: false,
                writable: false,
                bounded: true,
                durable: true,
            },
        }
    }

    pub fn location(&self) -> Option<&str> {
        match self {
            Self::Images { location, .. } | Self::Videos { location, .. } => Some(location),
            Self::Rtsp(config) => Some(&config.endpoint),
            Self::Kafka(_) => None,
            Self::External {
                storage_location, ..
            } => storage_location.as_deref(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableDef {
    pub name: String,
    pub provider: TableProvider,
    #[serde(default)]
    pub metadata: SecurableMetadata,
}

impl TableDef {
    pub fn new(name: impl Into<String>, provider: TableProvider) -> Self {
        Self {
            name: name.into(),
            provider,
            metadata: SecurableMetadata::default(),
        }
    }

    pub const fn capabilities(&self) -> TableCapabilities {
        self.provider.capabilities()
    }

    pub fn validate(&self) -> Result<()> {
        match &self.provider {
            TableProvider::Images { location, .. } => validate_local_location(location)?,
            TableProvider::Videos { location, fps, .. } => {
                validate_local_location(location)?;
                if fps.is_some_and(|fps| !fps.is_finite() || fps <= 0.0 || fps > 120.0) {
                    return Err(invalid_table(
                        "video fps must be greater than 0 and at most 120",
                    ));
                }
            }
            TableProvider::Rtsp(config) => {
                let endpoint = url::Url::parse(&config.endpoint).map_err(|error| {
                    invalid_table("RTSP endpoint must be an absolute rtsp:// URL")
                        .with_source(error)
                })?;
                if endpoint.scheme() != "rtsp" || endpoint.host_str().is_none() {
                    return Err(invalid_table(
                        "RTSP endpoint must be an absolute rtsp:// URL with a host",
                    ));
                }
                if !endpoint.username().is_empty()
                    || endpoint.password().is_some()
                    || endpoint.query().is_some()
                    || endpoint.fragment().is_some()
                {
                    return Err(invalid_table(
                        "RTSP credentials, query parameters, and fragments cannot be stored in the catalog",
                    ));
                }
                if !config.fps.is_finite() || config.fps <= 0.0 || config.fps > 120.0 {
                    return Err(invalid_table(
                        "RTSP fps must be greater than 0 and at most 120",
                    ));
                }
                if config.watermark_delay_ms < 0 {
                    return Err(invalid_table("RTSP watermark delay must be non-negative"));
                }
            }
            TableProvider::Kafka(config) => validate_kafka(config)?,
            TableProvider::External { .. } => {}
        }
        Ok(())
    }
}

fn validate_local_location(location: &str) -> Result<()> {
    let path = Path::new(location);
    if location.trim().is_empty() || !path.is_absolute() {
        return Err(invalid_table(
            "local table location must be a non-empty absolute path",
        ));
    }
    let metadata = path.metadata().map_err(|error| {
        invalid_table("local table location must be a readable directory").with_source(error)
    })?;
    if !metadata.is_dir() {
        return Err(invalid_table(
            "local table location must be a readable directory",
        ));
    }
    std::fs::read_dir(path).map_err(|error| {
        invalid_table("local table location must be a readable directory").with_source(error)
    })?;
    Ok(())
}

fn validate_kafka(config: &KafkaTableConfig) -> Result<()> {
    let invalid_bootstrap = || {
        invalid_table(
            "bootstrap_servers must be a comma-separated list of host:port endpoints without URI schemes or credentials",
        )
    };
    for endpoint in config.bootstrap_servers.split(',') {
        let endpoint = endpoint.trim();
        if endpoint.is_empty()
            || endpoint.chars().any(|character| {
                character.is_whitespace() || matches!(character, '@' | '/' | '?' | '#')
            })
        {
            return Err(invalid_bootstrap());
        }
        let (host, port) = if let Some(bracketed) = endpoint.strip_prefix('[') {
            let (host, port) = bracketed.split_once("]:").ok_or_else(&invalid_bootstrap)?;
            if host.is_empty() || host.contains('[') || host.contains(']') {
                return Err(invalid_bootstrap());
            }
            (host, port)
        } else {
            let (host, port) = endpoint.rsplit_once(':').ok_or_else(&invalid_bootstrap)?;
            if host.is_empty() || host.contains(':') || host.contains('[') || host.contains(']') {
                return Err(invalid_bootstrap());
            }
            (host, port)
        };
        if host.is_empty() || port.parse::<u16>().ok().filter(|port| *port > 0).is_none() {
            return Err(invalid_bootstrap());
        }
    }
    if config.topic.is_empty()
        || config.topic.len() > 249
        || matches!(config.topic.as_str(), "." | "..")
        || !config
            .topic
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(invalid_table(
            "topic must be 1-249 ASCII letters, digits, '.', '_', or '-' and cannot be '.' or '..'",
        ));
    }
    if let Some(reference) = config.credential_ref.as_deref()
        && (reference.is_empty()
            || reference.trim() != reference
            || reference.len() > 1_024
            || reference.chars().any(char::is_control))
    {
        return Err(invalid_table(
            "credential_ref must be a non-empty opaque reference of at most 1024 characters",
        ));
    }
    if !(1..=3_600_000).contains(&config.delivery_timeout_ms) {
        return Err(invalid_table(
            "delivery_timeout_ms must be between 1 and 3600000",
        ));
    }
    if !(1..=100_000).contains(&config.buffer_capacity) {
        return Err(invalid_table(
            "buffer_capacity must be between 1 and 100000",
        ));
    }
    Ok(())
}

fn invalid_table(message: impl Into<String>) -> CatalogError {
    CatalogError::new(CatalogErrorCode::InvalidArgument, message)
}

pub fn image_storage_fields() -> Fields {
    Fields::from(vec![
        Arc::new(Field::new("uri", DataType::Utf8, true)),
        Arc::new(Field::new("locator", DataType::Utf8, true)),
        Arc::new(Field::new("pts_ms", DataType::Int64, true)),
        Arc::new(Field::new("frame_id", DataType::UInt64, true)),
        Arc::new(Field::new("encoded", DataType::Binary, true)),
        Arc::new(Field::new("encoding", DataType::Utf8, true)),
        Arc::new(Field::new("width", DataType::Int32, true)),
        Arc::new(Field::new("height", DataType::Int32, true)),
        Arc::new(Field::new("buffer_id", DataType::UInt64, true)),
        Arc::new(Field::new("buffer_slot", DataType::UInt32, true)),
    ])
}

pub fn image_field(name: impl Into<String>, nullable: bool) -> Field {
    Field::new(
        name.into(),
        DataType::Struct(image_storage_fields()),
        nullable,
    )
    .with_metadata(HashMap::from([
        (
            "ARROW:extension:name".to_owned(),
            IMAGE_EXTENSION_NAME.to_owned(),
        ),
        (
            "ARROW:extension:metadata".to_owned(),
            r#"{"version":1}"#.to_owned(),
        ),
    ]))
}

pub fn images_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("uri", DataType::Utf8, false),
        image_field("image", false),
        Field::new("width", DataType::Int32, true),
        Field::new("height", DataType::Int32, true),
        Field::new(
            "captured_at",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
    ]))
}

pub fn videos_schema(synthetic_event_time: bool) -> SchemaRef {
    let mut timestamp = Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        false,
    );
    if synthetic_event_time {
        timestamp = timestamp.with_metadata(HashMap::from([(
            "vql.synthetic_event_time".to_owned(),
            "true".to_owned(),
        )]));
    }
    Arc::new(Schema::new(vec![
        Field::new("uri", DataType::Utf8, false),
        timestamp,
        Field::new("pts_ms", DataType::Int64, false),
        Field::new("frame_id", DataType::UInt64, false),
        image_field("frame", false),
        Field::new("duration", DataType::Float64, true),
        Field::new("fps", DataType::Float64, true),
        Field::new("width", DataType::Int32, true),
        Field::new("height", DataType::Int32, true),
        Field::new("codec", DataType::Utf8, true),
    ]))
}

pub fn rtsp_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        image_field("frame", false),
        Field::new("frame_id", DataType::Int64, false),
        Field::new("source", DataType::Utf8, false),
    ]))
}

pub fn provider_schema(provider: &TableProvider) -> Option<SchemaRef> {
    match provider {
        TableProvider::Images { .. } => Some(images_schema()),
        TableProvider::Videos { start_time_ms, .. } => Some(videos_schema(start_time_ms.is_none())),
        TableProvider::Rtsp(_) => Some(rtsp_schema()),
        TableProvider::Kafka(_) | TableProvider::External { .. } => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ModelType {
    ObjectDetection,
    ImageClassification,
}

impl ModelType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ObjectDetection => "OBJECT_DETECTION",
            Self::ImageClassification => "IMAGE_CLASSIFICATION",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelParameter {
    pub name: String,
    pub data_type: String,
    #[serde(default)]
    pub constant: bool,
    #[serde(default)]
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelInterface {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability: Option<ModelType>,
    pub parameters: Vec<ModelParameter>,
    #[serde(default)]
    pub semantic_arguments: Vec<ModelParameter>,
    pub return_type: String,
    pub processing_family: String,
    #[serde(default = "default_true")]
    pub deterministic: bool,
}

impl ModelInterface {
    pub fn arguments(&self) -> impl Iterator<Item = &ModelParameter> {
        self.parameters.iter().chain(&self.semantic_arguments)
    }

    pub fn render_arguments(&self) -> String {
        self.arguments()
            .map(|parameter| {
                let constant = if parameter.constant { "CONST " } else { "" };
                let optional = if parameter.optional { "?" } else { "" };
                format!(
                    "{} {constant}{}{optional}",
                    parameter.name, parameter.data_type
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

const fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeSpec {
    pub kind: String,
    #[serde(default)]
    pub protocol: Option<String>,
    #[serde(default)]
    pub options: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessorSpec {
    pub kind: String,
    #[serde(default)]
    pub options: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GenericTensorSpec {
    pub name: String,
    pub data_type: String,
    pub shape: Vec<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processor: Option<ProcessorSpec>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelVersion {
    pub name: String,
    pub source: String,
    pub runtime_kind: String,
    #[serde(default)]
    pub options: BTreeMap<String, serde_json::Value>,
    pub declaration_fingerprint: String,
    #[serde(default)]
    pub resolved: Option<ResolvedModelSpec>,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelDef {
    pub name: String,
    pub interface: ModelInterface,
    pub versions: Vec<ModelVersion>,
    pub initial_version_fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(default)]
    pub builtin: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedModelSpec {
    pub resolved_source: String,
    pub artifact_hash: Option<String>,
    pub execution: ResolvedExecutionSpec,
    pub semantic_fingerprint: String,
    #[serde(default)]
    pub volatile: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ResolvedExecutionSpec {
    Embedded {
        runtime: RuntimeSpec,
        pre_processor: ProcessorSpec,
        post_processor: ProcessorSpec,
    },
    Service {
        runtime: RuntimeSpec,
    },
    Generic {
        runtime: RuntimeSpec,
        inputs: Vec<GenericTensorSpec>,
        outputs: Vec<GenericTensorSpec>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedModelDef {
    pub name: String,
    pub version: String,
    pub interface: ModelInterface,
    pub source: String,
    pub resolved_source: String,
    pub artifact_hash: Option<String>,
    pub execution: ResolvedExecutionSpec,
    pub semantic_fingerprint: String,
    pub volatile: bool,
}

impl ModelDef {
    pub fn version(&self, name: &str) -> Option<&ModelVersion> {
        self.versions.iter().find(|version| version.name == name)
    }

    pub fn version_mut(&mut self, name: &str) -> Option<&mut ModelVersion> {
        self.versions
            .iter_mut()
            .find(|version| version.name == name)
    }

    pub fn default(&self) -> Option<&ModelVersion> {
        self.version(self.default_version.as_deref()?)
    }

    pub fn resolved_definition(&self, version: &str) -> Option<ResolvedModelDef> {
        let version = self.version(version)?;
        let resolved = version.resolved.as_ref()?;
        Some(ResolvedModelDef {
            name: self.name.clone(),
            version: version.name.clone(),
            interface: self.interface.clone(),
            source: version.source.clone(),
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
pub enum FunctionImplementation {
    Python { entry: String },
    SqlMacro { expression: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionDef {
    pub name: String,
    pub implementation: FunctionImplementation,
    pub parameters: Vec<(String, String)>,
    #[serde(default)]
    pub constant_parameters: Vec<String>,
    pub return_type: String,
    pub semantic_fingerprint: String,
}

pub fn encode_schema(schema: &SchemaRef) -> Result<Vec<u8>> {
    let writer = StreamWriter::try_new(Vec::new(), schema).map_err(|error| {
        CatalogError::new(
            CatalogErrorCode::Storage,
            "failed to encode Arrow table schema",
        )
        .with_source(error)
    })?;
    writer.into_inner().map_err(|error| {
        CatalogError::new(
            CatalogErrorCode::Storage,
            "failed to finish Arrow schema stream",
        )
        .with_source(error)
    })
}

pub fn decode_schema(bytes: &[u8]) -> Result<SchemaRef> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(|error| {
        CatalogError::new(
            CatalogErrorCode::Storage,
            "catalog contains an invalid Arrow schema",
        )
        .with_source(error)
    })?;
    Ok(Arc::new(reader.schema().as_ref().clone()))
}
