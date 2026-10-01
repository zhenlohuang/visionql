//! VisionQL catalog domain, backend ports, and Unity Catalog wire API.

mod error;
mod job;
mod objects;
mod snapshot;
mod store;
pub mod uc;

pub use error::{CatalogError, CatalogErrorCode, Result};
pub use job::{CreateJob, JobDefinition, JobState, JobStatus, PersistentJob};
pub use objects::{
    CatalogInfo, EventTimePolicy, FunctionDef, FunctionImplementation, GenericTensorSpec,
    KafkaTableConfig, ModelDef, ModelInterface, ModelParameter, ModelType, ModelVersion,
    ObjectKind, ProcessorSpec, ResolvedExecutionSpec, ResolvedModelDef, ResolvedModelSpec,
    RtspTableConfig, RtspTransport, RuntimeSpec, SchemaInfo, SecurableMetadata, TableCapabilities,
    TableDef, TableProvider, TableProviderKind, decode_schema, encode_schema, image_field,
    image_storage_fields, images_schema, provider_schema, rtsp_schema, videos_schema,
};
pub use snapshot::{DefinitionSnapshot, SnapshotObject, SnapshotTable};
pub use store::{CatalogBackend, CatalogStore};

pub const DEFAULT_CATALOG: &str = "vql";
pub const DEFAULT_SCHEMA: &str = "default";
