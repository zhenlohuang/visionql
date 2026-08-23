mod catalog;
mod config;
mod connectors;
mod engine;
mod error;
mod functions;
mod media;
mod models;
mod planner;
mod python;
mod resources;
mod secrets;
mod session;
mod sql;
mod stream;
#[cfg(test)]
mod test_util;
mod types;

pub use config::{EngineConfig, LogLevel};
pub use engine::Engine;
pub use error::{ErrorCode, Result, VqlError};
pub use python::{PyUdfHandle, PythonUdfHost, PythonUdfHostRef};
pub(crate) use resources::QueryResource;
pub use secrets::{KafkaAuthentication, KafkaTlsConfig, SecretProvider, SecretProviderRef};
pub use session::{
    DdlResult, QueryHandle, QueryInterruptAction, Session, SessionBuilder, Statement,
};
pub use sql::{ends_with_statement_terminator, split_statements};
pub use types::{
    MediaLocator, VqlType, audio_field, box2d_field, image_field, is_image_field, is_image_storage,
    locator_field, logical_type_of, mask_field, parse_locator, video_field,
};
