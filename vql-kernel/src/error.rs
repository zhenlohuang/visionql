use std::error::Error as StdError;
use std::fmt::{Display, Formatter};

/// Stable machine-readable error categories exposed by every host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorCode {
    FeatureNotAvailable,
    InvalidSql,
    InvalidOption,
    InvalidLocation,
    Catalog,
    AlreadyExists,
    NotFound,
    QueryCancelled,
    PythonHostRequired,
    Execution,
    Internal,
}

impl ErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FeatureNotAvailable => "FEATURE_NOT_AVAILABLE",
            Self::InvalidSql => "INVALID_SQL",
            Self::InvalidOption => "INVALID_OPTION",
            Self::InvalidLocation => "INVALID_LOCATION",
            Self::Catalog => "CATALOG_ERROR",
            Self::AlreadyExists => "ALREADY_EXISTS",
            Self::NotFound => "NOT_FOUND",
            Self::QueryCancelled => "QUERY_CANCELLED",
            Self::PythonHostRequired => "PYTHON_HOST_REQUIRED",
            Self::Execution => "EXECUTION_ERROR",
            Self::Internal => "INTERNAL_ERROR",
        }
    }
}

impl Display for ErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug)]
pub struct VqlError {
    pub code: ErrorCode,
    pub message: String,
    pub target_version: Option<String>,
    source: Option<Box<dyn StdError + Send + Sync>>,
}

impl VqlError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            target_version: None,
            source: None,
        }
    }

    pub fn feature(message: impl Into<String>, target_version: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::FeatureNotAvailable,
            message: message.into(),
            target_version: Some(target_version.into()),
            source: None,
        }
    }

    pub fn with_source<E>(mut self, source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        self.source = Some(Box::new(source));
        self
    }
}

impl Display for VqlError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[VQL:{}] {}", self.code, self.message)?;
        if let Some(target) = &self.target_version {
            write!(f, " (target: {target})")?;
        }
        Ok(())
    }
}

impl StdError for VqlError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

impl From<std::io::Error> for VqlError {
    fn from(source: std::io::Error) -> Self {
        Self::new(ErrorCode::Execution, source.to_string()).with_source(source)
    }
}

impl From<rusqlite::Error> for VqlError {
    fn from(source: rusqlite::Error) -> Self {
        Self::new(ErrorCode::Catalog, source.to_string()).with_source(source)
    }
}

impl From<serde_json::Error> for VqlError {
    fn from(source: serde_json::Error) -> Self {
        Self::new(ErrorCode::Catalog, source.to_string()).with_source(source)
    }
}

impl From<datafusion::error::DataFusionError> for VqlError {
    fn from(source: datafusion::error::DataFusionError) -> Self {
        let message = source.to_string();
        Self::new(ErrorCode::Execution, message).with_source(source)
    }
}

pub type Result<T> = std::result::Result<T, VqlError>;
