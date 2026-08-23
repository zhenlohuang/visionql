use std::error::Error as StdError;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogErrorCode {
    AlreadyExists,
    NameConflict,
    NotFound,
    InvalidArgument,
    Conflict,
    Storage,
    Internal,
}

impl CatalogErrorCode {
    /// Returns the stable VisionQL error identifier for the catalog domain.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotFound => "VQL-02001",
            Self::InvalidArgument => "VQL-22001",
            Self::AlreadyExists => "VQL-23001",
            Self::NameConflict => "VQL-23002",
            Self::Conflict => "VQL-23003",
            Self::Storage => "VQL-58001",
            Self::Internal => "VQL-XX001",
        }
    }

    pub const fn symbol(self) -> &'static str {
        match self {
            Self::NotFound => "NOT_FOUND",
            Self::InvalidArgument => "INVALID_ARGUMENT",
            Self::AlreadyExists => "ALREADY_EXISTS",
            Self::NameConflict => "NAME_CONFLICT",
            Self::Conflict => "FAILED_PRECONDITION",
            Self::Storage => "CATALOG_ERROR",
            Self::Internal => "INTERNAL_ERROR",
        }
    }
}

impl Display for CatalogErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug)]
pub struct CatalogError {
    pub code: CatalogErrorCode,
    pub message: String,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl Display for CatalogError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {}: {}",
            self.code,
            self.code.symbol(),
            self.message
        )
    }
}

impl StdError for CatalogError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

impl CatalogError {
    pub fn new(code: CatalogErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            source: None,
        }
    }

    pub fn with_source(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }
}

pub type Result<T> = std::result::Result<T, CatalogError>;

#[cfg(feature = "sqlite")]
impl From<rusqlite::Error> for CatalogError {
    fn from(error: rusqlite::Error) -> Self {
        Self::new(
            CatalogErrorCode::Storage,
            "catalog backend operation failed",
        )
        .with_source(error)
    }
}

impl From<serde_json::Error> for CatalogError {
    fn from(error: serde_json::Error) -> Self {
        Self::new(
            CatalogErrorCode::Storage,
            "catalog contains invalid object metadata",
        )
        .with_source(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_error_uses_the_public_vql_format() {
        let error = CatalogError::new(CatalogErrorCode::NotFound, "table 'photos' not found");

        assert_eq!(error.code.as_str(), "VQL-02001");
        assert_eq!(
            error.to_string(),
            "[VQL-02001] NOT_FOUND: table 'photos' not found"
        );
    }
}
