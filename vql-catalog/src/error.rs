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

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct CatalogError {
    pub code: CatalogErrorCode,
    pub message: String,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
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
