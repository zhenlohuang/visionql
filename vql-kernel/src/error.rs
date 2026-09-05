use std::error::Error as StdError;
use std::fmt::{Display, Formatter};

/// Stable machine-readable errors exposed by every host.
///
/// Each variant has a permanent `VQL-CCDDD` identifier and a readable symbol.
/// Identifiers are the client contract; prose messages are diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorCode {
    FeatureNotAvailable,
    InvalidArgument,
    InvalidSql,
    InvalidOption,
    InvalidLocation,
    Catalog,
    AlreadyExists,
    NameConflict,
    FailedPrecondition,
    NotFound,
    QueryCancelled,
    ResourceExhausted,
    PythonHostRequired,
    Execution,
    Internal,
}

impl ErrorCode {
    /// Returns the stable public `VQL-CCDDD` identifier.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FeatureNotAvailable => "VQL-0A001",
            Self::NotFound => "VQL-02001",
            Self::InvalidArgument => "VQL-22001",
            Self::InvalidOption => "VQL-22002",
            Self::InvalidLocation => "VQL-22003",
            Self::AlreadyExists => "VQL-23001",
            Self::NameConflict => "VQL-23002",
            Self::FailedPrecondition => "VQL-23003",
            Self::InvalidSql => "VQL-42001",
            Self::ResourceExhausted => "VQL-53001",
            Self::PythonHostRequired => "VQL-55001",
            Self::QueryCancelled => "VQL-57001",
            Self::Catalog => "VQL-58001",
            Self::Execution => "VQL-58002",
            Self::Internal => "VQL-XX001",
        }
    }

    /// Returns the stable readable name associated with the identifier.
    pub const fn symbol(self) -> &'static str {
        match self {
            Self::FeatureNotAvailable => "FEATURE_NOT_AVAILABLE",
            Self::NotFound => "NOT_FOUND",
            Self::InvalidArgument => "INVALID_ARGUMENT",
            Self::InvalidOption => "INVALID_OPTION",
            Self::InvalidLocation => "INVALID_LOCATION",
            Self::AlreadyExists => "ALREADY_EXISTS",
            Self::NameConflict => "NAME_CONFLICT",
            Self::FailedPrecondition => "FAILED_PRECONDITION",
            Self::InvalidSql => "INVALID_SQL",
            Self::ResourceExhausted => "RESOURCE_EXHAUSTED",
            Self::PythonHostRequired => "PYTHON_HOST_REQUIRED",
            Self::QueryCancelled => "QUERY_CANCELLED",
            Self::Catalog => "CATALOG_ERROR",
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
        write!(
            f,
            "[{}] {}: {}",
            self.code,
            self.code.symbol(),
            self.message
        )?;
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

impl From<serde_json::Error> for VqlError {
    fn from(source: serde_json::Error) -> Self {
        Self::new(ErrorCode::Catalog, source.to_string()).with_source(source)
    }
}

impl From<arrow::error::ArrowError> for VqlError {
    fn from(source: arrow::error::ArrowError) -> Self {
        Self::new(ErrorCode::Execution, source.to_string()).with_source(source)
    }
}

impl From<vql_catalog::CatalogError> for VqlError {
    fn from(source: vql_catalog::CatalogError) -> Self {
        let code = match source.code {
            vql_catalog::CatalogErrorCode::AlreadyExists => ErrorCode::AlreadyExists,
            vql_catalog::CatalogErrorCode::NameConflict => ErrorCode::NameConflict,
            vql_catalog::CatalogErrorCode::NotFound => ErrorCode::NotFound,
            vql_catalog::CatalogErrorCode::InvalidArgument => ErrorCode::InvalidOption,
            vql_catalog::CatalogErrorCode::Conflict => ErrorCode::FailedPrecondition,
            vql_catalog::CatalogErrorCode::Storage => ErrorCode::Catalog,
            vql_catalog::CatalogErrorCode::Internal => ErrorCode::Internal,
        };
        let message = source.message.clone();
        Self::new(code, message).with_source(source)
    }
}

impl From<datafusion::error::DataFusionError> for VqlError {
    fn from(source: datafusion::error::DataFusionError) -> Self {
        let message = source.to_string();
        let code = datafusion_error_code(&source).unwrap_or(ErrorCode::Execution);
        Self::new(code, message).with_source(source)
    }
}

fn datafusion_error_code(error: &datafusion::error::DataFusionError) -> Option<ErrorCode> {
    use datafusion::error::DataFusionError;

    match error {
        DataFusionError::ResourcesExhausted(_) => Some(ErrorCode::ResourceExhausted),
        DataFusionError::Context(_, source) | DataFusionError::Diagnostic(_, source) => {
            datafusion_error_code(source)
        }
        DataFusionError::External(source) => {
            source.downcast_ref::<VqlError>().map(|error| error.code)
        }
        DataFusionError::Shared(source) => datafusion_error_code(source),
        DataFusionError::Collection(errors) => errors.iter().find_map(datafusion_error_code),
        _ => None,
    }
}

pub type Result<T> = std::result::Result<T, VqlError>;

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_CODES: [(ErrorCode, &str, &str); 15] = [
        (
            ErrorCode::FeatureNotAvailable,
            "VQL-0A001",
            "FEATURE_NOT_AVAILABLE",
        ),
        (ErrorCode::NotFound, "VQL-02001", "NOT_FOUND"),
        (ErrorCode::InvalidArgument, "VQL-22001", "INVALID_ARGUMENT"),
        (ErrorCode::InvalidOption, "VQL-22002", "INVALID_OPTION"),
        (ErrorCode::InvalidLocation, "VQL-22003", "INVALID_LOCATION"),
        (ErrorCode::AlreadyExists, "VQL-23001", "ALREADY_EXISTS"),
        (ErrorCode::NameConflict, "VQL-23002", "NAME_CONFLICT"),
        (
            ErrorCode::FailedPrecondition,
            "VQL-23003",
            "FAILED_PRECONDITION",
        ),
        (ErrorCode::InvalidSql, "VQL-42001", "INVALID_SQL"),
        (
            ErrorCode::ResourceExhausted,
            "VQL-53001",
            "RESOURCE_EXHAUSTED",
        ),
        (
            ErrorCode::PythonHostRequired,
            "VQL-55001",
            "PYTHON_HOST_REQUIRED",
        ),
        (ErrorCode::QueryCancelled, "VQL-57001", "QUERY_CANCELLED"),
        (ErrorCode::Catalog, "VQL-58001", "CATALOG_ERROR"),
        (ErrorCode::Execution, "VQL-58002", "EXECUTION_ERROR"),
        (ErrorCode::Internal, "VQL-XX001", "INTERNAL_ERROR"),
    ];

    #[test]
    fn public_error_identifiers_are_unique_and_well_formed() {
        let mut identifiers = Vec::new();
        let mut symbols = Vec::new();

        for (code, expected_identifier, expected_symbol) in ALL_CODES {
            let identifier = code.as_str();
            assert_eq!(identifier, expected_identifier);
            assert_eq!(code.symbol(), expected_symbol);
            assert_eq!(identifier.len(), 9, "{identifier}");
            assert!(identifier.starts_with("VQL-"), "{identifier}");
            assert!(
                identifier[4..]
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()),
                "{identifier}"
            );
            assert!(!identifiers.contains(&identifier), "{identifier}");
            assert!(!symbols.contains(&code.symbol()), "{}", code.symbol());
            identifiers.push(identifier);
            symbols.push(code.symbol());
        }
    }

    #[test]
    fn display_separates_identifier_symbol_and_message() {
        let error = VqlError::feature("AUDIO is reserved", "未排期");

        assert_eq!(
            error.to_string(),
            "[VQL-0A001] FEATURE_NOT_AVAILABLE: AUDIO is reserved (target: 未排期)"
        );
    }

    #[test]
    fn catalog_conflict_preserves_failed_precondition() {
        let source = vql_catalog::CatalogError::new(
            vql_catalog::CatalogErrorCode::Conflict,
            "model changed; retry it",
        );

        let error = VqlError::from(source);

        assert_eq!(error.code, ErrorCode::FailedPrecondition);
        assert_eq!(error.code.as_str(), "VQL-23003");
    }
}
