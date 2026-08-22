use crate::{ErrorCode, Result, SecretProviderRef, VqlError};
use serde::Deserialize;
use std::ffi::OsString;
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::str::FromStr;

const CONFIG_VERSION: u32 = 1;
const DEFAULT_SESSION_MEMORY_LIMIT: &str = "512 MiB";
const DEFAULT_SESSION_MEMORY_LIMIT_BYTES: usize = 512 * 1024 * 1024;
const DEFAULT_CATALOG_PATH: &str = "catalog/vql.db";
const CONFIG_FILE_NAME: &str = "config.toml";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Error,
    Warn,
    #[default]
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        }
    }
}

impl FromStr for LogLevel {
    type Err = VqlError;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "error" => Ok(Self::Error),
            "warn" => Ok(Self::Warn),
            "info" => Ok(Self::Info),
            "debug" => Ok(Self::Debug),
            "trace" => Ok(Self::Trace),
            _ => Err(VqlError::new(
                ErrorCode::InvalidOption,
                format!(
                    "invalid VQL_LOG_LEVEL '{value}'; expected error, warn, info, debug, or trace"
                ),
            )),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    version: u32,
    #[serde(default)]
    log: LogConfig,
    #[serde(default)]
    catalog: CatalogConfig,
    #[serde(default)]
    kernel: KernelConfig,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogConfig {
    #[serde(default)]
    level: LogLevel,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum CatalogBackend {
    #[default]
    Embedded,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogConfig {
    #[serde(default)]
    backend: CatalogBackend,
    #[serde(default)]
    embedded: EmbeddedCatalogConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EmbeddedCatalogConfig {
    #[serde(default = "default_catalog_path")]
    path: PathBuf,
}

impl Default for EmbeddedCatalogConfig {
    fn default() -> Self {
        Self {
            path: default_catalog_path(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct KernelConfig {
    #[serde(default)]
    session: SessionConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionConfig {
    #[serde(default = "default_session_memory_limit")]
    memory_limit: String,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            memory_limit: default_session_memory_limit(),
        }
    }
}

fn default_catalog_path() -> PathBuf {
    PathBuf::from(DEFAULT_CATALOG_PATH)
}

fn default_session_memory_limit() -> String {
    DEFAULT_SESSION_MEMORY_LIMIT.to_owned()
}

#[derive(Clone)]
pub struct EngineConfig {
    vql_home: PathBuf,
    catalog_path: PathBuf,
    model_cache_dir: PathBuf,
    history_path: PathBuf,
    session_memory_limit_bytes: usize,
    log_level: LogLevel,
    secret_provider: Option<SecretProviderRef>,
}

impl EngineConfig {
    /// Build an isolated configuration around an explicit catalog path.
    ///
    /// Hosts that honor `$VQL_HOME/config.toml` should start with
    /// [`EngineConfig::load`], then apply explicit overrides such as
    /// [`EngineConfig::with_catalog_path`].
    pub fn new(catalog_path: impl Into<PathBuf>) -> Self {
        let catalog_path = catalog_path.into();
        let vql_home = catalog_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        Self::from_home(vql_home).with_catalog_path(catalog_path)
    }

    pub fn from_home(vql_home: impl Into<PathBuf>) -> Self {
        let vql_home = vql_home.into();
        let vql_home = if vql_home.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            vql_home
        };
        Self {
            catalog_path: vql_home.join(DEFAULT_CATALOG_PATH),
            model_cache_dir: vql_home.join("cache/models"),
            history_path: vql_home.join("history"),
            session_memory_limit_bytes: DEFAULT_SESSION_MEMORY_LIMIT_BYTES,
            log_level: LogLevel::Info,
            secret_provider: None,
            vql_home,
        }
    }

    /// Load `$VQL_HOME/config.toml`, falling back to built-in defaults when it is absent.
    pub fn load() -> Result<Self> {
        let vql_home = resolve_vql_home(std::env::var_os("VQL_HOME"), std::env::var_os("HOME"));
        let log_level = std::env::var_os("VQL_LOG_LEVEL").filter(|value| !value.is_empty());
        Self::load_from_home_with_log_level(vql_home, log_level)
    }

    /// Load `config.toml` from an explicit VisionQL home directory.
    pub fn load_from_home(vql_home: impl Into<PathBuf>) -> Result<Self> {
        Self::load_from_home_with_log_level(vql_home.into(), None)
    }

    fn load_from_home_with_log_level(
        vql_home: PathBuf,
        log_level: Option<OsString>,
    ) -> Result<Self> {
        let mut resolved = Self::from_home(vql_home);
        let config_path = resolved.vql_home.join(CONFIG_FILE_NAME);
        let file = match std::fs::read_to_string(&config_path) {
            Ok(file) => Some(file),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                return Err(VqlError::new(
                    ErrorCode::InvalidOption,
                    format!(
                        "failed to read configuration '{}': {error}",
                        config_path.display()
                    ),
                )
                .with_source(error));
            }
        };
        if let Some(file) = file {
            let config = toml::from_str::<FileConfig>(&file).map_err(|error| {
                VqlError::new(
                    ErrorCode::InvalidOption,
                    format!(
                        "failed to parse configuration '{}': {error}",
                        config_path.display()
                    ),
                )
                .with_source(error)
            })?;
            if config.version != CONFIG_VERSION {
                return Err(VqlError::new(
                    ErrorCode::InvalidOption,
                    format!(
                        "unsupported configuration version {} in '{}'; expected {CONFIG_VERSION}",
                        config.version,
                        config_path.display()
                    ),
                ));
            }
            match config.catalog.backend {
                CatalogBackend::Embedded => {
                    if config.catalog.embedded.path.as_os_str().is_empty() {
                        return Err(VqlError::new(
                            ErrorCode::InvalidOption,
                            "catalog.embedded.path must not be empty",
                        ));
                    }
                    resolved.catalog_path = if config.catalog.embedded.path.is_absolute() {
                        config.catalog.embedded.path
                    } else {
                        resolved.vql_home.join(config.catalog.embedded.path)
                    };
                }
            }
            resolved.session_memory_limit_bytes =
                parse_memory_limit(&config.kernel.session.memory_limit)?;
            resolved.log_level = config.log.level;
        }
        if let Some(level) = log_level {
            let level = level.into_string().map_err(|value| {
                VqlError::new(
                    ErrorCode::InvalidOption,
                    format!(
                        "VQL_LOG_LEVEL is not valid UTF-8: {}",
                        value.to_string_lossy()
                    ),
                )
            })?;
            resolved.log_level = level.parse()?;
        }
        Ok(resolved)
    }

    pub fn vql_home(&self) -> &Path {
        &self.vql_home
    }

    pub fn catalog_path(&self) -> &Path {
        &self.catalog_path
    }

    pub fn model_cache_dir(&self) -> &Path {
        &self.model_cache_dir
    }

    pub fn history_path(&self) -> &Path {
        &self.history_path
    }

    pub fn with_catalog_path(mut self, catalog_path: impl Into<PathBuf>) -> Self {
        self.catalog_path = catalog_path.into();
        self
    }

    pub fn with_model_cache_dir(mut self, model_cache_dir: impl Into<PathBuf>) -> Self {
        self.model_cache_dir = model_cache_dir.into();
        self
    }

    pub fn with_history_path(mut self, history_path: impl Into<PathBuf>) -> Self {
        self.history_path = history_path.into();
        self
    }

    /// Set the total tracked host-memory budget shared by one Session.
    pub fn with_session_memory_limit_bytes(mut self, limit: usize) -> Self {
        self.session_memory_limit_bytes = limit;
        self
    }

    pub fn session_memory_limit_bytes(&self) -> usize {
        self.session_memory_limit_bytes
    }

    pub fn log_level(&self) -> LogLevel {
        self.log_level
    }

    /// Install the host-owned resolver used by credential references.
    pub fn with_secret_provider(mut self, provider: SecretProviderRef) -> Self {
        self.secret_provider = Some(provider);
        self
    }

    pub fn secret_provider(&self) -> Option<&SecretProviderRef> {
        self.secret_provider.as_ref()
    }

    pub(crate) fn prepare(&self) -> Result<()> {
        if self.session_memory_limit_bytes == 0 {
            return Err(VqlError::new(
                ErrorCode::InvalidOption,
                "session memory limit must be greater than zero",
            ));
        }
        let parent = self.catalog_path.parent().ok_or_else(|| {
            VqlError::new(
                ErrorCode::InvalidLocation,
                "catalog path must have a parent directory",
            )
        })?;
        let parent = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        std::fs::create_dir_all(parent)?;
        if let Some(parent) = self.history_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(())
    }
}

impl Debug for EngineConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EngineConfig")
            .field("vql_home", &self.vql_home)
            .field("catalog_path", &self.catalog_path)
            .field("model_cache_dir", &self.model_cache_dir)
            .field("history_path", &self.history_path)
            .field(
                "session_memory_limit_bytes",
                &self.session_memory_limit_bytes,
            )
            .field("log_level", &self.log_level)
            .field(
                "secret_provider",
                &self.secret_provider.as_ref().map(|_| "[SecretProvider]"),
            )
            .finish()
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self::from_home(resolve_vql_home(
            std::env::var_os("VQL_HOME"),
            std::env::var_os("HOME"),
        ))
    }
}

fn parse_memory_limit(value: &str) -> Result<usize> {
    let value = value.trim();
    let number_end = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let (number, unit) = value.split_at(number_end);
    let unit = unit.trim();
    if number.is_empty() || unit.is_empty() {
        return Err(invalid_memory_limit(value));
    }
    let number = number
        .parse::<u64>()
        .map_err(|_| invalid_memory_limit(value))?;
    let multiplier = match unit {
        "B" => 1_u64,
        "KiB" => 1024,
        "MiB" => 1024 * 1024,
        "GiB" => 1024 * 1024 * 1024,
        "TiB" => 1024_u64.pow(4),
        _ => return Err(invalid_memory_limit(value)),
    };
    let bytes = number
        .checked_mul(multiplier)
        .filter(|bytes| *bytes > 0)
        .and_then(|bytes| usize::try_from(bytes).ok())
        .ok_or_else(|| invalid_memory_limit(value))?;
    Ok(bytes)
}

fn invalid_memory_limit(value: &str) -> VqlError {
    VqlError::new(
        ErrorCode::InvalidOption,
        format!(
            "invalid kernel.session.memory_limit '{value}'; expected a positive integer followed by B, KiB, MiB, GiB, or TiB"
        ),
    )
}

fn resolve_vql_home(vql_home: Option<OsString>, home: Option<OsString>) -> PathBuf {
    match vql_home.filter(|value| !value.is_empty()) {
        Some(value) => PathBuf::from(value),
        None => home
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".vql"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Engine;

    #[test]
    fn home_owns_catalog_history_and_model_cache() {
        let config = EngineConfig::from_home("./data/.vql");

        assert_eq!(config.vql_home(), Path::new("./data/.vql"));
        assert_eq!(
            config.catalog_path(),
            Path::new("./data/.vql/catalog/vql.db")
        );
        assert_eq!(config.history_path(), Path::new("./data/.vql/history"));
        assert_eq!(
            config.model_cache_dir(),
            Path::new("./data/.vql/cache/models")
        );
    }

    #[test]
    fn engine_creates_the_nested_default_catalog() {
        let temp = tempfile::tempdir().unwrap();
        let config = EngineConfig::from_home(temp.path().join(".vql"));
        let catalog_path = config.catalog_path().to_path_buf();

        let _engine = Engine::new(config).unwrap();

        assert_eq!(catalog_path, temp.path().join(".vql/catalog/vql.db"));
        assert!(catalog_path.is_file());
    }

    #[test]
    fn session_memory_limit_must_be_non_zero() {
        let temp = tempfile::tempdir().unwrap();
        let error = Engine::new(
            EngineConfig::new(temp.path().join("catalog.db")).with_session_memory_limit_bytes(0),
        )
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidOption);
    }

    #[test]
    fn config_file_resolves_embedded_catalog_and_session_memory() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(
            temp.path().join(CONFIG_FILE_NAME),
            r#"
version = 1

[log]
level = "debug"

[catalog]
backend = "embedded"

[catalog.embedded]
path = "state/catalog.db"

[kernel.session]
memory_limit = "768 MiB"
"#,
        )
        .unwrap();

        let config = EngineConfig::load_from_home(temp.path()).unwrap();

        assert_eq!(config.log_level(), LogLevel::Debug);
        assert_eq!(config.catalog_path(), temp.path().join("state/catalog.db"));
        assert_eq!(config.session_memory_limit_bytes(), 768 * 1024 * 1024);
    }

    #[test]
    fn missing_config_file_uses_built_in_defaults() {
        let temp = tempfile::tempdir().unwrap();

        let config = EngineConfig::load_from_home(temp.path()).unwrap();

        assert_eq!(config.log_level(), LogLevel::Info);
        assert_eq!(config.catalog_path(), temp.path().join("catalog/vql.db"));
        assert_eq!(
            config.session_memory_limit_bytes(),
            DEFAULT_SESSION_MEMORY_LIMIT_BYTES
        );
    }

    #[test]
    fn config_file_is_strict_and_versioned() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(
            temp.path().join(CONFIG_FILE_NAME),
            "version = 2\nunknown = true\n",
        )
        .unwrap();

        let error = EngineConfig::load_from_home(temp.path()).unwrap_err();

        assert_eq!(error.code, ErrorCode::InvalidOption);
        assert!(error.message.contains("unknown field `unknown`"));

        std::fs::write(temp.path().join(CONFIG_FILE_NAME), "version = 2\n").unwrap();
        let error = EngineConfig::load_from_home(temp.path()).unwrap_err();
        assert!(
            error
                .message
                .contains("unsupported configuration version 2")
        );
    }

    #[test]
    fn config_file_rejects_invalid_memory_units() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(
            temp.path().join(CONFIG_FILE_NAME),
            "version = 1\n[kernel.session]\nmemory_limit = \"512 MB\"\n",
        )
        .unwrap();

        let error = EngineConfig::load_from_home(temp.path()).unwrap_err();

        assert_eq!(error.code, ErrorCode::InvalidOption);
        assert!(error.message.contains("kernel.session.memory_limit"));
    }

    #[test]
    fn config_file_rejects_unknown_catalog_backend() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(
            temp.path().join(CONFIG_FILE_NAME),
            "version = 1\n[catalog]\nbackend = \"postgresql\"\n",
        )
        .unwrap();

        let error = EngineConfig::load_from_home(temp.path()).unwrap_err();

        assert_eq!(error.code, ErrorCode::InvalidOption);
        assert!(error.message.contains("unknown variant `postgresql`"));
        assert!(error.message.contains("embedded"));
    }

    #[test]
    fn log_level_override_is_validated() {
        let temp = tempfile::tempdir().unwrap();
        let config = EngineConfig::load_from_home_with_log_level(
            temp.path().to_path_buf(),
            Some(OsString::from("trace")),
        )
        .unwrap();
        assert_eq!(config.log_level(), LogLevel::Trace);

        let error = EngineConfig::load_from_home_with_log_level(
            temp.path().to_path_buf(),
            Some(OsString::from("off")),
        )
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidOption);
        assert!(error.message.contains("VQL_LOG_LEVEL"));
    }

    #[test]
    fn vql_home_takes_precedence_over_home() {
        let resolved = resolve_vql_home(
            Some(OsString::from("./custom-vql")),
            Some(OsString::from("/users/example")),
        );

        assert_eq!(resolved, Path::new("./custom-vql"));
    }

    #[test]
    fn home_fallback_uses_dot_vql() {
        let resolved = resolve_vql_home(None, Some(OsString::from("/users/example")));

        assert_eq!(resolved, Path::new("/users/example/.vql"));
    }

    #[test]
    fn relative_catalog_without_parent_uses_current_directory() {
        let config = EngineConfig::new("catalog.db");

        assert_eq!(config.vql_home(), Path::new("."));
        assert_eq!(config.catalog_path(), Path::new("catalog.db"));
    }
}
