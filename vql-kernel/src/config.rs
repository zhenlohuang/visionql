use crate::{ErrorCode, Result, SecretProviderRef, VqlError};
use std::ffi::OsString;
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};

#[derive(Clone)]
pub struct EngineConfig {
    vql_home: PathBuf,
    catalog_path: PathBuf,
    model_cache_dir: PathBuf,
    history_path: PathBuf,
    secret_provider: Option<SecretProviderRef>,
}

impl EngineConfig {
    /// Build an isolated configuration around an explicit catalog path.
    ///
    /// Hosts that only want to override the default catalog should use
    /// [`EngineConfig::default`], followed by [`EngineConfig::with_catalog_path`].
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
            catalog_path: vql_home.join("catalog/vql.db"),
            model_cache_dir: vql_home.join("cache/models"),
            history_path: vql_home.join("history"),
            secret_provider: None,
            vql_home,
        }
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

    /// Install the host-owned resolver used by credential references.
    pub fn with_secret_provider(mut self, provider: SecretProviderRef) -> Self {
        self.secret_provider = Some(provider);
        self
    }

    pub fn secret_provider(&self) -> Option<&SecretProviderRef> {
        self.secret_provider.as_ref()
    }

    pub(crate) fn prepare(&self) -> Result<()> {
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
