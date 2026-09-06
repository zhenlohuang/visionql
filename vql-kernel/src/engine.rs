use std::ops::Deref;
use std::sync::Arc;

use tokio::runtime::Runtime;

use crate::catalog::CatalogStore;
use crate::media::MediaRuntime;
use crate::models::{BuiltinModels, ModelRuntime, PipelineRegistry};
use crate::{EngineConfig, Result, SessionBuilder};

#[derive(Debug, Clone)]
pub struct Engine {
    pub(crate) inner: Arc<EngineInner>,
}

#[derive(Debug)]
pub(crate) struct EngineInner {
    pub(crate) config: EngineConfig,
    pub(crate) runtime: Arc<EngineRuntime>,
    pub(crate) catalog: Arc<CatalogStore>,
    pub(crate) media: Arc<MediaRuntime>,
    pub(crate) pipelines: Arc<PipelineRegistry>,
    pub(crate) builtins: Arc<BuiltinModels>,
    pub(crate) models: Arc<ModelRuntime>,
}

#[derive(Debug)]
pub(crate) struct EngineRuntime(Option<Runtime>);

impl EngineRuntime {
    fn new() -> Result<Self> {
        Ok(Self(Some(Runtime::new()?)))
    }
}

impl Deref for EngineRuntime {
    type Target = Runtime;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("Engine runtime is available")
    }
}

impl Drop for EngineRuntime {
    fn drop(&mut self) {
        let Some(runtime) = self.0.take() else {
            return;
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            runtime.shutdown_background();
        } else {
            drop(runtime);
        }
    }
}

impl Engine {
    pub fn new(config: EngineConfig) -> Result<Self> {
        config.prepare()?;
        let catalog = Arc::new(CatalogStore::open(config.catalog_path())?);
        let runtime = Arc::new(EngineRuntime::new()?);
        let media = Arc::new(MediaRuntime::new());
        let pipelines = Arc::new(PipelineRegistry::builtins());
        let builtins = Arc::new(BuiltinModels::new(
            config.vql_home().join("models"),
            config.model_cache_dir().to_path_buf(),
            Arc::clone(&pipelines),
        ));
        let models = Arc::new(ModelRuntime::new(
            Arc::clone(&catalog),
            Arc::clone(&media),
            Arc::clone(&pipelines),
        ));
        Ok(Self {
            inner: Arc::new(EngineInner {
                config,
                runtime,
                catalog,
                media,
                pipelines,
                builtins,
                models,
            }),
        })
    }

    pub fn session(&self) -> SessionBuilder {
        SessionBuilder::new(self.clone())
    }

    pub fn config(&self) -> &EngineConfig {
        &self.inner.config
    }

    /// Return the shared Catalog API used by service hosts for persistent Query control.
    pub fn catalog(&self) -> Arc<CatalogStore> {
        Arc::clone(&self.inner.catalog)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn engine_runtime_can_be_released_from_an_async_host() {
        let home = tempfile::tempdir().unwrap();
        let engine = Engine::new(EngineConfig::from_home(home.path())).unwrap();
        drop(engine);
    }
}
