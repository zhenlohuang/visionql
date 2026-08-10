use std::sync::Arc;

use tokio::runtime::Runtime;

use crate::catalog::CatalogStore;
use crate::media::MediaRuntime;
use crate::models::{ModelRuntime, PipelineRegistry};
use crate::{EngineConfig, Result, SessionBuilder};

#[derive(Debug, Clone)]
pub struct Engine {
    pub(crate) inner: Arc<EngineInner>,
}

#[derive(Debug)]
pub(crate) struct EngineInner {
    pub(crate) config: EngineConfig,
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) catalog: Arc<CatalogStore>,
    pub(crate) media: Arc<MediaRuntime>,
    pub(crate) pipelines: Arc<PipelineRegistry>,
    pub(crate) models: Arc<ModelRuntime>,
}

impl Engine {
    pub fn new(config: EngineConfig) -> Result<Self> {
        config.prepare()?;
        let catalog = Arc::new(CatalogStore::open(config.catalog_path())?);
        let runtime = Arc::new(Runtime::new()?);
        let media = Arc::new(MediaRuntime::new());
        let pipelines = Arc::new(PipelineRegistry::builtins());
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
}
