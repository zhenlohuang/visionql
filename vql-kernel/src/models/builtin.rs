use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

use super::{PipelineRegistry, canonical_model_options, semantic_fingerprint};
use crate::catalog::{ModelVersion, ResolvedModelDef};
use crate::{ErrorCode, Result, VqlError};

const YOLO26_DETECTOR_NAME: &str = "vql.builtin.yolo26n";
const YOLO26_CLASSIFIER_NAME: &str = "vql.builtin.yolo26n-cls";
const YOLO26_MODEL_VERSION: &str = "v0.1";
const YOLO26_DETECTOR_FILE: &str = "yolo26n.onnx";
const YOLO26_CLASSIFIER_FILE: &str = "yolo26n-cls.onnx";

#[derive(Debug)]
pub(crate) struct BuiltinModels {
    model_dir: PathBuf,
    cache_dir: PathBuf,
    pipelines: Arc<PipelineRegistry>,
    detector: OnceCell<ResolvedModelDef>,
    classifier: OnceCell<ResolvedModelDef>,
}

impl BuiltinModels {
    pub(crate) fn new(
        model_dir: PathBuf,
        cache_dir: PathBuf,
        pipelines: Arc<PipelineRegistry>,
    ) -> Self {
        Self {
            model_dir,
            cache_dir,
            pipelines,
            detector: OnceCell::new(),
            classifier: OnceCell::new(),
        }
    }

    pub(crate) fn detector_path(&self) -> PathBuf {
        self.model_dir.join(YOLO26_DETECTOR_FILE)
    }

    pub(crate) fn classifier_path(&self) -> PathBuf {
        self.model_dir.join(YOLO26_CLASSIFIER_FILE)
    }

    pub(crate) async fn detector(&self, cancel: CancellationToken) -> Result<ResolvedModelDef> {
        self.detector
            .get_or_try_init(|| async {
                let interface = crate::models::object_detection_interface();
                let source = self.detector_path().to_string_lossy().into_owned();
                let options = BTreeMap::from([
                    ("image_size".to_owned(), serde_json::json!(640)),
                    ("format".to_owned(), serde_json::json!("yolo_e2e")),
                    ("labels".to_owned(), serde_json::json!("coco80")),
                    ("box_format".to_owned(), serde_json::json!("xyxy")),
                ]);
                let declaration_fingerprint = semantic_fingerprint(&(
                    &interface,
                    YOLO26_MODEL_VERSION,
                    &source,
                    "onnx-runtime",
                    canonical_model_options(&options),
                ));
                let version = ModelVersion {
                    name: YOLO26_MODEL_VERSION.to_owned(),
                    source: source.clone(),
                    runtime_kind: "onnx-runtime".to_owned(),
                    options,
                    declaration_fingerprint,
                    resolved: None,
                    created_at: 0,
                };
                let resolved = self
                    .pipelines
                    .resolve_version(&interface, &version, &self.cache_dir, cancel)
                    .await
                    .map_err(|error| missing_builtin_artifact(error, &source, "detect"))?;
                Ok(ResolvedModelDef {
                    name: YOLO26_DETECTOR_NAME.to_owned(),
                    version: YOLO26_MODEL_VERSION.to_owned(),
                    interface,
                    source,
                    resolved_source: resolved.resolved_source,
                    artifact_hash: resolved.artifact_hash,
                    execution: resolved.execution,
                    semantic_fingerprint: resolved.semantic_fingerprint,
                    volatile: resolved.volatile,
                })
            })
            .await
            .cloned()
    }

    pub(crate) async fn classifier(&self, cancel: CancellationToken) -> Result<ResolvedModelDef> {
        self.classifier
            .get_or_try_init(|| async {
                let interface = crate::models::image_classification_interface();
                let source = self.classifier_path().to_string_lossy().into_owned();
                let options = BTreeMap::from([
                    ("image_size".to_owned(), serde_json::json!(224)),
                    ("format".to_owned(), serde_json::json!("classification")),
                    ("resize".to_owned(), serde_json::json!("center_crop")),
                ]);
                let declaration_fingerprint = semantic_fingerprint(&(
                    &interface,
                    YOLO26_MODEL_VERSION,
                    &source,
                    "onnx-runtime",
                    canonical_model_options(&options),
                ));
                let version = ModelVersion {
                    name: YOLO26_MODEL_VERSION.to_owned(),
                    source: source.clone(),
                    runtime_kind: "onnx-runtime".to_owned(),
                    options,
                    declaration_fingerprint,
                    resolved: None,
                    created_at: 0,
                };
                let resolved = self
                    .pipelines
                    .resolve_version(&interface, &version, &self.cache_dir, cancel)
                    .await
                    .map_err(|error| missing_builtin_artifact(error, &source, "classify"))?;
                Ok(ResolvedModelDef {
                    name: YOLO26_CLASSIFIER_NAME.to_owned(),
                    version: YOLO26_MODEL_VERSION.to_owned(),
                    interface,
                    source,
                    resolved_source: resolved.resolved_source,
                    artifact_hash: resolved.artifact_hash,
                    execution: resolved.execution,
                    semantic_fingerprint: resolved.semantic_fingerprint,
                    volatile: resolved.volatile,
                })
            })
            .await
            .cloned()
    }
}

fn missing_builtin_artifact(error: VqlError, source: &str, task: &str) -> VqlError {
    if error.code == ErrorCode::InvalidLocation && !Path::new(source).is_file() {
        VqlError::new(
            ErrorCode::InvalidLocation,
            format!(
                "built-in YOLO26 artifact is not installed at '{source}'; run scripts/export_yolo26.py --task {task} --install"
            ),
        )
        .with_source(error)
    } else {
        error
    }
}
