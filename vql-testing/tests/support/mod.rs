#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};

use arrow::array::{Array, BooleanArray};
use libtest_mimic::{Completion, Failed};
use tempfile::TempDir;
use vql_kernel::{Engine, EngineConfig, Session};

pub(crate) const REQUIRE_ENV: &str = "VQL_INTEGRATION_TEST";

#[derive(Debug, Clone)]
pub(crate) struct FixturePaths {
    pub(crate) images: PathBuf,
    pub(crate) videos: PathBuf,
    pub(crate) detector: PathBuf,
    pub(crate) classifier: PathBuf,
}

impl FixturePaths {
    pub(crate) fn from_workspace(workspace: &Path) -> Self {
        Self {
            images: workspace.join("data/datasets/images/coco128/images"),
            videos: workspace.join("data/datasets/videos/sample-videos"),
            detector: workspace.join("data/models/yolo26n.onnx"),
            classifier: workspace.join("data/models/yolo26n-cls.onnx"),
        }
    }
}

pub(crate) struct SystemSession {
    pub(crate) session: Session,
    _home: TempDir,
}

impl SystemSession {
    pub(crate) fn isolated(
        detector: Option<&Path>,
        classifier: Option<&Path>,
    ) -> Result<Self, String> {
        let home = tempfile::tempdir().map_err(|error| error.to_string())?;
        if detector.is_some() || classifier.is_some() {
            fs::create_dir_all(home.path().join("models")).map_err(|error| error.to_string())?;
        }
        if let Some(source) = detector {
            fs::copy(source, home.path().join("models/yolo26n.onnx"))
                .map_err(|error| format!("install built-in detection model: {error}"))?;
        }
        if let Some(source) = classifier {
            fs::copy(source, home.path().join("models/yolo26n-cls.onnx"))
                .map_err(|error| format!("install built-in classification model: {error}"))?;
        }
        let engine =
            Engine::new(EngineConfig::from_home(home.path())).map_err(|error| error.to_string())?;
        let session = engine
            .session()
            .build()
            .map_err(|error| error.to_string())?;
        Ok(Self {
            session,
            _home: home,
        })
    }
}

pub(crate) fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("vql-testing has a workspace parent")
        .to_path_buf()
}

pub(crate) fn missing_path(path: &Path, command: &str) -> Option<String> {
    (!path.exists()).then(|| format!("{} (run: {command})", path.display()))
}

pub(crate) fn prerequisite_result(
    boundary: &str,
    missing: &[String],
) -> Option<Result<Completion, Failed>> {
    if missing.is_empty() {
        return None;
    }
    let message = format!(
        "missing {boundary} integration dependencies:\n  {}",
        missing.join("\n  ")
    );
    Some(if std::env::var_os(REQUIRE_ENV).is_some() {
        Err(Failed::from(message))
    } else {
        Ok(Completion::ignored_with(message))
    })
}

pub(crate) fn collect_one_bool(
    session: &Session,
    sql: &str,
    context: &str,
) -> Result<bool, String> {
    let statement = session
        .sql(sql)
        .map_err(|error| format!("plan {context}: {error}"))?;
    let batches = statement
        .collect()
        .map_err(|error| format!("run {context}: {error}"))?;
    if batches.len() != 1 || batches[0].num_rows() != 1 || batches[0].num_columns() != 1 {
        return Err(format!(
            "{context} must return exactly one Boolean value, found {} batches and {} rows",
            batches.len(),
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
        ));
    }
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| format!("{context} result is not Boolean"))?;
    if values.is_null(0) {
        return Err(format!("{context} result is NULL"));
    }
    Ok(values.value(0))
}

pub(crate) fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}
