#![allow(dead_code)]

use std::path::{Path, PathBuf};

use libtest_mimic::{Completion, Failed};

pub(crate) const REQUIRE_ENV: &str = "VQL_SYSTEM_TEST";

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
        "missing {boundary} system-test dependencies:\n  {}",
        missing.join("\n  ")
    );
    Some(if std::env::var_os(REQUIRE_ENV).is_some() {
        Err(Failed::from(message))
    } else {
        Ok(Completion::ignored_with(message))
    })
}

pub(crate) fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}
