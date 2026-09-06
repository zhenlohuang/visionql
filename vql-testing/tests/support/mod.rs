#![allow(dead_code)]

use std::fs;
use std::path::Path;

use arrow::array::{Array, BooleanArray};
use tempfile::TempDir;
use vql_kernel::{Engine, EngineConfig, Session};

mod system;

#[allow(unused_imports)]
pub(crate) use system::{
    FixturePaths, escape_sql_literal, missing_path, prerequisite_result, workspace_root,
};

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
