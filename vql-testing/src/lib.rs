//! Shared integration-test support for VisionQL frontends.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use sqllogictest::{ColumnType, DBOutput, Runner, strict_column_validator};
use tempfile::TempDir;
use thiserror::Error;
use vql_kernel::{Engine, EngineConfig, Session, Statement};

pub const FILTER_ENV: &str = "VQL_TEST_CASE";
pub const REQUIRE_ENV: &str = "VQL_INTEGRATION_TEST";

/// Column families used by VisionQL sqllogictest cases.
///
/// The one-character representation is deliberately compatible with the
/// sqllogictest format while keeping booleans distinct from integers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VqlColumnType {
    Boolean,
    Integer,
    Real,
    Text,
}

impl ColumnType for VqlColumnType {
    fn from_char(value: char) -> Option<Self> {
        match value {
            'B' => Some(Self::Boolean),
            'I' => Some(Self::Integer),
            'R' => Some(Self::Real),
            'T' => Some(Self::Text),
            _ => None,
        }
    }

    fn to_char(&self) -> char {
        match self {
            Self::Boolean => 'B',
            Self::Integer => 'I',
            Self::Real => 'R',
            Self::Text => 'T',
        }
    }
}

#[derive(Debug, Error)]
pub enum TestError {
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Kernel(#[from] vql_kernel::VqlError),
    #[error("sqllogictest query record must contain exactly one SQL statement, found {0}")]
    StatementCount(usize),
}

#[derive(Debug, Clone)]
pub struct FixturePaths {
    pub images: PathBuf,
    pub videos: PathBuf,
    pub model: PathBuf,
    pub classification_model: PathBuf,
}

impl FixturePaths {
    pub fn from_workspace(workspace: &Path) -> Self {
        Self {
            images: workspace.join("data/datasets/images/coco128/images"),
            videos: workspace.join("data/datasets/videos/sample-videos"),
            model: workspace.join("data/models/yolo26n.onnx"),
            classification_model: workspace.join("data/models/yolo26n-cls.onnx"),
        }
    }

    pub fn missing_for(&self, source: &str) -> Vec<String> {
        [
            (
                "${IMAGES_LOCATION}",
                &self.images,
                "python scripts/fetch_datasets.py",
            ),
            (
                "${VIDEOS_LOCATION}",
                &self.videos,
                "python scripts/fetch_datasets.py",
            ),
            (
                "${MODEL}",
                &self.model,
                "python scripts/export_yolo26.py --size n",
            ),
            (
                "${BUILTIN_DETECTION_MODEL}",
                &self.model,
                "python scripts/export_yolo26.py --task detect --size n",
            ),
            (
                "${BUILTIN_CLASSIFICATION_MODEL}",
                &self.classification_model,
                "python scripts/export_yolo26.py --task classify --size n",
            ),
        ]
        .into_iter()
        .filter(|(placeholder, path, _)| source.contains(placeholder) && !path.exists())
        .map(|(_, path, command)| format!("{} (run: {command})", path.display()))
        .collect()
    }
}

#[derive(Clone)]
struct EmbeddedDatabaseFactory {
    engine: Engine,
    home: Arc<TempDir>,
}

impl EmbeddedDatabaseFactory {
    fn isolated(
        builtin_detection_model: Option<&Path>,
        builtin_classification_model: Option<&Path>,
    ) -> Result<Self, TestError> {
        let home = Arc::new(tempfile::tempdir()?);
        if builtin_detection_model.is_some() || builtin_classification_model.is_some() {
            let model_dir = home.path().join("models");
            std::fs::create_dir_all(&model_dir)?;
        }
        if let Some(source) = builtin_detection_model {
            let model_dir = home.path().join("models");
            std::fs::copy(source, model_dir.join("yolo26n.onnx"))?;
        }
        if let Some(source) = builtin_classification_model {
            let model_dir = home.path().join("models");
            std::fs::copy(source, model_dir.join("yolo26n-cls.onnx"))?;
        }
        let engine = Engine::new(EngineConfig::from_home(home.path()))?;
        Ok(Self { engine, home })
    }

    fn connect(&self) -> Result<EmbeddedDatabase, TestError> {
        Ok(EmbeddedDatabase {
            session: self.engine.session().build()?,
            _home: Arc::clone(&self.home),
        })
    }
}

pub struct EmbeddedDatabase {
    session: Session,
    _home: Arc<TempDir>,
}

impl sqllogictest::DB for EmbeddedDatabase {
    type Error = TestError;
    type ColumnType = VqlColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let statements = vql_kernel::split_statements(sql)?;
        if statements.len() != 1 {
            return Err(TestError::StatementCount(statements.len()));
        }
        let statement = self.session.sql(&statements[0])?;
        let schema = statement_schema(&statement);
        let batches = statement.collect()?;
        let types = schema
            .fields()
            .iter()
            .map(|field| column_type(field.data_type()))
            .collect();
        let rows = collect_rows(&batches)?;
        Ok(DBOutput::Rows { types, rows })
    }

    fn engine_name(&self) -> &str {
        "visionql"
    }
}

/// Run one `.slt` file against a fresh embedded Engine and catalog.
pub fn run_slt_file(path: &Path, fixtures: &FixturePaths) -> Result<(), String> {
    let source = std::fs::read_to_string(path).map_err(|error| error.to_string())?;
    let builtin_detection_model = source
        .contains("${BUILTIN_DETECTION_MODEL}")
        .then_some(fixtures.model.as_path());
    let builtin_classification_model = source
        .contains("${BUILTIN_CLASSIFICATION_MODEL}")
        .then_some(fixtures.classification_model.as_path());
    let factory =
        EmbeddedDatabaseFactory::isolated(builtin_detection_model, builtin_classification_model)
            .map_err(|error| error.to_string())?;
    let mut runner = Runner::new(move || {
        let connection = factory.connect();
        async move { connection }
    });
    runner.with_normalizer(|value| value.clone());
    runner.with_column_validator(strict_column_validator);
    runner.set_var("IMAGES_LOCATION".to_owned(), sql_path(&fixtures.images));
    runner.set_var("VIDEOS_LOCATION".to_owned(), sql_path(&fixtures.videos));
    runner.set_var("MODEL".to_owned(), sql_path(&fixtures.model));
    runner.run_file(path).map_err(|error| error.to_string())
}

fn sql_path(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}

fn statement_schema(statement: &Statement) -> SchemaRef {
    match statement {
        Statement::Query(query) | Statement::Explain(query) | Statement::Set(query) => {
            query.schema()
        }
        Statement::Ddl(result) => result
            .batches()
            .first()
            .map(RecordBatch::schema)
            .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty())),
    }
}

fn column_type(data_type: &DataType) -> VqlColumnType {
    match data_type {
        DataType::Boolean => VqlColumnType::Boolean,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => VqlColumnType::Integer,
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => VqlColumnType::Real,
        _ => VqlColumnType::Text,
    }
}

fn collect_rows(batches: &[RecordBatch]) -> Result<Vec<Vec<String>>, TestError> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|array| value_to_string(array.as_ref(), row))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
    }
    Ok(rows)
}

fn value_to_string(array: &dyn Array, row: usize) -> Result<String, TestError> {
    if array.is_null(row) {
        return Ok("NULL".to_owned());
    }
    Ok(arrow::util::display::array_value_to_string(array, row)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_codes_are_strict_and_stable() {
        for (code, expected) in [
            ('B', VqlColumnType::Boolean),
            ('I', VqlColumnType::Integer),
            ('R', VqlColumnType::Real),
            ('T', VqlColumnType::Text),
        ] {
            assert_eq!(VqlColumnType::from_char(code), Some(expected));
            assert_eq!(expected.to_char(), code);
        }
        assert_eq!(VqlColumnType::from_char('?'), None);
    }
}
