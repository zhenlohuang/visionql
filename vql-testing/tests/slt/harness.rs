use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use libtest_mimic::{Arguments, Completion, Failed, Trial};
use sqllogictest::{ColumnType, DBOutput, Runner, strict_column_validator};
use tempfile::TempDir;
use thiserror::Error;
use vql_kernel::{Engine, EngineConfig, Session, Statement};

const FILTER_ENV: &str = "VQL_TEST_CASE";
const REQUIRE_ENV: &str = "VQL_INTEGRATION_TEST";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VqlColumnType {
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
enum TestError {
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
struct FixturePaths {
    images: PathBuf,
    videos: PathBuf,
    model: PathBuf,
    classification_model: PathBuf,
}

impl FixturePaths {
    fn from_workspace(workspace: &Path) -> Self {
        Self {
            images: workspace.join("data/datasets/images/coco128/images"),
            videos: workspace.join("data/datasets/videos/sample-videos"),
            model: workspace.join("data/models/yolo26n.onnx"),
            classification_model: workspace.join("data/models/yolo26n-cls.onnx"),
        }
    }

    fn missing_for(&self, source: &str) -> Vec<String> {
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

pub(crate) fn run(root: PathBuf, workspace: &Path) -> ! {
    let mut arguments = Arguments::from_args();
    let environment_filter = arguments
        .filter
        .is_none()
        .then(|| std::env::var(FILTER_ENV).ok())
        .flatten();
    if arguments.test_threads.is_none() {
        arguments.test_threads = Some(1);
    }

    let fixtures = FixturePaths::from_workspace(workspace);
    let mut cases = discover_slt_files(&root).expect("discover system SQL cases");
    cases.sort();
    if let Some(filter) = &environment_filter {
        cases.retain(|path| case_name(&root, path).contains(filter));
    }

    let mut trials = Vec::new();
    if cases.is_empty() {
        let message = environment_filter.map_or_else(
            || format!("no system SQL cases under {}", root.display()),
            |filter| {
                format!(
                    "no system SQL cases matched {FILTER_ENV}={filter:?} under {}",
                    root.display()
                )
            },
        );
        trials.push(Trial::test("case_layout", move || {
            Err(message.clone().into())
        }));
    }

    let require_fixtures = std::env::var_os(REQUIRE_ENV).is_some();
    trials.extend(cases.into_iter().map(|path| {
        let name = case_name(&root, &path);
        let source = fs::read_to_string(&path);
        let parse_result = source.as_ref().map_err(ToString::to_string).and_then(|_| {
            sqllogictest::parse_file::<VqlColumnType>(&path)
                .map(|_| ())
                .map_err(|error| error.to_string())
        });
        let missing = source
            .as_ref()
            .map(|source| fixtures.missing_for(source))
            .unwrap_or_default();
        let fixtures = fixtures.clone();
        Trial::ignorable_test(name, move || {
            if let Err(error) = &parse_result {
                return Err(Failed::from(format!("{}: {error}", path.display())));
            }
            if !missing.is_empty() {
                let message = format!("missing system fixtures:\n  {}", missing.join("\n  "));
                return if require_fixtures {
                    Err(Failed::from(message))
                } else {
                    Ok(Completion::ignored_with(message))
                };
            }
            run_slt_file(&path, &fixtures)
                .map(|()| Completion::Completed)
                .map_err(Failed::from)
        })
    }));

    libtest_mimic::run(&arguments, trials).exit();
}

fn discover_slt_files(root: &Path) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut files = Vec::new();
    let mut directories = vec![root.to_path_buf()];
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            if path.is_dir() {
                directories.push(path);
            } else if path.extension().and_then(|value| value.to_str()) == Some("slt") {
                files.push(path);
            }
        }
    }
    Ok(files)
}

fn case_name(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .with_extension("")
        .to_string_lossy()
        .replace('\\', "/")
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
            fs::create_dir_all(home.path().join("models"))?;
        }
        if let Some(source) = builtin_detection_model {
            fs::copy(source, home.path().join("models/yolo26n.onnx"))?;
        }
        if let Some(source) = builtin_classification_model {
            fs::copy(source, home.path().join("models/yolo26n-cls.onnx"))?;
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

struct EmbeddedDatabase {
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
        "visionql-system"
    }
}

fn run_slt_file(path: &Path, fixtures: &FixturePaths) -> Result<(), String> {
    let source = fs::read_to_string(path).map_err(|error| error.to_string())?;
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
