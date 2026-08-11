//! SQL integration tests over the downloaded example datasets and YOLO26 model.
//!
//! Cases live under `tests/{ddl,functions,scenarios}`. A case has one main SQL statement,
//! one expected JSON file, and optional setup and teardown scripts:
//!
//! ```text
//! detect_objects.setup.sql
//! detect_objects.sql
//! detect_objects.expected.json
//! detect_objects.teardown.sql
//! ```
//!
//! See `tests/README.md` for the file format and commands.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use libtest_mimic::{Arguments, Completion, Failed, Trial};
use serde::Deserialize;
use serde_json::Value;
use tempfile::TempDir;
use vql_kernel::{Engine, EngineConfig, Session, Statement, VqlType, logical_type_of};

const REQUIRE_ENV: &str = "VQL_INTEGRATION_TEST";
const FILTER_ENV: &str = "VQL_TEST_CASE";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Expectation {
    schema: Vec<String>,
    rows: Vec<Vec<Value>>,
}

fn read_value(array: &dyn Array, row: usize) -> Result<Value, String> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    macro_rules! value {
        ($kind:ty) => {
            array
                .as_any()
                .downcast_ref::<$kind>()
                .ok_or_else(|| format!("column is not a {}", stringify!($kind)))?
                .value(row)
        };
    }
    Ok(match array.data_type() {
        arrow::datatypes::DataType::Boolean => Value::from(value!(BooleanArray)),
        arrow::datatypes::DataType::Int32 => Value::from(value!(Int32Array)),
        arrow::datatypes::DataType::Int64 => Value::from(value!(Int64Array)),
        arrow::datatypes::DataType::UInt32 => Value::from(value!(UInt32Array)),
        arrow::datatypes::DataType::UInt64 => Value::from(value!(UInt64Array)),
        arrow::datatypes::DataType::Float32 => Value::from(value!(Float32Array)),
        arrow::datatypes::DataType::Float64 => Value::from(value!(Float64Array)),
        arrow::datatypes::DataType::Utf8 => Value::from(value!(StringArray)),
        other => {
            return Err(format!(
                "unsupported result type {other}; cast the column to BOOLEAN, BIGINT, DOUBLE, or VARCHAR"
            ));
        }
    })
}

struct ResultSet {
    schema: Vec<String>,
    rows: Vec<Vec<Value>>,
}

impl ResultSet {
    fn collect(schema: &Schema, batches: &[RecordBatch]) -> Result<Self, String> {
        let schema = schema
            .fields()
            .iter()
            .map(|field| format!("{} {}", field.name(), canonical_type(field)))
            .collect::<Vec<_>>();
        let mut rows = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                let mut values = Vec::with_capacity(batch.num_columns());
                for array in batch.columns() {
                    values.push(read_value(array.as_ref(), row)?);
                }
                rows.push(values);
            }
        }
        Ok(Self { schema, rows })
    }
}

fn canonical_type(field: &Field) -> String {
    match logical_type_of(field) {
        VqlType::Image => return "IMAGE".to_owned(),
        VqlType::Video => return "VIDEO".to_owned(),
        VqlType::Box2d => return "BOX2D".to_owned(),
        VqlType::Audio => return "AUDIO".to_owned(),
        VqlType::Mask => return "MASK".to_owned(),
        _ => {}
    }
    match field.data_type() {
        DataType::Boolean => "BOOLEAN".to_owned(),
        DataType::Int8 => "INT8".to_owned(),
        DataType::Int16 => "INT16".to_owned(),
        DataType::Int32 => "INT32".to_owned(),
        DataType::Int64 => "INT64".to_owned(),
        DataType::UInt8 => "UINT8".to_owned(),
        DataType::UInt16 => "UINT16".to_owned(),
        DataType::UInt32 => "UINT32".to_owned(),
        DataType::UInt64 => "UINT64".to_owned(),
        DataType::Float16 => "FLOAT16".to_owned(),
        DataType::Float32 => "FLOAT32".to_owned(),
        DataType::Float64 => "FLOAT64".to_owned(),
        DataType::Utf8 => "UTF8".to_owned(),
        DataType::Binary => "BINARY".to_owned(),
        DataType::Timestamp(unit, timezone) => {
            let unit = match unit {
                TimeUnit::Second => "SECOND",
                TimeUnit::Millisecond => "MILLISECOND",
                TimeUnit::Microsecond => "MICROSECOND",
                TimeUnit::Nanosecond => "NANOSECOND",
            };
            timezone.as_ref().map_or_else(
                || format!("TIMESTAMP_{unit}"),
                |tz| format!("TIMESTAMP_{unit}_{tz}"),
            )
        }
        other => other.to_string().to_ascii_uppercase(),
    }
}

struct Fixtures {
    images: PathBuf,
    videos: PathBuf,
    model: PathBuf,
}

impl Fixtures {
    fn load() -> Result<Self, String> {
        let root = workspace_root();
        let fixtures = Self {
            images: root.join("data/datasets/images/coco128/images"),
            videos: root.join("data/datasets/videos/sample-videos"),
            model: root.join("data/models/yolo26n.onnx"),
        };
        let missing = [
            (&fixtures.images, "python scripts/fetch_datasets.py"),
            (&fixtures.videos, "python scripts/fetch_datasets.py"),
            (&fixtures.model, "python scripts/export_yolo26.py --size n"),
        ]
        .into_iter()
        .filter(|(path, _)| !path.exists())
        .map(|(path, command)| format!("  {} (run: {command})", path.display()))
        .collect::<Vec<_>>();

        if missing.is_empty() {
            Ok(fixtures)
        } else {
            Err(format!(
                "missing integration fixtures:\n{}",
                missing.join("\n")
            ))
        }
    }

    fn expand(&self, source: &str) -> String {
        source
            .replace("${IMAGES_LOCATION}", &self.images.to_string_lossy())
            .replace("${VIDEOS_LOCATION}", &self.videos.to_string_lossy())
            .replace("${MODEL}", &self.model.to_string_lossy())
    }
}

struct CaseEnvironment {
    session: Session,
    _engine: Engine,
    _home: TempDir,
}

impl CaseEnvironment {
    fn new() -> Result<Self, String> {
        let home = tempfile::tempdir().map_err(|error| error.to_string())?;
        let engine =
            Engine::new(EngineConfig::from_home(home.path())).map_err(|error| error.to_string())?;
        let session = engine
            .session()
            .build()
            .map_err(|error| error.to_string())?;
        Ok(Self {
            session,
            _engine: engine,
            _home: home,
        })
    }
}

fn main() {
    let mut arguments = Arguments::from_args();
    let environment_filter = arguments
        .filter
        .is_none()
        .then(|| std::env::var(FILTER_ENV).ok())
        .flatten();
    if arguments.test_threads.is_none() {
        arguments.test_threads = Some(1);
    }

    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut cases = discover(&root, "sql").expect("discover SQL files");
    let sidecars = cases
        .iter()
        .filter(|path| is_sidecar(path))
        .cloned()
        .collect::<Vec<_>>();
    cases.retain(|path| !is_sidecar(path));
    cases.sort();
    if let Some(filter) = &environment_filter {
        cases.retain(|path| case_name(&root, path).contains(filter));
    }
    let mut trials = Vec::new();
    if cases.is_empty() {
        let empty_root = root.clone();
        let message = environment_filter.map_or_else(
            || format!("no integration cases under {}", empty_root.display()),
            |filter| {
                format!(
                    "no integration cases matched {FILTER_ENV}={filter:?} under {}",
                    empty_root.display()
                )
            },
        );
        trials.push(Trial::test("case_layout", move || {
            Err(message.clone().into())
        }));
    } else {
        let layout_failures = validate_case_files(&root, &cases, &sidecars);
        if !layout_failures.is_empty() {
            trials.push(Trial::test("case_layout", move || {
                Err(format!(
                    "invalid integration case layout:\n{}",
                    layout_failures.join("\n")
                )
                .into())
            }));
        }
    }

    let fixtures = Arc::new(Fixtures::load());
    let require_fixtures = std::env::var_os(REQUIRE_ENV).is_some();
    trials.extend(cases.into_iter().map(|path| {
        let name = case_name(&root, &path);
        let fixtures = Arc::clone(&fixtures);
        Trial::ignorable_test(name, move || match fixtures.as_ref() {
            Ok(fixtures) => run_case(&path, fixtures)
                .map(|()| Completion::Completed)
                .map_err(Failed::from),
            Err(error) if require_fixtures => Err(error.clone().into()),
            Err(error) => Ok(Completion::ignored_with(error)),
        })
    }));

    libtest_mimic::run(&arguments, trials).exit();
}

fn run_case(path: &Path, fixtures: &Fixtures) -> Result<(), String> {
    let environment = CaseEnvironment::new()?;
    let setup = sidecar_path(path, "setup.sql");
    let teardown = sidecar_path(path, "teardown.sql");
    let expected = expected_path(path);
    let mut failures = Vec::new();

    let setup_ok = match run_optional_script(&environment.session, &setup, fixtures) {
        Ok(()) => true,
        Err(error) => {
            failures.push(format!("setup: {error}"));
            false
        }
    };

    if setup_ok {
        match run_main_statement(&environment.session, path, fixtures)
            .and_then(|actual| verify(&expected, &actual))
        {
            Ok(()) => {}
            Err(error) => failures.push(error),
        }
    }

    if let Err(error) = run_optional_script(&environment.session, &teardown, fixtures) {
        failures.push(format!("teardown: {error}"));
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("\n"))
    }
}

fn run_optional_script(session: &Session, path: &Path, fixtures: &Fixtures) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    let source = read_and_expand(path, fixtures)?;
    let statements = vql_kernel::split_statements(&source).map_err(|error| error.to_string())?;
    for sql in statements {
        session
            .sql(&sql)
            .and_then(|statement| statement.collect().map(|_| ()))
            .map_err(|error| format!("{}: {error}", path.display()))?;
    }
    Ok(())
}

fn run_main_statement(
    session: &Session,
    path: &Path,
    fixtures: &Fixtures,
) -> Result<ResultSet, String> {
    let source = read_and_expand(path, fixtures)?;
    let statements = vql_kernel::split_statements(&source).map_err(|error| error.to_string())?;
    if statements.len() != 1 {
        return Err(format!(
            "{} must contain exactly one statement; move initialization and cleanup to sidecar files",
            path.display()
        ));
    }
    let statement = session
        .sql(&statements[0])
        .map_err(|error| format!("planning: {error}"))?;
    let schema = statement_schema(&statement);
    let batches = statement
        .collect()
        .map_err(|error| format!("execution: {error}"))?;
    ResultSet::collect(&schema, &batches)
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
            .unwrap_or_else(|| Arc::new(Schema::empty())),
    }
}

fn verify(path: &Path, actual: &ResultSet) -> Result<(), String> {
    let expectation: Expectation = serde_json::from_str(
        &fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))?,
    )
    .map_err(|error| format!("{}: {error}", path.display()))?;
    let mut failures = Vec::new();
    if expectation.schema != actual.schema {
        failures.push(format!(
            "schema: expected {:?}, found {:?}",
            expectation.schema, actual.schema
        ));
    }
    if expectation.rows != actual.rows {
        failures.push(format!(
            "rows: expected {:?}, found {:?}",
            expectation.rows, actual.rows
        ));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("\n"))
    }
}

fn validate_case_files(root: &Path, cases: &[PathBuf], sidecars: &[PathBuf]) -> Vec<String> {
    let mut failures = Vec::new();
    for path in cases {
        let expected = expected_path(path);
        match fs::read_to_string(&expected) {
            Ok(source) => {
                if let Err(error) = serde_json::from_str::<Expectation>(&source) {
                    failures.push(format!("{}: {error}", expected.display()));
                }
            }
            Err(error) => failures.push(format!("{}: {error}", expected.display())),
        }
    }
    for path in sidecars {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        let main_name = file_name
            .strip_suffix(".setup.sql")
            .or_else(|| file_name.strip_suffix(".teardown.sql"))
            .map(|stem| format!("{stem}.sql"));
        let main = main_name.map(|name| path.with_file_name(name));
        if main.as_ref().is_none_or(|path| !path.exists()) {
            failures.push(format!(
                "orphan sidecar {} has no matching main SQL file",
                path.display()
            ));
        }
    }
    for path in discover(root, "json").unwrap_or_default() {
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".expected.json"))
        {
            let stem = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default()
                .trim_end_matches(".expected.json");
            if !path.with_file_name(format!("{stem}.sql")).exists() {
                failures.push(format!(
                    "orphan expected file {} has no matching main SQL file",
                    path.display()
                ));
            }
        }
    }
    failures
}

fn read_and_expand(path: &Path, fixtures: &Fixtures) -> Result<String, String> {
    fs::read_to_string(path)
        .map(|source| fixtures.expand(&source))
        .map_err(|error| format!("{}: {error}", path.display()))
}

fn discover(root: &Path, extension: &str) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut files = Vec::new();
    let mut directories = vec![root.to_path_buf()];
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            if path.is_dir() {
                directories.push(path);
            } else if path.extension().and_then(|value| value.to_str()) == Some(extension) {
                files.push(path);
            }
        }
    }
    Ok(files)
}

fn is_sidecar(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".setup.sql") || name.ends_with(".teardown.sql"))
}

fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    path.with_file_name(format!(
        "{}.{}",
        path.file_stem()
            .and_then(|stem| stem.to_str())
            .expect("case path is UTF-8"),
        suffix
    ))
}

fn expected_path(path: &Path) -> PathBuf {
    sidecar_path(path, "expected.json")
}

fn case_name(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .with_extension("")
        .to_string_lossy()
        .replace('\\', "/")
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("vql-kernel has a parent directory")
        .to_path_buf()
}
