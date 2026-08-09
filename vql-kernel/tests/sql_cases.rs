use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, BinaryArray, BinaryViewArray, FixedSizeBinaryArray, FixedSizeListArray, Float32Array,
    Float64Array, LargeBinaryArray, LargeListArray, ListArray, RecordBatchWriter, StructArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_json::writer::{JsonArray, WriterBuilder};
use image::{Rgb, RgbImage};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use vql_kernel::{Engine, EngineConfig, Statement, VqlError, VqlType, logical_type_of};

const UPDATE_ENV: &str = "VQL_UPDATE_GOLDEN";
const FILTER_ENV: &str = "VQL_SQL_CASE";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CaseOutput {
    statements: Vec<StatementOutput>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum StatementOutput {
    Ok {
        kind: StatementKind,
        message: Option<String>,
        result: Option<ResultSet>,
    },
    Error {
        error: ErrorOutput,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StatementKind {
    Ddl,
    Query,
    Explain,
    Set,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResultSet {
    schema: Vec<FieldOutput>,
    rows: Vec<Vec<Value>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FieldOutput {
    name: String,
    r#type: String,
    nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ErrorOutput {
    code: String,
    message: String,
    target_version: Option<String>,
}

struct CaseEnvironment {
    _temp: TempDir,
    test_data: PathBuf,
    vql_home: PathBuf,
    engine: Engine,
}

#[test]
fn sql_cases_match_expected_json() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/sql");
    let update = std::env::var(UPDATE_ENV).is_ok_and(|value| value == "1");
    let filter = std::env::var(FILTER_ENV).ok();
    let mut sql_files = discover_files(&root, "sql").expect("discover SQL cases");
    sql_files.sort();
    if let Some(filter) = &filter {
        sql_files.retain(|path| case_name(&root, path).contains(filter));
    }
    assert!(
        !sql_files.is_empty(),
        "no SQL cases matched {}={:?}",
        FILTER_ENV,
        filter
    );

    let mut failures = Vec::new();
    for sql_path in &sql_files {
        let name = case_name(&root, sql_path);
        let expected_path = expected_path(sql_path);
        match run_case(sql_path) {
            Ok(actual) => {
                let expected = read_expected(&expected_path);
                match expected {
                    Ok(expected) if expected == actual => {}
                    Ok(_) if update => {
                        write_expected(&expected_path, &actual).unwrap_or_else(|error| {
                            panic!("failed to update {}: {error}", expected_path.display())
                        });
                        eprintln!("updated {name}");
                    }
                    Err(_) if update => {
                        write_expected(&expected_path, &actual).unwrap_or_else(|error| {
                            panic!("failed to update {}: {error}", expected_path.display())
                        });
                        eprintln!("created {name}");
                    }
                    Ok(expected) => failures.push(format!(
                        "case {name} differs\n{}",
                        simple_diff(&expected, &actual)
                    )),
                    Err(error) => failures.push(format!(
                        "case {name} has invalid expected file {}: {error}",
                        expected_path.display()
                    )),
                }
            }
            Err(error) => failures.push(format!("case {name} failed to run: {error}")),
        }
    }

    let expected_files = discover_files(&root, "json").expect("discover expected JSON files");
    for path in expected_files {
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".expected.json"))
        {
            let sql_path = sql_path_for_expected(&path);
            if !sql_path.exists() {
                failures.push(format!(
                    "orphan expected file {} has no matching SQL case",
                    path.display()
                ));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "SQL golden failures:\n\n{}\n\nSet {UPDATE_ENV}=1 to accept intentional changes.",
        failures.join("\n\n")
    );
}

fn run_case(path: &Path) -> Result<CaseOutput, String> {
    let environment = CaseEnvironment::new()?;
    let source = fs::read_to_string(path).map_err(|error| error.to_string())?;
    let source = expand_placeholders(&source, &environment);
    let statements = vql_kernel::split_statements(&source).map_err(|error| error.to_string())?;
    let session = environment
        .engine
        .session()
        .build()
        .map_err(|error| error.to_string())?;
    let mut outputs = Vec::with_capacity(statements.len());

    for sql in statements {
        let statement = match session.sql(&sql) {
            Ok(statement) => statement,
            Err(error) => {
                outputs.push(error_output(error, &environment));
                break;
            }
        };
        let (kind, message, schema, suppress_message_batch) = match &statement {
            Statement::Ddl(result) => (
                StatementKind::Ddl,
                Some(normalize_string(&result.message, &environment)),
                result.batches().first().map(RecordBatch::schema),
                is_message_only(result),
            ),
            Statement::Query(query) => (StatementKind::Query, None, Some(query.schema()), false),
            Statement::Explain(query) => {
                (StatementKind::Explain, None, Some(query.schema()), false)
            }
            Statement::Set(query) => (StatementKind::Set, None, Some(query.schema()), false),
        };
        match statement.collect() {
            Ok(batches) => {
                let result = if suppress_message_batch {
                    None
                } else {
                    schema
                        .map(|schema| render_result(&schema, &batches, &environment))
                        .transpose()?
                };
                outputs.push(StatementOutput::Ok {
                    kind,
                    message,
                    result,
                });
            }
            Err(error) => {
                outputs.push(error_output(error, &environment));
                break;
            }
        }
    }
    Ok(CaseOutput {
        statements: outputs,
    })
}

impl CaseEnvironment {
    fn new() -> Result<Self, String> {
        let temp = tempfile::tempdir().map_err(|error| error.to_string())?;
        let test_data = temp.path().join("data");
        let images = test_data.join("images");
        let broken_images = test_data.join("broken-images");
        fs::create_dir_all(&images).map_err(|error| error.to_string())?;
        fs::create_dir_all(&broken_images).map_err(|error| error.to_string())?;
        RgbImage::from_pixel(120, 80, Rgb([4, 5, 6]))
            .save(images.join("a.jpg"))
            .map_err(|error| error.to_string())?;
        RgbImage::from_pixel(200, 100, Rgb([1, 2, 3]))
            .save(images.join("b.png"))
            .map_err(|error| error.to_string())?;
        fs::write(broken_images.join("broken.png"), b"not an image")
            .map_err(|error| error.to_string())?;
        let test_data = test_data
            .canonicalize()
            .map_err(|error| error.to_string())?;
        let vql_home = temp.path().join(".vql");
        let engine =
            Engine::new(EngineConfig::from_home(&vql_home)).map_err(|error| error.to_string())?;
        let vql_home = vql_home.canonicalize().map_err(|error| error.to_string())?;
        Ok(Self {
            _temp: temp,
            test_data,
            vql_home,
            engine,
        })
    }
}

fn render_result(
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
    environment: &CaseEnvironment,
) -> Result<ResultSet, String> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| FieldOutput {
            name: field.name().clone(),
            r#type: canonical_field_type(field),
            nullable: field.is_nullable(),
        })
        .collect();
    let mut rows = Vec::new();
    for batch in batches {
        let raw_rows = batch_to_json_rows(batch)?;
        for (row_index, raw_row) in raw_rows.into_iter().enumerate() {
            let object = raw_row
                .as_object()
                .ok_or_else(|| "Arrow JSON writer returned a non-object row".to_owned())?;
            let mut row = Vec::with_capacity(batch.num_columns());
            for (column_index, (field, array)) in batch
                .schema()
                .fields()
                .iter()
                .zip(batch.columns())
                .enumerate()
            {
                let raw = object
                    .get(&column_index.to_string())
                    .cloned()
                    .unwrap_or(Value::Null);
                let value = canonical_value(field, array.as_ref(), row_index, raw)?;
                row.push(normalize_value(value, environment));
            }
            rows.push(row);
        }
    }
    Ok(ResultSet {
        schema: fields,
        rows,
    })
}

fn batch_to_json_rows(batch: &RecordBatch) -> Result<Vec<Value>, String> {
    let fields = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            Arc::new(
                Field::new(
                    index.to_string(),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
                .with_metadata(field.metadata().clone()),
            )
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    let renamed = RecordBatch::try_new(schema, batch.columns().to_vec())
        .map_err(|error| error.to_string())?;
    let mut bytes = Vec::new();
    {
        let mut writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, JsonArray>(&mut bytes);
        writer.write(&renamed).map_err(|error| error.to_string())?;
        writer.close().map_err(|error| error.to_string())?;
    }
    serde_json::from_slice(&bytes).map_err(|error| error.to_string())
}

fn canonical_value(
    field: &Field,
    array: &dyn Array,
    row: usize,
    raw: Value,
) -> Result<Value, String> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    if matches!(logical_type_of(field), VqlType::Image) {
        return canonical_image(raw);
    }
    match field.data_type() {
        DataType::Float32 => {
            let value = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "FLOAT32 field is not a Float32Array".to_owned())?
                .value(row);
            Ok(canonical_float(f64::from(value), raw))
        }
        DataType::Float64 => {
            let value = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "FLOAT64 field is not a Float64Array".to_owned())?
                .value(row);
            Ok(canonical_float(value, raw))
        }
        DataType::Binary => canonical_binary(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "BINARY field is not a BinaryArray".to_owned())?
                .value(row),
        ),
        DataType::LargeBinary => canonical_binary(
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| "LARGE_BINARY field is not a LargeBinaryArray".to_owned())?
                .value(row),
        ),
        DataType::BinaryView => canonical_binary(
            array
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .ok_or_else(|| "BINARY_VIEW field is not a BinaryViewArray".to_owned())?
                .value(row),
        ),
        DataType::FixedSizeBinary(_) => canonical_binary(
            array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "FIXED_SIZE_BINARY field is not a FixedSizeBinaryArray".to_owned())?
                .value(row),
        ),
        DataType::Struct(fields) => {
            let values = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| "STRUCT field is not a StructArray".to_owned())?;
            let raw = raw.as_object().cloned().unwrap_or_default();
            let mut output = Map::new();
            for (index, child) in fields.iter().enumerate() {
                let child_raw = raw.get(child.name()).cloned().unwrap_or(Value::Null);
                output.insert(
                    child.name().clone(),
                    canonical_value(child, values.column(index).as_ref(), row, child_raw)?,
                );
            }
            Ok(Value::Object(output))
        }
        DataType::List(child) => {
            let values = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| "LIST field is not a ListArray".to_owned())?
                .value(row);
            canonical_list(child, values.as_ref(), raw)
        }
        DataType::LargeList(child) => {
            let values = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| "LARGE_LIST field is not a LargeListArray".to_owned())?
                .value(row);
            canonical_list(child, values.as_ref(), raw)
        }
        DataType::FixedSizeList(child, _) => {
            let values = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| "FIXED_SIZE_LIST field is not a FixedSizeListArray".to_owned())?
                .value(row);
            canonical_list(child, values.as_ref(), raw)
        }
        _ => Ok(raw),
    }
}

fn canonical_list(field: &Field, values: &dyn Array, raw: Value) -> Result<Value, String> {
    let raw = raw.as_array().cloned().unwrap_or_default();
    (0..values.len())
        .map(|index| {
            canonical_value(
                field,
                values,
                index,
                raw.get(index).cloned().unwrap_or(Value::Null),
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
}

fn canonical_float(value: f64, finite: Value) -> Value {
    if value.is_nan() {
        json!({"$float": "NaN"})
    } else if value == f64::INFINITY {
        json!({"$float": "Infinity"})
    } else if value == f64::NEG_INFINITY {
        json!({"$float": "-Infinity"})
    } else {
        finite
    }
}

fn canonical_binary(value: &[u8]) -> Result<Value, String> {
    Ok(json!({
        "$binary": {
            "length": value.len(),
            "sha256": format!("{:x}", Sha256::digest(value)),
        }
    }))
}

fn canonical_image(raw: Value) -> Result<Value, String> {
    let raw = raw
        .as_object()
        .ok_or_else(|| "IMAGE JSON value is not an object".to_owned())?;
    let mut image = Map::new();
    for key in ["uri", "pts_ms", "frame_id", "width", "height"] {
        if let Some(value) = raw.get(key).filter(|value| !value.is_null()) {
            image.insert(key.to_owned(), value.clone());
        }
    }
    let format = raw
        .get("encoding")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            raw.get("uri")
                .and_then(Value::as_str)
                .and_then(|uri| Path::new(uri).extension())
                .and_then(|extension| extension.to_str())
                .map(|extension| match extension.to_ascii_lowercase().as_str() {
                    "jpg" => "jpeg".to_owned(),
                    other => other.to_owned(),
                })
        });
    if let Some(format) = format {
        image.insert("format".to_owned(), Value::String(format));
    }
    Ok(json!({"$image": image}))
}

fn canonical_field_type(field: &Field) -> String {
    match logical_type_of(field) {
        VqlType::Image => "IMAGE".to_owned(),
        VqlType::Video => "VIDEO".to_owned(),
        VqlType::Box2d => "BOX2D".to_owned(),
        VqlType::Audio => "AUDIO".to_owned(),
        VqlType::Mask => "MASK".to_owned(),
        VqlType::Arrow => canonical_data_type(field.data_type()),
        _ => canonical_data_type(field.data_type()),
    }
}

fn canonical_data_type(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "NULL".to_owned(),
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
        DataType::LargeUtf8 => "LARGE_UTF8".to_owned(),
        DataType::Utf8View => "UTF8_VIEW".to_owned(),
        DataType::Binary => "BINARY".to_owned(),
        DataType::LargeBinary => "LARGE_BINARY".to_owned(),
        DataType::BinaryView => "BINARY_VIEW".to_owned(),
        DataType::FixedSizeBinary(size) => format!("FIXED_SIZE_BINARY<{size}>"),
        DataType::Date32 => "DATE32".to_owned(),
        DataType::Date64 => "DATE64".to_owned(),
        DataType::Timestamp(unit, timezone) => match timezone {
            Some(timezone) => format!("TIMESTAMP_{}_{}", time_unit(unit), timezone),
            None => format!("TIMESTAMP_{}", time_unit(unit)),
        },
        DataType::Time32(unit) => format!("TIME32_{}", time_unit(unit)),
        DataType::Time64(unit) => format!("TIME64_{}", time_unit(unit)),
        DataType::Duration(unit) => format!("DURATION_{}", time_unit(unit)),
        DataType::List(field) => format!("LIST<{}>", canonical_field_type(field)),
        DataType::LargeList(field) => format!("LARGE_LIST<{}>", canonical_field_type(field)),
        DataType::FixedSizeList(field, size) => {
            format!("FIXED_SIZE_LIST<{},{}>", canonical_field_type(field), size)
        }
        DataType::Struct(fields) => format!(
            "STRUCT<{}>",
            fields
                .iter()
                .map(|field| format!(
                    "{}:{}{}",
                    field.name(),
                    canonical_field_type(field),
                    if field.is_nullable() { "?" } else { "!" }
                ))
                .collect::<Vec<_>>()
                .join(",")
        ),
        DataType::Dictionary(_, value) => canonical_data_type(value),
        DataType::Decimal32(precision, scale) => format!("DECIMAL32({precision},{scale})"),
        DataType::Decimal64(precision, scale) => format!("DECIMAL64({precision},{scale})"),
        DataType::Decimal128(precision, scale) => format!("DECIMAL128({precision},{scale})"),
        DataType::Decimal256(precision, scale) => format!("DECIMAL256({precision},{scale})"),
        other => other.to_string().to_ascii_uppercase(),
    }
}

fn time_unit(unit: &TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "SECOND",
        TimeUnit::Millisecond => "MILLISECOND",
        TimeUnit::Microsecond => "MICROSECOND",
        TimeUnit::Nanosecond => "NANOSECOND",
    }
}

fn is_message_only(result: &vql_kernel::DdlResult) -> bool {
    let batches = result.batches();
    if batches.len() != 1 || batches[0].num_columns() != 1 || batches[0].num_rows() != 1 {
        return false;
    }
    let field = batches[0].schema().field(0).clone();
    field.name() == "result"
        && batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .is_some_and(|values| values.value(0) == result.message)
}

fn error_output(error: VqlError, environment: &CaseEnvironment) -> StatementOutput {
    StatementOutput::Error {
        error: ErrorOutput {
            code: error.code.as_str().to_owned(),
            message: normalize_string(&error.message, environment),
            target_version: error.target_version,
        },
    }
}

fn expand_placeholders(source: &str, environment: &CaseEnvironment) -> String {
    source
        .replace("${TEST_DATA}", &environment.test_data.to_string_lossy())
        .replace("${VQL_HOME}", &environment.vql_home.to_string_lossy())
}

fn normalize_value(value: Value, environment: &CaseEnvironment) -> Value {
    match value {
        Value::String(value) => Value::String(normalize_string(&value, environment)),
        Value::Array(values) => Value::Array(
            values
                .into_iter()
                .map(|value| normalize_value(value, environment))
                .collect(),
        ),
        Value::Object(values) => Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, normalize_value(value, environment)))
                .collect(),
        ),
        value => value,
    }
}

fn normalize_string(value: &str, environment: &CaseEnvironment) -> String {
    let test_data = environment.test_data.to_string_lossy();
    let vql_home = environment.vql_home.to_string_lossy();
    value
        .replace(test_data.as_ref(), "${TEST_DATA}")
        .replace(vql_home.as_ref(), "${VQL_HOME}")
}

fn discover_files(root: &Path, extension: &str) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut output = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().and_then(|value| value.to_str()) == Some(extension) {
                output.push(path);
            }
        }
    }
    Ok(output)
}

fn expected_path(sql_path: &Path) -> PathBuf {
    sql_path.with_extension("expected.json")
}

fn sql_path_for_expected(expected_path: &Path) -> PathBuf {
    let name = expected_path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("expected path is UTF-8")
        .trim_end_matches(".expected.json");
    expected_path.with_file_name(format!("{name}.sql"))
}

fn case_name(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .with_extension("")
        .to_string_lossy()
        .replace('\\', "/")
}

fn read_expected(path: &Path) -> Result<CaseOutput, String> {
    let source = fs::read_to_string(path).map_err(|error| error.to_string())?;
    serde_json::from_str(&source).map_err(|error| error.to_string())
}

fn write_expected(path: &Path, actual: &CaseOutput) -> Result<(), String> {
    let mut source = serde_json::to_string_pretty(actual).map_err(|error| error.to_string())?;
    source.push('\n');
    fs::write(path, source).map_err(|error| error.to_string())
}

fn simple_diff(expected: &CaseOutput, actual: &CaseOutput) -> String {
    let expected = serde_json::to_string_pretty(expected).expect("serialize expected output");
    let actual = serde_json::to_string_pretty(actual).expect("serialize actual output");
    let expected_lines = expected.lines().collect::<Vec<_>>();
    let actual_lines = actual.lines().collect::<Vec<_>>();
    let mut output = String::from("--- expected\n+++ actual\n");
    let line_count = expected_lines.len().max(actual_lines.len());
    for index in 0..line_count {
        match (expected_lines.get(index), actual_lines.get(index)) {
            (Some(expected), Some(actual)) if expected == actual => {
                output.push_str(&format!(" {expected}\n"));
            }
            (Some(expected), Some(actual)) => {
                output.push_str(&format!("-{expected}\n+{actual}\n"));
            }
            (Some(expected), None) => output.push_str(&format!("-{expected}\n")),
            (None, Some(actual)) => output.push_str(&format!("+{actual}\n")),
            (None, None) => {}
        }
    }
    output
}

#[test]
fn expected_json_rejects_unknown_fields() {
    let error = serde_json::from_value::<CaseOutput>(json!({
        "statements": [],
        "typo": true
    }))
    .unwrap_err();
    assert!(error.to_string().contains("unknown field"));
}

#[test]
fn result_rows_remain_arrays_when_columns_have_duplicate_names() {
    let fields = vec![
        Arc::new(Field::new("value", DataType::Int64, false)),
        Arc::new(Field::new("value", DataType::Int64, false)),
    ];
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(arrow::array::Int64Array::from(vec![1])),
            Arc::new(arrow::array::Int64Array::from(vec![2])),
        ],
    )
    .unwrap();
    let environment = CaseEnvironment::new().unwrap();
    let output = render_result(&batch.schema(), &[batch], &environment).unwrap();
    assert_eq!(output.rows, vec![vec![json!(1), json!(2)]]);
}

#[test]
fn canonical_special_values_are_valid_json() {
    assert_eq!(
        canonical_float(f64::NAN, Value::Null),
        json!({"$float": "NaN"})
    );
    assert_eq!(
        canonical_float(f64::INFINITY, Value::Null),
        json!({"$float": "Infinity"})
    );
    let binary = canonical_binary(b"abc").unwrap();
    assert_eq!(binary["$binary"]["length"], 3);
    assert_eq!(
        binary["$binary"]["sha256"],
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}
