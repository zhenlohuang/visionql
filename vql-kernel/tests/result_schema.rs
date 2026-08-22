use arrow::datatypes::DataType;
use tempfile::tempdir;
use vql_kernel::{Engine, EngineConfig, Session, Statement};

#[test]
fn ddl_results_keep_the_kernel_owned_schema_contract() {
    let temp = tempdir().expect("create test directory");
    let images = temp.path().join("images");
    let videos = temp.path().join("videos");
    std::fs::create_dir(&images).expect("create images directory");
    std::fs::create_dir(&videos).expect("create videos directory");
    let engine =
        Engine::new(EngineConfig::from_home(temp.path().join("vql-home"))).expect("create engine");
    let session = engine.session().build().expect("create session");

    for sql in [
        format!(
            "CREATE TABLE photos USING IMAGES LOCATION '{}'",
            images.display()
        ),
        format!(
            "CREATE TABLE clips USING VIDEOS LOCATION '{}' OPTIONS (fps = 1)",
            videos.display()
        ),
        "CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://person' USING ONNX_RUNTIME"
            .to_owned(),
    ] {
        assert_ddl_result_schema(&session, &sql);
    }
}

#[cfg(feature = "ffmpeg-native")]
#[test]
fn rtsp_table_ddl_result_keeps_the_kernel_owned_schema_contract() {
    let temp = tempdir().expect("create test directory");
    let engine =
        Engine::new(EngineConfig::from_home(temp.path().join("vql-home"))).expect("create engine");
    let session = engine.session().build().expect("create session");

    assert_ddl_result_schema(
        &session,
        "CREATE TABLE entrance USING RTSP OPTIONS (\
         url = 'rtsp://127.0.0.1:8554/live', transport = 'tcp')",
    );
}

#[test]
fn sql_behavior_results_keep_names_types_and_nullability() {
    let temp = tempdir().expect("create test directory");
    let images = temp.path().join("images");
    let videos = temp.path().join("videos");
    std::fs::create_dir(&images).expect("create images directory");
    std::fs::create_dir(&videos).expect("create videos directory");
    let engine =
        Engine::new(EngineConfig::from_home(temp.path().join("vql-home"))).expect("create engine");
    let session = engine.session().build().expect("create session");
    session
        .run_script(&format!(
            "CREATE TABLE photos USING IMAGES LOCATION '{}';
             CREATE TABLE clips USING VIDEOS LOCATION '{}' OPTIONS (fps = 1);
             CREATE MODEL detector TYPE OBJECT_DETECTION
               FROM 'mock://person' USING ONNX_RUNTIME;
             RESOLVE MODEL detector;",
            images.display(),
            videos.display()
        ))
        .expect("create schema-planning fixtures");

    assert_query_schema(
        &session,
        "SELECT COUNT(*) > 0 AS found
         FROM photos,
              UNNEST(IMAGE_DETECTION(
                'detector', image, classes => ['person'], min_confidence => 0.25
              )) AS u(detection)",
        &[("found", DataType::Boolean, false)],
    );
    assert_query_schema(
        &session,
        "SELECT CAST(COUNT(*) AS BIGINT) AS windows
         FROM (
           SELECT TUMBLE(ts, INTERVAL '5' SECOND)
           FROM clips
           GROUP BY 1
         )",
        &[("windows", DataType::Int64, false)],
    );
    assert_query_schema(
        &session,
        "WITH inferred AS (
           SELECT width, IMAGE_DETECTION('detector', image) AS detections
           FROM photos
         )
         SELECT
           CAST(COUNT(*) AS BIGINT) AS images,
           COUNT(detections) = 3 AS all_inferred,
           COUNT(DISTINCT width) = 3 AS mixed_widths
         FROM inferred",
        &[
            ("images", DataType::Int64, false),
            ("all_inferred", DataType::Boolean, false),
            ("mixed_widths", DataType::Boolean, false),
        ],
    );
    assert_query_schema(
        &session,
        "WITH per_window AS (
           SELECT
             TUMBLE(ts, INTERVAL '5' SECOND) AS window_start,
             SUM(CARDINALITY(IMAGE_DETECTION(
               'detector', frame, classes => ['person'], min_confidence => 0.5
             ))) AS people
           FROM clips
           GROUP BY 1
         )
         SELECT
           CAST(COUNT(*) AS BIGINT) AS windows,
           SUM(people) > 0 AS found_people
         FROM per_window",
        &[
            ("windows", DataType::Int64, false),
            ("found_people", DataType::Boolean, true),
        ],
    );
    assert_query_schema(
        &session,
        "SELECT width, height FROM photos",
        &[
            ("width", DataType::Int32, true),
            ("height", DataType::Int32, true),
        ],
    );
}

fn assert_query_schema(session: &Session, sql: &str, expected: &[(&str, DataType, bool)]) {
    let Statement::Query(query) = session.sql(sql).expect("plan schema query") else {
        panic!("expected query statement for {sql}");
    };
    let schema = query.schema();
    let actual = schema
        .fields()
        .iter()
        .map(|field| {
            (
                field.name().as_str(),
                field.data_type().clone(),
                field.is_nullable(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "unexpected schema for {sql}");
}

fn assert_ddl_result_schema(session: &Session, sql: &str) {
    let Statement::Ddl(result) = session.sql(sql).expect("execute DDL") else {
        panic!("expected DDL result for {sql}");
    };
    let schema = result.batches()[0].schema();
    assert_eq!(schema.fields().len(), 1);
    let field = schema.field(0);
    assert_eq!(field.name(), "result");
    assert_eq!(field.data_type(), &DataType::Utf8);
    assert!(!field.is_nullable());
}
