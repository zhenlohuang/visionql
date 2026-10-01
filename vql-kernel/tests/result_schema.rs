use std::collections::BTreeMap;

use arrow::datatypes::{DataType, TimeUnit};
use tempfile::tempdir;
use vql_kernel::{
    Engine, EngineConfig, ErrorCode, PersistentCommand, QueryMode, ResultMode, Session, Statement,
    StatementKind,
};

#[test]
fn show_jobs_keeps_the_persistent_listing_schema_and_service_host_boundary() {
    let temp = tempdir().expect("create test directory");
    let engine =
        Engine::new(EngineConfig::from_home(temp.path().join("vql-home"))).expect("create engine");
    let session = engine.session().build().expect("create session");
    let prepared = session
        .prepare("SHOW JOBS", "service", BTreeMap::new())
        .expect("prepare job listing");
    let info = prepared.statement_info();
    assert_eq!(info.kind, StatementKind::Query);
    assert_eq!(info.query_mode, QueryMode::Bounded);
    assert_eq!(info.result_mode, ResultMode::Bounded);
    assert_eq!(
        prepared.persistent_command(),
        Some(&PersistentCommand::Show)
    );
    assert!(prepared.definition_generations().is_empty());
    let schema = prepared.result_schema();
    let fields = schema
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
    let timestamp = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
    assert_eq!(
        fields,
        [
            ("job_id", DataType::Utf8, false),
            ("name", DataType::Utf8, false),
            ("state", DataType::Utf8, false),
            ("source_health", DataType::Utf8, true),
            ("last_event_time", timestamp.clone(), true),
            ("started_at", timestamp.clone(), true),
            ("updated_at", timestamp, false),
            ("restart_gap_count", DataType::Int64, false),
            ("error_code", DataType::Utf8, true),
            ("error_message", DataType::Utf8, true),
        ]
    );
    let error = session
        .sql("SHOW JOBS")
        .expect_err("embedded SQL requires a service host");
    assert_eq!(error.code, ErrorCode::InvalidArgument);
    assert_eq!(
        error.message,
        "persistent Job statements require the vqld service host"
    );
}

#[test]
fn job_lifecycle_preparation_uses_job_identity_and_bounded_control_results() {
    let temp = tempdir().expect("create test directory");
    let engine =
        Engine::new(EngineConfig::from_home(temp.path().join("vql-home"))).expect("create engine");
    let session = engine.session().build().expect("create session");
    session
        .run_script(
            "CREATE TABLE camera USING RTSP OPTIONS (
                url = 'rtsp://127.0.0.1:1/main', fps = 1,
                event_time = 'capture_time', watermark = '2 seconds', transport = 'tcp'
            );
            CREATE TABLE sink (frame_id BIGINT) USING KAFKA OPTIONS (
                bootstrap_servers = '127.0.0.1:1', topic = 'job-contract'
            );",
        )
        .expect("declare job source and sink without starting execution");
    for (sql, command, kind, mode) in [
        (
            "SUBMIT JOB people AS INSERT INTO sink SELECT frame_id FROM camera",
            PersistentCommand::Submit {
                name: "people".to_owned(),
                sql: "INSERT INTO sink SELECT frame_id FROM camera".to_owned(),
            },
            StatementKind::PersistentSubmission,
            QueryMode::Unbounded,
        ),
        (
            "DESCRIBE JOB 'job-id'",
            PersistentCommand::Describe {
                job_id: "job-id".to_owned(),
            },
            StatementKind::Query,
            QueryMode::Bounded,
        ),
        (
            "STOP JOB 'job-id'",
            PersistentCommand::Stop {
                job_id: "job-id".to_owned(),
            },
            StatementKind::Query,
            QueryMode::Bounded,
        ),
    ] {
        let prepared = session
            .prepare(sql, "service", BTreeMap::new())
            .expect("prepare job lifecycle command");
        assert_eq!(prepared.persistent_command(), Some(&command));
        let info = prepared.statement_info();
        assert_eq!(info.kind, kind);
        assert_eq!(info.query_mode, mode);
        assert_eq!(info.result_mode, ResultMode::Bounded);
        let schema = prepared.result_schema();
        let identity = schema.field(0);
        assert_eq!(identity.name(), "job_id");
        assert_eq!(identity.data_type(), &DataType::Utf8);
        assert!(!identity.is_nullable());
        assert!(schema.field_with_name("query_id").is_err());
        assert_eq!(
            session.sql(sql).expect_err("requires service host").code,
            ErrorCode::InvalidArgument
        );
    }
    assert!(engine.catalog().list_jobs().unwrap().is_empty());
}

#[test]
fn show_create_model_returns_its_actual_version_in_direct_and_prepared_results() {
    let temp = tempdir().expect("create test directory");
    let engine =
        Engine::new(EngineConfig::from_home(temp.path().join("vql-home"))).expect("create engine");
    let session = engine.session().build().expect("create session");
    session
        .run_script(
            "CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://person';
         CREATE FUNCTION plus_one(BIGINT) RETURNS BIGINT RETURN $1 + 1;",
        )
        .expect("create catalog objects");
    for (sql, has_version) in [
        ("SHOW CREATE MODEL detector", true),
        ("SHOW CREATE MODEL detector VERSION 'v1'", true),
        ("SHOW CREATE FUNCTION plus_one", false),
    ] {
        let prepared = session
            .prepare(sql, "service", BTreeMap::new())
            .expect("prepare SHOW CREATE");
        let batches = session
            .sql(sql)
            .expect("execute SHOW CREATE")
            .collect()
            .expect("collect SHOW CREATE");
        let mut expected = vec![
            ("object_name", DataType::Utf8, false),
            ("object_type", DataType::Utf8, false),
            ("create_sql", DataType::Utf8, false),
        ];
        if has_version {
            expected.push(("version", DataType::Utf8, false));
        }
        for schema in [prepared.result_schema(), batches[0].schema()] {
            let fields = schema
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
            assert_eq!(fields, expected, "unexpected schema for {sql}");
        }
    }
}

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
              UNNEST(detector(image, classes => ['person'], min_confidence => 0.25
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
           SELECT width, detector(image) AS detections
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
             SUM(CARDINALITY(detector(frame, classes => ['person'], min_confidence => 0.5
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
