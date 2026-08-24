use libtest_mimic::{Arguments, Completion, Failed, Trial};

#[path = "support/mod.rs"]
mod support;

use support::{FixturePaths, SystemSession};

const IMAGE_SETUP_SQL: &str = include_str!("image/setup.sql");
const IMAGE_DETECT_SQL: &str = include_str!("image/detect.sql");
const IMAGE_CLASSIFY_SQL: &str = include_str!("image/classify.sql");
const MIXED_SIZE_SQL: &str = include_str!("image/mixed_size.sql");

fn main() {
    let mut arguments = Arguments::from_args();
    if arguments.test_threads.is_none() {
        arguments.test_threads = Some(1);
    }

    let fixtures = FixturePaths::from_workspace(&support::workspace_root());
    let missing = [
        support::missing_path(&fixtures.images, "python scripts/fetch_datasets.py"),
        support::missing_path(
            &fixtures.detector,
            "python scripts/export_yolo26.py --task detect --size n",
        ),
        support::missing_path(
            &fixtures.classifier,
            "python scripts/export_yolo26.py --task classify --size n",
        ),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    let trial = Trial::ignorable_test("image/real_images_run_task_shaped_inference", move || {
        if let Some(result) = support::prerequisite_result("image", &missing) {
            return result;
        }
        run_image_case(&fixtures)
            .map(|()| Completion::Completed)
            .map_err(Failed::from)
    });

    libtest_mimic::run(&arguments, vec![trial]).exit();
}

fn run_image_case(fixtures: &FixturePaths) -> Result<(), String> {
    let system = SystemSession::isolated(Some(&fixtures.detector), Some(&fixtures.classifier))?;
    let setup = IMAGE_SETUP_SQL.replace(
        "${IMAGES_LOCATION}",
        &support::escape_sql_literal(&fixtures.images.to_string_lossy()),
    );
    system
        .session
        .run_script(&setup)
        .map_err(|error| format!("set up image test table: {error}"))?;

    assert_query(&system, IMAGE_DETECT_SQL, "real-image VQL_DETECT")?;
    assert_query(&system, IMAGE_CLASSIFY_SQL, "real-image VQL_CLASSIFY")?;
    assert_query(&system, MIXED_SIZE_SQL, "mixed-size image inference")?;
    Ok(())
}

fn assert_query(system: &SystemSession, sql: &str, context: &str) -> Result<(), String> {
    if support::collect_one_bool(&system.session, sql, context)? {
        Ok(())
    } else {
        Err(format!("{context} assertions failed"))
    }
}
