use libtest_mimic::{Arguments, Completion, Failed, Trial};

#[path = "support/mod.rs"]
mod support;

use support::{FixturePaths, SystemSession};

const MODEL_SETUP_SQL: &str = include_str!("model/setup.sql");
const MODEL_INFER_SQL: &str = include_str!("model/infer.sql");

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
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    let trial = Trial::ignorable_test("model/real_onnx_model_resolves_and_executes", move || {
        if let Some(result) = support::prerequisite_result("model", &missing) {
            return result;
        }
        run_model_case(&fixtures)
            .map(|()| Completion::Completed)
            .map_err(Failed::from)
    });

    libtest_mimic::run(&arguments, vec![trial]).exit();
}

fn run_model_case(fixtures: &FixturePaths) -> Result<(), String> {
    let system = SystemSession::isolated(None, None)?;
    let setup = MODEL_SETUP_SQL
        .replace(
            "${IMAGES_LOCATION}",
            &support::escape_sql_literal(&fixtures.images.to_string_lossy()),
        )
        .replace(
            "${MODEL}",
            &support::escape_sql_literal(&fixtures.detector.to_string_lossy()),
        );
    system
        .session
        .run_script(&setup)
        .map_err(|error| format!("set up real Catalog Model: {error}"))?;

    if support::collect_one_bool(
        &system.session,
        MODEL_INFER_SQL,
        "real Catalog Model inference",
    )? {
        Ok(())
    } else {
        Err("real Catalog Model produced no person detections".to_owned())
    }
}
