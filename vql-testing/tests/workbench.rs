use std::process::Command;

use libtest_mimic::{Arguments, Completion, Failed, Trial};

#[path = "support/system.rs"]
mod support;

fn main() {
    let arguments = Arguments::from_args();
    let workspace = support::workspace_root();
    let fixtures = support::FixturePaths::from_workspace(&workspace);
    let missing = [
        support::missing_path(
            &fixtures.images.join("000000000049.jpg"),
            "python scripts/fetch_datasets.py",
        ),
        support::missing_path(
            &fixtures.detector,
            "python scripts/export_yolo26.py --task detect --size n",
        ),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    let trial = Trial::ignorable_test("workbench/python_notebook_visual_parity", move || {
        if let Some(result) = support::prerequisite_result("workbench", &missing) {
            return result;
        }
        let status = Command::new("bash")
            .arg(workspace.join("vql-workbench/scripts/run-e2e.sh"))
            .args(["--visual-parity", "visual-parity.spec.ts"])
            .current_dir(&workspace)
            .status()
            .map_err(|error| Failed::from(format!("start Workbench acceptance: {error}")))?;
        if status.success() {
            Ok(Completion::Completed)
        } else {
            Err(Failed::from(format!(
                "Python/Workbench visual acceptance failed: {status}"
            )))
        }
    });

    libtest_mimic::run(&arguments, vec![trial]).exit();
}
