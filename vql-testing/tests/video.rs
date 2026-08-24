use std::process::{Command, Stdio};

use libtest_mimic::{Arguments, Completion, Failed, Trial};

#[path = "support/mod.rs"]
mod support;

use support::{FixturePaths, SystemSession};

const VIDEO_SETUP_SQL: &str = include_str!("video/setup.sql");
const VIDEO_DETECT_SQL: &str = include_str!("video/detect.sql");

fn main() {
    let mut arguments = Arguments::from_args();
    if arguments.test_threads.is_none() {
        arguments.test_threads = Some(1);
    }

    let fixtures = FixturePaths::from_workspace(&support::workspace_root());
    let mut missing = [
        support::missing_path(&fixtures.videos, "python scripts/fetch_datasets.py"),
        support::missing_path(
            &fixtures.detector,
            "python scripts/export_yolo26.py --task detect --size n",
        ),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    if !command_available("ffmpeg", "-version") {
        missing.push("ffmpeg executable".to_owned());
    }

    let trial = Trial::ignorable_test(
        "video/real_video_decodes_frames_and_runs_vql_detect",
        move || {
            if let Some(result) = support::prerequisite_result("video", &missing) {
                return result;
            }
            run_video_case(&fixtures)
                .map(|()| Completion::Completed)
                .map_err(Failed::from)
        },
    );

    libtest_mimic::run(&arguments, vec![trial]).exit();
}

fn run_video_case(fixtures: &FixturePaths) -> Result<(), String> {
    let system = SystemSession::isolated(Some(&fixtures.detector), None)?;
    let setup = VIDEO_SETUP_SQL.replace(
        "${VIDEOS_LOCATION}",
        &support::escape_sql_literal(&fixtures.videos.to_string_lossy()),
    );
    system
        .session
        .run_script(&setup)
        .map_err(|error| format!("set up video test table: {error}"))?;

    if support::collect_one_bool(&system.session, VIDEO_DETECT_SQL, "real-video VQL_DETECT")? {
        Ok(())
    } else {
        Err("real-video decoding or VQL_DETECT assertions failed".to_owned())
    }
}

fn command_available(name: &str, version_arg: &str) -> bool {
    Command::new(name)
        .arg(version_arg)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}
