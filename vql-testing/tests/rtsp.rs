use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use arrow::array::UInt64Array;
use libtest_mimic::{Arguments, Completion, Failed, Trial};
use tempfile::tempdir;
use vql_kernel::{Engine, EngineConfig};
use vql_testing::REQUIRE_ENV;

const RTSP_URL_ENV: &str = "VQL_TEST_RTSP_URL";

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn main() {
    let mut arguments = Arguments::from_args();
    if arguments.test_threads.is_none() {
        arguments.test_threads = Some(1);
    }

    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("vql-testing has a workspace parent");
    let video = workspace.join("data/datasets/videos/sample-videos/people-detection.mp4");
    let model = workspace.join("data/models/yolo26n.onnx");
    let endpoint = std::env::var(RTSP_URL_ENV)
        .ok()
        .filter(|value| !value.is_empty());
    let mut missing = Vec::new();
    if endpoint.is_none() {
        missing.push(format!(
            "{RTSP_URL_ENV} (start MediaMTX with docker compose --profile rtsp up -d)"
        ));
    }
    if !command_available("ffmpeg", "-version") {
        missing.push("ffmpeg executable".to_owned());
    }
    if !video.is_file() {
        missing.push(format!(
            "{} (run: python scripts/fetch_datasets.py)",
            video.display()
        ));
    }
    if !model.is_file() {
        missing.push(format!(
            "{} (run: python scripts/export_yolo26.py --size n)",
            model.display()
        ));
    }
    let require_dependencies = std::env::var_os(REQUIRE_ENV).is_some();

    let trial = Trial::ignorable_test(
        "rtsp/real_stream_detects_people_from_local_video",
        move || {
            if !missing.is_empty() {
                let message = format!(
                    "missing RTSP integration dependencies:\n  {}",
                    missing.join("\n  ")
                );
                return if require_dependencies {
                    Err(Failed::from(message))
                } else {
                    Ok(Completion::ignored_with(message))
                };
            }
            run_rtsp_case(
                endpoint.as_deref().expect("endpoint was checked"),
                &video,
                &model,
            )
            .map(|()| Completion::Completed)
            .map_err(Failed::from)
        },
    );

    libtest_mimic::run(&arguments, vec![trial]).exit();
}

fn run_rtsp_case(endpoint: &str, video: &Path, model: &Path) -> Result<(), String> {
    let temp = tempdir().map_err(|error| error.to_string())?;
    let _publisher = ChildGuard(
        Command::new("ffmpeg")
            .args(["-nostdin", "-v", "error", "-re", "-stream_loop", "-1", "-i"])
            .arg(video)
            .args([
                "-an",
                "-c:v",
                "copy",
                "-f",
                "rtsp",
                "-rtsp_transport",
                "tcp",
            ])
            .arg(endpoint)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("start FFmpeg publisher: {error}"))?,
    );

    let engine = Engine::new(EngineConfig::from_home(temp.path().join("vql-home")))
        .map_err(|error| error.to_string())?;
    let session = engine
        .session()
        .build()
        .map_err(|error| error.to_string())?;
    session
        .sql(&format!(
            "CREATE STREAM people_stream FROM '{endpoint}' WITH (
               fps = 1,
               event_time = 'capture_time',
               watermark = INTERVAL '2' SECOND,
               transport = 'tcp'
             )"
        ))
        .map_err(|error| error.to_string())?;
    session
        .sql(&format!(
            "CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'file://{}' USING ONNX_RUNTIME",
            model.display()
        ))
        .map_err(|error| error.to_string())?;
    session
        .sql("RESOLVE MODEL detector")
        .map_err(|error| error.to_string())?;

    let statement = session
        .sql(
            "SELECT frame_id, CARDINALITY(IMAGE_DETECTION(
               'detector', frame, classes => ['person'], min_confidence => 0.5
             )) AS people
             FROM people_stream
             LIMIT 8",
        )
        .map_err(|error| format!("plan RTSP people detection: {error}"))?;
    let (finished, watchdog) = query_watchdog(session.clone());
    let batches = statement
        .collect()
        .map_err(|error| format!("run RTSP people detection: {error}"))?;
    let _ = finished.send(());
    watchdog
        .join()
        .map_err(|_| "query watchdog panicked".to_owned())?;

    let people = batches
        .iter()
        .flat_map(|batch| {
            let people = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("people is UInt64");
            (0..batch.num_rows())
                .map(|row| people.value(row))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    if people.len() != 8 {
        return Err(format!("expected 8 RTSP frames, found {}", people.len()));
    }
    if !people.iter().any(|count| *count > 0) {
        return Err("the RTSP scenario produced no person detections".to_owned());
    }
    let metrics = statement
        .metrics()
        .ok_or_else(|| "RTSP query has no metrics".to_owned())?;
    if metrics.inference_rows() != 8 {
        return Err(format!(
            "expected 8 inference rows, found {}",
            metrics.inference_rows()
        ));
    }
    if metrics.decode_frames() < 8 {
        return Err(format!(
            "expected at least 8 decoded frames, found {}",
            metrics.decode_frames()
        ));
    }
    if metrics.watermark_ms().is_none() {
        return Err("RTSP query did not publish a watermark".to_owned());
    }
    Ok(())
}

fn command_available(name: &str, version_arg: &str) -> bool {
    Command::new(name)
        .arg(version_arg)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn query_watchdog(session: vql_kernel::Session) -> (mpsc::Sender<()>, std::thread::JoinHandle<()>) {
    let (finished, wait) = mpsc::channel();
    let watchdog = std::thread::spawn(move || {
        if wait.recv_timeout(Duration::from_secs(180)).is_err() {
            session.cancel_active_query();
        }
    });
    (finished, watchdog)
}
