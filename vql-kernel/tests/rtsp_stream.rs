use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use arrow::array::UInt64Array;
use tempfile::tempdir;
use vql_kernel::{Engine, EngineConfig};

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn real_rtsp_stream_detects_people_from_a_local_video() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("vql-kernel has a workspace parent");
    let video = workspace.join("data/datasets/videos/sample-videos/people-detection.mp4");
    let model = workspace.join("data/models/yolo26n.onnx");
    if !video.is_file()
        || !model.is_file()
        || !command_available("mediamtx", "--version")
        || !command_available("ffmpeg", "-version")
    {
        eprintln!(
            "skipping RTSP system test: mediamtx, ffmpeg, {}, and {} are required",
            video.display(),
            model.display()
        );
        return;
    }

    let temp = tempdir().expect("create RTSP integration temp directory");
    let address = reserve_address();
    let config = temp.path().join("mediamtx.yml");
    std::fs::write(
        &config,
        format!(
            "logLevel: error\n\
             rtsp: yes\n\
             rtspAddress: {address}\n\
             rtspTransports: [tcp]\n\
             rtmp: no\n\
             hls: no\n\
             webrtc: no\n\
             srt: no\n\
             paths:\n\
             \x20\x20people:\n\
             \x20\x20\x20\x20source: publisher\n"
        ),
    )
    .expect("write MediaMTX config");

    let mut server = ChildGuard(
        Command::new("mediamtx")
            .arg(&config)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start MediaMTX"),
    );
    wait_until_listening(&mut server.0, address);

    let endpoint = format!("rtsp://{address}/people");
    let _publisher = ChildGuard(
        Command::new("ffmpeg")
            .args(["-nostdin", "-v", "error", "-re", "-stream_loop", "-1", "-i"])
            .arg(&video)
            .args([
                "-an",
                "-c:v",
                "copy",
                "-f",
                "rtsp",
                "-rtsp_transport",
                "tcp",
            ])
            .arg(&endpoint)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("publish local video to MediaMTX"),
    );

    let engine = Engine::new(EngineConfig::from_home(temp.path().join("vql-home")))
        .expect("create VisionQL engine");
    let session = engine.session().build().expect("create VisionQL session");
    session
        .sql(&format!(
            "CREATE STREAM people_stream FROM '{endpoint}' WITH (
               fps = 1,
               event_time = 'capture_time',
               watermark = INTERVAL '2' SECOND,
               transport = 'tcp'
             )"
        ))
        .expect("create RTSP stream");
    session
        .sql(&format!(
            "CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'file://{}' USING ONNX_RUNTIME",
            model.display()
        ))
        .expect("create YOLO26 Model");
    session
        .sql("RESOLVE MODEL detector")
        .expect("resolve YOLO26 Model");

    let statement = session
        .sql(
            "SELECT frame_id, CARDINALITY(IMAGE_DETECTION(
               'detector', frame, classes => ['person'], min_confidence => 0.5
             )) AS people
             FROM people_stream
             LIMIT 8",
        )
        .expect("plan RTSP people detection");
    let (finished, watchdog) = query_watchdog(session.clone());
    let batches = statement.collect().expect("run RTSP people detection");
    let _ = finished.send(());
    watchdog.join().expect("join query watchdog");

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
    assert_eq!(people.len(), 8);
    assert!(
        people.iter().any(|count| *count > 0),
        "the RTSP people-detection scenario produced no person detections"
    );
    let metrics = statement.metrics().expect("query metrics");
    assert_eq!(metrics.inference_rows(), 8);
    assert!(metrics.decode_frames() >= 8);
    assert!(metrics.watermark_ms().is_some());
}

fn command_available(name: &str, version_arg: &str) -> bool {
    Command::new(name)
        .arg(version_arg)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn reserve_address() -> SocketAddr {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("reserve RTSP port");
    listener.local_addr().expect("read reserved RTSP port")
}

fn wait_until_listening(child: &mut Child, address: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_ok() {
            return;
        }
        if let Some(status) = child.try_wait().expect("inspect MediaMTX process") {
            panic!("MediaMTX stopped before accepting RTSP connections: {status}");
        }
        assert!(Instant::now() < deadline, "MediaMTX startup timed out");
        std::thread::sleep(Duration::from_millis(50));
    }
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
