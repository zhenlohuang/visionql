use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use arrow_flight::flight_service_server::FlightServiceServer;
use tempfile::tempdir;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use vql_kernel::{Engine, EngineConfig};
use vql_server::config::ServiceConfig;
use vql_server::controller::JobController;
use vql_server::flight::VqlFlightSqlService;

#[test]
fn shell_executes_through_vqld_public_flight_sql() {
    let (endpoint, shutdown, server) = start_server();
    let client_home = tempdir().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_vql"))
        .args([
            "shell",
            "--endpoint",
            &endpoint,
            "--token",
            "test-service-token",
        ])
        .env("VQL_HOME", client_home.path())
        .env("VQLD_SERVICE_TOKEN", "wrong-environment-token")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(
            b"SELECT 42 AS answer;\nSET vql.on_error = 'fail';\nCREATE FUNCTION detect USING MODEL detector;\n\\q\n",
        )
        .unwrap();
    let output = child.wait_with_output().unwrap();
    let _ = shutdown.send(());
    server.join().unwrap();

    assert!(
        output.status.success(),
        "vql shell failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(&format!("vqld {endpoint}")), "{stdout}");
    assert!(stdout.contains("answer"), "{stdout}");
    assert!(stdout.contains("42"), "{stdout}");
    assert!(stdout.contains("OK (0 rows affected)"), "{stdout}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("[VQL-42001] INVALID_SQL"), "{stderr}");
    assert!(
        !client_home.path().join("catalog/vql.db").exists(),
        "remote shell must not create or open a local Catalog"
    );
}

fn start_server() -> (
    String,
    tokio::sync::oneshot::Sender<()>,
    std::thread::JoinHandle<()>,
) {
    let (address_tx, address_rx) = std::sync::mpsc::sync_channel(1);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let home = tempdir().unwrap();
            let engine = Engine::new(EngineConfig::from_home(home.path())).unwrap();
            let config = Arc::new(ServiceConfig {
                service_token: Some("test-service-token".to_owned()),
                principal: "catalog-owner".to_owned(),
                ..ServiceConfig::default()
            });
            let controller = Arc::new(JobController::new(engine.clone(), 100, 30));
            let service = VqlFlightSqlService::new(
                engine,
                controller,
                config,
                Arc::new(AtomicBool::new(true)),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            address_tx.send(listener.local_addr().unwrap()).unwrap();
            Server::builder()
                .add_service(FlightServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });
    });
    let address = address_rx.recv().unwrap();
    (format!("http://{address}"), shutdown_tx, server)
}
