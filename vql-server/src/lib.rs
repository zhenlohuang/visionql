pub mod config;
pub mod controller;
pub mod flight;
pub mod health;
mod image_boundary;
mod instance_lock;

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow_flight::flight_service_server::FlightServiceServer;
use tokio::sync::watch;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tracing_subscriber::EnvFilter;
use vql_kernel::{Engine, EngineConfig};

use config::ServiceConfig;
use controller::JobController;
use flight::VqlFlightSqlService;
use health::HealthState;
use instance_lock::InstanceLock;

pub async fn run(config: ServiceConfig) -> Result<(), Box<dyn Error + Send + Sync>> {
    config
        .validate()
        .map_err(|message| std::io::Error::new(std::io::ErrorKind::InvalidInput, message))?;
    let engine_config = EngineConfig::load()?;
    let filter = EnvFilter::new(engine_config.log_level().as_str());
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
    let _instance_lock = InstanceLock::acquire(engine_config.vql_home())?;
    let engine = Engine::new(engine_config)?;
    let config = Arc::new(config);
    let controller = Arc::new(JobController::new(
        engine.clone(),
        config.terminal_history_count,
        config.terminal_history_days,
    ));
    controller.recover().await?;

    let ready = Arc::new(AtomicBool::new(false));
    let flight = VqlFlightSqlService::new(
        engine,
        Arc::clone(&controller),
        Arc::clone(&config),
        Arc::clone(&ready),
    );
    let health = health::router(HealthState {
        ready: Arc::clone(&ready),
        flight: flight.clone(),
        controller: Arc::clone(&controller),
        config: Arc::clone(&config),
    });

    let flight_listener = tokio::net::TcpListener::bind(config.flight_addr).await?;
    let http_listener = tokio::net::TcpListener::bind(config.http_addr).await?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut flight_shutdown = shutdown_rx.clone();
    let mut http_shutdown = shutdown_rx.clone();
    let mut maintenance_shutdown = shutdown_rx;

    let maintenance_controller = Arc::clone(&controller);
    let maintenance_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(error) = maintenance_controller.prune_history() {
                        tracing::warn!(error = %error, "failed to prune terminal Job history");
                    }
                }
                changed = maintenance_shutdown.changed() => {
                    if changed.is_err() || *maintenance_shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    });

    let mut server = Server::builder();
    if let (Some(cert), Some(key)) = (&config.tls_cert, &config.tls_key) {
        let certificate = std::fs::read(cert)?;
        let private_key = std::fs::read(key)?;
        server = server.tls_config(
            ServerTlsConfig::new().identity(Identity::from_pem(certificate, private_key)),
        )?;
    }
    let flight_service = FlightServiceServer::new(flight);
    let mut flight_task = tokio::spawn(async move {
        server
            .add_service(flight_service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(flight_listener), async move {
                let _ = flight_shutdown.changed().await;
            })
            .await
    });
    let mut http_task = tokio::spawn(async move {
        axum::serve(http_listener, health)
            .with_graceful_shutdown(async move {
                let _ = http_shutdown.changed().await;
            })
            .await
    });
    ready.store(true, Ordering::Relaxed);
    tracing::info!(
        flight_addr = %config.flight_addr,
        http_addr = %config.http_addr,
        "vqld is ready"
    );

    tokio::select! {
        signal = tokio::signal::ctrl_c() => signal?,
        result = &mut flight_task => result??,
        result = &mut http_task => result??,
    }
    ready.store(false, Ordering::Relaxed);
    controller.shutdown();
    let _ = shutdown_tx.send(true);
    if !flight_task.is_finished() {
        flight_task.await??;
    }
    if !http_task.is_finished() {
        http_task.await??;
    }
    let _ = maintenance_task.await;
    Ok(())
}
