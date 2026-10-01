use clap::Parser;
use vql_workbench_backend::config::WorkbenchConfig;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "vql_workbench_backend=info,tower_http=info".into()),
        )
        .init();
    if let Err(error) = vql_workbench_backend::run(WorkbenchConfig::parse()).await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
