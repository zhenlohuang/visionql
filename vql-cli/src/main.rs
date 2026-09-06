mod backend;
mod commands;
mod render;

use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;
use vql_kernel::{Engine, EngineConfig, ErrorCode, Result, VqlError};

use crate::backend::{EmbeddedBackend, FlightBackend, ShellBackend};

#[derive(Debug, Parser)]
#[command(
    name = "vql",
    version,
    about = "Visual data SQL shell and script runner"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Open an interactive SQL shell.
    Shell {
        /// Connect to a vqld Flight SQL URI instead of using the embedded engine.
        #[arg(long, value_name = "URI")]
        endpoint: Option<String>,

        /// Service token for the vqld Flight SQL handshake.
        #[arg(long, value_name = "TOKEN", requires = "endpoint")]
        token: Option<String>,

        /// Trust an additional PEM CA certificate for an HTTPS vqld endpoint.
        #[arg(long, value_name = "PEM", requires = "endpoint")]
        tls_ca: Option<PathBuf>,
    },
    /// Run every statement in a SQL script in order.
    Run { script: PathBuf },
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    let config = EngineConfig::load()?;
    let filter = EnvFilter::new(config.log_level().as_str());
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
    match cli.command {
        Command::Shell {
            endpoint,
            token,
            tls_ca,
        } => {
            let history_path = config.history_path().to_path_buf();
            let backend: Arc<dyn ShellBackend> = if let Some(endpoint) = endpoint {
                let token = token
                    .or(optional_env("VQLD_SERVICE_TOKEN")?)
                    .unwrap_or_default();
                Arc::new(FlightBackend::connect(endpoint, token, tls_ca.as_deref())?)
            } else {
                let engine = Engine::new(config)?;
                Arc::new(EmbeddedBackend::new(engine.session().build()?))
            };
            commands::shell::run(backend, &history_path)
        }
        Command::Run { script } => {
            let engine = Engine::new(config)?;
            let session = engine.session().build()?;
            commands::run_file(&session, &script)
        }
    }
}

fn optional_env(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(value)) => Err(VqlError::new(
            ErrorCode::InvalidOption,
            format!("{name} is not valid UTF-8: {}", value.to_string_lossy()),
        )),
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::Cli;

    #[test]
    fn explain_is_not_a_cli_subcommand() {
        assert!(Cli::try_parse_from(["vql", "explain", "SELECT 1"]).is_err());
    }

    #[test]
    fn metrics_is_not_a_cli_option() {
        assert!(Cli::try_parse_from(["vql", "--metrics", "shell"]).is_err());
    }

    #[test]
    fn engine_settings_are_not_cli_options() {
        assert!(Cli::try_parse_from(["vql", "--catalog", "catalog.db", "shell"]).is_err());
        assert!(
            Cli::try_parse_from(["vql", "--query-memory-limit-bytes", "1024", "shell"]).is_err()
        );
    }

    #[test]
    fn shell_accepts_a_flight_endpoint_and_optional_tls_ca() {
        assert!(
            Cli::try_parse_from([
                "vql",
                "shell",
                "--endpoint",
                "https://vqld.example:6031",
                "--token",
                "secret",
                "--tls-ca",
                "ca.pem",
            ])
            .is_ok()
        );
        assert!(Cli::try_parse_from(["vql", "shell", "--token", "secret"]).is_err());
        assert!(Cli::try_parse_from(["vql", "shell", "--tls-ca", "ca.pem"]).is_err());
        assert!(
            Cli::try_parse_from([
                "vql",
                "run",
                "query.sql",
                "--endpoint",
                "http://127.0.0.1:6031",
            ])
            .is_err()
        );
    }
}
