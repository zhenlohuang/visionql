mod commands;
mod render;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;
use vql_kernel::{Engine, EngineConfig, Result};

#[derive(Debug, Parser)]
#[command(name = "vql", version, about = "Local visual data SQL")]
struct Cli {
    /// Override the SQLite catalog path without changing VQL_HOME.
    #[arg(long, global = true, env = "VQL_CATALOG")]
    catalog: Option<PathBuf>,

    /// Print query metrics after execution.
    #[arg(long, global = true, env = "VQL_METRICS")]
    metrics: bool,

    /// Override the per-query host-memory budget in bytes.
    #[arg(long, global = true, env = "VQL_QUERY_MEMORY_LIMIT_BYTES")]
    query_memory_limit_bytes: Option<usize>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Open an interactive SQL shell.
    Shell,
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
    let filter = EnvFilter::try_from_env("VQL_LOG").unwrap_or_else(|_| EnvFilter::new("off"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
    let cli = Cli::parse();
    let mut config = cli
        .catalog
        .map(|path| EngineConfig::default().with_catalog_path(path))
        .unwrap_or_default();
    if let Some(limit) = cli.query_memory_limit_bytes {
        config = config.with_query_memory_limit_bytes(limit);
    }
    let engine = Engine::new(config)?;
    let session = engine.session().build()?;
    match cli.command {
        Command::Shell => {
            commands::shell::run(session, engine.config().history_path(), cli.metrics)
        }
        Command::Run { script } => commands::run_file(&session, &script, cli.metrics),
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
}
