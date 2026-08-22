mod commands;
mod render;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;
use vql_kernel::{Engine, EngineConfig, Result};

#[derive(Debug, Parser)]
#[command(name = "vql", version, about = "Local visual data SQL")]
struct Cli {
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
    let cli = Cli::parse();
    let config = EngineConfig::load()?;
    let filter = EnvFilter::new(config.log_level().as_str());
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
    let engine = Engine::new(config)?;
    let session = engine.session().build()?;
    match cli.command {
        Command::Shell => commands::shell::run(session, engine.config().history_path()),
        Command::Run { script } => commands::run_file(&session, &script),
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
}
