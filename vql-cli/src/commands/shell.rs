use std::io::{BufRead, IsTerminal, Write};
use std::path::Path;
use std::sync::Arc;

use reedline::{DefaultPrompt, FileBackedHistory, Reedline, Signal};
use vql_kernel::{ErrorCode, Result, VqlError, ends_with_statement_terminator, split_statements};

use crate::backend::{ExecutionOutput, ShellBackend};

pub(crate) fn run(backend: Arc<dyn ShellBackend>, history_path: &Path) -> Result<()> {
    super::install_interrupt_handler(Arc::clone(&backend))?;

    println!(
        "VisionQL v{} — {} — terminate statements with ';'; use \\q or Ctrl-D to exit",
        env!("CARGO_PKG_VERSION"),
        backend.description(),
    );
    if !std::io::stdin().is_terminal()
        || std::env::var("TERM").is_ok_and(|term| term.eq_ignore_ascii_case("dumb"))
    {
        return run_basic(backend.as_ref());
    }

    if let Some(parent) = history_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let history =
        FileBackedHistory::with_file(1_000, history_path.to_path_buf()).map_err(|error| {
            VqlError::new(
                ErrorCode::Execution,
                format!("failed to open shell history: {error}"),
            )
        })?;
    let mut editor = Reedline::create().with_history(Box::new(history));
    let prompt = DefaultPrompt::default();
    let mut pending = String::new();
    loop {
        match editor.read_line(&prompt) {
            Ok(Signal::Success(line)) => {
                if is_quit_command(&line) {
                    break;
                }
                pending.push_str(&line);
                pending.push('\n');
                if !ends_with_statement_terminator(&pending)? {
                    continue;
                }
                let statements = split_statements(&pending)?;
                pending.clear();
                execute(backend.as_ref(), statements)?;
            }
            Ok(Signal::CtrlC) => {
                pending.clear();
                println!("^C");
            }
            Ok(Signal::CtrlD) => break,
            Ok(_) => continue,
            Err(error) => {
                return Err(VqlError::new(
                    ErrorCode::Execution,
                    format!("shell input failed: {error}"),
                ));
            }
        }
    }
    Ok(())
}

fn run_basic(backend: &dyn ShellBackend) -> Result<()> {
    let stdin = std::io::stdin();
    let mut input = stdin.lock();
    let mut pending = String::new();
    loop {
        print!("vql> ");
        std::io::stdout().flush()?;
        let mut line = String::new();
        if input.read_line(&mut line)? == 0 {
            break;
        }
        if is_quit_command(&line) {
            return Ok(());
        }
        pending.push_str(&line);
        if ends_with_statement_terminator(&pending)? {
            let statements = split_statements(&pending)?;
            pending.clear();
            execute(backend, statements)?;
        }
    }
    if !pending.trim().is_empty() {
        return Err(VqlError::new(
            ErrorCode::InvalidSql,
            "incomplete statement at end of input; terminate it with ';'",
        ));
    }
    Ok(())
}

fn is_quit_command(line: &str) -> bool {
    line.trim() == r"\q"
}

fn execute(backend: &dyn ShellBackend, statements: Vec<String>) -> Result<()> {
    for sql in statements {
        let result = backend.execute(&sql, &mut |output| match output {
            ExecutionOutput::Batches(batches) => super::super::render::print_batches(&batches),
            ExecutionOutput::Update { affected_rows } => {
                let suffix = if affected_rows == 1 { "row" } else { "rows" };
                println!("OK ({affected_rows} {suffix} affected)");
                Ok(())
            }
        });
        match result {
            Ok(()) => {}
            Err(error) => eprintln!("{error}"),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::is_quit_command;

    #[test]
    fn quit_command_is_a_standalone_line_without_a_terminator() {
        assert!(is_quit_command(r"\q"));
        assert!(is_quit_command("  \\q  \n"));
        assert!(!is_quit_command(r"\q;"));
        assert!(!is_quit_command(r"SELECT '\q';"));
    }
}
