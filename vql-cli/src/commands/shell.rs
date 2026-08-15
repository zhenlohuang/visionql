use std::io::{BufRead, IsTerminal, Write};
use std::path::Path;

use reedline::{DefaultPrompt, FileBackedHistory, Reedline, Signal};
use vql_kernel::{
    ErrorCode, Result, Session, VqlError, ends_with_statement_terminator, split_statements,
};

pub(crate) fn run(session: Session, history_path: &Path) -> Result<()> {
    let cancel_session = session.clone();
    ctrlc::set_handler(move || cancel_session.cancel_active_query()).map_err(|error| {
        VqlError::new(
            ErrorCode::Execution,
            format!("failed to install Ctrl-C handler: {error}"),
        )
    })?;

    println!(
        "VisionQL v{} — terminate statements with ';'",
        env!("CARGO_PKG_VERSION")
    );
    if !std::io::stdin().is_terminal()
        || std::env::var("TERM").is_ok_and(|term| term.eq_ignore_ascii_case("dumb"))
    {
        return run_basic(&session);
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
                pending.push_str(&line);
                pending.push('\n');
                if !ends_with_statement_terminator(&pending)? {
                    continue;
                }
                let statements = split_statements(&pending)?;
                pending.clear();
                execute(&session, statements)?;
            }
            Ok(Signal::CtrlC) => {
                pending.clear();
                session.cancel_active_query();
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

fn run_basic(session: &Session) -> Result<()> {
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
        pending.push_str(&line);
        if ends_with_statement_terminator(&pending)? {
            let statements = split_statements(&pending)?;
            pending.clear();
            execute(session, statements)?;
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

fn execute(session: &Session, statements: Vec<String>) -> Result<()> {
    for sql in statements {
        let result = session.sql(&sql).and_then(|statement| {
            if statement.is_unbounded() {
                statement.for_each_batch(|batch| {
                    super::super::render::print_batches(std::slice::from_ref(batch))
                })
            } else {
                super::super::render::print_batches(&statement.collect()?)
            }
        });
        match result {
            Ok(()) => {}
            Err(error) => eprintln!("{error}"),
        }
    }
    Ok(())
}
