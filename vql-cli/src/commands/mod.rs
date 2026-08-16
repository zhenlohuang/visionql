pub(crate) mod shell;

use std::path::Path;

use vql_kernel::{ErrorCode, QueryInterruptAction, Result, Session, VqlError, split_statements};

pub(crate) fn install_interrupt_handler(session: Session) -> Result<()> {
    ctrlc::set_handler(move || match session.interrupt_active_query() {
        QueryInterruptAction::NoActiveQuery => {}
        QueryInterruptAction::GracefulStopRequested => {
            eprintln!("graceful stop requested; press Ctrl-C again to cancel immediately");
        }
        QueryInterruptAction::ImmediateCancellationRequested => {
            eprintln!("cancelling active query immediately");
        }
    })
    .map_err(|error| {
        VqlError::new(
            ErrorCode::Execution,
            format!("failed to install Ctrl-C handler: {error}"),
        )
    })
}

pub(crate) fn run_file(session: &Session, path: &Path) -> Result<()> {
    let script = std::fs::read_to_string(path)?;
    install_interrupt_handler(session.clone())?;
    run_statements(session, split_statements(&script)?)
}

fn run_statements(session: &Session, statements: Vec<String>) -> Result<()> {
    let statement_count = statements.len();
    for (index, sql) in statements.into_iter().enumerate() {
        let statement = session.sql(&sql)?;
        if statement.is_unbounded() {
            if index + 1 != statement_count {
                return Err(VqlError::new(
                    ErrorCode::InvalidSql,
                    "an unbounded statement must be last in a vql run script; split the script before this statement",
                ));
            }
            statement.for_each_batch(|batch| {
                super::render::print_batches(std::slice::from_ref(batch))
            })?;
        } else {
            super::render::print_batches(&statement.collect()?)?;
        }
    }
    Ok(())
}

pub(crate) fn explain(session: &Session, sql: &str) -> Result<()> {
    let statement = session.sql(&format!("EXPLAIN {sql}"))?;
    super::render::print_batches(&statement.collect()?)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use vql_kernel::{Engine, EngineConfig};

    use super::*;

    #[test]
    fn run_rejects_an_unbounded_statement_before_later_sql() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        let statements = split_statements(
            "CREATE STREAM camera FROM 'rtsp://127.0.0.1/live';
             SELECT frame_id FROM camera;
             DROP STREAM camera;",
        )
        .unwrap();

        let error = run_statements(&session, statements).unwrap_err();

        assert_eq!(error.code, ErrorCode::InvalidSql);
        assert!(error.message.contains("must be last"));
        session.sql("DESCRIBE camera").unwrap();
    }
}
