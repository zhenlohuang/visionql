pub(crate) mod shell;

use std::path::Path;

use vql_kernel::{ErrorCode, Result, Session, VqlError, split_statements};

pub(crate) fn run_file(session: &Session, path: &Path) -> Result<()> {
    let script = std::fs::read_to_string(path)?;
    let cancel_session = session.clone();
    ctrlc::set_handler(move || cancel_session.cancel_active_query()).map_err(|error| {
        VqlError::new(
            ErrorCode::Execution,
            format!("failed to install Ctrl-C handler: {error}"),
        )
    })?;
    for sql in split_statements(&script)? {
        let statement = session.sql(&sql)?;
        if statement.is_unbounded() {
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
