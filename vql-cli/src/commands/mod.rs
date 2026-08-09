pub(crate) mod shell;

use std::path::Path;

use vql_kernel::{Result, Session};

pub(crate) fn run_file(session: &Session, path: &Path) -> Result<()> {
    let script = std::fs::read_to_string(path)?;
    for statement in session.run_script(&script)? {
        super::render::print_batches(&statement.collect()?)?;
    }
    Ok(())
}

pub(crate) fn explain(session: &Session, sql: &str) -> Result<()> {
    let statement = session.sql(&format!("EXPLAIN {sql}"))?;
    super::render::print_batches(&statement.collect()?)
}
