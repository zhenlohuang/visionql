use rusqlite::Connection;

use crate::Result;

pub(crate) const FORMAT_VERSION: i64 = 1;

pub(crate) fn migrate(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "PRAGMA foreign_keys = ON;
         PRAGMA journal_mode = WAL;
         CREATE TABLE IF NOT EXISTS meta (
             key TEXT PRIMARY KEY,
             value TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS revisions (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             namespace TEXT NOT NULL,
             kind TEXT NOT NULL,
             name TEXT NOT NULL,
             definition_json TEXT,
             schema_ipc BLOB,
             tombstone INTEGER NOT NULL DEFAULT 0,
             created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
         );
         CREATE TABLE IF NOT EXISTS objects (
             namespace TEXT NOT NULL,
             kind TEXT NOT NULL,
             name TEXT NOT NULL,
             head_revision INTEGER NOT NULL REFERENCES revisions(id),
             PRIMARY KEY(namespace, kind, name)
         );",
    )?;

    let version = connection.query_row(
        "SELECT value FROM meta WHERE key = 'format_version'",
        [],
        |row| row.get::<_, String>(0),
    );
    match version {
        Ok(version) if version == FORMAT_VERSION.to_string() => Ok(()),
        Ok(version) => Err(crate::VqlError::new(
            crate::ErrorCode::Catalog,
            format!("catalog format {version} is not supported; expected {FORMAT_VERSION}"),
        )),
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            connection.execute(
                "INSERT INTO meta(key, value) VALUES ('format_version', ?1)",
                [FORMAT_VERSION.to_string()],
            )?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}
