use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

use arrow::datatypes::SchemaRef;
use rusqlite::{Connection, OptionalExtension, params};

use super::migrations;
use super::objects::{
    FunctionDef, ModelDef, ObjectKind, SinkDef, StreamDef, TableDef, decode_schema, encode_schema,
};
use super::snapshot::{DefinitionSnapshot, SnapshotObject, SnapshotTable};
use crate::{ErrorCode, Result, VqlError};

#[derive(Debug)]
pub(crate) struct CatalogStore {
    connection: Mutex<Connection>,
}

impl CatalogStore {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let connection = Connection::open(path)?;
        migrations::migrate(&connection)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub(crate) fn create_table(&self, definition: &TableDef, schema: &SchemaRef) -> Result<i64> {
        let name = definition.name.to_ascii_lowercase();
        let kind = ObjectKind::Table.as_str();
        let definition_json = serde_json::to_string(definition)?;
        let schema_ipc = encode_schema(schema)?;
        let mut connection = self.lock()?;
        let transaction = connection.transaction()?;
        let exists = transaction
            .query_row(
                "SELECT 1 FROM objects WHERE namespace='relation' AND name=?1",
                params![name],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if exists {
            return Err(VqlError::new(
                ErrorCode::AlreadyExists,
                format!("table '{name}' already exists"),
            ));
        }
        transaction.execute(
            "INSERT INTO revisions(namespace, kind, name, definition_json, schema_ipc)
             VALUES ('relation', ?1, ?2, ?3, ?4)",
            params![kind, name, definition_json, schema_ipc],
        )?;
        let revision = transaction.last_insert_rowid();
        transaction.execute(
            "INSERT INTO objects(namespace, kind, name, head_revision)
             VALUES ('relation', ?1, ?2, ?3)",
            params![kind, name, revision],
        )?;
        transaction.commit()?;
        Ok(revision)
    }

    pub(crate) fn drop_table(&self, name: &str) -> Result<i64> {
        let name = name.to_ascii_lowercase();
        let mut connection = self.lock()?;
        let transaction = connection.transaction()?;
        let deleted = transaction.execute(
            "DELETE FROM objects WHERE namespace='relation' AND kind='table' AND name=?1",
            [&name],
        )?;
        if deleted == 0 {
            return Err(VqlError::new(
                ErrorCode::NotFound,
                format!("table '{name}' does not exist"),
            ));
        }
        transaction.execute(
            "INSERT INTO revisions(namespace, kind, name, tombstone)
             VALUES ('relation', 'table', ?1, 1)",
            [&name],
        )?;
        let revision = transaction.last_insert_rowid();
        transaction.commit()?;
        Ok(revision)
    }

    pub(crate) fn snapshot(&self) -> Result<DefinitionSnapshot> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(
            "SELECT o.name, o.head_revision, r.definition_json, r.schema_ipc
             FROM objects o
             JOIN revisions r ON r.id = o.head_revision
             WHERE o.namespace='relation' AND o.kind='table'
             ORDER BY o.name",
        )?;
        let mut rows = statement.query([])?;
        let mut tables = BTreeMap::new();
        while let Some(row) = rows.next()? {
            let name: String = row.get(0)?;
            let revision: i64 = row.get(1)?;
            let definition_json: String = row.get(2)?;
            let schema_ipc: Vec<u8> = row.get(3)?;
            let definition: TableDef = serde_json::from_str(&definition_json)?;
            let schema = decode_schema(&schema_ipc)?;
            tables.insert(
                name,
                SnapshotTable {
                    revision,
                    definition,
                    schema,
                },
            );
        }
        drop(rows);
        drop(statement);
        let streams = load_objects::<StreamDef>(&connection, "relation", "stream")?;
        let models = load_objects::<ModelDef>(&connection, "model", "model")?;
        let functions = load_objects::<FunctionDef>(&connection, "function", "function")?;
        let sinks = load_objects::<SinkDef>(&connection, "sink", "sink")?;
        Ok(DefinitionSnapshot::new(
            tables, streams, models, functions, sinks,
        ))
    }

    pub(crate) fn create_model(&self, definition: &ModelDef) -> Result<i64> {
        self.create_object("model", ObjectKind::Model, &definition.name, definition)
    }

    pub(crate) fn update_model(
        &self,
        definition: &ModelDef,
        expected_revision: i64,
    ) -> Result<i64> {
        let name = definition.name.to_ascii_lowercase();
        let definition_json = serde_json::to_string(definition)?;
        let mut connection = self.lock()?;
        let transaction = connection.transaction()?;
        let current_revision = transaction
            .query_row(
                "SELECT head_revision FROM objects
                 WHERE namespace='model' AND kind='model' AND name=?1",
                [&name],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        let Some(current_revision) = current_revision else {
            return Err(VqlError::new(
                ErrorCode::NotFound,
                format!("model '{name}' does not exist"),
            ));
        };
        if current_revision != expected_revision {
            return Err(VqlError::new(
                ErrorCode::Catalog,
                format!(
                    "model '{name}' changed while RESOLVE MODEL was running; retry the statement"
                ),
            ));
        }
        transaction.execute(
            "INSERT INTO revisions(namespace, kind, name, definition_json)
             VALUES ('model', 'model', ?1, ?2)",
            params![name, definition_json],
        )?;
        let revision = transaction.last_insert_rowid();
        transaction.execute(
            "UPDATE objects SET head_revision=?1
             WHERE namespace='model' AND kind='model' AND name=?2",
            params![revision, name],
        )?;
        transaction.commit()?;
        Ok(revision)
    }

    pub(crate) fn create_stream(&self, definition: &StreamDef) -> Result<i64> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction()?;
        ensure_relation_absent(&transaction, &definition.name)?;
        let revision = insert_object(
            &transaction,
            "relation",
            ObjectKind::Stream,
            &definition.name,
            definition,
        )?;
        transaction.commit()?;
        Ok(revision)
    }

    pub(crate) fn drop_stream(&self, name: &str) -> Result<i64> {
        self.drop_object("relation", ObjectKind::Stream, name)
    }

    pub(crate) fn create_function(&self, definition: &FunctionDef) -> Result<i64> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction()?;
        ensure_absent(
            &transaction,
            "function",
            ObjectKind::Function,
            &definition.name,
        )?;
        let revision = insert_object(
            &transaction,
            "function",
            ObjectKind::Function,
            &definition.name,
            definition,
        )?;
        transaction.commit()?;
        Ok(revision)
    }

    pub(crate) fn create_sink(&self, definition: &SinkDef) -> Result<i64> {
        self.create_object("sink", ObjectKind::Sink, &definition.name, definition)
    }

    pub(crate) fn drop_object(&self, namespace: &str, kind: ObjectKind, name: &str) -> Result<i64> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction()?;
        let revision = drop_object_in_transaction(&transaction, namespace, kind, name)?;
        transaction.commit()?;
        Ok(revision)
    }

    pub(crate) fn drop_model(&self, name: &str) -> Result<i64> {
        self.drop_object("model", ObjectKind::Model, name)
    }

    pub(crate) fn table_at_revision(&self, revision: i64) -> Result<TableDef> {
        let connection = self.lock()?;
        let definition = connection
            .query_row(
                "SELECT definition_json FROM revisions
                 WHERE id=?1 AND namespace='relation' AND kind='table' AND tombstone=0",
                [revision],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten()
            .ok_or_else(|| {
                VqlError::new(
                    ErrorCode::NotFound,
                    format!("table revision {revision} does not exist"),
                )
            })?;
        serde_json::from_str(&definition).map_err(Into::into)
    }

    #[cfg(test)]
    pub(crate) fn revision_count(&self, kind: super::objects::ObjectKind) -> Result<i64> {
        let connection = self.lock()?;
        connection
            .query_row(
                "SELECT COUNT(*) FROM revisions WHERE kind=?1",
                [kind.as_str()],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    fn create_object<T: serde::Serialize>(
        &self,
        namespace: &str,
        kind: ObjectKind,
        name: &str,
        definition: &T,
    ) -> Result<i64> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction()?;
        ensure_absent(&transaction, namespace, kind, name)?;
        let revision = insert_object(&transaction, namespace, kind, name, definition)?;
        transaction.commit()?;
        Ok(revision)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| VqlError::new(ErrorCode::Catalog, "catalog connection lock was poisoned"))
    }
}

fn ensure_absent(
    transaction: &rusqlite::Transaction<'_>,
    namespace: &str,
    kind: ObjectKind,
    name: &str,
) -> Result<()> {
    let name = name.to_ascii_lowercase();
    let exists = transaction
        .query_row(
            "SELECT 1 FROM objects WHERE namespace=?1 AND kind=?2 AND name=?3",
            params![namespace, kind.as_str(), name],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if exists {
        return Err(VqlError::new(
            ErrorCode::AlreadyExists,
            format!("{} '{name}' already exists", kind.as_str()),
        ));
    }
    Ok(())
}

fn ensure_relation_absent(transaction: &rusqlite::Transaction<'_>, name: &str) -> Result<()> {
    let name = name.to_ascii_lowercase();
    let exists = transaction
        .query_row(
            "SELECT kind FROM objects WHERE namespace='relation' AND name=?1",
            [&name],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if let Some(kind) = exists {
        return Err(VqlError::new(
            ErrorCode::AlreadyExists,
            format!("relation '{name}' already exists as {kind}"),
        ));
    }
    Ok(())
}

fn drop_object_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    namespace: &str,
    kind: ObjectKind,
    name: &str,
) -> Result<i64> {
    let name = name.to_ascii_lowercase();
    let deleted = transaction.execute(
        "DELETE FROM objects WHERE namespace=?1 AND kind=?2 AND name=?3",
        params![namespace, kind.as_str(), name],
    )?;
    if deleted == 0 {
        return Err(VqlError::new(
            ErrorCode::NotFound,
            format!("{} '{name}' does not exist", kind.as_str()),
        ));
    }
    transaction.execute(
        "INSERT INTO revisions(namespace, kind, name, tombstone) VALUES (?1, ?2, ?3, 1)",
        params![namespace, kind.as_str(), name],
    )?;
    Ok(transaction.last_insert_rowid())
}

fn insert_object<T: serde::Serialize>(
    transaction: &rusqlite::Transaction<'_>,
    namespace: &str,
    kind: ObjectKind,
    name: &str,
    definition: &T,
) -> Result<i64> {
    let name = name.to_ascii_lowercase();
    let definition_json = serde_json::to_string(definition)?;
    transaction.execute(
        "INSERT INTO revisions(namespace, kind, name, definition_json) VALUES (?1, ?2, ?3, ?4)",
        params![namespace, kind.as_str(), name, definition_json],
    )?;
    let revision = transaction.last_insert_rowid();
    transaction.execute(
        "INSERT INTO objects(namespace, kind, name, head_revision) VALUES (?1, ?2, ?3, ?4)",
        params![namespace, kind.as_str(), name, revision],
    )?;
    Ok(revision)
}

fn load_objects<T: serde::de::DeserializeOwned>(
    connection: &Connection,
    namespace: &str,
    kind: &str,
) -> Result<BTreeMap<String, SnapshotObject<T>>> {
    let mut statement = connection.prepare(
        "SELECT o.name, o.head_revision, r.definition_json
         FROM objects o JOIN revisions r ON r.id=o.head_revision
         WHERE o.namespace=?1 AND o.kind=?2 ORDER BY o.name",
    )?;
    let mut rows = statement.query(params![namespace, kind])?;
    let mut values = BTreeMap::new();
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        let revision: i64 = row.get(1)?;
        let json: String = row.get(2)?;
        values.insert(
            name,
            SnapshotObject {
                revision,
                definition: serde_json::from_str(&json)?,
            },
        );
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        EventTimePolicy, ModelType, ObjectKind, RtspTransport, StreamDef, TableProviderKind,
    };
    use crate::connectors::images::images_schema;
    use tempfile::tempdir;

    #[test]
    fn catalog_crud_is_revisioned_and_reopens() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("catalog.db");
        let definition = TableDef {
            name: "photos".to_owned(),
            provider: TableProviderKind::Images,
            location: temp.path().to_string_lossy().into_owned(),
            recursive: true,
            fps: None,
            start_time_ms: None,
        };
        {
            let catalog = CatalogStore::open(&path).unwrap();
            let revision = catalog.create_table(&definition, &images_schema()).unwrap();
            assert_eq!(revision, 1);
            assert_eq!(catalog.revision_count(ObjectKind::Table).unwrap(), 1);
            assert!(catalog.snapshot().unwrap().table("PHOTOS").is_some());
        }
        {
            let catalog = CatalogStore::open(&path).unwrap();
            assert!(catalog.snapshot().unwrap().table("photos").is_some());
            catalog.drop_table("photos").unwrap();
            assert!(catalog.snapshot().unwrap().table("photos").is_none());
            assert_eq!(catalog.revision_count(ObjectKind::Table).unwrap(), 2);
        }
    }

    #[test]
    fn duplicate_create_does_not_leave_a_revision() {
        let temp = tempdir().unwrap();
        let catalog = CatalogStore::open(&temp.path().join("catalog.db")).unwrap();
        let definition = TableDef {
            name: "photos".to_owned(),
            provider: TableProviderKind::Images,
            location: temp.path().to_string_lossy().into_owned(),
            recursive: false,
            fps: None,
            start_time_ms: None,
        };
        catalog.create_table(&definition, &images_schema()).unwrap();
        let error = catalog
            .create_table(&definition, &images_schema())
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::AlreadyExists);
        assert_eq!(catalog.revision_count(ObjectKind::Table).unwrap(), 1);
    }

    #[test]
    fn stream_reopens_and_shares_the_relation_namespace() {
        let temp = tempdir().unwrap();
        let catalog = CatalogStore::open(&temp.path().join("catalog.db")).unwrap();
        let stream = StreamDef {
            name: "entrance".to_owned(),
            endpoint: "rtsp://camera/live".to_owned(),
            fps: 5.0,
            event_time: EventTimePolicy::CaptureTime,
            watermark_delay_ms: 2_000,
            transport: RtspTransport::Tcp,
        };
        catalog.create_stream(&stream).unwrap();
        assert_eq!(
            catalog
                .snapshot()
                .unwrap()
                .stream("ENTRANCE")
                .unwrap()
                .definition,
            stream
        );

        let table = TableDef {
            name: "entrance".to_owned(),
            provider: TableProviderKind::Images,
            location: temp.path().to_string_lossy().into_owned(),
            recursive: false,
            fps: None,
            start_time_ms: None,
        };
        assert_eq!(
            catalog
                .create_table(&table, &images_schema())
                .unwrap_err()
                .code,
            ErrorCode::AlreadyExists
        );
        catalog.drop_stream("entrance").unwrap();
        assert!(catalog.snapshot().unwrap().stream("entrance").is_none());
    }

    #[test]
    fn model_update_rejects_a_stale_declaration_revision() {
        let temp = tempdir().unwrap();
        let catalog = CatalogStore::open(&temp.path().join("catalog.db")).unwrap();
        let model = ModelDef {
            name: "detector".to_owned(),
            model_type: ModelType::ObjectDetection,
            source: "mock://person".to_owned(),
            runtime_kind: "onnx-runtime".to_owned(),
            options: BTreeMap::new(),
            declaration_fingerprint: "declaration".to_owned(),
            resolved: None,
        };
        let declaration_revision = catalog.create_model(&model).unwrap();
        catalog.update_model(&model, declaration_revision).unwrap();

        let error = catalog
            .update_model(&model, declaration_revision)
            .unwrap_err();

        assert_eq!(error.code, ErrorCode::Catalog);
        assert!(
            error
                .message
                .contains("changed while RESOLVE MODEL was running")
        );
        assert_eq!(catalog.revision_count(ObjectKind::Model).unwrap(), 2);
    }
}
