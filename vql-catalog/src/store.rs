use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use crate::{
    CatalogError, CatalogErrorCode, CatalogInfo, DEFAULT_CATALOG, DEFAULT_SCHEMA,
    DefinitionSnapshot, FunctionDef, ModelDef, ObjectKind, Result, SchemaInfo, SecurableMetadata,
    SnapshotObject, SnapshotTable, TableDef, provider_schema,
};

pub trait CatalogBackend: Debug + Send + Sync {
    fn create_catalog(&self, name: &str, metadata: SecurableMetadata) -> Result<CatalogInfo>;
    fn get_catalog(&self, name: &str) -> Result<CatalogInfo>;
    fn list_catalogs(&self) -> Result<Vec<CatalogInfo>>;
    fn update_catalog(
        &self,
        name: &str,
        new_name: Option<&str>,
        metadata: SecurableMetadata,
    ) -> Result<CatalogInfo>;
    fn delete_catalog(&self, name: &str, force: bool) -> Result<()>;

    fn create_schema(
        &self,
        catalog_name: &str,
        name: &str,
        metadata: SecurableMetadata,
    ) -> Result<SchemaInfo>;
    fn get_schema(&self, catalog_name: &str, name: &str) -> Result<SchemaInfo>;
    fn list_schemas(&self, catalog_name: &str) -> Result<Vec<SchemaInfo>>;
    fn update_schema(
        &self,
        catalog_name: &str,
        name: &str,
        new_name: Option<&str>,
        metadata: SecurableMetadata,
    ) -> Result<SchemaInfo>;
    fn delete_schema(&self, catalog_name: &str, name: &str, force: bool) -> Result<()>;

    fn create_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        definition: &TableDef,
        schema: &SchemaRef,
    ) -> Result<i64>;
    fn drop_table(&self, catalog_name: &str, schema_name: &str, name: &str) -> Result<i64>;
    fn snapshot(&self, catalog_name: &str, schema_name: &str) -> Result<DefinitionSnapshot>;
    fn table_at_generation(&self, generation: i64) -> Result<TableDef>;

    fn create_model(&self, definition: &ModelDef) -> Result<i64>;
    fn update_model(
        &self,
        current_name: &str,
        definition: &ModelDef,
        expected_generation: i64,
    ) -> Result<i64>;
    fn create_function(&self, definition: &FunctionDef) -> Result<i64>;
    fn drop_object(&self, kind: ObjectKind, name: &str) -> Result<i64>;
    fn history_count(&self, kind: ObjectKind) -> Result<i64>;
}

#[derive(Debug, Clone)]
pub struct CatalogStore {
    backend: Arc<dyn CatalogBackend>,
}

impl CatalogStore {
    pub fn new(backend: Arc<dyn CatalogBackend>) -> Self {
        Self { backend }
    }

    #[cfg(feature = "sqlite")]
    pub fn open(path: &Path) -> Result<Self> {
        Ok(Self::new(Arc::new(SqliteCatalogBackend::open(path)?)))
    }

    pub fn backend(&self) -> &Arc<dyn CatalogBackend> {
        &self.backend
    }

    pub fn create_catalog(&self, name: &str, metadata: SecurableMetadata) -> Result<CatalogInfo> {
        self.backend.create_catalog(name, metadata)
    }

    pub fn get_catalog(&self, name: &str) -> Result<CatalogInfo> {
        self.backend.get_catalog(name)
    }

    pub fn list_catalogs(&self) -> Result<Vec<CatalogInfo>> {
        self.backend.list_catalogs()
    }

    pub fn update_catalog(
        &self,
        name: &str,
        new_name: Option<&str>,
        metadata: SecurableMetadata,
    ) -> Result<CatalogInfo> {
        self.backend.update_catalog(name, new_name, metadata)
    }

    pub fn delete_catalog(&self, name: &str, force: bool) -> Result<()> {
        self.backend.delete_catalog(name, force)
    }

    pub fn create_schema(
        &self,
        catalog_name: &str,
        name: &str,
        metadata: SecurableMetadata,
    ) -> Result<SchemaInfo> {
        self.backend.create_schema(catalog_name, name, metadata)
    }

    pub fn get_schema(&self, catalog_name: &str, name: &str) -> Result<SchemaInfo> {
        self.backend.get_schema(catalog_name, name)
    }

    pub fn list_schemas(&self, catalog_name: &str) -> Result<Vec<SchemaInfo>> {
        self.backend.list_schemas(catalog_name)
    }

    pub fn update_schema(
        &self,
        catalog_name: &str,
        name: &str,
        new_name: Option<&str>,
        metadata: SecurableMetadata,
    ) -> Result<SchemaInfo> {
        self.backend
            .update_schema(catalog_name, name, new_name, metadata)
    }

    pub fn delete_schema(&self, catalog_name: &str, name: &str, force: bool) -> Result<()> {
        self.backend.delete_schema(catalog_name, name, force)
    }

    pub fn create_table(&self, definition: &TableDef, schema: &SchemaRef) -> Result<i64> {
        self.create_table_in(DEFAULT_CATALOG, DEFAULT_SCHEMA, definition, schema)
    }

    pub fn create_table_in(
        &self,
        catalog_name: &str,
        schema_name: &str,
        definition: &TableDef,
        schema: &SchemaRef,
    ) -> Result<i64> {
        definition.validate()?;
        let schema = provider_schema(&definition.provider).unwrap_or_else(|| Arc::clone(schema));
        self.backend
            .create_table(catalog_name, schema_name, definition, &schema)
    }

    pub fn drop_table(&self, name: &str) -> Result<i64> {
        self.drop_table_in(DEFAULT_CATALOG, DEFAULT_SCHEMA, name)
    }

    pub fn drop_table_in(&self, catalog_name: &str, schema_name: &str, name: &str) -> Result<i64> {
        self.backend.drop_table(catalog_name, schema_name, name)
    }

    pub fn snapshot(&self) -> Result<DefinitionSnapshot> {
        self.snapshot_in(DEFAULT_CATALOG, DEFAULT_SCHEMA)
    }

    pub fn snapshot_in(&self, catalog_name: &str, schema_name: &str) -> Result<DefinitionSnapshot> {
        self.backend.snapshot(catalog_name, schema_name)
    }

    pub fn table_at_generation(&self, generation: i64) -> Result<TableDef> {
        self.backend.table_at_generation(generation)
    }

    pub fn create_model(&self, definition: &ModelDef) -> Result<i64> {
        self.backend.create_model(definition)
    }

    pub fn update_model(
        &self,
        current_name: &str,
        definition: &ModelDef,
        expected_generation: i64,
    ) -> Result<i64> {
        self.backend
            .update_model(current_name, definition, expected_generation)
    }

    pub fn create_function(&self, definition: &FunctionDef) -> Result<i64> {
        self.backend.create_function(definition)
    }

    pub fn drop_model(&self, name: &str) -> Result<i64> {
        self.backend.drop_object(ObjectKind::Model, name)
    }

    pub fn drop_object(&self, kind: ObjectKind, name: &str) -> Result<i64> {
        self.backend.drop_object(kind, name)
    }

    pub fn history_count(&self, kind: ObjectKind) -> Result<i64> {
        self.backend.history_count(kind)
    }
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use std::sync::{Mutex, MutexGuard};

    use chrono::Utc;
    use rusqlite::{Connection, OptionalExtension, Transaction, params};

    use super::*;
    use crate::{decode_schema, encode_schema};

    #[derive(Debug)]
    pub struct SqliteCatalogBackend {
        connection: Mutex<Connection>,
    }

    impl SqliteCatalogBackend {
        pub fn open(path: &Path) -> Result<Self> {
            let connection = Connection::open(path)?;
            connection.busy_timeout(std::time::Duration::from_secs(5))?;
            initialize(&connection)?;
            Ok(Self {
                connection: Mutex::new(connection),
            })
        }

        fn lock(&self) -> Result<MutexGuard<'_, Connection>> {
            self.connection.lock().map_err(|_| {
                CatalogError::new(
                    CatalogErrorCode::Internal,
                    "catalog connection lock was poisoned",
                )
            })
        }

        fn history_count(&self, kind: ObjectKind) -> Result<i64> {
            self.lock()?
                .query_row(
                    "SELECT COUNT(*) FROM object_revisions WHERE kind=?1",
                    [kind.as_str()],
                    |row| row.get(0),
                )
                .map_err(Into::into)
        }
    }

    impl CatalogBackend for SqliteCatalogBackend {
        fn create_catalog(&self, name: &str, metadata: SecurableMetadata) -> Result<CatalogInfo> {
            let name = normalize_name(name, "catalog")?;
            let now = Utc::now().timestamp_millis();
            let id = uuid::Uuid::new_v4().to_string();
            let properties = serde_json::to_string(&metadata.properties)?;
            let result = self.lock()?.execute(
                "INSERT INTO catalogs(name, comment, properties_json, owner, object_id, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
                params![name, metadata.comment, properties, metadata.owner, id, now],
            );
            map_unique(result, "catalog", &name)?;
            self.get_catalog(&name)
        }

        fn get_catalog(&self, name: &str) -> Result<CatalogInfo> {
            let name = normalize_name(name, "catalog")?;
            self.lock()?
                .query_row(
                    "SELECT name, comment, properties_json, owner, object_id, created_at, updated_at
                     FROM catalogs WHERE name=?1",
                    [&name],
                    catalog_from_row,
                )
                .optional()?
                .ok_or_else(|| not_found("catalog", &name))
        }

        fn list_catalogs(&self) -> Result<Vec<CatalogInfo>> {
            let connection = self.lock()?;
            let mut statement = connection.prepare(
                "SELECT name, comment, properties_json, owner, object_id, created_at, updated_at
                 FROM catalogs ORDER BY name",
            )?;
            let rows = statement.query_map([], catalog_from_row)?;
            collect_rows(rows)
        }

        fn update_catalog(
            &self,
            name: &str,
            new_name: Option<&str>,
            metadata: SecurableMetadata,
        ) -> Result<CatalogInfo> {
            let name = normalize_name(name, "catalog")?;
            let new_name = normalize_name(new_name.unwrap_or(&name), "catalog")?;
            let properties = serde_json::to_string(&metadata.properties)?;
            let result = self.lock()?.execute(
                "UPDATE catalogs SET name=?1, comment=?2, properties_json=?3, owner=?4, updated_at=?5
                 WHERE name=?6",
                params![
                    new_name,
                    metadata.comment,
                    properties,
                    metadata.owner,
                    Utc::now().timestamp_millis(),
                    name
                ],
            );
            let updated = map_unique(result, "catalog", &new_name)?;
            if updated == 0 {
                return Err(not_found("catalog", &name));
            }
            self.get_catalog(&new_name)
        }

        fn delete_catalog(&self, name: &str, force: bool) -> Result<()> {
            let name = normalize_name(name, "catalog")?;
            if name == DEFAULT_CATALOG {
                return Err(CatalogError::new(
                    CatalogErrorCode::InvalidArgument,
                    "the default catalog 'vql' cannot be deleted",
                ));
            }
            let mut connection = self.lock()?;
            let transaction = connection.transaction()?;
            let children: i64 = transaction.query_row(
                "SELECT COUNT(*) FROM schemas WHERE catalog_name=?1",
                [&name],
                |row| row.get(0),
            )?;
            if children > 0 && !force {
                return Err(not_empty("catalog", &name));
            }
            let deleted = transaction.execute("DELETE FROM catalogs WHERE name=?1", [&name])?;
            if deleted == 0 {
                return Err(not_found("catalog", &name));
            }
            transaction.commit()?;
            Ok(())
        }

        fn create_schema(
            &self,
            catalog_name: &str,
            name: &str,
            metadata: SecurableMetadata,
        ) -> Result<SchemaInfo> {
            let catalog_name = normalize_name(catalog_name, "catalog")?;
            let name = normalize_name(name, "schema")?;
            self.get_catalog(&catalog_name)?;
            let now = Utc::now().timestamp_millis();
            let id = uuid::Uuid::new_v4().to_string();
            let properties = serde_json::to_string(&metadata.properties)?;
            let result = self.lock()?.execute(
                "INSERT INTO schemas(catalog_name, name, comment, properties_json, owner, object_id, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)",
                params![
                    catalog_name,
                    name,
                    metadata.comment,
                    properties,
                    metadata.owner,
                    id,
                    now
                ],
            );
            map_unique(result, "schema", &format!("{catalog_name}.{name}"))?;
            self.get_schema(&catalog_name, &name)
        }

        fn get_schema(&self, catalog_name: &str, name: &str) -> Result<SchemaInfo> {
            let catalog_name = normalize_name(catalog_name, "catalog")?;
            let name = normalize_name(name, "schema")?;
            self.lock()?
                .query_row(
                    "SELECT catalog_name, name, comment, properties_json, owner, object_id, created_at, updated_at
                     FROM schemas WHERE catalog_name=?1 AND name=?2",
                    params![catalog_name, name],
                    schema_from_row,
                )
                .optional()?
                .ok_or_else(|| not_found("schema", &format!("{catalog_name}.{name}")))
        }

        fn list_schemas(&self, catalog_name: &str) -> Result<Vec<SchemaInfo>> {
            let catalog_name = normalize_name(catalog_name, "catalog")?;
            self.get_catalog(&catalog_name)?;
            let connection = self.lock()?;
            let mut statement = connection.prepare(
                "SELECT catalog_name, name, comment, properties_json, owner, object_id, created_at, updated_at
                 FROM schemas WHERE catalog_name=?1 ORDER BY name",
            )?;
            let rows = statement.query_map([catalog_name], schema_from_row)?;
            collect_rows(rows)
        }

        fn update_schema(
            &self,
            catalog_name: &str,
            name: &str,
            new_name: Option<&str>,
            metadata: SecurableMetadata,
        ) -> Result<SchemaInfo> {
            let catalog_name = normalize_name(catalog_name, "catalog")?;
            let name = normalize_name(name, "schema")?;
            let new_name = normalize_name(new_name.unwrap_or(&name), "schema")?;
            let properties = serde_json::to_string(&metadata.properties)?;
            let result = self.lock()?.execute(
                "UPDATE schemas SET name=?1, comment=?2, properties_json=?3, owner=?4, updated_at=?5
                 WHERE catalog_name=?6 AND name=?7",
                params![
                    new_name,
                    metadata.comment,
                    properties,
                    metadata.owner,
                    Utc::now().timestamp_millis(),
                    catalog_name,
                    name
                ],
            );
            let updated = map_unique(result, "schema", &format!("{catalog_name}.{new_name}"))?;
            if updated == 0 {
                return Err(not_found("schema", &format!("{catalog_name}.{name}")));
            }
            self.get_schema(&catalog_name, &new_name)
        }

        fn delete_schema(&self, catalog_name: &str, name: &str, force: bool) -> Result<()> {
            let catalog_name = normalize_name(catalog_name, "catalog")?;
            let name = normalize_name(name, "schema")?;
            if catalog_name == DEFAULT_CATALOG && name == DEFAULT_SCHEMA {
                return Err(CatalogError::new(
                    CatalogErrorCode::InvalidArgument,
                    "the default schema 'vql.default' cannot be deleted",
                ));
            }
            let mut connection = self.lock()?;
            let transaction = connection.transaction()?;
            let children: i64 = transaction.query_row(
                "SELECT COUNT(*) FROM objects WHERE catalog_name=?1 AND schema_name=?2",
                params![catalog_name, name],
                |row| row.get(0),
            )?;
            if children > 0 && !force {
                return Err(not_empty("schema", &format!("{catalog_name}.{name}")));
            }
            let deleted = transaction.execute(
                "DELETE FROM schemas WHERE catalog_name=?1 AND name=?2",
                params![catalog_name, name],
            )?;
            if deleted == 0 {
                return Err(not_found("schema", &format!("{catalog_name}.{name}")));
            }
            transaction.commit()?;
            Ok(())
        }

        fn create_table(
            &self,
            catalog_name: &str,
            schema_name: &str,
            definition: &TableDef,
            schema: &SchemaRef,
        ) -> Result<i64> {
            let catalog_name = normalize_name(catalog_name, "catalog")?;
            let schema_name = normalize_name(schema_name, "schema")?;
            self.get_schema(&catalog_name, &schema_name)?;
            let mut connection = self.lock()?;
            let transaction = connection.transaction()?;
            ensure_absent(
                &transaction,
                &catalog_name,
                &schema_name,
                ObjectKind::Table,
                &definition.name,
            )?;
            let revision = insert_object(
                &transaction,
                &catalog_name,
                &schema_name,
                ObjectKind::Table,
                &definition.name,
                definition,
                Some(encode_schema(schema)?),
            )?;
            transaction.commit()?;
            Ok(revision)
        }

        fn drop_table(&self, catalog_name: &str, schema_name: &str, name: &str) -> Result<i64> {
            let mut connection = self.lock()?;
            let transaction = connection.transaction()?;
            let revision = drop_object_in_transaction(
                &transaction,
                catalog_name,
                schema_name,
                ObjectKind::Table,
                name,
            )?;
            transaction.commit()?;
            Ok(revision)
        }

        fn snapshot(&self, catalog_name: &str, schema_name: &str) -> Result<DefinitionSnapshot> {
            let catalog_name = normalize_name(catalog_name, "catalog")?;
            let schema_name = normalize_name(schema_name, "schema")?;
            self.get_schema(&catalog_name, &schema_name)?;
            let mut connection = self.lock()?;
            let transaction = connection.transaction()?;
            let tables = load_tables(&transaction, &catalog_name, &schema_name)?;
            let models = load_objects::<ModelDef>(
                &transaction,
                &catalog_name,
                &schema_name,
                ObjectKind::Model,
            )?;
            let functions = load_objects::<FunctionDef>(
                &transaction,
                &catalog_name,
                &schema_name,
                ObjectKind::Function,
            )?;
            transaction.commit()?;
            Ok(DefinitionSnapshot::new(tables, models, functions))
        }

        fn table_at_generation(&self, generation: i64) -> Result<TableDef> {
            let definition = self
                .lock()?
                .query_row(
                    "SELECT definition_json FROM object_revisions
                     WHERE generation=?1 AND kind='table' AND tombstone=0",
                    [generation],
                    |row| row.get::<_, Option<String>>(0),
                )
                .optional()?
                .flatten()
                .ok_or_else(|| {
                    CatalogError::new(
                        CatalogErrorCode::NotFound,
                        format!("table generation {generation} does not exist"),
                    )
                })?;
            serde_json::from_str(&definition).map_err(Into::into)
        }

        fn create_model(&self, definition: &ModelDef) -> Result<i64> {
            create_default_object(self, ObjectKind::Model, &definition.name, definition)
        }

        fn update_model(
            &self,
            current_name: &str,
            definition: &ModelDef,
            expected_generation: i64,
        ) -> Result<i64> {
            let current_name = normalize_name(current_name, "model")?;
            let name = normalize_name(&definition.name, "model")?;
            let definition_json = serde_json::to_string(definition)?;
            let mut connection = self.lock()?;
            let transaction =
                connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let current = transaction
                .query_row(
                    "SELECT o.object_id, o.head_generation, r.object_revision, r.definition_json FROM objects o
                     JOIN object_revisions r ON r.generation=o.head_generation
                     WHERE o.catalog_name=?1 AND o.schema_name=?2 AND o.kind='model' AND o.name=?3",
                    params![DEFAULT_CATALOG, DEFAULT_SCHEMA, current_name],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, String>(3)?,
                        ))
                    },
                )
                .optional()?;
            let Some((object_id, current_generation, current_revision, current_json)) = current
            else {
                return Err(not_found("model", &current_name));
            };
            if current_generation != expected_generation {
                return Err(CatalogError::new(
                    CatalogErrorCode::Conflict,
                    format!(
                        "model '{current_name}' changed while the statement was running; retry it"
                    ),
                ));
            }
            let current: ModelDef = serde_json::from_str(&current_json)?;
            validate_resolved_versions_immutable(&current, definition)?;
            if current_name != name {
                ensure_callable_absent(
                    &transaction,
                    DEFAULT_CATALOG,
                    DEFAULT_SCHEMA,
                    &name,
                    ObjectKind::Model,
                    CatalogErrorCode::NameConflict,
                )?;
                insert_callable(
                    &transaction,
                    DEFAULT_CATALOG,
                    DEFAULT_SCHEMA,
                    &name,
                    ObjectKind::Model,
                )?;
            }
            transaction.execute(
                "INSERT INTO object_revisions(
                     object_id, catalog_name, schema_name, kind, name, object_revision, definition_json
                 ) VALUES (?1, ?2, ?3, 'model', ?4, ?5, ?6)",
                params![
                    object_id,
                    DEFAULT_CATALOG,
                    DEFAULT_SCHEMA,
                    name,
                    current_revision + 1,
                    definition_json
                ],
            )?;
            let generation = transaction.last_insert_rowid();
            transaction.execute(
                "UPDATE objects SET name=?1, head_generation=?2
                 WHERE catalog_name=?3 AND schema_name=?4 AND kind='model' AND name=?5",
                params![
                    name,
                    generation,
                    DEFAULT_CATALOG,
                    DEFAULT_SCHEMA,
                    current_name
                ],
            )?;
            if current_name != name {
                transaction.execute(
                    "DELETE FROM callables
                     WHERE catalog_name=?1 AND schema_name=?2 AND name=?3",
                    params![DEFAULT_CATALOG, DEFAULT_SCHEMA, current_name],
                )?;
            }
            transaction.commit()?;
            Ok(current_revision + 1)
        }

        fn create_function(&self, definition: &FunctionDef) -> Result<i64> {
            create_default_object(self, ObjectKind::Function, &definition.name, definition)
        }

        fn drop_object(&self, kind: ObjectKind, name: &str) -> Result<i64> {
            let mut connection = self.lock()?;
            let transaction = connection.transaction()?;
            let revision = drop_object_in_transaction(
                &transaction,
                DEFAULT_CATALOG,
                DEFAULT_SCHEMA,
                kind,
                name,
            )?;
            transaction.commit()?;
            Ok(revision)
        }

        fn history_count(&self, kind: ObjectKind) -> Result<i64> {
            SqliteCatalogBackend::history_count(self, kind)
        }
    }

    fn initialize(connection: &Connection) -> Result<()> {
        connection.execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA journal_mode = WAL;
             CREATE TABLE IF NOT EXISTS catalogs (
                 name TEXT PRIMARY KEY,
                 comment TEXT,
                 properties_json TEXT NOT NULL DEFAULT '{}',
                 owner TEXT,
                 object_id TEXT NOT NULL UNIQUE,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS schemas (
                 catalog_name TEXT NOT NULL REFERENCES catalogs(name) ON UPDATE CASCADE ON DELETE CASCADE,
                 name TEXT NOT NULL,
                 comment TEXT,
                 properties_json TEXT NOT NULL DEFAULT '{}',
                 owner TEXT,
                 object_id TEXT NOT NULL UNIQUE,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL,
                 PRIMARY KEY(catalog_name, name)
             );
             CREATE TABLE IF NOT EXISTS object_revisions (
                 generation INTEGER PRIMARY KEY AUTOINCREMENT,
                 object_id TEXT NOT NULL,
                 catalog_name TEXT NOT NULL,
                 schema_name TEXT NOT NULL,
                 kind TEXT NOT NULL,
                 name TEXT NOT NULL,
                 object_revision INTEGER NOT NULL CHECK(object_revision >= 1),
                 definition_json TEXT,
                 schema_ipc BLOB,
                 tombstone INTEGER NOT NULL DEFAULT 0,
                 created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000)
             );
             CREATE TABLE IF NOT EXISTS objects (
                 object_id TEXT NOT NULL UNIQUE,
                 catalog_name TEXT NOT NULL,
                 schema_name TEXT NOT NULL,
                 kind TEXT NOT NULL,
                 name TEXT NOT NULL,
                 head_generation INTEGER NOT NULL REFERENCES object_revisions(generation),
                 PRIMARY KEY(catalog_name, schema_name, kind, name),
                 FOREIGN KEY(catalog_name, schema_name) REFERENCES schemas(catalog_name, name)
                     ON UPDATE CASCADE ON DELETE CASCADE
             );
             CREATE TABLE IF NOT EXISTS callables (
                 catalog_name TEXT NOT NULL,
                 schema_name TEXT NOT NULL,
                 name TEXT NOT NULL,
                 kind TEXT NOT NULL CHECK(kind IN ('model', 'function')),
                 PRIMARY KEY(catalog_name, schema_name, name),
                 FOREIGN KEY(catalog_name, schema_name) REFERENCES schemas(catalog_name, name)
                     ON UPDATE CASCADE ON DELETE CASCADE
             );",
        )?;
        let now = Utc::now().timestamp_millis();
        connection.execute(
            "INSERT OR IGNORE INTO catalogs(name, properties_json, owner, object_id, created_at, updated_at)
             VALUES (?1, '{}', 'system', ?2, ?3, ?3)",
            params![DEFAULT_CATALOG, uuid::Uuid::new_v4().to_string(), now],
        )?;
        connection.execute(
            "INSERT OR IGNORE INTO schemas(catalog_name, name, properties_json, owner, object_id, created_at, updated_at)
             VALUES (?1, ?2, '{}', 'system', ?3, ?4, ?4)",
            params![
                DEFAULT_CATALOG,
                DEFAULT_SCHEMA,
                uuid::Uuid::new_v4().to_string(),
                now
            ],
        )?;
        connection.execute(
            "INSERT OR IGNORE INTO callables(catalog_name, schema_name, name, kind)
             SELECT catalog_name, schema_name, name, kind FROM objects
             WHERE kind IN ('model', 'function')",
            [],
        )?;
        Ok(())
    }

    fn create_default_object<T: serde::Serialize>(
        backend: &SqliteCatalogBackend,
        kind: ObjectKind,
        name: &str,
        definition: &T,
    ) -> Result<i64> {
        let mut connection = backend.lock()?;
        let transaction = if matches!(kind, ObjectKind::Model | ObjectKind::Function) {
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?
        } else {
            connection.transaction()?
        };
        if matches!(kind, ObjectKind::Model | ObjectKind::Function) {
            ensure_callable_absent(
                &transaction,
                DEFAULT_CATALOG,
                DEFAULT_SCHEMA,
                name,
                kind,
                CatalogErrorCode::AlreadyExists,
            )?;
            insert_callable(&transaction, DEFAULT_CATALOG, DEFAULT_SCHEMA, name, kind)?;
        } else {
            ensure_absent(&transaction, DEFAULT_CATALOG, DEFAULT_SCHEMA, kind, name)?;
        }
        let revision = insert_object(
            &transaction,
            DEFAULT_CATALOG,
            DEFAULT_SCHEMA,
            kind,
            name,
            definition,
            None,
        )?;
        transaction.commit()?;
        Ok(revision)
    }

    fn normalize_name(value: &str, kind: &str) -> Result<String> {
        let value = value.trim();
        let qualified_callable = matches!(kind, "model" | "function" | "callable");
        if value.is_empty()
            || value.len() > 255
            || (!qualified_callable && value.contains('.'))
            || (qualified_callable && value.split('.').any(|segment| segment.is_empty()))
            || value.chars().any(char::is_control)
        {
            return Err(CatalogError::new(
                CatalogErrorCode::InvalidArgument,
                format!("{kind} name must be 1-255 characters with non-empty path segments"),
            ));
        }
        Ok(value.to_ascii_lowercase())
    }

    fn ensure_absent(
        transaction: &Transaction<'_>,
        catalog_name: &str,
        schema_name: &str,
        kind: ObjectKind,
        name: &str,
    ) -> Result<()> {
        let name = normalize_name(name, kind.as_str())?;
        let exists = transaction
            .query_row(
                "SELECT 1 FROM objects
                 WHERE catalog_name=?1 AND schema_name=?2 AND kind=?3 AND name=?4",
                params![catalog_name, schema_name, kind.as_str(), name],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if exists {
            return Err(CatalogError::new(
                CatalogErrorCode::AlreadyExists,
                format!("{} '{name}' already exists", kind.as_str()),
            ));
        }
        Ok(())
    }

    fn ensure_callable_absent(
        transaction: &Transaction<'_>,
        catalog_name: &str,
        schema_name: &str,
        name: &str,
        requested_kind: ObjectKind,
        same_kind_code: CatalogErrorCode,
    ) -> Result<()> {
        let name = normalize_name(name, "callable")?;
        let existing = transaction
            .query_row(
                "SELECT kind FROM callables
                 WHERE catalog_name=?1 AND schema_name=?2
                   AND name=?3",
                params![catalog_name, schema_name, name],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(kind) = existing {
            let code = if kind == requested_kind.as_str() {
                same_kind_code
            } else {
                CatalogErrorCode::NameConflict
            };
            return Err(CatalogError::new(
                code,
                format!("callable name '{name}' conflicts with existing {kind}"),
            ));
        }
        Ok(())
    }

    fn insert_callable(
        transaction: &Transaction<'_>,
        catalog_name: &str,
        schema_name: &str,
        name: &str,
        kind: ObjectKind,
    ) -> Result<()> {
        let name = normalize_name(name, "callable")?;
        let result = transaction.execute(
            "INSERT INTO callables(catalog_name, schema_name, name, kind)
             VALUES (?1, ?2, ?3, ?4)",
            params![catalog_name, schema_name, name, kind.as_str()],
        );
        match result {
            Ok(_) => Ok(()),
            Err(rusqlite::Error::SqliteFailure(error, _))
                if error.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                let existing = transaction
                    .query_row(
                        "SELECT kind FROM callables
                         WHERE catalog_name=?1 AND schema_name=?2 AND name=?3",
                        params![catalog_name, schema_name, name],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?
                    .unwrap_or_else(|| "callable".to_owned());
                Err(CatalogError::new(
                    CatalogErrorCode::NameConflict,
                    format!("callable name '{name}' conflicts with existing {existing}"),
                ))
            }
            Err(error) => Err(error.into()),
        }
    }

    fn validate_resolved_versions_immutable(
        current: &ModelDef,
        replacement: &ModelDef,
    ) -> Result<()> {
        for old_version in &current.versions {
            if old_version.resolved.is_none() {
                continue;
            }
            let Some(new_version) = replacement.version(&old_version.name) else {
                continue;
            };
            if new_version != old_version {
                return Err(CatalogError::new(
                    CatalogErrorCode::Conflict,
                    format!(
                        "resolved model version '{}:{}' is immutable",
                        current.name, old_version.name
                    ),
                ));
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_object<T: serde::Serialize>(
        transaction: &Transaction<'_>,
        catalog_name: &str,
        schema_name: &str,
        kind: ObjectKind,
        name: &str,
        definition: &T,
        schema_ipc: Option<Vec<u8>>,
    ) -> Result<i64> {
        let name = normalize_name(name, kind.as_str())?;
        let definition_json = serde_json::to_string(definition)?;
        let object_id = uuid::Uuid::new_v4().to_string();
        transaction.execute(
            "INSERT INTO object_revisions(
                 object_id, catalog_name, schema_name, kind, name, object_revision, definition_json, schema_ipc
             ) VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6, ?7)",
            params![
                object_id,
                catalog_name,
                schema_name,
                kind.as_str(),
                name,
                definition_json,
                schema_ipc
            ],
        )?;
        let generation = transaction.last_insert_rowid();
        transaction.execute(
            "INSERT INTO objects(object_id, catalog_name, schema_name, kind, name, head_generation)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                object_id,
                catalog_name,
                schema_name,
                kind.as_str(),
                name,
                generation
            ],
        )?;
        Ok(1)
    }

    fn drop_object_in_transaction(
        transaction: &Transaction<'_>,
        catalog_name: &str,
        schema_name: &str,
        kind: ObjectKind,
        name: &str,
    ) -> Result<i64> {
        let catalog_name = normalize_name(catalog_name, "catalog")?;
        let schema_name = normalize_name(schema_name, "schema")?;
        let name = normalize_name(name, kind.as_str())?;
        let (object_id, current_revision) = transaction
            .query_row(
                "SELECT o.object_id, r.object_revision FROM objects o
                 JOIN object_revisions r ON r.generation=o.head_generation
                 WHERE o.catalog_name=?1 AND o.schema_name=?2 AND o.kind=?3 AND o.name=?4",
                params![catalog_name, schema_name, kind.as_str(), name],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?
            .ok_or_else(|| not_found(kind.as_str(), &name))?;
        transaction.execute(
            "DELETE FROM objects
             WHERE catalog_name=?1 AND schema_name=?2 AND kind=?3 AND name=?4",
            params![catalog_name, schema_name, kind.as_str(), name],
        )?;
        if matches!(kind, ObjectKind::Model | ObjectKind::Function) {
            transaction.execute(
                "DELETE FROM callables
                 WHERE catalog_name=?1 AND schema_name=?2 AND name=?3",
                params![catalog_name, schema_name, name],
            )?;
        }
        let revision = current_revision + 1;
        transaction.execute(
            "INSERT INTO object_revisions(
                 object_id, catalog_name, schema_name, kind, name, object_revision, tombstone
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)",
            params![
                object_id,
                catalog_name,
                schema_name,
                kind.as_str(),
                name,
                revision
            ],
        )?;
        Ok(revision)
    }

    fn load_tables(
        transaction: &Transaction<'_>,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<BTreeMap<String, SnapshotTable>> {
        let mut statement = transaction.prepare(
            "SELECT o.name, o.object_id, o.head_generation, r.object_revision, r.definition_json, r.schema_ipc
             FROM objects o JOIN object_revisions r ON r.generation=o.head_generation
             WHERE o.catalog_name=?1 AND o.schema_name=?2 AND o.kind='table'
             ORDER BY o.name",
        )?;
        let mut rows = statement.query(params![catalog_name, schema_name])?;
        let mut tables = BTreeMap::new();
        while let Some(row) = rows.next()? {
            let name: String = row.get(0)?;
            let definition_json: String = row.get(4)?;
            let schema_ipc: Vec<u8> = row.get(5)?;
            tables.insert(
                name,
                SnapshotTable {
                    object_id: row.get(1)?,
                    generation: row.get(2)?,
                    revision: row.get(3)?,
                    definition: serde_json::from_str(&definition_json)?,
                    schema: decode_schema(&schema_ipc)?,
                },
            );
        }
        Ok(tables)
    }

    fn load_objects<T: serde::de::DeserializeOwned>(
        transaction: &Transaction<'_>,
        catalog_name: &str,
        schema_name: &str,
        kind: ObjectKind,
    ) -> Result<BTreeMap<String, SnapshotObject<T>>> {
        let mut statement = transaction.prepare(
            "SELECT o.name, o.object_id, o.head_generation, r.object_revision, r.definition_json
             FROM objects o JOIN object_revisions r ON r.generation=o.head_generation
             WHERE o.catalog_name=?1 AND o.schema_name=?2 AND o.kind=?3 ORDER BY o.name",
        )?;
        let mut rows = statement.query(params![catalog_name, schema_name, kind.as_str()])?;
        let mut values = BTreeMap::new();
        while let Some(row) = rows.next()? {
            let name: String = row.get(0)?;
            let json: String = row.get(4)?;
            values.insert(
                name,
                SnapshotObject {
                    object_id: row.get(1)?,
                    generation: row.get(2)?,
                    revision: row.get(3)?,
                    definition: serde_json::from_str(&json)?,
                },
            );
        }
        Ok(values)
    }

    fn catalog_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<CatalogInfo> {
        Ok(CatalogInfo {
            name: row.get(0)?,
            metadata: SecurableMetadata {
                comment: row.get(1)?,
                properties: decode_properties(row.get::<_, String>(2)?),
                owner: row.get(3)?,
            },
            id: row.get(4)?,
            created_at: row.get(5)?,
            updated_at: row.get(6)?,
        })
    }

    fn schema_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SchemaInfo> {
        Ok(SchemaInfo {
            catalog_name: row.get(0)?,
            name: row.get(1)?,
            metadata: SecurableMetadata {
                comment: row.get(2)?,
                properties: decode_properties(row.get::<_, String>(3)?),
                owner: row.get(4)?,
            },
            id: row.get(5)?,
            created_at: row.get(6)?,
            updated_at: row.get(7)?,
        })
    }

    fn decode_properties(value: String) -> BTreeMap<String, String> {
        serde_json::from_str(&value).unwrap_or_default()
    }

    fn collect_rows<T>(
        rows: rusqlite::MappedRows<'_, impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>>,
    ) -> Result<Vec<T>> {
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn map_unique(
        result: std::result::Result<usize, rusqlite::Error>,
        kind: &str,
        name: &str,
    ) -> Result<usize> {
        match result {
            Err(rusqlite::Error::SqliteFailure(error, _))
                if error.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                Err(CatalogError::new(
                    CatalogErrorCode::AlreadyExists,
                    format!("{kind} '{name}' already exists"),
                ))
            }
            Err(error) => Err(error.into()),
            Ok(value) => Ok(value),
        }
    }

    fn not_found(kind: &str, name: &str) -> CatalogError {
        CatalogError::new(
            CatalogErrorCode::NotFound,
            format!("{kind} '{name}' does not exist"),
        )
    }

    fn not_empty(kind: &str, name: &str) -> CatalogError {
        CatalogError::new(
            CatalogErrorCode::Conflict,
            format!("{kind} '{name}' is not empty; set force=true to delete it"),
        )
    }

    #[cfg(test)]
    mod tests {
        use arrow::datatypes::{DataType, Field, Schema};
        use tempfile::tempdir;

        use super::*;
        use crate::{
            FunctionDef, FunctionImplementation, ModelDef, ModelInterface, ModelParameter,
            ModelType, ModelVersion, ResolvedExecutionSpec, ResolvedModelSpec, RuntimeSpec,
            TableProvider, TableProviderKind,
        };

        fn model(name: &str) -> ModelDef {
            ModelDef {
                name: name.to_owned(),
                interface: ModelInterface {
                    capability: Some(ModelType::ObjectDetection),
                    parameters: vec![ModelParameter {
                        name: "image".to_owned(),
                        data_type: "IMAGE".to_owned(),
                        constant: false,
                        optional: false,
                    }],
                    semantic_arguments: Vec::new(),
                    return_type: "ARRAY<STRUCT<label STRING, confidence FLOAT, box BOX2D>>"
                        .to_owned(),
                    processing_family: "vision.object_detection".to_owned(),
                    deterministic: true,
                },
                versions: vec![ModelVersion {
                    name: "v1".to_owned(),
                    source: "mock://person".to_owned(),
                    runtime_kind: "onnx-runtime".to_owned(),
                    options: BTreeMap::new(),
                    declaration_fingerprint: "declaration".to_owned(),
                    resolved: None,
                    created_at: 1,
                }],
                initial_version_fingerprint: "declaration".to_owned(),
                default_version: None,
                comment: None,
                builtin: false,
            }
        }

        fn function(name: &str) -> FunctionDef {
            FunctionDef {
                name: name.to_owned(),
                implementation: FunctionImplementation::SqlMacro {
                    expression: "$1".to_owned(),
                },
                parameters: vec![("value".to_owned(), "BIGINT".to_owned())],
                constant_parameters: Vec::new(),
                return_type: "BIGINT".to_owned(),
                semantic_fingerprint: "function".to_owned(),
            }
        }

        #[test]
        fn default_namespace_and_revisioned_table_reopen() {
            let temp = tempdir().unwrap();
            let path = temp.path().join("catalog.db");
            let schema = Arc::new(Schema::new(vec![Field::new("uri", DataType::Utf8, false)]));
            let definition = TableDef::new(
                "photos",
                TableProvider::Images {
                    location: temp.path().to_string_lossy().into_owned(),
                    recursive: true,
                },
            );
            {
                let backend = SqliteCatalogBackend::open(&path).unwrap();
                assert_eq!(backend.list_catalogs().unwrap()[0].name, DEFAULT_CATALOG);
                assert_eq!(
                    backend.list_schemas(DEFAULT_CATALOG).unwrap()[0].name,
                    DEFAULT_SCHEMA
                );
                assert_eq!(
                    backend
                        .create_table(DEFAULT_CATALOG, DEFAULT_SCHEMA, &definition, &schema)
                        .unwrap(),
                    1
                );
                assert_eq!(backend.history_count(ObjectKind::Table).unwrap(), 1);
            }
            let backend = SqliteCatalogBackend::open(&path).unwrap();
            let table = backend
                .snapshot(DEFAULT_CATALOG, DEFAULT_SCHEMA)
                .unwrap()
                .table("PHOTOS")
                .unwrap()
                .definition
                .clone();
            assert_eq!(table.provider.kind(), TableProviderKind::Images);
        }

        #[test]
        fn duplicate_create_does_not_leave_a_revision() {
            let temp = tempdir().unwrap();
            let backend = SqliteCatalogBackend::open(&temp.path().join("catalog.db")).unwrap();
            let schema = Arc::new(Schema::empty());
            let definition = TableDef::new(
                "photos",
                TableProvider::Images {
                    location: temp.path().to_string_lossy().into_owned(),
                    recursive: false,
                },
            );
            backend
                .create_table(DEFAULT_CATALOG, DEFAULT_SCHEMA, &definition, &schema)
                .unwrap();
            assert_eq!(
                backend
                    .create_table(DEFAULT_CATALOG, DEFAULT_SCHEMA, &definition, &schema)
                    .unwrap_err()
                    .code,
                CatalogErrorCode::AlreadyExists
            );
            assert_eq!(backend.history_count(ObjectKind::Table).unwrap(), 1);
        }

        #[test]
        fn revisions_are_monotonic_per_object_not_global() {
            let temp = tempdir().unwrap();
            let backend = SqliteCatalogBackend::open(&temp.path().join("catalog.db")).unwrap();
            let schema = Arc::new(Schema::empty());
            let photos = TableDef::new(
                "photos",
                TableProvider::Images {
                    location: temp.path().to_string_lossy().into_owned(),
                    recursive: false,
                },
            );
            let clips = TableDef::new(
                "clips",
                TableProvider::Videos {
                    location: temp.path().to_string_lossy().into_owned(),
                    recursive: false,
                    fps: Some(1.0),
                    start_time_ms: None,
                },
            );

            assert_eq!(backend.create_model(&model("detector")).unwrap(), 1);
            assert_eq!(
                backend
                    .create_table(DEFAULT_CATALOG, DEFAULT_SCHEMA, &photos, &schema)
                    .unwrap(),
                1
            );
            assert_eq!(
                backend
                    .create_table(DEFAULT_CATALOG, DEFAULT_SCHEMA, &clips, &schema)
                    .unwrap(),
                1
            );
            assert_eq!(backend.create_function(&function("identity")).unwrap(), 1);

            let snapshot = backend.snapshot(DEFAULT_CATALOG, DEFAULT_SCHEMA).unwrap();
            let detector = snapshot.model("detector").unwrap();
            let detector_id = detector.object_id.clone();
            let mut definition = detector.definition.clone();
            definition.comment = Some("updated".to_owned());
            assert_eq!(
                backend
                    .update_model("detector", &definition, detector.generation)
                    .unwrap(),
                2
            );
            let snapshot = backend.snapshot(DEFAULT_CATALOG, DEFAULT_SCHEMA).unwrap();
            assert_eq!(snapshot.table("photos").unwrap().revision, 1);
            assert_eq!(snapshot.table("clips").unwrap().revision, 1);
            assert_eq!(snapshot.model("detector").unwrap().revision, 2);
            assert_eq!(snapshot.function("identity").unwrap().revision, 1);
            assert_eq!(snapshot.model("detector").unwrap().object_id, detector_id);
            assert_ne!(
                snapshot.table("photos").unwrap().object_id,
                snapshot.table("clips").unwrap().object_id
            );
        }

        #[test]
        fn models_and_functions_share_one_callable_namespace() {
            let temp = tempdir().unwrap();
            let backend = SqliteCatalogBackend::open(&temp.path().join("catalog.db")).unwrap();
            backend.create_model(&model("shared")).unwrap();
            let error = backend.create_function(&function("shared")).unwrap_err();
            assert_eq!(error.code, CatalogErrorCode::NameConflict);
            assert!(error.message.contains("model"));

            backend.create_function(&function("other")).unwrap();
            let error = backend.create_model(&model("other")).unwrap_err();
            assert_eq!(error.code, CatalogErrorCode::NameConflict);
            assert!(error.message.contains("function"));
        }

        #[test]
        fn concurrent_cross_kind_create_commits_exactly_one_callable() {
            let temp = tempdir().unwrap();
            let path = temp.path().join("catalog.db");
            let model_backend = Arc::new(SqliteCatalogBackend::open(&path).unwrap());
            let function_backend = Arc::new(SqliteCatalogBackend::open(&path).unwrap());
            let barrier = Arc::new(std::sync::Barrier::new(3));
            let model_barrier = Arc::clone(&barrier);
            let model = std::thread::spawn(move || {
                model_barrier.wait();
                model_backend.create_model(&model("shared"))
            });
            let function_barrier = Arc::clone(&barrier);
            let function = std::thread::spawn(move || {
                function_barrier.wait();
                function_backend.create_function(&function("shared"))
            });
            barrier.wait();
            let results = [model.join().unwrap(), function.join().unwrap()];
            assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
            let error = results
                .iter()
                .find_map(|result| result.as_ref().err())
                .expect("one create must lose the callable namespace race");
            assert_eq!(error.code, CatalogErrorCode::NameConflict);
        }

        #[test]
        fn rename_uses_the_callable_namespace_constraint() {
            let temp = tempdir().unwrap();
            let backend = SqliteCatalogBackend::open(&temp.path().join("catalog.db")).unwrap();
            backend.create_model(&model("detector")).unwrap();
            backend.create_function(&function("occupied")).unwrap();
            let mut renamed = model("occupied");
            let generation = backend
                .snapshot(DEFAULT_CATALOG, DEFAULT_SCHEMA)
                .unwrap()
                .model("detector")
                .unwrap()
                .generation;
            let error = backend
                .update_model("detector", &renamed, generation)
                .unwrap_err();
            assert_eq!(error.code, CatalogErrorCode::NameConflict);
            renamed.name = "free".to_owned();
            backend
                .update_model("detector", &renamed, generation)
                .unwrap();
        }

        #[test]
        fn resolved_model_versions_are_immutable_across_aggregate_updates() {
            let temp = tempdir().unwrap();
            let backend = SqliteCatalogBackend::open(&temp.path().join("catalog.db")).unwrap();
            let mut definition = model("detector");
            definition.versions[0].resolved = Some(ResolvedModelSpec {
                resolved_source: "mock://person".to_owned(),
                artifact_hash: Some("hash".to_owned()),
                execution: ResolvedExecutionSpec::Generic {
                    runtime: RuntimeSpec {
                        kind: "onnx-runtime".to_owned(),
                        protocol: None,
                        options: BTreeMap::new(),
                    },
                    inputs: Vec::new(),
                    outputs: Vec::new(),
                },
                semantic_fingerprint: "resolved".to_owned(),
                volatile: false,
            });
            backend.create_model(&definition).unwrap();
            let generation = backend
                .snapshot(DEFAULT_CATALOG, DEFAULT_SCHEMA)
                .unwrap()
                .model("detector")
                .unwrap()
                .generation;
            definition.versions[0].source = "mock://changed".to_owned();
            let error = backend
                .update_model("detector", &definition, generation)
                .unwrap_err();
            assert_eq!(error.code, CatalogErrorCode::Conflict);
            assert!(error.message.contains("immutable"));
        }
    }
}

#[cfg(feature = "sqlite")]
pub use sqlite::SqliteCatalogBackend;
