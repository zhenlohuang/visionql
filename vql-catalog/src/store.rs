use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use crate::{
    CatalogError, CatalogErrorCode, CatalogInfo, CreateJob, DEFAULT_CATALOG, DEFAULT_SCHEMA,
    DefinitionSnapshot, FunctionDef, JobDefinition, JobState, JobStatus, ModelDef, ObjectKind,
    PersistentJob, Result, SchemaInfo, SecurableMetadata, SnapshotObject, SnapshotTable, TableDef,
    provider_schema,
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
    fn snapshot_at_generations(
        &self,
        catalog_name: &str,
        schema_name: &str,
        generations: &[i64],
    ) -> Result<DefinitionSnapshot>;
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

    fn create_job(&self, job: &CreateJob) -> Result<PersistentJob>;
    fn get_job(&self, job_id: &str) -> Result<PersistentJob>;
    fn list_jobs(&self) -> Result<Vec<PersistentJob>>;
    fn compare_and_swap_job_status(
        &self,
        job_id: &str,
        expected_status_version: i64,
        status: &JobStatus,
    ) -> Result<JobStatus>;
    fn prune_terminal_jobs(
        &self,
        retain_count: Option<usize>,
        older_than: Option<i64>,
    ) -> Result<usize>;
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

    pub fn snapshot_at_generations(&self, generations: &[i64]) -> Result<DefinitionSnapshot> {
        self.snapshot_at_generations_in(DEFAULT_CATALOG, DEFAULT_SCHEMA, generations)
    }

    pub fn snapshot_at_generations_in(
        &self,
        catalog_name: &str,
        schema_name: &str,
        generations: &[i64],
    ) -> Result<DefinitionSnapshot> {
        self.backend
            .snapshot_at_generations(catalog_name, schema_name, generations)
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

    pub fn create_job(&self, job: &CreateJob) -> Result<PersistentJob> {
        self.backend.create_job(job)
    }

    pub fn get_job(&self, job_id: &str) -> Result<PersistentJob> {
        self.backend.get_job(job_id)
    }

    pub fn list_jobs(&self) -> Result<Vec<PersistentJob>> {
        self.backend.list_jobs()
    }

    pub fn compare_and_swap_job_status(
        &self,
        job_id: &str,
        expected_status_version: i64,
        status: &JobStatus,
    ) -> Result<JobStatus> {
        self.backend
            .compare_and_swap_job_status(job_id, expected_status_version, status)
    }

    pub fn prune_terminal_jobs(
        &self,
        retain_count: Option<usize>,
        older_than: Option<i64>,
    ) -> Result<usize> {
        self.backend.prune_terminal_jobs(retain_count, older_than)
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

        fn snapshot_at_generations(
            &self,
            catalog_name: &str,
            schema_name: &str,
            generations: &[i64],
        ) -> Result<DefinitionSnapshot> {
            let catalog_name = normalize_name(catalog_name, "catalog")?;
            let schema_name = normalize_name(schema_name, "schema")?;
            self.get_schema(&catalog_name, &schema_name)?;
            let mut connection = self.lock()?;
            let transaction = connection.transaction()?;
            let snapshot = load_snapshot_at_generations(
                &transaction,
                &catalog_name,
                &schema_name,
                generations,
            )?;
            transaction.commit()?;
            Ok(snapshot)
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

        fn create_job(&self, job: &CreateJob) -> Result<PersistentJob> {
            let catalog_name = normalize_name(&job.catalog_name, "catalog")?;
            let schema_name = normalize_name(&job.schema_name, "schema")?;
            let name = normalize_name(&job.name, "job")?;
            if job.principal.trim().is_empty() {
                return Err(CatalogError::new(
                    CatalogErrorCode::InvalidArgument,
                    "Job principal cannot be empty",
                ));
            }
            if job.normalized_sql.trim().is_empty() {
                return Err(CatalogError::new(
                    CatalogErrorCode::InvalidArgument,
                    "Job SQL cannot be empty",
                ));
            }
            let mut generations = job.definition_generations.clone();
            generations.sort_unstable();
            generations.dedup();
            if generations.is_empty() {
                return Err(CatalogError::new(
                    CatalogErrorCode::InvalidArgument,
                    "persistent Job must pin at least one definition generation",
                ));
            }

            let now = Utc::now().timestamp_millis();
            let job_id = uuid::Uuid::new_v4().to_string();
            let settings = serde_json::to_string(&job.session_settings)?;
            let mut connection = self.lock()?;
            let transaction =
                connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let schema_exists = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM schemas WHERE catalog_name=?1 AND name=?2)",
                params![catalog_name, schema_name],
                |row| row.get::<_, bool>(0),
            )?;
            if !schema_exists {
                return Err(not_found(
                    "schema",
                    &format!("{catalog_name}.{schema_name}"),
                ));
            }
            for generation in &generations {
                let valid = transaction.query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM object_revisions
                         WHERE generation=?1 AND catalog_name=?2 AND schema_name=?3 AND tombstone=0
                     )",
                    params![generation, catalog_name, schema_name],
                    |row| row.get::<_, bool>(0),
                )?;
                if !valid {
                    return Err(CatalogError::new(
                        CatalogErrorCode::Conflict,
                        format!(
                            "definition generation {generation} is unavailable in {catalog_name}.{schema_name}"
                        ),
                    ));
                }
            }
            let inserted = transaction.execute(
                "INSERT INTO jobs(
                     job_id, catalog_name, schema_name, name, principal, normalized_sql,
                     sql_redacted, session_settings_json, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    job_id,
                    catalog_name,
                    schema_name,
                    name,
                    job.principal.trim(),
                    job.normalized_sql,
                    job.sql_redacted,
                    settings,
                    now
                ],
            );
            map_job_unique(inserted, &name)?;
            for generation in &generations {
                transaction.execute(
                    "INSERT INTO job_dependencies(job_id, generation) VALUES (?1, ?2)",
                    params![job_id, generation],
                )?;
            }
            let status = JobStatus::starting(now);
            insert_job_status(&transaction, &job_id, &status)?;
            transaction.commit()?;
            drop(connection);
            self.get_job(&job_id)
        }

        fn get_job(&self, job_id: &str) -> Result<PersistentJob> {
            let connection = self.lock()?;
            load_job(&connection, job_id)?.ok_or_else(|| not_found("job", job_id))
        }

        fn list_jobs(&self) -> Result<Vec<PersistentJob>> {
            let connection = self.lock()?;
            let mut statement = connection
                .prepare("SELECT job_id FROM jobs ORDER BY created_at DESC, job_id DESC")?;
            let ids = statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            ids.into_iter()
                .map(|job_id| {
                    load_job(&connection, &job_id)?.ok_or_else(|| {
                        CatalogError::new(
                            CatalogErrorCode::Internal,
                            "Job disappeared while it was being listed",
                        )
                    })
                })
                .collect()
        }

        fn compare_and_swap_job_status(
            &self,
            job_id: &str,
            expected_status_version: i64,
            status: &JobStatus,
        ) -> Result<JobStatus> {
            let mut connection = self.lock()?;
            let transaction =
                connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let next_version = expected_status_version.checked_add(1).ok_or_else(|| {
                CatalogError::new(CatalogErrorCode::Internal, "Job status version overflowed")
            })?;
            let updated = transaction.execute(
                "UPDATE job_status SET
                     state=?1, status_version=?2, stop_requested=?3, source_health=?4,
                     last_event_time=?5, started_at=?6, updated_at=?7, last_restart_at=?8,
                     restart_gap_count=?9, restart_gap_started_at=?10,
                     restart_gap_ended_at=?11, last_restart_reset_window_state=?12,
                     error_code=?13, error_message=?14
                 WHERE job_id=?15 AND status_version=?16",
                params![
                    status.state.as_str(),
                    next_version,
                    status.stop_requested,
                    status.source_health,
                    status.last_event_time,
                    status.started_at,
                    status.updated_at,
                    status.last_restart_at,
                    status.restart_gap_count,
                    status.restart_gap_started_at,
                    status.restart_gap_ended_at,
                    status.last_restart_reset_window_state,
                    status.error_code,
                    status.error_message,
                    job_id,
                    expected_status_version
                ],
            )?;
            if updated == 0 {
                let exists = transaction.query_row(
                    "SELECT EXISTS(SELECT 1 FROM jobs WHERE job_id=?1)",
                    [job_id],
                    |row| row.get::<_, bool>(0),
                )?;
                return Err(if exists {
                    CatalogError::new(
                        CatalogErrorCode::Conflict,
                        format!("Job '{job_id}' status changed; retry the operation"),
                    )
                } else {
                    not_found("job", job_id)
                });
            }
            transaction.execute(
                "UPDATE jobs SET terminal=?1 WHERE job_id=?2",
                params![status.state.is_terminal(), job_id],
            )?;
            transaction.commit()?;
            let mut persisted = status.clone();
            persisted.status_version = next_version;
            Ok(persisted)
        }

        fn prune_terminal_jobs(
            &self,
            retain_count: Option<usize>,
            older_than: Option<i64>,
        ) -> Result<usize> {
            if retain_count.is_none() && older_than.is_none() {
                return Ok(0);
            }
            let mut connection = self.lock()?;
            let transaction =
                connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let mut statement = transaction.prepare(
                "SELECT q.job_id, s.updated_at
                 FROM jobs q JOIN job_status s USING(job_id)
                 WHERE q.terminal=1 ORDER BY s.updated_at DESC, q.job_id DESC",
            )?;
            let terminal = statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            drop(statement);
            let ids = terminal
                .into_iter()
                .enumerate()
                .filter(|(index, (_, updated_at))| {
                    retain_count.is_some_and(|retain| *index >= retain)
                        || older_than.is_some_and(|cutoff| *updated_at < cutoff)
                })
                .map(|(_, (job_id, _))| job_id)
                .collect::<Vec<_>>();
            for job_id in &ids {
                transaction.execute("DELETE FROM jobs WHERE job_id=?1", [job_id])?;
            }
            transaction.commit()?;
            Ok(ids.len())
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
             );
             CREATE TABLE IF NOT EXISTS jobs (
                 job_id TEXT PRIMARY KEY,
                 catalog_name TEXT NOT NULL,
                 schema_name TEXT NOT NULL,
                 name TEXT NOT NULL,
                 principal TEXT NOT NULL,
                 normalized_sql TEXT NOT NULL,
                 sql_redacted TEXT NOT NULL,
                 session_settings_json TEXT NOT NULL DEFAULT '{}',
                 created_at INTEGER NOT NULL,
                 terminal INTEGER NOT NULL DEFAULT 0,
                 FOREIGN KEY(catalog_name, schema_name) REFERENCES schemas(catalog_name, name)
                     ON UPDATE CASCADE ON DELETE RESTRICT
             );
             CREATE UNIQUE INDEX IF NOT EXISTS jobs_active_name
                 ON jobs(catalog_name, schema_name, name) WHERE terminal=0;
             CREATE TABLE IF NOT EXISTS job_dependencies (
                 job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
                 generation INTEGER NOT NULL REFERENCES object_revisions(generation) ON DELETE RESTRICT,
                 PRIMARY KEY(job_id, generation)
             );
             CREATE TABLE IF NOT EXISTS job_status (
                 job_id TEXT PRIMARY KEY REFERENCES jobs(job_id) ON DELETE CASCADE,
                 state TEXT NOT NULL CHECK(state IN ('STARTING', 'RUNNING', 'STOPPED', 'FAILED')),
                 status_version INTEGER NOT NULL CHECK(status_version >= 1),
                 stop_requested INTEGER NOT NULL DEFAULT 0,
                 source_health TEXT,
                 last_event_time INTEGER,
                 started_at INTEGER,
                 updated_at INTEGER NOT NULL,
                 last_restart_at INTEGER,
                 restart_gap_count INTEGER NOT NULL DEFAULT 0,
                 restart_gap_started_at INTEGER,
                 restart_gap_ended_at INTEGER,
                 last_restart_reset_window_state INTEGER NOT NULL DEFAULT 0,
                 error_code TEXT,
                 error_message TEXT
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

    fn load_snapshot_at_generations(
        transaction: &Transaction<'_>,
        catalog_name: &str,
        schema_name: &str,
        generations: &[i64],
    ) -> Result<DefinitionSnapshot> {
        let mut requested = generations.to_vec();
        requested.sort_unstable();
        requested.dedup();
        if requested.len() != generations.len() {
            return Err(CatalogError::new(
                CatalogErrorCode::InvalidArgument,
                "definition generations must not contain duplicates",
            ));
        }
        let mut tables = BTreeMap::new();
        let mut models = BTreeMap::new();
        let mut functions = BTreeMap::new();
        for generation in requested {
            let revision = transaction
                .query_row(
                    "SELECT object_id, kind, name, object_revision, definition_json, schema_ipc
                     FROM object_revisions
                     WHERE generation=?1 AND catalog_name=?2 AND schema_name=?3 AND tombstone=0",
                    params![generation, catalog_name, schema_name],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, Option<String>>(4)?,
                            row.get::<_, Option<Vec<u8>>>(5)?,
                        ))
                    },
                )
                .optional()?
                .ok_or_else(|| {
                    CatalogError::new(
                        CatalogErrorCode::NotFound,
                        format!("definition generation {generation} does not exist"),
                    )
                })?;
            let (object_id, kind, name, revision, definition, schema) = revision;
            let definition = definition.ok_or_else(|| {
                CatalogError::new(
                    CatalogErrorCode::Storage,
                    format!("definition generation {generation} has no definition"),
                )
            })?;
            match kind.as_str() {
                "table" => {
                    let schema = schema.ok_or_else(|| {
                        CatalogError::new(
                            CatalogErrorCode::Storage,
                            format!("table generation {generation} has no Arrow schema"),
                        )
                    })?;
                    let previous = tables.insert(
                        name,
                        SnapshotTable {
                            object_id,
                            generation,
                            revision,
                            definition: serde_json::from_str(&definition)?,
                            schema: decode_schema(&schema)?,
                        },
                    );
                    if previous.is_some() {
                        return Err(CatalogError::new(
                            CatalogErrorCode::InvalidArgument,
                            "definition generations contain multiple revisions of one Table",
                        ));
                    }
                }
                "model" => {
                    let previous = models.insert(
                        name,
                        SnapshotObject {
                            object_id,
                            generation,
                            revision,
                            definition: serde_json::from_str(&definition)?,
                        },
                    );
                    if previous.is_some() {
                        return Err(CatalogError::new(
                            CatalogErrorCode::InvalidArgument,
                            "definition generations contain multiple revisions of one Model",
                        ));
                    }
                }
                "function" => {
                    let previous = functions.insert(
                        name,
                        SnapshotObject {
                            object_id,
                            generation,
                            revision,
                            definition: serde_json::from_str(&definition)?,
                        },
                    );
                    if previous.is_some() {
                        return Err(CatalogError::new(
                            CatalogErrorCode::InvalidArgument,
                            "definition generations contain multiple revisions of one Function",
                        ));
                    }
                }
                _ => {
                    return Err(CatalogError::new(
                        CatalogErrorCode::Storage,
                        format!("definition generation {generation} has an invalid object kind"),
                    ));
                }
            }
        }
        Ok(DefinitionSnapshot::new(tables, models, functions))
    }

    fn insert_job_status(
        transaction: &Transaction<'_>,
        job_id: &str,
        status: &JobStatus,
    ) -> Result<()> {
        transaction.execute(
            "INSERT INTO job_status(
                 job_id, state, status_version, stop_requested, source_health, last_event_time,
                 started_at, updated_at, last_restart_at, restart_gap_count,
                 restart_gap_started_at, restart_gap_ended_at, last_restart_reset_window_state,
                 error_code, error_message
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            params![
                job_id,
                status.state.as_str(),
                status.status_version,
                status.stop_requested,
                status.source_health,
                status.last_event_time,
                status.started_at,
                status.updated_at,
                status.last_restart_at,
                status.restart_gap_count,
                status.restart_gap_started_at,
                status.restart_gap_ended_at,
                status.last_restart_reset_window_state,
                status.error_code,
                status.error_message
            ],
        )?;
        Ok(())
    }

    fn load_job(connection: &Connection, job_id: &str) -> Result<Option<PersistentJob>> {
        let row = connection
            .query_row(
                "SELECT q.job_id, q.catalog_name, q.schema_name, q.name, q.principal,
                        q.normalized_sql, q.sql_redacted, q.session_settings_json, q.created_at,
                        s.state, s.status_version, s.stop_requested, s.source_health,
                        s.last_event_time, s.started_at, s.updated_at, s.last_restart_at,
                        s.restart_gap_count, s.restart_gap_started_at, s.restart_gap_ended_at,
                        s.last_restart_reset_window_state, s.error_code, s.error_message
                 FROM jobs q JOIN job_status s USING(job_id) WHERE q.job_id=?1",
                [job_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, i64>(8)?,
                        row.get::<_, String>(9)?,
                        row.get::<_, i64>(10)?,
                        row.get::<_, bool>(11)?,
                        row.get::<_, Option<String>>(12)?,
                        row.get::<_, Option<i64>>(13)?,
                        row.get::<_, Option<i64>>(14)?,
                        row.get::<_, i64>(15)?,
                        row.get::<_, Option<i64>>(16)?,
                        row.get::<_, i64>(17)?,
                        row.get::<_, Option<i64>>(18)?,
                        row.get::<_, Option<i64>>(19)?,
                        row.get::<_, bool>(20)?,
                        row.get::<_, Option<String>>(21)?,
                        row.get::<_, Option<String>>(22)?,
                    ))
                },
            )
            .optional()?;
        let Some(row) = row else {
            return Ok(None);
        };
        let mut dependencies = connection.prepare(
            "SELECT generation FROM job_dependencies WHERE job_id=?1 ORDER BY generation",
        )?;
        let generations = dependencies
            .query_map([job_id], |row| row.get::<_, i64>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let settings = serde_json::from_str(&row.7)?;
        Ok(Some(PersistentJob {
            definition: JobDefinition {
                job_id: row.0,
                catalog_name: row.1,
                schema_name: row.2,
                name: row.3,
                principal: row.4,
                normalized_sql: row.5,
                sql_redacted: row.6,
                session_settings: settings,
                definition_generations: generations,
                created_at: row.8,
            },
            status: JobStatus {
                state: JobState::try_from(row.9.as_str())?,
                status_version: row.10,
                stop_requested: row.11,
                source_health: row.12,
                last_event_time: row.13,
                started_at: row.14,
                updated_at: row.15,
                last_restart_at: row.16,
                restart_gap_count: row.17,
                restart_gap_started_at: row.18,
                restart_gap_ended_at: row.19,
                last_restart_reset_window_state: row.20,
                error_code: row.21,
                error_message: row.22,
            },
        }))
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

    fn map_job_unique(
        result: std::result::Result<usize, rusqlite::Error>,
        name: &str,
    ) -> Result<usize> {
        match result {
            Err(rusqlite::Error::SqliteFailure(error, _))
                if error.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                Err(CatalogError::new(
                    CatalogErrorCode::AlreadyExists,
                    format!("active Job '{name}' already exists"),
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

        #[test]
        fn persistent_job_pins_generations_and_status_updates_use_cas() {
            let temp = tempdir().unwrap();
            let backend = SqliteCatalogBackend::open(&temp.path().join("catalog.db")).unwrap();
            backend.create_model(&model("detector")).unwrap();
            let generation = backend
                .snapshot(DEFAULT_CATALOG, DEFAULT_SCHEMA)
                .unwrap()
                .model("detector")
                .unwrap()
                .generation;
            let job = backend
                .create_job(&CreateJob {
                    catalog_name: DEFAULT_CATALOG.to_owned(),
                    schema_name: DEFAULT_SCHEMA.to_owned(),
                    name: "people_per_minute".to_owned(),
                    principal: "service".to_owned(),
                    normalized_sql: "INSERT INTO sink SELECT detector(image) FROM camera"
                        .to_owned(),
                    sql_redacted: "INSERT INTO sink SELECT detector(image) FROM camera".to_owned(),
                    session_settings: BTreeMap::new(),
                    definition_generations: vec![generation],
                })
                .unwrap();

            assert_eq!(job.status.state, JobState::Starting);
            assert_eq!(job.definition.definition_generations, vec![generation]);
            assert!(
                backend
                    .snapshot_at_generations(
                        DEFAULT_CATALOG,
                        DEFAULT_SCHEMA,
                        &job.definition.definition_generations,
                    )
                    .unwrap()
                    .model("detector")
                    .is_some()
            );

            let mut running = job.status.clone();
            running.state = JobState::Running;
            running.started_at = Some(running.updated_at);
            let running = backend
                .compare_and_swap_job_status(
                    &job.definition.job_id,
                    job.status.status_version,
                    &running,
                )
                .unwrap();
            assert_eq!(running.status_version, 2);
            let error = backend
                .compare_and_swap_job_status(
                    &job.definition.job_id,
                    job.status.status_version,
                    &running,
                )
                .unwrap_err();
            assert_eq!(error.code, CatalogErrorCode::Conflict);
            drop(backend);
            let reopened = SqliteCatalogBackend::open(&temp.path().join("catalog.db")).unwrap();
            let persisted = reopened.get_job(&job.definition.job_id).unwrap();
            assert_eq!(persisted.definition, job.definition);
            assert_eq!(persisted.status, running);
        }

        #[test]
        fn active_job_names_are_unique_and_terminal_history_is_prunable() {
            let temp = tempdir().unwrap();
            let backend = SqliteCatalogBackend::open(&temp.path().join("catalog.db")).unwrap();
            backend.create_model(&model("detector")).unwrap();
            let generation = backend
                .snapshot(DEFAULT_CATALOG, DEFAULT_SCHEMA)
                .unwrap()
                .model("detector")
                .unwrap()
                .generation;
            let create = CreateJob {
                catalog_name: DEFAULT_CATALOG.to_owned(),
                schema_name: DEFAULT_SCHEMA.to_owned(),
                name: "watch".to_owned(),
                principal: "service".to_owned(),
                normalized_sql: "INSERT INTO sink SELECT 1".to_owned(),
                sql_redacted: "INSERT INTO sink SELECT 1".to_owned(),
                session_settings: BTreeMap::new(),
                definition_generations: vec![generation],
            };
            let first = backend.create_job(&create).unwrap();
            assert_eq!(
                backend.create_job(&create).unwrap_err().code,
                CatalogErrorCode::AlreadyExists
            );
            let mut stopped = first.status.clone();
            stopped.state = JobState::Stopped;
            stopped.stop_requested = true;
            backend
                .compare_and_swap_job_status(
                    &first.definition.job_id,
                    first.status.status_version,
                    &stopped,
                )
                .unwrap();
            backend.create_job(&create).unwrap();
            assert_eq!(backend.prune_terminal_jobs(Some(0), None).unwrap(), 1);
            assert_eq!(backend.list_jobs().unwrap().len(), 1);
        }
    }
}

#[cfg(feature = "sqlite")]
pub use sqlite::SqliteCatalogBackend;
