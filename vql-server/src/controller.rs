use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use tokio::sync::Notify;
use vql_catalog::{CatalogStore, CreateQuery, PersistentQuery, QueryState};
use vql_kernel::{
    Engine, ErrorCode, PersistentCommand, PreparedResult, PreparedStatement, QueryHandle, Result,
    VqlError,
};

#[derive(Debug)]
struct ActiveQuery {
    handle: QueryHandle,
    done: Arc<Notify>,
}

#[derive(Debug)]
pub struct QueryController {
    engine: Engine,
    catalog: Arc<CatalogStore>,
    active: Arc<Mutex<HashMap<String, ActiveQuery>>>,
    shutting_down: Arc<AtomicBool>,
    terminal_history_count: usize,
    terminal_history_days: u64,
}

impl QueryController {
    pub fn new(engine: Engine, terminal_history_count: usize, terminal_history_days: u64) -> Self {
        Self {
            catalog: engine.catalog(),
            engine,
            active: Arc::new(Mutex::new(HashMap::new())),
            shutting_down: Arc::new(AtomicBool::new(false)),
            terminal_history_count,
            terminal_history_days,
        }
    }

    pub async fn execute(&self, prepared: &PreparedStatement) -> Result<Vec<RecordBatch>> {
        let command = prepared.persistent_command().ok_or_else(|| {
            VqlError::new(
                ErrorCode::InvalidArgument,
                "statement is not a persistent Query command",
            )
        })?;
        match command {
            PersistentCommand::Submit { name, sql } => self.submit(prepared, name, sql).await,
            PersistentCommand::Show => self.show(prepared.result_schema()),
            PersistentCommand::Describe { query_id } => {
                self.describe(prepared.result_schema(), query_id)
            }
            PersistentCommand::Stop { query_id } => {
                self.stop(prepared.result_schema(), query_id).await
            }
        }
    }

    async fn submit(
        &self,
        prepared: &PreparedStatement,
        name: &str,
        sql: &str,
    ) -> Result<Vec<RecordBatch>> {
        let query = self.catalog.create_query(&CreateQuery {
            catalog_name: vql_catalog::DEFAULT_CATALOG.to_owned(),
            schema_name: vql_catalog::DEFAULT_SCHEMA.to_owned(),
            name: name.to_owned(),
            principal: prepared.principal().to_owned(),
            normalized_sql: sql.to_owned(),
            sql_redacted: redact_sql(sql),
            session_settings: prepared.session_settings().clone(),
            definition_generations: prepared.definition_generations().to_vec(),
        })?;
        if let Err(error) = self.launch(query.clone()).await {
            self.fail_query(&query.definition.query_id, &error)?;
            return Err(error);
        }
        let query = self.catalog.get_query(&query.definition.query_id)?;
        Ok(vec![query_identity_batch(
            prepared.result_schema(),
            &query,
        )?])
    }

    fn show(&self, schema: SchemaRef) -> Result<Vec<RecordBatch>> {
        let queries = self.catalog.list_queries()?;
        let query_ids = queries
            .iter()
            .map(|query| query.definition.query_id.as_str())
            .collect::<Vec<_>>();
        let names = queries
            .iter()
            .map(|query| query.definition.name.as_str())
            .collect::<Vec<_>>();
        let states = queries
            .iter()
            .map(|query| query.status.state.as_str())
            .collect::<Vec<_>>();
        let health = queries
            .iter()
            .map(|query| query.status.source_health.as_deref())
            .collect::<Vec<_>>();
        let last_event = queries
            .iter()
            .map(|query| query.status.last_event_time)
            .collect::<Vec<_>>();
        let started = queries
            .iter()
            .map(|query| query.status.started_at)
            .collect::<Vec<_>>();
        let updated = queries
            .iter()
            .map(|query| Some(query.status.updated_at))
            .collect::<Vec<_>>();
        let gaps = queries
            .iter()
            .map(|query| query.status.restart_gap_count)
            .collect::<Vec<_>>();
        let codes = queries
            .iter()
            .map(|query| query.status.error_code.as_deref())
            .collect::<Vec<_>>();
        let messages = queries
            .iter()
            .map(|query| query.status.error_message.as_deref())
            .collect::<Vec<_>>();
        Ok(vec![RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(query_ids)) as ArrayRef,
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(states)),
                Arc::new(StringArray::from(health)),
                timestamp_array(last_event),
                timestamp_array(started),
                timestamp_array(updated),
                Arc::new(Int64Array::from(gaps)),
                Arc::new(StringArray::from(codes)),
                Arc::new(StringArray::from(messages)),
            ],
        )?])
    }

    fn describe(&self, schema: SchemaRef, query_id: &str) -> Result<Vec<RecordBatch>> {
        let query = self.catalog.get_query(query_id)?;
        Ok(vec![RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![query.definition.query_id.as_str()])) as ArrayRef,
                Arc::new(StringArray::from(vec![query.definition.name.as_str()])),
                Arc::new(StringArray::from(vec![query.status.state.as_str()])),
                Arc::new(StringArray::from(vec![
                    query.definition.sql_redacted.as_str(),
                ])),
                timestamp_array(vec![Some(query.definition.created_at)]),
                timestamp_array(vec![query.status.started_at]),
                timestamp_array(vec![Some(query.status.updated_at)]),
                timestamp_array(vec![query.status.last_restart_at]),
                timestamp_array(vec![query.status.restart_gap_started_at]),
                timestamp_array(vec![query.status.restart_gap_ended_at]),
                Arc::new(BooleanArray::from(vec![
                    query.status.last_restart_reset_window_state,
                ])),
                Arc::new(StringArray::from(vec![query.status.error_code.as_deref()])),
                Arc::new(StringArray::from(vec![
                    query.status.error_message.as_deref(),
                ])),
            ],
        )?])
    }

    async fn stop(&self, schema: SchemaRef, query_id: &str) -> Result<Vec<RecordBatch>> {
        let query = self.catalog.get_query(query_id)?;
        if !query.status.state.is_terminal() {
            let mut requested = query.status.clone();
            requested.stop_requested = true;
            requested.updated_at = Utc::now().timestamp_millis();
            self.catalog.compare_and_swap_query_status(
                query_id,
                query.status.status_version,
                &requested,
            )?;
            let done = self
                .active
                .lock()
                .map_err(|_| VqlError::new(ErrorCode::Internal, "Query registry was poisoned"))?
                .get(query_id)
                .map(|active| {
                    active.handle.cancel();
                    Arc::clone(&active.done)
                });
            if let Some(done) = done {
                let _ = tokio::time::timeout(Duration::from_secs(10), done.notified()).await;
            }
            let current = self.catalog.get_query(query_id)?;
            if !current.status.state.is_terminal() && current.status.stop_requested {
                let mut stopped = current.status.clone();
                stopped.state = QueryState::Stopped;
                stopped.updated_at = Utc::now().timestamp_millis();
                let _ = self.catalog.compare_and_swap_query_status(
                    query_id,
                    current.status.status_version,
                    &stopped,
                );
            }
        }
        let query = self.catalog.get_query(query_id)?;
        let batch = query_identity_batch(schema, &query)?;
        if query.status.state.is_terminal() {
            self.prune_history_best_effort();
        }
        Ok(vec![batch])
    }

    pub async fn recover(&self) -> Result<()> {
        let queries = self.catalog.list_queries()?;
        for query in queries {
            if query.status.state.is_terminal() {
                continue;
            }
            if query.status.stop_requested {
                self.complete_stopped(&query.definition.query_id)?;
                continue;
            }
            let now = Utc::now().timestamp_millis();
            let mut restarting = query.status.clone();
            restarting.state = QueryState::Starting;
            restarting.updated_at = now;
            restarting.last_restart_at = Some(now);
            restarting.restart_gap_count += 1;
            restarting.restart_gap_started_at = query.status.last_event_time;
            restarting.restart_gap_ended_at = None;
            restarting.last_restart_reset_window_state = false;
            restarting.error_code = None;
            restarting.error_message = None;
            let restarting = self.catalog.compare_and_swap_query_status(
                &query.definition.query_id,
                query.status.status_version,
                &restarting,
            )?;
            let mut query = query;
            query.status = restarting;
            if let Err(error) = self.launch(query.clone()).await {
                self.fail_query(&query.definition.query_id, &error)?;
            }
        }
        self.prune_history()?;
        Ok(())
    }

    async fn launch(&self, query: PersistentQuery) -> Result<()> {
        let engine = self.engine.clone();
        let execution_query = query.clone();
        let prepared_result = tokio::task::spawn_blocking(move || {
            let session = engine.session().for_service().build()?;
            let prepared = session.prepare_pinned(
                &execution_query.definition.normalized_sql,
                &execution_query.definition.principal,
                execution_query.definition.session_settings.clone(),
                &execution_query.definition.definition_generations,
            )?;
            prepared.execute_query()
        })
        .await
        .map_err(|error| {
            VqlError::new(
                ErrorCode::Internal,
                format!("persistent Query launch task failed: {error}"),
            )
        })??;
        let PreparedResult::Query(handle) = prepared_result else {
            return Err(VqlError::new(
                ErrorCode::Internal,
                "persistent Query preparation did not produce an execution handle",
            ));
        };
        let handle = *handle;
        if !handle.is_unbounded() {
            return Err(VqlError::new(
                ErrorCode::InvalidSql,
                "persistent Query is no longer unbounded",
            ));
        }
        let resets_window_state = handle.resets_window_state_on_restart();
        let mut progress = handle.subscribe_progress();
        let query_id = query.definition.query_id.clone();
        let done = Arc::new(Notify::new());
        self.active
            .lock()
            .map_err(|_| VqlError::new(ErrorCode::Internal, "Query registry was poisoned"))?
            .insert(
                query_id.clone(),
                ActiveQuery {
                    handle: handle.clone(),
                    done: Arc::clone(&done),
                },
            );
        let now = Utc::now().timestamp_millis();
        let mut running = query.status.clone();
        running.state = QueryState::Running;
        running.started_at.get_or_insert(now);
        running.updated_at = now;
        running.source_health = None;
        running.last_restart_reset_window_state =
            running.last_restart_at.is_some() && resets_window_state;
        running.error_code = None;
        running.error_message = None;
        if let Err(error) = self.catalog.compare_and_swap_query_status(
            &query.definition.query_id,
            query.status.status_version,
            &running,
        ) {
            handle.cancel();
            if let Ok(mut active) = self.active.lock() {
                active.remove(&query_id);
            }
            done.notify_one();
            return Err(error.into());
        }
        let catalog = Arc::clone(&self.catalog);
        let active = Arc::clone(&self.active);
        let shutting_down = Arc::clone(&self.shutting_down);
        let progress_catalog = Arc::clone(&catalog);
        let progress_query_id = query_id.clone();
        let terminal_history_count = self.terminal_history_count;
        let terminal_history_days = self.terminal_history_days;
        tokio::spawn(async move {
            while progress.changed().await.is_ok() {
                let observed = *progress.borrow_and_update();
                let Ok(current) = progress_catalog.get_query(&progress_query_id) else {
                    break;
                };
                if current.status.state.is_terminal() || current.status.stop_requested {
                    break;
                }
                let source_health = observed
                    .source_health
                    .map(|health| health.as_str().to_owned());
                let last_event_time =
                    match (current.status.last_event_time, observed.last_event_time) {
                        (Some(current), Some(observed)) => Some(current.max(observed)),
                        (current, observed) => current.or(observed),
                    };
                if current.status.source_health == source_health
                    && current.status.last_event_time == last_event_time
                {
                    continue;
                }
                let mut update = current.status.clone();
                update.source_health = source_health;
                update.last_event_time = last_event_time;
                update.updated_at = Utc::now().timestamp_millis();
                if update.last_restart_at.is_some()
                    && update.restart_gap_ended_at.is_none()
                    && observed.last_event_time.is_some()
                {
                    update.restart_gap_ended_at = observed.last_event_time;
                }
                let _ = progress_catalog.compare_and_swap_query_status(
                    &progress_query_id,
                    current.status.status_version,
                    &update,
                );
            }
        });
        tokio::task::spawn_blocking(move || {
            let result = handle.for_each_batch(|_| Ok(()));
            if let Ok(mut registry) = active.lock() {
                registry.remove(&query_id);
            }
            if !shutting_down.load(Ordering::Relaxed)
                && let Ok(current) = catalog.get_query(&query_id)
                && !current.status.state.is_terminal()
            {
                let mut terminal = current.status.clone();
                terminal.updated_at = Utc::now().timestamp_millis();
                if current.status.stop_requested {
                    terminal.state = QueryState::Stopped;
                } else {
                    terminal.state = QueryState::Failed;
                    let error = match result {
                        Ok(()) => VqlError::new(
                            ErrorCode::Execution,
                            "persistent Query ended unexpectedly",
                        ),
                        Err(error) => error,
                    };
                    terminal.error_code = Some(error.code.as_str().to_owned());
                    terminal.error_message = Some(error.message);
                }
                let terminal_persisted = catalog
                    .compare_and_swap_query_status(
                        &query_id,
                        current.status.status_version,
                        &terminal,
                    )
                    .is_ok();
                if terminal_persisted
                    && let Err(error) = prune_history_in_catalog(
                        &catalog,
                        terminal_history_count,
                        terminal_history_days,
                    )
                {
                    tracing::warn!(error = %error, "failed to prune terminal Query history");
                }
            }
            done.notify_one();
        });
        Ok(())
    }

    fn fail_query(&self, query_id: &str, error: &VqlError) -> Result<()> {
        let query = self.catalog.get_query(query_id)?;
        if query.status.state.is_terminal() {
            return Ok(());
        }
        let mut failed = query.status.clone();
        failed.state = QueryState::Failed;
        failed.updated_at = Utc::now().timestamp_millis();
        failed.error_code = Some(error.code.as_str().to_owned());
        failed.error_message = Some(error.message.clone());
        self.catalog.compare_and_swap_query_status(
            query_id,
            query.status.status_version,
            &failed,
        )?;
        self.prune_history_best_effort();
        Ok(())
    }

    fn complete_stopped(&self, query_id: &str) -> Result<()> {
        let query = self.catalog.get_query(query_id)?;
        let mut stopped = query.status.clone();
        stopped.state = QueryState::Stopped;
        stopped.updated_at = Utc::now().timestamp_millis();
        self.catalog.compare_and_swap_query_status(
            query_id,
            query.status.status_version,
            &stopped,
        )?;
        self.prune_history_best_effort();
        Ok(())
    }

    pub fn prune_history(&self) -> Result<usize> {
        prune_history_in_catalog(
            &self.catalog,
            self.terminal_history_count,
            self.terminal_history_days,
        )
    }

    fn prune_history_best_effort(&self) {
        if let Err(error) = self.prune_history() {
            tracing::warn!(error = %error, "failed to prune terminal Query history");
        }
    }

    pub fn active_count(&self) -> usize {
        self.active.lock().map(|active| active.len()).unwrap_or(0)
    }

    pub fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::Relaxed);
        if let Ok(active) = self.active.lock() {
            for query in active.values() {
                query.handle.cancel();
            }
        }
    }
}

fn prune_history_in_catalog(
    catalog: &CatalogStore,
    terminal_history_count: usize,
    terminal_history_days: u64,
) -> Result<usize> {
    let age_ms = i64::try_from(terminal_history_days)
        .unwrap_or(i64::MAX)
        .saturating_mul(24 * 60 * 60 * 1000);
    let cutoff = Utc::now().timestamp_millis().saturating_sub(age_ms);
    catalog
        .prune_terminal_queries(Some(terminal_history_count), Some(cutoff))
        .map_err(Into::into)
}

fn query_identity_batch(schema: SchemaRef, query: &PersistentQuery) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![query.definition.query_id.as_str()])) as ArrayRef,
            Arc::new(StringArray::from(vec![query.definition.name.as_str()])),
            Arc::new(StringArray::from(vec![query.status.state.as_str()])),
        ],
    )?)
}

fn timestamp_array(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(TimestampMillisecondArray::from(values).with_timezone("UTC"))
}

fn redact_sql(sql: &str) -> String {
    let mut output = String::with_capacity(sql.len());
    let mut characters = sql.chars().peekable();
    while let Some(character) = characters.next() {
        if character != '\'' {
            output.push(character);
            continue;
        }
        output.push('\'');
        output.push('?');
        while let Some(character) = characters.next() {
            if character == '\'' {
                if characters.peek() == Some(&'\'') {
                    characters.next();
                    continue;
                }
                output.push('\'');
                break;
            }
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use tempfile::tempdir;
    use vql_catalog::CreateQuery;
    use vql_kernel::{Engine, EngineConfig};

    #[test]
    fn redaction_removes_string_literals_and_preserves_statement_shape() {
        assert_eq!(
            redact_sql("INSERT INTO sink SELECT * FROM camera WHERE label = 'secret''value'"),
            "INSERT INTO sink SELECT * FROM camera WHERE label = '?'"
        );
    }

    #[test]
    fn terminal_transition_enforces_history_count_without_restart() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let controller = QueryController::new(engine.clone(), 1, 30);
        engine
            .session()
            .build()
            .unwrap()
            .sql(&format!(
                "CREATE TABLE source USING IMAGES LOCATION '{}'",
                temp.path().display()
            ))
            .unwrap();
        let generations = engine.catalog().snapshot().unwrap().generations();

        for name in ["first", "second"] {
            let query = engine
                .catalog()
                .create_query(&CreateQuery {
                    catalog_name: vql_catalog::DEFAULT_CATALOG.to_owned(),
                    schema_name: vql_catalog::DEFAULT_SCHEMA.to_owned(),
                    name: name.to_owned(),
                    principal: "service".to_owned(),
                    normalized_sql: "INSERT INTO sink SELECT * FROM source".to_owned(),
                    sql_redacted: "INSERT INTO sink SELECT * FROM source".to_owned(),
                    session_settings: BTreeMap::new(),
                    definition_generations: generations.clone(),
                })
                .unwrap();
            controller
                .fail_query(
                    &query.definition.query_id,
                    &VqlError::new(ErrorCode::Execution, "failed"),
                )
                .unwrap();
        }

        let queries = engine.catalog().list_queries().unwrap();
        assert_eq!(queries.len(), 1);
        assert!(queries[0].status.state.is_terminal());
    }
}
