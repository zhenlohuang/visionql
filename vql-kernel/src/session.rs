use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::catalog::{
    FunctionImplementation, ModelDef, ModelInterface, ModelParameter, ModelType, ModelVersion,
    ObjectKind, RtspTableConfig, TableDef, TableProvider,
};
use crate::connectors::images::{ImagesTableProvider, images_schema};
use crate::connectors::rtsp::{rtsp_schema, start_rtsp_source};
use crate::connectors::videos::{VideosTableProvider, videos_schema};
use crate::engine::EngineRuntime;
use crate::functions::{VqlFunctionFactory, materialize_batch_images};
use crate::media::MediaRuntime;
use crate::models::{canonical_model_options, semantic_fingerprint};
use crate::planner::{
    SinkTarget, bind_stream_epoch, bind_tumble_output, context_for_function_ddl,
    context_for_snapshot, infer_constant_parameters, normalize_function_ddl, plan_statement,
    wrap_sink,
};
use crate::resources::{QueryBudget, QueryReservation, SessionMemoryPool};
use crate::sql::{
    AlterModel, CreateModel, CreateTable, ModelInterfaceSpec, ShowKind, TableColumn, VqlStatement,
    parse_statement, render_create, render_create_model, render_create_table,
};
use crate::types::{image_field, is_image_storage};
use crate::{Engine, ErrorCode, PythonUdfHostRef, Result, VqlError};

#[derive(Debug)]
pub struct SessionBuilder {
    engine: Engine,
    python_udf_host: Option<PythonUdfHostRef>,
    service_mode: bool,
}

impl SessionBuilder {
    pub(crate) fn new(engine: Engine) -> Self {
        Self {
            engine,
            python_udf_host: None,
            service_mode: false,
        }
    }

    pub fn with_python_udf_host(mut self, host: PythonUdfHostRef) -> Self {
        self.python_udf_host = Some(host);
        self
    }

    pub fn for_service(mut self) -> Self {
        self.service_mode = true;
        self
    }

    pub fn build(self) -> Result<Session> {
        self.engine.inner.catalog.snapshot()?;
        let memory_pool = Arc::new(SessionMemoryPool::new(
            self.engine.inner.config.session_memory_limit_bytes(),
        ));
        Ok(Session {
            engine: self.engine,
            active_query: Arc::new(Mutex::new(None)),
            fail_on_error: Arc::new(AtomicBool::new(false)),
            python_udf_host: self.python_udf_host,
            service_mode: self.service_mode,
            memory_pool,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Session {
    engine: Engine,
    active_query: Arc<Mutex<Option<ActiveQueryControl>>>,
    fail_on_error: Arc<AtomicBool>,
    python_udf_host: Option<PythonUdfHostRef>,
    service_mode: bool,
    memory_pool: Arc<SessionMemoryPool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryInterruptAction {
    NoActiveQuery,
    GracefulStopRequested,
    ImmediateCancellationRequested,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementKind {
    Query,
    Update,
    PersistentSubmission,
}

impl StatementKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Update => "update",
            Self::PersistentSubmission => "persistent_submission",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryMode {
    Bounded,
    Unbounded,
    NotApplicable,
}

impl QueryMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bounded => "bounded",
            Self::Unbounded => "unbounded",
            Self::NotApplicable => "not_applicable",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultMode {
    Bounded,
    Unbounded,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceHealth {
    Connected,
    Reconnecting,
}

impl SourceHealth {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Connected => "connected",
            Self::Reconnecting => "reconnecting",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryProgress {
    pub source_health: Option<SourceHealth>,
    pub last_event_time: Option<i64>,
}

impl ResultMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bounded => "bounded",
            Self::Unbounded => "unbounded",
            Self::None => "none",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatementInfo {
    pub kind: StatementKind,
    pub query_mode: QueryMode,
    pub result_mode: ResultMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersistentCommand {
    Submit { name: String, sql: String },
    Show,
    Describe { query_id: String },
    Stop { query_id: String },
}

#[derive(Debug, Clone)]
enum PreparedOperation {
    Query {
        sql: String,
        snapshot: crate::catalog::DefinitionSnapshot,
        insert: bool,
    },
    Explain {
        sql: String,
        snapshot: crate::catalog::DefinitionSnapshot,
    },
    CatalogQuery {
        command: CatalogQuery,
        snapshot: crate::catalog::DefinitionSnapshot,
    },
    Update {
        sql: String,
    },
    Persistent(PersistentCommand),
}

#[derive(Debug, Clone)]
enum CatalogQuery {
    Show(ShowKind),
    ShowModelVersions {
        name: String,
    },
    ShowCreate {
        kind: ShowKind,
        name: String,
        version: Option<String>,
    },
    Describe {
        kind: ShowKind,
        name: String,
    },
}

#[derive(Debug, Clone)]
pub struct PreparedStatement {
    session: Session,
    principal: String,
    session_settings: BTreeMap<String, String>,
    info: StatementInfo,
    result_schema: SchemaRef,
    definition_generations: Vec<i64>,
    operation: PreparedOperation,
    execution_profile: ExecutionProfile,
}

#[derive(Debug, Clone, Copy, Default)]
struct ExecutionProfile {
    uses_inference: bool,
    uses_source: bool,
    uses_sink: bool,
}

impl ExecutionProfile {
    fn from_handle(handle: &QueryHandle) -> Self {
        Self {
            uses_inference: handle.uses_inference(),
            uses_source: handle.uses_source(),
            uses_sink: handle.uses_sink(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum PreparedResult {
    Query(Box<QueryHandle>),
    Batches(Vec<RecordBatch>),
}

#[derive(Debug, Clone)]
struct ActiveQueryControl {
    cancellation: CancellationToken,
    graceful_stop: Option<CancellationToken>,
}

impl ActiveQueryControl {
    fn cancel_immediately(&self) {
        if let Some(graceful_stop) = self.graceful_stop.as_ref() {
            graceful_stop.cancel();
        }
        self.cancellation.cancel();
    }

    fn interrupt(&self) -> QueryInterruptAction {
        if let Some(graceful_stop) = self.graceful_stop.as_ref()
            && !graceful_stop.is_cancelled()
        {
            graceful_stop.cancel();
            return QueryInterruptAction::GracefulStopRequested;
        }
        self.cancel_immediately();
        QueryInterruptAction::ImmediateCancellationRequested
    }
}

#[derive(Debug)]
pub enum Statement {
    Query(QueryHandle),
    Ddl(DdlResult),
    Explain(QueryHandle),
    Set(QueryHandle),
}

impl Statement {
    pub fn collect(&self) -> Result<Vec<RecordBatch>> {
        match self {
            Self::Query(query) | Self::Explain(query) | Self::Set(query) => query.collect(),
            Self::Ddl(result) => Ok(result.batches.clone()),
        }
    }

    pub fn cancel(&self) {
        match self {
            Self::Query(query) | Self::Explain(query) | Self::Set(query) => query.cancel(),
            Self::Ddl(_) => {}
        }
    }

    pub fn request_graceful_stop(&self) {
        match self {
            Self::Query(query) | Self::Explain(query) | Self::Set(query) => {
                query.request_graceful_stop()
            }
            Self::Ddl(_) => {}
        }
    }

    pub fn is_unbounded(&self) -> bool {
        matches!(self, Self::Query(query) if query.is_unbounded())
    }

    pub fn for_each_batch(
        &self,
        mut callback: impl FnMut(&RecordBatch) -> Result<()>,
    ) -> Result<()> {
        match self {
            Self::Query(query) | Self::Explain(query) | Self::Set(query) => {
                query.for_each_batch(callback)
            }
            Self::Ddl(result) => {
                for batch in &result.batches {
                    callback(batch)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct DdlResult {
    pub message: String,
    batches: Vec<RecordBatch>,
}

impl DdlResult {
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }
}

impl PreparedStatement {
    pub const fn statement_info(&self) -> StatementInfo {
        self.info
    }

    pub fn result_schema(&self) -> SchemaRef {
        Arc::clone(&self.result_schema)
    }

    pub fn definition_generations(&self) -> &[i64] {
        &self.definition_generations
    }

    pub fn principal(&self) -> &str {
        &self.principal
    }

    pub fn session_settings(&self) -> &BTreeMap<String, String> {
        &self.session_settings
    }

    pub fn persistent_command(&self) -> Option<&PersistentCommand> {
        match &self.operation {
            PreparedOperation::Persistent(command) => Some(command),
            _ => None,
        }
    }

    pub const fn uses_inference(&self) -> bool {
        self.execution_profile.uses_inference
    }

    pub const fn uses_source(&self) -> bool {
        self.execution_profile.uses_source
    }

    pub const fn uses_sink(&self) -> bool {
        self.execution_profile.uses_sink
    }

    pub fn execute_query(&self) -> Result<PreparedResult> {
        match &self.operation {
            PreparedOperation::Query {
                sql,
                snapshot,
                insert,
            } => {
                let session = self.session.execution_session(&self.session_settings)?;
                let query = if *insert {
                    session.insert_into_table_with_snapshot(sql, snapshot.clone())?
                } else {
                    session.query_with_snapshot(sql, snapshot.clone())?
                };
                Ok(PreparedResult::Query(Box::new(query)))
            }
            PreparedOperation::Explain { sql, snapshot } => self
                .session
                .execution_session(&self.session_settings)?
                .explain_with_snapshot(sql, snapshot.clone())
                .map(|query| PreparedResult::Query(Box::new(query))),
            PreparedOperation::CatalogQuery { command, snapshot } => self
                .session
                .execute_catalog_query(command, snapshot)
                .map(|result| PreparedResult::Batches(result.batches)),
            PreparedOperation::Update { .. } => Err(VqlError::new(
                ErrorCode::InvalidArgument,
                "prepared update must be executed with execute_update",
            )),
            PreparedOperation::Persistent(_) => Err(VqlError::new(
                ErrorCode::InvalidArgument,
                "persistent Query commands must be executed by a service host",
            )),
        }
    }

    pub fn execute_update(&self) -> Result<i64> {
        let PreparedOperation::Update { sql } = &self.operation else {
            return Err(VqlError::new(
                ErrorCode::InvalidArgument,
                "prepared query must be executed with execute_query",
            ));
        };
        let statement = self.session.sql(sql)?;
        statement.collect()?;
        Ok(0)
    }
}

#[derive(Debug, Clone)]
pub struct QueryHandle {
    dataframe: DataFrame,
    runtime: Arc<EngineRuntime>,
    cancellation: CancellationToken,
    graceful_stop: CancellationToken,
    active_query: Arc<Mutex<Option<ActiveQueryControl>>>,
    output_schema: SchemaRef,
    budget: QueryBudget,
    output_reservation: Arc<Mutex<Option<QueryReservation>>>,
    collected: Arc<Mutex<Option<Vec<RecordBatch>>>>,
    media: Arc<MediaRuntime>,
    catalog: Arc<crate::catalog::CatalogStore>,
    fail_on_error: Arc<AtomicBool>,
    streaming: Option<StreamingQuery>,
    sink_target: Option<SinkTarget>,
    progress: tokio::sync::watch::Sender<QueryProgress>,
    uses_inference: bool,
    uses_source: bool,
    uses_sink: bool,
}

#[derive(Debug, Clone)]
struct StreamingQuery {
    name: String,
    definition: RtspTableConfig,
    skip: usize,
    fetch: Option<usize>,
    tumble: Option<crate::stream::TumblePlan>,
    sink: Option<SinkTarget>,
}

#[derive(Debug, Clone)]
struct QueryResources {
    runtime: Arc<EngineRuntime>,
    active_query: Arc<Mutex<Option<ActiveQueryControl>>>,
    media: Arc<MediaRuntime>,
    catalog: Arc<crate::catalog::CatalogStore>,
    fail_on_error: Arc<AtomicBool>,
    budget: QueryBudget,
}

struct ActiveQueryGuard(Arc<Mutex<Option<ActiveQueryControl>>>);

impl Drop for ActiveQueryGuard {
    fn drop(&mut self) {
        if let Ok(mut active) = self.0.lock() {
            *active = None;
        }
    }
}

struct SinkCloseGuard {
    target: Option<SinkTarget>,
    runtime: Arc<EngineRuntime>,
}

impl SinkCloseGuard {
    fn new(target: SinkTarget, runtime: Arc<EngineRuntime>) -> Self {
        Self {
            target: Some(target),
            runtime,
        }
    }

    fn disarm(&mut self) {
        self.target = None;
    }
}

impl Drop for SinkCloseGuard {
    fn drop(&mut self) {
        let Some(target) = self.target.take() else {
            return;
        };
        self.runtime.handle().spawn(async move {
            if let Err(error) = target.finish_execution().await {
                tracing::warn!(error = %error, "failed to close Sink after query stream was dropped");
            }
        });
    }
}

impl QueryHandle {
    fn new(
        dataframe: DataFrame,
        cancellation: CancellationToken,
        resources: QueryResources,
        streaming: Option<StreamingQuery>,
        uses_inference: bool,
        uses_source: bool,
    ) -> Self {
        let output_schema = restamp_schema(dataframe.schema().inner());
        let (progress, _) = tokio::sync::watch::channel(QueryProgress::default());
        Self {
            dataframe,
            runtime: resources.runtime,
            cancellation,
            graceful_stop: CancellationToken::new(),
            active_query: resources.active_query,
            output_schema,
            budget: resources.budget,
            output_reservation: Arc::new(Mutex::new(None)),
            collected: Arc::new(Mutex::new(None)),
            media: resources.media,
            catalog: resources.catalog,
            fail_on_error: resources.fail_on_error,
            streaming,
            sink_target: None,
            progress,
            uses_inference,
            uses_source,
            uses_sink: false,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    pub fn subscribe_progress(&self) -> tokio::sync::watch::Receiver<QueryProgress> {
        self.progress.subscribe()
    }

    pub fn stream(&self) -> Result<SendableRecordBatchStream> {
        self.stream_with_output_budget(true)
    }

    /// Stream rows with every projected IMAGE materialized as encoded JPEG bytes.
    ///
    /// Network hosts use this boundary before applying their transport-specific
    /// thumbnail and metadata sanitization policy.
    pub fn stream_materialized_images(&self) -> Result<SendableRecordBatchStream> {
        let input = self.stream_with_output_budget(false)?;
        let schema = Arc::clone(&self.output_schema);
        let stream_schema = Arc::clone(&schema);
        let catalog = Arc::clone(&self.catalog);
        let media = Arc::clone(&self.media);
        let fail_on_error = Arc::clone(&self.fail_on_error);
        let budget = self.budget.clone();
        let stream = async_stream::try_stream! {
            let mut input = input;
            while let Some(batch) = input.next().await {
                let materialized = materialize_batch_images(
                    Arc::clone(&catalog),
                    Arc::clone(&media),
                    batch?,
                    fail_on_error.load(Ordering::Relaxed),
                    budget.clone(),
                )
                .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?;
                yield RecordBatch::try_new(
                    Arc::clone(&schema),
                    materialized.batch.columns().to_vec(),
                )?;
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }

    fn stream_with_output_budget(&self, reserve_output: bool) -> Result<SendableRecordBatchStream> {
        if let Some(batches) = self.cached_batches()? {
            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.output_schema),
                stream,
            )));
        }
        self.set_active()?;
        let active_guard = ActiveQueryGuard(Arc::clone(&self.active_query));
        let input = if self.streaming.is_some() {
            self.stream_rtsp(reserve_output)?
        } else {
            self.stream_bounded(reserve_output)?
        };
        let input = if let Some(target) = self.sink_target.clone() {
            close_sink_stream(
                input,
                target,
                Arc::clone(&self.runtime),
                Arc::clone(&self.output_schema),
            )
        } else {
            input
        };
        Ok(keep_active_stream(
            input,
            active_guard,
            Arc::clone(&self.output_schema),
        ))
    }

    fn stream_bounded(&self, reserve_output: bool) -> Result<SendableRecordBatchStream> {
        let input = self
            .runtime
            .block_on(self.dataframe.clone().execute_stream())?;
        let output_schema = Arc::clone(&self.output_schema);
        let stream_schema = Arc::clone(&output_schema);
        let cancellation = self.cancellation.clone();
        let budget = self.budget.clone();
        let stream = async_stream::try_stream! {
            let mut input = input;
            loop {
                let next = tokio::select! {
                    _ = cancellation.cancelled() => None,
                    next = input.next() => next,
                };
                if cancellation.is_cancelled() {
                    Err(datafusion::error::DataFusionError::External(Box::new(
                        VqlError::new(ErrorCode::QueryCancelled, "query cancelled"),
                    )))?;
                }
                let Some(batch) = next else { break; };
                let batch = batch?;
                let _output_reservation = if reserve_output {
                    Some(budget
                        .reserve(crate::QueryResource::Arrow, batch.get_array_memory_size())
                        .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?)
                } else {
                    None
                };
                yield RecordBatch::try_new(
                    Arc::clone(&output_schema),
                    batch.columns().to_vec(),
                )?;
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }

    fn stream_rtsp(&self, reserve_output: bool) -> Result<SendableRecordBatchStream> {
        let streaming = self
            .streaming
            .clone()
            .ok_or_else(|| VqlError::new(ErrorCode::Internal, "stream definition is missing"))?;
        if streaming.fetch == Some(0) {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.output_schema),
                futures::stream::empty::<datafusion::error::Result<RecordBatch>>(),
            )));
        }
        let mut source = start_rtsp_source(
            streaming.definition,
            Arc::clone(&self.media),
            Arc::clone(&self.fail_on_error),
            self.graceful_stop.clone(),
            self.budget.clone(),
            Some(self.progress.clone()),
        )?;
        let stream_name = streaming.name;
        let mut skip_remaining = streaming.skip;
        let mut fetch_remaining = streaming.fetch;
        let template = streaming
            .tumble
            .as_ref()
            .map(crate::stream::TumblePlan::input)
            .unwrap_or_else(|| self.dataframe.clone());
        let mut tumble_state = streaming
            .tumble
            .as_ref()
            .map(crate::stream::TumblePlan::create_state);
        let tumble_plan = streaming.tumble;
        let sink = streaming.sink;
        let output_schema = Arc::clone(&self.output_schema);
        let stream_schema = Arc::clone(&output_schema);
        let cancellation = self.cancellation.clone();
        let budget = self.budget.clone();
        let catalog = Arc::clone(&self.catalog);
        let media = Arc::clone(&self.media);
        let fail_on_error = Arc::clone(&self.fail_on_error);
        let stream = async_stream::try_stream! {
            'epochs: loop {
                let next = tokio::select! {
                    _ = cancellation.cancelled() => Ok(None),
                    next = source.next() => next,
                };
                let next = next.map_err(|error| {
                    datafusion::error::DataFusionError::External(Box::new(error))
                })?;
                if cancellation.is_cancelled() {
                    Err(datafusion::error::DataFusionError::External(Box::new(
                        VqlError::new(ErrorCode::QueryCancelled, "query cancelled"),
                    )))?;
                }
                let Some(epoch) = next else { break; };
                let dataframe = bind_stream_epoch(
                    template.clone(),
                    &stream_name,
                    epoch.batches.clone(),
                )
                .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?;
                let mut fragment_output = dataframe.execute_stream().await?;
                let mut output = if let Some(state) = tumble_state.as_mut() {
                    let mut batches = Vec::new();
                    while let Some(batch) = fragment_output.next().await {
                        batches.push(batch?);
                    }
                    let window_output = state
                        .apply_epoch(&batches, epoch.watermark_ms, epoch.epoch_id)
                        .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?;
                    match window_output.closed {
                        Some(batch) => {
                            let plan = tumble_plan.as_ref().ok_or_else(|| {
                                datafusion::error::DataFusionError::Internal(
                                    "TUMBLE state exists without a streaming plan".to_owned(),
                                )
                            })?;
                            Some(bind_tumble_output(plan, batch)
                                .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?
                                .execute_stream()
                                .await?)
                        }
                        None => None,
                    }
                } else {
                    Some(fragment_output)
                };
                while let Some(batch) = match output.as_mut() {
                    Some(output) => output.next().await,
                    None => None,
                } {
                    let batch = batch?;
                    let Some(materialized) = prepare_stream_output(
                        batch,
                        &mut skip_remaining,
                        &mut fetch_remaining,
                        sink.as_ref(),
                        |batch| {
                            materialize_batch_images(
                                Arc::clone(&catalog),
                                Arc::clone(&media),
                                batch,
                                fail_on_error.load(Ordering::Relaxed),
                                budget.clone(),
                            )
                        },
                    )
                    .await?
                    else {
                        continue;
                    };
                    let batch = materialized.batch;
                    let _output_reservation = if reserve_output {
                        Some(budget
                            .reserve(crate::QueryResource::Arrow, batch.get_array_memory_size())
                            .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?)
                    } else {
                        None
                    };
                    drop(materialized.reservations);
                    yield RecordBatch::try_new(
                        Arc::clone(&output_schema),
                        batch.columns().to_vec(),
                    )?;
                    if fetch_remaining == Some(0) {
                        break 'epochs;
                    }
                }
                tracing::debug!(
                    epoch_id = epoch.epoch_id,
                    watermark_ms = epoch.watermark_ms,
                    "completed RTSP epoch"
                );
                drop(epoch.frame_lease);
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }

    pub fn collect(&self) -> Result<Vec<RecordBatch>> {
        if let Some(batches) = self.cached_batches()? {
            return Ok(batches);
        }
        let mut stream = self.stream_with_output_budget(false)?;
        let mut output_reservation = self.budget.reserve(crate::QueryResource::Arrow, 0)?;
        let result = self.runtime.block_on(async {
            let mut batches = Vec::new();
            while let Some(batch) = stream.next().await {
                match batch {
                    Ok(batch) => {
                        output_reservation.try_grow(batch.get_array_memory_size())?;
                        batches.push(batch);
                    }
                    Err(_error) if self.cancellation.is_cancelled() => {
                        return Err(VqlError::new(ErrorCode::QueryCancelled, "query cancelled"));
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Ok(batches)
        });
        if let Ok(mut active) = self.active_query.lock() {
            *active = None;
        }
        let batches = result?;
        let mut collected = self
            .collected
            .lock()
            .map_err(|_| VqlError::new(ErrorCode::Internal, "query result cache was poisoned"))?;
        *collected = Some(batches.clone());
        *self.output_reservation.lock().map_err(|_| {
            VqlError::new(ErrorCode::Internal, "query result reservation was poisoned")
        })? = Some(output_reservation);
        Ok(batches)
    }

    pub fn cancel(&self) {
        self.graceful_stop.cancel();
        self.cancellation.cancel();
    }

    pub fn request_graceful_stop(&self) {
        if self.is_unbounded() {
            self.graceful_stop.cancel();
        } else {
            self.cancel();
        }
    }

    pub fn is_unbounded(&self) -> bool {
        self.streaming
            .as_ref()
            .is_some_and(|streaming| streaming.fetch.is_none())
    }

    pub fn resets_window_state_on_restart(&self) -> bool {
        self.streaming
            .as_ref()
            .is_some_and(|streaming| streaming.tumble.is_some())
    }

    pub const fn uses_inference(&self) -> bool {
        self.uses_inference
    }

    pub const fn uses_source(&self) -> bool {
        self.uses_source
    }

    pub const fn uses_sink(&self) -> bool {
        self.uses_sink
    }

    pub fn for_each_batch(
        &self,
        mut callback: impl FnMut(&RecordBatch) -> Result<()>,
    ) -> Result<()> {
        let mut stream = self.stream()?;
        self.runtime.block_on(async {
            while let Some(batch) = stream.next().await {
                match batch {
                    Ok(batch) => callback(&batch)?,
                    Err(_error) if self.cancellation.is_cancelled() => {
                        return Err(VqlError::new(ErrorCode::QueryCancelled, "query cancelled"));
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Ok(())
        })
    }

    fn set_active(&self) -> Result<()> {
        let mut active = self
            .active_query
            .lock()
            .map_err(|_| VqlError::new(ErrorCode::Internal, "active query lock was poisoned"))?;
        *active = Some(ActiveQueryControl {
            cancellation: self.cancellation.clone(),
            graceful_stop: self.is_unbounded().then(|| self.graceful_stop.clone()),
        });
        Ok(())
    }

    fn cached_batches(&self) -> Result<Option<Vec<RecordBatch>>> {
        self.collected
            .lock()
            .map(|batches| batches.clone())
            .map_err(|_| VqlError::new(ErrorCode::Internal, "query result cache was poisoned"))
    }
}

fn keep_active_stream(
    input: SendableRecordBatchStream,
    active_guard: ActiveQueryGuard,
    schema: SchemaRef,
) -> SendableRecordBatchStream {
    let stream_schema = Arc::clone(&schema);
    let stream = async_stream::try_stream! {
        let _active_guard = active_guard;
        let mut input = input;
        while let Some(batch) = input.next().await {
            yield batch?;
        }
    };
    Box::pin(RecordBatchStreamAdapter::new(stream_schema, stream))
}

fn close_sink_stream(
    input: SendableRecordBatchStream,
    target: SinkTarget,
    runtime: Arc<EngineRuntime>,
    schema: SchemaRef,
) -> SendableRecordBatchStream {
    let stream_schema = Arc::clone(&schema);
    let stream = async_stream::stream! {
        target.begin_execution().await;
        let mut guard = SinkCloseGuard::new(target.clone(), runtime);
        let mut input = input;
        let mut terminal_error = None;
        while let Some(batch) = input.next().await {
            match batch {
                Ok(batch) => yield Ok(batch),
                Err(error) => {
                    terminal_error = Some(error);
                    break;
                }
            }
        }
        let close_result = target.finish_execution().await;
        guard.disarm();
        if let Some(error) = terminal_error {
            if let Err(close_error) = close_result {
                tracing::warn!(error = %close_error, "failed to close Sink after query error");
            }
            yield Err(error);
        } else if let Err(error) = close_result {
            yield Err(datafusion::error::DataFusionError::External(Box::new(error)));
        }
    };
    Box::pin(RecordBatchStreamAdapter::new(stream_schema, stream))
}

async fn prepare_stream_output<F>(
    mut batch: RecordBatch,
    skip_remaining: &mut usize,
    fetch_remaining: &mut Option<usize>,
    sink: Option<&SinkTarget>,
    materialize: F,
) -> datafusion::error::Result<Option<crate::functions::MaterializedBatch>>
where
    F: FnOnce(RecordBatch) -> Result<crate::functions::MaterializedBatch>,
{
    if *skip_remaining >= batch.num_rows() {
        *skip_remaining -= batch.num_rows();
        return Ok(None);
    }
    if *skip_remaining > 0 {
        batch = batch.slice(*skip_remaining, batch.num_rows() - *skip_remaining);
        *skip_remaining = 0;
    }
    if let Some(remaining) = *fetch_remaining
        && batch.num_rows() > remaining
    {
        batch = batch.slice(0, remaining);
    }
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    let materialized = materialize(batch)
        .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?;
    if let Some(sink) = sink {
        sink.write(&materialized.batch)
            .await
            .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?;
    }
    if let Some(remaining) = fetch_remaining.as_mut() {
        *remaining = remaining.saturating_sub(materialized.batch.num_rows());
    }
    Ok(Some(materialized))
}

impl Session {
    pub fn semantic_settings(&self) -> BTreeMap<String, String> {
        BTreeMap::from([(
            "vql.on_error".to_owned(),
            if self.fail_on_error.load(Ordering::Relaxed) {
                "fail"
            } else {
                "null"
            }
            .to_owned(),
        )])
    }

    pub fn prepare(
        &self,
        sql: &str,
        principal: impl Into<String>,
        session_settings: BTreeMap<String, String>,
    ) -> Result<PreparedStatement> {
        let snapshot = self.engine.inner.catalog.snapshot()?;
        self.prepare_with_snapshot(sql, principal.into(), session_settings, snapshot)
    }

    pub fn prepare_pinned(
        &self,
        sql: &str,
        principal: impl Into<String>,
        session_settings: BTreeMap<String, String>,
        definition_generations: &[i64],
    ) -> Result<PreparedStatement> {
        let snapshot = self
            .engine
            .inner
            .catalog
            .snapshot_at_generations(definition_generations)?;
        self.prepare_with_snapshot(sql, principal.into(), session_settings, snapshot)
    }

    fn prepare_with_snapshot(
        &self,
        sql: &str,
        principal: String,
        session_settings: BTreeMap<String, String>,
        snapshot: crate::catalog::DefinitionSnapshot,
    ) -> Result<PreparedStatement> {
        if principal.trim().is_empty() {
            return Err(VqlError::new(
                ErrorCode::InvalidArgument,
                "statement principal cannot be empty",
            ));
        }
        validate_semantic_settings(&session_settings)?;
        let sql = normalize_statement_sql(sql)?;
        let parsed = parse_statement(&sql)?;
        let generations = snapshot.generations();
        let prepared_session = self.execution_session(&session_settings)?;
        let (info, result_schema, definition_generations, operation, execution_profile) =
            match parsed {
                VqlStatement::Query { sql: query_sql } => {
                    let insert = query_sql
                        .trim_start()
                        .to_ascii_uppercase()
                        .starts_with("INSERT");
                    let handle = if insert {
                        prepared_session
                            .insert_into_table_with_snapshot(&query_sql, snapshot.clone())?
                    } else {
                        prepared_session.query_with_snapshot(&query_sql, snapshot.clone())?
                    };
                    let unbounded = handle.is_unbounded();
                    let execution_profile = ExecutionProfile::from_handle(&handle);
                    (
                        StatementInfo {
                            kind: StatementKind::Query,
                            query_mode: if unbounded {
                                QueryMode::Unbounded
                            } else {
                                QueryMode::Bounded
                            },
                            result_mode: if unbounded {
                                ResultMode::Unbounded
                            } else {
                                ResultMode::Bounded
                            },
                        },
                        handle.schema(),
                        generations,
                        PreparedOperation::Query {
                            sql: query_sql,
                            snapshot,
                            insert,
                        },
                        execution_profile,
                    )
                }
                VqlStatement::Explain { sql } => {
                    let handle = prepared_session.explain_with_snapshot(&sql, snapshot.clone())?;
                    let execution_profile = ExecutionProfile::from_handle(&handle);
                    (
                        StatementInfo {
                            kind: StatementKind::Query,
                            query_mode: QueryMode::Bounded,
                            result_mode: ResultMode::Bounded,
                        },
                        handle.schema(),
                        generations,
                        PreparedOperation::Explain { sql, snapshot },
                        execution_profile,
                    )
                }
                VqlStatement::SubmitQuery { name, sql } => {
                    let handle =
                        prepared_session.insert_into_table_with_snapshot(&sql, snapshot.clone())?;
                    if !handle.is_unbounded() {
                        return Err(VqlError::new(
                            ErrorCode::InvalidSql,
                            "SUBMIT QUERY accepts exactly one unbounded INSERT INTO <table> SELECT ...",
                        ));
                    }
                    let execution_profile = ExecutionProfile::from_handle(&handle);
                    (
                        StatementInfo {
                            kind: StatementKind::PersistentSubmission,
                            query_mode: QueryMode::Unbounded,
                            result_mode: ResultMode::Bounded,
                        },
                        persistent_submission_schema(),
                        generations,
                        PreparedOperation::Persistent(PersistentCommand::Submit { name, sql }),
                        execution_profile,
                    )
                }
                VqlStatement::ShowQueries => (
                    query_management_info(),
                    show_queries_schema(),
                    Vec::new(),
                    PreparedOperation::Persistent(PersistentCommand::Show),
                    ExecutionProfile::default(),
                ),
                VqlStatement::DescribeQuery { query_id } => (
                    query_management_info(),
                    describe_query_schema(),
                    Vec::new(),
                    PreparedOperation::Persistent(PersistentCommand::Describe { query_id }),
                    ExecutionProfile::default(),
                ),
                VqlStatement::StopQuery { query_id } => (
                    query_management_info(),
                    persistent_submission_schema(),
                    Vec::new(),
                    PreparedOperation::Persistent(PersistentCommand::Stop { query_id }),
                    ExecutionProfile::default(),
                ),
                statement @ (VqlStatement::Show(_)
                | VqlStatement::ShowModelVersions { .. }
                | VqlStatement::ShowCreate { .. }
                | VqlStatement::Describe { .. }) => {
                    let command = match statement {
                        VqlStatement::Show(kind) => CatalogQuery::Show(kind),
                        VqlStatement::ShowModelVersions { name } => {
                            CatalogQuery::ShowModelVersions { name }
                        }
                        VqlStatement::ShowCreate {
                            kind,
                            name,
                            version,
                        } => CatalogQuery::ShowCreate {
                            kind,
                            name,
                            version,
                        },
                        VqlStatement::Describe { kind, name } => {
                            CatalogQuery::Describe { kind, name }
                        }
                        _ => unreachable!("catalog query arm contains only catalog queries"),
                    };
                    let result_schema = catalog_query_schema(&command);
                    (
                        StatementInfo {
                            kind: StatementKind::Query,
                            query_mode: QueryMode::Bounded,
                            result_mode: ResultMode::Bounded,
                        },
                        result_schema,
                        generations,
                        PreparedOperation::CatalogQuery { command, snapshot },
                        ExecutionProfile::default(),
                    )
                }
                VqlStatement::CreateTable(_)
                | VqlStatement::CreateModel(_)
                | VqlStatement::ResolveModel { .. }
                | VqlStatement::AlterModel { .. }
                | VqlStatement::CreateFunction { .. }
                | VqlStatement::Drop { .. }
                | VqlStatement::Set { .. } => (
                    StatementInfo {
                        kind: StatementKind::Update,
                        query_mode: QueryMode::NotApplicable,
                        result_mode: ResultMode::None,
                    },
                    Arc::new(Schema::empty()),
                    generations,
                    PreparedOperation::Update { sql: sql.clone() },
                    ExecutionProfile::default(),
                ),
            };
        Ok(PreparedStatement {
            session: self.clone(),
            principal,
            session_settings,
            info,
            result_schema: statement_schema_with_metadata(&result_schema, info),
            definition_generations,
            operation,
            execution_profile,
        })
    }

    fn execution_session(&self, settings: &BTreeMap<String, String>) -> Result<Self> {
        validate_semantic_settings(settings)?;
        let fail_on_error = settings
            .get("vql.on_error")
            .map(|value| value == "fail")
            .unwrap_or_else(|| self.fail_on_error.load(Ordering::Relaxed));
        Ok(Self {
            engine: self.engine.clone(),
            active_query: Arc::new(Mutex::new(None)),
            fail_on_error: Arc::new(AtomicBool::new(fail_on_error)),
            python_udf_host: self.python_udf_host.clone(),
            service_mode: self.service_mode,
            memory_pool: Arc::clone(&self.memory_pool),
        })
    }

    pub fn sql(&self, sql: &str) -> Result<Statement> {
        match parse_statement(sql)? {
            VqlStatement::CreateTable(create) => self.create_table(create).map(Statement::Ddl),
            VqlStatement::CreateModel(create) => self.create_model(create).map(Statement::Ddl),
            VqlStatement::ResolveModel { name, version } => self
                .resolve_model(&name, version.as_deref())
                .map(Statement::Ddl),
            VqlStatement::AlterModel { name, action } => {
                self.alter_model(&name, action).map(Statement::Ddl)
            }
            VqlStatement::CreateFunction { sql } => self.create_function(&sql).map(Statement::Ddl),
            VqlStatement::Drop { kind, name } => self.drop_object(kind, &name).map(Statement::Ddl),
            VqlStatement::Show(kind) => self.show_objects(kind).map(Statement::Ddl),
            VqlStatement::ShowModelVersions { name } => {
                self.show_model_versions(&name).map(Statement::Ddl)
            }
            VqlStatement::ShowCreate {
                kind,
                name,
                version,
            } => self
                .show_create(kind, &name, version.as_deref())
                .map(Statement::Ddl),
            VqlStatement::Describe { kind, name } => self.describe(kind, &name).map(Statement::Ddl),
            VqlStatement::SubmitQuery { .. }
            | VqlStatement::ShowQueries
            | VqlStatement::DescribeQuery { .. }
            | VqlStatement::StopQuery { .. } => Err(VqlError::new(
                ErrorCode::InvalidArgument,
                "persistent Query statements require the vqld service host",
            )),
            VqlStatement::Query { sql }
                if sql.trim_start().to_ascii_uppercase().starts_with("INSERT") =>
            {
                self.insert_into_table(&sql).map(Statement::Query)
            }
            VqlStatement::Query { sql } => self.query(&sql).map(Statement::Query),
            VqlStatement::Explain { sql } => self.explain(&sql).map(Statement::Explain),
            VqlStatement::Set { sql } => self.set(&sql).map(Statement::Ddl),
        }
    }

    pub fn run_script(&self, script: &str) -> Result<Vec<Statement>> {
        let mut results = Vec::new();
        for sql in crate::sql::split_statements(script)? {
            let statement = self.sql(&sql)?;
            statement.collect()?;
            results.push(statement);
        }
        Ok(results)
    }

    pub fn cancel_active_query(&self) {
        if let Ok(active) = self.active_query.lock()
            && let Some(control) = active.as_ref()
        {
            control.cancel_immediately();
        }
    }

    pub fn interrupt_active_query(&self) -> QueryInterruptAction {
        self.active_query
            .lock()
            .ok()
            .and_then(|active| active.as_ref().map(ActiveQueryControl::interrupt))
            .unwrap_or(QueryInterruptAction::NoActiveQuery)
    }

    fn query(&self, sql: &str) -> Result<QueryHandle> {
        let snapshot = self.engine.inner.catalog.snapshot()?;
        self.query_with_snapshot(sql, snapshot)
    }

    fn query_with_snapshot(
        &self,
        sql: &str,
        snapshot: crate::catalog::DefinitionSnapshot,
    ) -> Result<QueryHandle> {
        let budget = QueryBudget::for_session(Arc::clone(&self.memory_pool));
        let context = context_for_snapshot(
            &snapshot,
            Arc::clone(&self.engine.inner.catalog),
            Arc::clone(&self.engine.inner.media),
            Arc::clone(&self.fail_on_error),
            self.python_udf_host.clone(),
            &budget,
        )?;
        let cancellation = CancellationToken::new();
        let planned = self.engine.inner.runtime.block_on(plan_statement(
            &context,
            &snapshot,
            sql,
            Arc::clone(&self.engine.inner.builtins),
            Arc::clone(&self.engine.inner.models),
            Arc::clone(&self.fail_on_error),
            cancellation.clone(),
            budget.clone(),
            self.python_udf_host
                .is_none()
                .then_some(if self.service_mode {
                    ErrorCode::FeatureNotAvailable
                } else {
                    ErrorCode::PythonHostRequired
                }),
        ))?;
        let streaming = planned.stream_name.as_ref().map(|name| {
            let table = snapshot
                .table(name)
                .expect("planned RTSP table exists in the query snapshot");
            let TableProvider::Rtsp(definition) = &table.definition.provider else {
                unreachable!("planned unbounded table must use RTSP")
            };
            StreamingQuery {
                name: name.clone(),
                definition: definition.clone(),
                skip: planned.stream_skip,
                fetch: planned.stream_fetch,
                tumble: planned.tumble.clone(),
                sink: None,
            }
        });
        Ok(QueryHandle::new(
            planned.dataframe,
            cancellation,
            QueryResources {
                runtime: Arc::clone(&self.engine.inner.runtime),
                active_query: Arc::clone(&self.active_query),
                media: Arc::clone(&self.engine.inner.media),
                catalog: Arc::clone(&self.engine.inner.catalog),
                fail_on_error: Arc::clone(&self.fail_on_error),
                budget,
            },
            streaming,
            planned.uses_inference,
            planned.uses_source,
        ))
    }

    fn explain(&self, sql: &str) -> Result<QueryHandle> {
        let snapshot = self.engine.inner.catalog.snapshot()?;
        self.explain_with_snapshot(sql, snapshot)
    }

    fn explain_with_snapshot(
        &self,
        sql: &str,
        snapshot: crate::catalog::DefinitionSnapshot,
    ) -> Result<QueryHandle> {
        let sql = sql.trim();
        let (keyword, target) = sql
            .split_at_checked("EXPLAIN".len())
            .ok_or_else(|| VqlError::new(ErrorCode::InvalidSql, "expected EXPLAIN <query>"))?;
        if !keyword.eq_ignore_ascii_case("EXPLAIN") {
            return Err(VqlError::new(
                ErrorCode::InvalidSql,
                "expected EXPLAIN <query>",
            ));
        }
        let target = target.trim_start();
        if target
            .split_whitespace()
            .next()
            .is_some_and(|token| token.eq_ignore_ascii_case("ANALYZE"))
        {
            return Err(VqlError::new(
                ErrorCode::InvalidSql,
                "EXPLAIN ANALYZE is not supported because VisionQL EXPLAIN is side-effect free; use EXPLAIN without ANALYZE",
            ));
        }
        if !target.to_ascii_uppercase().starts_with("INSERT") {
            return self.query_with_snapshot(sql, snapshot);
        }

        let mut parts = target.splitn(4, char::is_whitespace);
        let _insert = parts.next();
        if !parts
            .next()
            .is_some_and(|token| token.eq_ignore_ascii_case("INTO"))
        {
            return Err(VqlError::new(
                ErrorCode::InvalidSql,
                "expected EXPLAIN INSERT INTO <table> <query>",
            ));
        }
        let table_name = parts.next().ok_or_else(|| {
            VqlError::new(ErrorCode::InvalidSql, "INSERT INTO requires a table name")
        })?;
        let query = parts.next().ok_or_else(|| {
            VqlError::new(ErrorCode::InvalidSql, "INSERT INTO requires a SELECT query")
        })?;
        let table = snapshot.table(table_name).cloned().ok_or_else(|| {
            VqlError::new(
                ErrorCode::NotFound,
                format!("table '{table_name}' does not exist"),
            )
        })?;
        let TableProvider::Kafka(_) = &table.definition.provider else {
            return Err(VqlError::new(
                ErrorCode::InvalidSql,
                format!("table '{table_name}' is not writable"),
            ));
        };
        let mut handle = self.query_with_snapshot(&format!("EXPLAIN {query}"), snapshot)?;
        let (state, plan) = handle.dataframe.into_parts();
        let datafusion::logical_expr::LogicalPlan::Explain(mut explain) = plan else {
            return Err(VqlError::new(
                ErrorCode::Internal,
                "EXPLAIN did not produce an Explain logical plan",
            ));
        };
        explain.stringified_plans.insert(
            0,
            datafusion::logical_expr::StringifiedPlan::new(
                datafusion::logical_expr::PlanType::FinalLogicalPlan,
                format!(
                    "VisionQLWrite name={} provider=KAFKA topology_append=TableWrite",
                    table.definition.name
                ),
            ),
        );
        handle.dataframe = DataFrame::new(
            state,
            datafusion::logical_expr::LogicalPlan::Explain(explain),
        );
        Ok(handle)
    }

    fn create_table(&self, create: CreateTable) -> Result<DdlResult> {
        let name = create.name.to_ascii_lowercase();
        let mut provider = create.provider;
        let schema = match &mut provider {
            TableProvider::Images {
                location,
                recursive,
            } => {
                *location = normalize_location(location)?.to_string_lossy().into_owned();
                ImagesTableProvider::try_new(location.clone(), 0, *recursive)?;
                images_schema()
            }
            TableProvider::Videos {
                location,
                recursive,
                fps,
                start_time_ms,
            } => {
                if !self.engine.inner.media.video_available() {
                    return Err(VqlError::new(
                        ErrorCode::FeatureNotAvailable,
                        "USING VIDEOS requires FFmpeg 8 or ffmpeg/ffprobe executables",
                    ));
                }
                *location = normalize_location(location)?.to_string_lossy().into_owned();
                VideosTableProvider::try_new(
                    location.clone(),
                    0,
                    *recursive,
                    *fps,
                    *start_time_ms,
                    Arc::clone(&self.engine.inner.media),
                )?;
                videos_schema(start_time_ms.is_none())
            }
            TableProvider::Rtsp(config) => {
                if !self.engine.inner.media.rtsp_available() {
                    return Err(VqlError::new(
                        ErrorCode::FeatureNotAvailable,
                        "USING RTSP requires the ffmpeg-native feature",
                    ));
                }
                config.name = name.clone();
                config.endpoint = normalize_rtsp_endpoint(&config.endpoint)?;
                rtsp_schema()
            }
            TableProvider::Kafka(_) => schema_for_columns(&create.columns)?,
            TableProvider::External { .. } => {
                return Err(VqlError::new(
                    ErrorCode::FeatureNotAvailable,
                    "this external table provider is catalog-only and cannot be created with VQL SQL",
                ));
            }
        };
        let definition = TableDef::new(name.clone(), provider);
        self.engine
            .inner
            .catalog
            .create_table(&definition, &schema)?;
        Ok(message_result(format!("created table '{name}'")))
    }

    fn drop_table(&self, name: &str) -> Result<DdlResult> {
        self.engine.inner.catalog.drop_table(name)?;
        Ok(message_result(format!("dropped table '{name}'")))
    }

    fn create_model(&self, create: CreateModel) -> Result<DdlResult> {
        let name = validate_user_model_name(&create.name)?;
        let version_name = validate_version_name(&create.version)?;
        let interface = model_interface(create.interface)?;
        let (source, runtime_kind, options) =
            normalize_model_declaration(create.source, create.runtime_kind, create.options)?;
        let declaration_fingerprint = semantic_fingerprint(&(
            &interface,
            &version_name,
            &source,
            &runtime_kind,
            canonical_model_options(&options),
        ));
        let version = ModelVersion {
            name: version_name.clone(),
            source,
            runtime_kind,
            options,
            declaration_fingerprint,
            resolved: None,
            created_at: chrono::Utc::now().timestamp_millis(),
        };
        let initial_version_fingerprint = version.declaration_fingerprint.clone();
        self.engine
            .inner
            .pipelines
            .validate_declaration(&interface, &version)?;
        let model = ModelDef {
            name,
            interface,
            versions: vec![version],
            initial_version_fingerprint,
            default_version: None,
            comment: create.comment,
            builtin: false,
        };
        match self.engine.inner.catalog.create_model(&model) {
            Ok(_) => {}
            Err(error)
                if create.if_not_exists
                    && matches!(error.code, vql_catalog::CatalogErrorCode::AlreadyExists) =>
            {
                return Ok(message_result(format!(
                    "model '{}' already exists",
                    model.name
                )));
            }
            Err(error) if matches!(error.code, vql_catalog::CatalogErrorCode::AlreadyExists) => {
                return Err(VqlError::new(
                    ErrorCode::AlreadyExists,
                    format!(
                        "model '{}' already exists; use ALTER MODEL {} ADD VERSION or DROP MODEL",
                        model.name, model.name
                    ),
                )
                .with_source(error));
            }
            Err(error) => return Err(error.into()),
        }
        Ok(message_result(format!("created model '{}'", model.name)))
    }

    fn resolve_model(&self, name: &str, requested_version: Option<&str>) -> Result<DdlResult> {
        self.resolve_model_internal(name, requested_version, true)
    }

    fn resolve_model_internal(
        &self,
        name: &str,
        requested_version: Option<&str>,
        cancellable: bool,
    ) -> Result<DdlResult> {
        let name = name.to_ascii_lowercase();
        let initial_snapshot = self.engine.inner.catalog.snapshot()?;
        let initial_model = initial_snapshot.model(&name).ok_or_else(|| {
            VqlError::new(
                ErrorCode::NotFound,
                format!("model '{name}' does not exist"),
            )
        })?;
        let version_name = match requested_version {
            Some(version) => validate_version_name(version)?,
            None if initial_model.definition.versions.len() == 1 => {
                initial_model.definition.versions[0].name.clone()
            }
            None => {
                return Err(VqlError::new(
                    ErrorCode::InvalidOption,
                    format!(
                        "model '{name}' has multiple versions ({}); use RESOLVE MODEL {name} VERSION '<version>'",
                        initial_model
                            .definition
                            .versions
                            .iter()
                            .map(|version| version.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                ));
            }
        };
        let initial_version = initial_model
            .definition
            .version(&version_name)
            .ok_or_else(|| {
                VqlError::new(
                    ErrorCode::NotFound,
                    format!("model '{name}' has no version '{version_name}'"),
                )
            })?
            .clone();
        if initial_version.resolved.is_some() {
            return Ok(message_result(format!(
                "model '{}:{}' is already resolved",
                initial_model.definition.name, version_name
            )));
        }
        let interface = initial_model.definition.interface.clone();
        let cancellation = CancellationToken::new();
        let _active_guard = if cancellable {
            let mut active = self.active_query.lock().map_err(|_| {
                VqlError::new(ErrorCode::Internal, "active query lock was poisoned")
            })?;
            *active = Some(ActiveQueryControl {
                cancellation: cancellation.clone(),
                graceful_stop: None,
            });
            Some(ActiveQueryGuard(Arc::clone(&self.active_query)))
        } else {
            None
        };
        let resolved =
            self.engine
                .inner
                .runtime
                .block_on(self.engine.inner.models.resolve_version(
                    &interface,
                    &initial_version,
                    self.engine.inner.config.model_cache_dir(),
                    cancellation,
                ))?;
        let snapshot = self.engine.inner.catalog.snapshot()?;
        let model_object = snapshot.model(&name).ok_or_else(|| {
            VqlError::new(
                ErrorCode::NotFound,
                format!("model '{name}' does not exist"),
            )
        })?;
        let expected_generation = model_object.generation;
        let mut model = model_object.definition.clone();
        let version = model.version(&version_name).ok_or_else(|| {
            VqlError::new(
                ErrorCode::NotFound,
                format!("model '{name}' has no version '{version_name}'"),
            )
        })?;
        ensure_resolution_declaration_unchanged(
            &name,
            &version_name,
            &initial_version.declaration_fingerprint,
            &version.declaration_fingerprint,
        )?;
        if version.resolved.is_some() {
            return Ok(message_result(format!(
                "model '{}:{}' is already resolved",
                model.name, version_name
            )));
        }
        model
            .version_mut(&version_name)
            .expect("selected model version remains present")
            .resolved = Some(resolved);
        if model.default_version.is_none()
            && model.initial_version_fingerprint == initial_version.declaration_fingerprint
        {
            model.default_version = Some(version_name.clone());
        }
        match self
            .engine
            .inner
            .catalog
            .update_model(&name, &model, expected_generation)
        {
            Ok(_) => {}
            Err(error) if error.code == vql_catalog::CatalogErrorCode::Conflict => {
                let peer = self.engine.inner.catalog.snapshot()?;
                let peer_resolved = peer
                    .model(&name)
                    .and_then(|object| object.definition.version(&version_name))
                    .and_then(|version| version.resolved.as_ref());
                let ours = model
                    .version(&version_name)
                    .and_then(|version| version.resolved.as_ref());
                if peer_resolved.map(|resolved| &resolved.semantic_fingerprint)
                    == ours.map(|resolved| &resolved.semantic_fingerprint)
                {
                    // Another resolver committed the same immutable result.
                } else {
                    return Err(error.into());
                }
            }
            Err(error) => return Err(error.into()),
        }
        self.engine.inner.models.evict_stale()?;
        Ok(message_result(format!(
            "resolved model '{}:{}'",
            model.name, version_name
        )))
    }

    fn alter_model(&self, name: &str, action: AlterModel) -> Result<DdlResult> {
        let name = name.to_ascii_lowercase();
        let snapshot = self.engine.inner.catalog.snapshot()?;
        let model_object = snapshot.model(&name).ok_or_else(|| {
            VqlError::new(
                ErrorCode::NotFound,
                format!("model '{name}' does not exist"),
            )
        })?;
        if model_object.definition.builtin {
            return Err(VqlError::new(
                ErrorCode::InvalidOption,
                "built-in Model definitions are release-managed",
            ));
        }
        let expected_generation = model_object.generation;
        let mut model = model_object.definition.clone();
        let message = match action {
            AlterModel::AddVersion {
                if_not_exists,
                version,
                source,
                runtime_kind,
                options,
            } => {
                let version_name = validate_version_name(&version)?;
                if model.version(&version_name).is_some() {
                    if if_not_exists {
                        return Ok(message_result(format!(
                            "model '{}:{}' already exists",
                            model.name, version_name
                        )));
                    }
                    return Err(VqlError::new(
                        ErrorCode::AlreadyExists,
                        format!("model '{}:{}' already exists", model.name, version_name),
                    ));
                }
                let (source, runtime_kind, options) =
                    normalize_model_declaration(source, runtime_kind, options)?;
                let declaration_fingerprint = semantic_fingerprint(&(
                    &model.interface,
                    &version_name,
                    &source,
                    &runtime_kind,
                    canonical_model_options(&options),
                ));
                let version = ModelVersion {
                    name: version_name.clone(),
                    source,
                    runtime_kind,
                    options,
                    declaration_fingerprint,
                    resolved: None,
                    created_at: chrono::Utc::now().timestamp_millis(),
                };
                self.engine
                    .inner
                    .pipelines
                    .validate_declaration(&model.interface, &version)?;
                model.versions.push(version);
                format!("added model version '{}:{}'", model.name, version_name)
            }
            AlterModel::DropVersion { version } => {
                let version_name = validate_version_name(&version)?;
                let position = model
                    .versions
                    .iter()
                    .position(|candidate| candidate.name == version_name)
                    .ok_or_else(|| {
                        VqlError::new(
                            ErrorCode::NotFound,
                            format!("model '{}' has no version '{version_name}'", model.name),
                        )
                    })?;
                if model.default_version.as_deref() == Some(version_name.as_str()) {
                    return Err(VqlError::new(
                        ErrorCode::InvalidOption,
                        format!(
                            "cannot drop default model version '{}:{}'; move default first",
                            model.name, version_name
                        ),
                    ));
                }
                if model.versions.len() == 1 {
                    return Err(VqlError::new(
                        ErrorCode::InvalidOption,
                        format!(
                            "cannot drop the last version of model '{}'; use DROP MODEL {}",
                            model.name, model.name
                        ),
                    ));
                }
                model.versions.remove(position);
                format!("dropped model version '{}:{}'", model.name, version_name)
            }
            AlterModel::SetDefaultVersion { version } => {
                let version_name = validate_version_name(&version)?;
                let version = model.version(&version_name).ok_or_else(|| {
                    VqlError::new(
                        ErrorCode::NotFound,
                        format!("model '{}' has no version '{version_name}'", model.name),
                    )
                })?;
                if version.resolved.is_none() {
                    return Err(VqlError::new(
                        ErrorCode::InvalidOption,
                        format!(
                            "model version '{}:{}' is unresolved; run RESOLVE MODEL {} VERSION '{}'",
                            model.name, version_name, model.name, version_name
                        ),
                    ));
                }
                model.default_version = Some(version_name.clone());
                format!(
                    "set model '{}' default version to '{version_name}'",
                    model.name
                )
            }
            AlterModel::SetComment { comment } => {
                model.comment = Some(comment);
                format!("updated model '{}' comment", model.name)
            }
            AlterModel::RenameTo { name: new_name } => {
                let new_name = validate_user_model_name(&new_name)?;
                model.name = new_name.clone();
                format!("renamed model '{name}' to '{new_name}'")
            }
        };
        self.engine
            .inner
            .catalog
            .update_model(&name, &model, expected_generation)?;
        self.engine.inner.models.evict_stale()?;
        Ok(message_result(message))
    }

    fn create_function(&self, sql: &str) -> Result<DdlResult> {
        let snapshot = self.engine.inner.catalog.snapshot()?;
        let sql = normalize_function_ddl(sql, &snapshot)?;
        let factory = Arc::new(VqlFunctionFactory::default());
        let context = context_for_function_ddl(
            &snapshot,
            Arc::clone(&self.engine.inner.catalog),
            Arc::clone(&self.engine.inner.media),
            Arc::clone(&self.fail_on_error),
            self.python_udf_host.clone(),
            Arc::clone(&factory),
        )?;
        self.engine
            .inner
            .runtime
            .block_on(context.sql(&sql))
            .map_err(function_ddl_error)?;
        let mut function = factory.take_definition()?;
        if let FunctionImplementation::SqlMacro { expression } = &function.implementation {
            function.constant_parameters =
                infer_constant_parameters(expression, &function.parameters, &snapshot)?;
            function.semantic_fingerprint = semantic_fingerprint(&function);
        }
        self.engine.inner.catalog.create_function(&function)?;
        Ok(message_result(format!(
            "created function '{}'",
            function.name
        )))
    }

    fn insert_into_table(&self, sql: &str) -> Result<QueryHandle> {
        let snapshot = self.engine.inner.catalog.snapshot()?;
        self.insert_into_table_with_snapshot(sql, snapshot)
    }

    fn insert_into_table_with_snapshot(
        &self,
        sql: &str,
        snapshot: crate::catalog::DefinitionSnapshot,
    ) -> Result<QueryHandle> {
        let mut parts = sql
            .trim()
            .trim_end_matches(';')
            .splitn(4, char::is_whitespace);
        if !parts
            .next()
            .is_some_and(|value| value.eq_ignore_ascii_case("INSERT"))
            || !parts
                .next()
                .is_some_and(|value| value.eq_ignore_ascii_case("INTO"))
        {
            return Err(VqlError::new(
                ErrorCode::InvalidSql,
                "expected INSERT INTO <table> SELECT ...",
            ));
        }
        let table_name = parts.next().ok_or_else(|| {
            VqlError::new(ErrorCode::InvalidSql, "INSERT INTO requires a table name")
        })?;
        let query = parts.next().ok_or_else(|| {
            VqlError::new(ErrorCode::InvalidSql, "INSERT INTO requires a SELECT query")
        })?;
        if !query
            .trim_start()
            .to_ascii_uppercase()
            .starts_with("SELECT")
            && !query.trim_start().to_ascii_uppercase().starts_with("WITH")
        {
            return Err(VqlError::new(
                ErrorCode::InvalidSql,
                "INSERT INTO writes require SELECT or WITH",
            ));
        }
        let table = snapshot.table(table_name).cloned().ok_or_else(|| {
            VqlError::new(
                ErrorCode::NotFound,
                format!("table '{table_name}' does not exist"),
            )
        })?;
        let TableProvider::Kafka(config) = &table.definition.provider else {
            return Err(VqlError::new(
                ErrorCode::InvalidSql,
                format!("table '{table_name}' is not writable"),
            ));
        };
        let mut handle = self.query_with_snapshot(query, snapshot)?;
        if !table.schema.fields().is_empty()
            && !schemas_are_write_compatible(&table.schema, &handle.output_schema)
        {
            return Err(VqlError::new(
                ErrorCode::InvalidSql,
                format!(
                    "INSERT INTO table '{table_name}' output schema does not match the declared table schema"
                ),
            ));
        }
        let write_schema = if table.schema.fields().is_empty() {
            Arc::clone(&handle.output_schema)
        } else {
            Arc::clone(&table.schema)
        };
        let target = SinkTarget::try_new(
            table.definition.name.clone(),
            config.clone(),
            &handle.output_schema,
            write_schema,
            handle.cancellation.clone(),
            self.engine.inner.config.secret_provider().cloned(),
            handle.budget.clone(),
        )?;
        if let Some(streaming) = handle.streaming.as_mut() {
            streaming.sink = Some(target.clone());
        } else {
            handle.dataframe = wrap_sink(handle.dataframe, target.clone());
        }
        handle.sink_target = Some(target);
        handle.uses_sink = true;
        Ok(handle)
    }

    fn drop_object(&self, kind: ShowKind, name: &str) -> Result<DdlResult> {
        match kind {
            ShowKind::Tables => return self.drop_table(name),
            ShowKind::Models => {
                let snapshot = self.engine.inner.catalog.snapshot()?;
                if snapshot
                    .model(name)
                    .is_some_and(|model| model.definition.builtin)
                {
                    return Err(VqlError::new(
                        ErrorCode::InvalidOption,
                        "built-in Model definitions are release-managed",
                    ));
                }
                self.engine.inner.catalog.drop_model(name)?;
                self.engine.inner.models.evict_stale()?;
            }
            ShowKind::Functions => {
                self.engine
                    .inner
                    .catalog
                    .drop_object(ObjectKind::Function, name)?;
            }
        }
        Ok(message_result(format!("dropped object '{name}'")))
    }

    fn show_objects(&self, kind: ShowKind) -> Result<DdlResult> {
        let snapshot = self.engine.inner.catalog.snapshot()?;
        self.show_objects_with_snapshot(kind, &snapshot)
    }

    fn show_objects_with_snapshot(
        &self,
        kind: ShowKind,
        snapshot: &crate::catalog::DefinitionSnapshot,
    ) -> Result<DdlResult> {
        if kind == ShowKind::Tables {
            return self.show_tables(snapshot);
        }
        if kind == ShowKind::Models {
            return self.show_models(snapshot);
        }
        self.show_functions(snapshot)
    }

    fn show_create(&self, kind: ShowKind, name: &str, version: Option<&str>) -> Result<DdlResult> {
        let snapshot = self.engine.inner.catalog.snapshot()?;
        self.show_create_with_snapshot(kind, name, version, &snapshot)
    }

    fn show_create_with_snapshot(
        &self,
        kind: ShowKind,
        name: &str,
        version: Option<&str>,
        snapshot: &crate::catalog::DefinitionSnapshot,
    ) -> Result<DdlResult> {
        if kind == ShowKind::Models {
            let model = snapshot.model(name).ok_or_else(|| {
                VqlError::new(
                    ErrorCode::NotFound,
                    format!("model '{name}' does not exist"),
                )
            })?;
            let model = &model.definition;
            let selected = match version {
                Some(version) => model.version(version).ok_or_else(|| {
                    VqlError::new(
                        ErrorCode::NotFound,
                        format!("model '{name}' has no version '{version}'"),
                    )
                })?,
                None => model.versions.last().ok_or_else(|| {
                    VqlError::new(
                        ErrorCode::Catalog,
                        format!("model '{name}' has no live versions"),
                    )
                })?,
            };
            return show_create_result(
                &model.name,
                kind,
                render_create_model(model, selected),
                Some(&selected.name),
            );
        }
        let (object_type, create_sql) = match kind {
            ShowKind::Tables => (
                "TABLE",
                snapshot
                    .table(name)
                    .map(|object| render_create_table(&object.definition, &object.schema)),
            ),
            ShowKind::Models => {
                unreachable!("Model definitions are rendered with a selected version")
            }
            ShowKind::Functions => (
                "FUNCTION",
                snapshot
                    .function(name)
                    .map(|object| render_create(&object.definition)),
            ),
        };
        let create_sql = create_sql.ok_or_else(|| {
            VqlError::new(
                ErrorCode::NotFound,
                format!(
                    "{} '{}' does not exist",
                    object_type.to_ascii_lowercase(),
                    name
                ),
            )
        })??;
        show_create_result(name, kind, create_sql, None)
    }

    fn set(&self, sql: &str) -> Result<DdlResult> {
        let (name, value) = parse_session_setting(sql)?;
        match name.as_str() {
            "vql.on_error" => {
                let fail = match value.as_str() {
                    "fail" => true,
                    "null" => false,
                    _ => {
                        return Err(VqlError::new(
                            ErrorCode::InvalidOption,
                            "SET vql.on_error accepts 'null' or 'fail'",
                        ));
                    }
                };
                self.fail_on_error.store(fail, Ordering::Relaxed);
                Ok(message_result(format!(
                    "vql.on_error = '{}'",
                    if fail { "fail" } else { "null" }
                )))
            }
            _ => Err(VqlError::new(
                ErrorCode::InvalidOption,
                format!("unknown session setting '{name}'"),
            )),
        }
    }

    fn show_tables(&self, snapshot: &crate::catalog::DefinitionSnapshot) -> Result<DdlResult> {
        let mut names = Vec::new();
        let mut providers = Vec::new();
        let mut locations = Vec::new();
        for (name, table) in snapshot.tables() {
            names.push(name.to_owned());
            providers.push(format!("{:?}", table.definition.provider.kind()).to_ascii_uppercase());
            locations.push(
                table
                    .definition
                    .provider
                    .location()
                    .unwrap_or_default()
                    .to_owned(),
            );
        }
        let schema = show_tables_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names)) as ArrayRef,
                Arc::new(StringArray::from(providers)),
                Arc::new(StringArray::from(locations)),
            ],
        )
        .map_err(|error| {
            VqlError::new(ErrorCode::Execution, error.to_string()).with_source(error)
        })?;
        Ok(DdlResult {
            message: format!("{} table(s)", batch.num_rows()),
            batches: vec![batch],
        })
    }

    fn show_models(&self, snapshot: &crate::catalog::DefinitionSnapshot) -> Result<DdlResult> {
        let models = snapshot.models().collect::<Vec<_>>();
        let schema = show_models_schema();
        let addresses = models
            .iter()
            .map(|(name, _)| object_address(name))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    addresses
                        .iter()
                        .map(|(catalog, _, _)| catalog.as_str())
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    addresses
                        .iter()
                        .map(|(_, schema, _)| schema.as_str())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    addresses
                        .iter()
                        .map(|(_, _, name)| name.as_str())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    models
                        .iter()
                        .map(|(_, model)| format_model_interface(&model.definition.interface))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    models
                        .iter()
                        .map(|(_, model)| model.definition.versions.len() as i64)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    models
                        .iter()
                        .map(|(_, model)| model.definition.default_version.clone())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    models
                        .iter()
                        .map(|(_, model)| model.definition.comment.clone())
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .map_err(|error| {
            VqlError::new(ErrorCode::Execution, "failed to build Model catalog result")
                .with_source(error)
        })?;
        Ok(DdlResult {
            message: format!("{} model(s)", batch.num_rows()),
            batches: vec![batch],
        })
    }

    fn show_model_versions(&self, name: &str) -> Result<DdlResult> {
        let snapshot = self.engine.inner.catalog.snapshot()?;
        self.show_model_versions_with_snapshot(name, &snapshot)
    }

    fn show_model_versions_with_snapshot(
        &self,
        name: &str,
        snapshot: &crate::catalog::DefinitionSnapshot,
    ) -> Result<DdlResult> {
        let model = snapshot.model(name).ok_or_else(|| {
            VqlError::new(
                ErrorCode::NotFound,
                format!("model '{name}' does not exist"),
            )
        })?;
        let (catalog, schema_name, object_name) = object_address(name);
        let versions = &model.definition.versions;
        let schema = show_model_versions_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![catalog.as_str(); versions.len()])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    schema_name.as_str();
                    versions.len()
                ])),
                Arc::new(StringArray::from(vec![
                    object_name.as_str();
                    versions.len()
                ])),
                Arc::new(StringArray::from(
                    versions
                        .iter()
                        .map(|version| version.name.as_str())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    versions
                        .iter()
                        .map(|version| {
                            if version.resolved.is_some() {
                                "RESOLVED"
                            } else {
                                "UNRESOLVED"
                            }
                        })
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    versions
                        .iter()
                        .map(|version| {
                            if version
                                .resolved
                                .as_ref()
                                .is_some_and(|value| value.volatile)
                            {
                                "VOLATILE"
                            } else {
                                "IMMUTABLE"
                            }
                        })
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    versions
                        .iter()
                        .map(|version| {
                            version
                                .resolved
                                .as_ref()
                                .map(|resolved| resolved.semantic_fingerprint.clone())
                        })
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    versions
                        .iter()
                        .map(|version| version.created_at)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(BooleanArray::from(
                    versions
                        .iter()
                        .map(|version| {
                            model.definition.default_version.as_deref()
                                == Some(version.name.as_str())
                        })
                        .collect::<Vec<_>>(),
                )),
            ],
        )?;
        Ok(DdlResult {
            message: format!("{} version(s)", versions.len()),
            batches: vec![batch],
        })
    }

    fn show_functions(&self, snapshot: &crate::catalog::DefinitionSnapshot) -> Result<DdlResult> {
        let mut rows = snapshot
            .functions()
            .map(|(name, function)| {
                let (catalog, schema, name) = object_address(name);
                (
                    catalog,
                    schema,
                    name,
                    "FUNCTION".to_owned(),
                    format_function_arguments(&function.definition),
                    function.definition.return_type.clone(),
                )
            })
            .collect::<Vec<_>>();
        rows.extend(snapshot.models().map(|(name, model)| {
            let (catalog, schema, name) = object_address(name);
            (
                catalog,
                schema,
                name,
                "MODEL".to_owned(),
                model.definition.interface.render_arguments(),
                model.definition.interface.return_type.clone(),
            )
        }));
        rows.sort_by(|left, right| {
            (&left.0, &left.1, &left.2).cmp(&(&right.0, &right.1, &right.2))
        });
        let schema = show_functions_schema();
        let batch = RecordBatch::try_new(
            schema,
            (0..6)
                .map(|column| {
                    Arc::new(StringArray::from(
                        rows.iter()
                            .map(|row| match column {
                                0 => row.0.as_str(),
                                1 => row.1.as_str(),
                                2 => row.2.as_str(),
                                3 => row.3.as_str(),
                                4 => row.4.as_str(),
                                _ => row.5.as_str(),
                            })
                            .collect::<Vec<_>>(),
                    )) as ArrayRef
                })
                .collect(),
        )?;
        Ok(DdlResult {
            message: format!("{} callable(s)", rows.len()),
            batches: vec![batch],
        })
    }

    fn describe(&self, kind: ShowKind, name: &str) -> Result<DdlResult> {
        let snapshot = self.engine.inner.catalog.snapshot()?;
        self.describe_with_snapshot(kind, name, &snapshot)
    }

    fn describe_with_snapshot(
        &self,
        kind: ShowKind,
        name: &str,
        snapshot: &crate::catalog::DefinitionSnapshot,
    ) -> Result<DdlResult> {
        if kind == ShowKind::Models {
            return self.describe_model(name, snapshot);
        }
        if kind == ShowKind::Functions {
            return self.describe_function(name, snapshot);
        }
        let relation_schema = snapshot
            .table(name)
            .map(|table| Arc::clone(&table.schema))
            .ok_or_else(|| {
                VqlError::new(
                    ErrorCode::NotFound,
                    format!("relation '{name}' does not exist"),
                )
            })?;
        let fields = relation_schema.fields();
        let schema = describe_table_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    fields
                        .iter()
                        .map(|field| field.name().as_str())
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    fields
                        .iter()
                        .map(|field| field.data_type().to_string())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    fields
                        .iter()
                        .map(|field| field.is_nullable().to_string())
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .map_err(|error| {
            VqlError::new(ErrorCode::Execution, error.to_string()).with_source(error)
        })?;
        Ok(DdlResult {
            message: format!("relation '{name}'"),
            batches: vec![batch],
        })
    }

    fn describe_model(
        &self,
        name: &str,
        snapshot: &crate::catalog::DefinitionSnapshot,
    ) -> Result<DdlResult> {
        let model = snapshot.model(name).ok_or_else(|| {
            VqlError::new(
                ErrorCode::NotFound,
                format!("model '{name}' does not exist"),
            )
        })?;
        let status = match model.definition.default() {
            Some(version) if version.resolved.is_some() => "RESOLVED",
            Some(_) => "UNRESOLVED",
            None => "UNPUBLISHED",
        };
        callable_description_result(
            &model.definition.name,
            "MODEL",
            &model.definition.interface.render_arguments(),
            &model.definition.interface.return_type,
            status,
            model.definition.default_version.as_deref(),
        )
    }

    fn describe_function(
        &self,
        name: &str,
        snapshot: &crate::catalog::DefinitionSnapshot,
    ) -> Result<DdlResult> {
        let function = snapshot.function(name).ok_or_else(|| {
            VqlError::new(
                ErrorCode::NotFound,
                format!("function '{name}' does not exist"),
            )
        })?;
        let status = match &function.definition.implementation {
            FunctionImplementation::SqlMacro { expression } => snapshot
                .models()
                .find(|(model_name, _)| {
                    expression
                        .to_ascii_lowercase()
                        .contains(&format!("{}(", model_name.to_ascii_lowercase()))
                })
                .map_or("AVAILABLE".to_owned(), |(model_name, model)| {
                    if model
                        .definition
                        .default()
                        .is_some_and(|version| version.resolved.is_some())
                    {
                        format!("MODEL {model_name} RESOLVED")
                    } else {
                        format!("MODEL {model_name} UNRESOLVED")
                    }
                }),
            FunctionImplementation::Python { .. } => "AVAILABLE".to_owned(),
        };
        callable_description_result(
            &function.definition.name,
            "FUNCTION",
            &format_function_arguments(&function.definition),
            &function.definition.return_type,
            &status,
            None,
        )
    }

    fn execute_catalog_query(
        &self,
        command: &CatalogQuery,
        snapshot: &crate::catalog::DefinitionSnapshot,
    ) -> Result<DdlResult> {
        match command {
            CatalogQuery::Show(kind) => self.show_objects_with_snapshot(*kind, snapshot),
            CatalogQuery::ShowModelVersions { name } => {
                self.show_model_versions_with_snapshot(name, snapshot)
            }
            CatalogQuery::ShowCreate {
                kind,
                name,
                version,
            } => self.show_create_with_snapshot(*kind, name, version.as_deref(), snapshot),
            CatalogQuery::Describe { kind, name } => {
                self.describe_with_snapshot(*kind, name, snapshot)
            }
        }
    }
}

fn validate_user_model_name(name: &str) -> Result<String> {
    let name = name.to_ascii_lowercase();
    if name.is_empty() {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            "model name must not be empty",
        ));
    }
    let reserved_leaf = name
        .rsplit('.')
        .next()
        .map(str::to_ascii_uppercase)
        .unwrap_or_default();
    if reserved_leaf.starts_with("VQL_") {
        return Err(VqlError::new(
            ErrorCode::NameConflict,
            "VQL_* callable names are reserved for built-in AI functions",
        ));
    }
    if reserved_leaf.starts_with("__VQL_") {
        return Err(VqlError::new(
            ErrorCode::NameConflict,
            "__VQL_* callable names are reserved for internal planning markers",
        ));
    }
    if name == "builtin" || name.starts_with("builtin.") || name.starts_with("vql.builtin.") {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            "built-in Model definitions are release-managed",
        ));
    }
    Ok(name)
}

fn validate_version_name(version: &str) -> Result<String> {
    let version = version.trim();
    if version.is_empty() || version.chars().any(char::is_control) {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            "model version name must be a non-empty string without control characters",
        ));
    }
    if version.eq_ignore_ascii_case("default") {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            "model version name 'default' is reserved",
        ));
    }
    Ok(version.to_owned())
}

fn model_interface(spec: ModelInterfaceSpec) -> Result<ModelInterface> {
    match spec {
        ModelInterfaceSpec::Capability(ModelType::ObjectDetection) => {
            Ok(crate::models::object_detection_interface())
        }
        ModelInterfaceSpec::Capability(ModelType::ImageClassification) => {
            Ok(crate::models::image_classification_interface())
        }
        ModelInterfaceSpec::Signature {
            parameters,
            return_type,
        } => {
            for (_, data_type) in &parameters {
                validate_generic_model_type(data_type, false)?;
            }
            validate_generic_model_type(&return_type, true)?;
            Ok(ModelInterface {
                capability: None,
                parameters: parameters
                    .into_iter()
                    .map(|(name, data_type)| ModelParameter {
                        name,
                        data_type,
                        constant: false,
                        optional: false,
                    })
                    .collect(),
                semantic_arguments: Vec::new(),
                return_type,
                processing_family: "generic.tensor".to_owned(),
                deterministic: true,
            })
        }
    }
}

fn validate_generic_model_type(data_type: &str, output: bool) -> Result<()> {
    let upper = data_type.trim().to_ascii_uppercase();
    if upper == "MODEL" || upper.contains(" MODEL") {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            "MODEL is not a SQL value type; use the model as a call target",
        ));
    }
    if upper == "STRING" || upper == "VARCHAR" || upper == "TEXT" {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            "STRING is not supported at an embedded generic Model boundary; use a capability preset with tokenization",
        ));
    }
    let scalar = matches!(
        upper.as_str(),
        "TINYINT"
            | "INT8"
            | "SMALLINT"
            | "INT16"
            | "INT"
            | "INTEGER"
            | "INT32"
            | "BIGINT"
            | "INT64"
            | "UINT8"
            | "FLOAT"
            | "FLOAT32"
            | "DOUBLE"
            | "FLOAT64"
            | "REAL"
    );
    let vector = upper
        .strip_prefix("VECTOR(")
        .and_then(|value| value.strip_suffix(')'))
        .and_then(|value| value.trim().parse::<usize>().ok())
        .is_some_and(|dimension| dimension > 0);
    let tensor = upper
        .strip_prefix("TENSOR(")
        .and_then(|value| value.strip_suffix(')'))
        .is_some_and(valid_tensor_signature);
    let structure = output && upper.starts_with("STRUCT<") && upper.ends_with('>');
    if upper == "IMAGE" || scalar || vector || tensor || structure {
        return Ok(());
    }
    Err(VqlError::new(
        ErrorCode::InvalidOption,
        format!("unsupported generic Model boundary type '{data_type}'"),
    ))
}

fn valid_tensor_signature(value: &str) -> bool {
    let mut parts = value.split(',').map(str::trim);
    let dtype = parts.next().unwrap_or_default();
    if !matches!(
        dtype,
        "FLOAT32" | "FLOAT64" | "INT8" | "INT16" | "INT32" | "INT64" | "UINT8"
    ) {
        return false;
    }
    let dimensions = parts.collect::<Vec<_>>();
    !dimensions.is_empty()
        && dimensions
            .iter()
            .all(|value| value.parse::<usize>().is_ok_and(|dimension| dimension > 0))
}

fn normalize_model_declaration(
    mut source: String,
    runtime_kind: Option<String>,
    mut options: std::collections::BTreeMap<String, serde_json::Value>,
) -> Result<(
    String,
    String,
    std::collections::BTreeMap<String, serde_json::Value>,
)> {
    let inferred =
        if source.starts_with("mock://") || source.to_ascii_lowercase().ends_with(".onnx") {
            Some("onnx-runtime")
        } else if source.starts_with("triton+http://") || source.starts_with("triton+https://") {
            Some("triton-inference-server")
        } else {
            None
        };
    let runtime_kind = runtime_kind
        .or(inferred.map(str::to_owned))
        .ok_or_else(|| {
            VqlError::new(
                ErrorCode::InvalidOption,
                "Model source has no unambiguous Runtime default; add USING <runtime>",
            )
        })?;
    if runtime_kind == "triton-inference-server"
        && (source.starts_with("triton+http://") || source.starts_with("triton+https://"))
    {
        let http_source = source.strip_prefix("triton+").expect("prefix checked");
        let mut url = url::Url::parse(http_source).map_err(|error| {
            VqlError::new(
                ErrorCode::InvalidLocation,
                "invalid triton+http(s) Model source URI",
            )
            .with_source(error)
        })?;
        let route = url.path().trim_matches('/');
        if route.is_empty() || route.contains('/') {
            return Err(VqlError::new(
                ErrorCode::InvalidLocation,
                "Triton Model source must end in /model[@server_version]",
            ));
        }
        let (model, server_version) = route
            .rsplit_once('@')
            .map_or((route, None), |(model, version)| (model, Some(version)));
        if model.is_empty() || server_version.is_some_and(str::is_empty) {
            return Err(VqlError::new(
                ErrorCode::InvalidLocation,
                "Triton Model source must end in /model[@server_version]",
            ));
        }
        if options
            .insert("model".to_owned(), serde_json::json!(model))
            .is_some()
        {
            return Err(VqlError::new(
                ErrorCode::InvalidOption,
                "OPTIONS.model conflicts with the Triton source URI",
            ));
        }
        if let Some(version) = server_version
            && options
                .insert("version".to_owned(), serde_json::json!(version))
                .is_some()
        {
            return Err(VqlError::new(
                ErrorCode::InvalidOption,
                "OPTIONS.version conflicts with @server_version in the Triton source URI",
            ));
        }
        url.set_path("");
        source = url.to_string().trim_end_matches('/').to_owned();
    }
    Ok((source, runtime_kind, options))
}

fn object_address(name: &str) -> (String, String, String) {
    let parts = name.split('.').collect::<Vec<_>>();
    match parts.as_slice() {
        [catalog, schema, name] => (
            (*catalog).to_owned(),
            (*schema).to_owned(),
            (*name).to_owned(),
        ),
        [schema, name] => ("vql".to_owned(), (*schema).to_owned(), (*name).to_owned()),
        [name] => ("vql".to_owned(), "default".to_owned(), (*name).to_owned()),
        _ => ("vql".to_owned(), "default".to_owned(), name.to_owned()),
    }
}

fn parse_session_setting(sql: &str) -> Result<(String, String)> {
    let statement = sql.trim().trim_end_matches(';').trim();
    let rest = statement.get(3..).filter(|_| {
        statement
            .get(..3)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("SET"))
    });
    let rest = rest.ok_or_else(|| VqlError::new(ErrorCode::InvalidSql, "expected SET"))?;
    let (name, value) = rest
        .split_once('=')
        .ok_or_else(|| VqlError::new(ErrorCode::InvalidSql, "SET requires <setting> = <value>"))?;
    let name = name.trim().to_ascii_lowercase();
    let value = value.trim();
    let value = if value.len() >= 2
        && ((value.starts_with('\'') && value.ends_with('\''))
            || (value.starts_with('"') && value.ends_with('"')))
    {
        &value[1..value.len() - 1]
    } else {
        value
    };
    Ok((name, value.trim().to_ascii_lowercase()))
}

fn ensure_resolution_declaration_unchanged(
    model_name: &str,
    version_name: &str,
    initial_fingerprint: &str,
    current_fingerprint: &str,
) -> Result<()> {
    if current_fingerprint == initial_fingerprint {
        return Ok(());
    }
    Err(VqlError::new(
        ErrorCode::Catalog,
        format!(
            "model '{model_name}:{version_name}' changed while RESOLVE MODEL was running; retry the statement"
        ),
    ))
}

fn format_model_interface(interface: &ModelInterface) -> String {
    interface.capability.map_or_else(
        || {
            format!(
                "({}) RETURNS {}",
                interface
                    .parameters
                    .iter()
                    .map(|parameter| format!("{} {}", parameter.name, parameter.data_type))
                    .collect::<Vec<_>>()
                    .join(", "),
                interface.return_type
            )
        },
        |capability| format!("TYPE {}", capability.as_str()),
    )
}

fn format_function_arguments(function: &crate::catalog::FunctionDef) -> String {
    function
        .parameters
        .iter()
        .map(|(name, data_type)| {
            if function
                .constant_parameters
                .iter()
                .any(|constant| constant.eq_ignore_ascii_case(name))
            {
                format!("{name} CONST {data_type}")
            } else {
                format!("{name} {data_type}")
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn callable_description_result(
    name: &str,
    kind: &str,
    arguments: &str,
    return_type: &str,
    status: &str,
    default_version: Option<&str>,
) -> Result<DdlResult> {
    let (catalog, schema_name, object_name) = object_address(name);
    let schema = describe_callable_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![catalog])) as ArrayRef,
            Arc::new(StringArray::from(vec![schema_name])),
            Arc::new(StringArray::from(vec![object_name])),
            Arc::new(StringArray::from(vec![kind])),
            Arc::new(StringArray::from(vec![arguments])),
            Arc::new(StringArray::from(vec![return_type])),
            Arc::new(StringArray::from(vec![status])),
            Arc::new(StringArray::from(vec![default_version])),
        ],
    )?;
    Ok(DdlResult {
        message: format!("{} '{}'", kind.to_ascii_lowercase(), name),
        batches: vec![batch],
    })
}

fn schema_for_columns(columns: &[TableColumn]) -> Result<SchemaRef> {
    let fields = columns
        .iter()
        .map(|column| {
            let data_type = match column.data_type.as_str() {
                "STRING" => DataType::Utf8,
                "BIGINT" | "LONG" => DataType::Int64,
                "INT" | "INTEGER" => DataType::Int32,
                "BOOLEAN" => DataType::Boolean,
                "FLOAT" => DataType::Float32,
                "DOUBLE" => DataType::Float64,
                "TIMESTAMP" => {
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, Some("UTC".into()))
                }
                data_type => {
                    return Err(VqlError::new(
                        ErrorCode::InvalidSql,
                        format!("unsupported table column type '{data_type}'"),
                    ));
                }
            };
            Ok(Field::new(&column.name, data_type, column.nullable))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn schemas_are_write_compatible(target: &SchemaRef, output: &SchemaRef) -> bool {
    target.fields().len() == output.fields().len()
        && target
            .fields()
            .iter()
            .zip(output.fields())
            .all(|(target, output)| {
                target.data_type() == output.data_type()
                    && (target.is_nullable() || !output.is_nullable())
            })
}

fn function_ddl_error(error: datafusion::common::DataFusionError) -> VqlError {
    match error {
        datafusion::common::DataFusionError::External(source) => {
            match source.downcast::<VqlError>() {
                Ok(error) => *error,
                Err(source) => VqlError::new(ErrorCode::InvalidSql, source.to_string()),
            }
        }
        datafusion::common::DataFusionError::Context(_, source)
        | datafusion::common::DataFusionError::Diagnostic(_, source) => function_ddl_error(*source),
        source => VqlError::new(ErrorCode::InvalidSql, source.to_string()).with_source(source),
    }
}

fn normalize_rtsp_endpoint(endpoint: &str) -> Result<String> {
    let parsed = url::Url::parse(endpoint).map_err(|error| {
        VqlError::new(
            ErrorCode::InvalidLocation,
            "RTSP endpoint must be an absolute rtsp:// URL",
        )
        .with_source(error)
    })?;
    if parsed.scheme() != "rtsp" || parsed.host_str().is_none() {
        return Err(VqlError::new(
            ErrorCode::InvalidLocation,
            "RTSP endpoint must be an absolute rtsp:// URL with a host",
        ));
    }
    if !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            "RTSP credentials, query parameters, and fragments cannot be stored in the Catalog; use an endpoint without embedded secrets",
        ));
    }
    Ok(parsed.to_string())
}

fn normalize_location(location: &str) -> Result<PathBuf> {
    let path = Path::new(location);
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    path.canonicalize().map_err(|error| {
        VqlError::new(
            ErrorCode::InvalidLocation,
            format!("cannot open LOCATION '{}': {error}", path.display()),
        )
        .with_source(error)
    })
}

fn message_result(message: String) -> DdlResult {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![message.as_str()]))],
    )
    .expect("message result schema and column always match");
    DdlResult {
        message,
        batches: vec![batch],
    }
}

fn normalize_statement_sql(sql: &str) -> Result<String> {
    let normalized = sql.trim().trim_end_matches(';').trim();
    if normalized.is_empty() {
        return Err(VqlError::new(ErrorCode::InvalidSql, "expected a statement"));
    }
    Ok(normalized.to_owned())
}

fn validate_semantic_settings(settings: &BTreeMap<String, String>) -> Result<()> {
    for (name, value) in settings {
        match (name.as_str(), value.as_str()) {
            ("vql.on_error", "null" | "fail") => {}
            ("vql.on_error", _) => {
                return Err(VqlError::new(
                    ErrorCode::InvalidOption,
                    "vql.on_error accepts 'null' or 'fail'",
                ));
            }
            _ => {
                return Err(VqlError::new(
                    ErrorCode::InvalidOption,
                    format!("unknown semantic session setting '{name}'"),
                ));
            }
        }
    }
    Ok(())
}

fn statement_schema_with_metadata(schema: &SchemaRef, info: StatementInfo) -> SchemaRef {
    let mut metadata = schema.metadata().clone();
    metadata.insert("vql.statement_info.version".to_owned(), "1".to_owned());
    metadata.insert(
        "vql.statement_info.kind".to_owned(),
        info.kind.as_str().to_owned(),
    );
    metadata.insert(
        "vql.statement_info.query_mode".to_owned(),
        info.query_mode.as_str().to_owned(),
    );
    metadata.insert(
        "vql.statement_info.result_mode".to_owned(),
        info.result_mode.as_str().to_owned(),
    );
    Arc::new(Schema::new_with_metadata(schema.fields().clone(), metadata))
}

fn query_management_info() -> StatementInfo {
    StatementInfo {
        kind: StatementKind::Query,
        query_mode: QueryMode::Bounded,
        result_mode: ResultMode::Bounded,
    }
}

fn catalog_query_schema(command: &CatalogQuery) -> SchemaRef {
    match command {
        CatalogQuery::Show(ShowKind::Tables) => show_tables_schema(),
        CatalogQuery::Show(ShowKind::Models) => show_models_schema(),
        CatalogQuery::Show(ShowKind::Functions) => show_functions_schema(),
        CatalogQuery::ShowModelVersions { .. } => show_model_versions_schema(),
        CatalogQuery::ShowCreate { kind, .. } => show_create_schema(*kind),
        CatalogQuery::Describe {
            kind: ShowKind::Tables,
            ..
        } => describe_table_schema(),
        CatalogQuery::Describe { .. } => describe_callable_schema(),
    }
}

fn show_tables_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("table_name", DataType::Utf8, false),
        Field::new("provider", DataType::Utf8, false),
        Field::new("location", DataType::Utf8, false),
    ]))
}

fn show_models_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("catalog", DataType::Utf8, false),
        Field::new("schema", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("interface", DataType::Utf8, false),
        Field::new("versions", DataType::Int64, false),
        Field::new("default_version", DataType::Utf8, true),
        Field::new("comment", DataType::Utf8, true),
    ]))
}

fn show_functions_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("catalog", DataType::Utf8, false),
        Field::new("schema", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("arguments", DataType::Utf8, false),
        Field::new("return_type", DataType::Utf8, false),
    ]))
}

fn show_model_versions_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("catalog", DataType::Utf8, false),
        Field::new("schema", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("version", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("volatility", DataType::Utf8, false),
        Field::new("fingerprint", DataType::Utf8, true),
        Field::new("created_at", DataType::Int64, false),
        Field::new("is_default", DataType::Boolean, false),
    ]))
}

fn show_create_schema(kind: ShowKind) -> SchemaRef {
    let mut fields = vec![
        Field::new("object_name", DataType::Utf8, false),
        Field::new("object_type", DataType::Utf8, false),
        Field::new("create_sql", DataType::Utf8, false),
    ];
    if kind == ShowKind::Models {
        fields.push(Field::new("version", DataType::Utf8, false));
    }
    Arc::new(Schema::new(fields))
}

fn describe_table_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("column_name", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("nullable", DataType::Utf8, false),
    ]))
}

fn describe_callable_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("catalog", DataType::Utf8, false),
        Field::new("schema", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("arguments", DataType::Utf8, false),
        Field::new("return_type", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("default_version", DataType::Utf8, true),
    ]))
}

fn persistent_submission_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("query_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("state", DataType::Utf8, false),
    ]))
}

fn timestamp_field(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        nullable,
    )
}

fn show_queries_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("query_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("source_health", DataType::Utf8, true),
        timestamp_field("last_event_time", true),
        timestamp_field("started_at", true),
        timestamp_field("updated_at", false),
        Field::new("restart_gap_count", DataType::Int64, false),
        Field::new("error_code", DataType::Utf8, true),
        Field::new("error_message", DataType::Utf8, true),
    ]))
}

fn describe_query_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("query_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("sql_redacted", DataType::Utf8, false),
        timestamp_field("created_at", false),
        timestamp_field("started_at", true),
        timestamp_field("updated_at", false),
        timestamp_field("last_restart_at", true),
        timestamp_field("restart_gap_started_at", true),
        timestamp_field("restart_gap_ended_at", true),
        Field::new("last_restart_reset_window_state", DataType::Boolean, false),
        Field::new("error_code", DataType::Utf8, true),
        Field::new("error_message", DataType::Utf8, true),
    ]))
}

fn show_create_result(
    name: &str,
    kind: ShowKind,
    create_sql: String,
    version: Option<&str>,
) -> Result<DdlResult> {
    let schema = show_create_schema(kind);
    let object_type = match kind {
        ShowKind::Tables => "TABLE",
        ShowKind::Models => "MODEL",
        ShowKind::Functions => "FUNCTION",
    };
    let mut columns = vec![
        Arc::new(StringArray::from(vec![name])) as ArrayRef,
        Arc::new(StringArray::from(vec![object_type])),
        Arc::new(StringArray::from(vec![create_sql.as_str()])),
    ];
    if let Some(version) = version {
        columns.push(Arc::new(StringArray::from(vec![version])));
    }
    let batch = RecordBatch::try_new(schema, columns).map_err(|error| {
        VqlError::new(ErrorCode::Execution, "failed to build SHOW CREATE result").with_source(error)
    })?;
    Ok(DdlResult {
        message: format!("{object_type} '{name}'"),
        batches: vec![batch],
    })
}

fn restamp_schema(schema: &SchemaRef) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| {
                if is_image_storage(field.data_type()) {
                    Arc::new(image_field(field.name(), field.is_nullable()))
                } else {
                    Arc::clone(field)
                }
            })
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EngineConfig;
    use arrow::array::{Array, Float64Array, Int64Array};
    use base64::Engine as _;
    use datafusion::execution::memory_pool::MemoryPool;
    use image::{Rgb, RgbImage};
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener};
    use tempfile::tempdir;

    fn triton_metadata_body() -> &'static str {
        r#"{"inputs":[{"name":"image","datatype":"BYTES","shape":[-1]}],"outputs":[{"name":"detections","datatype":"BYTES","shape":[-1]}]}"#
    }

    fn write_http_response(stream: &mut std::net::TcpStream, body: &str) {
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .unwrap();
    }

    fn serve_triton_metadata_once() -> Option<(SocketAddr, std::thread::JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").ok()?;
        let address = listener.local_addr().ok()?;
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0_u8; 4096];
            let _ = stream.read(&mut buffer);
            write_http_response(&mut stream, triton_metadata_body());
        });
        Some((address, server))
    }

    #[test]
    fn active_unbounded_query_interrupts_gracefully_then_immediately() {
        let cancellation = CancellationToken::new();
        let graceful_stop = CancellationToken::new();
        let control = ActiveQueryControl {
            cancellation: cancellation.clone(),
            graceful_stop: Some(graceful_stop.clone()),
        };

        assert_eq!(
            control.interrupt(),
            QueryInterruptAction::GracefulStopRequested
        );
        assert!(graceful_stop.is_cancelled());
        assert!(!cancellation.is_cancelled());

        assert_eq!(
            control.interrupt(),
            QueryInterruptAction::ImmediateCancellationRequested
        );
        assert!(cancellation.is_cancelled());
    }

    #[test]
    fn active_bounded_query_interrupts_immediately() {
        let cancellation = CancellationToken::new();
        let control = ActiveQueryControl {
            cancellation: cancellation.clone(),
            graceful_stop: None,
        };

        assert_eq!(
            control.interrupt(),
            QueryInterruptAction::ImmediateCancellationRequested
        );
        assert!(cancellation.is_cancelled());
    }

    #[test]
    fn immediate_cancellation_stops_an_unbounded_source() {
        let cancellation = CancellationToken::new();
        let graceful_stop = CancellationToken::new();
        let control = ActiveQueryControl {
            cancellation: cancellation.clone(),
            graceful_stop: Some(graceful_stop.clone()),
        };

        control.cancel_immediately();

        assert!(graceful_stop.is_cancelled());
        assert!(cancellation.is_cancelled());
    }

    #[test]
    fn catalog_reopens_created_image_table() {
        let temp = tempdir().unwrap();
        let photos = temp.path().join("photos");
        std::fs::create_dir(&photos).unwrap();
        let catalog_path = temp.path().join("catalog.db");
        let create = format!(
            "CREATE TABLE photos USING IMAGES LOCATION '{}' OPTIONS (recursive = true);",
            photos.display()
        );

        {
            let engine = Engine::new(EngineConfig::new(&catalog_path)).unwrap();
            let session = engine.session().build().unwrap();
            session.run_script(&create).unwrap();
        }

        {
            let engine = Engine::new(EngineConfig::new(&catalog_path)).unwrap();
            let session = engine.session().build().unwrap();
            let batches = session.sql("SHOW TABLES").unwrap().collect().unwrap();
            assert_eq!(batches[0].num_rows(), 1);
            let batches = session
                .sql("SELECT COUNT(*) AS count FROM photos")
                .unwrap()
                .collect()
                .unwrap();
            assert_eq!(batches[0].num_rows(), 1);
        }
    }

    fn show_create_sql(session: &Session, statement: &str) -> String {
        let batches = session.sql(statement).unwrap().collect().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_owned()
    }

    fn explain_text(session: &Session, sql: &str) -> String {
        session
            .sql(sql)
            .unwrap()
            .collect()
            .unwrap()
            .iter()
            .flat_map(|batch| {
                let plans = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..plans.len())
                    .map(|row| plans.value(row).to_owned())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn create_rtsp_table(engine: &Engine, name: &str, endpoint: &str) {
        let definition = TableDef::new(
            name,
            TableProvider::Rtsp(RtspTableConfig {
                name: name.to_owned(),
                endpoint: endpoint.to_owned(),
                fps: 5.0,
                event_time: crate::catalog::EventTimePolicy::CaptureTime,
                watermark_delay_ms: 100,
                transport: crate::catalog::RtspTransport::Tcp,
            }),
        );
        engine
            .inner
            .catalog
            .backend()
            .create_table(
                crate::catalog::DEFAULT_CATALOG,
                crate::catalog::DEFAULT_SCHEMA,
                &definition,
                &rtsp_schema(),
            )
            .unwrap();
    }

    #[test]
    fn show_create_round_trips_all_catalog_objects_and_redacts_secrets() {
        let temp = tempdir().unwrap();
        let photos = temp.path().join("photos");
        std::fs::create_dir(&photos).unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .run_script(&format!(
                "CREATE TABLE photos USING IMAGES LOCATION '{}';
                 CREATE TABLE entrance USING RTSP OPTIONS (
                   url = 'rtsp://camera.example/live', fps = '7.5', event_time = 'ingest_time',
                   watermark = '1500 milliseconds', transport = 'udp');
                 CREATE MODEL detector TYPE OBJECT_DETECTION
                   FROM 'mock://person' USING ONNX_RUNTIME;
                 CREATE FUNCTION plus_one(BIGINT) RETURNS BIGINT RETURN $1 + 1;
                 CREATE FUNCTION py_double(value BIGINT) RETURNS BIGINT
                   LANGUAGE PYTHON AS 'ops:double';
                 CREATE TABLE events (event_id BIGINT NOT NULL) USING KAFKA OPTIONS (
                   bootstrap_servers = 'broker:9092', topic = 'events', format = 'json',
                   credential_ref = 'secret://kafka/producer',
                   delivery_timeout_ms = '45000', buffer_capacity = '256');",
                photos.display()
            ))
            .unwrap();

        let unresolved_model = show_create_sql(&session, "SHOW CREATE MODEL detector");
        session.sql("RESOLVE MODEL detector").unwrap();
        let resolved_model = show_create_sql(&session, "SHOW CREATE MODEL detector");
        assert_eq!(resolved_model, unresolved_model);
        assert!(resolved_model.contains("VERSION 'v1'"));

        let statements = [
            show_create_sql(&session, "SHOW CREATE TABLE photos"),
            show_create_sql(&session, "SHOW CREATE TABLE entrance"),
            unresolved_model,
            show_create_sql(&session, "SHOW CREATE FUNCTION plus_one"),
            show_create_sql(&session, "SHOW CREATE FUNCTION py_double"),
            show_create_sql(&session, "SHOW CREATE TABLE events"),
        ];
        let kafka = statements.last().unwrap();
        assert!(!kafka.contains("secret://kafka/producer"));
        assert!(kafka.contains("[REDACTED_SECRET_REF]"));

        let copy = Engine::new(EngineConfig::new(temp.path().join("copy.db"))).unwrap();
        let copy_session = copy.session().build().unwrap();
        for statement in &statements {
            copy_session.run_script(statement).unwrap();
        }
        let copied_events = copy
            .inner
            .catalog
            .snapshot()
            .unwrap()
            .table("events")
            .unwrap()
            .schema
            .clone();
        assert_eq!(copied_events.fields().len(), 1);
        assert_eq!(copied_events.field(0).name(), "event_id");
        assert!(!copied_events.field(0).is_nullable());
        assert!(
            copy.inner
                .catalog
                .snapshot()
                .unwrap()
                .model("detector")
                .unwrap()
                .definition
                .versions[0]
                .resolved
                .is_none()
        );
    }

    #[test]
    fn show_create_reports_the_requested_object_kind() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        let error = session.sql("SHOW CREATE FUNCTION missing").unwrap_err();
        assert_eq!(error.code, ErrorCode::NotFound);
        assert_eq!(error.message, "function 'missing' does not exist");
    }

    #[test]
    fn show_create_model_selects_latest_live_or_exact_version_independently_of_default() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .run_script(
                "CREATE MODEL quality.detector TYPE OBJECT_DETECTION VERSION 'v10'
                   FROM 'mock://person' COMMENT 'Vision''s detector';
                 RESOLVE MODEL quality.detector VERSION 'v10';
                 ALTER MODEL quality.detector ADD VERSION 'v2' FROM 'mock://candidate';",
            )
            .unwrap();
        let latest = show_create_sql(&session, "SHOW CREATE MODEL quality.detector");
        assert_eq!(
            latest,
            "CREATE MODEL \"quality.detector\" TYPE OBJECT_DETECTION VERSION 'v2' FROM 'mock://candidate' USING ONNX_RUNTIME COMMENT 'Vision''s detector'"
        );
        assert!(!latest.contains("ALTER MODEL"));
        let first = show_create_sql(&session, "SHOW CREATE MODEL quality.detector VERSION 'v10'");
        assert!(first.contains("VERSION 'v10' FROM 'mock://person'"));

        // Each returned definition can create its selected version on its own.
        for (index, sql) in [&latest, &first].into_iter().enumerate() {
            let copy = Engine::new(EngineConfig::new(
                temp.path().join(format!("copy-{index}.db")),
            ))
            .unwrap();
            let copy_session = copy.session().build().unwrap();
            copy_session.run_script(sql).unwrap();
            assert_eq!(
                show_create_sql(&copy_session, "SHOW CREATE MODEL quality.detector"),
                *sql
            );
        }

        session
            .sql("ALTER MODEL quality.detector ADD VERSION 'Release''2;Blue' FROM 'mock://blue'")
            .unwrap();
        let escaped = show_create_sql(
            &session,
            "SHOW CREATE MODEL quality.detector VERSION 'Release''2;Blue'",
        );
        assert!(escaped.contains("VERSION 'Release''2;Blue' FROM 'mock://blue'"));
        assert_eq!(
            show_create_sql(&session, "SHOW CREATE MODEL quality.detector"),
            escaped
        );
        session
            .sql("ALTER MODEL quality.detector DROP VERSION 'Release''2;Blue'")
            .unwrap();
        assert_eq!(
            show_create_sql(&session, "SHOW CREATE MODEL quality.detector"),
            latest
        );
        session
            .sql("ALTER MODEL quality.detector DROP VERSION 'v2'")
            .unwrap();
        assert_eq!(
            show_create_sql(&session, "SHOW CREATE MODEL quality.detector"),
            first
        );
        let error = session
            .sql("SHOW CREATE MODEL quality.detector VERSION 'v2'")
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::NotFound);
        assert_eq!(
            error.message,
            "model 'quality.detector' has no version 'v2'"
        );
        let error = session
            .sql("SHOW CREATE MODEL quality.detector VERSION 'V10'")
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::NotFound);
        session
            .sql("ALTER MODEL quality.detector ADD VERSION 'v2' FROM 'mock://reused'")
            .unwrap();
        assert!(
            show_create_sql(&session, "SHOW CREATE MODEL quality.detector")
                .contains("VERSION 'v2' FROM 'mock://reused'")
        );
    }

    #[test]
    fn prepared_show_create_model_pins_latest_and_explicit_versions_to_its_snapshot() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .run_script(
                "CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://person';
             ALTER MODEL detector ADD VERSION 'v2' FROM 'mock://candidate';",
            )
            .unwrap();
        let prepared = [
            "SHOW CREATE MODEL detector",
            "SHOW CREATE MODEL detector VERSION 'v2'",
        ]
        .map(|sql| {
            session
                .prepare(sql, "service", session.semantic_settings())
                .unwrap()
        });
        session
            .run_script(
                "ALTER MODEL detector ADD VERSION 'v3' FROM 'mock://third';
             ALTER MODEL detector DROP VERSION 'v2';",
            )
            .unwrap();
        for query in prepared {
            let schema = query.result_schema();
            let PreparedResult::Batches(batches) = query.execute_query().unwrap() else {
                panic!("SHOW CREATE must return catalog batches");
            };
            assert_eq!(schema.fields(), batches[0].schema().fields());
            assert_eq!(
                batches[0]
                    .column_by_name("version")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "v2"
            );
            assert!(
                batches[0]
                    .column_by_name("create_sql")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0)
                    .contains("VERSION 'v2' FROM 'mock://candidate'")
            );
        }
        assert!(show_create_sql(&session, "SHOW CREATE MODEL detector").contains("VERSION 'v3'"));
    }

    #[test]
    fn session_memory_failure_releases_every_reservation() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(
            EngineConfig::new(temp.path().join("catalog.db")).with_session_memory_limit_bytes(128),
        )
        .unwrap();
        let session = engine.session().build().unwrap();
        let sql = format!("SELECT '{}' AS value", "x".repeat(1024));
        let statement = session.sql(&sql).unwrap();

        let error = statement.collect().unwrap_err();
        assert_eq!(error.code, ErrorCode::ResourceExhausted);
    }

    #[test]
    fn cloned_session_handles_share_memory_but_new_sessions_do_not() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let first = engine.session().build().unwrap();
        let cloned = first.clone();
        let second = engine.session().build().unwrap();

        assert!(Arc::ptr_eq(&first.memory_pool, &cloned.memory_pool));
        assert!(!Arc::ptr_eq(&first.memory_pool, &second.memory_pool));
    }

    #[test]
    fn collect_enforces_session_limit_across_multiple_output_batches() {
        let temp = tempdir().unwrap();
        let photos = temp.path().join("photos");
        std::fs::create_dir(&photos).unwrap();
        let seed = photos.join("0000.png");
        RgbImage::from_pixel(1, 1, Rgb([10, 20, 30]))
            .save(&seed)
            .unwrap();
        for index in 1..=1024 {
            std::fs::hard_link(&seed, photos.join(format!("{index:04}.png"))).unwrap();
        }

        let ddl = format!(
            "CREATE TABLE photos USING IMAGES LOCATION '{}'",
            photos.display()
        );
        let sizing_engine = Engine::new(EngineConfig::new(temp.path().join("sizing.db"))).unwrap();
        let sizing_session = sizing_engine.session().build().unwrap();
        sizing_session.sql(&ddl).unwrap();
        let batches = sizing_session
            .sql("SELECT uri FROM photos")
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(batches.len(), 2);
        let total = batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .sum::<usize>();
        let largest = batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .max()
            .unwrap();
        assert!(largest < total);

        let limited_engine = Engine::new(
            EngineConfig::new(temp.path().join("limited.db"))
                .with_session_memory_limit_bytes(total - 1),
        )
        .unwrap();
        let limited_session = limited_engine.session().build().unwrap();
        limited_session.sql(&ddl).unwrap();
        let statement = limited_session.sql("SELECT uri FROM photos").unwrap();

        let error = statement.collect().unwrap_err();

        assert_eq!(error.code, ErrorCode::ResourceExhausted);
    }

    #[test]
    fn dropping_query_handle_releases_cached_arrow_results() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        let statement = session.sql("SELECT 'cached result' AS value").unwrap();

        statement.collect().unwrap();
        assert!(session.memory_pool.reserved() > 0);
        drop(statement);

        assert_eq!(session.memory_pool.reserved(), 0);
    }

    #[test]
    fn concurrent_sessions_return_independent_results() {
        let temp = tempdir().unwrap();
        let one = temp.path().join("one");
        let three = temp.path().join("three");
        std::fs::create_dir(&one).unwrap();
        std::fs::create_dir(&three).unwrap();
        RgbImage::from_pixel(1, 1, Rgb([1, 2, 3]))
            .save(one.join("one.png"))
            .unwrap();
        for index in 0..3 {
            RgbImage::from_pixel(1, 1, Rgb([1, 2, 3]))
                .save(three.join(format!("{index}.png")))
                .unwrap();
        }
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let setup = engine.session().build().unwrap();
        setup
            .run_script(&format!(
                "CREATE TABLE one USING IMAGES LOCATION '{}';
                 CREATE TABLE three USING IMAGES LOCATION '{}'",
                one.display(),
                three.display()
            ))
            .unwrap();
        let left = engine
            .session()
            .build()
            .unwrap()
            .sql("SELECT uri FROM one")
            .unwrap();
        let right = engine
            .session()
            .build()
            .unwrap()
            .sql("SELECT uri FROM three")
            .unwrap();

        std::thread::scope(|scope| {
            let left_task = scope.spawn(|| left.collect().unwrap());
            let right_task = scope.spawn(|| right.collect().unwrap());
            assert_eq!(
                left_task
                    .join()
                    .unwrap()
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>(),
                1
            );
            assert_eq!(
                right_task
                    .join()
                    .unwrap()
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>(),
                3
            );
        });
    }

    #[test]
    fn typed_models_do_not_create_function_dependencies() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .sql(
                "CREATE MODEL detector TYPE OBJECT_DETECTION \
                 FROM 'mock://person' USING ONNX_RUNTIME",
            )
            .unwrap();

        let result = session.sql("DROP MODEL detector").unwrap();

        assert!(matches!(result, Statement::Ddl(_)));
        assert!(session.sql("SELECT 1").unwrap().collect().is_ok());
    }

    #[test]
    fn create_model_is_local_and_inference_requires_resolve() {
        let temp = tempdir().unwrap();
        let photos = temp.path().join("photos");
        std::fs::create_dir(&photos).unwrap();
        RgbImage::from_pixel(8, 8, Rgb([10, 20, 30]))
            .save(photos.join("one.png"))
            .unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();

        let error = session
            .sql("CREATE MODEL ambiguous TYPE OBJECT_DETECTION FROM './weights.bin'")
            .unwrap_err();
        assert!(error.message.contains("USING"));

        session
            .run_script(&format!(
                "CREATE TABLE photos USING IMAGES LOCATION '{}';
                 CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://person' USING ONNX_RUNTIME;
                 CREATE MODEL remote TYPE OBJECT_DETECTION \
                   FROM 'triton+http://127.0.0.1:9/remote';",
                photos.display()
            ))
            .unwrap();

        let snapshot = engine.inner.catalog.snapshot().unwrap();
        assert!(
            snapshot.model("detector").unwrap().definition.versions[0]
                .resolved
                .is_none()
        );
        assert!(
            snapshot.model("remote").unwrap().definition.versions[0]
                .resolved
                .is_none()
        );
        let shown = session
            .sql("SHOW MODEL VERSIONS detector")
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(shown[0].schema().field(4).name(), "status");
        let statuses = shown[0]
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(statuses.value(0), "UNRESOLVED");
        let error = session
            .sql("SELECT detector(image) FROM photos")
            .unwrap_err();
        assert!(error.message.contains("RESOLVE MODEL detector"));

        session.sql("RESOLVE MODEL detector").unwrap();
        assert_eq!(engine.inner.models.active_resolution_job_count(), 0);
        assert!(
            engine
                .inner
                .catalog
                .snapshot()
                .unwrap()
                .model("detector")
                .unwrap()
                .definition
                .versions[0]
                .resolved
                .is_some()
        );
        let shown = session
            .sql("SHOW MODEL VERSIONS detector")
            .unwrap()
            .collect()
            .unwrap();
        let statuses = shown[0]
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(statuses.value(0), "RESOLVED");
        let explain = explain_text(&session, "EXPLAIN SELECT detector(image) FROM photos");
        assert!(explain.contains("VisionQLPlan mode=bounded"));
        assert!(explain.contains("Inference model=detector"));
        assert!(explain.contains("batching_owner=engine"));
        assert!(explain.contains("dedup=enabled"));
        assert!(explain.contains("decode=skipped(mock)"));
        assert!(explain.contains("image_payload=locator_or_encoded"));
    }

    #[test]
    fn model_versions_require_explicit_publication_and_support_call_site_selection() {
        let temp = tempdir().unwrap();
        let photos = temp.path().join("photos");
        std::fs::create_dir(&photos).unwrap();
        RgbImage::from_pixel(8, 8, Rgb([10, 20, 30]))
            .save(photos.join("one.png"))
            .unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        let error = session
            .sql(
                "CREATE MODEL invalid_version TYPE OBJECT_DETECTION VERSION 'default'
                 FROM 'mock://invalid'",
            )
            .unwrap_err();
        assert!(error.message.contains("reserved"));
        session
            .run_script(&format!(
                "CREATE TABLE photos USING IMAGES LOCATION '{}';
                 CREATE MODEL detector TYPE OBJECT_DETECTION VERSION 'alpha'
                   FROM 'mock://alpha';
                 ALTER MODEL detector ADD VERSION 'beta' FROM 'mock://beta';",
                photos.display()
            ))
            .unwrap();
        let error = session
            .sql("CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://duplicate'")
            .unwrap_err();
        assert!(error.message.contains("ALTER MODEL detector ADD VERSION"));

        let error = session
            .sql("ALTER MODEL detector SET DEFAULT_VERSION = 'beta'")
            .unwrap_err();
        assert!(error.message.contains("unresolved"));
        session
            .sql("RESOLVE MODEL detector VERSION 'beta'")
            .unwrap();

        let definition = engine
            .inner
            .catalog
            .snapshot()
            .unwrap()
            .model("detector")
            .unwrap()
            .definition
            .clone();
        assert_eq!(definition.default_version, None);
        let error = session.sql("RESOLVE MODEL detector").unwrap_err();
        assert!(error.message.contains("alpha, beta"));
        session
            .sql("RESOLVE MODEL detector VERSION 'alpha'")
            .unwrap();
        assert_eq!(
            engine
                .inner
                .catalog
                .snapshot()
                .unwrap()
                .model("detector")
                .unwrap()
                .definition
                .default_version
                .as_deref(),
            Some("alpha")
        );

        let explain = explain_text(
            &session,
            "EXPLAIN SELECT detector(image), detector(image, version => 'beta') FROM photos",
        );
        assert!(explain.contains("detector@alpha"));
        assert!(explain.contains("detector@beta"));
        let error = session
            .sql("SELECT detector(image, version => 'missing') FROM photos")
            .unwrap_err();
        assert!(error.message.contains("known versions"));
        let error = session
            .sql("SELECT detector(image, version => uri) FROM photos")
            .unwrap_err();
        assert!(error.message.contains("version"));
        assert!(error.message.contains("constant"));
        let ab = session
            .sql(
                "SELECT detector(image) AS alpha_one,
                        detector(image) AS alpha_two,
                        detector(image, version => 'beta') AS beta_one,
                        detector(image, version => 'beta') AS beta_two
                 FROM photos",
            )
            .unwrap();
        let Statement::Query(query) = &ab else {
            panic!("model SELECT must produce a query");
        };
        assert_eq!(
            query
                .dataframe
                .logical_plan()
                .display_indent()
                .to_string()
                .matches("InferenceNode")
                .count(),
            2
        );
        let error = session
            .sql("ALTER MODEL detector DROP VERSION 'alpha'")
            .unwrap_err();
        assert!(error.message.contains("move default first"));
        let pinned = session
            .sql("SELECT detector(image) AS detections FROM photos")
            .unwrap();
        session
            .run_script(
                "ALTER MODEL detector SET DEFAULT_VERSION = 'beta';
                 ALTER MODEL detector DROP VERSION 'alpha';
                 ALTER MODEL detector ADD VERSION 'alpha' FROM 'mock://alpha2';",
            )
            .unwrap();
        let Statement::Query(query) = &pinned else {
            panic!("model SELECT must produce a query");
        };
        assert!(
            query
                .dataframe
                .logical_plan()
                .display_indent()
                .to_string()
                .contains("detector@alpha")
        );
        pinned.collect().unwrap();
        let versions = session
            .sql("SHOW MODEL VERSIONS detector")
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(versions[0].num_rows(), 2);

        session
            .run_script(
                "CREATE MODEL unpublished TYPE OBJECT_DETECTION VERSION 'origin'
                   FROM 'mock://origin';
                 ALTER MODEL unpublished ADD VERSION 'candidate' FROM 'mock://candidate';
                 RESOLVE MODEL unpublished VERSION 'candidate';
                 ALTER MODEL unpublished DROP VERSION 'origin';
                 ALTER MODEL unpublished ADD VERSION 'origin' FROM 'mock://replacement';
                 RESOLVE MODEL unpublished VERSION 'origin';",
            )
            .unwrap();
        assert_eq!(
            engine
                .inner
                .catalog
                .snapshot()
                .unwrap()
                .model("unpublished")
                .unwrap()
                .definition
                .default_version,
            None
        );
    }

    #[test]
    fn generic_mock_model_preserves_types_and_null_row_alignment() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .run_script(
                "CREATE MODEL identity(value DOUBLE) RETURNS DOUBLE FROM 'mock://identity';
                 RESOLVE MODEL identity;",
            )
            .unwrap();

        let batches = session
            .sql(
                "SELECT identity(value) AS result
                 FROM (VALUES (CAST(1.5 AS DOUBLE)), (CAST(NULL AS DOUBLE))) AS t(value)",
            )
            .unwrap()
            .collect()
            .unwrap();
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(values.value(0), 1.5);
        assert!(values.is_null(1));

        let stable = session
            .sql("SELECT identity(1.5) AS first, identity(1.5) AS second")
            .unwrap();
        let Statement::Query(query) = stable else {
            panic!("generic Model SELECT must produce a query")
        };
        assert_eq!(
            query
                .dataframe
                .logical_plan()
                .display_indent()
                .to_string()
                .matches("InferenceNode")
                .count(),
            1
        );
        let volatile = session
            .sql("SELECT identity(random()) AS first, identity(random()) AS second")
            .unwrap();
        let Statement::Query(query) = volatile else {
            panic!("generic Model SELECT must produce a query")
        };
        assert_eq!(
            query
                .dataframe
                .logical_plan()
                .display_indent()
                .to_string()
                .matches("InferenceNode")
                .count(),
            2
        );
    }

    #[test]
    fn generic_onnx_model_executes_multiple_inputs_and_restores_null_rows() {
        const ADD_MODEL: &str = "CAo6SwoOCgF4CgF5EgF6IgNBZGQSA2FkZFoQCgF4EgsKCQgLEgUKAxIBTloQCgF5EgsKCQgLEgUKAxIBTmIQCgF6EgsKCQgLEgUKAxIBTkIECgAQEg==";
        let temp = tempdir().unwrap();
        let model = temp.path().join("add.onnx");
        std::fs::write(
            &model,
            base64::engine::general_purpose::STANDARD
                .decode(ADD_MODEL)
                .unwrap(),
        )
        .unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .run_script(&format!(
                "CREATE MODEL add_values(left DOUBLE, right DOUBLE) RETURNS DOUBLE
                   FROM '{}';
                 RESOLVE MODEL add_values;",
                model.display()
            ))
            .unwrap();

        let batches = session
            .sql(
                "SELECT add_values(left_value, right_value) AS result
                 FROM (VALUES
                   (CAST(1.5 AS DOUBLE), CAST(2.5 AS DOUBLE)),
                   (CAST(NULL AS DOUBLE), CAST(2.0 AS DOUBLE)),
                   (CAST(4.0 AS DOUBLE), CAST(NULL AS DOUBLE))
                 ) AS values(left_value, right_value)",
            )
            .unwrap()
            .collect()
            .unwrap();
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(values.value(0), 4.0);
        assert!(values.is_null(1));
        assert!(values.is_null(2));
    }

    #[test]
    fn dropping_and_recreating_a_model_builds_a_fresh_pipeline() {
        let temp = tempdir().unwrap();
        let photos = temp.path().join("photos");
        std::fs::create_dir(&photos).unwrap();
        RgbImage::from_pixel(8, 8, Rgb([10, 20, 30]))
            .save(photos.join("one.png"))
            .unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .run_script(&format!(
                "CREATE TABLE photos USING IMAGES LOCATION '{}';
                 CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://person' USING ONNX_RUNTIME;
                 RESOLVE MODEL detector;",
                photos.display()
            ))
            .unwrap();
        session
            .sql("SELECT detector(image) FROM photos")
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(engine.inner.models.cached_pipeline_count(), 1);

        session.sql("DROP MODEL detector").unwrap();
        assert_eq!(engine.inner.models.cached_pipeline_count(), 0);

        session
            .run_script(
                "CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://person' USING ONNX_RUNTIME;
                 RESOLVE MODEL detector;",
            )
            .unwrap();
        session
            .sql("SELECT detector(image) FROM photos")
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(engine.inner.models.cached_pipeline_count(), 1);
    }

    #[test]
    fn structured_model_config_reopens_from_catalog() {
        let temp = tempdir().unwrap();
        let catalog = temp.path().join("catalog.db");
        {
            let engine = Engine::new(EngineConfig::new(&catalog)).unwrap();
            let session = engine.session().build().unwrap();
            session
                .sql(
                    "CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://person' \
                     USING ONNX_RUNTIME \
                     OPTIONS (input_name='pixels', image_size=[320, 192], \
                              output_name='detections', format='yolo_e2e', labels=['person'])",
                )
                .unwrap();
        }

        let engine = Engine::new(EngineConfig::new(&catalog)).unwrap();
        let snapshot = engine.inner.catalog.snapshot().unwrap();
        let model = &snapshot.model("detector").unwrap().definition;

        let version = &model.versions[0];
        assert_eq!(version.runtime_kind, "onnx-runtime");
        assert_eq!(version.options["image_size"], serde_json::json!([320, 192]));
        assert_eq!(version.options["labels"], serde_json::json!(["person"]));
        assert!(version.resolved.is_none());
    }

    #[test]
    fn model_backed_create_function_syntax_is_not_accepted() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();

        let error = session
            .sql("CREATE FUNCTION detect USING MODEL detector")
            .unwrap_err();

        assert_eq!(error.code, ErrorCode::InvalidSql);
        let functions = session.sql("SHOW FUNCTIONS").unwrap().collect().unwrap();
        assert_eq!(functions[0].num_rows(), 0);
    }

    #[test]
    fn vql_function_names_are_reserved() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();

        for name in ["VQL_CLASSIFY", "vql_custom"] {
            let error = session
                .sql(&format!(
                    "CREATE FUNCTION {name}(BIGINT) RETURNS BIGINT RETURN $1"
                ))
                .unwrap_err();

            assert_eq!(error.code, ErrorCode::NameConflict);
            assert_eq!(
                error.message,
                format!(
                    "function name '{}' uses the reserved VQL_* prefix",
                    name.to_ascii_lowercase()
                )
            );
        }
        let error = session
            .sql("CREATE FUNCTION __vql_detect(BIGINT) RETURNS BIGINT RETURN $1")
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::NameConflict);
        assert!(error.message.contains("reserved internal prefix"));
        let error = session
            .sql("CREATE MODEL VQL_CUSTOM TYPE OBJECT_DETECTION FROM 'mock://person'")
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::NameConflict);
        assert!(error.message.contains("VQL_*"));

        let error = session
            .sql("CREATE MODEL __VQL_DETECT TYPE OBJECT_DETECTION FROM 'mock://person'")
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::NameConflict);

        let functions = session.sql("SHOW FUNCTIONS").unwrap().collect().unwrap();
        assert_eq!(functions[0].num_rows(), 0);
    }

    #[test]
    fn duplicate_function_declarations_preserve_the_existing_definition() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .sql("CREATE FUNCTION plus_one(BIGINT) RETURNS BIGINT RETURN $1 + 1")
            .unwrap();

        for sql in [
            "CREATE FUNCTION plus_one(BIGINT) RETURNS BIGINT RETURN $1 + 1",
            "CREATE FUNCTION PLUS_ONE(value BIGINT) RETURNS BIGINT RETURN value + 2",
            "CREATE FUNCTION plus_one(BIGINT, BIGINT) RETURNS BIGINT RETURN $1 + $2",
            "CREATE FUNCTION \"plus_one\"(BIGINT) RETURNS BIGINT RETURN $1 + 2",
            "CREATE FUNCTION plus_one(BIGINT) RETURNS BIGINT LANGUAGE PYTHON AS 'ops:increment'",
        ] {
            let error = session.sql(sql).unwrap_err();
            assert_eq!(error.code, ErrorCode::AlreadyExists, "{sql}: {error}");
            assert_eq!(
                error.message,
                "callable name 'plus_one' conflicts with existing function"
            );
        }

        let batches = session
            .sql("SELECT plus_one(41) AS answer")
            .unwrap()
            .collect()
            .unwrap();
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 42);
    }

    #[test]
    fn function_declarations_conflicting_with_models_report_name_conflict() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .sql("CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://person'")
            .unwrap();

        let error = session
            .sql("CREATE FUNCTION detector(IMAGE) RETURNS BIGINT RETURN 1")
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::NameConflict);
        assert_eq!(
            error.message,
            "callable name 'detector' conflicts with existing model"
        );
    }

    #[test]
    fn sql_macro_names_inside_literals_are_not_expanded() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .sql("CREATE FUNCTION plus_one(BIGINT) RETURNS BIGINT RETURN $1 + 1")
            .unwrap();

        let batches = session
            .sql("SELECT 'plus_one(1)' AS literal")
            .unwrap()
            .collect()
            .unwrap();
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "plus_one(1)");

        let batches = session
            .sql("SELECT plus_one(41) AS answer")
            .unwrap()
            .collect()
            .unwrap();
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 42);
    }

    #[test]
    fn python_integer_types_reopen_from_catalog() {
        let temp = tempdir().unwrap();
        let catalog = temp.path().join("catalog.db");
        {
            let engine = Engine::new(EngineConfig::new(&catalog)).unwrap();
            let session = engine.session().build().unwrap();
            session
                .sql("CREATE FUNCTION py_int(value INT) RETURNS INT LANGUAGE PYTHON AS 'm:f'")
                .unwrap();
            session.sql("SELECT 1").unwrap().collect().unwrap();
        }

        let engine = Engine::new(EngineConfig::new(&catalog)).unwrap();
        let session = engine.session().build().unwrap();
        session.sql("SELECT 1").unwrap().collect().unwrap();
    }

    #[test]
    fn sql_expression_function_can_call_existing_function() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .sql(
                "CREATE FUNCTION fahrenheit(DOUBLE) RETURNS DOUBLE \
                 RETURN $1 * 9.0 / 5.0 + 32.0",
            )
            .unwrap();
        session
            .sql(
                "CREATE FUNCTION hotter(DOUBLE) RETURNS DOUBLE \
                 RETURN fahrenheit($1) + 1.0",
            )
            .unwrap();

        let batches = session
            .sql("SELECT hotter(0.0) AS temperature")
            .unwrap()
            .collect()
            .unwrap();
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(values.value(0), 33.0);
    }

    #[test]
    fn sql_expression_function_can_call_tumble() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();

        session
            .sql(
                "CREATE FUNCTION minute_bucket(TIMESTAMP) RETURNS TIMESTAMP \
                 RETURN TUMBLE($1, INTERVAL '1' MINUTE)",
            )
            .unwrap();
    }

    #[test]
    fn sql_expression_function_can_wrap_typed_inference() {
        let temp = tempdir().unwrap();
        let photos = temp.path().join("photos");
        std::fs::create_dir(&photos).unwrap();
        RgbImage::from_pixel(8, 8, Rgb([10, 20, 30]))
            .save(photos.join("one.png"))
            .unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .run_script(&format!(
                "CREATE TABLE photos USING IMAGES LOCATION '{}';
                 CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://person' USING ONNX_RUNTIME;
                 RESOLVE MODEL detector;
                 CREATE FUNCTION detect_people(IMAGE)
                 RETURN detector($1,
                   classes => ['person'], min_confidence => 0.5
                 );",
                photos.display()
            ))
            .unwrap();

        let batches = session
            .sql("SELECT CARDINALITY(detect_people(image)) AS people FROM photos")
            .unwrap()
            .collect()
            .unwrap();
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();

        assert_eq!(values.value(0), 1);
    }

    #[test]
    fn function_constant_parameters_are_inferred_and_propagated() {
        let temp = tempdir().unwrap();
        let photos = temp.path().join("photos");
        std::fs::create_dir(&photos).unwrap();
        RgbImage::from_pixel(8, 8, Rgb([10, 20, 30]))
            .save(photos.join("one.png"))
            .unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .run_script(&format!(
                "CREATE TABLE photos USING IMAGES LOCATION '{}';
                 CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://person';
                 RESOLVE MODEL detector;
                 CREATE FUNCTION detect_at(image IMAGE, threshold DOUBLE)
                   RETURN detector(image, min_confidence => threshold);
                 CREATE FUNCTION nested_detect(image IMAGE, threshold DOUBLE)
                   RETURN detect_at(image, threshold);",
                photos.display()
            ))
            .unwrap();

        for name in ["detect_at", "nested_detect"] {
            let described = session
                .sql(&format!("DESCRIBE FUNCTION {name}"))
                .unwrap()
                .collect()
                .unwrap();
            let arguments = described[0]
                .column(4)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert!(arguments.value(0).contains("threshold CONST DOUBLE"));
            let error = session
                .sql(&format!("SELECT {name}(image, width) FROM photos"))
                .unwrap_err();
            assert!(
                error
                    .message
                    .contains("argument 2 ('threshold') must be constant")
            );
            assert!(
                session
                    .sql(&format!("SELECT {name}(image, 0.5) FROM photos"))
                    .unwrap()
                    .collect()
                    .is_ok()
            );
        }
    }

    #[test]
    fn builtin_ai_overloads_and_nulls_follow_the_task_contract() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        let error = session
            .sql("CREATE MODEL classifier TYPE IMAGE_CLASSIFICATION FROM 'mock://classifier'")
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::FeatureNotAvailable);
        assert_eq!(error.target_version.as_deref(), Some("未排期"));

        for sql in [
            "SELECT VQL_CLASSIFY('image.jpg', ['cat'])",
            "SELECT VQL_EXTRACT('document.pdf', MAP {'title': 'What is the title?'})",
        ] {
            let error = session.sql(sql).unwrap_err();
            assert_eq!(error.code, ErrorCode::FeatureNotAvailable, "{sql}");
            assert_eq!(error.target_version.as_deref(), Some("未排期"), "{sql}");
        }

        let migration = session
            .sql("SELECT VQL_EXTRACT('document.pdf')")
            .unwrap_err();
        assert_eq!(migration.code, ErrorCode::InvalidArgument);
        assert!(migration.message.contains("VQL_DETECT"));

        for sql in [
            "SELECT VQL_CLASSIFY(X'CAFE', ['cat'])",
            "SELECT VQL_EXTRACT(X'CAFE', MAP {'title': 'What is the title?'})",
            "SELECT VQL_DETECT('image.jpg')",
        ] {
            let error = session.sql(sql).unwrap_err();
            assert_eq!(error.code, ErrorCode::InvalidSql, "{sql}");
        }

        let error = session
            .sql("SELECT VQL_EXTRACT(42, MAP {'title': 'What is the title?'})")
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidSql);

        for sql in [
            "SELECT VQL_CLASSIFY(NULL, ['cat'])",
            "SELECT VQL_DETECT(NULL)",
            "SELECT VQL_EXTRACT(NULL, MAP {'title': 'What is the title?'})",
        ] {
            let batches = session.sql(sql).unwrap().collect().unwrap();
            assert!(batches[0].column(0).is_null(0), "{sql}");
        }
        let batches = session
            .sql(
                "WITH extracted AS (\
                   SELECT VQL_EXTRACT(NULL, MAP {\
                     'total_amount': 'What is the total?',\
                     'items': STRUCT('List the items' AS question, TRUE AS list)\
                   }) AS r\
                 )\
                 SELECT r.total_amount.value FROM extracted",
            )
            .unwrap()
            .collect()
            .unwrap();
        assert!(batches[0].column(0).is_null(0));
        assert!(
            session
                .sql("SELECT 'VQL_CLASSIFY(NULL, [''cat''])'")
                .unwrap()
                .collect()
                .is_ok()
        );

        let error = session
            .sql("SET vql.classify.model = 'classifier'")
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidOption);
        assert!(error.message.contains("unknown session setting"));
    }

    #[test]
    fn resolve_rejects_a_replaced_declaration() {
        let error = ensure_resolution_declaration_unchanged(
            "detector",
            "v1",
            "original-declaration",
            "replacement-declaration",
        )
        .unwrap_err();

        assert_eq!(error.code, ErrorCode::Catalog);
        assert!(
            error
                .message
                .contains("changed while RESOLVE MODEL was running")
        );
        assert!(error.message.contains("retry the statement"));
    }

    #[test]
    fn correlated_unnest_preserves_the_left_relation_alias() {
        let temp = tempdir().unwrap();
        let photos = temp.path().join("photos");
        std::fs::create_dir(&photos).unwrap();
        RgbImage::from_pixel(8, 8, Rgb([10, 20, 30]))
            .save(photos.join("one.png"))
            .unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .run_script(&format!(
                "CREATE TABLE photos USING IMAGES LOCATION '{}';
                 CREATE MODEL detector TYPE OBJECT_DETECTION
                 FROM 'mock://person' USING ONNX_RUNTIME;
                 RESOLVE MODEL detector;",
                photos.display()
            ))
            .unwrap();

        let batches = session
            .sql(
                "SELECT f.uri, det.label
                 FROM photos AS f, UNNEST(detector(f.image)) AS u(det)",
            )
            .unwrap()
            .collect()
            .unwrap();

        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let labels = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(labels.value(0), "person");
    }

    #[test]
    fn invalid_video_location_does_not_write_catalog() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        let error = session
            .sql("CREATE TABLE clips USING VIDEOS LOCATION './clips'")
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidLocation);
        assert_eq!(
            engine
                .inner
                .catalog
                .history_count(crate::catalog::ObjectKind::Table)
                .unwrap(),
            0
        );
    }

    #[test]
    fn run_script_stops_before_later_ddl_on_execution_error() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        let script = format!(
            "SELECT CAST('not-a-number' AS INT); \
             CREATE TABLE photos USING IMAGES LOCATION '{}'",
            temp.path().display()
        );
        assert!(session.run_script(&script).is_err());
        assert_eq!(
            engine
                .inner
                .catalog
                .history_count(crate::catalog::ObjectKind::Table)
                .unwrap(),
            0
        );
    }

    #[test]
    fn videos_sample_by_pts_without_decoding_pixels() {
        if !crate::test_util::ffmpeg_available() {
            return;
        }
        let temp = tempdir().unwrap();
        let videos = temp.path().join("videos");
        std::fs::create_dir(&videos).unwrap();
        assert!(crate::test_util::generate_test_video(
            &videos.join("test.mp4")
        ));
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .sql(&format!(
                "CREATE TABLE clips USING VIDEOS LOCATION '{}' OPTIONS (fps = 5)",
                videos.display()
            ))
            .unwrap();

        let batches = session
            .sql("SELECT pts_ms, width, height FROM clips ORDER BY pts_ms")
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 20);
        let pts = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(pts.value(0), 0);
        assert!((pts.value(1) - 200).abs() <= 2);
        assert_eq!(
            engine.inner.media.counters().decoded_frames,
            0,
            "metadata query must not decode pixels"
        );

        let batches = session
            .sql("SELECT TUMBLE(ts, INTERVAL '1' MINUTE) AS minute, COUNT(*) FROM clips GROUP BY 1")
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn model_calls_are_optimizer_visible_and_deduplicated() {
        let temp = tempdir().unwrap();
        let photos = temp.path().join("photos");
        std::fs::create_dir(&photos).unwrap();
        RgbImage::from_pixel(32, 24, Rgb([10, 20, 30]))
            .save(photos.join("one.png"))
            .unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .run_script(&format!(
                "CREATE TABLE photos USING IMAGES LOCATION '{}';
                 CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://person' USING ONNX_RUNTIME;
                 RESOLVE MODEL detector;",
                photos.display()
            ))
            .unwrap();
        let statement = session
            .sql("SELECT CARDINALITY(detector(image, classes => ['person'], min_confidence => 0.6)) AS people FROM photos")
            .unwrap();
        let Statement::Query(query) = &statement else {
            panic!("model SELECT must produce a query");
        };
        let logical = query.dataframe.logical_plan().display_indent().to_string();
        assert!(logical.contains("InferenceNode"));
        assert!(!logical.contains("image_detection("));
        let physical = engine
            .inner
            .runtime
            .block_on(query.dataframe.create_physical_plan())
            .unwrap();
        let physical = datafusion::physical_plan::displayable(physical.as_ref())
            .indent(true)
            .to_string();
        assert!(physical.contains("InferenceExec"));
        statement.collect().unwrap();

        session
            .sql(
                "CREATE FUNCTION detect_people(input IMAGE)
                 RETURN detector(input, classes => ['person'], min_confidence => 0.6)",
            )
            .unwrap();
        let preset_and_direct = session
            .sql(
                "SELECT detector(image, classes => ['person'], min_confidence => 0.6) AS direct,
                        detect_people(image) AS preset
                 FROM photos",
            )
            .unwrap();
        let Statement::Query(query) = &preset_and_direct else {
            panic!("model SELECT must produce a query");
        };
        assert_eq!(
            query
                .dataframe
                .logical_plan()
                .display_indent()
                .to_string()
                .matches("InferenceNode")
                .count(),
            1
        );

        let deduplicated = session
            .sql(
                "SELECT CARDINALITY(detector(image, classes => ['person'], min_confidence => 0.6)) AS first, \
                        CARDINALITY(detector(image, classes => ['person'], min_confidence => 0.6)) AS second FROM photos",
            )
            .unwrap();
        let Statement::Query(query) = &deduplicated else {
            panic!("model SELECT must produce a query");
        };
        assert_eq!(
            query
                .dataframe
                .logical_plan()
                .display_indent()
                .to_string()
                .matches("InferenceNode")
                .count(),
            1
        );
        deduplicated.collect().unwrap();

        let Some((address, server)) = serve_triton_metadata_once() else {
            return;
        };
        session
            .run_script(&format!(
                "CREATE MODEL remote TYPE OBJECT_DETECTION \
                 FROM 'triton+http://{address}/remote';
                 RESOLVE MODEL remote;"
            ))
            .unwrap();
        server.join().unwrap();
        let volatile = session
            .sql(
                "SELECT remote(image) AS first, \
                        remote(image) AS second FROM photos",
            )
            .unwrap();
        let Statement::Query(query) = volatile else {
            panic!("model SELECT must produce a query");
        };
        assert_eq!(
            query
                .dataframe
                .logical_plan()
                .display_indent()
                .to_string()
                .matches("InferenceNode")
                .count(),
            2
        );
    }

    #[test]
    fn query_cancellation_reaches_model_scheduler() {
        use std::time::{Duration, Instant};

        let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
            return;
        };
        let address = listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0_u8; 8192];
            let _ = stream.read(&mut buffer);
            write_http_response(&mut stream, triton_metadata_body());

            let (mut stream, _) = listener.accept().unwrap();
            let _ = stream.read(&mut buffer);
            accepted_tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(500));
            drop(stream);
        });
        let temp = tempdir().unwrap();
        let photos = temp.path().join("photos");
        std::fs::create_dir(&photos).unwrap();
        RgbImage::from_pixel(16, 16, Rgb([1, 2, 3]))
            .save(photos.join("one.png"))
            .unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .run_script(&format!(
                "CREATE TABLE photos USING IMAGES LOCATION '{}';
                 CREATE MODEL remote TYPE OBJECT_DETECTION
                 FROM 'triton+http://{address}/remote';
                 RESOLVE MODEL remote;",
                photos.display()
            ))
            .unwrap();
        let Statement::Query(query) = session.sql("SELECT remote(image) FROM photos").unwrap()
        else {
            panic!("model SELECT must produce a query");
        };
        let runner = query.clone();
        let execution = std::thread::spawn(move || runner.collect());
        accepted_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let started = Instant::now();

        query.cancel();
        let error = execution.join().unwrap().unwrap_err();

        assert_eq!(error.code, ErrorCode::QueryCancelled);
        assert!(started.elapsed() < Duration::from_millis(400));
        server.join().unwrap();
    }

    #[test]
    fn streaming_kafka_table_runs_after_the_epoch_plan() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .run_script(
                "CREATE TABLE camera USING RTSP OPTIONS (\
                 url = 'rtsp://127.0.0.1/live', event_time = 'ingest_time', \
                 watermark = '0 seconds');\
                 CREATE TABLE events USING KAFKA OPTIONS (\
                 bootstrap_servers = '127.0.0.1:9092', topic = 'events');",
            )
            .unwrap();

        let Statement::Query(insert) = session
            .sql(
                "INSERT INTO events \
                 SELECT TUMBLE(ts, INTERVAL '1' SECOND) AS window_start, COUNT(*) AS frames \
                 FROM camera GROUP BY TUMBLE(ts, INTERVAL '1' SECOND)",
            )
            .unwrap()
        else {
            panic!("INSERT INTO Kafka table must produce a foreground query");
        };

        let streaming = insert.streaming.as_ref().expect("streaming query");
        assert!(streaming.tumble.is_some());
        assert!(streaming.sink.is_some());
        assert!(insert.sink_target.is_some());
        assert!(
            !insert
                .dataframe
                .logical_plan()
                .display_indent()
                .to_string()
                .contains("SinkWrite")
        );
    }

    #[test]
    fn streaming_limit_and_offset_are_applied_before_sink_write() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![10, 20, 30]))])
                .unwrap();
        let (sink, written_rows) = SinkTarget::recording();
        let mut skip = 1;
        let mut fetch = Some(1);

        let output = engine
            .inner
            .runtime
            .block_on(prepare_stream_output(
                batch,
                &mut skip,
                &mut fetch,
                Some(&sink),
                |batch| {
                    Ok(crate::functions::MaterializedBatch {
                        batch,
                        reservations: Vec::new(),
                    })
                },
            ))
            .unwrap()
            .unwrap();

        assert_eq!(output.batch.num_rows(), 1);
        assert_eq!(
            output
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            20
        );
        assert_eq!(written_rows.load(Ordering::Relaxed), 1);
        assert_eq!(skip, 0);
        assert_eq!(fetch, Some(0));
    }

    #[test]
    fn kafka_table_reopens_from_catalog_without_connecting() {
        let temp = tempdir().unwrap();
        let catalog = temp.path().join("catalog.db");
        {
            let engine = Engine::new(EngineConfig::new(&catalog)).unwrap();
            let session = engine.session().build().unwrap();
            session
                .sql(
                    "CREATE TABLE events USING KAFKA OPTIONS (\
                     bootstrap_servers = 'broker-1:9092,broker-2:9092', \
                     topic = 'events', delivery_timeout_ms = 45000, buffer_capacity = 256)",
                )
                .unwrap();
        }

        let engine = Engine::new(EngineConfig::new(catalog)).unwrap();
        let session = engine.session().build().unwrap();
        let Statement::Query(insert) = session
            .sql("INSERT INTO events SELECT 1 AS event_id")
            .unwrap()
        else {
            panic!("reopened Kafka table must produce a query");
        };

        assert!(
            insert
                .dataframe
                .logical_plan()
                .display_indent()
                .to_string()
                .contains("SinkWrite: name=events, type=kafka")
        );
    }

    #[test]
    fn insert_into_declared_kafka_table_matches_columns_by_position() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .sql(
                "CREATE TABLE events (people BIGINT) USING KAFKA OPTIONS (\
                 bootstrap_servers = '127.0.0.1:9092', topic = 'events')",
            )
            .unwrap();

        let Statement::Query(insert) = session
            .sql("INSERT INTO events SELECT CAST(1 AS BIGINT)")
            .unwrap()
        else {
            panic!("INSERT INTO Kafka table must produce a query");
        };
        let target = insert.sink_target.as_ref().unwrap();
        assert_eq!(target.write_schema().field(0).name(), "people");
        assert_eq!(target.write_schema().field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn kafka_credential_reference_requires_a_host_provider() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .sql(
                "CREATE TABLE events USING KAFKA OPTIONS (\
                 bootstrap_servers = '127.0.0.1:9092', topic = 'events', \
                 credential_ref = 'secret://kafka/producer')",
            )
            .unwrap();
        let explain = explain_text(&session, "EXPLAIN INSERT INTO events SELECT 1 AS event_id");
        assert!(explain.contains("VisionQLWrite name=events provider=KAFKA"));
        let insert = session
            .sql("INSERT INTO events SELECT 1 AS event_id")
            .unwrap();

        let error = insert.collect().unwrap_err();

        assert_eq!(error.code, ErrorCode::Execution);
        assert!(error.message.contains("requires a SecretProvider"));
    }

    #[test]
    fn kafka_credential_reference_is_resolved_by_the_host() {
        #[derive(Default)]
        struct RecordingProvider(Mutex<Vec<String>>);

        impl crate::SecretProvider for RecordingProvider {
            fn resolve_kafka_authentication(
                &self,
                reference: &str,
            ) -> Result<crate::KafkaAuthentication> {
                self.0.lock().unwrap().push(reference.to_owned());
                Err(VqlError::new(ErrorCode::NotFound, "test secret is absent"))
            }
        }

        let temp = tempdir().unwrap();
        let provider = Arc::new(RecordingProvider::default());
        let config = EngineConfig::new(temp.path().join("catalog.db"))
            .with_secret_provider(provider.clone());
        let engine = Engine::new(config).unwrap();
        let session = engine.session().build().unwrap();
        session
            .sql(
                "CREATE TABLE events USING KAFKA OPTIONS (\
                 bootstrap_servers = '127.0.0.1:9092', topic = 'events', \
                 credential_ref = 'secret://kafka/producer')",
            )
            .unwrap();
        let insert = session
            .sql("INSERT INTO events SELECT 1 AS event_id")
            .unwrap();

        let error = insert.collect().unwrap_err();

        assert_eq!(error.code, ErrorCode::Execution);
        assert!(error.message.contains("failed to resolve credential_ref"));
        assert_eq!(
            provider.0.lock().unwrap().as_slice(),
            ["secret://kafka/producer"]
        );
    }

    #[test]
    fn python_function_without_a_host_reports_python_host_required() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .sql("CREATE FUNCTION py_double(x BIGINT) RETURNS BIGINT LANGUAGE PYTHON AS 'ops:double'")
            .unwrap();

        let error = session.sql("SELECT py_double(1)").unwrap_err();

        assert_eq!(error.code, ErrorCode::PythonHostRequired);
        assert!(error.message.contains("py_double"));
    }

    #[test]
    fn service_mode_rejects_python_functions_before_execution() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        engine
            .session()
            .build()
            .unwrap()
            .sql("CREATE FUNCTION py_double(x BIGINT) RETURNS BIGINT LANGUAGE PYTHON AS 'ops:double'")
            .unwrap();
        let session = engine.session().for_service().build().unwrap();

        let error = session
            .prepare(
                "SELECT py_double(1)",
                "service",
                session.semantic_settings(),
            )
            .unwrap_err();

        assert_eq!(error.code, ErrorCode::FeatureNotAvailable);
        assert_eq!(error.target_version.as_deref(), Some("未排期"));
    }

    #[test]
    fn service_mode_python_detection_uses_the_planned_function_call() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        engine
            .session()
            .build()
            .unwrap()
            .sql("CREATE FUNCTION py_double(x BIGINT) RETURNS BIGINT LANGUAGE PYTHON AS 'ops:double'")
            .unwrap();
        let session = engine.session().for_service().build().unwrap();

        session
            .prepare(
                "SELECT 'py_double(' AS text",
                "service",
                session.semantic_settings(),
            )
            .unwrap();
        let error = session
            .prepare(
                "SELECT py_double (1)",
                "service",
                session.semantic_settings(),
            )
            .unwrap_err();

        assert_eq!(error.code, ErrorCode::FeatureNotAvailable);
        assert!(error.message.contains("py_double"));
    }

    #[test]
    fn inference_row_failure_is_null_by_default_and_fails_in_strict_mode() {
        let temp = tempdir().unwrap();
        let photos = temp.path().join("photos");
        std::fs::create_dir(&photos).unwrap();
        // Keep enough of the PNG header for dimension probing to succeed, then truncate the
        // payload so this fixture exercises the decode failure asserted below.
        const TRUNCATED_PNG: &[u8] = &[
            0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, // signature
            0x00, 0x00, 0x00, 0x0d, b'I', b'H', b'D', b'R', // IHDR length and type
            0x00, 0x00, 0x00, 0x01, // width
            0x00, 0x00, 0x00, 0x01, // height
            0x08, 0x02, 0x00, 0x00, 0x00, // 8-bit RGB, default compression/filter
            0x90, 0x77, 0x53, 0xde, // IHDR CRC
            0x00, 0x00, 0x00, 0x00, b'I', b'D', b'A', b'T', // empty IDAT
            0x35, 0xaf, 0x06, 0x1e, // IDAT CRC
            0x00, 0x00, 0x00, 0x00, b'I', b'E', b'N', b'D', // IEND
            0xae, 0x42, 0x60, 0x82, // IEND CRC
        ];
        std::fs::write(photos.join("broken.png"), TRUNCATED_PNG).unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .sql(&format!(
                "CREATE TABLE photos USING IMAGES LOCATION '{}'",
                photos.display()
            ))
            .unwrap();
        // A Triton service Model decodes the IMAGE before it issues an inference request, so the
        // server only needs to serve metadata during RESOLVE MODEL. `mock://` cannot stand in
        // here because it skips decoding and always returns a synthetic detection.
        let Some((address, server)) = serve_triton_metadata_once() else {
            return;
        };
        session
            .run_script(&format!(
                "CREATE MODEL detector TYPE OBJECT_DETECTION \
                 FROM 'triton+http://{address}/detector';
                 RESOLVE MODEL detector;"
            ))
            .unwrap();
        server.join().unwrap();

        let query = session
            .sql(
                "SELECT CARDINALITY(detector(image, classes => ['person'], min_confidence => 0.5)) AS people \
                 FROM photos",
            )
            .unwrap();
        let batches = query.collect().unwrap();
        assert!(
            batches[0].column(0).is_null(0),
            "a row that fails inference must yield NULL"
        );

        session.sql("SET vql.on_error='fail'").unwrap();
        let error = session
            .sql("SELECT detector(image) FROM photos")
            .unwrap()
            .collect()
            .unwrap_err();

        assert_eq!(error.code, ErrorCode::Execution);
        assert!(
            error
                .to_string()
                .contains("failed to decode model IMAGE input"),
            "unexpected strict inference error: {error:?}"
        );
    }

    #[test]
    fn table_lifecycle_shows_describes_and_drops() {
        let temp = tempdir().unwrap();
        let photos = temp.path().join("photos");
        std::fs::create_dir(&photos).unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .sql(&format!(
                "CREATE TABLE photos USING IMAGES LOCATION '{}'",
                photos.display()
            ))
            .unwrap();

        let shown = session.sql("SHOW TABLES").unwrap().collect().unwrap();
        assert_eq!(
            shown[0]
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().clone(),
                    field.data_type().clone(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("table_name".to_owned(), DataType::Utf8, false),
                ("provider".to_owned(), DataType::Utf8, false),
                ("location".to_owned(), DataType::Utf8, false),
            ]
        );
        assert_eq!(shown[0].num_rows(), 1);
        let table_names = shown[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let providers = shown[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let locations = shown[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(table_names.value(0), "photos");
        assert_eq!(providers.value(0), "IMAGES");
        assert_eq!(
            locations.value(0),
            photos.canonicalize().unwrap().to_string_lossy()
        );

        let described = session.sql("DESCRIBE photos").unwrap().collect().unwrap();
        let columns = described[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let data_types = described[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let nullable = described[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            (0..described[0].num_rows())
                .map(|row| (
                    columns.value(row),
                    data_types.value(row),
                    nullable.value(row)
                ))
                .collect::<Vec<_>>(),
            [
                ("uri", "Utf8", "false"),
                (
                    "image",
                    "Struct(\"uri\": Utf8, \"locator\": Utf8, \"pts_ms\": Int64, \"frame_id\": UInt64, \"encoded\": Binary, \"encoding\": Utf8, \"width\": Int32, \"height\": Int32, \"buffer_id\": UInt64, \"buffer_slot\": UInt32)",
                    "false"
                ),
                ("width", "Int32", "true"),
                ("height", "Int32", "true"),
                ("captured_at", "Timestamp(ms, \"UTC\")", "true"),
            ]
        );

        session.sql("DROP TABLE photos").unwrap();

        let shown = session.sql("SHOW TABLES").unwrap().collect().unwrap();
        assert_eq!(
            shown.iter().map(RecordBatch::num_rows).sum::<usize>(),
            0,
            "a dropped table must disappear from SHOW TABLES"
        );
    }

    #[cfg(feature = "ffmpeg-native")]
    #[test]
    fn rtsp_table_lifecycle_and_streaming_planning_are_available_without_connecting() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .sql(
                "CREATE TABLE entrance USING RTSP OPTIONS (\
                 url = 'rtsp://camera.example:554/live', fps = 5, \
                 event_time = 'capture_time', watermark = '2 seconds', transport = 'tcp')",
            )
            .unwrap();

        let shown = session.sql("SHOW TABLES").unwrap().collect().unwrap();
        assert_eq!(shown[0].num_rows(), 1);
        let names = shown[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "entrance");

        let described = session.sql("DESCRIBE entrance").unwrap().collect().unwrap();
        let names = described[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            (0..names.len())
                .map(|row| names.value(row))
                .collect::<Vec<_>>(),
            ["ts", "frame", "frame_id", "source"]
        );
        let data_types = described[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(data_types.value(2), "Int64");

        let statement = session
            .sql("SELECT ts, frame_id, source FROM entrance WHERE frame_id >= 0")
            .unwrap();
        assert!(statement.is_unbounded());
        let explain = explain_text(&session, "EXPLAIN SELECT frame_id FROM entrance");
        assert!(explain.contains("VisionQLPlan mode=continuous"));
        assert!(explain.contains("Source RTSP name=entrance fps=5"));
        assert!(explain.contains("RTSPSource -> EpochCoordinator"));
        assert!(explain.contains("Watermark"));
        assert!(explain.contains("projection=[\"frame_id\"]"));
        let bounded = explain_text(&session, "EXPLAIN SELECT frame_id FROM entrance LIMIT 2");
        assert!(bounded.contains("VisionQLPlan mode=bounded"));
        let tumble = explain_text(
            &session,
            "EXPLAIN SELECT TUMBLE(ts, INTERVAL '1' SECOND), COUNT(*) FROM entrance GROUP BY 1",
        );
        assert!(tumble.contains("TumblePlan"));
        let unsupported = explain_text(
            &session,
            "EXPLAIN SELECT frame_id FROM entrance ORDER BY frame_id",
        );
        assert!(unsupported.contains("Unsupported node=Sort"));
        assert!(unsupported.contains("move ordering to a bounded result"));
        for (sql, node, fragment) in [
            (
                "SELECT 'ORDER BY' AS note, frame_id FROM entrance ORDER BY frame_id",
                "Sort",
                "ORDER BY frame_id",
            ),
            (
                "SELECT DISTINCT frame_id FROM entrance",
                "Distinct",
                "DISTINCT frame_id FROM entrance",
            ),
            (
                "SELECT frame_id, ROW_NUMBER() OVER (ORDER BY frame_id) FROM entrance",
                "Window",
                "OVER (ORDER BY frame_id) FROM entrance",
            ),
            (
                "SELECT frame_id FROM entrance UNION ALL SELECT frame_id FROM entrance",
                "Union",
                "UNION ALL SELECT frame_id FROM entrance",
            ),
            (
                "SELECT a.frame_id FROM entrance a JOIN entrance b ON a.frame_id = b.frame_id",
                "Join",
                "JOIN entrance b ON a.frame_id = b.frame_id",
            ),
        ] {
            let error = session.sql(sql).unwrap_err();
            assert_eq!(error.code, ErrorCode::InvalidSql);
            assert!(
                error
                    .message
                    .contains(&format!("first unsupported node: {node}")),
                "{}",
                error.message
            );
            assert!(error.message.contains(fragment), "{}", error.message);
            assert!(error.message.contains("SQL bytes"), "{}", error.message);
        }
        let error = session
            .sql("EXPLAIN ANALYZE SELECT frame_id FROM entrance")
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidSql);
        assert!(error.message.contains("side-effect free"));
        let error = session.sql("SELECT COUNT(*) FROM entrance").unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidSql);
        assert!(error.message.contains("require TUMBLE"));

        let windowed = session
            .sql(
                "SELECT TUMBLE(ts, INTERVAL '1' SECOND) AS window_start,
                        source,
                        COUNT(*) AS frames,
                        SUM(frame_id) AS frame_sum,
                        AVG(frame_id) AS frame_avg,
                        MIN(frame_id) AS first_frame,
                        MAX(frame_id) AS last_frame
                 FROM entrance
                 GROUP BY 1, 2",
            )
            .unwrap();
        assert!(windowed.is_unbounded());
        let error = session
            .sql(
                "SELECT TUMBLE(ts, INTERVAL '1' SECOND), COUNT(DISTINCT frame_id)
                 FROM entrance
                 GROUP BY 1",
            )
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidSql);
        assert!(error.message.contains("COUNT(DISTINCT"));
        let error = session
            .sql(
                "SELECT TUMBLE(ts, INTERVAL '1' SECOND), COUNT(frame)
                 FROM entrance
                 GROUP BY 1",
            )
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidSql);
        assert!(error.message.contains("cannot enter window state"));
        for sql in [
            "SELECT TUMBLE(ts, INTERVAL '1' SECOND), frame.buffer_id, COUNT(*)
             FROM entrance
             GROUP BY 1, 2",
            "SELECT TUMBLE(ts, INTERVAL '1' SECOND), SUM(frame.buffer_slot)
             FROM entrance
             GROUP BY 1",
        ] {
            let error = session.sql(sql).unwrap_err();
            assert_eq!(error.code, ErrorCode::InvalidSql);
            assert!(error.message.contains("process-local"));
        }
        let error = session
            .sql(
                "SELECT TUMBLE(ts + INTERVAL '1' SECOND, INTERVAL '1' SECOND), COUNT(*)
                 FROM entrance
                 GROUP BY 1",
            )
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidSql);
        assert!(error.message.contains("event-time column 'ts'"));

        session.sql("DROP TABLE entrance").unwrap();
        assert_eq!(
            session.sql("SHOW TABLES").unwrap().collect().unwrap()[0].num_rows(),
            0
        );
    }

    #[cfg(feature = "ffmpeg-native")]
    #[test]
    fn rtsp_catalog_rejects_embedded_secrets() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        for endpoint in [
            "rtsp://user:password@camera/live",
            "rtsp://camera/live?token=secret",
        ] {
            let error = session
                .sql(&format!(
                    "CREATE TABLE cam USING RTSP OPTIONS (url = '{endpoint}')"
                ))
                .unwrap_err();
            assert_eq!(error.code, ErrorCode::InvalidOption);
        }
    }

    #[cfg(feature = "ffmpeg-native")]
    #[test]
    fn finite_rtsp_query_runs_epochs_and_encodes_egress_images() {
        if !crate::test_util::ffmpeg_available() {
            return;
        }
        let temp = tempdir().unwrap();
        let video = temp.path().join("stream.mp4");
        assert!(crate::test_util::generate_test_video(&video));
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        create_rtsp_table(&engine, "local_stream", &video.to_string_lossy());
        let session = engine.session().build().unwrap();
        let statement = session
            .sql("SELECT frame FROM local_stream LIMIT 2 OFFSET 1")
            .unwrap();
        assert!(!statement.is_unbounded());
        let batches = statement.collect().unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        let images = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();
        let encoded = images
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();
        let buffer_ids = images
            .column(8)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        assert!(!encoded.is_null(0));
        assert!(buffer_ids.is_null(0));
    }

    #[cfg(feature = "ffmpeg-native")]
    #[test]
    fn local_video_stream_runs_tumble_aggregates() {
        if !crate::test_util::ffmpeg_available() {
            return;
        }
        let temp = tempdir().unwrap();
        let video = temp.path().join("window-stream.mp4");
        assert!(crate::test_util::generate_test_video(&video));
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        create_rtsp_table(&engine, "window_stream", &video.to_string_lossy());
        let session = engine.session().build().unwrap();

        let statement = session
            .sql(
                "SELECT TUMBLE(ts, INTERVAL '1' SECOND) AS window_start,
                        COUNT(*) AS frames,
                        SUM(frame_id) AS frame_sum,
                        AVG(frame_id) AS frame_avg,
                        MIN(frame_id) AS first_frame,
                        MAX(frame_id) AS last_frame
                 FROM window_stream
                 GROUP BY 1
                 HAVING COUNT(*) > 0
                 LIMIT 3",
            )
            .unwrap();
        let batches = statement.collect().unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        for batch in &batches {
            let counts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let sums = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let averages = batch
                .column(3)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            let minimums = batch
                .column(4)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let maximums = batch
                .column(5)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let count = counts.value(row);
                let sum = sums.value(row);
                let minimum = minimums.value(row);
                let maximum = maximums.value(row);
                assert!(count > 0);
                assert!(minimum <= maximum);
                // Bounded source backpressure may leave gaps between sampled frame IDs.
                assert!(sum >= count * minimum);
                assert!(sum <= count * maximum);
                assert_eq!(averages.value(row), sum as f64 / count as f64);
            }
        }
    }

    #[cfg(feature = "ffmpeg-native")]
    #[test]
    fn local_video_stream_completes_after_graceful_stop() {
        use std::sync::mpsc;
        use std::time::Duration;

        if !crate::test_util::ffmpeg_available() {
            return;
        }
        let temp = tempdir().unwrap();
        let video = temp.path().join("graceful-stream.mp4");
        assert!(crate::test_util::generate_test_video(&video));
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        create_rtsp_table(&engine, "graceful_stream", &video.to_string_lossy());
        let session = engine.session().build().unwrap();
        let Statement::Query(query) = session.sql("SELECT frame_id FROM graceful_stream").unwrap()
        else {
            panic!("unbounded SELECT must produce a query");
        };
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (finished_tx, finished_rx) = mpsc::sync_channel(1);
        let execution = std::thread::spawn(move || {
            let mut started_tx = Some(started_tx);
            let result = query.for_each_batch(|_| {
                if let Some(started_tx) = started_tx.take() {
                    started_tx.send(()).unwrap();
                }
                Ok(())
            });
            finished_tx.send(result).unwrap();
        });
        if let Err(error) = started_rx.recv_timeout(Duration::from_secs(5)) {
            session.cancel_active_query();
            let _ = finished_rx.recv_timeout(Duration::from_secs(5));
            execution.join().unwrap();
            panic!("unbounded query did not emit a batch: {error}");
        }

        assert_eq!(
            session.interrupt_active_query(),
            QueryInterruptAction::GracefulStopRequested
        );
        let result = match finished_rx.recv_timeout(Duration::from_secs(5)) {
            Ok(result) => result,
            Err(error) => {
                session.cancel_active_query();
                panic!("graceful query stop timed out: {error}");
            }
        };

        result.expect("graceful stop must complete without a cancellation error");
        execution.join().unwrap();
        assert_eq!(
            session.interrupt_active_query(),
            QueryInterruptAction::NoActiveQuery
        );
    }

    #[cfg(feature = "ffmpeg-native")]
    #[test]
    fn local_video_stream_runs_people_detection_scenario() {
        if !crate::test_util::ffmpeg_available() {
            return;
        }
        let temp = tempdir().unwrap();
        let video = temp.path().join("people-stream.mp4");
        assert!(crate::test_util::generate_test_video(&video));
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        create_rtsp_table(&engine, "people_stream", &video.to_string_lossy());
        let session = engine.session().build().unwrap();
        session
            .run_script(
                "CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://person' USING ONNX_RUNTIME;
                 RESOLVE MODEL detector;",
            )
            .unwrap();

        let statement = session
            .sql(
                "WITH detected AS (
                   SELECT frame_id, CARDINALITY(detector(frame, classes => ['person'], min_confidence => 0.5
                   )) AS people
                   FROM people_stream
                 )
                 SELECT frame_id, people
                 FROM detected
                 WHERE people > 0
                 LIMIT 3",
            )
            .unwrap();
        let batches = statement.collect().unwrap();
        let rows = batches
            .iter()
            .flat_map(|batch| {
                let frame_ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let people = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::UInt64Array>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|row| (frame_ids.value(row), people.value(row)))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert_eq!(rows, [(0, 1), (1, 1), (2, 1)]);

        let windowed = session
            .sql(
                "WITH detected AS (
                   SELECT ts, CARDINALITY(detector(frame, classes => ['person'], min_confidence => 0.5
                   )) AS people
                   FROM people_stream
                 )
                 SELECT TUMBLE(ts, INTERVAL '1' SECOND) AS window_start,
                        COUNT(*) AS frames,
                        SUM(people) AS total_people,
                        AVG(people) AS average_people,
                        MIN(people) AS minimum_people,
                        MAX(people) AS maximum_people
                 FROM detected
                 GROUP BY 1
                 LIMIT 2",
            )
            .unwrap();
        let batches = windowed.collect().unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        for batch in &batches {
            let frames = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let totals = batch
                .column(2)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .unwrap();
            let averages = batch
                .column(3)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert_eq!(totals.value(row), frames.value(row) as u64);
                assert_eq!(averages.value(row), 1.0);
            }
        }
    }

    #[test]
    fn prepare_classifies_statements_and_defers_catalog_mutation() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        let table_sql = format!(
            "CREATE TABLE photos USING IMAGES LOCATION '{}'",
            temp.path().display()
        );
        let prepared = session
            .prepare(&table_sql, "service", session.semantic_settings())
            .unwrap();
        assert_eq!(prepared.statement_info().kind, StatementKind::Update);
        assert!(
            engine
                .catalog()
                .snapshot()
                .unwrap()
                .table("photos")
                .is_none()
        );
        prepared.execute_update().unwrap();
        assert!(
            engine
                .catalog()
                .snapshot()
                .unwrap()
                .table("photos")
                .is_some()
        );

        let prepared = session
            .prepare(
                "SELECT 42 AS answer",
                "service",
                session.semantic_settings(),
            )
            .unwrap();
        assert_eq!(prepared.statement_info().query_mode, QueryMode::Bounded);
        assert_eq!(
            prepared
                .result_schema()
                .metadata()
                .get("vql.statement_info.kind")
                .map(String::as_str),
            Some("query")
        );
        let PreparedResult::Query(query) = prepared.execute_query().unwrap() else {
            panic!("SELECT must create a query handle")
        };
        assert_eq!(query.collect().unwrap()[0].num_rows(), 1);
    }

    #[test]
    fn prepared_query_keeps_its_definition_snapshot_after_drop() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .sql(&format!(
                "CREATE TABLE photos USING IMAGES LOCATION '{}'",
                temp.path().display()
            ))
            .unwrap();
        let prepared = session
            .prepare(
                "SELECT uri FROM photos",
                "service",
                session.semantic_settings(),
            )
            .unwrap();
        let explained = session
            .prepare(
                "EXPLAIN SELECT uri FROM photos",
                "service",
                session.semantic_settings(),
            )
            .unwrap();
        let generations = prepared.definition_generations().to_vec();
        session.sql("DROP TABLE photos").unwrap();

        let PreparedResult::Query(query) = prepared.execute_query().unwrap() else {
            panic!("SELECT must create a query handle")
        };
        assert_eq!(
            query
                .collect()
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
        let PreparedResult::Query(explain) = explained.execute_query().unwrap() else {
            panic!("EXPLAIN must create a query handle")
        };
        assert!(!explain.collect().unwrap().is_empty());
        let pinned = session
            .prepare_pinned(
                "SELECT uri FROM photos",
                "service",
                session.semantic_settings(),
                &generations,
            )
            .unwrap();
        assert_eq!(pinned.definition_generations(), generations);
    }

    #[test]
    fn prepared_catalog_query_defers_execution_and_uses_its_snapshot() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        let missing = session
            .prepare(
                "SHOW CREATE TABLE missing",
                "service",
                session.semantic_settings(),
            )
            .unwrap();
        assert_eq!(missing.result_schema().fields().len(), 3);
        assert_eq!(
            missing.execute_query().unwrap_err().code,
            ErrorCode::NotFound
        );

        session
            .sql(&format!(
                "CREATE TABLE photos USING IMAGES LOCATION '{}'",
                temp.path().display()
            ))
            .unwrap();
        let prepared = session
            .prepare("SHOW TABLES", "service", session.semantic_settings())
            .unwrap();
        session.sql("DROP TABLE photos").unwrap();

        let PreparedResult::Batches(batches) = prepared.execute_query().unwrap() else {
            panic!("SHOW TABLES must produce bounded batches")
        };
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn prepared_execution_profile_tracks_sources_and_sinks() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .run_script(&format!(
                "CREATE TABLE photos USING IMAGES LOCATION '{}';
                 CREATE TABLE events (uri STRING) USING KAFKA OPTIONS (
                   bootstrap_servers = '127.0.0.1:9092', topic = 'events');",
                temp.path().display()
            ))
            .unwrap();

        let prepared = session
            .prepare(
                "INSERT INTO events SELECT uri FROM photos",
                "service",
                session.semantic_settings(),
            )
            .unwrap();

        assert!(prepared.uses_source());
        assert!(prepared.uses_sink());
        assert!(!prepared.uses_inference());
    }
}
