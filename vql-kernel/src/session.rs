use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::catalog::{FunctionImplementation, ObjectKind, SinkDef, TableDef, TableProviderKind};
use crate::connectors::images::{ImagesTableProvider, images_schema};
use crate::connectors::videos::{VideosTableProvider, videos_schema};
use crate::functions::VqlFunctionFactory;
use crate::media::{MediaCounters, MediaRuntime};
use crate::models::{ModelCounters, resolve_model};
use crate::planner::{
    context_for_function_ddl, context_for_snapshot, normalize_function_ddl, plan_statement,
    wrap_console_sink,
};
use crate::sql::{CreateModel, CreateTable, ShowKind, VqlStatement, parse_statement};
use crate::types::{image_field, is_image_storage};
use crate::{Engine, ErrorCode, PythonUdfHostRef, Result, VqlError};

#[derive(Debug)]
pub struct SessionBuilder {
    engine: Engine,
    python_udf_host: Option<PythonUdfHostRef>,
}

impl SessionBuilder {
    pub(crate) fn new(engine: Engine) -> Self {
        Self {
            engine,
            python_udf_host: None,
        }
    }

    pub fn with_python_udf_host(mut self, host: PythonUdfHostRef) -> Self {
        self.python_udf_host = Some(host);
        self
    }

    pub fn build(self) -> Result<Session> {
        self.engine.inner.catalog.snapshot()?;
        Ok(Session {
            engine: self.engine,
            active_query: Arc::new(Mutex::new(None)),
            fail_on_error: Arc::new(AtomicBool::new(false)),
            python_udf_host: self.python_udf_host,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Session {
    engine: Engine,
    active_query: Arc<Mutex<Option<CancellationToken>>>,
    fail_on_error: Arc<AtomicBool>,
    python_udf_host: Option<PythonUdfHostRef>,
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

    pub fn metrics(&self) -> Option<Arc<QueryMetrics>> {
        match self {
            Self::Query(query) | Self::Explain(query) | Self::Set(query) => Some(query.metrics()),
            Self::Ddl(_) => None,
        }
    }

    pub fn cancel(&self) {
        match self {
            Self::Query(query) | Self::Explain(query) | Self::Set(query) => query.cancel(),
            Self::Ddl(_) => {}
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

#[derive(Debug, Default)]
pub struct QueryMetrics {
    input_rows: AtomicU64,
    output_rows: AtomicU64,
    decode_frames: AtomicU64,
    inference_rows: AtomicU64,
    inference_batches: AtomicU64,
    error_rows: AtomicU64,
    inference_p50_micros: AtomicU64,
    inference_p95_micros: AtomicU64,
    batch_histogram: Mutex<Vec<u64>>,
}

impl QueryMetrics {
    pub fn input_rows(&self) -> u64 {
        self.input_rows.load(Ordering::Relaxed)
    }
    pub fn output_rows(&self) -> u64 {
        self.output_rows.load(Ordering::Relaxed)
    }

    pub fn decode_frames(&self) -> u64 {
        self.decode_frames.load(Ordering::Relaxed)
    }

    pub fn inference_rows(&self) -> u64 {
        self.inference_rows.load(Ordering::Relaxed)
    }
    pub fn inference_batches(&self) -> u64 {
        self.inference_batches.load(Ordering::Relaxed)
    }
    pub fn error_rows(&self) -> u64 {
        self.error_rows.load(Ordering::Relaxed)
    }
    pub fn inference_calls(&self) -> u64 {
        self.inference_batches()
    }
    pub fn inference_p50_ms(&self) -> f64 {
        self.inference_p50_micros.load(Ordering::Relaxed) as f64 / 1_000.0
    }
    pub fn inference_p95_ms(&self) -> f64 {
        self.inference_p95_micros.load(Ordering::Relaxed) as f64 / 1_000.0
    }
    pub fn batch_histogram(&self) -> Vec<u64> {
        self.batch_histogram
            .lock()
            .map(|values| values.clone())
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone)]
pub struct QueryHandle {
    dataframe: DataFrame,
    runtime: Arc<tokio::runtime::Runtime>,
    cancellation: CancellationToken,
    active_query: Arc<Mutex<Option<CancellationToken>>>,
    output_schema: SchemaRef,
    metrics: Arc<QueryMetrics>,
    collected: Arc<Mutex<Option<Vec<RecordBatch>>>>,
    media: Arc<MediaRuntime>,
    media_start: MediaCounters,
    models: Arc<crate::models::ModelRuntime>,
    model_start: ModelCounters,
    model_sample_start: usize,
}

impl QueryHandle {
    fn new(
        dataframe: DataFrame,
        runtime: Arc<tokio::runtime::Runtime>,
        active_query: Arc<Mutex<Option<CancellationToken>>>,
        cancellation: CancellationToken,
        media: Arc<MediaRuntime>,
        models: Arc<crate::models::ModelRuntime>,
    ) -> Self {
        let output_schema = restamp_schema(dataframe.schema().inner());
        let media_start = media.counters();
        let model_start = models.counters();
        let model_sample_start = models.sample_count();
        Self {
            dataframe,
            runtime,
            cancellation,
            active_query,
            output_schema,
            metrics: Arc::new(QueryMetrics::default()),
            collected: Arc::new(Mutex::new(None)),
            media,
            media_start,
            models,
            model_start,
            model_sample_start,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    pub fn stream(&self) -> Result<SendableRecordBatchStream> {
        if let Some(batches) = self.cached_batches()? {
            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.output_schema),
                stream,
            )));
        }
        self.set_active()?;
        let input = self
            .runtime
            .block_on(self.dataframe.clone().execute_stream())?;
        let output_schema = Arc::clone(&self.output_schema);
        let stream_schema = Arc::clone(&output_schema);
        let cancellation = self.cancellation.clone();
        let active_query = Arc::clone(&self.active_query);
        let metrics = Arc::clone(&self.metrics);
        let stream = async_stream::try_stream! {
            let mut input = input;
            loop {
                let next = tokio::select! {
                    _ = cancellation.cancelled() => None,
                    next = input.next() => next,
                };
                if cancellation.is_cancelled() {
                    Err(datafusion::error::DataFusionError::Execution(
                        "[VQL:QUERY_CANCELLED] query cancelled".to_owned(),
                    ))?;
                }
                let Some(batch) = next else { break; };
                let batch = batch?;
                metrics.output_rows.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                yield RecordBatch::try_new(
                    Arc::clone(&output_schema),
                    batch.columns().to_vec(),
                )?;
            }
            if let Ok(mut active) = active_query.lock() {
                *active = None;
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
        let stream = self.stream()?;
        let result = self
            .runtime
            .block_on(async { stream.collect::<Vec<_>>().await });
        if let Ok(mut active) = self.active_query.lock() {
            *active = None;
        }
        let mut batches = Vec::with_capacity(result.len());
        for batch in result {
            match batch {
                Ok(batch) => batches.push(batch),
                Err(_error) if self.cancellation.is_cancelled() => {
                    return Err(VqlError::new(ErrorCode::QueryCancelled, "query cancelled"));
                }
                Err(error) => return Err(error.into()),
            }
        }
        let mut collected = self
            .collected
            .lock()
            .map_err(|_| VqlError::new(ErrorCode::Internal, "query result cache was poisoned"))?;
        *collected = Some(batches.clone());
        let media_end = self.media.counters();
        self.metrics.decode_frames.store(
            media_end
                .decoded_frames
                .saturating_sub(self.media_start.decoded_frames),
            Ordering::Relaxed,
        );
        let model_end = self.models.counters();
        self.metrics.inference_rows.store(
            model_end
                .inference_rows
                .saturating_sub(self.model_start.inference_rows),
            Ordering::Relaxed,
        );
        self.metrics.inference_batches.store(
            model_end
                .inference_batches
                .saturating_sub(self.model_start.inference_batches),
            Ordering::Relaxed,
        );
        self.metrics.error_rows.store(
            media_end
                .decode_errors
                .saturating_sub(self.media_start.decode_errors)
                .saturating_add(
                    model_end
                        .inference_errors
                        .saturating_sub(self.model_start.inference_errors),
                ),
            Ordering::Relaxed,
        );
        self.metrics.input_rows.store(
            self.metrics.output_rows.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        let samples = self.models.samples_since(self.model_sample_start);
        let mut latencies = samples.iter().map(|sample| sample.0).collect::<Vec<_>>();
        latencies.sort_unstable();
        if !latencies.is_empty() {
            self.metrics
                .inference_p50_micros
                .store(percentile(&latencies, 0.50), Ordering::Relaxed);
            self.metrics
                .inference_p95_micros
                .store(percentile(&latencies, 0.95), Ordering::Relaxed);
        }
        if let Ok(mut histogram) = self.metrics.batch_histogram.lock() {
            *histogram = samples.into_iter().map(|sample| sample.1).collect();
        }
        Ok(batches)
    }

    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub fn metrics(&self) -> Arc<QueryMetrics> {
        Arc::clone(&self.metrics)
    }

    fn set_active(&self) -> Result<()> {
        let mut active = self
            .active_query
            .lock()
            .map_err(|_| VqlError::new(ErrorCode::Internal, "active query lock was poisoned"))?;
        *active = Some(self.cancellation.clone());
        Ok(())
    }

    fn cached_batches(&self) -> Result<Option<Vec<RecordBatch>>> {
        self.collected
            .lock()
            .map(|batches| batches.clone())
            .map_err(|_| VqlError::new(ErrorCode::Internal, "query result cache was poisoned"))
    }
}

impl Session {
    pub fn sql(&self, sql: &str) -> Result<Statement> {
        match parse_statement(sql)? {
            VqlStatement::CreateTable(create) => self.create_table(create).map(Statement::Ddl),
            VqlStatement::CreateModel(create) => self.create_model(create).map(Statement::Ddl),
            VqlStatement::CreateFunction { sql } => self.create_function(&sql).map(Statement::Ddl),
            VqlStatement::CreateSink { name, kind } => {
                self.create_sink(&name, kind).map(Statement::Ddl)
            }
            VqlStatement::Drop { kind, name } => self.drop_object(kind, &name).map(Statement::Ddl),
            VqlStatement::Show(kind) => self.show_objects(kind).map(Statement::Ddl),
            VqlStatement::Describe { name } => self.describe(&name).map(Statement::Ddl),
            VqlStatement::Query { sql }
                if sql.trim_start().to_ascii_uppercase().starts_with("INSERT") =>
            {
                self.insert_into_sink(&sql).map(Statement::Query)
            }
            VqlStatement::Query { sql } => self.query(&sql).map(Statement::Query),
            VqlStatement::Explain { sql } => self.query(&sql).map(Statement::Explain),
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
            && let Some(token) = active.as_ref()
        {
            token.cancel();
        }
    }

    fn query(&self, sql: &str) -> Result<QueryHandle> {
        let snapshot = self.engine.inner.catalog.snapshot()?;
        if self.python_udf_host.is_none() {
            let upper = sql.to_ascii_uppercase();
            if let Some((name, _)) = snapshot.functions().find(|(name, function)| {
                matches!(
                    function.definition.implementation,
                    FunctionImplementation::Python { .. }
                ) && upper.contains(&format!("{}(", name.to_ascii_uppercase()))
            }) {
                return Err(VqlError::new(
                    ErrorCode::PythonHostRequired,
                    format!("function '{name}' requires the visionql Python host"),
                ));
            }
        }
        let context = context_for_snapshot(
            &snapshot,
            Arc::clone(&self.engine.inner.catalog),
            Arc::clone(&self.engine.inner.media),
            Arc::clone(&self.fail_on_error),
            self.python_udf_host.clone(),
        )?;
        let cancellation = CancellationToken::new();
        let dataframe = self.engine.inner.runtime.block_on(plan_statement(
            &context,
            &snapshot,
            sql,
            Arc::clone(&self.engine.inner.models),
            Arc::clone(&self.fail_on_error),
            cancellation.clone(),
        ))?;
        Ok(QueryHandle::new(
            dataframe,
            Arc::clone(&self.engine.inner.runtime),
            Arc::clone(&self.active_query),
            cancellation,
            Arc::clone(&self.engine.inner.media),
            Arc::clone(&self.engine.inner.models),
        ))
    }

    fn create_table(&self, create: CreateTable) -> Result<DdlResult> {
        let location = normalize_location(&create.location)?;
        let definition = TableDef {
            name: create.name.clone(),
            provider: create.provider,
            location: location.to_string_lossy().into_owned(),
            recursive: create.recursive,
            fps: create.fps,
            start_time_ms: create.start_time_ms,
        };
        match definition.provider {
            TableProviderKind::Images => {
                ImagesTableProvider::try_new(&definition.location, 0, definition.recursive)?;
            }
            TableProviderKind::Videos => {
                if !self.engine.inner.media.video_available() {
                    return Err(VqlError::new(
                        ErrorCode::FeatureNotAvailable,
                        "USING VIDEOS requires FFmpeg 8 or ffmpeg/ffprobe executables",
                    ));
                }
                VideosTableProvider::try_new(
                    &definition.location,
                    0,
                    definition.recursive,
                    definition.fps,
                    definition.start_time_ms,
                    Arc::clone(&self.engine.inner.media),
                )?;
            }
        }
        let schema = match definition.provider {
            TableProviderKind::Images => images_schema(),
            TableProviderKind::Videos => videos_schema(definition.start_time_ms.is_none()),
        };
        let revision = self
            .engine
            .inner
            .catalog
            .create_table(&definition, &schema)?;
        Ok(message_result(format!(
            "created table '{}' at revision {revision}",
            create.name
        )))
    }

    fn drop_table(&self, name: &str) -> Result<DdlResult> {
        let revision = self.engine.inner.catalog.drop_table(name)?;
        Ok(message_result(format!(
            "dropped table '{name}' at revision {revision}"
        )))
    }

    fn create_model(&self, create: CreateModel) -> Result<DdlResult> {
        let (runtime, pre_processor, post_processor) = self
            .engine
            .inner
            .pipelines
            .model_specs_for_options(create.model_type, &create.source, &create.options)?;
        let model = resolve_model(
            &create.name,
            create.model_type,
            &create.source,
            runtime,
            pre_processor,
            post_processor,
            self.engine.inner.config.model_cache_dir(),
        )?;
        self.engine.inner.pipelines.validate_model(&model)?;
        let revision = self.engine.inner.catalog.create_model(&model)?;
        Ok(message_result(format!(
            "created model '{}' at revision {revision}",
            model.name
        )))
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
        let function = factory.take_definition()?;
        let revision = self.engine.inner.catalog.create_function(&function)?;
        Ok(message_result(format!(
            "created function '{}' at revision {revision}",
            function.name
        )))
    }

    fn create_sink(&self, name: &str, kind: crate::catalog::SinkKind) -> Result<DdlResult> {
        let sink = SinkDef {
            name: name.to_ascii_lowercase(),
            kind,
        };
        let revision = self.engine.inner.catalog.create_sink(&sink)?;
        Ok(message_result(format!(
            "created sink '{}' at revision {revision}",
            sink.name
        )))
    }

    fn insert_into_sink(&self, sql: &str) -> Result<QueryHandle> {
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
                "expected INSERT INTO <sink> SELECT ...",
            ));
        }
        let sink_name = parts.next().ok_or_else(|| {
            VqlError::new(ErrorCode::InvalidSql, "INSERT INTO requires a sink name")
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
                "v0.1 Sink writes require SELECT or WITH",
            ));
        }
        let snapshot = self.engine.inner.catalog.snapshot()?;
        let sink = snapshot.sink(sink_name).ok_or_else(|| {
            VqlError::new(
                ErrorCode::NotFound,
                format!("sink '{sink_name}' does not exist"),
            )
        })?;
        match sink.definition.kind {
            crate::catalog::SinkKind::Console => {
                let mut handle = self.query(query)?;
                handle.dataframe = wrap_console_sink(handle.dataframe, sink_name.to_owned());
                Ok(handle)
            }
        }
    }

    fn drop_object(&self, kind: ShowKind, name: &str) -> Result<DdlResult> {
        let revision = match kind {
            ShowKind::Tables => return self.drop_table(name),
            ShowKind::Models => {
                let revision = self.engine.inner.catalog.drop_model(name)?;
                self.engine.inner.models.evict_stale()?;
                revision
            }
            ShowKind::Functions => {
                self.engine
                    .inner
                    .catalog
                    .drop_object("function", ObjectKind::Function, name)?
            }
            ShowKind::Sinks => {
                self.engine
                    .inner
                    .catalog
                    .drop_object("sink", ObjectKind::Sink, name)?
            }
        };
        Ok(message_result(format!(
            "dropped object '{name}' at revision {revision}"
        )))
    }

    fn show_objects(&self, kind: ShowKind) -> Result<DdlResult> {
        if kind == ShowKind::Tables {
            return self.show_tables();
        }
        let snapshot = self.engine.inner.catalog.snapshot()?;
        let rows = match kind {
            ShowKind::Models => snapshot
                .models()
                .map(|(name, value)| {
                    (
                        name.to_owned(),
                        format!("{:?}", value.definition.model_type),
                        value.revision,
                    )
                })
                .collect::<Vec<_>>(),
            ShowKind::Functions => snapshot
                .functions()
                .map(|(name, value)| {
                    let implementation = match &value.definition.implementation {
                        FunctionImplementation::Python { .. } => "PYTHON",
                        FunctionImplementation::SqlMacro { .. } => "SQL_MACRO",
                    };
                    (name.to_owned(), implementation.to_owned(), value.revision)
                })
                .collect(),
            ShowKind::Sinks => snapshot
                .sinks()
                .map(|(name, value)| {
                    (
                        name.to_owned(),
                        format!("{:?}", value.definition.kind),
                        value.revision,
                    )
                })
                .collect(),
            ShowKind::Tables => unreachable!(),
        };
        named_objects_result(rows)
    }

    fn set(&self, sql: &str) -> Result<DdlResult> {
        let normalized = sql
            .trim()
            .trim_end_matches(';')
            .replace(' ', "")
            .to_ascii_lowercase();
        let fail = match normalized.as_str() {
            "setvql.on_error='fail'" | "setvql.on_error=fail" => true,
            "setvql.on_error='null'" | "setvql.on_error=null" => false,
            _ => {
                return Err(VqlError::new(
                    ErrorCode::InvalidOption,
                    "v0.1 supports SET vql.on_error='null'|'fail'",
                ));
            }
        };
        self.fail_on_error.store(fail, Ordering::Relaxed);
        Ok(message_result(format!(
            "vql.on_error = '{}'",
            if fail { "fail" } else { "null" }
        )))
    }

    fn show_tables(&self) -> Result<DdlResult> {
        let snapshot = self.engine.inner.catalog.snapshot()?;
        let mut names = Vec::new();
        let mut providers = Vec::new();
        let mut locations = Vec::new();
        let mut revisions = Vec::new();
        for (name, table) in snapshot.tables() {
            names.push(name.to_owned());
            providers.push(format!("{:?}", table.definition.provider).to_ascii_uppercase());
            locations.push(table.definition.location.clone());
            revisions.push(table.revision);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_name", DataType::Utf8, false),
            Field::new("provider", DataType::Utf8, false),
            Field::new("location", DataType::Utf8, false),
            Field::new("revision", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names)) as ArrayRef,
                Arc::new(StringArray::from(providers)),
                Arc::new(StringArray::from(locations)),
                Arc::new(Int64Array::from(revisions)),
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

    fn describe(&self, name: &str) -> Result<DdlResult> {
        let snapshot = self.engine.inner.catalog.snapshot()?;
        let table = snapshot.table(name).ok_or_else(|| {
            VqlError::new(
                ErrorCode::NotFound,
                format!("table '{name}' does not exist"),
            )
        })?;
        let fields = table.schema.fields();
        let schema = Arc::new(Schema::new(vec![
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("nullable", DataType::Utf8, false),
        ]));
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
            message: format!("table '{name}'"),
            batches: vec![batch],
        })
    }
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

fn named_objects_result(rows: Vec<(String, String, i64)>) -> Result<DdlResult> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("revision", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.0.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(|error| {
        VqlError::new(ErrorCode::Execution, "failed to build catalog result").with_source(error)
    })?;
    Ok(DdlResult {
        message: format!("{} object(s)", batch.num_rows()),
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

fn percentile(values: &[u64], percentile: f64) -> u64 {
    let index = ((values.len() - 1) as f64 * percentile).ceil() as usize;
    values[index]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EngineConfig;
    use arrow::array::{Array, Float64Array};
    use image::{Rgb, RgbImage};
    use tempfile::tempdir;

    #[test]
    fn catalog_reopens_created_image_table() {
        let temp = tempdir().unwrap();
        let photos = temp.path().join("photos");
        std::fs::create_dir(&photos).unwrap();
        let catalog_path = temp.path().join("catalog.db");
        let create = format!(
            "CREATE TABLE photos USING IMAGES LOCATION '{}' WITH (recursive=true);",
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

    #[test]
    fn typed_models_do_not_create_function_dependencies() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .sql(
                "CREATE MODEL detector TYPE OBJECT_DETECTION \
                 FROM 'mock://person'",
            )
            .unwrap();

        let result = session.sql("DROP MODEL detector").unwrap();

        assert!(matches!(result, Statement::Ddl(_)));
        assert!(session.sql("SELECT 1").unwrap().collect().is_ok());
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
                 CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://person';",
                photos.display()
            ))
            .unwrap();
        session
            .sql("SELECT DETECT_OBJECTS('detector', image) FROM photos")
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(engine.inner.models.cached_pipeline_count(), 1);

        session.sql("DROP MODEL detector").unwrap();
        assert_eq!(engine.inner.models.cached_pipeline_count(), 0);

        session
            .sql("CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://person'")
            .unwrap();
        session
            .sql("SELECT DETECT_OBJECTS('detector', image) FROM photos")
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
                     WITH (runtime.kind='onnxruntime', \
                           pre_processor.options={input_name='pixels', width=320, height=192}, \
                           post_processor.options={output_name='detections', labels=['person']})",
                )
                .unwrap();
        }

        let engine = Engine::new(EngineConfig::new(&catalog)).unwrap();
        let snapshot = engine.inner.catalog.snapshot().unwrap();
        let model = &snapshot.model("detector").unwrap().definition;

        assert_eq!(model.runtime.kind, "onnxruntime");
        assert_eq!(model.pre_processor.kind, "vision.image_tensor@1");
        assert_eq!(model.pre_processor.options["width"], serde_json::json!(320));
        assert_eq!(model.post_processor.kind, "vision.yolo_e2e@1");
        assert_eq!(
            model.post_processor.options["labels"],
            serde_json::json!(["person"])
        );
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
                 CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://person';
                 CREATE FUNCTION detect_people(IMAGE)
                 RETURN DETECT_OBJECTS(
                   'detector', $1,
                   classes => ['person'], min_confidence => 0.5
                 );",
                photos.display()
            ))
            .unwrap();

        let batches = session
            .sql("SELECT COUNT_OBJECTS(detect_people(image), 'person', 0.5) AS people FROM photos")
            .unwrap()
            .collect()
            .unwrap();
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(values.value(0), 1);
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
                 FROM 'mock://person';",
                photos.display()
            ))
            .unwrap();

        let batches = session
            .sql(
                "SELECT f.uri, det.label
                 FROM photos AS f, UNNEST(DETECT_OBJECTS('detector', f.image)) AS u(det)",
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
                .revision_count(crate::catalog::ObjectKind::Table)
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
                .revision_count(crate::catalog::ObjectKind::Table)
                .unwrap(),
            0
        );
    }

    #[test]
    fn videos_sample_by_pts_and_to_jpeg_decodes_on_demand() {
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
                "CREATE TABLE clips USING VIDEOS LOCATION '{}' WITH (fps=5)",
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
            .sql("SELECT TO_JPEG(frame, 80) AS jpeg FROM clips WHERE pts_ms = 0 LIMIT 1")
            .unwrap()
            .collect()
            .unwrap();
        let jpeg = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();
        let image = image::load_from_memory(jpeg.value(0)).unwrap();
        assert_eq!((image.width(), image.height()), (320, 240));
        assert_eq!(engine.inner.media.counters().decoded_frames, 1);

        let batches = session
            .sql("SELECT TUMBLE(ts, INTERVAL '1' MINUTE) AS minute, COUNT(*) FROM clips GROUP BY 1")
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn to_jpeg_rejects_invalid_quality_in_null_mode() {
        let temp = tempdir().unwrap();
        let photos = temp.path().join("photos");
        std::fs::create_dir(&photos).unwrap();
        RgbImage::from_pixel(8, 8, Rgb([10, 20, 30]))
            .save(photos.join("one.png"))
            .unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .sql(&format!(
                "CREATE TABLE photos USING IMAGES LOCATION '{}'",
                photos.display()
            ))
            .unwrap();

        let error = session
            .sql("SELECT TO_JPEG(image, 0) FROM photos")
            .unwrap()
            .collect()
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("quality must be between 1 and 100")
        );
    }

    #[test]
    fn to_jpeg_honors_error_mode_and_counts_null_rows() {
        let temp = tempdir().unwrap();
        let photos = temp.path().join("photos");
        std::fs::create_dir(&photos).unwrap();
        std::fs::write(photos.join("broken.jpg"), b"not an image").unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .sql(&format!(
                "CREATE TABLE photos USING IMAGES LOCATION '{}'",
                photos.display()
            ))
            .unwrap();

        let query = session
            .sql("SELECT TO_JPEG(image) AS jpeg FROM photos")
            .unwrap();
        let batches = query.collect().unwrap();
        let jpeg = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();
        assert!(jpeg.is_null(0));
        assert_eq!(query.metrics().unwrap().error_rows(), 1);

        session.sql("SET vql.on_error='fail'").unwrap();
        let error = session
            .sql("SELECT TO_JPEG(image) FROM photos")
            .unwrap()
            .collect()
            .unwrap_err();
        assert!(error.to_string().contains("failed to decode image"));
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
                 CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://person';",
                photos.display()
            ))
            .unwrap();
        let statement = session
            .sql("SELECT COUNT_OBJECTS(DETECT_OBJECTS('detector', image), 'person', 0.6) AS people FROM photos")
            .unwrap();
        let Statement::Query(query) = &statement else {
            panic!("model SELECT must produce a query");
        };
        let logical = query.dataframe.logical_plan().display_indent().to_string();
        assert!(logical.contains("InferenceNode"));
        assert!(!logical.contains("detect_objects("));
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
        assert_eq!(statement.metrics().unwrap().inference_rows(), 1);

        let deduplicated = session
            .sql(
                "SELECT COUNT_OBJECTS(DETECT_OBJECTS('detector', image), 'person', 0.6), \
                        COUNT_OBJECTS(DETECT_OBJECTS('detector', image), 'person', 0.8) FROM photos",
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
        assert_eq!(deduplicated.metrics().unwrap().inference_rows(), 1);

        session
            .sql(
                "CREATE MODEL remote TYPE OBJECT_DETECTION \
                 FROM 'endpoint://http://127.0.0.1:9' \
                 WITH (runtime.kind='triton', runtime.protocol='kserve_v2_http', \
                       runtime.model_name='remote')",
            )
            .unwrap();
        let volatile = session
            .sql(
                "SELECT DETECT_OBJECTS('remote', image) AS first, \
                        DETECT_OBJECTS('remote', image) AS second FROM photos",
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
        use std::io::{Read, Write};
        use std::net::TcpListener;
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
            accepted_tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(500));
            let body =
                r#"{"outputs":[{"name":"output0","shape":[1,0,6],"datatype":"FP32","data":[]}]}"#;
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
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
                 FROM 'endpoint://http://{address}'
                 WITH (runtime.kind='triton', runtime.protocol='kserve_v2_http',
                       runtime.model_name='remote');",
                photos.display()
            ))
            .unwrap();
        let Statement::Query(query) = session
            .sql("SELECT DETECT_OBJECTS('remote', image) FROM photos")
            .unwrap()
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
    fn console_sink_uses_sink_plan_nodes() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session.sql("CREATE SINK terminal TYPE console").unwrap();
        let insert = session
            .sql("INSERT INTO terminal SELECT 42 AS answer")
            .unwrap();
        let Statement::Query(query) = &insert else {
            panic!("INSERT INTO console sink must produce a foreground query");
        };
        assert!(
            query
                .dataframe
                .logical_plan()
                .display_indent()
                .to_string()
                .contains("SinkWrite")
        );
        let physical = engine
            .inner
            .runtime
            .block_on(query.dataframe.create_physical_plan())
            .unwrap();
        assert!(
            datafusion::physical_plan::displayable(physical.as_ref())
                .indent(true)
                .to_string()
                .contains("SinkExec")
        );
        insert.collect().unwrap();
    }
}
