use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::catalog::{
    FunctionImplementation, ModelDef, ObjectKind, SinkDef, StreamDef, TableDef, TableProviderKind,
};
use crate::connectors::images::{ImagesTableProvider, images_schema};
use crate::connectors::rtsp::{rtsp_schema, start_rtsp_source};
use crate::connectors::videos::{VideosTableProvider, videos_schema};
use crate::functions::{VqlFunctionFactory, materialize_batch_images};
use crate::media::{MediaCounters, MediaRuntime};
use crate::models::{ModelCounters, semantic_fingerprint};
use crate::planner::{
    bind_stream_epoch, bind_tumble_output, context_for_function_ddl, context_for_snapshot,
    normalize_function_ddl, plan_statement, wrap_console_sink,
};
use crate::sql::{CreateModel, CreateStream, CreateTable, ShowKind, VqlStatement, parse_statement};
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
    source_generation: AtomicU64,
    source_reconnects: AtomicU64,
    event_time_fallbacks: AtomicU64,
    source_dropped_frames: AtomicU64,
    watermark_ms: AtomicI64,
    has_watermark: AtomicBool,
    late_rows: AtomicU64,
    window_state_bytes: AtomicU64,
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

    pub fn source_generation(&self) -> u64 {
        self.source_generation.load(Ordering::Relaxed)
    }

    pub fn source_reconnects(&self) -> u64 {
        self.source_reconnects.load(Ordering::Relaxed)
    }

    pub fn event_time_fallbacks(&self) -> u64 {
        self.event_time_fallbacks.load(Ordering::Relaxed)
    }

    pub fn source_dropped_frames(&self) -> u64 {
        self.source_dropped_frames.load(Ordering::Relaxed)
    }

    pub fn watermark_ms(&self) -> Option<i64> {
        self.has_watermark
            .load(Ordering::Relaxed)
            .then(|| self.watermark_ms.load(Ordering::Relaxed))
    }

    pub fn late_rows(&self) -> u64 {
        self.late_rows.load(Ordering::Relaxed)
    }

    pub fn window_state_bytes(&self) -> u64 {
        self.window_state_bytes.load(Ordering::Relaxed)
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
    catalog: Arc<crate::catalog::CatalogStore>,
    fail_on_error: Arc<AtomicBool>,
    model_start: ModelCounters,
    model_sample_start: usize,
    streaming: Option<StreamingQuery>,
}

#[derive(Debug, Clone)]
struct StreamingQuery {
    name: String,
    definition: StreamDef,
    skip: usize,
    fetch: Option<usize>,
    tumble: Option<crate::stream::TumblePlan>,
}

#[derive(Debug, Clone)]
struct QueryResources {
    runtime: Arc<tokio::runtime::Runtime>,
    active_query: Arc<Mutex<Option<CancellationToken>>>,
    media: Arc<MediaRuntime>,
    models: Arc<crate::models::ModelRuntime>,
    catalog: Arc<crate::catalog::CatalogStore>,
    fail_on_error: Arc<AtomicBool>,
}

struct ActiveQueryGuard(Arc<Mutex<Option<CancellationToken>>>);

impl Drop for ActiveQueryGuard {
    fn drop(&mut self) {
        if let Ok(mut active) = self.0.lock() {
            *active = None;
        }
    }
}

struct WindowStateMetricGuard(Arc<QueryMetrics>);

impl Drop for WindowStateMetricGuard {
    fn drop(&mut self) {
        self.0.window_state_bytes.store(0, Ordering::Relaxed);
    }
}

impl QueryHandle {
    fn new(
        dataframe: DataFrame,
        cancellation: CancellationToken,
        resources: QueryResources,
        streaming: Option<StreamingQuery>,
    ) -> Self {
        let output_schema = restamp_schema(dataframe.schema().inner());
        let media_start = resources.media.counters();
        let model_start = resources.models.counters();
        let model_sample_start = resources.models.sample_count();
        Self {
            dataframe,
            runtime: resources.runtime,
            cancellation,
            active_query: resources.active_query,
            output_schema,
            metrics: Arc::new(QueryMetrics::default()),
            collected: Arc::new(Mutex::new(None)),
            media: resources.media,
            media_start,
            models: resources.models,
            catalog: resources.catalog,
            fail_on_error: resources.fail_on_error,
            model_start,
            model_sample_start,
            streaming,
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
        if self.streaming.is_some() {
            return self.stream_rtsp();
        }
        self.set_active()?;
        let active_guard = ActiveQueryGuard(Arc::clone(&self.active_query));
        let input = self
            .runtime
            .block_on(self.dataframe.clone().execute_stream())?;
        let output_schema = Arc::clone(&self.output_schema);
        let stream_schema = Arc::clone(&output_schema);
        let cancellation = self.cancellation.clone();
        let metrics = Arc::clone(&self.metrics);
        let stream = async_stream::try_stream! {
            let _active_guard = active_guard;
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
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }

    fn stream_rtsp(&self) -> Result<SendableRecordBatchStream> {
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
        self.set_active()?;
        let mut source = match start_rtsp_source(
            streaming.definition,
            Arc::clone(&self.media),
            Arc::clone(&self.fail_on_error),
            self.cancellation.clone(),
        ) {
            Ok(source) => source,
            Err(error) => {
                if let Ok(mut active) = self.active_query.lock() {
                    *active = None;
                }
                return Err(error);
            }
        };
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
        let output_schema = Arc::clone(&self.output_schema);
        let stream_schema = Arc::clone(&output_schema);
        let cancellation = self.cancellation.clone();
        let active_query = Arc::clone(&self.active_query);
        let metrics = Arc::clone(&self.metrics);
        let window_state_metric_guard = tumble_state
            .as_ref()
            .map(|_| WindowStateMetricGuard(Arc::clone(&metrics)));
        let catalog = Arc::clone(&self.catalog);
        let media = Arc::clone(&self.media);
        let fail_on_error = Arc::clone(&self.fail_on_error);
        let models = Arc::clone(&self.models);
        let model_start = self.model_start;
        let active_guard = ActiveQueryGuard(active_query);
        let stream = async_stream::try_stream! {
            let _active_guard = active_guard;
            let _window_state_metric_guard = window_state_metric_guard;
            'epochs: loop {
                let next = tokio::select! {
                    _ = cancellation.cancelled() => Ok(None),
                    next = source.next() => next,
                };
                let next = next.map_err(|error| {
                    datafusion::error::DataFusionError::External(Box::new(error))
                })?;
                if cancellation.is_cancelled() {
                    Err(datafusion::error::DataFusionError::Execution(
                        "[VQL:QUERY_CANCELLED] query cancelled".to_owned(),
                    ))?;
                }
                let Some(epoch) = next else { break; };
                let input_rows = epoch.batches.iter().map(RecordBatch::num_rows).sum::<usize>();
                metrics.input_rows.fetch_add(input_rows as u64, Ordering::Relaxed);
                metrics.source_generation.store(
                    epoch.source_progress.generation,
                    Ordering::Relaxed,
                );
                metrics.decode_frames.store(
                    epoch.source_progress.decoded_frames,
                    Ordering::Relaxed,
                );
                metrics.source_reconnects.store(
                    epoch.source_progress.reconnects,
                    Ordering::Relaxed,
                );
                metrics.event_time_fallbacks.store(
                    epoch.source_progress.event_time_fallbacks,
                    Ordering::Relaxed,
                );
                metrics.source_dropped_frames.store(
                    epoch.source_progress.dropped_frames,
                    Ordering::Relaxed,
                );
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
                    metrics.late_rows.fetch_add(window_output.late_rows, Ordering::Relaxed);
                    metrics.window_state_bytes.store(
                        window_output.state_bytes as u64,
                        Ordering::Relaxed,
                    );
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
                    let mut batch = batch?;
                    if skip_remaining >= batch.num_rows() {
                        skip_remaining -= batch.num_rows();
                        continue;
                    }
                    if skip_remaining > 0 {
                        batch = batch.slice(skip_remaining, batch.num_rows() - skip_remaining);
                        skip_remaining = 0;
                    }
                    if let Some(remaining) = fetch_remaining
                        && batch.num_rows() > remaining
                    {
                        batch = batch.slice(0, remaining);
                    }
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let batch = materialize_batch_images(
                        Arc::clone(&catalog),
                        Arc::clone(&media),
                        batch,
                        fail_on_error.load(Ordering::Relaxed),
                    )
                    .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?;
                    metrics.output_rows.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                    if let Some(remaining) = fetch_remaining.as_mut() {
                        *remaining = remaining.saturating_sub(batch.num_rows());
                    }
                    yield RecordBatch::try_new(
                        Arc::clone(&output_schema),
                        batch.columns().to_vec(),
                    )?;
                    if fetch_remaining == Some(0) {
                        if let Some(watermark_ms) = epoch.watermark_ms {
                            metrics.watermark_ms.store(watermark_ms, Ordering::Relaxed);
                            metrics.has_watermark.store(true, Ordering::Relaxed);
                        }
                        break 'epochs;
                    }
                }
                if let Some(watermark_ms) = epoch.watermark_ms {
                    metrics.watermark_ms.store(watermark_ms, Ordering::Relaxed);
                    metrics.has_watermark.store(true, Ordering::Relaxed);
                }
                tracing::debug!(
                    epoch_id = epoch.epoch_id,
                    watermark_ms = epoch.watermark_ms,
                    sampled_frames = epoch.source_progress.sampled_frames,
                    "completed RTSP epoch"
                );
                let model_end = models.counters();
                metrics.inference_rows.store(
                    model_end
                        .inference_rows
                        .saturating_sub(model_start.inference_rows),
                    Ordering::Relaxed,
                );
                metrics.inference_batches.store(
                    model_end
                        .inference_batches
                        .saturating_sub(model_start.inference_batches),
                    Ordering::Relaxed,
                );
                metrics.error_rows.store(
                    model_end
                        .inference_errors
                        .saturating_sub(model_start.inference_errors),
                    Ordering::Relaxed,
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
        if self.streaming.is_none() {
            self.metrics.decode_frames.store(
                media_end
                    .decoded_frames
                    .saturating_sub(self.media_start.decoded_frames),
                Ordering::Relaxed,
            );
        }
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
        if self.streaming.is_none() {
            self.metrics.input_rows.store(
                self.metrics.output_rows.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
        }
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

    pub fn is_unbounded(&self) -> bool {
        self.streaming
            .as_ref()
            .is_some_and(|streaming| streaming.fetch.is_none())
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
            VqlStatement::CreateStream(create) => self.create_stream(create).map(Statement::Ddl),
            VqlStatement::CreateModel(create) => self.create_model(create).map(Statement::Ddl),
            VqlStatement::ResolveModel { name } => self.resolve_model(&name).map(Statement::Ddl),
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
        let planned = self.engine.inner.runtime.block_on(plan_statement(
            &context,
            &snapshot,
            sql,
            Arc::clone(&self.engine.inner.models),
            Arc::clone(&self.fail_on_error),
            cancellation.clone(),
        ))?;
        let streaming = planned.stream_name.as_ref().map(|name| {
            let definition = snapshot
                .stream(name)
                .expect("planned stream exists in the query snapshot")
                .definition
                .clone();
            StreamingQuery {
                name: name.clone(),
                definition,
                skip: planned.stream_skip,
                fetch: planned.stream_fetch,
                tumble: planned.tumble.clone(),
            }
        });
        Ok(QueryHandle::new(
            planned.dataframe,
            cancellation,
            QueryResources {
                runtime: Arc::clone(&self.engine.inner.runtime),
                active_query: Arc::clone(&self.active_query),
                media: Arc::clone(&self.engine.inner.media),
                models: Arc::clone(&self.engine.inner.models),
                catalog: Arc::clone(&self.engine.inner.catalog),
                fail_on_error: Arc::clone(&self.fail_on_error),
            },
            streaming,
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

    fn create_stream(&self, create: CreateStream) -> Result<DdlResult> {
        if !self.engine.inner.media.rtsp_available() {
            return Err(VqlError::new(
                ErrorCode::FeatureNotAvailable,
                "CREATE STREAM requires the ffmpeg-native feature",
            ));
        }
        let endpoint = normalize_rtsp_endpoint(&create.endpoint)?;
        let definition = StreamDef {
            name: create.name.to_ascii_lowercase(),
            endpoint,
            fps: create.fps,
            event_time: create.event_time,
            watermark_delay_ms: create.watermark_delay_ms,
            transport: create.transport,
        };
        let revision = self.engine.inner.catalog.create_stream(&definition)?;
        Ok(message_result(format!(
            "created stream '{}' at revision {revision}",
            definition.name
        )))
    }

    fn drop_table(&self, name: &str) -> Result<DdlResult> {
        let revision = self.engine.inner.catalog.drop_table(name)?;
        Ok(message_result(format!(
            "dropped table '{name}' at revision {revision}"
        )))
    }

    fn create_model(&self, create: CreateModel) -> Result<DdlResult> {
        let name = create.name.to_ascii_lowercase();
        let declaration_fingerprint = semantic_fingerprint(&(
            create.model_type,
            &create.source,
            &create.runtime_kind,
            &create.options,
        ));
        let model = ModelDef {
            name,
            model_type: create.model_type,
            source: create.source,
            runtime_kind: create.runtime_kind,
            options: create.options,
            declaration_fingerprint,
            resolved: None,
        };
        self.engine.inner.pipelines.validate_declaration(&model)?;
        let revision = self.engine.inner.catalog.create_model(&model)?;
        Ok(message_result(format!(
            "created model '{}' at revision {revision}",
            model.name
        )))
    }

    fn resolve_model(&self, name: &str) -> Result<DdlResult> {
        let name = name.to_ascii_lowercase();
        let snapshot = self.engine.inner.catalog.snapshot()?;
        let model_object = snapshot.model(&name).ok_or_else(|| {
            VqlError::new(
                ErrorCode::NotFound,
                format!("model '{name}' does not exist"),
            )
        })?;
        let expected_revision = model_object.revision;
        let mut model = model_object.definition.clone();
        let cancellation = CancellationToken::new();
        {
            let mut active = self.active_query.lock().map_err(|_| {
                VqlError::new(ErrorCode::Internal, "active query lock was poisoned")
            })?;
            *active = Some(cancellation.clone());
        }
        let _active_guard = ActiveQueryGuard(Arc::clone(&self.active_query));
        let resolved =
            self.engine
                .inner
                .runtime
                .block_on(self.engine.inner.pipelines.resolve_model(
                    &model,
                    self.engine.inner.config.model_cache_dir(),
                    cancellation,
                ))?;
        model.resolved = Some(resolved);
        let revision = self
            .engine
            .inner
            .catalog
            .update_model(&model, expected_revision)?;
        self.engine.inner.models.evict_stale()?;
        Ok(message_result(format!(
            "resolved model '{}' at revision {revision}",
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
            ShowKind::Streams => self.engine.inner.catalog.drop_stream(name)?,
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
        if kind == ShowKind::Streams {
            return self.show_streams();
        }
        if kind == ShowKind::Models {
            return self.show_models();
        }
        let snapshot = self.engine.inner.catalog.snapshot()?;
        let rows = match kind {
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
            ShowKind::Tables | ShowKind::Streams | ShowKind::Models => unreachable!(),
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

    fn show_streams(&self) -> Result<DdlResult> {
        let snapshot = self.engine.inner.catalog.snapshot()?;
        let streams = snapshot.streams().collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(vec![
            Field::new("stream_name", DataType::Utf8, false),
            Field::new("connector", DataType::Utf8, false),
            Field::new("endpoint", DataType::Utf8, false),
            Field::new("fps", DataType::Float64, false),
            Field::new("event_time", DataType::Utf8, false),
            Field::new("watermark_ms", DataType::Int64, false),
            Field::new("transport", DataType::Utf8, false),
            Field::new("revision", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    streams.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(vec!["RTSP"; streams.len()])),
                Arc::new(StringArray::from(
                    streams
                        .iter()
                        .map(|(_, stream)| stream.definition.endpoint.as_str())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(arrow::array::Float64Array::from(
                    streams
                        .iter()
                        .map(|(_, stream)| stream.definition.fps)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    streams
                        .iter()
                        .map(|(_, stream)| match stream.definition.event_time {
                            crate::catalog::EventTimePolicy::CaptureTime => "capture_time",
                            crate::catalog::EventTimePolicy::IngestTime => "ingest_time",
                        })
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    streams
                        .iter()
                        .map(|(_, stream)| stream.definition.watermark_delay_ms)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    streams
                        .iter()
                        .map(|(_, stream)| match stream.definition.transport {
                            crate::catalog::RtspTransport::Tcp => "tcp",
                            crate::catalog::RtspTransport::Udp => "udp",
                        })
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    streams
                        .iter()
                        .map(|(_, stream)| stream.revision)
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .map_err(|error| {
            VqlError::new(
                ErrorCode::Execution,
                "failed to build stream catalog result",
            )
            .with_source(error)
        })?;
        Ok(DdlResult {
            message: format!("{} stream(s)", batch.num_rows()),
            batches: vec![batch],
        })
    }

    fn show_models(&self) -> Result<DdlResult> {
        let snapshot = self.engine.inner.catalog.snapshot()?;
        let models = snapshot.models().collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(vec![
            Field::new("model_name", DataType::Utf8, false),
            Field::new("type", DataType::Utf8, false),
            Field::new("runtime", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("revision", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    models.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(vec!["OBJECT_DETECTION"; models.len()])),
                Arc::new(StringArray::from(
                    models
                        .iter()
                        .map(|(_, model)| {
                            model
                                .definition
                                .runtime_kind
                                .to_ascii_uppercase()
                                .replace('-', "_")
                        })
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    models
                        .iter()
                        .map(|(_, model)| {
                            if model.definition.resolved.is_some() {
                                "RESOLVED"
                            } else {
                                "UNRESOLVED"
                            }
                        })
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    models
                        .iter()
                        .map(|(_, model)| model.revision)
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

    fn describe(&self, name: &str) -> Result<DdlResult> {
        let snapshot = self.engine.inner.catalog.snapshot()?;
        let relation_schema = snapshot
            .table(name)
            .map(|table| Arc::clone(&table.schema))
            .or_else(|| snapshot.stream(name).map(|_| rtsp_schema()))
            .ok_or_else(|| {
                VqlError::new(
                    ErrorCode::NotFound,
                    format!("relation '{name}' does not exist"),
                )
            })?;
        let fields = relation_schema.fields();
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
            message: format!("relation '{name}'"),
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

        session
            .run_script(&format!(
                "CREATE TABLE photos USING IMAGES LOCATION '{}';
                 CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://person' USING ONNX_RUNTIME;
                 CREATE MODEL remote TYPE OBJECT_DETECTION FROM 'http://127.0.0.1:9' \
                   USING TRITON_INFERENCE_SERVER WITH (model='remote');",
                photos.display()
            ))
            .unwrap();

        let snapshot = engine.inner.catalog.snapshot().unwrap();
        assert!(
            snapshot
                .model("detector")
                .unwrap()
                .definition
                .resolved
                .is_none()
        );
        assert!(
            snapshot
                .model("remote")
                .unwrap()
                .definition
                .resolved
                .is_none()
        );
        let shown = session.sql("SHOW MODELS").unwrap().collect().unwrap();
        assert_eq!(shown[0].schema().field(3).name(), "status");
        let statuses = shown[0]
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(statuses.value(0), "UNRESOLVED");
        assert_eq!(statuses.value(1), "UNRESOLVED");
        let error = session
            .sql("SELECT IMAGE_DETECTION('detector', image) FROM photos")
            .unwrap_err();
        assert!(error.message.contains("RESOLVE MODEL detector"));

        session.sql("RESOLVE MODEL detector").unwrap();
        assert!(
            engine
                .inner
                .catalog
                .snapshot()
                .unwrap()
                .model("detector")
                .unwrap()
                .definition
                .resolved
                .is_some()
        );
        let shown = session.sql("SHOW MODELS").unwrap().collect().unwrap();
        let statuses = shown[0]
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(statuses.value(0), "RESOLVED");
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
            .sql("SELECT IMAGE_DETECTION('detector', image) FROM photos")
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
            .sql("SELECT IMAGE_DETECTION('detector', image) FROM photos")
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
                     WITH (input={name='pixels', width=320, height=192}, \
                           output={name='detections', labels=['person']})",
                )
                .unwrap();
        }

        let engine = Engine::new(EngineConfig::new(&catalog)).unwrap();
        let snapshot = engine.inner.catalog.snapshot().unwrap();
        let model = &snapshot.model("detector").unwrap().definition;

        assert_eq!(model.runtime_kind, "onnx-runtime");
        assert_eq!(model.options["input"]["width"], serde_json::json!(320));
        assert_eq!(
            model.options["output"]["labels"],
            serde_json::json!(["person"])
        );
        assert!(model.resolved.is_none());
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
    fn typed_inference_function_names_are_reserved() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();

        for name in [
            "IMAGE_DETECTION",
            "IMAGE_CLASSIFICATION",
            "IMAGE_EMBEDDING",
            "TEXT_EMBEDDING",
            "TEXT_GENERATION",
        ] {
            let error = session
                .sql(&format!(
                    "CREATE FUNCTION {name}(BIGINT) RETURNS BIGINT RETURN $1"
                ))
                .unwrap_err();

            assert_eq!(error.code, ErrorCode::InvalidOption);
            assert_eq!(
                error.message,
                format!(
                    "function name '{}' is reserved for built-in typed inference",
                    name.to_ascii_lowercase()
                )
            );
        }

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
                 CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://person' USING ONNX_RUNTIME;
                 RESOLVE MODEL detector;
                 CREATE FUNCTION detect_people(IMAGE)
                 RETURN IMAGE_DETECTION(
                   'detector', $1,
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
                 FROM photos AS f, UNNEST(IMAGE_DETECTION('detector', f.image)) AS u(det)",
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
            .sql("SELECT CARDINALITY(IMAGE_DETECTION('detector', image, classes => ['person'], min_confidence => 0.6)) AS people FROM photos")
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
        assert_eq!(statement.metrics().unwrap().inference_rows(), 1);

        let deduplicated = session
            .sql(
                "SELECT CARDINALITY(IMAGE_DETECTION('detector', image, classes => ['person'], min_confidence => 0.6)) AS first, \
                        CARDINALITY(IMAGE_DETECTION('detector', image, classes => ['person'], min_confidence => 0.6)) AS second FROM photos",
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

        let Some((address, server)) = serve_triton_metadata_once() else {
            return;
        };
        session
            .run_script(&format!(
                "CREATE MODEL remote TYPE OBJECT_DETECTION \
                 FROM 'http://{address}' USING TRITON_INFERENCE_SERVER \
                 WITH (model='remote');
                 RESOLVE MODEL remote;"
            ))
            .unwrap();
        server.join().unwrap();
        let volatile = session
            .sql(
                "SELECT IMAGE_DETECTION('remote', image) AS first, \
                        IMAGE_DETECTION('remote', image) AS second FROM photos",
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
            let body = r#"{"outputs":[{"name":"detections","shape":[1],"datatype":"BYTES","data":["[]"]}]}"#;
            write_http_response(&mut stream, body);
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
                 FROM 'http://{address}' USING TRITON_INFERENCE_SERVER
                 WITH (model='remote');
                 RESOLVE MODEL remote;",
                photos.display()
            ))
            .unwrap();
        let Statement::Query(query) = session
            .sql("SELECT IMAGE_DETECTION('remote', image) FROM photos")
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
    fn inference_row_failure_is_null_by_default_and_fails_in_strict_mode() {
        let temp = tempdir().unwrap();
        let photos = temp.path().join("photos");
        std::fs::create_dir(&photos).unwrap();
        std::fs::write(photos.join("broken.png"), b"not an image").unwrap();
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
                "CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'http://{address}' \
                 USING TRITON_INFERENCE_SERVER WITH (model='detector');
                 RESOLVE MODEL detector;"
            ))
            .unwrap();
        server.join().unwrap();

        let query = session
            .sql(
                "SELECT CARDINALITY(IMAGE_DETECTION('detector', image, classes => ['person'], min_confidence => 0.5)) AS people \
                 FROM photos",
            )
            .unwrap();
        let batches = query.collect().unwrap();
        assert!(
            batches[0].column(0).is_null(0),
            "a row that fails inference must yield NULL"
        );
        assert_eq!(query.metrics().unwrap().error_rows(), 1);

        session.sql("SET vql.on_error='fail'").unwrap();
        let error = session
            .sql("SELECT IMAGE_DETECTION('detector', image) FROM photos")
            .unwrap()
            .collect()
            .unwrap_err();

        assert_eq!(error.code, ErrorCode::Execution);
        assert!(
            error
                .to_string()
                .contains("failed to decode model IMAGE input")
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
                ("revision".to_owned(), DataType::Int64, false),
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
        let revisions = shown[0]
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(table_names.value(0), "photos");
        assert_eq!(providers.value(0), "IMAGES");
        assert_eq!(
            locations.value(0),
            photos.canonicalize().unwrap().to_string_lossy()
        );
        assert_eq!(revisions.value(0), 1);

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
    fn stream_lifecycle_and_streaming_planning_are_available_without_connecting() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .sql(
                "CREATE STREAM entrance FROM 'rtsp://camera.example:554/live' WITH (\
                 fps=5, event_time='capture_time', watermark=INTERVAL '2' SECOND, transport='tcp')",
            )
            .unwrap();

        let shown = session.sql("SHOW STREAMS").unwrap().collect().unwrap();
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
        assert!(
            session
                .sql("EXPLAIN SELECT frame_id FROM entrance")
                .unwrap()
                .collect()
                .is_ok()
        );
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

        session.sql("DROP STREAM entrance").unwrap();
        assert_eq!(
            session.sql("SHOW STREAMS").unwrap().collect().unwrap()[0].num_rows(),
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
                .sql(&format!("CREATE STREAM cam FROM '{endpoint}'"))
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
        engine
            .inner
            .catalog
            .create_stream(&StreamDef {
                name: "local_stream".to_owned(),
                endpoint: video.to_string_lossy().into_owned(),
                fps: 5.0,
                event_time: crate::catalog::EventTimePolicy::CaptureTime,
                watermark_delay_ms: 100,
                transport: crate::catalog::RtspTransport::Tcp,
            })
            .unwrap();
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
        let metrics = statement.metrics().unwrap();
        assert!(metrics.decode_frames() > 0);
        assert!(metrics.source_generation() > 0);
        assert!(metrics.watermark_ms().is_some());
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
        engine
            .inner
            .catalog
            .create_stream(&StreamDef {
                name: "window_stream".to_owned(),
                endpoint: video.to_string_lossy().into_owned(),
                fps: 5.0,
                event_time: crate::catalog::EventTimePolicy::CaptureTime,
                watermark_delay_ms: 100,
                transport: crate::catalog::RtspTransport::Tcp,
            })
            .unwrap();
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
                assert!(counts.value(row) > 0);
                assert!(minimums.value(row) <= maximums.value(row));
                assert_eq!(
                    sums.value(row),
                    counts.value(row) * (minimums.value(row) + maximums.value(row)) / 2
                );
                assert_eq!(
                    averages.value(row),
                    sums.value(row) as f64 / counts.value(row) as f64
                );
            }
        }
        let metrics = statement.metrics().unwrap();
        assert_eq!(metrics.output_rows(), 3);
        assert_eq!(metrics.late_rows(), 0);
        assert_eq!(metrics.window_state_bytes(), 0);
        assert!(metrics.watermark_ms().is_some());
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
        engine
            .inner
            .catalog
            .create_stream(&StreamDef {
                name: "people_stream".to_owned(),
                endpoint: video.to_string_lossy().into_owned(),
                fps: 5.0,
                event_time: crate::catalog::EventTimePolicy::CaptureTime,
                watermark_delay_ms: 100,
                transport: crate::catalog::RtspTransport::Tcp,
            })
            .unwrap();
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
                   SELECT frame_id, CARDINALITY(IMAGE_DETECTION(
                     'detector', frame, classes => ['person'], min_confidence => 0.5
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
        let metrics = statement.metrics().unwrap();
        assert!(metrics.decode_frames() >= 3);
        assert_eq!(metrics.inference_rows(), 3);
        assert!(metrics.watermark_ms().is_some());

        let windowed = session
            .sql(
                "WITH detected AS (
                   SELECT ts, CARDINALITY(IMAGE_DETECTION(
                     'detector', frame, classes => ['person'], min_confidence => 0.5
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
        assert!(windowed.metrics().unwrap().inference_rows() > 0);
    }
}
