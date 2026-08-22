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
    FunctionImplementation, ModelDef, ObjectKind, RtspTableConfig, TableDef, TableProvider,
};
use crate::connectors::images::{ImagesTableProvider, images_schema};
use crate::connectors::rtsp::{rtsp_schema, start_rtsp_source};
use crate::connectors::videos::{VideosTableProvider, videos_schema};
use crate::functions::{VqlFunctionFactory, materialize_batch_images};
use crate::media::MediaRuntime;
use crate::models::semantic_fingerprint;
use crate::planner::{
    SinkTarget, bind_stream_epoch, bind_tumble_output, context_for_function_ddl,
    context_for_snapshot, normalize_function_ddl, plan_statement, wrap_sink,
};
use crate::resources::{QueryBudget, QueryReservation, ResourceMetrics, SessionMemoryPool};
use crate::sql::{
    CreateModel, CreateTable, ShowKind, TableColumn, VqlStatement, parse_statement, render_create,
    render_create_table,
};
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
        let memory_pool = Arc::new(SessionMemoryPool::new(
            self.engine.inner.config.session_memory_limit_bytes(),
        ));
        Ok(Session {
            engine: self.engine,
            active_query: Arc::new(Mutex::new(None)),
            fail_on_error: Arc::new(AtomicBool::new(false)),
            python_udf_host: self.python_udf_host,
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
    memory_pool: Arc<SessionMemoryPool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryInterruptAction {
    NoActiveQuery,
    GracefulStopRequested,
    ImmediateCancellationRequested,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameDropReason {
    SourceOverrun,
    ResourceBudget,
    DecodeError,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DroppedFrameRange {
    pub reason: FrameDropReason,
    pub count: u64,
    pub first_event_time_ms: Option<i64>,
    pub last_event_time_ms: Option<i64>,
}

const MAX_PERCENTILE_SAMPLES: usize = 2_048;
const MAX_BATCH_HISTOGRAM_BUCKET: usize = 64;

#[derive(Debug, Default)]
struct PercentileSamples {
    values: Vec<u64>,
    next: usize,
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
    inference_latencies_micros: Mutex<PercentileSamples>,
    model_queue_p50_micros: AtomicU64,
    model_queue_p95_micros: AtomicU64,
    model_queue_samples_micros: Mutex<PercentileSamples>,
    model_service_p50_micros: AtomicU64,
    model_service_p95_micros: AtomicU64,
    model_service_samples_micros: Mutex<PercentileSamples>,
    source_generation: AtomicU64,
    source_reconnects: AtomicU64,
    event_time_fallbacks: AtomicU64,
    source_dropped_frames: AtomicU64,
    sampled_frames: AtomicU64,
    first_sampled_event_time_ms: AtomicI64,
    last_sampled_event_time_ms: AtomicI64,
    has_sampled_event_range: AtomicBool,
    source_input_bytes: AtomicU64,
    observation_micros: AtomicU64,
    source_gap_duration_ms: AtomicU64,
    dropped_frame_ranges: Mutex<Vec<DroppedFrameRange>>,
    watermark_ms: AtomicI64,
    has_watermark: AtomicBool,
    late_rows: AtomicU64,
    window_state_bytes: AtomicU64,
    sink_retries: AtomicU64,
    epoch_p50_micros: AtomicU64,
    epoch_p95_micros: AtomicU64,
    epoch_samples_micros: Mutex<PercentileSamples>,
    e2e_p50_micros: AtomicU64,
    e2e_p95_micros: AtomicU64,
    e2e_samples_micros: Mutex<PercentileSamples>,
    pub(crate) resources: Arc<ResourceMetrics>,
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

    pub fn model_queue_p50_ms(&self) -> f64 {
        self.model_queue_p50_micros.load(Ordering::Relaxed) as f64 / 1_000.0
    }

    pub fn model_queue_p95_ms(&self) -> f64 {
        self.model_queue_p95_micros.load(Ordering::Relaxed) as f64 / 1_000.0
    }

    pub fn model_service_p50_ms(&self) -> f64 {
        self.model_service_p50_micros.load(Ordering::Relaxed) as f64 / 1_000.0
    }

    pub fn model_service_p95_ms(&self) -> f64 {
        self.model_service_p95_micros.load(Ordering::Relaxed) as f64 / 1_000.0
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

    pub fn sampled_fps(&self) -> f64 {
        if !self.has_sampled_event_range.load(Ordering::Relaxed) {
            return 0.0;
        }
        let duration_ms = self
            .last_sampled_event_time_ms
            .load(Ordering::Relaxed)
            .saturating_sub(self.first_sampled_event_time_ms.load(Ordering::Relaxed));
        if duration_ms <= 0 {
            return 0.0;
        }
        self.sampled_frames().saturating_sub(1) as f64 * 1_000.0 / duration_ms as f64
    }

    pub fn sampled_frames(&self) -> u64 {
        self.sampled_frames.load(Ordering::Relaxed)
    }

    pub fn source_input_bytes(&self) -> u64 {
        self.source_input_bytes.load(Ordering::Relaxed)
    }

    pub fn input_bitrate_bps(&self) -> f64 {
        let elapsed = self.observation_micros.load(Ordering::Relaxed);
        if elapsed == 0 {
            return 0.0;
        }
        self.source_input_bytes() as f64 * 8_000_000.0 / elapsed as f64
    }

    pub fn source_gap_duration_ms(&self) -> u64 {
        self.source_gap_duration_ms.load(Ordering::Relaxed)
    }

    pub fn dropped_frame_ranges(&self) -> Vec<DroppedFrameRange> {
        self.dropped_frame_ranges
            .lock()
            .map(|ranges| ranges.clone())
            .unwrap_or_default()
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

    pub fn sink_retries(&self) -> u64 {
        self.sink_retries.load(Ordering::Relaxed)
    }

    pub fn epoch_p50_ms(&self) -> f64 {
        self.epoch_p50_micros.load(Ordering::Relaxed) as f64 / 1_000.0
    }

    pub fn epoch_p95_ms(&self) -> f64 {
        self.epoch_p95_micros.load(Ordering::Relaxed) as f64 / 1_000.0
    }

    pub fn end_to_end_p50_ms(&self) -> f64 {
        self.e2e_p50_micros.load(Ordering::Relaxed) as f64 / 1_000.0
    }

    pub fn end_to_end_p95_ms(&self) -> f64 {
        self.e2e_p95_micros.load(Ordering::Relaxed) as f64 / 1_000.0
    }

    pub fn resource_usage(&self, resource: crate::QueryResource) -> crate::ResourceUsage {
        self.resources.usage(resource)
    }

    pub fn total_resource_usage(&self) -> crate::ResourceUsage {
        self.resources.total_usage()
    }

    pub(crate) fn add_input_rows(&self, rows: usize) {
        self.input_rows.fetch_add(rows as u64, Ordering::Relaxed);
    }

    pub(crate) fn add_decode_frame(&self) {
        self.decode_frames.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn add_error_rows(&self, rows: usize) {
        self.error_rows.fetch_add(rows as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_inference(
        &self,
        rows: usize,
        latency_micros: u64,
        queue_wait_micros: u64,
        service_micros: u64,
    ) {
        self.inference_rows
            .fetch_add(rows as u64, Ordering::Relaxed);
        self.inference_batches.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut histogram) = self.batch_histogram.lock() {
            if histogram.is_empty() {
                histogram.resize(MAX_BATCH_HISTOGRAM_BUCKET + 1, 0);
            }
            let bucket = rows.min(MAX_BATCH_HISTOGRAM_BUCKET);
            histogram[bucket] = histogram[bucket].saturating_add(1);
        }
        record_percentiles(
            &self.inference_latencies_micros,
            &self.inference_p50_micros,
            &self.inference_p95_micros,
            latency_micros,
        );
        record_percentiles(
            &self.model_queue_samples_micros,
            &self.model_queue_p50_micros,
            &self.model_queue_p95_micros,
            queue_wait_micros,
        );
        record_percentiles(
            &self.model_service_samples_micros,
            &self.model_service_p50_micros,
            &self.model_service_p95_micros,
            service_micros,
        );
    }

    pub(crate) fn update_source_progress(
        &self,
        progress: &crate::connectors::rtsp::SourceProgress,
        observation_micros: u64,
    ) {
        self.sampled_frames
            .store(progress.sampled_frames, Ordering::Relaxed);
        if let (Some(first), Some(last)) = (
            progress.sampled_first_event_time_ms,
            progress.sampled_last_event_time_ms,
        ) {
            self.first_sampled_event_time_ms
                .store(first, Ordering::Relaxed);
            self.last_sampled_event_time_ms
                .store(last, Ordering::Relaxed);
            self.has_sampled_event_range.store(true, Ordering::Relaxed);
        }
        self.source_input_bytes
            .store(progress.input_bytes, Ordering::Relaxed);
        self.observation_micros
            .fetch_max(observation_micros, Ordering::Relaxed);
        self.source_gap_duration_ms
            .store(progress.gap_duration_ms, Ordering::Relaxed);
        if let Ok(mut ranges) = self.dropped_frame_ranges.lock() {
            *ranges = progress.dropped_ranges.clone();
        }
    }

    pub(crate) fn record_epoch(&self, epoch_micros: u64, end_to_end_micros: u64) {
        record_percentiles(
            &self.epoch_samples_micros,
            &self.epoch_p50_micros,
            &self.epoch_p95_micros,
            epoch_micros,
        );
        self.record_end_to_end(end_to_end_micros);
    }

    pub(crate) fn record_end_to_end(&self, end_to_end_micros: u64) {
        record_percentiles(
            &self.e2e_samples_micros,
            &self.e2e_p50_micros,
            &self.e2e_p95_micros,
            end_to_end_micros,
        );
    }
}

#[derive(Debug, Clone)]
pub struct QueryHandle {
    dataframe: DataFrame,
    runtime: Arc<tokio::runtime::Runtime>,
    cancellation: CancellationToken,
    graceful_stop: CancellationToken,
    active_query: Arc<Mutex<Option<ActiveQueryControl>>>,
    output_schema: SchemaRef,
    metrics: Arc<QueryMetrics>,
    budget: QueryBudget,
    output_reservation: Arc<Mutex<Option<QueryReservation>>>,
    execution_started_at: Arc<Mutex<Option<std::time::Instant>>>,
    collected: Arc<Mutex<Option<Vec<RecordBatch>>>>,
    media: Arc<MediaRuntime>,
    catalog: Arc<crate::catalog::CatalogStore>,
    fail_on_error: Arc<AtomicBool>,
    streaming: Option<StreamingQuery>,
    sink_target: Option<SinkTarget>,
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
    runtime: Arc<tokio::runtime::Runtime>,
    active_query: Arc<Mutex<Option<ActiveQueryControl>>>,
    media: Arc<MediaRuntime>,
    catalog: Arc<crate::catalog::CatalogStore>,
    fail_on_error: Arc<AtomicBool>,
    metrics: Arc<QueryMetrics>,
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

struct WindowStateMetricGuard(Arc<QueryMetrics>);

impl Drop for WindowStateMetricGuard {
    fn drop(&mut self) {
        self.0.window_state_bytes.store(0, Ordering::Relaxed);
    }
}

struct SinkCloseGuard {
    target: Option<SinkTarget>,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl SinkCloseGuard {
    fn new(target: SinkTarget, runtime: Arc<tokio::runtime::Runtime>) -> Self {
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
    ) -> Self {
        let output_schema = restamp_schema(dataframe.schema().inner());
        Self {
            dataframe,
            runtime: resources.runtime,
            cancellation,
            graceful_stop: CancellationToken::new(),
            active_query: resources.active_query,
            output_schema,
            metrics: resources.metrics,
            budget: resources.budget,
            output_reservation: Arc::new(Mutex::new(None)),
            execution_started_at: Arc::new(Mutex::new(None)),
            collected: Arc::new(Mutex::new(None)),
            media: resources.media,
            catalog: resources.catalog,
            fail_on_error: resources.fail_on_error,
            streaming,
            sink_target: None,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    pub fn stream(&self) -> Result<SendableRecordBatchStream> {
        self.stream_with_output_budget(true)
    }

    fn stream_with_output_budget(&self, reserve_output: bool) -> Result<SendableRecordBatchStream> {
        if let Some(batches) = self.cached_batches()? {
            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.output_schema),
                stream,
            )));
        }
        let execution_started_at = self.start_execution()?;
        self.set_active()?;
        let active_guard = ActiveQueryGuard(Arc::clone(&self.active_query));
        let input = if self.streaming.is_some() {
            self.stream_rtsp(execution_started_at, reserve_output)?
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
        let metrics = Arc::clone(&self.metrics);
        let budget = self.budget.clone();
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
                let _output_reservation = if reserve_output {
                    Some(budget
                        .reserve(crate::QueryResource::Arrow, batch.get_array_memory_size())
                        .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?)
                } else {
                    None
                };
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

    fn stream_rtsp(
        &self,
        query_started_at: std::time::Instant,
        reserve_output: bool,
    ) -> Result<SendableRecordBatchStream> {
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
        let metrics = Arc::clone(&self.metrics);
        let budget = self.budget.clone();
        let window_state_metric_guard = tumble_state
            .as_ref()
            .map(|_| WindowStateMetricGuard(Arc::clone(&metrics)));
        let catalog = Arc::clone(&self.catalog);
        let media = Arc::clone(&self.media);
        let fail_on_error = Arc::clone(&self.fail_on_error);
        let stream = async_stream::try_stream! {
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
                let epoch_started = std::time::Instant::now();
                let input_rows = epoch.batches.iter().map(RecordBatch::num_rows).sum::<usize>();
                metrics.add_input_rows(input_rows);
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
                metrics.update_source_progress(
                    &epoch.source_progress,
                    query_started_at.elapsed().as_micros() as u64,
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
                    metrics.output_rows.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                    yield RecordBatch::try_new(
                        Arc::clone(&output_schema),
                        batch.columns().to_vec(),
                    )?;
                    if fetch_remaining == Some(0) {
                        if let Some(watermark_ms) = epoch.watermark_ms {
                            metrics.watermark_ms.store(watermark_ms, Ordering::Relaxed);
                            metrics.has_watermark.store(true, Ordering::Relaxed);
                        }
                        metrics.record_epoch(
                            epoch_started.elapsed().as_micros() as u64,
                            epoch.admitted_at.elapsed().as_micros() as u64,
                        );
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
                metrics.record_epoch(
                    epoch_started.elapsed().as_micros() as u64,
                    epoch.admitted_at.elapsed().as_micros() as u64,
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
        if self.streaming.is_none() && self.metrics.input_rows() == 0 {
            self.metrics.input_rows.store(
                self.metrics.output_rows.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
        }
        if self.streaming.is_none() {
            self.metrics
                .record_end_to_end(self.execution_elapsed_micros()?);
        }
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
        let result = self.runtime.block_on(async {
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
        });
        if result.is_ok() && self.streaming.is_none() {
            self.metrics
                .record_end_to_end(self.execution_elapsed_micros()?);
        }
        result
    }

    fn start_execution(&self) -> Result<std::time::Instant> {
        let mut started_at = self.execution_started_at.lock().map_err(|_| {
            VqlError::new(ErrorCode::Internal, "query execution timer was poisoned")
        })?;
        Ok(*started_at.get_or_insert_with(std::time::Instant::now))
    }

    fn execution_elapsed_micros(&self) -> Result<u64> {
        Ok(self.start_execution()?.elapsed().as_micros() as u64)
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
    runtime: Arc<tokio::runtime::Runtime>,
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
    pub fn sql(&self, sql: &str) -> Result<Statement> {
        match parse_statement(sql)? {
            VqlStatement::CreateTable(create) => self.create_table(create).map(Statement::Ddl),
            VqlStatement::CreateModel(create) => self.create_model(create).map(Statement::Ddl),
            VqlStatement::ResolveModel { name } => self.resolve_model(&name).map(Statement::Ddl),
            VqlStatement::CreateFunction { sql } => self.create_function(&sql).map(Statement::Ddl),
            VqlStatement::Drop { kind, name } => self.drop_object(kind, &name).map(Statement::Ddl),
            VqlStatement::Show(kind) => self.show_objects(kind).map(Statement::Ddl),
            VqlStatement::ShowCreate { kind, name } => {
                self.show_create(kind, &name).map(Statement::Ddl)
            }
            VqlStatement::Describe { name } => self.describe(&name).map(Statement::Ddl),
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
        let metrics = Arc::new(QueryMetrics::default());
        let budget = QueryBudget::for_session(
            Arc::clone(&self.memory_pool),
            Arc::clone(&metrics.resources),
        );
        let context = context_for_snapshot(
            &snapshot,
            Arc::clone(&self.engine.inner.catalog),
            Arc::clone(&self.engine.inner.media),
            Arc::clone(&self.fail_on_error),
            self.python_udf_host.clone(),
            &budget,
            Arc::clone(&metrics),
        )?;
        let cancellation = CancellationToken::new();
        let planned = self.engine.inner.runtime.block_on(plan_statement(
            &context,
            &snapshot,
            sql,
            Arc::clone(&self.engine.inner.models),
            Arc::clone(&self.fail_on_error),
            cancellation.clone(),
            budget.clone(),
            Arc::clone(&metrics),
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
                metrics,
                budget,
            },
            streaming,
        ))
    }

    fn explain(&self, sql: &str) -> Result<QueryHandle> {
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
            return self.query(sql);
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
        let snapshot = self.engine.inner.catalog.snapshot()?;
        let table = snapshot.table(table_name).ok_or_else(|| {
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
        let mut handle = self.query(&format!("EXPLAIN {query}"))?;
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
        let revision = self
            .engine
            .inner
            .catalog
            .create_table(&definition, &schema)?;
        Ok(message_result(format!(
            "created table '{}' at revision {revision}",
            name
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
            *active = Some(ActiveQueryControl {
                cancellation: cancellation.clone(),
                graceful_stop: None,
            });
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

    fn insert_into_table(&self, sql: &str) -> Result<QueryHandle> {
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
        let snapshot = self.engine.inner.catalog.snapshot()?;
        let table = snapshot.table(table_name).ok_or_else(|| {
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
        let mut handle = self.query(query)?;
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
        Ok(handle)
    }

    fn drop_object(&self, kind: ShowKind, name: &str) -> Result<DdlResult> {
        let revision = match kind {
            ShowKind::Tables => return self.drop_table(name),
            ShowKind::Models => {
                let revision = self.engine.inner.catalog.drop_model(name)?;
                self.engine.inner.models.evict_stale()?;
                revision
            }
            ShowKind::Functions => self
                .engine
                .inner
                .catalog
                .drop_object(ObjectKind::Function, name)?,
        };
        Ok(message_result(format!(
            "dropped object '{name}' at revision {revision}"
        )))
    }

    fn show_objects(&self, kind: ShowKind) -> Result<DdlResult> {
        if kind == ShowKind::Tables {
            return self.show_tables();
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
            ShowKind::Tables | ShowKind::Models => unreachable!(),
        };
        named_objects_result(rows)
    }

    fn show_create(&self, kind: ShowKind, name: &str) -> Result<DdlResult> {
        let snapshot = self.engine.inner.catalog.snapshot()?;
        let (object_type, create_sql) = match kind {
            ShowKind::Tables => (
                "TABLE",
                snapshot
                    .table(name)
                    .map(|object| render_create_table(&object.definition, &object.schema)),
            ),
            ShowKind::Models => (
                "MODEL",
                snapshot
                    .model(name)
                    .map(|object| render_create(&object.definition)),
            ),
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
        show_create_result(name, object_type, create_sql)
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
            providers.push(format!("{:?}", table.definition.provider.kind()).to_ascii_uppercase());
            locations.push(
                table
                    .definition
                    .provider
                    .location()
                    .unwrap_or_default()
                    .to_owned(),
            );
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

fn show_create_result(name: &str, object_type: &str, create_sql: String) -> Result<DdlResult> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("object_name", DataType::Utf8, false),
        Field::new("object_type", DataType::Utf8, false),
        Field::new("create_sql", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![name])) as ArrayRef,
            Arc::new(StringArray::from(vec![object_type])),
            Arc::new(StringArray::from(vec![create_sql.as_str()])),
        ],
    )
    .map_err(|error| {
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

fn percentile(values: &[u64], percentile: f64) -> u64 {
    let index = ((values.len() - 1) as f64 * percentile).ceil() as usize;
    values[index]
}

fn record_percentiles(
    samples: &Mutex<PercentileSamples>,
    p50: &AtomicU64,
    p95: &AtomicU64,
    sample: u64,
) {
    if let Ok(mut samples) = samples.lock() {
        if samples.values.len() < MAX_PERCENTILE_SAMPLES {
            samples.values.push(sample);
        } else {
            let next = samples.next;
            samples.values[next] = sample;
            samples.next = (next + 1) % MAX_PERCENTILE_SAMPLES;
        }
        let mut sorted = samples.values.clone();
        sorted.sort_unstable();
        p50.store(percentile(&sorted, 0.50), Ordering::Relaxed);
        p95.store(percentile(&sorted, 0.95), Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EngineConfig;
    use arrow::array::{Array, Float64Array, Int64Array};
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
    fn continuous_metric_samples_are_bounded_and_histogrammed() {
        let metrics = QueryMetrics::default();
        for sample in 0..(MAX_PERCENTILE_SAMPLES + 100) {
            metrics.record_inference(3, sample as u64, sample as u64, sample as u64);
            metrics.record_epoch(sample as u64, sample as u64);
        }

        assert_eq!(
            metrics
                .inference_latencies_micros
                .lock()
                .unwrap()
                .values
                .len(),
            MAX_PERCENTILE_SAMPLES
        );
        assert_eq!(
            metrics.epoch_samples_micros.lock().unwrap().values.len(),
            MAX_PERCENTILE_SAMPLES
        );
        assert_eq!(
            metrics.batch_histogram()[3],
            (MAX_PERCENTILE_SAMPLES + 100) as u64
        );
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

        let statements = [
            show_create_sql(&session, "SHOW CREATE TABLE photos"),
            show_create_sql(&session, "SHOW CREATE TABLE entrance"),
            resolved_model,
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
            parse_statement(statement).unwrap();
            copy_session.sql(statement).unwrap();
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
    fn session_memory_failure_releases_every_reservation() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(
            EngineConfig::new(temp.path().join("catalog.db")).with_session_memory_limit_bytes(128),
        )
        .unwrap();
        let session = engine.session().build().unwrap();
        let sql = format!("SELECT '{}' AS value", "x".repeat(1024));
        let statement = session.sql(&sql).unwrap();
        let metrics = statement.metrics().unwrap();

        let error = statement.collect().unwrap_err();
        assert_eq!(error.code, ErrorCode::ResourceExhausted);
        assert_eq!(metrics.total_resource_usage().current_bytes, 0);
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
    fn query_execution_timer_starts_when_results_are_requested() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        let statement = session.sql("SELECT 1").unwrap();
        let Statement::Query(query) = &statement else {
            panic!("SELECT must produce a query");
        };
        assert!(query.execution_started_at.lock().unwrap().is_none());

        let planned_at = std::time::Instant::now();
        std::thread::sleep(std::time::Duration::from_millis(20));
        statement.collect().unwrap();

        let started_at = query.execution_started_at.lock().unwrap().unwrap();
        assert!(started_at.duration_since(planned_at) >= std::time::Duration::from_millis(15));
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
        let metrics = statement.metrics().unwrap();

        let error = statement.collect().unwrap_err();

        assert_eq!(error.code, ErrorCode::ResourceExhausted);
        assert_eq!(metrics.total_resource_usage().current_bytes, 0);
    }

    #[test]
    fn dropping_query_handle_releases_cached_arrow_results() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        let statement = session.sql("SELECT 'cached result' AS value").unwrap();
        let metrics = statement.metrics().unwrap();

        statement.collect().unwrap();
        assert!(
            metrics
                .resource_usage(crate::QueryResource::Arrow)
                .current_bytes
                > 0
        );
        drop(statement);

        assert_eq!(metrics.total_resource_usage().current_bytes, 0);
        assert!(
            metrics
                .resource_usage(crate::QueryResource::Arrow)
                .peak_bytes
                > 0
        );
    }

    #[test]
    fn bounded_source_metrics_count_rows_before_filtering() {
        let temp = tempdir().unwrap();
        let photos = temp.path().join("photos");
        std::fs::create_dir(&photos).unwrap();
        for (name, width) in [("small.png", 8), ("large.png", 16)] {
            RgbImage::from_pixel(width, 4, Rgb([1, 2, 3]))
                .save(photos.join(name))
                .unwrap();
        }
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        session
            .sql(&format!(
                "CREATE TABLE photos USING IMAGES LOCATION '{}'",
                photos.display()
            ))
            .unwrap();

        let statement = session
            .sql("SELECT uri FROM photos WHERE width > 100")
            .unwrap();
        let batches = statement.collect().unwrap();

        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        assert_eq!(statement.metrics().unwrap().input_rows(), 2);
        assert_eq!(statement.metrics().unwrap().output_rows(), 0);
    }

    #[test]
    fn concurrent_sessions_keep_query_metrics_isolated() {
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
        assert_eq!(left.metrics().unwrap().input_rows(), 1);
        assert_eq!(left.metrics().unwrap().output_rows(), 1);
        assert_eq!(right.metrics().unwrap().input_rows(), 3);
        assert_eq!(right.metrics().unwrap().output_rows(), 3);
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
        let explain = explain_text(
            &session,
            "EXPLAIN SELECT IMAGE_DETECTION('detector', image) FROM photos",
        );
        assert!(explain.contains("VisionQLPlan mode=bounded"));
        assert!(explain.contains("Inference model=detector"));
        assert!(explain.contains("batching_owner=visionql"));
        assert!(explain.contains("dedup=enabled"));
        assert!(explain.contains("decode=skipped(mock)"));
        assert!(explain.contains("image_payload=locator_or_encoded"));
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
        let metrics = statement.metrics().unwrap();
        assert_eq!(metrics.inference_rows(), 1);
        assert!(
            metrics
                .resource_usage(crate::QueryResource::Media)
                .peak_bytes
                > 0
        );
        assert!(
            metrics
                .resource_usage(crate::QueryResource::ModelQueue)
                .peak_bytes
                > 0
        );
        assert_eq!(
            metrics
                .resource_usage(crate::QueryResource::Media)
                .current_bytes,
            0
        );

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
        assert_eq!(query.metrics().total_resource_usage().current_bytes, 0);
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
        assert_eq!(
            insert
                .metrics()
                .unwrap()
                .total_resource_usage()
                .current_bytes,
            0
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
        let metrics = statement.metrics().unwrap();
        assert!(metrics.decode_frames() > 0);
        assert!(metrics.source_generation() > 0);
        assert!(metrics.sampled_fps() > 0.0);
        assert!(metrics.source_input_bytes() > 0);
        assert!(metrics.epoch_p50_ms() >= 0.0);
        assert!(metrics.end_to_end_p50_ms() >= metrics.epoch_p50_ms());
        assert!(metrics.watermark_ms().is_some());
        assert_eq!(
            metrics
                .resource_usage(crate::QueryResource::FrameBuffer)
                .current_bytes,
            0
        );
        assert!(
            metrics
                .resource_usage(crate::QueryResource::FrameBuffer)
                .peak_bytes
                > 0
        );
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
