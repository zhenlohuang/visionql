use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
#[cfg(feature = "ffmpeg-native")]
use std::sync::atomic::Ordering;

#[cfg(feature = "ffmpeg-native")]
use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
#[cfg(feature = "ffmpeg-native")]
use arrow::record_batch::RecordBatchOptions;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::catalog::StreamDef;
#[cfg(feature = "ffmpeg-native")]
use crate::catalog::{EventTimePolicy, RtspTransport};
#[cfg(feature = "ffmpeg-native")]
use crate::media::DecodedFrame;
use crate::media::{FrameBufferLease, MediaRuntime};
use crate::types::image_field;
#[cfg(feature = "ffmpeg-native")]
use crate::types::{ImageRef, ImageRefBuilder};
use crate::{ErrorCode, Result, VqlError};

#[cfg(feature = "ffmpeg-native")]
const EPOCH_DURATION_MS: i64 = 200;
#[cfg(feature = "ffmpeg-native")]
const MAX_EPOCH_ROWS: usize = 64;
const EPOCH_BUFFER_CAPACITY: usize = 4;
#[cfg(feature = "ffmpeg-native")]
const MAX_CAPTURE_DRIFT_MS: i64 = 30_000;

pub(crate) fn rtsp_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        image_field("frame", false),
        Field::new("frame_id", DataType::Int64, false),
        Field::new("source", DataType::Utf8, false),
    ]))
}

#[derive(Debug)]
pub(crate) struct RtspTableProvider {
    schema: SchemaRef,
}

impl RtspTableProvider {
    pub(crate) fn new() -> Self {
        Self {
            schema: rtsp_schema(),
        }
    }
}

#[async_trait]
impl TableProvider for RtspTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(UnboundRtspExec::new(
            Arc::clone(&self.schema),
            projection.cloned(),
        )))
    }
}

struct UnboundRtspExec {
    projection: Option<Vec<usize>>,
    properties: Arc<PlanProperties>,
}

impl UnboundRtspExec {
    fn new(schema: SchemaRef, projection: Option<Vec<usize>>) -> Self {
        let output = projected_schema(&schema, projection.as_deref());
        Self {
            projection,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(output),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Unbounded {
                    requires_infinite_memory: false,
                },
            )),
        }
    }
}

impl Debug for UnboundRtspExec {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RtspSourceExec")
            .field("projection", &self.projection)
            .finish()
    }
}

impl DisplayAs for UnboundRtspExec {
    fn fmt_as(
        &self,
        _display_type: DisplayFormatType,
        formatter: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        write!(
            formatter,
            "RtspSourceExec: projection={:?}",
            self.projection
        )
    }
}

impl ExecutionPlan for UnboundRtspExec {
    fn name(&self) -> &str {
        "RtspSourceExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "RtspSourceExec is a leaf and cannot accept children".to_owned(),
            ))
        }
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        Err(DataFusionError::Internal(
            "RTSP execution must be driven by the VisionQL epoch coordinator".to_owned(),
        ))
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SourceProgress {
    pub(crate) generation: u64,
    pub(crate) decoded_frames: u64,
    pub(crate) sampled_frames: u64,
    pub(crate) reconnects: u64,
    pub(crate) event_time_fallbacks: u64,
    pub(crate) dropped_frames: u64,
}

#[derive(Debug)]
pub(crate) struct StreamEpoch {
    pub(crate) epoch_id: u64,
    pub(crate) batches: Vec<RecordBatch>,
    pub(crate) source_progress: SourceProgress,
    pub(crate) watermark_ms: Option<i64>,
    pub(crate) frame_lease: Option<FrameBufferLease>,
}

pub(crate) struct RtspEpochReceiver {
    receiver: mpsc::Receiver<Result<StreamEpoch>>,
    cancellation: CancellationToken,
}

impl RtspEpochReceiver {
    pub(crate) async fn next(&mut self) -> Result<Option<StreamEpoch>> {
        match self.receiver.recv().await {
            Some(result) => result.map(Some),
            None if self.cancellation.is_cancelled() => Ok(None),
            None => Err(VqlError::new(
                ErrorCode::Execution,
                "RTSP source worker stopped unexpectedly",
            )),
        }
    }
}

impl Drop for RtspEpochReceiver {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

pub(crate) fn start_rtsp_source(
    definition: StreamDef,
    media: Arc<MediaRuntime>,
    fail_on_error: Arc<AtomicBool>,
    cancellation: CancellationToken,
) -> Result<RtspEpochReceiver> {
    if !media.rtsp_available() {
        return Err(VqlError::new(
            ErrorCode::FeatureNotAvailable,
            "RTSP streaming requires the ffmpeg-native feature",
        ));
    }
    let (sender, receiver) = mpsc::channel(EPOCH_BUFFER_CAPACITY);
    let worker_cancel = cancellation.child_token();
    let receiver_cancel = worker_cancel.clone();
    std::thread::Builder::new()
        .name(format!("vql-rtsp-{}", definition.name))
        .spawn(move || run_source_worker(definition, media, fail_on_error, sender, worker_cancel))
        .map_err(|error| {
            VqlError::new(ErrorCode::Execution, "failed to start RTSP source worker")
                .with_source(error)
        })?;
    Ok(RtspEpochReceiver {
        receiver,
        cancellation: receiver_cancel,
    })
}

#[cfg(feature = "ffmpeg-native")]
#[derive(Debug)]
struct SampledFrame {
    event_time_ms: i64,
    frame_id: u64,
    frame: DecodedFrame,
}

#[cfg(feature = "ffmpeg-native")]
struct EpochBuilder {
    definition: StreamDef,
    media: Arc<MediaRuntime>,
    epoch_id: u64,
    epoch_start_ms: Option<i64>,
    frames: Vec<SampledFrame>,
    progress: SourceProgress,
    max_seen_event_time_ms: Option<i64>,
    watermark_ms: Option<i64>,
}

#[cfg(feature = "ffmpeg-native")]
impl EpochBuilder {
    fn new(definition: StreamDef, media: Arc<MediaRuntime>) -> Self {
        Self {
            definition,
            media,
            epoch_id: 0,
            epoch_start_ms: None,
            frames: Vec::new(),
            progress: SourceProgress::default(),
            max_seen_event_time_ms: None,
            watermark_ms: None,
        }
    }

    fn observe_event_time(&mut self, event_time_ms: i64) {
        self.max_seen_event_time_ms = Some(
            self.max_seen_event_time_ms
                .map_or(event_time_ms, |value| value.max(event_time_ms)),
        );
        let candidate = self
            .max_seen_event_time_ms
            .map(|value| value.saturating_sub(self.definition.watermark_delay_ms));
        self.watermark_ms = match (self.watermark_ms, candidate) {
            (Some(previous), Some(candidate)) => Some(previous.max(candidate)),
            (None, candidate) => candidate,
            (previous, None) => previous,
        };
        self.epoch_start_ms.get_or_insert(event_time_ms);
    }

    fn push(&mut self, event_time_ms: i64, frame_id: u64, frame: DecodedFrame) {
        self.frames.push(SampledFrame {
            event_time_ms,
            frame_id,
            frame,
        });
        self.progress.sampled_frames = self.progress.sampled_frames.saturating_add(1);
    }

    fn should_close(&self, event_time_ms: i64) -> bool {
        self.frames.len() >= MAX_EPOCH_ROWS
            || self
                .epoch_start_ms
                .is_some_and(|start| event_time_ms.saturating_sub(start) >= EPOCH_DURATION_MS)
    }

    fn finish(&mut self) -> Result<Option<StreamEpoch>> {
        if self.epoch_start_ms.is_none() {
            return Ok(None);
        }
        let frames = std::mem::take(&mut self.frames);
        let (batch, frame_lease) = if frames.is_empty() {
            (empty_batch()?, None)
        } else {
            let metadata = frames
                .iter()
                .map(|sample| {
                    (
                        sample.event_time_ms,
                        sample.frame_id,
                        sample.frame.width,
                        sample.frame.height,
                    )
                })
                .collect::<Vec<_>>();
            let decoded = frames
                .into_iter()
                .map(|sample| sample.frame)
                .collect::<Vec<_>>();
            let (buffer_id, lease) = self.media.register_frame_buffer(decoded)?;
            (
                build_batch(&self.definition.endpoint, buffer_id, &metadata)?,
                Some(lease),
            )
        };
        let epoch = StreamEpoch {
            epoch_id: self.epoch_id,
            batches: vec![batch],
            source_progress: self.progress,
            watermark_ms: self.watermark_ms,
            frame_lease,
        };
        self.epoch_id = self.epoch_id.saturating_add(1);
        self.epoch_start_ms = None;
        Ok(Some(epoch))
    }

    fn drop_pending_frames(&mut self) {
        let count = self.frames.len() as u64;
        let first_event_time_ms = self.frames.first().map(|frame| frame.event_time_ms);
        let last_event_time_ms = self.frames.last().map(|frame| frame.event_time_ms);
        self.frames.clear();
        self.epoch_start_ms = None;
        self.progress.dropped_frames = self.progress.dropped_frames.saturating_add(count);
        if count > 0 {
            tracing::warn!(
                stream = %self.definition.name,
                reason = "source_overrun",
                count,
                first_event_time_ms,
                last_event_time_ms,
                "dropping sampled RTSP frames before epoch admission"
            );
        }
    }
}

#[cfg(feature = "ffmpeg-native")]
fn empty_batch() -> Result<RecordBatch> {
    RecordBatch::try_new_with_options(
        rtsp_schema(),
        vec![
            Arc::new(TimestampMillisecondArray::from(Vec::<i64>::new()).with_timezone("UTC"))
                as ArrayRef,
            Arc::new(ImageRefBuilder::default().finish()),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(StringArray::from(Vec::<String>::new())),
        ],
        &RecordBatchOptions::new().with_row_count(Some(0)),
    )
    .map_err(|error| VqlError::new(ErrorCode::Execution, error.to_string()).with_source(error))
}

#[cfg(feature = "ffmpeg-native")]
fn build_batch(source: &str, buffer_id: u64, rows: &[(i64, u64, u32, u32)]) -> Result<RecordBatch> {
    let mut images = ImageRefBuilder::with_capacity(rows.len());
    for (slot, (_, frame_id, width, height)) in rows.iter().enumerate() {
        images.append(ImageRef::frame_buffer(
            source,
            *frame_id,
            i32::try_from(*width).unwrap_or(i32::MAX),
            i32::try_from(*height).unwrap_or(i32::MAX),
            buffer_id,
            slot as u32,
        ));
    }
    RecordBatch::try_new(
        rtsp_schema(),
        vec![
            Arc::new(
                TimestampMillisecondArray::from(rows.iter().map(|row| row.0).collect::<Vec<_>>())
                    .with_timezone("UTC"),
            ),
            Arc::new(images.finish()),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| i64::try_from(row.1).unwrap_or(i64::MAX))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec![source; rows.len()])),
        ],
    )
    .map_err(|error| VqlError::new(ErrorCode::Execution, error.to_string()).with_source(error))
}

#[cfg(feature = "ffmpeg-native")]
fn send_epoch(
    sender: &mpsc::Sender<Result<StreamEpoch>>,
    mut epoch: StreamEpoch,
    cancellation: &CancellationToken,
) -> bool {
    loop {
        match sender.try_send(Ok(epoch)) {
            Ok(()) => return true,
            Err(mpsc::error::TrySendError::Full(Ok(returned))) => {
                epoch = returned;
                if cancellation.is_cancelled() {
                    return false;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(mpsc::error::TrySendError::Full(Err(_))) => unreachable!(),
            Err(mpsc::error::TrySendError::Closed(_)) => return false,
        }
    }
}

fn send_error(sender: &mpsc::Sender<Result<StreamEpoch>>, error: VqlError) {
    let _ = sender.blocking_send(Err(error));
}

fn run_source_worker(
    definition: StreamDef,
    media: Arc<MediaRuntime>,
    fail_on_error: Arc<AtomicBool>,
    sender: mpsc::Sender<Result<StreamEpoch>>,
    cancellation: CancellationToken,
) {
    #[cfg(feature = "ffmpeg-native")]
    {
        if let Err(error) =
            run_native_source(definition, media, fail_on_error, &sender, &cancellation)
        {
            send_error(&sender, error);
        }
    }
    #[cfg(not(feature = "ffmpeg-native"))]
    {
        let _ = (definition, media, fail_on_error, cancellation);
        send_error(
            &sender,
            VqlError::new(
                ErrorCode::FeatureNotAvailable,
                "RTSP streaming requires the ffmpeg-native feature",
            ),
        );
    }
}

#[cfg(feature = "ffmpeg-native")]
fn run_native_source(
    definition: StreamDef,
    media: Arc<MediaRuntime>,
    fail_on_error: Arc<AtomicBool>,
    sender: &mpsc::Sender<Result<StreamEpoch>>,
    cancellation: &CancellationToken,
) -> Result<()> {
    use ffmpeg::format::Pixel;
    use ffmpeg::media::Type;
    use ffmpeg::software::scaling::context::Context as ScalingContext;
    use ffmpeg::util::frame::video::Video;
    use ffmpeg_next as ffmpeg;

    ffmpeg::init().map_err(ffmpeg_error)?;
    let ingest_clock = IngestClock::new();
    let mut builder = EpochBuilder::new(definition.clone(), media);
    let mut sampler = EventSampler::new(definition.fps);
    let mut frame_id = 0_u64;
    let mut backoff_seconds = 1_u64;

    while !cancellation.is_cancelled() {
        builder.progress.generation = builder.progress.generation.saturating_add(1);
        let mut options = ffmpeg::Dictionary::new();
        if definition.endpoint.starts_with("rtsp://") {
            options.set(
                "rtsp_transport",
                match definition.transport {
                    RtspTransport::Tcp => "tcp",
                    RtspTransport::Udp => "udp",
                },
            );
            options.set("rw_timeout", "5000000");
        }
        let mut input = match ffmpeg::format::input_with_dictionary(&definition.endpoint, options) {
            Ok(input) => input,
            Err(error) => {
                tracing::warn!(
                    stream = %definition.name,
                    error = %error,
                    "RTSP connection failed; retrying"
                );
                builder.progress.reconnects = builder.progress.reconnects.saturating_add(1);
                if !sleep_with_cancel(backoff_seconds, cancellation) {
                    break;
                }
                backoff_seconds = (backoff_seconds * 2).min(30);
                continue;
            }
        };
        let (stream_index, time_base, mut decoder) = {
            let stream = input.streams().best(Type::Video).ok_or_else(|| {
                VqlError::new(ErrorCode::Execution, "RTSP source has no video stream")
            })?;
            let codec = ffmpeg::codec::context::Context::from_parameters(stream.parameters())
                .map_err(ffmpeg_error)?;
            (
                stream.index(),
                stream.time_base(),
                codec.decoder().video().map_err(ffmpeg_error)?,
            )
        };
        let mut generation_clock = GenerationClock::new(definition.event_time);
        let mut scaler: Option<(Pixel, u32, u32, ScalingContext)> = None;
        let mut disconnected = false;
        let mut received_frame = false;

        'packets: for (stream, packet) in input.packets() {
            if cancellation.is_cancelled() {
                break 'packets;
            }
            if stream.index() != stream_index {
                continue;
            }
            if let Err(error) = decoder.send_packet(&packet) {
                if fail_on_error.load(Ordering::Relaxed) {
                    return Err(ffmpeg_error(error));
                }
                tracing::warn!(stream = %definition.name, error = %error, "RTSP decode failed");
                disconnected = true;
                break 'packets;
            }
            let mut decoded = Video::empty();
            while decoder.receive_frame(&mut decoded).is_ok() {
                if !received_frame {
                    received_frame = true;
                    backoff_seconds = 1;
                }
                let ingest_ms = ingest_clock.now_ms();
                let pts_ms = decoded.timestamp().map(|timestamp| {
                    (timestamp as f64 * f64::from(time_base) * 1_000.0).round() as i64
                });
                let (event_time_ms, fell_back) =
                    generation_clock.event_time(pts_ms, ingest_ms, builder.watermark_ms);
                if fell_back {
                    builder.progress.generation = builder.progress.generation.saturating_add(1);
                    builder.progress.event_time_fallbacks =
                        builder.progress.event_time_fallbacks.saturating_add(1);
                }
                builder.progress.decoded_frames = builder.progress.decoded_frames.saturating_add(1);
                builder.observe_event_time(event_time_ms);
                if sampler.should_sample(event_time_ms) {
                    let frame =
                        match scaled_rgb_frame(&decoded, pts_ms.unwrap_or_default(), &mut scaler) {
                            Ok(frame) => frame,
                            Err(error) if fail_on_error.load(Ordering::Relaxed) => {
                                return Err(error);
                            }
                            Err(error) => {
                                builder.media.record_decode_error();
                                tracing::warn!(
                                    stream = %definition.name,
                                    error = %error,
                                    "RTSP frame conversion failed; dropping frame"
                                );
                                continue;
                            }
                        };
                    builder.push(event_time_ms, frame_id, frame);
                    frame_id = frame_id.saturating_add(1);
                }
                if builder.should_close(event_time_ms) {
                    if sender.capacity() == 0 {
                        builder.drop_pending_frames();
                    } else if let Some(epoch) = builder.finish()?
                        && !send_epoch(sender, epoch, cancellation)
                    {
                        return Ok(());
                    }
                }
            }
        }
        if cancellation.is_cancelled() {
            break;
        }
        if sender.capacity() == 0 {
            builder.drop_pending_frames();
        } else if let Some(epoch) = builder.finish()?
            && !send_epoch(sender, epoch, cancellation)
        {
            return Ok(());
        }
        builder.progress.reconnects = builder.progress.reconnects.saturating_add(1);
        tracing::warn!(
            stream = %definition.name,
            decode_error = disconnected,
            "RTSP source disconnected; retrying"
        );
        if !sleep_with_cancel(backoff_seconds, cancellation) {
            break;
        }
        backoff_seconds = (backoff_seconds * 2).min(30);
    }
    Ok(())
}

#[cfg(feature = "ffmpeg-native")]
fn scaled_rgb_frame(
    decoded: &ffmpeg_next::util::frame::video::Video,
    pts_ms: i64,
    scaler: &mut Option<(
        ffmpeg_next::format::Pixel,
        u32,
        u32,
        ffmpeg_next::software::scaling::context::Context,
    )>,
) -> Result<DecodedFrame> {
    use ffmpeg_next::format::Pixel;
    use ffmpeg_next::software::scaling::{context::Context as ScalingContext, flag::Flags};
    use ffmpeg_next::util::frame::video::Video;

    let needs_scaler = scaler.as_ref().is_none_or(|(format, width, height, _)| {
        *format != decoded.format() || *width != decoded.width() || *height != decoded.height()
    });
    if needs_scaler {
        *scaler = Some((
            decoded.format(),
            decoded.width(),
            decoded.height(),
            ScalingContext::get(
                decoded.format(),
                decoded.width(),
                decoded.height(),
                Pixel::RGB24,
                decoded.width(),
                decoded.height(),
                Flags::BILINEAR,
            )
            .map_err(ffmpeg_error)?,
        ));
    }
    let (_, _, _, scaler) = scaler.as_mut().expect("scaler was initialized");
    let mut rgb = Video::empty();
    scaler.run(decoded, &mut rgb).map_err(ffmpeg_error)?;
    let row_bytes = rgb.width() as usize * 3;
    let mut pixels = Vec::with_capacity(row_bytes * rgb.height() as usize);
    for row in 0..rgb.height() as usize {
        let start = row * rgb.stride(0);
        pixels.extend_from_slice(&rgb.data(0)[start..start + row_bytes]);
    }
    Ok(DecodedFrame {
        pts_ms,
        width: rgb.width(),
        height: rgb.height(),
        rgb: pixels,
    })
}

#[cfg(feature = "ffmpeg-native")]
fn ffmpeg_error(error: ffmpeg_next::Error) -> VqlError {
    VqlError::new(
        ErrorCode::Execution,
        format!("native FFmpeg RTSP error: {error}"),
    )
}

#[cfg(feature = "ffmpeg-native")]
fn sleep_with_cancel(seconds: u64, cancellation: &CancellationToken) -> bool {
    let slices = seconds.saturating_mul(10);
    for _ in 0..slices {
        if cancellation.is_cancelled() {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    !cancellation.is_cancelled()
}

#[cfg(feature = "ffmpeg-native")]
#[derive(Debug)]
struct IngestClock {
    utc_start_ms: i64,
    monotonic_start: std::time::Instant,
}

#[cfg(feature = "ffmpeg-native")]
impl IngestClock {
    fn new() -> Self {
        Self {
            utc_start_ms: chrono::Utc::now().timestamp_millis(),
            monotonic_start: std::time::Instant::now(),
        }
    }

    fn now_ms(&self) -> i64 {
        self.utc_start_ms.saturating_add(
            i64::try_from(self.monotonic_start.elapsed().as_millis()).unwrap_or(i64::MAX),
        )
    }
}

#[cfg(feature = "ffmpeg-native")]
#[derive(Debug)]
enum GenerationClock {
    Uninitialized(EventTimePolicy),
    Capture {
        first_pts_ms: i64,
        first_ingest_ms: i64,
        last_event_time_ms: i64,
    },
    Ingest,
}

#[cfg(feature = "ffmpeg-native")]
impl GenerationClock {
    fn new(policy: EventTimePolicy) -> Self {
        Self::Uninitialized(policy)
    }

    fn event_time(
        &mut self,
        pts_ms: Option<i64>,
        ingest_ms: i64,
        current_watermark_ms: Option<i64>,
    ) -> (i64, bool) {
        match self {
            Self::Uninitialized(EventTimePolicy::IngestTime) => {
                *self = Self::Ingest;
                (ingest_ms, false)
            }
            Self::Uninitialized(EventTimePolicy::CaptureTime) => {
                let Some(first_pts_ms) = pts_ms else {
                    *self = Self::Ingest;
                    return (ingest_ms, true);
                };
                if current_watermark_ms.is_some_and(|watermark| ingest_ms < watermark) {
                    *self = Self::Ingest;
                    return (ingest_ms, true);
                }
                *self = Self::Capture {
                    first_pts_ms,
                    first_ingest_ms: ingest_ms,
                    last_event_time_ms: ingest_ms,
                };
                (ingest_ms, false)
            }
            Self::Capture {
                first_pts_ms,
                first_ingest_ms,
                last_event_time_ms,
            } => {
                let Some(pts_ms) = pts_ms else {
                    *self = Self::Ingest;
                    return (ingest_ms, true);
                };
                let candidate =
                    first_ingest_ms.saturating_add(pts_ms.saturating_sub(*first_pts_ms));
                let invalid = candidate < *last_event_time_ms
                    || candidate.abs_diff(ingest_ms) > MAX_CAPTURE_DRIFT_MS as u64
                    || current_watermark_ms.is_some_and(|watermark| candidate < watermark);
                if invalid {
                    *self = Self::Ingest;
                    (ingest_ms, true)
                } else {
                    *last_event_time_ms = candidate;
                    (candidate, false)
                }
            }
            Self::Ingest => (ingest_ms, false),
        }
    }
}

#[cfg(feature = "ffmpeg-native")]
#[derive(Debug)]
struct EventSampler {
    interval_ms: f64,
    next_event_time_ms: Option<f64>,
}

#[cfg(feature = "ffmpeg-native")]
impl EventSampler {
    fn new(fps: f64) -> Self {
        Self {
            interval_ms: 1_000.0 / fps,
            next_event_time_ms: None,
        }
    }

    fn should_sample(&mut self, event_time_ms: i64) -> bool {
        let event_time_ms = event_time_ms as f64;
        let Some(mut next) = self.next_event_time_ms else {
            self.next_event_time_ms = Some(event_time_ms + self.interval_ms);
            return true;
        };
        if event_time_ms + 0.5 < next {
            return false;
        }
        while next <= event_time_ms + 0.5 {
            next += self.interval_ms;
        }
        self.next_event_time_ms = Some(next);
        true
    }
}

fn projected_schema(schema: &SchemaRef, projection: Option<&[usize]>) -> SchemaRef {
    projection.map_or_else(
        || Arc::clone(schema),
        |indices| {
            Arc::new(Schema::new(
                indices
                    .iter()
                    .map(|index| schema.field(*index).clone())
                    .collect::<Vec<_>>(),
            ))
        },
    )
}

#[cfg(all(test, feature = "ffmpeg-native"))]
mod tests {
    use super::*;
    #[cfg(feature = "ffmpeg-native")]
    use arrow::array::{Array, StructArray, UInt32Array, UInt64Array};

    #[test]
    fn sampling_uses_event_time_not_frame_ordinal() {
        let mut sampler = EventSampler::new(5.0);
        let selected = [0, 33, 80, 205, 260, 401, 610]
            .into_iter()
            .filter(|timestamp| sampler.should_sample(*timestamp))
            .collect::<Vec<_>>();
        assert_eq!(selected, [0, 205, 401, 610]);
    }

    #[test]
    fn watermark_never_moves_backward() {
        let definition = StreamDef {
            name: "cam".to_owned(),
            endpoint: "rtsp://camera/live".to_owned(),
            fps: 5.0,
            event_time: EventTimePolicy::CaptureTime,
            watermark_delay_ms: 2_000,
            transport: RtspTransport::Tcp,
        };
        let media = Arc::new(MediaRuntime::new());
        let mut builder = EpochBuilder::new(definition, media);
        builder.observe_event_time(10_000);
        assert_eq!(builder.watermark_ms, Some(8_000));
        builder.observe_event_time(9_000);
        assert_eq!(builder.watermark_ms, Some(8_000));
        builder.observe_event_time(12_500);
        assert_eq!(builder.watermark_ms, Some(10_500));
    }

    #[test]
    fn overload_drops_only_frames_before_epoch_admission() {
        let definition = StreamDef {
            name: "cam".to_owned(),
            endpoint: "rtsp://camera/live".to_owned(),
            fps: 5.0,
            event_time: EventTimePolicy::CaptureTime,
            watermark_delay_ms: 2_000,
            transport: RtspTransport::Tcp,
        };
        let media = Arc::new(MediaRuntime::new());
        let mut builder = EpochBuilder::new(definition, media);
        builder.observe_event_time(10_000);
        builder.push(
            10_000,
            0,
            DecodedFrame {
                pts_ms: 0,
                width: 1,
                height: 1,
                rgb: vec![0, 0, 0],
            },
        );

        builder.drop_pending_frames();

        assert!(builder.frames.is_empty());
        assert_eq!(builder.progress.dropped_frames, 1);
        assert_eq!(builder.watermark_ms, Some(8_000));
    }

    #[test]
    fn invalid_capture_clock_starts_an_ingest_generation() {
        let mut clock = GenerationClock::new(EventTimePolicy::CaptureTime);
        assert_eq!(clock.event_time(Some(100), 1_000, None), (1_000, false));
        assert_eq!(clock.event_time(Some(90), 1_010, None), (1_010, true));
        assert_eq!(clock.event_time(Some(110), 1_020, None), (1_020, false));
    }

    #[cfg(feature = "ffmpeg-native")]
    #[test]
    fn native_worker_emits_an_epoch_with_leased_frames() {
        if !crate::test_util::ffmpeg_available() {
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let video = temp.path().join("source.mp4");
        assert!(crate::test_util::generate_test_video(&video));
        let media = Arc::new(MediaRuntime::new());
        let cancellation = CancellationToken::new();
        let definition = StreamDef {
            name: "test".to_owned(),
            endpoint: video.to_string_lossy().into_owned(),
            fps: 5.0,
            event_time: EventTimePolicy::CaptureTime,
            watermark_delay_ms: 100,
            transport: RtspTransport::Tcp,
        };
        let mut source = start_rtsp_source(
            definition,
            Arc::clone(&media),
            Arc::new(AtomicBool::new(false)),
            cancellation.clone(),
        )
        .unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let epoch = runtime
            .block_on(async {
                tokio::time::timeout(std::time::Duration::from_secs(5), source.next()).await
            })
            .expect("RTSP worker timed out")
            .unwrap()
            .unwrap();
        cancellation.cancel();

        assert!(!epoch.batches.is_empty());
        assert!(epoch.batches[0].num_rows() > 0);
        assert!(epoch.watermark_ms.is_some());
        let images = epoch.batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let buffer_ids = images
            .column(8)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let buffer_slots = images
            .column(9)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert!(!buffer_ids.is_null(0));
        let buffer_id = buffer_ids.value(0);
        assert!(
            media
                .resolve_buffered_frame(buffer_id, buffer_slots.value(0))
                .is_ok()
        );
        drop(epoch);
        assert!(media.resolve_buffered_frame(buffer_id, 0).is_err());
    }
}
