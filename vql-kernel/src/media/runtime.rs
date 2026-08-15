use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[cfg(feature = "ffmpeg-native")]
use super::FrameBufferLease;
#[cfg(feature = "ffmpeg-native")]
use super::ffmpeg::NativeDecoder;
use super::ffmpeg::SubprocessDecoder;
use super::{
    DecodedFrame, FrameBufferRegistry, FrameInfo, SampleSpec, TimeRange, VideoDecoder,
    VideoMetadata, sample_timestamps,
};
use crate::{ErrorCode, Result, VqlError};

#[derive(Debug)]
pub(crate) struct MediaRuntime {
    preferred: Arc<dyn VideoDecoder>,
    fallback: Arc<SubprocessDecoder>,
    decode_gate: Mutex<()>,
    probe_calls: AtomicU64,
    timestamp_scans: AtomicU64,
    decoded_frames: AtomicU64,
    decode_errors: AtomicU64,
    frame_buffers: Arc<FrameBufferRegistry>,
}

impl MediaRuntime {
    pub(crate) fn new() -> Self {
        let fallback = Arc::new(SubprocessDecoder);
        #[cfg(feature = "ffmpeg-native")]
        let preferred: Arc<dyn VideoDecoder> = NativeDecoder::try_new()
            .map(|decoder| Arc::new(decoder) as Arc<dyn VideoDecoder>)
            .unwrap_or_else(|_| Arc::clone(&fallback) as Arc<dyn VideoDecoder>);
        #[cfg(not(feature = "ffmpeg-native"))]
        let preferred: Arc<dyn VideoDecoder> = Arc::clone(&fallback) as Arc<dyn VideoDecoder>;
        Self {
            preferred,
            fallback,
            decode_gate: Mutex::new(()),
            probe_calls: AtomicU64::new(0),
            timestamp_scans: AtomicU64::new(0),
            decoded_frames: AtomicU64::new(0),
            decode_errors: AtomicU64::new(0),
            frame_buffers: FrameBufferRegistry::new(),
        }
    }

    pub(crate) fn video_available(&self) -> bool {
        self.backend_name() == "ffmpeg-native" || SubprocessDecoder::available()
    }

    pub(crate) const fn rtsp_available(&self) -> bool {
        cfg!(feature = "ffmpeg-native")
    }

    #[cfg(feature = "ffmpeg-native")]
    pub(crate) fn register_frame_buffer(
        &self,
        frames: Vec<DecodedFrame>,
    ) -> Result<(u64, FrameBufferLease)> {
        self.frame_buffers.register(frames)
    }

    pub(crate) fn resolve_buffered_frame(&self, buffer_id: u64, slot: u32) -> Result<DecodedFrame> {
        self.frame_buffers.resolve(buffer_id, slot)
    }

    pub(crate) fn backend_name(&self) -> &'static str {
        self.preferred.name()
    }

    pub(crate) fn probe(&self, path: &Path) -> Result<VideoMetadata> {
        self.probe_calls.fetch_add(1, Ordering::Relaxed);
        self.preferred
            .probe(path)
            .or_else(|_| self.fallback.probe(path))
    }

    pub(crate) fn sampled_frames(
        &self,
        path: &Path,
        range: TimeRange,
        fps: f64,
    ) -> Result<Vec<FrameInfo>> {
        self.timestamp_scans.fetch_add(1, Ordering::Relaxed);
        let timestamps = self
            .preferred
            .timestamps(path)
            .or_else(|_| self.fallback.timestamps(path))?;
        sample_timestamps(&timestamps, range, SampleSpec { fps })
    }

    pub(crate) fn decode_frame(&self, path: &Path, pts_ms: i64) -> Result<DecodedFrame> {
        let _permit = self
            .decode_gate
            .lock()
            .map_err(|_| VqlError::new(ErrorCode::Internal, "media decode gate was poisoned"))?;
        let frame = self
            .preferred
            .decode_frame(path, pts_ms)
            .or_else(|_| self.fallback.decode_frame(path, pts_ms))?;
        self.decoded_frames.fetch_add(1, Ordering::Relaxed);
        Ok(frame)
    }

    pub(crate) fn counters(&self) -> MediaCounters {
        MediaCounters {
            probe_calls: self.probe_calls.load(Ordering::Relaxed),
            timestamp_scans: self.timestamp_scans.load(Ordering::Relaxed),
            decoded_frames: self.decoded_frames.load(Ordering::Relaxed),
            decode_errors: self.decode_errors.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record_decode_error(&self) {
        self.decode_errors.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct MediaCounters {
    pub(crate) probe_calls: u64,
    pub(crate) timestamp_scans: u64,
    pub(crate) decoded_frames: u64,
    pub(crate) decode_errors: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media::ffmpeg::SubprocessDecoder;
    use crate::media::{SampleSpec, TimeRange, VideoDecoder};
    use crate::test_util::{ffmpeg_available, generate_test_video};
    use tempfile::tempdir;

    #[test]
    fn subprocess_probe_sampling_and_open_are_real() {
        if !ffmpeg_available() {
            return;
        }
        let temp = tempdir().unwrap();
        let path = temp.path().join("test.mp4");
        assert!(generate_test_video(&path));
        let decoder = SubprocessDecoder;
        let metadata = decoder.probe(&path).unwrap();
        assert_eq!((metadata.width, metadata.height), (320, 240));
        assert!((3_900..=4_100).contains(&metadata.duration_ms));
        let timestamps = decoder.timestamps(&path).unwrap();
        let selected = crate::media::sample_timestamps(
            &timestamps,
            TimeRange::default(),
            SampleSpec { fps: 5.0 },
        )
        .unwrap();
        assert_eq!(selected.len(), 20);
        assert!(
            selected
                .iter()
                .zip((0..20).map(|frame| frame * 200))
                .all(|(actual, expected)| (actual.pts_ms - expected).abs() <= 2)
        );

        let frames = decoder
            .open(
                &path,
                TimeRange {
                    start_ms: Some(0),
                    end_ms: Some(400),
                },
                SampleSpec { fps: 5.0 },
            )
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(frames.len(), 2);
        assert!(
            frames
                .iter()
                .all(|frame| (frame.width, frame.height) == (320, 240))
        );
    }

    #[cfg(feature = "ffmpeg-native")]
    #[test]
    fn native_and_subprocess_decode_agree_on_shape() {
        if !ffmpeg_available() {
            return;
        }
        let temp = tempdir().unwrap();
        let path = temp.path().join("test.mp4");
        assert!(generate_test_video(&path));
        let native = NativeDecoder::try_new().unwrap();
        let subprocess = SubprocessDecoder;
        let left = native.decode_frame(&path, 1_000).unwrap();
        let right = subprocess.decode_frame(&path, 1_000).unwrap();
        assert_eq!((left.width, left.height), (right.width, right.height));
        assert_eq!(left.rgb.len(), right.rgb.len());
        let mean = |pixels: &[u8]| {
            pixels.iter().map(|value| *value as f64).sum::<f64>() / pixels.len() as f64
        };
        assert!((mean(&left.rgb) - mean(&right.rgb)).abs() < 8.0);
    }
}
