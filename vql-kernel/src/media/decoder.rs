use std::path::Path;

use crate::Result;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct TimeRange {
    pub(crate) start_ms: Option<i64>,
    pub(crate) end_ms: Option<i64>,
}

impl TimeRange {
    pub(crate) fn contains(self, pts_ms: i64) -> bool {
        self.start_ms.is_none_or(|start| pts_ms >= start)
            && self.end_ms.is_none_or(|end| pts_ms < end)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct SampleSpec {
    pub(crate) fps: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VideoMetadata {
    pub(crate) duration_ms: i64,
    pub(crate) source_fps: Option<f64>,
    pub(crate) width: i32,
    pub(crate) height: i32,
    pub(crate) codec: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FrameInfo {
    pub(crate) pts_ms: i64,
    pub(crate) frame_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedFrame {
    pub(crate) pts_ms: i64,
    pub(crate) width: u32,
    pub(crate) height: u32,
    pub(crate) rgb: Vec<u8>,
}

#[allow(dead_code)] // Kept as the backend contract even when scans request only references.
pub(crate) type FrameIter = Box<dyn Iterator<Item = Result<DecodedFrame>> + Send>;

pub(crate) trait VideoDecoder: std::fmt::Debug + Send + Sync {
    fn name(&self) -> &'static str;
    fn probe(&self, path: &Path) -> Result<VideoMetadata>;
    fn timestamps(&self, path: &Path) -> Result<Vec<i64>>;
    fn decode_frame(&self, path: &Path, pts_ms: i64) -> Result<DecodedFrame>;

    #[allow(dead_code)] // Implementors expose full iteration; v0.1 scans avoid eager pixel decode.
    fn open(&self, path: &Path, range: TimeRange, sample: SampleSpec) -> Result<FrameIter> {
        let selected = sample_timestamps(&self.timestamps(path)?, range, sample)?;
        let decoded = selected
            .into_iter()
            .map(|frame| self.decode_frame(path, frame.pts_ms))
            .collect::<Vec<_>>();
        Ok(Box::new(decoded.into_iter()))
    }
}

pub(crate) fn sample_timestamps(
    timestamps: &[i64],
    range: TimeRange,
    sample: SampleSpec,
) -> Result<Vec<FrameInfo>> {
    if !sample.fps.is_finite() || sample.fps <= 0.0 {
        return Err(crate::VqlError::new(
            crate::ErrorCode::InvalidOption,
            "sample fps must be finite and greater than zero",
        ));
    }
    let interval_ms = 1_000.0 / sample.fps;
    let mut next_target = range.start_ms.unwrap_or(0).max(0) as f64;
    let mut selected = Vec::new();
    for &pts_ms in timestamps {
        if !range.contains(pts_ms) || (pts_ms as f64) + 0.5 < next_target {
            continue;
        }
        selected.push(FrameInfo {
            pts_ms,
            frame_id: selected.len() as u64,
        });
        while next_target <= pts_ms as f64 + 0.5 {
            next_target += interval_ms;
        }
    }
    Ok(selected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pts_sampling_is_stable_for_constant_rate_video() {
        let timestamps = (0..100).map(|frame| frame * 40).collect::<Vec<_>>();
        let selected =
            sample_timestamps(&timestamps, TimeRange::default(), SampleSpec { fps: 5.0 }).unwrap();
        assert_eq!(selected.len(), 20);
        assert_eq!(
            selected
                .iter()
                .map(|frame| frame.pts_ms)
                .collect::<Vec<_>>(),
            (0..20).map(|frame| frame * 200).collect::<Vec<_>>()
        );
    }

    #[test]
    fn sampling_respects_time_range_without_rebasing_pts() {
        let timestamps = (0..100).map(|frame| frame * 40).collect::<Vec<_>>();
        let selected = sample_timestamps(
            &timestamps,
            TimeRange {
                start_ms: Some(1_000),
                end_ms: Some(2_000),
            },
            SampleSpec { fps: 5.0 },
        )
        .unwrap();
        assert_eq!(
            selected
                .iter()
                .map(|frame| frame.pts_ms)
                .collect::<Vec<_>>(),
            vec![1_000, 1_200, 1_400, 1_600, 1_800]
        );
    }

    #[test]
    fn vfr_sampling_uses_pts_instead_of_frame_numbers() {
        let timestamps = vec![0, 33, 80, 121, 205, 260, 401, 610, 799, 1_005];
        let selected =
            sample_timestamps(&timestamps, TimeRange::default(), SampleSpec { fps: 5.0 }).unwrap();
        assert_eq!(
            selected
                .iter()
                .map(|frame| frame.pts_ms)
                .collect::<Vec<_>>(),
            vec![0, 205, 401, 610, 1_005]
        );
    }
}
