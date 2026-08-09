use std::path::Path;
use std::process::Command;

use serde::Deserialize;

use super::{DecodedFrame, VideoDecoder, VideoMetadata};
use crate::{ErrorCode, Result, VqlError};

#[derive(Debug, Default)]
pub(crate) struct SubprocessDecoder;

impl SubprocessDecoder {
    pub(crate) fn available() -> bool {
        Command::new("ffprobe")
            .arg("-version")
            .output()
            .is_ok_and(|output| output.status.success())
            && Command::new("ffmpeg")
                .arg("-version")
                .output()
                .is_ok_and(|output| output.status.success())
    }

    fn ffprobe_json(path: &Path, entries: &str, show_frames: bool) -> Result<Vec<u8>> {
        let mut command = Command::new("ffprobe");
        command
            .args(["-v", "error", "-select_streams", "v:0"])
            .arg(if show_frames {
                "-show_frames"
            } else {
                "-show_streams"
            })
            .args(["-show_entries", entries, "-of", "json"])
            .arg(path);
        let output = command.output().map_err(|error| {
            VqlError::new(ErrorCode::Execution, "failed to start ffprobe").with_source(error)
        })?;
        if !output.status.success() {
            return Err(VqlError::new(
                ErrorCode::Execution,
                format!(
                    "ffprobe failed for '{}': {}",
                    path.display(),
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
            ));
        }
        Ok(output.stdout)
    }
}

#[derive(Debug, Deserialize)]
struct ProbeOutput {
    #[serde(default)]
    streams: Vec<ProbeStream>,
    format: Option<ProbeFormat>,
}

#[derive(Debug, Deserialize)]
struct ProbeStream {
    width: Option<i32>,
    height: Option<i32>,
    codec_name: Option<String>,
    avg_frame_rate: Option<String>,
    duration: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ProbeFormat {
    duration: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FramesOutput {
    #[serde(default)]
    frames: Vec<ProbeFrame>,
}

#[derive(Debug, Deserialize)]
struct ProbeFrame {
    best_effort_timestamp_time: Option<String>,
}

impl VideoDecoder for SubprocessDecoder {
    fn name(&self) -> &'static str {
        "ffmpeg-subprocess"
    }

    fn probe(&self, path: &Path) -> Result<VideoMetadata> {
        let bytes = Self::ffprobe_json(
            path,
            "stream=width,height,codec_name,avg_frame_rate,duration:format=duration",
            false,
        )?;
        let output: ProbeOutput = serde_json::from_slice(&bytes).map_err(|error| {
            VqlError::new(ErrorCode::Execution, "ffprobe returned invalid JSON").with_source(error)
        })?;
        let stream = output.streams.first().ok_or_else(|| {
            VqlError::new(
                ErrorCode::Execution,
                format!("'{}' has no video stream", path.display()),
            )
        })?;
        let duration = stream
            .duration
            .as_deref()
            .or_else(|| output.format.as_ref()?.duration.as_deref())
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(0.0);
        Ok(VideoMetadata {
            duration_ms: (duration * 1_000.0).round() as i64,
            source_fps: stream.avg_frame_rate.as_deref().and_then(parse_ratio),
            width: stream.width.unwrap_or_default(),
            height: stream.height.unwrap_or_default(),
            codec: stream
                .codec_name
                .clone()
                .unwrap_or_else(|| "unknown".to_owned()),
        })
    }

    fn timestamps(&self, path: &Path) -> Result<Vec<i64>> {
        let bytes = Self::ffprobe_json(path, "frame=best_effort_timestamp_time", true)?;
        let output: FramesOutput = serde_json::from_slice(&bytes).map_err(|error| {
            VqlError::new(ErrorCode::Execution, "ffprobe returned invalid frame JSON")
                .with_source(error)
        })?;
        let mut timestamps = output
            .frames
            .iter()
            .filter_map(|frame| frame.best_effort_timestamp_time.as_deref())
            .filter_map(|value| value.parse::<f64>().ok())
            .map(|seconds| (seconds * 1_000.0).round() as i64)
            .collect::<Vec<_>>();
        timestamps.sort_unstable();
        timestamps.dedup();
        Ok(timestamps)
    }

    fn decode_frame(&self, path: &Path, pts_ms: i64) -> Result<DecodedFrame> {
        let output = Command::new("ffmpeg")
            .args(["-v", "error", "-ss"])
            .arg(format!("{:.6}", pts_ms as f64 / 1_000.0))
            .arg("-i")
            .arg(path)
            .args(["-frames:v", "1", "-f", "image2pipe", "-vcodec", "png", "-"])
            .output()
            .map_err(|error| {
                VqlError::new(ErrorCode::Execution, "failed to start ffmpeg").with_source(error)
            })?;
        if !output.status.success() {
            return Err(VqlError::new(
                ErrorCode::Execution,
                format!(
                    "ffmpeg failed to decode '{}': {}",
                    path.display(),
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
            ));
        }
        let image = image::load_from_memory(&output.stdout).map_err(|error| {
            VqlError::new(ErrorCode::Execution, "ffmpeg returned an invalid image")
                .with_source(error)
        })?;
        let rgb = image.into_rgb8();
        Ok(DecodedFrame {
            pts_ms,
            width: rgb.width(),
            height: rgb.height(),
            rgb: rgb.into_raw(),
        })
    }
}

fn parse_ratio(value: &str) -> Option<f64> {
    let (numerator, denominator) = value.split_once('/')?;
    let numerator = numerator.parse::<f64>().ok()?;
    let denominator = denominator.parse::<f64>().ok()?;
    (denominator != 0.0).then_some(numerator / denominator)
}
