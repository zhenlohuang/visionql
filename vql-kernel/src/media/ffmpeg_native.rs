use std::path::Path;

use ffmpeg::format::Pixel;
use ffmpeg::media::Type;
use ffmpeg::software::scaling::{context::Context as ScalingContext, flag::Flags};
use ffmpeg::util::frame::video::Video;
use ffmpeg_next as ffmpeg;

use super::ffmpeg_subprocess::SubprocessDecoder;
use super::{DecodedFrame, VideoDecoder, VideoMetadata};
use crate::{ErrorCode, Result, VqlError};

#[derive(Debug, Default)]
pub(crate) struct NativeDecoder;

impl NativeDecoder {
    pub(crate) fn try_new() -> Result<Self> {
        ffmpeg::init().map_err(ffmpeg_error)?;
        Ok(Self)
    }
}

impl VideoDecoder for NativeDecoder {
    fn name(&self) -> &'static str {
        "ffmpeg-native"
    }

    fn probe(&self, path: &Path) -> Result<VideoMetadata> {
        let context = ffmpeg::format::input(path).map_err(ffmpeg_error)?;
        let stream = context.streams().best(Type::Video).ok_or_else(|| {
            VqlError::new(
                ErrorCode::Execution,
                format!("'{}' has no video stream", path.display()),
            )
        })?;
        let rate = stream.rate();
        let source_fps = (rate.denominator() != 0)
            .then_some(rate.numerator() as f64 / rate.denominator() as f64);
        let codec = ffmpeg::codec::context::Context::from_parameters(stream.parameters())
            .map_err(ffmpeg_error)?;
        let decoder = codec.decoder().video().map_err(ffmpeg_error)?;
        let duration_ms = if context.duration() > 0 {
            context.duration() / 1_000
        } else {
            (stream.duration() as f64 * f64::from(stream.time_base()) * 1_000.0).round() as i64
        };
        Ok(VideoMetadata {
            duration_ms,
            source_fps,
            width: i32::try_from(decoder.width()).unwrap_or(i32::MAX),
            height: i32::try_from(decoder.height()).unwrap_or(i32::MAX),
            codec: decoder.id().name().to_owned(),
        })
    }

    fn timestamps(&self, path: &Path) -> Result<Vec<i64>> {
        // ffprobe reads decoded best-effort timestamps without transferring pixels.
        SubprocessDecoder.timestamps(path)
    }

    fn decode_frame(&self, path: &Path, pts_ms: i64) -> Result<DecodedFrame> {
        let mut input = ffmpeg::format::input(path).map_err(ffmpeg_error)?;
        let (stream_index, time_base, mut decoder) = {
            let stream = input.streams().best(Type::Video).ok_or_else(|| {
                VqlError::new(
                    ErrorCode::Execution,
                    format!("'{}' has no video stream", path.display()),
                )
            })?;
            let codec = ffmpeg::codec::context::Context::from_parameters(stream.parameters())
                .map_err(ffmpeg_error)?;
            (
                stream.index(),
                stream.time_base(),
                codec.decoder().video().map_err(ffmpeg_error)?,
            )
        };
        let mut scaler = ScalingContext::get(
            decoder.format(),
            decoder.width(),
            decoder.height(),
            Pixel::RGB24,
            decoder.width(),
            decoder.height(),
            Flags::BILINEAR,
        )
        .map_err(ffmpeg_error)?;
        let target_us = pts_ms.saturating_mul(1_000);
        input.seek(target_us, ..target_us).map_err(ffmpeg_error)?;
        decoder.flush();

        for (stream, packet) in input.packets() {
            if stream.index() != stream_index {
                continue;
            }
            decoder.send_packet(&packet).map_err(ffmpeg_error)?;
            if let Some(frame) = receive_target(&mut decoder, &mut scaler, time_base, pts_ms)? {
                return Ok(frame);
            }
        }
        decoder.send_eof().map_err(ffmpeg_error)?;
        receive_target(&mut decoder, &mut scaler, time_base, pts_ms)?.ok_or_else(|| {
            VqlError::new(
                ErrorCode::Execution,
                format!("no frame found at {pts_ms}ms in '{}'", path.display()),
            )
        })
    }
}

fn receive_target(
    decoder: &mut ffmpeg::decoder::Video,
    scaler: &mut ScalingContext,
    time_base: ffmpeg::Rational,
    target_ms: i64,
) -> Result<Option<DecodedFrame>> {
    let mut decoded = Video::empty();
    while decoder.receive_frame(&mut decoded).is_ok() {
        let decoded_ms = decoded
            .timestamp()
            .map(|timestamp| (timestamp as f64 * f64::from(time_base) * 1_000.0).round() as i64)
            .unwrap_or(target_ms);
        if decoded_ms + 1 < target_ms {
            continue;
        }
        let mut rgb = Video::empty();
        scaler.run(&decoded, &mut rgb).map_err(ffmpeg_error)?;
        let width = rgb.width();
        let height = rgb.height();
        let row_bytes = width as usize * 3;
        let mut pixels = Vec::with_capacity(row_bytes * height as usize);
        for row in 0..height as usize {
            let start = row * rgb.stride(0);
            pixels.extend_from_slice(&rgb.data(0)[start..start + row_bytes]);
        }
        return Ok(Some(DecodedFrame {
            pts_ms: decoded_ms,
            width,
            height,
            rgb: pixels,
        }));
    }
    Ok(None)
}

fn ffmpeg_error(error: ffmpeg::Error) -> VqlError {
    VqlError::new(
        ErrorCode::Execution,
        format!("native FFmpeg error: {error}"),
    )
}
