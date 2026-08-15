#[cfg(feature = "ffmpeg-native")]
mod native;
mod subprocess;

#[cfg(feature = "ffmpeg-native")]
pub(super) use native::NativeDecoder;
pub(super) use subprocess::SubprocessDecoder;
