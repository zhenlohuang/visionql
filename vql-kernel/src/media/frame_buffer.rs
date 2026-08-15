use std::collections::HashMap;
#[cfg(feature = "ffmpeg-native")]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use super::DecodedFrame;
use crate::{ErrorCode, Result, VqlError};

#[derive(Debug)]
pub(crate) struct FrameBufferRegistry {
    #[cfg(feature = "ffmpeg-native")]
    next_id: AtomicU64,
    buffers: Mutex<HashMap<u64, Arc<Vec<DecodedFrame>>>>,
}

impl FrameBufferRegistry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            #[cfg(feature = "ffmpeg-native")]
            next_id: AtomicU64::new(1),
            buffers: Mutex::new(HashMap::new()),
        })
    }

    #[cfg(feature = "ffmpeg-native")]
    pub(crate) fn register(
        self: &Arc<Self>,
        frames: Vec<DecodedFrame>,
    ) -> Result<(u64, FrameBufferLease)> {
        let buffer_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.buffers
            .lock()
            .map_err(|_| VqlError::new(ErrorCode::Internal, "frame buffer registry was poisoned"))?
            .insert(buffer_id, Arc::new(frames));
        Ok((
            buffer_id,
            FrameBufferLease {
                buffer_id,
                registry: Arc::downgrade(self),
            },
        ))
    }

    pub(crate) fn resolve(&self, buffer_id: u64, slot: u32) -> Result<DecodedFrame> {
        let buffers = self.buffers.lock().map_err(|_| {
            VqlError::new(ErrorCode::Internal, "frame buffer registry was poisoned")
        })?;
        let buffer = buffers.get(&buffer_id).ok_or_else(|| {
            VqlError::new(
                ErrorCode::Execution,
                format!("frame buffer {buffer_id} is no longer available"),
            )
        })?;
        buffer.get(slot as usize).cloned().ok_or_else(|| {
            VqlError::new(
                ErrorCode::Internal,
                format!("frame buffer {buffer_id} has no slot {slot}"),
            )
        })
    }

    #[cfg(all(test, feature = "ffmpeg-native"))]
    pub(crate) fn contains(&self, buffer_id: u64) -> bool {
        self.buffers
            .lock()
            .is_ok_and(|buffers| buffers.contains_key(&buffer_id))
    }
}

#[derive(Debug)]
pub(crate) struct FrameBufferLease {
    buffer_id: u64,
    registry: Weak<FrameBufferRegistry>,
}

impl Drop for FrameBufferLease {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade()
            && let Ok(mut buffers) = registry.buffers.lock()
        {
            buffers.remove(&self.buffer_id);
        }
    }
}

#[cfg(all(test, feature = "ffmpeg-native"))]
mod tests {
    use super::*;

    #[test]
    fn lease_controls_buffer_lifetime() {
        let registry = FrameBufferRegistry::new();
        let (buffer_id, lease) = registry
            .register(vec![DecodedFrame {
                pts_ms: 0,
                width: 1,
                height: 1,
                rgb: vec![1, 2, 3],
            }])
            .unwrap();
        assert_eq!(registry.resolve(buffer_id, 0).unwrap().rgb, [1, 2, 3]);
        assert!(registry.contains(buffer_id));
        drop(lease);
        assert!(!registry.contains(buffer_id));
        assert!(registry.resolve(buffer_id, 0).is_err());
    }
}
