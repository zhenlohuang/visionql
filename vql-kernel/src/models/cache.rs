use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use sha2::{Digest, Sha256};

use crate::{ErrorCode, Result, VqlError};

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub(super) struct ModelCache {
    root: PathBuf,
}

impl ModelCache {
    pub(super) fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub(super) fn store_download(
        &self,
        artifact_name: &Path,
        artifact: &[u8],
    ) -> Result<(PathBuf, String)> {
        let filename = artifact_name.file_name().ok_or_else(|| {
            VqlError::new(
                ErrorCode::InvalidLocation,
                "downloaded model has no filename",
            )
        })?;
        let hash = content_hash(artifact);
        let target_dir = self.root.join(&hash);
        std::fs::create_dir_all(&target_dir)?;
        let target = target_dir.join(filename);
        atomic_write_if_missing(&target, artifact)?;
        Ok((target, hash))
    }
}

fn content_hash(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    hex(&digest.finalize())
}

fn atomic_write_if_missing(path: &Path, bytes: &[u8]) -> Result<()> {
    if path.exists() {
        return Ok(());
    }
    let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("model");
    let temporary =
        path.with_file_name(format!(".{filename}.tmp-{}-{sequence}", std::process::id()));
    std::fs::write(&temporary, bytes)?;
    match std::fs::rename(&temporary, path) {
        Ok(()) => Ok(()),
        Err(_) if path.exists() => {
            std::fs::remove_file(temporary)?;
            Ok(())
        }
        Err(error) => {
            let _ = std::fs::remove_file(temporary);
            Err(error.into())
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn downloaded_artifact_is_content_addressed_under_models_root() {
        let temp = tempfile::tempdir().unwrap();
        let cache = ModelCache::new(temp.path().join("cache/models"));

        let (path, hash) = cache
            .store_download(Path::new("detector.onnx"), b"model bytes")
            .unwrap();

        assert_eq!(
            path,
            temp.path()
                .join("cache/models")
                .join(&hash)
                .join("detector.onnx")
        );
        assert_eq!(std::fs::read(path).unwrap(), b"model bytes");
        assert_eq!(
            std::fs::read_dir(temp.path().join("cache/models").join(hash))
                .unwrap()
                .count(),
            1
        );
    }
}
