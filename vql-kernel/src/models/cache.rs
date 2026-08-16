use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use super::definition::{hash_bytes, hex};
use crate::{ErrorCode, Result, VqlError};

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub(super) struct ModelCache {
    root: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
struct SourceEntry {
    path: String,
    sha256: String,
}

impl ModelCache {
    pub(super) fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub(super) fn lookup_source(&self, source: &str) -> Result<Option<(PathBuf, String)>> {
        let path = self.source_entry_path(source);
        let entry = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<SourceEntry>(&bytes).map_err(|error| {
                VqlError::new(ErrorCode::Execution, "model cache index is invalid")
                    .with_source(error)
            })?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let artifact = PathBuf::from(&entry.path);
        if artifact.is_file() {
            Ok(Some((artifact, entry.sha256)))
        } else {
            Ok(None)
        }
    }

    pub(super) fn store_download(
        &self,
        source: &str,
        artifact_name: &Path,
        reader: &mut dyn Read,
        expected_sha256: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<(PathBuf, String)> {
        let filename = artifact_name.file_name().ok_or_else(|| {
            VqlError::new(
                ErrorCode::InvalidLocation,
                "downloaded model has no filename",
            )
        })?;
        let temporary_dir = self.root.join(".downloads");
        std::fs::create_dir_all(&temporary_dir)?;
        let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary = temporary_dir.join(format!(
            ".{}.tmp-{}-{sequence}",
            filename.to_string_lossy(),
            std::process::id()
        ));
        let result = self.write_download(
            source,
            filename,
            reader,
            expected_sha256,
            cancel,
            &temporary,
        );
        if result.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        result
    }

    fn write_download(
        &self,
        source: &str,
        filename: &std::ffi::OsStr,
        reader: &mut dyn Read,
        expected_sha256: Option<&str>,
        cancel: &CancellationToken,
        temporary: &Path,
    ) -> Result<(PathBuf, String)> {
        let mut output = File::create(temporary)?;
        let mut digest = Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            if cancel.is_cancelled() {
                return Err(VqlError::new(
                    ErrorCode::QueryCancelled,
                    "model resolve cancelled",
                ));
            }
            let count = reader.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            output.write_all(&buffer[..count])?;
            digest.update(&buffer[..count]);
        }
        output.sync_all()?;
        drop(output);
        let sha256 = hex(&digest.finalize());
        if let Some(expected) = expected_sha256
            && !sha256.eq_ignore_ascii_case(expected)
        {
            return Err(VqlError::new(
                ErrorCode::InvalidLocation,
                format!("model artifact checksum mismatch: expected {expected}, got {sha256}"),
            ));
        }

        let target_dir = self.root.join(&sha256);
        std::fs::create_dir_all(&target_dir)?;
        let target = target_dir.join(filename);
        if target.exists() {
            std::fs::remove_file(temporary)?;
        } else {
            std::fs::rename(temporary, &target)?;
        }
        let target = target.canonicalize()?;
        let entry = SourceEntry {
            path: target.to_string_lossy().into_owned(),
            sha256: sha256.clone(),
        };
        let entry_path = self.source_entry_path(source);
        if let Some(parent) = entry_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        atomic_write(&entry_path, &serde_json::to_vec(&entry)?)?;
        Ok((target, sha256))
    }

    fn source_entry_path(&self, source: &str) -> PathBuf {
        self.root
            .join("sources")
            .join(format!("{}.json", hash_bytes(source.as_bytes())))
    }
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("cache-entry");
    let temporary =
        path.with_file_name(format!(".{filename}.tmp-{}-{sequence}", std::process::id()));
    std::fs::write(&temporary, bytes)?;
    match std::fs::rename(&temporary, path) {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = std::fs::remove_file(temporary);
            Err(error.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn downloaded_artifact_is_content_addressed_and_indexed_by_source() {
        let temp = tempfile::tempdir().unwrap();
        let cache = ModelCache::new(temp.path().join("cache/models"));
        let source = "https://example.test/detector.onnx";
        let mut bytes = Cursor::new(b"model bytes");

        let (path, hash) = cache
            .store_download(
                source,
                Path::new("detector.onnx"),
                &mut bytes,
                None,
                &CancellationToken::new(),
            )
            .unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), b"model bytes");
        assert_eq!(cache.lookup_source(source).unwrap(), Some((path, hash)));
    }

    #[test]
    fn checksum_mismatch_does_not_install_an_artifact() {
        let temp = tempfile::tempdir().unwrap();
        let cache = ModelCache::new(temp.path().join("cache/models"));
        let mut bytes = Cursor::new(b"model bytes");

        let error = cache
            .store_download(
                "https://example.test/detector.onnx",
                Path::new("detector.onnx"),
                &mut bytes,
                Some("deadbeef"),
                &CancellationToken::new(),
            )
            .unwrap_err();

        assert_eq!(error.code, ErrorCode::InvalidLocation);
        assert!(error.message.contains("checksum mismatch"));
    }

    #[test]
    fn cancelled_download_does_not_install_an_artifact() {
        let temp = tempfile::tempdir().unwrap();
        let cache_root = temp.path().join("cache/models");
        let cache = ModelCache::new(&cache_root);
        let mut bytes = Cursor::new(b"model bytes");
        let cancel = CancellationToken::new();
        cancel.cancel();

        let error = cache
            .store_download(
                "https://example.test/detector.onnx",
                Path::new("detector.onnx"),
                &mut bytes,
                None,
                &cancel,
            )
            .unwrap_err();

        assert_eq!(error.code, ErrorCode::QueryCancelled);
        assert!(
            cache
                .lookup_source("https://example.test/detector.onnx")
                .unwrap()
                .is_none()
        );
    }
}
