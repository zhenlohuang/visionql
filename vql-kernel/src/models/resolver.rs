use std::io::Read;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use super::cache::ModelCache;
use crate::{ErrorCode, Result, VqlError};

pub(super) struct ResolvedSource {
    pub(super) resolved_source: String,
    pub(super) artifact_hash: Option<String>,
    pub(super) volatile: bool,
}

pub(super) fn resolve_source(source: &str, model_cache_dir: &Path) -> Result<ResolvedSource> {
    if source.starts_with("mock://") {
        return Ok(ResolvedSource {
            resolved_source: source.to_owned(),
            artifact_hash: Some(hash_bytes(source.as_bytes())),
            volatile: false,
        });
    }
    if source.starts_with("hf://") {
        return resolve_hf(source, model_cache_dir);
    }
    if source.starts_with("endpoint://") {
        return Ok(ResolvedSource {
            resolved_source: source.to_owned(),
            artifact_hash: None,
            volatile: true,
        });
    }
    let path = source.strip_prefix("file://").unwrap_or(source);
    let path = PathBuf::from(path).canonicalize().map_err(|error| {
        VqlError::new(
            ErrorCode::InvalidLocation,
            format!("model artifact '{path}' does not exist"),
        )
        .with_source(error)
    })?;
    if !path.is_file() {
        return Err(VqlError::new(
            ErrorCode::InvalidLocation,
            "model source must resolve to an ONNX file",
        ));
    }
    let hash = hash_file(&path)?;
    Ok(ResolvedSource {
        resolved_source: path.to_string_lossy().into_owned(),
        artifact_hash: Some(hash),
        volatile: false,
    })
}

fn resolve_hf(source: &str, cache_dir: &Path) -> Result<ResolvedSource> {
    let spec = source.trim_start_matches("hf://");
    let parts = spec.split('/').collect::<Vec<_>>();
    if parts.len() < 2 {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            "hf:// source must be hf://owner/repository[@revision][/artifact.onnx]",
        ));
    }
    let owner = parts[0];
    let (repository, revision) = parts[1].split_once('@').unwrap_or((parts[1], "main"));
    let artifact = if parts.len() > 2 {
        parts[2..].join("/")
    } else {
        "model.onnx".to_owned()
    };
    if !artifact.ends_with(".onnx") {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            "hf:// v0.1 model artifact must be ONNX",
        ));
    }
    let base = format!("https://huggingface.co/{owner}/{repository}/resolve/{revision}");
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .map_err(|error| {
            VqlError::new(ErrorCode::Execution, "failed to build Hugging Face client")
                .with_source(error)
        })?;
    let mut request = client.get(format!("{base}/{artifact}"));
    if let Ok(token) = std::env::var("HF_TOKEN") {
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .and_then(reqwest::blocking::Response::error_for_status)
        .map_err(|error| {
            VqlError::new(
                ErrorCode::InvalidLocation,
                format!("cannot resolve '{source}'; repository must contain '{artifact}'"),
            )
            .with_source(error)
        })?;
    let bytes = response.bytes().map_err(|error| {
        VqlError::new(
            ErrorCode::Execution,
            "failed to download Hugging Face artifact",
        )
        .with_source(error)
    })?;
    let cache = ModelCache::new(cache_dir);
    let (target, hash) = cache.store_download(Path::new(&artifact), bytes.as_ref())?;
    Ok(ResolvedSource {
        resolved_source: target.to_string_lossy().into_owned(),
        artifact_hash: Some(hash),
        volatile: false,
    })
}

fn hash_file(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(hex(&digest.finalize()))
}

fn hash_bytes(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_models_are_resolved_in_place_without_cache_copy() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("local.onnx");
        std::fs::write(&source, b"local model").unwrap();
        let cache = temp.path().join("cache/models");

        let resolved = resolve_source(source.to_str().unwrap(), &cache).unwrap();

        assert_eq!(
            Path::new(&resolved.resolved_source),
            source.canonicalize().unwrap()
        );
        assert!(!cache.exists());
    }
}
