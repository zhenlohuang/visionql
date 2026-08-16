use std::io::Read;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use super::cache::ModelCache;
use super::definition::{hash_bytes, hex};
use crate::{ErrorCode, Result, VqlError};

pub(super) struct ResolvedSource {
    pub(super) resolved_source: String,
    pub(super) artifact_hash: Option<String>,
}

pub(super) fn validate_onnx_source(source: &str, expected_sha256: Option<&str>) -> Result<()> {
    if source.starts_with("mock://") {
        return Ok(());
    }
    if source.starts_with("hf://") {
        parse_hf(source)?;
        return Ok(());
    }
    if source.starts_with("http://") || source.starts_with("https://") {
        let url = reqwest::Url::parse(source).map_err(|error| {
            VqlError::new(ErrorCode::InvalidLocation, "model source URL is invalid")
                .with_source(error)
        })?;
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(VqlError::new(
                ErrorCode::InvalidOption,
                "ONNX_RUNTIME FROM cannot contain credentials, query parameters, or fragments",
            ));
        }
        ensure_onnx_filename(url.path())?;
        let expected = expected_sha256.ok_or_else(|| {
            VqlError::new(
                ErrorCode::InvalidOption,
                "ONNX_RUNTIME requires WITH (sha256 = '...') for HTTP(S) artifacts",
            )
        })?;
        validate_sha256(expected)?;
        return Ok(());
    }
    let path = source.strip_prefix("file://").unwrap_or(source);
    ensure_onnx_filename(path)
}

pub(super) fn resolve_onnx_source(
    source: &str,
    model_cache_dir: &Path,
    expected_sha256: Option<&str>,
    cancel: &CancellationToken,
) -> Result<ResolvedSource> {
    validate_onnx_source(source, expected_sha256)?;
    if source.starts_with("mock://") {
        return Ok(ResolvedSource {
            resolved_source: source.to_owned(),
            artifact_hash: Some(hash_bytes(source.as_bytes())),
        });
    }
    if source.starts_with("hf://") {
        return resolve_hf(source, model_cache_dir, cancel);
    }
    if source.starts_with("http://") || source.starts_with("https://") {
        return resolve_http(source, model_cache_dir, expected_sha256, None, cancel);
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
    let hash = hash_file(&path, cancel)?;
    Ok(ResolvedSource {
        resolved_source: path.to_string_lossy().into_owned(),
        artifact_hash: Some(hash),
    })
}

fn resolve_hf(
    source: &str,
    cache_dir: &Path,
    cancel: &CancellationToken,
) -> Result<ResolvedSource> {
    let (owner, repository, revision, artifact) = parse_hf(source)?;
    let url = format!("https://huggingface.co/{owner}/{repository}/resolve/{revision}/{artifact}");
    resolve_http(source, cache_dir, None, Some((&url, &artifact)), cancel)
}

fn resolve_http(
    source_identity: &str,
    cache_dir: &Path,
    expected_sha256: Option<&str>,
    override_url_and_name: Option<(&str, &str)>,
    cancel: &CancellationToken,
) -> Result<ResolvedSource> {
    let cache = ModelCache::new(cache_dir);
    if let Some((path, hash)) = cache.lookup_source(source_identity)?
        && expected_sha256.is_none_or(|expected| hash.eq_ignore_ascii_case(expected))
    {
        return Ok(ResolvedSource {
            resolved_source: path.to_string_lossy().into_owned(),
            artifact_hash: Some(hash),
        });
    }
    if cancel.is_cancelled() {
        return Err(VqlError::new(
            ErrorCode::QueryCancelled,
            "model resolve cancelled",
        ));
    }
    let (url, artifact_name) = match override_url_and_name {
        Some((url, name)) => (url.to_owned(), name.to_owned()),
        None => {
            let url = reqwest::Url::parse(source_identity).map_err(|error| {
                VqlError::new(ErrorCode::InvalidLocation, "model source URL is invalid")
                    .with_source(error)
            })?;
            let name = url
                .path_segments()
                .and_then(|mut segments| segments.next_back())
                .filter(|name| !name.is_empty())
                .unwrap_or("model.onnx")
                .to_owned();
            (url.to_string(), name)
        }
    };
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .map_err(|error| {
            VqlError::new(
                ErrorCode::Execution,
                "failed to build model download client",
            )
            .with_source(error)
        })?;
    let mut request = client.get(&url);
    if source_identity.starts_with("hf://")
        && let Ok(token) = std::env::var("HF_TOKEN")
    {
        request = request.bearer_auth(token);
    }
    let mut response = request
        .send()
        .and_then(reqwest::blocking::Response::error_for_status)
        .map_err(|error| {
            VqlError::new(
                ErrorCode::InvalidLocation,
                format!("cannot resolve model source '{source_identity}'"),
            )
            .with_source(error)
        })?;
    let (target, hash) = cache.store_download(
        source_identity,
        Path::new(&artifact_name),
        &mut response,
        expected_sha256,
        cancel,
    )?;
    Ok(ResolvedSource {
        resolved_source: target.to_string_lossy().into_owned(),
        artifact_hash: Some(hash),
    })
}

fn parse_hf(source: &str) -> Result<(String, String, String, String)> {
    let spec = source.trim_start_matches("hf://");
    let parts = spec.split('/').collect::<Vec<_>>();
    if parts.len() < 2 {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            "hf:// source must be hf://owner/repository@revision[/artifact.onnx]",
        ));
    }
    let owner = parts[0];
    let (repository, revision) = parts[1].split_once('@').ok_or_else(|| {
        VqlError::new(
            ErrorCode::InvalidOption,
            "hf:// source must pin an immutable revision with repository@revision",
        )
    })?;
    if owner.is_empty() || repository.is_empty() || revision.is_empty() {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            "hf:// source owner, repository, and revision must be non-empty",
        ));
    }
    let artifact = if parts.len() > 2 {
        parts[2..].join("/")
    } else {
        "model.onnx".to_owned()
    };
    ensure_onnx_filename(&artifact)?;
    Ok((
        owner.to_owned(),
        repository.to_owned(),
        revision.to_owned(),
        artifact,
    ))
}

fn ensure_onnx_filename(value: &str) -> Result<()> {
    if Path::new(value)
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("onnx"))
    {
        Ok(())
    } else {
        Err(VqlError::new(
            ErrorCode::InvalidOption,
            "ONNX_RUNTIME source must identify an artifact with a .onnx suffix",
        ))
    }
}

fn validate_sha256(value: &str) -> Result<()> {
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(VqlError::new(
            ErrorCode::InvalidOption,
            "sha256 must contain exactly 64 hexadecimal characters",
        ))
    }
}

fn hash_file(path: &Path, cancel: &CancellationToken) -> Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        if cancel.is_cancelled() {
            return Err(VqlError::new(
                ErrorCode::QueryCancelled,
                "model resolve cancelled",
            ));
        }
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(hex(&digest.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    #[test]
    fn local_models_are_resolved_in_place_without_cache_copy() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("local.onnx");
        std::fs::write(&source, b"local model").unwrap();
        let cache = temp.path().join("cache/models");

        let resolved = resolve_onnx_source(
            source.to_str().unwrap(),
            &cache,
            None,
            &CancellationToken::new(),
        )
        .unwrap();

        assert_eq!(
            Path::new(&resolved.resolved_source),
            source.canonicalize().unwrap()
        );
        assert!(!cache.exists());
    }

    #[test]
    fn remote_sources_require_an_immutable_identity() {
        assert!(validate_onnx_source("hf://owner/repository/model.onnx", None).is_err());
        assert!(validate_onnx_source("https://example.test/model.onnx", None).is_err());
        assert!(
            validate_onnx_source(
                "https://token@example.test/model.onnx?signature=secret",
                Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            )
            .is_err()
        );
    }

    #[test]
    fn http_model_is_streamed_into_the_cache_and_reused_offline() {
        let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
            return;
        };
        let address = listener.local_addr().unwrap();
        let bytes = b"downloaded onnx model";
        let expected_sha256 = hash_bytes(bytes);
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                bytes.len()
            )
            .unwrap();
            stream.write_all(bytes).unwrap();
        });
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache/models");
        let source = format!("http://{address}/detector.onnx");

        let first = resolve_onnx_source(
            &source,
            &cache,
            Some(&expected_sha256),
            &CancellationToken::new(),
        )
        .unwrap();
        server.join().unwrap();
        let second = resolve_onnx_source(
            &source,
            &cache,
            Some(&expected_sha256),
            &CancellationToken::new(),
        )
        .unwrap();

        assert_eq!(first.resolved_source, second.resolved_source);
        assert_eq!(
            first.artifact_hash.as_deref(),
            Some(expected_sha256.as_str())
        );
        assert_eq!(std::fs::read(first.resolved_source).unwrap(), bytes);
    }
}
