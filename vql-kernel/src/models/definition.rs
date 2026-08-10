use std::hash::{Hash, Hasher};
use std::path::Path;

use sha2::{Digest, Sha256};

use crate::Result;
use crate::catalog::{ModelDef, ModelType, ProcessorSpec, RuntimeSpec};

use super::resolver::resolve_source;

pub(crate) fn resolve_model(
    name: &str,
    model_type: ModelType,
    source: &str,
    runtime: RuntimeSpec,
    pre_processor: ProcessorSpec,
    post_processor: ProcessorSpec,
    cache_dir: &Path,
) -> Result<ModelDef> {
    let resolved = resolve_source(source, cache_dir)?;
    let mut fingerprint = Sha256::new();
    model_type.hash(&mut FingerprintHasher(&mut fingerprint));
    fingerprint.update(source.as_bytes());
    fingerprint.update(resolved.resolved_source.as_bytes());
    if let Some(hash) = &resolved.artifact_hash {
        fingerprint.update(hash.as_bytes());
    }
    fingerprint.update(serde_json::to_vec(&runtime).expect("runtime spec serializes"));
    fingerprint.update(serde_json::to_vec(&pre_processor).expect("pre-processor spec serializes"));
    fingerprint
        .update(serde_json::to_vec(&post_processor).expect("post-processor spec serializes"));
    Ok(ModelDef {
        name: name.to_ascii_lowercase(),
        model_type,
        source: source.to_owned(),
        resolved_source: resolved.resolved_source,
        artifact_hash: resolved.artifact_hash,
        runtime,
        pre_processor,
        post_processor,
        semantic_fingerprint: hex(&fingerprint.finalize()),
        volatile: resolved.volatile,
    })
}

struct FingerprintHasher<'a>(&'a mut Sha256);

impl Hasher for FingerprintHasher<'_> {
    fn finish(&self) -> u64 {
        0
    }

    fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }
}

impl Hash for ModelType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u8(match self {
            Self::ObjectDetection => 1,
        });
    }
}

pub(crate) fn semantic_fingerprint(value: &impl serde::Serialize) -> String {
    let bytes = serde_json::to_vec(value).expect("catalog definitions serialize");
    hex(&Sha256::digest(bytes))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
