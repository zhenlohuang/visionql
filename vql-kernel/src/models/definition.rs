use sha2::{Digest, Sha256};

pub(crate) fn semantic_fingerprint(value: &impl serde::Serialize) -> String {
    let bytes = serde_json::to_vec(value).expect("catalog definitions serialize");
    hex(&Sha256::digest(bytes))
}

pub(super) fn hash_bytes(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

pub(super) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
