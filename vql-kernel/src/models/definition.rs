use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(serde::Serialize)]
pub(crate) struct CanonicalModelOptions<'a> {
    artifact: BTreeMap<&'a str, &'a serde_json::Value>,
    runtime: BTreeMap<&'a str, &'a serde_json::Value>,
    input: BTreeMap<&'a str, &'a serde_json::Value>,
    output: BTreeMap<&'a str, &'a serde_json::Value>,
}

pub(crate) fn canonical_model_options(
    options: &BTreeMap<String, serde_json::Value>,
) -> CanonicalModelOptions<'_> {
    let mut canonical = CanonicalModelOptions {
        artifact: BTreeMap::new(),
        runtime: BTreeMap::new(),
        input: BTreeMap::new(),
        output: BTreeMap::new(),
    };
    for (key, value) in options {
        let leaf = key.rsplit('.').next().unwrap_or(key);
        let group = match leaf {
            "sha256" => &mut canonical.artifact,
            "input_name" | "layout" | "resize" | "color_space" | "image_size" | "preprocess"
            | "mean" | "std" | "scale" | "pad_value" => &mut canonical.input,
            "format" | "labels" | "box_format" | "output_name" => &mut canonical.output,
            _ => &mut canonical.runtime,
        };
        group.insert(key.as_str(), value);
    }
    canonical
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_options_fingerprint_under_exactly_one_owning_group() {
        let options = BTreeMap::from([
            ("sha256".to_owned(), serde_json::json!("digest")),
            ("threads".to_owned(), serde_json::json!(2)),
            ("image.preprocess".to_owned(), serde_json::json!("imagenet")),
            (
                "features.output_name".to_owned(),
                serde_json::json!("embedding"),
            ),
        ]);
        let canonical = serde_json::to_value(canonical_model_options(&options)).unwrap();

        assert_eq!(canonical["artifact"]["sha256"], "digest");
        assert_eq!(canonical["runtime"]["threads"], 2);
        assert_eq!(canonical["input"]["image.preprocess"], "imagenet");
        assert_eq!(canonical["output"]["features.output_name"], "embedding");
        let owned = canonical
            .as_object()
            .unwrap()
            .values()
            .map(|group| group.as_object().unwrap().len())
            .sum::<usize>();
        assert_eq!(owned, options.len());
    }
}
