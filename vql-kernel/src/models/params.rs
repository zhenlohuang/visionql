use serde::{Deserialize, Serialize};

#[cfg(test)]
use serde_json::Value;
#[cfg(test)]
use std::collections::BTreeMap;

#[cfg(test)]
use super::registry::PipelineRegistry;
use super::registry::invalid_option;
use crate::Result;
#[cfg(test)]
use crate::catalog::{ModelType, ProcessorSpec, RuntimeSpec};

pub(crate) const DEFAULT_MIN_CONFIDENCE: f32 = 0.25;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct BoundInferenceParams {
    #[serde(default)]
    pub(crate) classes: Option<Vec<String>>,
    pub(crate) min_confidence: f32,
}

impl Default for BoundInferenceParams {
    fn default() -> Self {
        Self {
            classes: None,
            min_confidence: DEFAULT_MIN_CONFIDENCE,
        }
    }
}

#[cfg(test)]
fn model_specs_for_options(
    model_type: ModelType,
    source: &str,
    options: &BTreeMap<String, Value>,
) -> Result<(RuntimeSpec, ProcessorSpec, ProcessorSpec)> {
    PipelineRegistry::builtins().model_specs_for_options(model_type, source, options)
}

pub(crate) fn bind_inference_params(
    classes: Option<Vec<String>>,
    min_confidence: Option<f32>,
) -> Result<BoundInferenceParams> {
    if classes
        .as_ref()
        .is_some_and(|values| values.iter().any(String::is_empty))
    {
        return invalid_option("classes", "must not contain empty labels");
    }
    let min_confidence = min_confidence.unwrap_or(DEFAULT_MIN_CONFIDENCE);
    if !min_confidence.is_finite() || !(0.0..=1.0).contains(&min_confidence) {
        return invalid_option("min_confidence", "must be between 0 and 1");
    }
    Ok(BoundInferenceParams {
        classes,
        min_confidence,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorCode;

    #[test]
    fn structured_processor_options_are_validated_by_the_registry() {
        let options = BTreeMap::from([
            ("runtime.kind".to_owned(), serde_json::json!("onnxruntime")),
            (
                "pre_processor.options".to_owned(),
                serde_json::json!({"input_name": "images", "width": 320, "height": 192}),
            ),
            (
                "post_processor.options".to_owned(),
                serde_json::json!({"output_name": "output0", "labels": ["person"]}),
            ),
        ]);
        let (runtime, pre_processor, post_processor) =
            model_specs_for_options(ModelType::ObjectDetection, "file:///model.onnx", &options)
                .unwrap();
        assert_eq!(runtime.kind, "onnxruntime");
        assert_eq!(pre_processor.options["width"], serde_json::json!(320));
        assert_eq!(
            post_processor.options["labels"],
            serde_json::json!(["person"])
        );
    }

    #[test]
    fn flat_processor_options_are_rejected() {
        let options = BTreeMap::from([("pre_processor.width".to_owned(), serde_json::json!(640))]);
        let error =
            model_specs_for_options(ModelType::ObjectDetection, "file:///model.onnx", &options)
                .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidOption);
        assert!(error.message.contains("pre_processor.width"));
    }

    #[test]
    fn known_future_runtimes_are_version_gated() {
        for (kind, target) in [
            ("transformers", "v0.4"),
            ("vllm", "未排期"),
            ("sglang", "未排期"),
        ] {
            let options = BTreeMap::from([("runtime.kind".to_owned(), serde_json::json!(kind))]);
            let error =
                model_specs_for_options(ModelType::ObjectDetection, "file:///model.onnx", &options)
                    .unwrap_err();
            assert_eq!(error.code, ErrorCode::FeatureNotAvailable);
            assert_eq!(error.target_version.as_deref(), Some(target));
        }
    }

    #[test]
    fn triton_defaults_its_protocol() {
        let options = BTreeMap::from([
            ("runtime.kind".to_owned(), serde_json::json!("triton")),
            (
                "runtime.model_name".to_owned(),
                serde_json::json!("yolo_26-n"),
            ),
        ]);
        let (runtime, _, _) = model_specs_for_options(
            ModelType::ObjectDetection,
            "endpoint://http://127.0.0.1:8000",
            &options,
        )
        .unwrap();
        assert_eq!(runtime.protocol.as_deref(), Some("kserve_v2_http"));
    }
}
