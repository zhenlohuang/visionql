use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::catalog::{ModelDef, ModelType, ProcessorSpec, RuntimeSpec};
use crate::{ErrorCode, Result, VqlError};

pub(crate) const DEFAULT_MIN_CONFIDENCE: f32 = 0.25;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum ModelOutputFormat {
    XywhNormalized,
    UltralyticsRaw,
    #[default]
    UltralyticsEndToEnd,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ModelParams {
    pub(crate) input_name: String,
    pub(crate) output_name: String,
    pub(crate) input_width: u32,
    pub(crate) input_height: u32,
    pub(crate) labels: Vec<String>,
    pub(crate) output_format: ModelOutputFormat,
    pub(crate) nms_iou_threshold: f32,
}

impl Default for ModelParams {
    fn default() -> Self {
        Self {
            input_name: "images".to_owned(),
            output_name: "output0".to_owned(),
            input_width: 640,
            input_height: 640,
            labels: coco_detection_labels(),
            output_format: ModelOutputFormat::default(),
            nms_iou_threshold: 0.45,
        }
    }
}

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

pub(crate) fn model_specs_for_options(
    model_type: ModelType,
    source: &str,
    options: &BTreeMap<String, Value>,
) -> Result<(RuntimeSpec, ProcessorSpec, ProcessorSpec)> {
    let default_runtime = if source.starts_with("endpoint://") {
        "triton"
    } else {
        "onnxruntime"
    };
    let mut runtime_kind = default_runtime.to_owned();
    let mut runtime_protocol = None;
    let mut runtime_options = BTreeMap::new();
    let mut pre_processor_kind = "vision.image_tensor@1".to_owned();
    let mut pre_processor_options = BTreeMap::new();
    let mut post_processor_kind = "vision.yolo_e2e@1".to_owned();
    let mut post_processor_options = BTreeMap::new();

    for (name, value) in options {
        match name.as_str() {
            "runtime.kind" => runtime_kind = string_value(name, value)?.to_ascii_lowercase(),
            "runtime.protocol" => {
                runtime_protocol = Some(string_value(name, value)?.to_ascii_lowercase())
            }
            "pre_processor.kind" => {
                pre_processor_kind = string_value(name, value)?.to_ascii_lowercase()
            }
            "pre_processor.options" => {
                pre_processor_options = object_value(name, value)?;
            }
            "post_processor.kind" => {
                post_processor_kind = string_value(name, value)?.to_ascii_lowercase()
            }
            "post_processor.options" => {
                post_processor_options = object_value(name, value)?;
            }
            _ if name.starts_with("runtime.") => {
                let option = name.trim_start_matches("runtime.");
                if !matches!(option, "model_name" | "model_version") {
                    return invalid_parameter(name, "unknown Runtime option");
                }
                runtime_options.insert(option.to_owned(), value.clone());
            }
            _ => return invalid_parameter(name, "unknown Model option"),
        }
    }

    validate_runtime(
        source,
        &runtime_kind,
        runtime_protocol.as_deref(),
        &runtime_options,
    )?;
    if runtime_kind == "triton" && runtime_protocol.is_none() {
        runtime_protocol = Some("kserve_v2_http".to_owned());
    }
    let runtime = RuntimeSpec {
        kind: runtime_kind,
        protocol: runtime_protocol,
        options: runtime_options,
    };
    let pre_processor = ProcessorSpec {
        kind: pre_processor_kind,
        options: pre_processor_options,
    };
    let post_processor = ProcessorSpec {
        kind: post_processor_kind,
        options: post_processor_options,
    };
    validate_pipeline_specs(model_type, &runtime, &pre_processor, &post_processor)?;
    Ok((runtime, pre_processor, post_processor))
}

pub(crate) fn compile_model_params(model: &ModelDef) -> Result<ModelParams> {
    validate_pipeline_specs(
        model.model_type,
        &model.runtime,
        &model.pre_processor,
        &model.post_processor,
    )?;

    let pre = &model.pre_processor.options;
    reject_unknown_options(
        "pre_processor.options",
        pre,
        &[
            "input_name",
            "width",
            "height",
            "resize",
            "color_space",
            "layout",
        ],
    )?;
    let input_name = optional_string(pre, "input_name", "images")?;
    let input_width = optional_dimension(pre, "width", 640)?;
    let input_height = optional_dimension(pre, "height", 640)?;
    require_enum(pre, "resize", "letterbox")?;
    require_enum(pre, "color_space", "rgb")?;
    require_enum(pre, "layout", "nchw")?;

    let post = &model.post_processor.options;
    reject_unknown_options(
        "post_processor.options",
        post,
        &["output_name", "box_format", "labels", "nms_iou_threshold"],
    )?;
    let output_name = optional_string(post, "output_name", "output0")?;
    let labels = match post.get("labels") {
        None => coco_detection_labels(),
        Some(Value::String(value)) if value.eq_ignore_ascii_case("coco80") => {
            coco_detection_labels()
        }
        Some(value) => string_array("post_processor.options.labels", value)?,
    };
    let nms_iou_threshold = optional_probability(post, "nms_iou_threshold", 0.45)?;
    let output_format = match model.post_processor.kind.as_str() {
        "vision.yolo_e2e@1" => {
            require_enum(post, "box_format", "xyxy")?;
            ModelOutputFormat::UltralyticsEndToEnd
        }
        "vision.yolo_raw@1" => {
            require_enum(post, "box_format", "xywh")?;
            ModelOutputFormat::UltralyticsRaw
        }
        "vision.xywh_normalized@1" => {
            require_enum(post, "box_format", "xywh")?;
            ModelOutputFormat::XywhNormalized
        }
        kind => {
            return invalid_parameter(
                "post_processor.kind",
                format!("unsupported OBJECT_DETECTION PostProcessor '{kind}'"),
            );
        }
    };

    if labels.iter().any(String::is_empty) {
        return invalid_parameter(
            "post_processor.options.labels",
            "must not contain empty labels",
        );
    }
    Ok(ModelParams {
        input_name,
        output_name,
        input_width,
        input_height,
        labels,
        output_format,
        nms_iou_threshold,
    })
}

pub(crate) fn bind_inference_params(
    classes: Option<Vec<String>>,
    min_confidence: Option<f32>,
) -> Result<BoundInferenceParams> {
    if classes
        .as_ref()
        .is_some_and(|values| values.iter().any(String::is_empty))
    {
        return invalid_parameter("classes", "must not contain empty labels");
    }
    let min_confidence = min_confidence.unwrap_or(DEFAULT_MIN_CONFIDENCE);
    if !min_confidence.is_finite() || !(0.0..=1.0).contains(&min_confidence) {
        return invalid_parameter("min_confidence", "must be between 0 and 1");
    }
    Ok(BoundInferenceParams {
        classes,
        min_confidence,
    })
}

fn validate_pipeline_specs(
    model_type: ModelType,
    runtime: &RuntimeSpec,
    pre_processor: &ProcessorSpec,
    post_processor: &ProcessorSpec,
) -> Result<()> {
    match model_type {
        ModelType::ObjectDetection => {
            if pre_processor.kind != "vision.image_tensor@1" {
                return invalid_parameter(
                    "pre_processor.kind",
                    format!(
                        "unsupported OBJECT_DETECTION PreProcessor '{}'",
                        pre_processor.kind
                    ),
                );
            }
            if !matches!(
                post_processor.kind.as_str(),
                "vision.yolo_e2e@1" | "vision.yolo_raw@1" | "vision.xywh_normalized@1"
            ) {
                return invalid_parameter(
                    "post_processor.kind",
                    format!(
                        "unsupported OBJECT_DETECTION PostProcessor '{}'",
                        post_processor.kind
                    ),
                );
            }
            if !matches!(runtime.kind.as_str(), "onnxruntime" | "triton") {
                return invalid_parameter(
                    "runtime.kind",
                    format!("unsupported v0.1 Runtime '{}'", runtime.kind),
                );
            }
        }
    }
    Ok(())
}

fn validate_runtime(
    source: &str,
    kind: &str,
    protocol: Option<&str>,
    options: &BTreeMap<String, Value>,
) -> Result<()> {
    match kind {
        "onnxruntime" => {
            if protocol.is_some() {
                return invalid_parameter(
                    "runtime.protocol",
                    "onnxruntime does not use a wire protocol",
                );
            }
            if !options.is_empty() {
                return invalid_parameter(
                    "runtime",
                    "onnxruntime does not accept binding options in v0.1",
                );
            }
            if source.starts_with("endpoint://") {
                return invalid_parameter(
                    "runtime.kind",
                    "onnxruntime requires a local or cached ONNX artifact",
                );
            }
            if !source.starts_with("mock://") && !is_onnx_source(source) {
                return invalid_parameter(
                    "source",
                    "onnxruntime requires an ONNX artifact with a .onnx suffix",
                );
            }
        }
        "triton" => {
            if !source.starts_with("endpoint://") {
                return invalid_parameter("runtime.kind", "triton requires endpoint:// source");
            }
            let endpoint = source.trim_start_matches("endpoint://");
            let url = reqwest::Url::parse(endpoint).map_err(|error| {
                parameter_error("source", "Triton endpoint must be an absolute HTTP(S) URL")
                    .with_source(error)
            })?;
            if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
                return invalid_parameter(
                    "source",
                    "Triton endpoint must be an absolute HTTP(S) URL",
                );
            }
            if protocol == Some("kserve_v2_grpc") {
                return Err(VqlError::feature(
                    "Triton kserve_v2_grpc is not available",
                    "未排期",
                ));
            }
            if protocol.unwrap_or("kserve_v2_http") != "kserve_v2_http" {
                return invalid_parameter(
                    "runtime.protocol",
                    "triton currently supports kserve_v2_http",
                );
            }
            let model_name = options
                .get("model_name")
                .ok_or_else(|| parameter_error("runtime.model_name", "is required for triton"))?;
            validate_path_segment(
                "runtime.model_name",
                &string_value("runtime.model_name", model_name)?,
            )?;
            if let Some(version) = options.get("model_version") {
                validate_path_segment(
                    "runtime.model_version",
                    &string_value("runtime.model_version", version)?,
                )?;
            }
        }
        "transformers" => {
            return Err(VqlError::feature(
                "the transformers Runtime is not available",
                "v0.4",
            ));
        }
        "vllm" | "sglang" | "llama_cpp" => {
            return Err(VqlError::feature(
                format!("the {kind} Runtime is not available"),
                "未排期",
            ));
        }
        other => {
            return invalid_parameter(
                "runtime.kind",
                format!("unsupported v0.1 Runtime '{other}'"),
            );
        }
    }
    Ok(())
}

fn is_onnx_source(source: &str) -> bool {
    if let Some(spec) = source.strip_prefix("hf://") {
        let path = spec.split('/').collect::<Vec<_>>();
        return path.len() == 2
            || path.get(2..).is_some_and(|components| {
                components.join("/").to_ascii_lowercase().ends_with(".onnx")
            });
    }
    Path::new(source.strip_prefix("file://").unwrap_or(source))
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("onnx"))
}

fn validate_path_segment(name: &str, value: &str) -> Result<()> {
    if value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        Ok(())
    } else {
        invalid_parameter(
            name,
            "must contain only ASCII letters, digits, '.', '-', or '_'",
        )
    }
}

fn object_value(name: &str, value: &Value) -> Result<BTreeMap<String, Value>> {
    let object = value
        .as_object()
        .ok_or_else(|| parameter_error(name, "must be an object"))?;
    Ok(object
        .iter()
        .map(|(key, value)| (key.to_ascii_lowercase(), value.clone()))
        .collect())
}

fn reject_unknown_options(
    namespace: &str,
    options: &BTreeMap<String, Value>,
    allowed: &[&str],
) -> Result<()> {
    if let Some(name) = options
        .keys()
        .find(|name| !allowed.contains(&name.as_str()))
    {
        return invalid_parameter(format!("{namespace}.{name}"), "unknown processor option");
    }
    Ok(())
}

fn optional_string(options: &BTreeMap<String, Value>, name: &str, default: &str) -> Result<String> {
    options
        .get(name)
        .map(|value| string_value(name, value))
        .unwrap_or_else(|| Ok(default.to_owned()))
}

fn optional_dimension(options: &BTreeMap<String, Value>, name: &str, default: u32) -> Result<u32> {
    options
        .get(name)
        .map(|value| dimension_value(name, value))
        .unwrap_or(Ok(default))
}

fn optional_probability(
    options: &BTreeMap<String, Value>,
    name: &str,
    default: f32,
) -> Result<f32> {
    options
        .get(name)
        .map(|value| probability_value(name, value))
        .unwrap_or(Ok(default))
}

fn require_enum(options: &BTreeMap<String, Value>, name: &str, expected: &str) -> Result<()> {
    let Some(value) = options.get(name) else {
        return Ok(());
    };
    let value = string_value(name, value)?;
    if value.eq_ignore_ascii_case(expected) {
        Ok(())
    } else {
        invalid_parameter(name, format!("must be '{expected}'"))
    }
}

fn string_value(name: &str, value: &Value) -> Result<String> {
    value
        .as_str()
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| parameter_error(name, "must be a non-empty string"))
}

fn string_array(name: &str, value: &Value) -> Result<Vec<String>> {
    value
        .as_array()
        .ok_or_else(|| parameter_error(name, "must be an array of strings"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(ToOwned::to_owned)
                .ok_or_else(|| parameter_error(name, "must be an array of strings"))
        })
        .collect()
}

fn dimension_value(name: &str, value: &Value) -> Result<u32> {
    let value = value
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| parameter_error(name, "must be a positive integer"))?;
    Ok(value)
}

fn probability_value(name: &str, value: &Value) -> Result<f32> {
    let value = value
        .as_f64()
        .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
        .ok_or_else(|| parameter_error(name, "must be between 0 and 1"))?;
    Ok(value as f32)
}

fn invalid_parameter<T>(name: impl AsRef<str>, message: impl Into<String>) -> Result<T> {
    Err(parameter_error(name, message))
}

fn parameter_error(name: impl AsRef<str>, message: impl Into<String>) -> VqlError {
    VqlError::new(
        ErrorCode::InvalidOption,
        format!(
            "invalid Model option '{}': {}",
            name.as_ref(),
            message.into()
        ),
    )
}

fn coco_detection_labels() -> Vec<String> {
    [
        "person",
        "bicycle",
        "car",
        "motorcycle",
        "airplane",
        "bus",
        "train",
        "truck",
        "boat",
        "traffic light",
        "fire hydrant",
        "stop sign",
        "parking meter",
        "bench",
        "bird",
        "cat",
        "dog",
        "horse",
        "sheep",
        "cow",
        "elephant",
        "bear",
        "zebra",
        "giraffe",
        "backpack",
        "umbrella",
        "handbag",
        "tie",
        "suitcase",
        "frisbee",
        "skis",
        "snowboard",
        "sports ball",
        "kite",
        "baseball bat",
        "baseball glove",
        "skateboard",
        "surfboard",
        "tennis racket",
        "bottle",
        "wine glass",
        "cup",
        "fork",
        "knife",
        "spoon",
        "bowl",
        "banana",
        "apple",
        "sandwich",
        "orange",
        "broccoli",
        "carrot",
        "hot dog",
        "pizza",
        "donut",
        "cake",
        "chair",
        "couch",
        "potted plant",
        "bed",
        "dining table",
        "toilet",
        "tv",
        "laptop",
        "mouse",
        "remote",
        "keyboard",
        "cell phone",
        "microwave",
        "oven",
        "toaster",
        "sink",
        "refrigerator",
        "book",
        "clock",
        "vase",
        "scissors",
        "teddy bear",
        "hair drier",
        "toothbrush",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_processor_options_compile() {
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
        let model = ModelDef {
            name: "detector".to_owned(),
            model_type: ModelType::ObjectDetection,
            source: "file:///model.onnx".to_owned(),
            resolved_source: "/model.onnx".to_owned(),
            artifact_hash: None,
            runtime,
            pre_processor,
            post_processor,
            semantic_fingerprint: "test".to_owned(),
            volatile: false,
        };

        let params = compile_model_params(&model).unwrap();

        assert_eq!((params.input_width, params.input_height), (320, 192));
        assert_eq!(params.labels, vec!["person"]);
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
    fn openai_is_a_protocol_not_a_runtime_kind() {
        let options = BTreeMap::from([("runtime.kind".to_owned(), serde_json::json!("openai"))]);

        let error =
            model_specs_for_options(ModelType::ObjectDetection, "file:///model.onnx", &options)
                .unwrap_err();

        assert!(error.message.contains("runtime.kind"));
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
    fn triton_defaults_and_validates_its_protocol_binding() {
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

        let invalid_options = BTreeMap::from([
            ("runtime.kind".to_owned(), serde_json::json!("triton")),
            (
                "runtime.model_name".to_owned(),
                serde_json::json!("../yolo"),
            ),
        ]);
        let error = model_specs_for_options(
            ModelType::ObjectDetection,
            "endpoint://http://127.0.0.1:8000",
            &invalid_options,
        )
        .unwrap_err();
        assert!(error.message.contains("runtime.model_name"));
    }

    #[test]
    fn runtime_source_combinations_are_validated_before_resolution() {
        let local = BTreeMap::from([("runtime.kind".to_owned(), serde_json::json!("onnxruntime"))]);
        let error =
            model_specs_for_options(ModelType::ObjectDetection, "file:///models/yolo.pt", &local)
                .unwrap_err();
        assert!(error.message.contains(".onnx"));

        let triton = BTreeMap::from([
            ("runtime.kind".to_owned(), serde_json::json!("triton")),
            ("runtime.model_name".to_owned(), serde_json::json!("yolo")),
        ]);
        let error = model_specs_for_options(
            ModelType::ObjectDetection,
            "endpoint://triton.internal:8000",
            &triton,
        )
        .unwrap_err();
        assert!(error.message.contains("absolute HTTP(S) URL"));
    }
}
