use std::collections::BTreeMap;

use serde_json::Value;

use crate::catalog::{ModelOutputFormat, ModelParams};
use crate::{ErrorCode, Result, VqlError};

pub(crate) fn model_params_for_source(
    source: &str,
    defaults: &BTreeMap<String, Value>,
) -> Result<ModelParams> {
    let mut params = match defaults.get("processor").and_then(Value::as_str) {
        Some("yolo-detect-v1") => ModelParams {
            processor: "yolo-detect-v1".to_owned(),
            output_name: "detections".to_owned(),
            labels: vec!["object".to_owned()],
            output_format: ModelOutputFormat::XywhNormalized,
            ..ModelParams::default()
        },
        _ => ModelParams::default(),
    };
    if source.starts_with("mock://") || source.starts_with("endpoint://") {
        params.labels = vec!["person".to_owned(), "object".to_owned()];
    }
    apply_model_parameters(&mut params, defaults)?;
    Ok(params)
}

pub(crate) fn effective_model_params(
    defaults: &ModelParams,
    overrides: &BTreeMap<String, Value>,
) -> Result<ModelParams> {
    let mut params = defaults.clone();
    apply_model_parameters(&mut params, overrides)?;
    Ok(params)
}

fn apply_model_parameters(
    params: &mut ModelParams,
    values: &BTreeMap<String, Value>,
) -> Result<()> {
    for (name, value) in values {
        match name.as_str() {
            "processor" => params.processor = string_value(name, value)?,
            "input_name" => params.input_name = string_value(name, value)?,
            "output_name" => params.output_name = string_value(name, value)?,
            "input_width" => params.input_width = dimension_value(name, value)?,
            "input_height" => params.input_height = dimension_value(name, value)?,
            "output_format" => {
                params.output_format = match string_value(name, value)?.as_str() {
                    "xywh_normalized" => ModelOutputFormat::XywhNormalized,
                    "ultralytics_raw" => ModelOutputFormat::UltralyticsRaw,
                    "ultralytics_end_to_end" | "ultralytics_nms" => {
                        ModelOutputFormat::UltralyticsEndToEnd
                    }
                    format => {
                        return invalid_parameter(
                            name,
                            format!("unsupported output format '{format}'"),
                        );
                    }
                }
            }
            "labels" => params.labels = string_array(name, value)?,
            "classes" => params.classes = Some(string_array(name, value)?),
            "min_confidence" => params.min_confidence = probability_value(name, value)?,
            "nms_iou_threshold" | "nms_threshold" => {
                params.nms_iou_threshold = probability_value(name, value)?;
            }
            _ => return invalid_parameter(name, "unknown model parameter"),
        }
    }
    validate(params)
}

fn validate(params: &ModelParams) -> Result<()> {
    if !matches!(
        params.processor.as_str(),
        "yolo26-detect-v1" | "yolo-detect-v1"
    ) {
        return invalid_parameter(
            "processor",
            format!("unsupported processor '{}'", params.processor),
        );
    }
    if params.input_name.is_empty() {
        return invalid_parameter("input_name", "must not be empty");
    }
    if params.output_name.is_empty() {
        return invalid_parameter("output_name", "must not be empty");
    }
    if params.labels.iter().any(String::is_empty) {
        return invalid_parameter("labels", "must not contain empty labels");
    }
    if params
        .classes
        .as_ref()
        .is_some_and(|classes| classes.iter().any(String::is_empty))
    {
        return invalid_parameter("classes", "must not contain empty labels");
    }
    Ok(())
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
    let integer = value.as_u64().or_else(|| {
        value.as_f64().and_then(|value| {
            (value.is_finite() && value > 0.0 && value.fract() == 0.0 && value <= u32::MAX as f64)
                .then_some(value as u64)
        })
    });
    let value = integer
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| parameter_error(name, "must be a positive integer"))?;
    Ok(value)
}

fn probability_value(name: &str, value: &Value) -> Result<f32> {
    let value = value
        .as_f64()
        .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
        .ok_or_else(|| parameter_error(name, "must be a number between 0 and 1"))?;
    Ok(value as f32)
}

fn invalid_parameter<T>(name: &str, message: impl Into<String>) -> Result<T> {
    Err(parameter_error(name, message))
}

fn parameter_error(name: &str, message: impl Into<String>) -> VqlError {
    VqlError::new(
        ErrorCode::InvalidOption,
        format!("invalid model parameter '{name}': {}", message.into()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn function_parameters_override_model_defaults() {
        let defaults = BTreeMap::from([
            ("labels".to_owned(), serde_json::json!(["person", "car"])),
            ("min_confidence".to_owned(), serde_json::json!(0.25)),
        ]);
        let params = model_params_for_source("file:///model.onnx", &defaults).unwrap();
        let overrides = BTreeMap::from([
            ("classes".to_owned(), serde_json::json!(["person"])),
            ("min_confidence".to_owned(), serde_json::json!(0.7)),
            ("nms_threshold".to_owned(), serde_json::json!(0.3)),
        ]);

        let effective = effective_model_params(&params, &overrides).unwrap();

        assert_eq!(effective.labels, vec!["person", "car"]);
        assert_eq!(effective.classes, Some(vec!["person".to_owned()]));
        assert_eq!(effective.min_confidence, 0.7);
        assert_eq!(effective.nms_iou_threshold, 0.3);
    }

    #[test]
    fn yolo26_processor_defaults_match_official_end_to_end_export() {
        let params = model_params_for_source("file:///yolo26n.onnx", &BTreeMap::new()).unwrap();

        assert_eq!(params.processor, "yolo26-detect-v1");
        assert_eq!(params.input_name, "images");
        assert_eq!(params.output_name, "output0");
        assert_eq!(params.output_format, ModelOutputFormat::UltralyticsEndToEnd);
        assert_eq!((params.input_width, params.input_height), (640, 640));
        assert_eq!(params.labels.len(), 80);
        assert_eq!(params.labels[0], "person");
        assert_eq!(params.labels[2], "car");
    }

    #[test]
    fn invalid_parameter_type_is_rejected() {
        let defaults = BTreeMap::from([("input_width".to_owned(), serde_json::json!(1.5))]);

        let error = model_params_for_source("file:///model.onnx", &defaults).unwrap_err();

        assert_eq!(error.code, ErrorCode::InvalidOption);
        assert!(error.to_string().contains("input_width"));
    }
}
