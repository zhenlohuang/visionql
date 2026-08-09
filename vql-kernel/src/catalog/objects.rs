use std::io::Cursor;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::{ErrorCode, Result, VqlError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObjectKind {
    Table,
    Model,
    Function,
    Sink,
}

impl ObjectKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Model => "model",
            Self::Function => "function",
            Self::Sink => "sink",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub(crate) enum TableProviderKind {
    Images,
    Videos,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct TableDef {
    pub(crate) name: String,
    pub(crate) provider: TableProviderKind,
    pub(crate) location: String,
    #[serde(default)]
    pub(crate) recursive: bool,
    #[serde(default)]
    pub(crate) fps: Option<f64>,
    #[serde(default)]
    pub(crate) start_time_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum ModelType {
    ObjectDetection,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ModelOutputFormat {
    XywhNormalized,
    UltralyticsRaw,
    #[default]
    #[serde(rename = "ultralytics_end_to_end", alias = "ultralytics_nms")]
    UltralyticsEndToEnd,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ModelParams {
    pub(crate) processor: String,
    #[serde(default = "default_input_name")]
    pub(crate) input_name: String,
    #[serde(default = "default_output_name")]
    pub(crate) output_name: String,
    #[serde(default = "default_input_width")]
    pub(crate) input_width: u32,
    #[serde(default = "default_input_height")]
    pub(crate) input_height: u32,
    #[serde(default)]
    pub(crate) labels: Vec<String>,
    #[serde(default)]
    pub(crate) classes: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) output_format: ModelOutputFormat,
    #[serde(default = "default_min_confidence", alias = "confidence_threshold")]
    pub(crate) min_confidence: f32,
    #[serde(default = "default_nms_iou_threshold")]
    pub(crate) nms_iou_threshold: f32,
}

fn default_input_name() -> String {
    "images".to_owned()
}

fn default_output_name() -> String {
    "output0".to_owned()
}

const fn default_input_width() -> u32 {
    640
}

const fn default_input_height() -> u32 {
    640
}

const fn default_min_confidence() -> f32 {
    0.25
}

const fn default_nms_iou_threshold() -> f32 {
    0.45
}

impl Default for ModelParams {
    fn default() -> Self {
        Self {
            processor: "yolo26-detect-v1".to_owned(),
            input_name: default_input_name(),
            output_name: default_output_name(),
            input_width: default_input_width(),
            input_height: default_input_height(),
            labels: coco_detection_labels(),
            classes: None,
            output_format: ModelOutputFormat::default(),
            min_confidence: default_min_confidence(),
            nms_iou_threshold: default_nms_iou_threshold(),
        }
    }
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ModelDef {
    pub(crate) name: String,
    pub(crate) model_type: ModelType,
    pub(crate) source: String,
    pub(crate) resolved_source: String,
    pub(crate) artifact_hash: Option<String>,
    #[serde(default, alias = "manifest")]
    pub(crate) params: ModelParams,
    pub(crate) semantic_fingerprint: String,
    #[serde(default)]
    pub(crate) volatile: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum FunctionImplementation {
    Model { model: String },
    Python { entry: String },
    SqlMacro { expression: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct FunctionDef {
    pub(crate) name: String,
    pub(crate) implementation: FunctionImplementation,
    pub(crate) parameters: Vec<(String, String)>,
    pub(crate) return_type: String,
    #[serde(default)]
    pub(crate) bindings: BTreeMap<String, serde_json::Value>,
    pub(crate) semantic_fingerprint: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SinkKind {
    Console,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SinkDef {
    pub(crate) name: String,
    pub(crate) kind: SinkKind,
}

pub(crate) fn encode_schema(schema: &SchemaRef) -> Result<Vec<u8>> {
    let writer = StreamWriter::try_new(Vec::new(), schema).map_err(|error| {
        VqlError::new(ErrorCode::Catalog, "failed to encode Arrow table schema").with_source(error)
    })?;
    writer.into_inner().map_err(|error| {
        VqlError::new(ErrorCode::Catalog, "failed to finish Arrow schema stream").with_source(error)
    })
}

pub(crate) fn decode_schema(bytes: &[u8]) -> Result<SchemaRef> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(|error| {
        VqlError::new(
            ErrorCode::Catalog,
            "catalog contains an invalid Arrow schema",
        )
        .with_source(error)
    })?;
    Ok(Arc::new(reader.schema().as_ref().clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_manifest_catalog_json_loads_as_model_params() {
        let definition: ModelDef = serde_json::from_value(serde_json::json!({
            "name": "detector",
            "model_type": "OBJECT_DETECTION",
            "source": "file:///model.onnx",
            "resolved_source": "/model.onnx",
            "artifact_hash": "abc",
            "manifest": {
                "processor": "yolo-detect-v1",
                "input_name": "images",
                "output_name": "detections",
                "input_width": 32,
                "input_height": 32,
                "labels": ["person"],
                "output_format": "xywh_normalized",
                "confidence_threshold": 0.6,
                "nms_iou_threshold": 0.4
            },
            "semantic_fingerprint": "legacy",
            "volatile": false
        }))
        .unwrap();

        assert_eq!(definition.params.labels, vec!["person"]);
        assert_eq!(definition.params.min_confidence, 0.6);
        assert_eq!(definition.params.classes, None);
    }
}
