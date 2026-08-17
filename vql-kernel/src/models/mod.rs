mod backend;
mod cache;
mod definition;
mod ort_backend;
mod params;
mod pipeline;
mod postprocess;
mod preprocess;
mod registry;
mod resolver;
mod runtime;
mod scheduler;
mod triton_backend;

pub(crate) use definition::semantic_fingerprint;
pub(crate) use params::{BoundInferenceParams, bind_inference_params};
pub(crate) use registry::PipelineRegistry;
pub(crate) use runtime::{ModelRuntime, image_detection};

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields};

use crate::catalog::ModelType;
use crate::types::box2d_field;

pub(crate) fn detection_fields() -> Fields {
    Fields::from(vec![
        Arc::new(Field::new("label", DataType::Utf8, false)),
        Arc::new(Field::new("confidence", DataType::Float32, false)),
        Arc::new(box2d_field("box", false)),
    ])
}

pub(crate) fn detections_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(detection_fields()),
        true,
    )))
}

impl ModelType {
    pub(crate) fn canonical_output_type(self) -> DataType {
        match self {
            Self::ObjectDetection => detections_type(),
        }
    }
}
