mod backend;
mod cache;
mod definition;
mod ort_backend;
mod params;
mod pipeline;
mod resolver;
mod runtime;
mod scheduler;
mod triton_backend;

pub(crate) use definition::{resolve_model, semantic_fingerprint};
pub(crate) use params::{
    BoundInferenceParams, ModelOutputFormat, ModelParams, bind_inference_params,
    compile_model_params, model_specs_for_options,
};
pub(crate) use runtime::{ModelCounters, ModelRuntime, detect_objects_udf};

use std::sync::Arc;

use arrow::array::{Float32Builder, ListBuilder, StringBuilder, StructBuilder};
use arrow::datatypes::{DataType, Field, Fields};

use crate::types::box2d_field;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Detection {
    pub(crate) label: String,
    pub(crate) confidence: f32,
    pub(crate) x: f32,
    pub(crate) y: f32,
    pub(crate) w: f32,
    pub(crate) h: f32,
}

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

pub(crate) fn detection_builder(capacity: usize) -> ListBuilder<StructBuilder> {
    let box_fields = match box2d_field("box", false).data_type() {
        DataType::Struct(fields) => fields.clone(),
        _ => unreachable!("BOX2D is always a struct"),
    };
    let box_builder = StructBuilder::new(
        box_fields,
        vec![
            Box::new(Float32Builder::with_capacity(capacity)),
            Box::new(Float32Builder::with_capacity(capacity)),
            Box::new(Float32Builder::with_capacity(capacity)),
            Box::new(Float32Builder::with_capacity(capacity)),
        ],
    );
    let detection_builder = StructBuilder::new(
        detection_fields(),
        vec![
            Box::new(StringBuilder::with_capacity(capacity, capacity * 8)),
            Box::new(Float32Builder::with_capacity(capacity)),
            Box::new(box_builder),
        ],
    );
    ListBuilder::new(detection_builder)
}

pub(crate) fn append_detections(
    builder: &mut ListBuilder<StructBuilder>,
    detections: Option<&[Detection]>,
) {
    let Some(detections) = detections else {
        builder.append(false);
        return;
    };
    for detection in detections {
        let values = builder.values();
        values
            .field_builder::<StringBuilder>(0)
            .expect("detection label builder")
            .append_value(&detection.label);
        values
            .field_builder::<Float32Builder>(1)
            .expect("detection confidence builder")
            .append_value(detection.confidence);
        let box_builder = values
            .field_builder::<StructBuilder>(2)
            .expect("detection box builder");
        for (index, value) in [detection.x, detection.y, detection.w, detection.h]
            .into_iter()
            .enumerate()
        {
            box_builder
                .field_builder::<Float32Builder>(index)
                .expect("BOX2D coordinate builder")
                .append_value(value);
        }
        box_builder.append(true);
        values.append(true);
    }
    builder.append(true);
}
