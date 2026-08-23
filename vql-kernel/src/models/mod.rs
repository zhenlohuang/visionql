mod backend;
mod builtin;
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

pub(crate) use builtin::BuiltinModels;
pub(crate) use definition::{canonical_model_options, semantic_fingerprint};
pub(crate) use params::{
    BoundInferenceParams, ClassificationOutputMode, ExtractFieldSpec, bind_classification_params,
    bind_detection_params, bind_inference_params,
};
pub(crate) use postprocess::task_detection_output;
pub(crate) use registry::PipelineRegistry;
pub(crate) use runtime::{ModelRuntime, model_marker};

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use arrow_schema::extension::{
    EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY, FixedShapeTensor,
};

use crate::types::{box2d_field, locator_field};

pub(crate) fn object_detection_interface() -> crate::catalog::ModelInterface {
    crate::catalog::ModelInterface {
        capability: Some(crate::catalog::ModelType::ObjectDetection),
        parameters: vec![crate::catalog::ModelParameter {
            name: "image".to_owned(),
            data_type: "IMAGE".to_owned(),
            constant: false,
            optional: false,
        }],
        semantic_arguments: vec![
            crate::catalog::ModelParameter {
                name: "classes".to_owned(),
                data_type: "ARRAY<STRING>".to_owned(),
                constant: true,
                optional: true,
            },
            crate::catalog::ModelParameter {
                name: "min_confidence".to_owned(),
                data_type: "FLOAT".to_owned(),
                constant: true,
                optional: true,
            },
        ],
        return_type: "ARRAY<STRUCT<label STRING, confidence FLOAT, box BOX2D>>".to_owned(),
        processing_family: "vision.object_detection".to_owned(),
        deterministic: true,
    }
}

pub(crate) fn image_classification_interface() -> crate::catalog::ModelInterface {
    crate::catalog::ModelInterface {
        capability: Some(crate::catalog::ModelType::ImageClassification),
        parameters: vec![crate::catalog::ModelParameter {
            name: "image".to_owned(),
            data_type: "IMAGE".to_owned(),
            constant: false,
            optional: false,
        }],
        semantic_arguments: vec![
            crate::catalog::ModelParameter {
                name: "categories".to_owned(),
                data_type: "ARRAY<STRING>".to_owned(),
                constant: true,
                optional: false,
            },
            crate::catalog::ModelParameter {
                name: "min_score".to_owned(),
                data_type: "FLOAT".to_owned(),
                constant: true,
                optional: true,
            },
        ],
        return_type: "ARRAY<STRUCT<label STRING, score FLOAT>>".to_owned(),
        processing_family: "vision.image_classification".to_owned(),
        deterministic: true,
    }
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

pub(crate) fn classification_fields() -> Fields {
    Fields::from(vec![
        Arc::new(Field::new("label", DataType::Utf8, false)),
        Arc::new(Field::new("score", DataType::Float32, false)),
    ])
}

pub(crate) fn classifications_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(classification_fields()),
        true,
    )))
}

pub(crate) fn classification_field(name: impl Into<String>, nullable: bool) -> FieldRef {
    Arc::new(Field::new(name, classifications_type(), nullable))
}

pub(crate) fn task_detection_fields() -> Fields {
    Fields::from(vec![
        Arc::new(Field::new("label", DataType::Utf8, false)),
        Arc::new(Field::new("score", DataType::Float32, false)),
        Arc::new(locator_field("locator", true)),
    ])
}

pub(crate) fn task_detections_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(task_detection_fields()),
        true,
    )))
}

pub(crate) fn task_detection_field(name: impl Into<String>, nullable: bool) -> FieldRef {
    Arc::new(Field::new(name, task_detections_type(), nullable))
}

pub(crate) fn extract_answer_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Arc::new(Field::new("value", DataType::Utf8, true)),
        Arc::new(Field::new("score", DataType::Float32, true)),
        Arc::new(locator_field("locator", true)),
    ]))
}

pub(crate) fn extraction_field(
    name: impl Into<String>,
    fields: &[ExtractFieldSpec],
    nullable: bool,
) -> FieldRef {
    let fields = fields
        .iter()
        .map(|field| {
            let answer = extract_answer_type();
            let data_type = if field.list {
                DataType::List(Arc::new(Field::new("item", answer, true)))
            } else {
                answer
            };
            Arc::new(Field::new(&field.name, data_type, false))
        })
        .collect::<Vec<_>>();
    Arc::new(Field::new(
        name,
        DataType::Struct(Fields::from(fields)),
        nullable,
    ))
}

pub(crate) fn canonical_output_type(model_type: crate::catalog::ModelType) -> DataType {
    match model_type {
        crate::catalog::ModelType::ObjectDetection => detections_type(),
        crate::catalog::ModelType::ImageClassification => classifications_type(),
    }
}

pub(crate) fn interface_output_field(
    name: impl Into<String>,
    interface: &crate::catalog::ModelInterface,
    nullable: bool,
) -> crate::Result<FieldRef> {
    match interface.capability {
        Some(capability) => Ok(Arc::new(Field::new(
            name,
            canonical_output_type(capability),
            nullable,
        ))),
        None => parse_boundary_field(name, &interface.return_type, nullable),
    }
}

pub(crate) fn parse_boundary_field(
    name: impl Into<String>,
    value: &str,
    nullable: bool,
) -> crate::Result<FieldRef> {
    let name = name.into();
    let upper = value.trim().to_ascii_uppercase();
    let (element, shape) = if let Some(inner) = upper
        .strip_prefix("VECTOR(")
        .and_then(|inner| inner.strip_suffix(')'))
    {
        let dimension = inner.trim().parse::<usize>().map_err(|error| {
            crate::VqlError::new(
                crate::ErrorCode::InvalidOption,
                "VECTOR dimension is invalid",
            )
            .with_source(error)
        })?;
        (DataType::Float32, vec![dimension])
    } else if let Some(inner) = upper
        .strip_prefix("TENSOR(")
        .and_then(|inner| inner.strip_suffix(')'))
    {
        let mut parts = inner.split(',').map(str::trim);
        let element = parse_boundary_type(parts.next().unwrap_or_default())?;
        let shape = parts
            .map(|dimension| {
                dimension.parse::<usize>().map_err(|error| {
                    crate::VqlError::new(
                        crate::ErrorCode::InvalidOption,
                        "TENSOR dimension is invalid",
                    )
                    .with_source(error)
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;
        (element, shape)
    } else {
        return Ok(Arc::new(Field::new(
            name,
            parse_boundary_type(value)?,
            nullable,
        )));
    };
    let tensor_shape = shape.clone();
    let tensor =
        FixedShapeTensor::try_new(element.clone(), shape, None, None).map_err(|error| {
            crate::VqlError::new(
                crate::ErrorCode::InvalidOption,
                "invalid fixed-shape tensor type",
            )
            .with_source(error)
        })?;
    let size = i32::try_from(tensor.list_size()).map_err(|_| {
        crate::VqlError::new(
            crate::ErrorCode::InvalidOption,
            "TENSOR element count exceeds Arrow limits",
        )
    })?;
    let field =
        Field::new_fixed_size_list(name, Field::new("item", element, false), size, nullable)
            .with_metadata(std::collections::HashMap::from([
                (
                    EXTENSION_TYPE_NAME_KEY.to_owned(),
                    "arrow.fixed_shape_tensor".to_owned(),
                ),
                (
                    EXTENSION_TYPE_METADATA_KEY.to_owned(),
                    serde_json::json!({"shape": tensor_shape}).to_string(),
                ),
            ]));
    Ok(Arc::new(field))
}

pub(crate) fn parse_boundary_type(value: &str) -> crate::Result<DataType> {
    let value = value.trim();
    let upper = value.to_ascii_uppercase();
    let scalar = match upper.as_str() {
        "TINYINT" | "INT8" => Some(DataType::Int8),
        "SMALLINT" | "INT16" => Some(DataType::Int16),
        "INT" | "INTEGER" | "INT32" => Some(DataType::Int32),
        "BIGINT" | "INT64" => Some(DataType::Int64),
        "FLOAT" | "FLOAT32" | "REAL" => Some(DataType::Float32),
        "DOUBLE" | "FLOAT64" => Some(DataType::Float64),
        "UINT8" => Some(DataType::UInt8),
        "IMAGE" => Some(DataType::Struct(crate::types::image_storage_fields())),
        _ => None,
    };
    if let Some(data_type) = scalar {
        return Ok(data_type);
    }
    if let Some(inner) = upper
        .strip_prefix("VECTOR(")
        .and_then(|inner| inner.strip_suffix(')'))
    {
        let length = inner.trim().parse::<i32>().map_err(|error| {
            crate::VqlError::new(
                crate::ErrorCode::InvalidOption,
                "VECTOR dimension is invalid",
            )
            .with_source(error)
        })?;
        return Ok(DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, false)),
            length,
        ));
    }
    if let Some(inner) = upper
        .strip_prefix("TENSOR(")
        .and_then(|inner| inner.strip_suffix(')'))
    {
        let mut parts = inner.split(',').map(str::trim);
        let element = parse_boundary_type(parts.next().unwrap_or_default())?;
        let shape = parts
            .map(|dimension| {
                dimension.parse::<usize>().map_err(|error| {
                    crate::VqlError::new(
                        crate::ErrorCode::InvalidOption,
                        "TENSOR dimension is invalid",
                    )
                    .with_source(error)
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;
        let size = shape.iter().try_fold(1usize, |size, dimension| {
            size.checked_mul(*dimension).ok_or_else(|| {
                crate::VqlError::new(
                    crate::ErrorCode::InvalidOption,
                    "TENSOR element count exceeds platform limits",
                )
            })
        })?;
        return Ok(DataType::FixedSizeList(
            Arc::new(Field::new("item", element, false)),
            i32::try_from(size).map_err(|_| {
                crate::VqlError::new(
                    crate::ErrorCode::InvalidOption,
                    "TENSOR element count exceeds Arrow limits",
                )
            })?,
        ));
    }
    if let Some(inner) = value
        .strip_prefix("STRUCT<")
        .or_else(|| value.strip_prefix("struct<"))
        .and_then(|inner| inner.strip_suffix('>'))
    {
        let fields = split_boundary_fields(inner)?
            .into_iter()
            .map(|field| {
                let (name, data_type) =
                    field
                        .trim()
                        .split_once(char::is_whitespace)
                        .ok_or_else(|| {
                            crate::VqlError::new(
                                crate::ErrorCode::InvalidOption,
                                "STRUCT Model output fields require a name and type",
                            )
                        })?;
                Ok(Arc::new(Field::new(
                    name.to_ascii_lowercase(),
                    parse_boundary_type(data_type.trim())?,
                    true,
                )))
            })
            .collect::<crate::Result<Vec<_>>>()?;
        return Ok(DataType::Struct(Fields::from(fields)));
    }
    Err(crate::VqlError::new(
        crate::ErrorCode::InvalidOption,
        format!("unsupported Model boundary type '{value}'"),
    ))
}

fn split_boundary_fields(value: &str) -> crate::Result<Vec<&str>> {
    let mut fields = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    for (index, character) in value.char_indices() {
        match character {
            '<' | '(' => depth += 1,
            '>' | ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                fields.push(value[start..index].trim());
                start = index + 1;
            }
            _ => {}
        }
    }
    fields.push(value[start..].trim());
    if fields.iter().any(|field| field.is_empty()) {
        return Err(crate::VqlError::new(
            crate::ErrorCode::InvalidOption,
            "STRUCT Model output contains an empty field",
        ));
    }
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::extension::{EXTENSION_TYPE_NAME_KEY, FixedShapeTensor};

    #[test]
    fn tensor_boundary_uses_the_canonical_arrow_extension() {
        let field = parse_boundary_field("features", "TENSOR(FLOAT32, 2, 3)", true).unwrap();
        assert_eq!(
            field
                .metadata()
                .get(EXTENSION_TYPE_NAME_KEY)
                .map(String::as_str),
            Some("arrow.fixed_shape_tensor")
        );
        let tensor = field.try_extension_type::<FixedShapeTensor>().unwrap();
        assert_eq!(tensor.dimensions(), 2);
        assert_eq!(tensor.list_size(), 6);
        assert_eq!(tensor.value_type(), &DataType::Float32);

        let vector = parse_boundary_field("embedding", "VECTOR(4)", false).unwrap();
        let vector = vector.try_extension_type::<FixedShapeTensor>().unwrap();
        assert_eq!(vector.dimensions(), 1);
        assert_eq!(vector.list_size(), 4);
    }
}
