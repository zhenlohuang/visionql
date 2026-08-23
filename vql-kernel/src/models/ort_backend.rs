use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, UInt8Array,
};
use arrow::datatypes::DataType;
use async_trait::async_trait;
use ort::session::{Session, SessionInputValue};
use ort::value::{DynTensor, Outlet, Tensor, TensorElementType, TensorRef, ValueType};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use super::pipeline::{
    BatchingOwner, RuntimeRequestBatch, RuntimeResponseBatch, RuntimeSession, TensorBatch,
    TensorContract,
};
use super::postprocess::{ClassificationPostProcessorFactory, YoloPostProcessorFactory};
use super::preprocess::ImageTensorFactory;
use super::registry::{
    PostProcessorFactory, PreProcessorFactory, RuntimeFactory, RuntimeResolution,
    deserialize_model_options, invalid_option,
};
use super::resolver::{resolve_onnx_source, validate_onnx_source};
use crate::catalog::{
    GenericTensorSpec, ModelInterface, ModelParameter, ModelType, ModelVersion, ProcessorSpec,
    ResolvedExecutionSpec, ResolvedModelDef, RuntimeSpec,
};
use crate::{ErrorCode, Result, VqlError};

const SUPPORTED_TYPES: &[ModelType] = &[ModelType::ObjectDetection, ModelType::ImageClassification];

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct OrtOptions {
    #[serde(default)]
    sha256: Option<String>,
    #[serde(default)]
    input_name: Option<String>,
    #[serde(default)]
    output_name: Option<String>,
    #[serde(default)]
    layout: Option<String>,
    #[serde(default)]
    resize: Option<String>,
    #[serde(default)]
    color_space: Option<String>,
    #[serde(default)]
    image_size: Option<serde_json::Value>,
    #[serde(default)]
    preprocess: Option<String>,
    #[serde(default)]
    mean: Option<Vec<f32>>,
    #[serde(default)]
    std: Option<Vec<f32>>,
    #[serde(default)]
    scale: Option<f32>,
    #[serde(default)]
    pad_value: Option<f32>,
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    labels: Option<serde_json::Value>,
    #[serde(default)]
    box_format: Option<String>,
}

#[derive(Debug)]
pub(super) struct OrtRuntimeFactory;

#[async_trait]
impl RuntimeFactory for OrtRuntimeFactory {
    fn kind(&self) -> &str {
        "onnx-runtime"
    }

    fn supported_types(&self) -> &[ModelType] {
        SUPPORTED_TYPES
    }

    fn validate_declaration(
        &self,
        interface: &ModelInterface,
        version: &ModelVersion,
    ) -> Result<()> {
        if interface.capability.is_none() {
            validate_generic_options(interface, &version.options)?;
            return validate_onnx_source(
                &version.source,
                option_string(&version.options, "sha256")?.as_deref(),
            );
        }
        let options = parse_declared_options(&version.options)?;
        validate_onnx_source(&version.source, options.sha256.as_deref())?;
        validate_declared_onnx_options(interface, &options)
    }

    async fn resolve(
        &self,
        interface: &ModelInterface,
        version: &ModelVersion,
        cache_dir: &Path,
        cancel: CancellationToken,
    ) -> Result<RuntimeResolution> {
        let expected_sha256 = if interface.capability.is_none() {
            option_string(&version.options, "sha256")?
        } else {
            parse_declared_options(&version.options)?.sha256
        };
        let source = version.source.clone();
        let cache_dir = cache_dir.to_path_buf();
        let resolved = tokio::task::spawn_blocking(move || {
            resolve_onnx_source(&source, &cache_dir, expected_sha256.as_deref(), &cancel)
        })
        .await
        .map_err(|error| {
            VqlError::new(ErrorCode::Execution, "ONNX model resolve task failed").with_source(error)
        })??;
        if interface.capability.is_none() {
            let (inputs, outputs) =
                resolve_generic_contract(interface, version, &resolved.resolved_source)?;
            return Ok(RuntimeResolution {
                resolved_source: resolved.resolved_source,
                artifact_hash: resolved.artifact_hash,
                execution: ResolvedExecutionSpec::Generic {
                    runtime: RuntimeSpec {
                        kind: self.kind().to_owned(),
                        protocol: None,
                        options: BTreeMap::new(),
                    },
                    inputs,
                    outputs,
                },
                volatile: false,
            });
        }
        let options = parse_declared_options(&version.options)?;
        let (pre_processor, post_processor) =
            resolve_processor_specs(interface, version, &resolved.resolved_source, options)?;
        Ok(RuntimeResolution {
            resolved_source: resolved.resolved_source,
            artifact_hash: resolved.artifact_hash,
            execution: ResolvedExecutionSpec::Embedded {
                runtime: RuntimeSpec {
                    kind: self.kind().to_owned(),
                    protocol: None,
                    options: BTreeMap::new(),
                },
                pre_processor,
                post_processor,
            },
            volatile: false,
        })
    }

    fn build_embedded(
        &self,
        model: &ResolvedModelDef,
        _runtime: &RuntimeSpec,
        input: &TensorContract,
        output: &TensorContract,
    ) -> Result<Arc<dyn RuntimeSession>> {
        Ok(Arc::new(OrtRuntime::new(
            Path::new(&model.resolved_source),
            input.clone(),
            output.clone(),
        )?))
    }
}

fn parse_declared_options(options: &BTreeMap<String, serde_json::Value>) -> Result<OrtOptions> {
    deserialize_model_options("ONNX_RUNTIME", options)
}

fn option_string(
    options: &BTreeMap<String, serde_json::Value>,
    key: &str,
) -> Result<Option<String>> {
    options
        .get(key)
        .map(|value| {
            value.as_str().map(str::to_owned).ok_or_else(|| {
                VqlError::new(
                    ErrorCode::InvalidOption,
                    format!("OPTIONS.{key} must be a string"),
                )
            })
        })
        .transpose()
}

const GENERIC_INPUT_OPTIONS: &[&str] = &[
    "input_name",
    "layout",
    "resize",
    "color_space",
    "image_size",
    "preprocess",
    "mean",
    "std",
    "scale",
    "pad_value",
];

fn validate_generic_options(
    interface: &ModelInterface,
    options: &BTreeMap<String, serde_json::Value>,
) -> Result<()> {
    let output_fields = generic_output_fields(&interface.return_type)?;
    for key in options.keys() {
        if key == "sha256" {
            continue;
        }
        let (prefix, leaf) = key
            .rsplit_once('.')
            .map_or((None, key.as_str()), |(prefix, leaf)| (Some(prefix), leaf));
        if GENERIC_INPUT_OPTIONS.contains(&leaf) {
            let parameter = match prefix {
                Some(prefix) => interface
                    .parameters
                    .iter()
                    .find(|parameter| parameter.name.eq_ignore_ascii_case(prefix)),
                None if interface.parameters.len() == 1 => interface.parameters.first(),
                None => None,
            }
            .ok_or_else(|| {
                VqlError::new(
                    ErrorCode::InvalidOption,
                    format!(
                        "OPTIONS.{key} must be scoped by a declared parameter name for this multi-input model"
                    ),
                )
            })?;
            if leaf != "input_name" && !parameter.data_type.eq_ignore_ascii_case("IMAGE") {
                return invalid_option(
                    format!("OPTIONS.{key}"),
                    format!(
                        "belongs to IMAGE preprocessing, but parameter '{}' is {}",
                        parameter.name, parameter.data_type
                    ),
                );
            }
            continue;
        }
        if leaf == "output_name" {
            let valid = match prefix {
                Some(prefix) => output_fields
                    .iter()
                    .any(|(name, _)| name.eq_ignore_ascii_case(prefix)),
                None => output_fields.len() == 1,
            };
            if valid {
                continue;
            }
            return invalid_option(
                format!("OPTIONS.{key}"),
                "must be scoped by a declared STRUCT output field for this multi-output model",
            );
        }
        return invalid_option(
            format!("OPTIONS.{key}"),
            "is not owned by the ONNX generic model artifact, input, or output option groups",
        );
    }

    for parameter in &interface.parameters {
        let scoped = scoped_input_options(interface, options, parameter)?;
        validate_declared_onnx_options(interface, &scoped)?;
        if parameter.data_type.eq_ignore_ascii_case("IMAGE") {
            validate_image_processing_options(parameter, &scoped)?;
        }
    }
    Ok(())
}

fn scoped_input_options(
    interface: &ModelInterface,
    options: &BTreeMap<String, serde_json::Value>,
    parameter: &ModelParameter,
) -> Result<OrtOptions> {
    let mut values = serde_json::Map::new();
    for leaf in GENERIC_INPUT_OPTIONS {
        let scoped = format!("{}.{}", parameter.name, leaf);
        let scoped_value = options.get(&scoped);
        let flat_value = (interface.parameters.len() == 1)
            .then(|| options.get(*leaf))
            .flatten();
        if scoped_value.is_some() && flat_value.is_some() {
            return invalid_option(
                format!("OPTIONS.{scoped}"),
                format!("duplicates OPTIONS.{leaf}"),
            );
        }
        if let Some(value) = scoped_value.or(flat_value) {
            values.insert((*leaf).to_owned(), value.clone());
        }
    }
    serde_json::from_value(serde_json::Value::Object(values)).map_err(|error| {
        VqlError::new(
            ErrorCode::InvalidOption,
            format!("OPTIONS for parameter '{}' are invalid", parameter.name),
        )
        .with_source(error)
    })
}

fn validate_image_processing_options(
    parameter: &ModelParameter,
    options: &OrtOptions,
) -> Result<()> {
    let inline = [
        ("mean", options.mean.is_some()),
        ("std", options.std.is_some()),
        ("scale", options.scale.is_some()),
        ("resize", options.resize.is_some()),
        ("pad_value", options.pad_value.is_some()),
    ];
    if options.preprocess.is_some() && inline.iter().any(|(_, present)| *present) {
        return invalid_option(
            format!("OPTIONS.{}.preprocess", parameter.name),
            "conflicts with inline mean/std/scale/resize/pad_value",
        );
    }
    if options.preprocess.is_none() {
        let missing = inline
            .iter()
            .filter_map(|(name, present)| (!present).then_some(*name))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return invalid_option(
                format!("OPTIONS.{}.preprocess", parameter.name),
                format!(
                    "generic IMAGE input requires a preprocess preset or complete inline fields; missing {}",
                    missing.join(", ")
                ),
            );
        }
    }
    if let Some(preprocess) = options.preprocess.as_deref()
        && !preprocess.eq_ignore_ascii_case("imagenet")
    {
        return invalid_option(
            format!("OPTIONS.{}.preprocess", parameter.name),
            "must be the supported 'imagenet' preset",
        );
    }
    Ok(())
}

fn resolve_generic_contract(
    interface: &ModelInterface,
    version: &ModelVersion,
    resolved_source: &str,
) -> Result<(Vec<GenericTensorSpec>, Vec<GenericTensorSpec>)> {
    let inspection = if version.source.starts_with("mock://") {
        mock_generic_inspection(interface)?
    } else {
        inspect_onnx(resolved_source)?
    };
    if inspection.inputs.len() != interface.parameters.len() {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            format!(
                "RESOLVE MODEL found {} graph inputs, but the declared signature has {} parameters",
                inspection.inputs.len(),
                interface.parameters.len()
            ),
        ));
    }

    let mut used_inputs = std::collections::HashSet::new();
    let mut inputs = Vec::with_capacity(interface.parameters.len());
    for (index, parameter) in interface.parameters.iter().enumerate() {
        let options = scoped_input_options(interface, &version.options, parameter)?;
        let outlet = if let Some(name) = options.input_name.as_deref() {
            inspection
                .inputs
                .iter()
                .find(|outlet| outlet.name == name)
                .ok_or_else(|| {
                    VqlError::new(
                        ErrorCode::InvalidOption,
                        format!(
                            "OPTIONS.{}.input_name names missing graph input '{name}'",
                            parameter.name
                        ),
                    )
                })?
        } else {
            &inspection.inputs[index]
        };
        if !used_inputs.insert(outlet.name.clone()) {
            return invalid_option(
                format!("OPTIONS.{}.input_name", parameter.name),
                format!("graph input '{}' is already bound", outlet.name),
            );
        }
        let processor = if parameter.data_type.eq_ignore_ascii_case("IMAGE") {
            Some(resolve_generic_image_processor(
                parameter, outlet, &options,
            )?)
        } else {
            validate_generic_outlet(parameter.data_type.as_str(), outlet, false)?;
            None
        };
        inputs.push(GenericTensorSpec {
            name: outlet.name.clone(),
            data_type: outlet.data_type.clone(),
            shape: outlet.shape.clone(),
            processor,
        });
    }

    let output_fields = generic_output_fields(&interface.return_type)?;
    if inspection.outputs.len() != output_fields.len() {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            format!(
                "RESOLVE MODEL found {} graph outputs, but the declared return type has {} outputs",
                inspection.outputs.len(),
                output_fields.len()
            ),
        ));
    }
    let mut used_outputs = std::collections::HashSet::new();
    let mut outputs = Vec::with_capacity(output_fields.len());
    for (index, (field_name, data_type)) in output_fields.iter().enumerate() {
        let option_key = if output_fields.len() == 1 {
            "output_name".to_owned()
        } else {
            format!("{field_name}.output_name")
        };
        let explicit = option_string(&version.options, &option_key)?;
        let outlet = if let Some(name) = explicit.as_deref() {
            inspection
                .outputs
                .iter()
                .find(|outlet| outlet.name == name)
                .ok_or_else(|| {
                    VqlError::new(
                        ErrorCode::InvalidOption,
                        format!("OPTIONS.{option_key} names missing graph output '{name}'"),
                    )
                })?
        } else if output_fields.len() == 1 {
            &inspection.outputs[index]
        } else {
            inspection
                .outputs
                .iter()
                .find(|outlet| outlet.name.eq_ignore_ascii_case(field_name))
                .ok_or_else(|| {
                    VqlError::new(
                        ErrorCode::InvalidOption,
                        format!(
                            "RESOLVE MODEL cannot bind STRUCT field '{field_name}'; set OPTIONS ({field_name}.output_name = '...')"
                        ),
                    )
                })?
        };
        if !used_outputs.insert(outlet.name.clone()) {
            return invalid_option(
                format!("OPTIONS.{option_key}"),
                format!("graph output '{}' is already bound", outlet.name),
            );
        }
        validate_generic_outlet(data_type, outlet, true)?;
        outputs.push(GenericTensorSpec {
            name: outlet.name.clone(),
            data_type: outlet.data_type.clone(),
            shape: outlet.shape.clone(),
            processor: None,
        });
    }
    Ok((inputs, outputs))
}

fn mock_generic_inspection(interface: &ModelInterface) -> Result<OnnxInspection> {
    let inputs = interface
        .parameters
        .iter()
        .map(|parameter| mock_outlet(&parameter.name, &parameter.data_type, true))
        .collect::<Result<Vec<_>>>()?;
    let output_fields = generic_output_fields(&interface.return_type)?;
    let outputs = output_fields
        .iter()
        .map(|(name, data_type)| mock_outlet(name, data_type, false))
        .collect::<Result<Vec<_>>>()?;
    Ok(OnnxInspection {
        inputs,
        outputs,
        format: None,
        labels: None,
        image_size: None,
    })
}

fn mock_outlet(name: &str, boundary_type: &str, input: bool) -> Result<InspectedOutlet> {
    if boundary_type.eq_ignore_ascii_case("IMAGE") {
        return Ok(InspectedOutlet {
            name: name.to_owned(),
            data_type: "FLOAT32".to_owned(),
            shape: vec![-1, 3, 224, 224],
        });
    }
    let (data_type, dimensions) = generic_boundary_contract(boundary_type)?;
    let mut shape = Vec::with_capacity(dimensions.len() + 1);
    shape.push(-1);
    shape.extend(dimensions);
    Ok(InspectedOutlet {
        name: if input || name != "value" {
            name.to_owned()
        } else {
            "output".to_owned()
        },
        data_type,
        shape,
    })
}

fn generic_output_fields(value: &str) -> Result<Vec<(String, String)>> {
    let value = value.trim();
    let upper = value.to_ascii_uppercase();
    if upper.starts_with("STRUCT<") && upper.ends_with('>') {
        let inner =
            &value[value.find('<').expect("STRUCT has opening bracket") + 1..value.len() - 1];
        return split_generic_fields(inner)?
            .into_iter()
            .map(|field| {
                let (name, data_type) = field.split_once(char::is_whitespace).ok_or_else(|| {
                    VqlError::new(
                        ErrorCode::InvalidOption,
                        "STRUCT Model output fields require a name and type",
                    )
                })?;
                Ok((name.to_ascii_lowercase(), data_type.trim().to_owned()))
            })
            .collect();
    }
    Ok(vec![("value".to_owned(), value.to_owned())])
}

fn split_generic_fields(value: &str) -> Result<Vec<&str>> {
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
    Ok(fields)
}

fn generic_boundary_contract(value: &str) -> Result<(String, Vec<i64>)> {
    let upper = value.trim().to_ascii_uppercase();
    let scalar = match upper.as_str() {
        "TINYINT" => Some("INT8"),
        "SMALLINT" => Some("INT16"),
        "INT" | "INTEGER" => Some("INT32"),
        "BIGINT" => Some("INT64"),
        "FLOAT" | "FLOAT32" | "REAL" => Some("FLOAT32"),
        "DOUBLE" | "FLOAT64" => Some("FLOAT64"),
        "UINT8" => Some("UINT8"),
        _ => None,
    };
    if let Some(data_type) = scalar {
        return Ok((data_type.to_owned(), Vec::new()));
    }
    if let Some(inner) = upper
        .strip_prefix("VECTOR(")
        .and_then(|inner| inner.strip_suffix(')'))
    {
        let size = inner.trim().parse::<i64>().map_err(|error| {
            VqlError::new(ErrorCode::InvalidOption, "VECTOR dimension is invalid")
                .with_source(error)
        })?;
        return Ok(("FLOAT32".to_owned(), vec![size]));
    }
    if let Some(inner) = upper
        .strip_prefix("TENSOR(")
        .and_then(|inner| inner.strip_suffix(')'))
    {
        let mut parts = inner.split(',').map(str::trim);
        let data_type = parts.next().unwrap_or_default();
        let dimensions = parts
            .map(|dimension| {
                dimension.parse::<i64>().map_err(|error| {
                    VqlError::new(ErrorCode::InvalidOption, "TENSOR dimension is invalid")
                        .with_source(error)
                })
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok((data_type.to_owned(), dimensions));
    }
    Err(VqlError::new(
        ErrorCode::InvalidOption,
        format!("unsupported generic Model tensor boundary type '{value}'"),
    ))
}

fn validate_generic_outlet(
    boundary_type: &str,
    outlet: &InspectedOutlet,
    output: bool,
) -> Result<()> {
    let (data_type, dimensions) = generic_boundary_contract(boundary_type)?;
    if outlet.data_type != data_type {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            format!(
                "RESOLVE MODEL tensor '{}' has dtype {}, but the SQL boundary declares {data_type}",
                outlet.name, outlet.data_type
            ),
        ));
    }
    let expected_rank = dimensions.len() + 1;
    if outlet.shape.len() != expected_rank
        || outlet
            .shape
            .iter()
            .skip(1)
            .zip(&dimensions)
            .any(|(actual, expected)| actual != expected)
    {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            format!(
                "RESOLVE MODEL tensor '{}' shape {:?} is incompatible with row boundary [{:?}]",
                outlet.name, outlet.shape, dimensions
            ),
        ));
    }
    if outlet.shape.first().is_some_and(|batch| *batch >= 0) {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            format!(
                "RESOLVE MODEL tensor '{}' requires a dynamic leading batch axis",
                outlet.name
            ),
        ));
    }
    if output && outlet.shape.iter().skip(1).any(|dimension| *dimension < 0) {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            format!(
                "RESOLVE MODEL output '{}' has a dynamic per-row shape; declare a capability preset that normalizes it",
                outlet.name
            ),
        ));
    }
    Ok(())
}

fn resolve_generic_image_processor(
    parameter: &ModelParameter,
    outlet: &InspectedOutlet,
    options: &OrtOptions,
) -> Result<ProcessorSpec> {
    if outlet.data_type != "FLOAT32" {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            format!(
                "RESOLVE MODEL IMAGE input '{}' must be FLOAT32, found {}",
                outlet.name, outlet.data_type
            ),
        ));
    }
    validate_image_processing_options(parameter, options)?;
    let (layout, width, height) = resolve_image_shape(outlet, options, None)?;
    let mut processor_options = BTreeMap::from([
        ("input_name".to_owned(), serde_json::json!(outlet.name)),
        ("width".to_owned(), serde_json::json!(width)),
        ("height".to_owned(), serde_json::json!(height)),
        ("layout".to_owned(), serde_json::json!(layout)),
        (
            "color_space".to_owned(),
            serde_json::json!(options.color_space.as_deref().unwrap_or("rgb")),
        ),
    ]);
    if let Some(preprocess) = &options.preprocess {
        processor_options.insert("preprocess".to_owned(), serde_json::json!(preprocess));
    } else {
        processor_options.insert("mean".to_owned(), serde_json::json!(options.mean));
        processor_options.insert("std".to_owned(), serde_json::json!(options.std));
        processor_options.insert("scale".to_owned(), serde_json::json!(options.scale));
        processor_options.insert("resize".to_owned(), serde_json::json!(options.resize));
        processor_options.insert("pad_value".to_owned(), serde_json::json!(options.pad_value));
    }
    let processor = ProcessorSpec {
        kind: "vision.image_tensor@1".to_owned(),
        options: processor_options,
    };
    ImageTensorFactory
        .validate(&processor)
        .map_err(remap_onnx_option_error)?;
    Ok(processor)
}

fn onnx_data_type(data_type: TensorElementType) -> Option<&'static str> {
    match data_type {
        TensorElementType::Int8 => Some("INT8"),
        TensorElementType::Int16 => Some("INT16"),
        TensorElementType::Int32 => Some("INT32"),
        TensorElementType::Int64 => Some("INT64"),
        TensorElementType::Uint8 => Some("UINT8"),
        TensorElementType::Float32 => Some("FLOAT32"),
        TensorElementType::Float64 => Some("FLOAT64"),
        _ => None,
    }
}

fn validate_declared_onnx_options(interface: &ModelInterface, options: &OrtOptions) -> Result<()> {
    if options.input_name.as_deref().is_some_and(str::is_empty) {
        return invalid_option("OPTIONS.input_name", "must be a non-empty string");
    }
    if options.output_name.as_deref().is_some_and(str::is_empty) {
        return invalid_option("OPTIONS.output_name", "must be a non-empty string");
    }
    if interface.capability.is_none() {
        let inline = [
            options.mean.is_some(),
            options.std.is_some(),
            options.scale.is_some(),
            options.resize.is_some(),
            options.pad_value.is_some(),
        ];
        if options.preprocess.is_some() && inline.iter().any(|present| *present) {
            return invalid_option(
                "OPTIONS.preprocess",
                "conflicts with inline mean/std/scale/resize/pad_value",
            );
        }
    }
    if let Some(image_size) = options.image_size.as_ref() {
        parse_image_size(image_size)?;
    }
    Ok(())
}

fn resolve_processor_specs(
    interface: &ModelInterface,
    version: &ModelVersion,
    resolved_source: &str,
    options: OrtOptions,
) -> Result<(ProcessorSpec, ProcessorSpec)> {
    let inspection = if version.source.starts_with("mock://") {
        OnnxInspection::mock(&version.source)
    } else {
        inspect_onnx(resolved_source)?
    };
    let input_name = resolve_tensor_name(
        "input_name",
        options.input_name.as_deref(),
        &inspection.inputs,
    )?;
    let output_name = resolve_tensor_name(
        "output_name",
        options.output_name.as_deref(),
        &inspection.outputs,
    )?;
    let input = inspection
        .inputs
        .iter()
        .find(|outlet| outlet.name == input_name)
        .expect("resolved input name is present");
    let (layout, width, height) =
        resolve_image_shape(input, &options, inspection.image_size.as_ref())?;
    if interface.capability.is_none()
        && interface
            .parameters
            .iter()
            .any(|value| value.data_type == "IMAGE")
    {
        let inline = [
            ("mean", options.mean.is_some()),
            ("std", options.std.is_some()),
            ("scale", options.scale.is_some()),
            ("resize", options.resize.is_some()),
            ("pad_value", options.pad_value.is_some()),
        ];
        if options.preprocess.is_none() {
            let missing = inline
                .iter()
                .filter_map(|(name, present)| (!present).then_some(*name))
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                return invalid_option(
                    "OPTIONS.preprocess",
                    format!(
                        "generic IMAGE input requires a preprocess preset or complete inline fields; missing {}",
                        missing.join(", ")
                    ),
                );
            }
        }
    }
    let format = options
        .format
        .or(inspection.format)
        .ok_or_else(|| {
            VqlError::new(
                ErrorCode::InvalidOption,
                "RESOLVE MODEL cannot determine output decoding; set OPTIONS (format = '...')",
            )
        })?
        .to_ascii_lowercase();
    validate_output_shape(
        &format,
        inspection
            .outputs
            .iter()
            .find(|value| value.name == output_name)
            .expect("resolved output exists"),
    )?;
    let capability = interface
        .capability
        .expect("capability Models use the vision pipeline");
    let post_processor_kind = match (capability, format.as_str()) {
        (ModelType::ObjectDetection, "yolo_e2e") => "vision.yolo_e2e@1",
        (ModelType::ObjectDetection, "yolo_raw") => "vision.yolo_raw@1",
        (ModelType::ObjectDetection, "xywh_normalized") => "vision.xywh_normalized@1",
        (ModelType::ImageClassification, "classification") => "vision.image_classification@1",
        (ModelType::ObjectDetection, _) => {
            return invalid_option(
                "OPTIONS.format",
                "must be 'yolo_e2e', 'yolo_raw', or 'xywh_normalized'",
            );
        }
        (ModelType::ImageClassification, _) => {
            return invalid_option("OPTIONS.format", "must be 'classification'");
        }
    };
    let labels = options.labels.or(inspection.labels).ok_or_else(|| {
        VqlError::new(
            ErrorCode::InvalidOption,
            "RESOLVE MODEL cannot determine class labels; set OPTIONS (labels = [...])",
        )
    })?;
    let mut input_options = BTreeMap::from([
        ("input_name".to_owned(), serde_json::json!(input_name)),
        ("width".to_owned(), serde_json::json!(width)),
        ("height".to_owned(), serde_json::json!(height)),
        ("layout".to_owned(), serde_json::json!(layout)),
    ]);
    if let Some(value) = options.resize {
        input_options.insert("resize".to_owned(), serde_json::json!(value));
    }
    if let Some(value) = options.color_space {
        input_options.insert("color_space".to_owned(), serde_json::json!(value));
    }
    if let Some(value) = options.preprocess {
        input_options.insert("preprocess".to_owned(), serde_json::json!(value));
    }
    if let Some(value) = options.mean {
        input_options.insert("mean".to_owned(), serde_json::json!(value));
    }
    if let Some(value) = options.std {
        input_options.insert("std".to_owned(), serde_json::json!(value));
    }
    if let Some(value) = options.scale {
        input_options.insert("scale".to_owned(), serde_json::json!(value));
    }
    if let Some(value) = options.pad_value {
        input_options.insert("pad_value".to_owned(), serde_json::json!(value));
    }
    let mut output_options = BTreeMap::from([
        ("output_name".to_owned(), serde_json::json!(output_name)),
        ("labels".to_owned(), labels),
    ]);
    if let Some(value) = options.box_format {
        output_options.insert("box_format".to_owned(), serde_json::json!(value));
    }
    let pre_processor = ProcessorSpec {
        kind: "vision.image_tensor@1".to_owned(),
        options: input_options,
    };
    let post_processor = ProcessorSpec {
        kind: post_processor_kind.to_owned(),
        options: output_options,
    };
    ImageTensorFactory
        .validate(&pre_processor)
        .map_err(remap_onnx_option_error)?;
    match post_processor.kind.as_str() {
        "vision.yolo_e2e@1" => YoloPostProcessorFactory::end_to_end().validate(&post_processor),
        "vision.yolo_raw@1" => YoloPostProcessorFactory::raw().validate(&post_processor),
        "vision.xywh_normalized@1" => {
            YoloPostProcessorFactory::xywh_normalized().validate(&post_processor)
        }
        "vision.image_classification@1" => {
            ClassificationPostProcessorFactory.validate(&post_processor)
        }
        _ => unreachable!("format mapping is exhaustive"),
    }
    .map_err(remap_onnx_option_error)?;
    Ok((pre_processor, post_processor))
}

#[derive(Debug)]
struct InspectedOutlet {
    name: String,
    data_type: String,
    shape: Vec<i64>,
}

#[derive(Debug)]
struct OnnxInspection {
    inputs: Vec<InspectedOutlet>,
    outputs: Vec<InspectedOutlet>,
    format: Option<String>,
    labels: Option<serde_json::Value>,
    image_size: Option<serde_json::Value>,
}

impl OnnxInspection {
    fn mock(source: &str) -> Self {
        let label = source
            .trim_start_matches("mock://")
            .split(['?', '#'])
            .next()
            .filter(|value| !value.is_empty())
            .unwrap_or("object");
        Self {
            inputs: vec![InspectedOutlet {
                name: "images".to_owned(),
                data_type: "FLOAT32".to_owned(),
                shape: vec![-1, 3, 640, 640],
            }],
            outputs: vec![InspectedOutlet {
                name: "output0".to_owned(),
                data_type: "FLOAT32".to_owned(),
                shape: vec![-1, 300, 6],
            }],
            format: Some("yolo_e2e".to_owned()),
            labels: Some(serde_json::json!([label])),
            image_size: Some(serde_json::json!([640, 640])),
        }
    }
}

fn inspect_onnx(path: &str) -> Result<OnnxInspection> {
    let session = Session::builder()
        .and_then(|mut builder| builder.commit_from_file(path))
        .map_err(|error| {
            VqlError::new(
                ErrorCode::Execution,
                "RESOLVE MODEL could not inspect the ONNX graph",
            )
            .with_source(error)
        })?;
    let inputs = inspect_outlets(session.inputs(), "input")?;
    let outputs = inspect_outlets(session.outputs(), "output")?;
    let metadata = session.metadata().ok();
    let format = metadata.as_ref().and_then(|metadata| {
        metadata
            .custom("visionql.output_format")
            .or_else(|| metadata.custom("output_format"))
            .or_else(|| metadata.custom("format"))
    });
    let labels = metadata
        .as_ref()
        .and_then(|metadata| metadata.custom("names"))
        .and_then(|value| parse_metadata_labels(&value));
    let image_size = metadata
        .as_ref()
        .and_then(|metadata| metadata.custom("visionql.image_size"))
        .and_then(|value| serde_json::from_str(&value).ok());
    Ok(OnnxInspection {
        inputs,
        outputs,
        format,
        labels,
        image_size,
    })
}

fn inspect_outlets(outlets: &[Outlet], role: &str) -> Result<Vec<InspectedOutlet>> {
    outlets
        .iter()
        .map(|outlet| {
            let ValueType::Tensor { ty, shape, .. } = outlet.dtype() else {
                return Err(VqlError::new(
                    ErrorCode::Execution,
                    format!("ONNX {role} '{}' must be a tensor", outlet.name()),
                ));
            };
            let data_type = onnx_data_type(*ty).ok_or_else(|| {
                VqlError::new(
                    ErrorCode::Execution,
                    format!(
                        "ONNX {role} '{}' uses unsupported tensor dtype {ty:?}",
                        outlet.name()
                    ),
                )
            })?;
            Ok(InspectedOutlet {
                name: outlet.name().to_owned(),
                data_type: data_type.to_owned(),
                shape: shape.iter().copied().collect(),
            })
        })
        .collect()
}

fn parse_metadata_labels(value: &str) -> Option<serde_json::Value> {
    let parsed = serde_json::from_str::<serde_json::Value>(value)
        .ok()
        .or_else(|| parse_python_label_map(value))?;
    match parsed {
        serde_json::Value::Array(values) => Some(serde_json::Value::Array(values)),
        serde_json::Value::Object(values) => {
            let mut indexed = values
                .into_iter()
                .map(|(index, label)| {
                    Some((index.parse::<usize>().ok()?, label.as_str()?.to_owned()))
                })
                .collect::<Option<Vec<_>>>()?;
            indexed.sort_by_key(|(index, _)| *index);
            Some(serde_json::json!(
                indexed
                    .into_iter()
                    .map(|(_, label)| label)
                    .collect::<Vec<_>>()
            ))
        }
        _ => None,
    }
}

fn parse_python_label_map(value: &str) -> Option<serde_json::Value> {
    let value = value.trim().strip_prefix('{')?.strip_suffix('}')?;
    let mut labels = serde_json::Map::new();
    for entry in value.split(',') {
        let (index, label) = entry.split_once(':')?;
        let index = index.trim().trim_matches(['\'', '"']);
        let label = label.trim().trim_matches(['\'', '"']);
        index.parse::<usize>().ok()?;
        labels.insert(index.to_owned(), serde_json::json!(label));
    }
    Some(serde_json::Value::Object(labels))
}

fn resolve_tensor_name(
    option: &str,
    explicit: Option<&str>,
    outlets: &[InspectedOutlet],
) -> Result<String> {
    if let Some(explicit) = explicit {
        if outlets.iter().any(|outlet| outlet.name == explicit) {
            return Ok(explicit.to_owned());
        }
        return invalid_option(
            format!("OPTIONS.{option}"),
            format!("tensor '{explicit}' does not exist in the graph"),
        );
    }
    if let [outlet] = outlets {
        return Ok(outlet.name.clone());
    }
    invalid_option(
        format!("OPTIONS.{option}"),
        format!(
            "is required because the graph has {} tensors",
            outlets.len()
        ),
    )
}

fn resolve_image_shape(
    input: &InspectedOutlet,
    options: &OrtOptions,
    metadata_image_size: Option<&serde_json::Value>,
) -> Result<(String, u32, u32)> {
    if input.shape.len() != 4 {
        return invalid_option(
            "OPTIONS.image_size",
            format!(
                "IMAGE input '{}' must have rank 4 including the batch axis",
                input.name
            ),
        );
    }
    let inferred_layout = match input.shape.as_slice() {
        [_, 3, _, _] => Some("nchw"),
        [_, _, _, 3] => Some("nhwc"),
        _ => None,
    };
    let layout = match (options.layout.as_deref(), inferred_layout) {
        (Some(explicit), Some(inferred)) if !explicit.eq_ignore_ascii_case(inferred) => {
            return invalid_option(
                "OPTIONS.layout",
                format!("'{explicit}' contradicts graph shape {:?}", input.shape),
            );
        }
        (Some(explicit), _)
            if matches!(explicit.to_ascii_lowercase().as_str(), "nchw" | "nhwc") =>
        {
            explicit.to_ascii_lowercase()
        }
        (Some(_), _) => return invalid_option("OPTIONS.layout", "must be 'nchw' or 'nhwc'"),
        (None, Some(inferred)) => inferred.to_owned(),
        (None, None) => {
            return invalid_option(
                "OPTIONS.layout",
                format!("is required for ambiguous graph shape {:?}", input.shape),
            );
        }
    };
    let (height, width) = if layout == "nchw" {
        (input.shape[2], input.shape[3])
    } else {
        (input.shape[1], input.shape[2])
    };
    let explicit_size = options
        .image_size
        .as_ref()
        .map(parse_image_size)
        .transpose()?;
    let discovered_size = explicit_size
        .or_else(|| metadata_image_size.and_then(|value| parse_image_size(value).ok()));
    if height > 0 && width > 0 {
        let graph_size = (
            u32::try_from(width).unwrap(),
            u32::try_from(height).unwrap(),
        );
        if discovered_size.is_some_and(|explicit| explicit != graph_size) {
            return invalid_option(
                "OPTIONS.image_size",
                format!(
                    "contradicts static graph size {}x{}",
                    graph_size.0, graph_size.1
                ),
            );
        }
        Ok((layout, graph_size.0, graph_size.1))
    } else {
        let (width, height) = discovered_size.ok_or_else(|| {
            VqlError::new(
                ErrorCode::InvalidOption,
                "RESOLVE MODEL cannot determine dynamic image dimensions; set OPTIONS (image_size = ...)",
            )
        })?;
        Ok((layout, width, height))
    }
}

fn parse_image_size(value: &serde_json::Value) -> Result<(u32, u32)> {
    if let Some(side) = value.as_u64().and_then(|value| u32::try_from(value).ok())
        && side > 0
    {
        return Ok((side, side));
    }
    if let Some(values) = value.as_array()
        && let [width, height] = values.as_slice()
        && let (Some(width), Some(height)) = (
            width.as_u64().and_then(|value| u32::try_from(value).ok()),
            height.as_u64().and_then(|value| u32::try_from(value).ok()),
        )
        && width > 0
        && height > 0
    {
        return Ok((width, height));
    }
    invalid_option(
        "OPTIONS.image_size",
        "must be a positive integer or [width, height]",
    )
}

fn validate_output_shape(format: &str, output: &InspectedOutlet) -> Result<()> {
    let compatible = match format {
        "yolo_e2e" => output
            .shape
            .last()
            .is_some_and(|dimension| *dimension == 6 || *dimension < 0),
        "yolo_raw" => output.shape.len() == 3,
        "xywh_normalized" => output
            .shape
            .last()
            .is_some_and(|dimension| *dimension >= 6 || *dimension < 0),
        "classification" => output.shape.len() == 2,
        _ => true,
    };
    if compatible {
        Ok(())
    } else {
        invalid_option(
            "OPTIONS.format",
            format!(
                "'{format}' is incompatible with output shape {:?}",
                output.shape
            ),
        )
    }
}

fn remap_onnx_option_error(mut error: VqlError) -> VqlError {
    if error.code == ErrorCode::InvalidOption {
        error.message = error
            .message
            .replace("pre_processor.options", "OPTIONS")
            .replace("post_processor.options", "OPTIONS");
    }
    error
}

pub(super) struct GenericOrtRuntime {
    session: Mutex<Session>,
}

impl std::fmt::Debug for GenericOrtRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GenericOrtRuntime")
            .finish_non_exhaustive()
    }
}

impl GenericOrtRuntime {
    pub(super) fn new(model: &ResolvedModelDef) -> Result<Self> {
        let ResolvedExecutionSpec::Generic {
            inputs, outputs, ..
        } = &model.execution
        else {
            return Err(VqlError::new(
                ErrorCode::Internal,
                "generic ONNX Runtime requires a generic resolved contract",
            ));
        };
        let session = Session::builder()
            .and_then(|mut builder| builder.commit_from_file(&model.resolved_source))
            .map_err(|error| {
                VqlError::new(
                    ErrorCode::Execution,
                    format!(
                        "failed to load generic ONNX model '{}'",
                        model.resolved_source
                    ),
                )
                .with_source(error)
            })?;
        validate_generic_session_outlets("input", session.inputs(), inputs)?;
        validate_generic_session_outlets("output", session.outputs(), outputs)?;
        Ok(Self {
            session: Mutex::new(session),
        })
    }

    pub(super) fn run(
        &self,
        inputs: &[(GenericTensorSpec, ArrayRef)],
        outputs: &[GenericTensorSpec],
    ) -> Result<Vec<ArrayRef>> {
        let input_values = inputs
            .iter()
            .map(|(spec, array)| {
                Ok((
                    spec.name.clone(),
                    SessionInputValue::from(array_to_ort_tensor(spec, array)?),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut session = self.session.lock().map_err(|_| {
            VqlError::new(
                ErrorCode::Internal,
                "generic ONNX session lock was poisoned",
            )
        })?;
        let values = session.run(input_values).map_err(|error| {
            VqlError::new(ErrorCode::Execution, "generic ONNX inference failed").with_source(error)
        })?;
        outputs
            .iter()
            .map(|spec| {
                let value = values.get(&spec.name).ok_or_else(|| {
                    VqlError::new(
                        ErrorCode::Execution,
                        format!("generic ONNX output '{}' is missing", spec.name),
                    )
                })?;
                ort_tensor_to_array(spec, value)
            })
            .collect()
    }
}

fn validate_generic_session_outlets(
    role: &str,
    outlets: &[Outlet],
    specs: &[GenericTensorSpec],
) -> Result<()> {
    for spec in specs {
        let outlet = outlets
            .iter()
            .find(|outlet| outlet.name() == spec.name)
            .ok_or_else(|| {
                VqlError::new(
                    ErrorCode::Execution,
                    format!("generic ONNX {role} '{}' is missing", spec.name),
                )
            })?;
        let ValueType::Tensor { ty, shape, .. } = outlet.dtype() else {
            return Err(VqlError::new(
                ErrorCode::Execution,
                format!("generic ONNX {role} '{}' is not a tensor", spec.name),
            ));
        };
        if onnx_data_type(*ty) != Some(spec.data_type.as_str())
            || shape.len() != spec.shape.len()
            || shape
                .iter()
                .zip(&spec.shape)
                .any(|(actual, expected)| *actual >= 0 && *expected >= 0 && actual != expected)
        {
            return Err(VqlError::new(
                ErrorCode::Execution,
                format!(
                    "generic ONNX {role} '{}' no longer matches its resolved dtype/shape contract",
                    spec.name
                ),
            ));
        }
    }
    Ok(())
}

fn array_to_ort_tensor(spec: &GenericTensorSpec, array: &ArrayRef) -> Result<DynTensor> {
    let values = tensor_values(array)?;
    if values.null_count() != 0 {
        return Err(VqlError::new(
            ErrorCode::Internal,
            format!(
                "generic tensor '{}' contains compacted NULL values",
                spec.name
            ),
        ));
    }
    let mut shape = spec
        .shape
        .iter()
        .enumerate()
        .map(|(index, dimension)| {
            if index == 0 {
                return Ok(array.len());
            }
            usize::try_from(*dimension).map_err(|_| {
                VqlError::new(
                    ErrorCode::Execution,
                    format!("generic tensor '{}' has an invalid shape", spec.name),
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    shape[0] = array.len();
    macro_rules! build_tensor {
        ($array_type:ty, $rust_type:ty) => {{
            let values = values
                .as_any()
                .downcast_ref::<$array_type>()
                .ok_or_else(|| tensor_dtype_error(spec, values.data_type()))?;
            Tensor::<$rust_type>::from_array((shape, values.values().to_vec()))
                .map(Tensor::upcast)
                .map_err(|error| {
                    VqlError::new(
                        ErrorCode::Execution,
                        format!("failed to build generic ONNX input '{}'", spec.name),
                    )
                    .with_source(error)
                })
        }};
    }
    match spec.data_type.as_str() {
        "INT8" => build_tensor!(Int8Array, i8),
        "INT16" => build_tensor!(Int16Array, i16),
        "INT32" => build_tensor!(Int32Array, i32),
        "INT64" => build_tensor!(Int64Array, i64),
        "UINT8" => build_tensor!(UInt8Array, u8),
        "FLOAT32" => build_tensor!(Float32Array, f32),
        "FLOAT64" => build_tensor!(Float64Array, f64),
        unsupported => Err(VqlError::new(
            ErrorCode::Execution,
            format!("generic ONNX dtype '{unsupported}' is unsupported"),
        )),
    }
}

fn tensor_values(array: &ArrayRef) -> Result<ArrayRef> {
    if let Some(list) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        let start = list.offset() * list.value_length() as usize;
        let length = list.len() * list.value_length() as usize;
        Ok(list.values().slice(start, length))
    } else {
        Ok(Arc::clone(array))
    }
}

fn tensor_dtype_error(spec: &GenericTensorSpec, actual: &DataType) -> VqlError {
    VqlError::new(
        ErrorCode::Execution,
        format!(
            "generic tensor '{}' has Arrow dtype {actual}, expected {}",
            spec.name, spec.data_type
        ),
    )
}

fn ort_tensor_to_array(spec: &GenericTensorSpec, value: &ort::value::DynValue) -> Result<ArrayRef> {
    macro_rules! extract_tensor {
        ($rust_type:ty, $array_type:ty) => {{
            let (shape, values) = value.try_extract_tensor::<$rust_type>().map_err(|error| {
                VqlError::new(
                    ErrorCode::Execution,
                    format!("generic ONNX output '{}' has the wrong dtype", spec.name),
                )
                .with_source(error)
            })?;
            validate_generic_output_shape(spec, shape)?;
            let values: ArrayRef = Arc::new(<$array_type>::from(values.to_vec()));
            if shape.len() == 1 {
                Ok(values)
            } else {
                TensorBatch::try_new(spec.name.clone(), shape.to_vec(), values, None)
                    .map(TensorBatch::into_array)
            }
        }};
    }
    match spec.data_type.as_str() {
        "INT8" => extract_tensor!(i8, Int8Array),
        "INT16" => extract_tensor!(i16, Int16Array),
        "INT32" => extract_tensor!(i32, Int32Array),
        "INT64" => extract_tensor!(i64, Int64Array),
        "UINT8" => extract_tensor!(u8, UInt8Array),
        "FLOAT32" => extract_tensor!(f32, Float32Array),
        "FLOAT64" => extract_tensor!(f64, Float64Array),
        unsupported => Err(VqlError::new(
            ErrorCode::Execution,
            format!("generic ONNX dtype '{unsupported}' is unsupported"),
        )),
    }
}

fn validate_generic_output_shape(spec: &GenericTensorSpec, actual: &[i64]) -> Result<()> {
    if actual.len() != spec.shape.len()
        || actual
            .iter()
            .skip(1)
            .zip(spec.shape.iter().skip(1))
            .any(|(actual, expected)| actual != expected)
    {
        return Err(VqlError::new(
            ErrorCode::Execution,
            format!(
                "generic ONNX output '{}' shape {actual:?} does not match resolved shape {:?}",
                spec.name, spec.shape
            ),
        ));
    }
    Ok(())
}

pub(super) struct OrtRuntime {
    session: Arc<Mutex<Session>>,
    input_contract: TensorContract,
    output_contract: TensorContract,
}

impl std::fmt::Debug for OrtRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OrtRuntime")
            .field("input_contract", &self.input_contract)
            .field("output_contract", &self.output_contract)
            .finish_non_exhaustive()
    }
}

impl OrtRuntime {
    fn new(
        path: &Path,
        input_contract: TensorContract,
        output_contract: TensorContract,
    ) -> Result<Self> {
        // CoreML's NeuralNetwork format binds a dynamic batch dimension to 1, so any batched
        // call fails at predict time. Restricting the EP to static shapes keeps CoreML for
        // fixed-shape models — the only case where it also outperforms the CPU provider — and
        // falls back to CPU for the dynamic-batch exports VisionQL requires.
        #[cfg(target_os = "macos")]
        let coreml = Session::builder()
            .ok()
            .and_then(|builder| {
                builder
                    .with_execution_providers([ort::ep::CoreML::default()
                        .with_static_input_shapes(true)
                        .build()])
                    .ok()
            })
            .and_then(|mut builder| builder.commit_from_file(path).ok());
        #[cfg(not(target_os = "macos"))]
        let coreml: Option<Session> = None;

        let session = match coreml {
            Some(session) => session,
            None => Session::builder()
                .and_then(|mut builder| builder.commit_from_file(path))
                .map_err(|error| {
                    VqlError::new(
                        ErrorCode::Execution,
                        format!("failed to load ONNX model '{}': {error}", path.display()),
                    )
                })?,
        };
        validate_outlet("input", session.inputs(), &input_contract)?;
        validate_outlet("output", session.outputs(), &output_contract)?;
        Ok(Self {
            session: Arc::new(Mutex::new(session)),
            input_contract,
            output_contract,
        })
    }
}

fn validate_outlet(role: &str, outlets: &[Outlet], contract: &TensorContract) -> Result<()> {
    let outlet = outlets
        .iter()
        .find(|outlet| outlet.name() == contract.name)
        .ok_or_else(|| {
            VqlError::new(
                ErrorCode::Execution,
                format!(
                    "ONNX model does not declare {role} tensor '{}'",
                    contract.name
                ),
            )
        })?;
    let ValueType::Tensor { ty, shape, .. } = outlet.dtype() else {
        return Err(VqlError::new(
            ErrorCode::Execution,
            format!("ONNX {role} tensor '{}' is not a tensor", contract.name),
        ));
    };
    let expected_type = match &contract.dtype {
        DataType::Float32 => TensorElementType::Float32,
        unsupported => {
            return Err(VqlError::new(
                ErrorCode::Execution,
                format!(
                    "ONNX {role} tensor '{}' uses unsupported dtype {unsupported}",
                    contract.name
                ),
            ));
        }
    };
    if ty != &expected_type {
        return Err(VqlError::new(
            ErrorCode::Execution,
            format!(
                "ONNX {role} tensor '{}' has dtype {ty:?}, expected {}",
                contract.name, contract.dtype
            ),
        ));
    }
    if shape.len() != contract.shape.len()
        || shape
            .iter()
            .zip(&contract.shape)
            .any(|(declared, expected)| *declared >= 0 && *expected >= 0 && declared != expected)
    {
        return Err(VqlError::new(
            ErrorCode::Execution,
            format!(
                "ONNX {role} tensor '{}' shape {:?} is incompatible with contract {:?}",
                contract.name, shape, contract.shape
            ),
        ));
    }
    Ok(())
}

#[async_trait]
impl RuntimeSession for OrtRuntime {
    fn kind(&self) -> &str {
        "onnx-runtime"
    }

    fn input_contract(&self) -> &TensorContract {
        &self.input_contract
    }

    fn output_contract(&self) -> &TensorContract {
        &self.output_contract
    }

    fn batching_owner(&self) -> BatchingOwner {
        BatchingOwner::VisionQl
    }

    async fn infer(
        &self,
        batch: RuntimeRequestBatch,
        cancel: CancellationToken,
        budget: crate::resources::QueryBudget,
    ) -> Result<RuntimeResponseBatch> {
        self.input_contract
            .validate_batch("ONNX input", &batch.input)?;
        let session = Arc::clone(&self.session);
        let output_contract = self.output_contract.clone();
        let task = tokio::task::spawn_blocking(move || {
            run_session(session, batch, output_contract, budget)
        });
        tokio::select! {
            _ = cancel.cancelled() => Err(VqlError::new(ErrorCode::QueryCancelled, "query cancelled")),
            result = task => result.map_err(|error| {
                VqlError::new(ErrorCode::Execution, "ONNX inference task failed").with_source(error)
            })?,
        }
    }
}

fn run_session(
    session: Arc<Mutex<Session>>,
    batch: RuntimeRequestBatch,
    output_contract: TensorContract,
    budget: crate::resources::QueryBudget,
) -> Result<RuntimeResponseBatch> {
    let input_name = batch.input.name().to_owned();
    let shape = batch
        .input
        .shape()
        .iter()
        .map(|value| {
            usize::try_from(*value).map_err(|_| {
                VqlError::new(
                    ErrorCode::Execution,
                    "ONNX input shape must be non-negative",
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let tensor = TensorRef::from_array_view((shape, batch.input.as_f32("ONNX input")?)).map_err(
        |error| {
            VqlError::new(ErrorCode::Execution, "failed to build ONNX input tensor")
                .with_source(error)
        },
    )?;
    let mut session = session
        .lock()
        .map_err(|_| VqlError::new(ErrorCode::Internal, "ONNX session lock was poisoned"))?;
    let outputs = session
        .run(ort::inputs![input_name.as_str() => tensor])
        .map_err(|error| {
            VqlError::new(ErrorCode::Execution, "ONNX inference failed").with_source(error)
        })?;
    let mut response = BTreeMap::new();
    let mut reservations = Vec::new();
    for output_name in batch.output_names {
        let output = outputs.get(&output_name).ok_or_else(|| {
            VqlError::new(
                ErrorCode::Execution,
                format!("ONNX output '{output_name}' is missing"),
            )
        })?;
        let (shape, values) = output.try_extract_tensor::<f32>().map_err(|error| {
            VqlError::new(ErrorCode::Execution, "ONNX output must be float32").with_source(error)
        })?;
        reservations.push(budget.reserve(
            crate::QueryResource::ModelTensor,
            values.len().saturating_mul(std::mem::size_of::<f32>()),
        )?);
        let tensor =
            TensorBatch::from_f32(output_name.clone(), shape.to_vec(), values.to_vec(), None)?;
        output_contract.validate_batch("ONNX output", &tensor)?;
        response.insert(output_name, tensor);
    }
    Ok(RuntimeResponseBatch {
        outputs: response,
        _reservations: reservations,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ort::value::{Shape, SymbolicDimensions};

    fn outlet(name: &str, shape: &[i64]) -> Outlet {
        Outlet::new(
            name,
            ValueType::Tensor {
                ty: TensorElementType::Float32,
                shape: Shape::new(shape.iter().copied()),
                dimension_symbols: SymbolicDimensions::new(
                    shape.iter().map(|_| String::new()).collect::<Vec<_>>(),
                ),
            },
        )
    }

    #[test]
    fn contract_validation_names_a_missing_onnx_tensor() {
        let error = validate_outlet(
            "input",
            &[outlet("pixels", &[-1, 3, 640, 640])],
            &TensorContract {
                name: "images".to_owned(),
                dtype: DataType::Float32,
                shape: vec![-1, 3, 640, 640],
            },
        )
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::Execution);
        assert!(error.message.contains("images"));
    }

    #[test]
    fn contract_validation_names_an_incompatible_onnx_resolution() {
        let error = validate_outlet(
            "input",
            &[outlet("images", &[-1, 3, 320, 320])],
            &TensorContract {
                name: "images".to_owned(),
                dtype: DataType::Float32,
                shape: vec![-1, 3, 640, 640],
            },
        )
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::Execution);
        assert!(error.message.contains("images"));
        assert!(error.message.contains("640"));
    }

    #[test]
    fn declaration_validates_runtime_scoped_processor_options_locally() {
        let interface = ModelInterface {
            capability: Some(ModelType::ObjectDetection),
            parameters: Vec::new(),
            semantic_arguments: Vec::new(),
            return_type: "detections".to_owned(),
            processing_family: "vision.object_detection".to_owned(),
            deterministic: true,
        };
        let version = ModelVersion {
            name: "v1".to_owned(),
            source: "mock://person".to_owned(),
            runtime_kind: "onnx-runtime".to_owned(),
            options: BTreeMap::from([("image_size".to_owned(), serde_json::json!(0))]),
            declaration_fingerprint: "declaration".to_owned(),
            resolved: None,
            created_at: 0,
        };

        let error = OrtRuntimeFactory
            .validate_declaration(&interface, &version)
            .unwrap_err();

        assert_eq!(error.code, ErrorCode::InvalidOption);
        assert!(error.message.contains("OPTIONS.image_size"));
    }

    fn generic_interface(parameters: Vec<(&str, &str)>, return_type: &str) -> ModelInterface {
        ModelInterface {
            capability: None,
            parameters: parameters
                .into_iter()
                .map(|(name, data_type)| ModelParameter {
                    name: name.to_owned(),
                    data_type: data_type.to_owned(),
                    constant: false,
                    optional: false,
                })
                .collect(),
            semantic_arguments: Vec::new(),
            return_type: return_type.to_owned(),
            processing_family: "generic.tensor".to_owned(),
            deterministic: true,
        }
    }

    #[test]
    fn generic_options_require_parameter_scopes_and_complete_image_processing() {
        let interface =
            generic_interface(vec![("image", "IMAGE"), ("scale", "FLOAT")], "VECTOR(4)");
        let error = validate_generic_options(
            &interface,
            &BTreeMap::from([("input_name".to_owned(), serde_json::json!("pixels"))]),
        )
        .unwrap_err();
        assert!(error.message.contains("scoped"));

        let error = validate_generic_options(
            &interface,
            &BTreeMap::from([
                ("image.input_name".to_owned(), serde_json::json!("pixels")),
                ("scale.input_name".to_owned(), serde_json::json!("gain")),
                ("image.mean".to_owned(), serde_json::json!([0.0, 0.0, 0.0])),
            ]),
        )
        .unwrap_err();
        assert!(error.message.contains("missing"));
        assert!(error.message.contains("std"));

        validate_generic_options(
            &interface,
            &BTreeMap::from([
                ("image.input_name".to_owned(), serde_json::json!("pixels")),
                ("scale.input_name".to_owned(), serde_json::json!("gain")),
                ("image.preprocess".to_owned(), serde_json::json!("imagenet")),
            ]),
        )
        .unwrap();
    }

    #[test]
    fn generic_contract_validates_scalar_and_static_tensor_shapes() {
        let scalar = InspectedOutlet {
            name: "value".to_owned(),
            data_type: "FLOAT64".to_owned(),
            shape: vec![-1],
        };
        validate_generic_outlet("DOUBLE", &scalar, false).unwrap();

        let dynamic = InspectedOutlet {
            name: "features".to_owned(),
            data_type: "FLOAT32".to_owned(),
            shape: vec![-1, -1],
        };
        let error = validate_generic_outlet("VECTOR(4)", &dynamic, true).unwrap_err();
        assert!(error.message.contains("shape"));
    }

    #[test]
    fn generic_tensor_arrow_and_ort_boundaries_round_trip() {
        let spec = GenericTensorSpec {
            name: "features".to_owned(),
            data_type: "FLOAT32".to_owned(),
            shape: vec![-1, 2],
            processor: None,
        };
        let input = TensorBatch::from_f32("features", vec![2, 2], vec![1.0, 2.0, 3.0, 4.0], None)
            .unwrap()
            .into_array();
        let value = array_to_ort_tensor(&spec, &input).unwrap().into_dyn();
        let output = ort_tensor_to_array(&spec, &value).unwrap();
        let output = output
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        let values = output
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(values.values(), &[1.0, 2.0, 3.0, 4.0]);
    }
}
