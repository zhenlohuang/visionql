use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float32Array, Float32Builder, ListArray, ListBuilder, StringArray,
    StringBuilder, StructArray, StructBuilder,
};
use arrow::datatypes::DataType;
use serde::Deserialize;

use super::super::pipeline::{
    ImageTransform, PostProcessor, PreProcessContext, RuntimeResponseBatch, TensorContract,
};
use super::super::registry::{PostProcessorFactory, deserialize_processor_options, invalid_option};
use super::super::{BoundInferenceParams, detection_fields};
use crate::catalog::{ModelType, ProcessorSpec};
use crate::types::box2d_field;
use crate::{ErrorCode, Result, VqlError};

const SUPPORTED_TYPES: &[ModelType] = &[ModelType::ObjectDetection];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputFormat {
    XywhNormalized,
    UltralyticsRaw,
    UltralyticsEndToEnd,
}

impl OutputFormat {
    const fn kind(self) -> &'static str {
        match self {
            Self::XywhNormalized => "vision.xywh_normalized@1",
            Self::UltralyticsRaw => "vision.yolo_raw@1",
            Self::UltralyticsEndToEnd => "vision.yolo_e2e@1",
        }
    }

    const fn box_format(self) -> BoxFormat {
        match self {
            Self::UltralyticsEndToEnd => BoxFormat::Xyxy,
            Self::XywhNormalized | Self::UltralyticsRaw => BoxFormat::Xywh,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum BoxFormat {
    Xywh,
    Xyxy,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum Labels {
    Preset(String),
    Values(Vec<String>),
}

impl Default for Labels {
    fn default() -> Self {
        Self::Preset("coco80".to_owned())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct YoloOptions {
    #[serde(default = "default_output_name")]
    output_name: String,
    #[serde(default)]
    box_format: Option<BoxFormat>,
    #[serde(default)]
    labels: Labels,
    #[serde(default = "default_nms_iou_threshold")]
    nms_iou_threshold: f32,
}

impl YoloOptions {
    fn parse(spec: &ProcessorSpec, format: OutputFormat) -> Result<Self> {
        let mut options: Self =
            deserialize_processor_options("post_processor.options", &spec.options)?;
        if options.output_name.is_empty() {
            return invalid_option(
                "post_processor.options.output_name",
                "must be a non-empty string",
            );
        }
        if let Some(box_format) = options.box_format
            && box_format != format.box_format()
        {
            let expected = match format.box_format() {
                BoxFormat::Xywh => "xywh",
                BoxFormat::Xyxy => "xyxy",
            };
            return invalid_option(
                "post_processor.options.box_format",
                format!("must be '{expected}'"),
            );
        }
        if !options.nms_iou_threshold.is_finite()
            || !(0.0..=1.0).contains(&options.nms_iou_threshold)
        {
            return invalid_option(
                "post_processor.options.nms_iou_threshold",
                "must be between 0 and 1",
            );
        }
        options.box_format = Some(format.box_format());
        Ok(options)
    }

    fn resolved_labels(&self) -> Result<Vec<String>> {
        let labels = match &self.labels {
            Labels::Preset(value) if value.eq_ignore_ascii_case("coco80") => {
                coco_detection_labels()
            }
            Labels::Preset(_) => {
                return invalid_option(
                    "post_processor.options.labels",
                    "must be an array of strings",
                );
            }
            Labels::Values(values) => values.clone(),
        };
        if labels.iter().any(String::is_empty) {
            return invalid_option(
                "post_processor.options.labels",
                "must not contain empty labels",
            );
        }
        Ok(labels)
    }
}

fn default_output_name() -> String {
    "output0".to_owned()
}

const fn default_nms_iou_threshold() -> f32 {
    0.45
}

#[derive(Debug)]
pub(in crate::models) struct YoloPostProcessorFactory {
    format: OutputFormat,
}

impl YoloPostProcessorFactory {
    pub(in crate::models) const fn end_to_end() -> Self {
        Self {
            format: OutputFormat::UltralyticsEndToEnd,
        }
    }

    pub(in crate::models) const fn raw() -> Self {
        Self {
            format: OutputFormat::UltralyticsRaw,
        }
    }

    pub(in crate::models) const fn xywh_normalized() -> Self {
        Self {
            format: OutputFormat::XywhNormalized,
        }
    }
}

impl PostProcessorFactory for YoloPostProcessorFactory {
    fn kind(&self) -> &str {
        self.format.kind()
    }

    fn supported_types(&self) -> &[ModelType] {
        SUPPORTED_TYPES
    }

    fn validate(&self, spec: &ProcessorSpec) -> Result<()> {
        let options = YoloOptions::parse(spec, self.format)?;
        options.resolved_labels().map(|_| ())
    }

    fn build(&self, spec: &ProcessorSpec) -> Result<Arc<dyn PostProcessor>> {
        let options = YoloOptions::parse(spec, self.format)?;
        let labels = options.resolved_labels()?;
        Ok(Arc::new(YoloPostProcessor::new(
            self.format,
            options,
            labels,
        )))
    }
}

#[derive(Debug)]
struct YoloPostProcessor {
    format: OutputFormat,
    output_contract: TensorContract,
    labels: Vec<String>,
    nms_iou_threshold: f32,
}

impl YoloPostProcessor {
    fn new(format: OutputFormat, options: YoloOptions, labels: Vec<String>) -> Self {
        let channels = i64::try_from(labels.len())
            .ok()
            .and_then(|count| count.checked_add(4))
            .unwrap_or(-1);
        let shape = match format {
            OutputFormat::UltralyticsRaw => vec![-1, channels, -1],
            OutputFormat::XywhNormalized | OutputFormat::UltralyticsEndToEnd => vec![-1, -1, 6],
        };
        Self {
            format,
            output_contract: TensorContract {
                name: options.output_name,
                dtype: DataType::Float32,
                shape,
            },
            labels,
            nms_iou_threshold: options.nms_iou_threshold,
        }
    }

    fn params(&self) -> DetectionParams<'_> {
        DetectionParams {
            labels: &self.labels,
            output_format: self.format,
            nms_iou_threshold: self.nms_iou_threshold,
        }
    }
}

impl PostProcessor for YoloPostProcessor {
    fn kind(&self) -> &str {
        self.format.kind()
    }

    fn runtime_input(&self) -> &TensorContract {
        &self.output_contract
    }

    fn process(
        &self,
        response: RuntimeResponseBatch,
        context: &PreProcessContext,
    ) -> Result<ArrayRef> {
        let output = response.output(&self.output_contract.name)?;
        self.output_contract
            .validate_batch("Runtime output", output)?;
        let detections = parse_batched_output(
            &output.shape(),
            output.as_f32("Runtime output")?,
            self.params(),
            &context.transforms,
        )?;
        Ok(build_detection_array(
            detections.into_iter().map(Some),
            context.transforms.len(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Detection {
    label: String,
    confidence: f32,
    x: f32,
    y: f32,
    w: f32,
    h: f32,
}

#[derive(Clone, Copy)]
struct DetectionParams<'a> {
    labels: &'a [String],
    output_format: OutputFormat,
    nms_iou_threshold: f32,
}

pub(in crate::models) fn mock_detection_output(label: &str, rows: usize) -> ArrayRef {
    build_detection_array(
        (0..rows).map(|_| {
            Some(vec![Detection {
                label: label.to_owned(),
                confidence: 0.9,
                x: 0.25,
                y: 0.25,
                w: 0.5,
                h: 0.5,
            }])
        }),
        rows,
    )
}

pub(in crate::models) fn mock_primary_label(spec: &ProcessorSpec) -> Result<String> {
    let format = match spec.kind.as_str() {
        "vision.yolo_e2e@1" => OutputFormat::UltralyticsEndToEnd,
        "vision.yolo_raw@1" => OutputFormat::UltralyticsRaw,
        "vision.xywh_normalized@1" => OutputFormat::XywhNormalized,
        _ => {
            return invalid_option(
                "post_processor.kind",
                format!("unsupported OBJECT_DETECTION PostProcessor '{}'", spec.kind),
            );
        }
    };
    let options = YoloOptions::parse(spec, format)?;
    Ok(options
        .resolved_labels()?
        .into_iter()
        .next()
        .unwrap_or_else(|| "object".to_owned()))
}

pub(in crate::models) fn filter_and_scatter_detections(
    input: &ArrayRef,
    positions: &[usize],
    total_rows: usize,
    invocation: &BoundInferenceParams,
) -> Result<ArrayRef> {
    let input = input.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        VqlError::new(
            ErrorCode::Internal,
            "OBJECT_DETECTION pipeline returned a non-detection Arrow array",
        )
    })?;
    if input.len() != positions.len() {
        return Err(VqlError::new(
            ErrorCode::Execution,
            format!(
                "model backend returned {} rows for {} inputs",
                input.len(),
                positions.len()
            ),
        ));
    }
    let mut rows = vec![None; total_rows];
    for (source_row, target_row) in positions.iter().copied().enumerate() {
        let detections = read_detection_row(input, source_row)?
            .into_iter()
            .filter(|detection| {
                detection.confidence >= invocation.min_confidence
                    && invocation
                        .classes
                        .as_ref()
                        .is_none_or(|classes| classes.contains(&detection.label))
            })
            .collect();
        rows[target_row] = Some(detections);
    }
    Ok(build_detection_array(rows, total_rows))
}

fn build_detection_array(
    rows: impl IntoIterator<Item = Option<Vec<Detection>>>,
    capacity: usize,
) -> ArrayRef {
    let box_fields = match box2d_field("box", false).data_type() {
        arrow::datatypes::DataType::Struct(fields) => fields.clone(),
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
    let mut builder = ListBuilder::new(detection_builder);
    for row in rows {
        let Some(detections) = row else {
            builder.append(false);
            continue;
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
    Arc::new(builder.finish())
}

fn read_detection_row(array: &ListArray, row: usize) -> Result<Vec<Detection>> {
    if array.is_null(row) {
        return Ok(Vec::new());
    }
    let values = array.value(row);
    let values = values
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| VqlError::new(ErrorCode::Internal, "detection item is not a struct"))?;
    let labels = values
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| VqlError::new(ErrorCode::Internal, "detection label is not STRING"))?;
    let confidence = values
        .column(1)
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| VqlError::new(ErrorCode::Internal, "detection confidence is not FLOAT"))?;
    let boxes = values
        .column(2)
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| VqlError::new(ErrorCode::Internal, "detection box is not BOX2D"))?;
    let coordinates = (0..4)
        .map(|index| {
            boxes
                .column(index)
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| VqlError::new(ErrorCode::Internal, "BOX2D coordinate is not FLOAT"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((0..values.len())
        .map(|index| Detection {
            label: labels.value(index).to_owned(),
            confidence: confidence.value(index),
            x: coordinates[0].value(index),
            y: coordinates[1].value(index),
            w: coordinates[2].value(index),
            h: coordinates[3].value(index),
        })
        .collect())
}

fn parse_batched_output(
    shape: &[i64],
    values: &[f32],
    params: DetectionParams<'_>,
    transforms: &[ImageTransform],
) -> Result<Vec<Vec<Detection>>> {
    let detections = match params.output_format {
        OutputFormat::XywhNormalized => split_batched_rows(
            shape,
            values,
            transforms.len(),
            OutputFormat::XywhNormalized,
        )?
        .into_iter()
        .zip(transforms.iter().copied())
        .map(|(values, transform)| {
            parse_xywh_normalized(
                &[i64::try_from(values.len() / 6).unwrap_or(i64::MAX), 6],
                values,
                params,
                transform,
            )
        })
        .collect::<Result<Vec<_>>>()?,
        OutputFormat::UltralyticsRaw => {
            parse_batched_ultralytics_raw(shape, values, params, transforms)?
        }
        OutputFormat::UltralyticsEndToEnd => split_batched_rows(
            shape,
            values,
            transforms.len(),
            OutputFormat::UltralyticsEndToEnd,
        )?
        .into_iter()
        .zip(transforms.iter().copied())
        .map(|(values, transform)| {
            parse_ultralytics_end_to_end(
                &[i64::try_from(values.len() / 6).unwrap_or(i64::MAX), 6],
                values,
                params,
                transform,
            )
        })
        .collect::<Result<Vec<_>>>()?,
    };
    Ok(detections
        .into_iter()
        .map(|detections| finalize_detections(detections, params))
        .collect())
}

fn split_batched_rows<'a>(
    shape: &[i64],
    values: &'a [f32],
    batch_size: usize,
    format: OutputFormat,
) -> Result<Vec<&'a [f32]>> {
    if batch_size == 0
        || shape.len() < 2
        || shape.last() != Some(&6)
        || tensor_element_count(shape) != Some(values.len())
    {
        return unsupported_shape(shape, format);
    }
    let tensor_batch = if shape.len() == 2 && batch_size == 1 {
        1
    } else {
        usize::try_from(shape[0]).unwrap_or_default()
    };
    if tensor_batch != batch_size || !values.len().is_multiple_of(batch_size * 6) {
        return unsupported_shape(shape, format);
    }
    let values_per_batch = values.len() / batch_size;
    if values_per_batch == 0 {
        return Ok((0..batch_size).map(|_| &values[0..0]).collect());
    }
    Ok(values.chunks_exact(values_per_batch).collect())
}

fn parse_batched_ultralytics_raw(
    shape: &[i64],
    values: &[f32],
    params: DetectionParams<'_>,
    transforms: &[ImageTransform],
) -> Result<Vec<Vec<Detection>>> {
    if shape.len() != 3
        || usize::try_from(shape[0]).ok() != Some(transforms.len())
        || tensor_element_count(shape) != Some(values.len())
    {
        return unsupported_shape(shape, OutputFormat::UltralyticsRaw);
    }
    let channels = usize::try_from(shape[1]).unwrap_or_default();
    let candidates = usize::try_from(shape[2]).unwrap_or_default();
    let Some(values_per_batch) = channels.checked_mul(candidates) else {
        return unsupported_shape(shape, OutputFormat::UltralyticsRaw);
    };
    if channels < 5 || candidates == 0 || values_per_batch == 0 {
        return unsupported_shape(shape, OutputFormat::UltralyticsRaw);
    }
    values
        .chunks_exact(values_per_batch)
        .zip(transforms.iter().copied())
        .map(|(values, transform)| {
            parse_ultralytics_raw(&[1, shape[1], shape[2]], values, params, transform)
        })
        .collect()
}

fn tensor_element_count(shape: &[i64]) -> Option<usize> {
    shape.iter().try_fold(1_usize, |elements, dimension| {
        elements.checked_mul(usize::try_from(*dimension).ok()?)
    })
}

fn finalize_detections(
    mut detections: Vec<Detection>,
    params: DetectionParams<'_>,
) -> Vec<Detection> {
    detections.retain(|detection| {
        detection.confidence > 0.0
            && detection.confidence.is_finite()
            && detection.x.is_finite()
            && detection.y.is_finite()
            && detection.w.is_finite()
            && detection.h.is_finite()
    });
    detections.sort_by(|left, right| right.confidence.total_cmp(&left.confidence));
    if params.output_format == OutputFormat::UltralyticsEndToEnd {
        return detections;
    }
    let mut kept: Vec<Detection> = Vec::new();
    for detection in detections {
        if kept.iter().all(|other| {
            other.label != detection.label
                || intersection_over_union(other, &detection) < params.nms_iou_threshold
        }) {
            kept.push(detection);
        }
    }
    kept
}

fn parse_xywh_normalized(
    shape: &[i64],
    values: &[f32],
    params: DetectionParams<'_>,
    transform: ImageTransform,
) -> Result<Vec<Detection>> {
    if shape.len() < 2 || shape.last() != Some(&6) || !values.len().is_multiple_of(6) {
        return unsupported_shape(shape, OutputFormat::XywhNormalized);
    }
    Ok(values
        .chunks_exact(6)
        .filter_map(|value| {
            detection_from_xywh(
                value[0] * transform.input_width,
                value[1] * transform.input_height,
                value[2] * transform.input_width,
                value[3] * transform.input_height,
                value[4],
                class_label(params, value[5]),
                transform,
                false,
            )
        })
        .collect())
}

fn parse_ultralytics_raw(
    shape: &[i64],
    values: &[f32],
    params: DetectionParams<'_>,
    transform: ImageTransform,
) -> Result<Vec<Detection>> {
    if shape.len() != 3 || shape[0] != 1 {
        return unsupported_shape(shape, OutputFormat::UltralyticsRaw);
    }
    let channels = usize::try_from(shape[1]).unwrap_or_default();
    let candidates = usize::try_from(shape[2]).unwrap_or_default();
    if channels < 5 || candidates == 0 || values.len() != channels * candidates {
        return unsupported_shape(shape, OutputFormat::UltralyticsRaw);
    }
    let class_count = channels - 4;
    let mut detections = Vec::new();
    for candidate in 0..candidates {
        let (class_index, confidence) = (0..class_count)
            .map(|class| (class, values[(class + 4) * candidates + candidate]))
            .max_by(|left, right| left.1.total_cmp(&right.1))
            .expect("raw YOLO output has at least one class");
        if let Some(detection) = detection_from_xywh(
            values[candidate],
            values[candidates + candidate],
            values[2 * candidates + candidate],
            values[3 * candidates + candidate],
            confidence,
            class_label(params, class_index as f32),
            transform,
            true,
        ) {
            detections.push(detection);
        }
    }
    Ok(detections)
}

fn parse_ultralytics_end_to_end(
    shape: &[i64],
    values: &[f32],
    params: DetectionParams<'_>,
    transform: ImageTransform,
) -> Result<Vec<Detection>> {
    if shape.len() < 2 || shape.last() != Some(&6) || !values.len().is_multiple_of(6) {
        return unsupported_shape(shape, OutputFormat::UltralyticsEndToEnd);
    }
    Ok(values
        .chunks_exact(6)
        .filter_map(|value| {
            if value[4] <= 0.0 {
                return None;
            }
            let x1 = (value[0] - transform.pad_x) / transform.scale;
            let y1 = (value[1] - transform.pad_y) / transform.scale;
            let x2 = (value[2] - transform.pad_x) / transform.scale;
            let y2 = (value[3] - transform.pad_y) / transform.scale;
            Some(Detection {
                label: class_label(params, value[5]),
                confidence: value[4],
                x: (x1 / transform.original_width).clamp(0.0, 1.0),
                y: (y1 / transform.original_height).clamp(0.0, 1.0),
                w: ((x2 - x1) / transform.original_width).clamp(0.0, 1.0),
                h: ((y2 - y1) / transform.original_height).clamp(0.0, 1.0),
            })
        })
        .collect())
}

#[allow(clippy::too_many_arguments)]
fn detection_from_xywh(
    x: f32,
    y: f32,
    width: f32,
    height: f32,
    confidence: f32,
    label: String,
    transform: ImageTransform,
    center_coordinates: bool,
) -> Option<Detection> {
    if confidence <= 0.0 {
        return None;
    }
    let mut x = (x - transform.pad_x) / transform.scale;
    let mut y = (y - transform.pad_y) / transform.scale;
    let width = width / transform.scale;
    let height = height / transform.scale;
    if center_coordinates {
        x -= width / 2.0;
        y -= height / 2.0;
    }
    Some(Detection {
        label,
        confidence,
        x: (x / transform.original_width).clamp(0.0, 1.0),
        y: (y / transform.original_height).clamp(0.0, 1.0),
        w: (width / transform.original_width).clamp(0.0, 1.0),
        h: (height / transform.original_height).clamp(0.0, 1.0),
    })
}

fn class_label(params: DetectionParams<'_>, class: f32) -> String {
    let index = class.round().max(0.0) as usize;
    params
        .labels
        .get(index)
        .cloned()
        .unwrap_or_else(|| format!("class_{index}"))
}

fn unsupported_shape<T>(shape: &[i64], format: OutputFormat) -> Result<T> {
    Err(VqlError::new(
        ErrorCode::Execution,
        format!("Runtime {format:?} detection output has unsupported shape {shape:?}"),
    ))
}

fn intersection_over_union(left: &Detection, right: &Detection) -> f32 {
    let left_x2 = left.x + left.w;
    let left_y2 = left.y + left.h;
    let right_x2 = right.x + right.w;
    let right_y2 = right.y + right.h;
    let intersection = (left_x2.min(right_x2) - left.x.max(right.x)).max(0.0)
        * (left_y2.min(right_y2) - left.y.max(right.y)).max(0.0);
    let union = left.w * left.h + right.w * right.h - intersection;
    if union <= 0.0 {
        0.0
    } else {
        intersection / union
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

#[cfg(test)]
mod tests {
    use super::*;

    fn transform() -> ImageTransform {
        ImageTransform {
            original_width: 32.0,
            original_height: 32.0,
            input_width: 32.0,
            input_height: 32.0,
            scale: 1.0,
            pad_x: 0.0,
            pad_y: 0.0,
        }
    }

    fn params(labels: &[&str], output_format: OutputFormat) -> DetectionParams<'static> {
        let labels = labels
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>()
            .leak();
        DetectionParams {
            labels,
            output_format,
            nms_iou_threshold: 0.45,
        }
    }

    #[test]
    fn parses_ultralytics_raw_channel_major_output() {
        let params = params(&["person", "car"], OutputFormat::UltralyticsRaw);
        let values = [
            16.0, 24.0, 16.0, 24.0, 8.0, 8.0, 8.0, 8.0, 0.9, 0.1, 0.1, 0.8,
        ];
        let detections = parse_ultralytics_raw(&[1, 6, 2], &values, params, transform()).unwrap();
        assert_eq!(detections.len(), 2);
        assert_eq!(detections[0].label, "person");
        assert_eq!(detections[1].label, "car");
        assert_eq!(detections[0].x, 0.375);
        assert_eq!(detections[1].y, 0.625);
    }

    #[test]
    fn parses_yolo26_end_to_end_xyxy_output() {
        let params = params(&["person"], OutputFormat::UltralyticsEndToEnd);
        let detections = parse_ultralytics_end_to_end(
            &[1, 1, 6],
            &[8.0, 4.0, 24.0, 20.0, 0.9, 0.0],
            params,
            transform(),
        )
        .unwrap();
        assert_eq!(detections.len(), 1);
        assert_eq!((detections[0].x, detections[0].y), (0.25, 0.125));
        assert_eq!((detections[0].w, detections[0].h), (0.5, 0.5));
    }

    #[test]
    fn end_to_end_output_does_not_run_external_nms() {
        let detection = Detection {
            label: "person".to_owned(),
            confidence: 0.9,
            x: 0.25,
            y: 0.25,
            w: 0.5,
            h: 0.5,
        };
        let params = params(&["person"], OutputFormat::UltralyticsEndToEnd);
        assert_eq!(
            finalize_detections(vec![detection.clone(), detection], params).len(),
            2
        );
    }

    #[test]
    fn parses_each_image_from_a_batched_end_to_end_output() {
        let params = params(&["person", "car"], OutputFormat::UltralyticsEndToEnd);
        let values = [
            4.0, 4.0, 20.0, 20.0, 0.9, 0.0, 8.0, 8.0, 24.0, 24.0, 0.8, 1.0,
        ];
        let batches =
            parse_batched_output(&[2, 1, 6], &values, params, &[transform(), transform()]).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0][0].label, "person");
        assert_eq!(batches[1][0].label, "car");
    }

    #[test]
    fn arrow_filter_restores_null_rows_and_applies_invocation_options() {
        let input = mock_detection_output("person", 2);
        let output = filter_and_scatter_detections(
            &input,
            &[0, 2],
            3,
            &BoundInferenceParams {
                classes: Some(vec!["car".to_owned()]),
                min_confidence: 0.25,
            },
        )
        .unwrap();
        let output = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(output.len(), 3);
        assert!(!output.is_null(0));
        assert_eq!(output.value_length(0), 0);
        assert!(output.is_null(1));
    }
}
