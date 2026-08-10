use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

use ort::session::Session;
use ort::value::Tensor;

use super::pipeline::{
    BatchingOwner, ImageTransform, RuntimeRequestBatch, RuntimeResponseBatch, RuntimeSession,
    TensorBatch,
};
use super::{Detection, ModelOutputFormat, ModelParams};
use crate::{ErrorCode, Result, VqlError};

pub(super) struct OrtRuntime {
    session: Mutex<Session>,
}

impl std::fmt::Debug for OrtRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("OrtRuntime").finish_non_exhaustive()
    }
}

impl OrtRuntime {
    pub(super) fn new(path: &Path) -> Result<Self> {
        #[cfg(target_os = "macos")]
        let coreml = Session::builder()
            .ok()
            .and_then(|builder| {
                builder
                    .with_execution_providers([ort::ep::CoreML::default().build()])
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
        Ok(Self {
            session: Mutex::new(session),
        })
    }
}

impl RuntimeSession for OrtRuntime {
    fn kind(&self) -> &str {
        "onnxruntime"
    }

    fn batching_owner(&self) -> BatchingOwner {
        BatchingOwner::VisionQl
    }

    fn infer(&self, batch: RuntimeRequestBatch) -> Result<RuntimeResponseBatch> {
        let input_name = batch.input.name;
        let shape = batch
            .input
            .shape
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
        let tensor = Tensor::<f32>::from_array((shape, batch.input.values)).map_err(|error| {
            VqlError::new(ErrorCode::Execution, "failed to build ONNX input tensor")
                .with_source(error)
        })?;
        let mut session = self
            .session
            .lock()
            .map_err(|_| VqlError::new(ErrorCode::Internal, "ONNX session lock was poisoned"))?;
        let outputs = session
            .run(ort::inputs![input_name.as_str() => tensor])
            .map_err(|error| {
                VqlError::new(ErrorCode::Execution, "ONNX inference failed").with_source(error)
            })?;
        let mut response = BTreeMap::new();
        for output_name in batch.output_names {
            let output = outputs.get(&output_name).ok_or_else(|| {
                VqlError::new(
                    ErrorCode::Execution,
                    format!("ONNX output '{output_name}' is missing"),
                )
            })?;
            let (shape, values) = output.try_extract_tensor::<f32>().map_err(|error| {
                VqlError::new(ErrorCode::Execution, "ONNX output must be float32")
                    .with_source(error)
            })?;
            response.insert(
                output_name.clone(),
                TensorBatch {
                    name: output_name,
                    shape: shape.to_vec(),
                    values: values.to_vec(),
                },
            );
        }
        Ok(RuntimeResponseBatch { outputs: response })
    }
}

pub(super) fn parse_batched_output(
    shape: &[i64],
    values: &[f32],
    params: &ModelParams,
    transforms: &[ImageTransform],
) -> Result<Vec<Vec<Detection>>> {
    let detections = match params.output_format {
        ModelOutputFormat::XywhNormalized => split_batched_rows(
            shape,
            values,
            transforms.len(),
            ModelOutputFormat::XywhNormalized,
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
        ModelOutputFormat::UltralyticsRaw => {
            parse_batched_ultralytics_raw(shape, values, params, transforms)?
        }
        ModelOutputFormat::UltralyticsEndToEnd => split_batched_rows(
            shape,
            values,
            transforms.len(),
            ModelOutputFormat::UltralyticsEndToEnd,
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
    format: ModelOutputFormat,
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
    params: &ModelParams,
    transforms: &[ImageTransform],
) -> Result<Vec<Vec<Detection>>> {
    if shape.len() != 3
        || usize::try_from(shape[0]).ok() != Some(transforms.len())
        || tensor_element_count(shape) != Some(values.len())
    {
        return unsupported_shape(shape, ModelOutputFormat::UltralyticsRaw);
    }
    let channels = usize::try_from(shape[1]).unwrap_or_default();
    let candidates = usize::try_from(shape[2]).unwrap_or_default();
    let Some(values_per_batch) = channels.checked_mul(candidates) else {
        return unsupported_shape(shape, ModelOutputFormat::UltralyticsRaw);
    };
    if channels < 5 || candidates == 0 || values_per_batch == 0 {
        return unsupported_shape(shape, ModelOutputFormat::UltralyticsRaw);
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

fn finalize_detections(mut detections: Vec<Detection>, params: &ModelParams) -> Vec<Detection> {
    detections.retain(|detection| {
        detection.confidence > 0.0
            && detection.confidence.is_finite()
            && detection.x.is_finite()
            && detection.y.is_finite()
            && detection.w.is_finite()
            && detection.h.is_finite()
    });
    detections.sort_by(|left, right| right.confidence.total_cmp(&left.confidence));
    if params.output_format == ModelOutputFormat::UltralyticsEndToEnd {
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
    params: &ModelParams,
    transform: ImageTransform,
) -> Result<Vec<Detection>> {
    if shape.len() < 2 || shape.last() != Some(&6) || !values.len().is_multiple_of(6) {
        return unsupported_shape(shape, ModelOutputFormat::XywhNormalized);
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
    params: &ModelParams,
    transform: ImageTransform,
) -> Result<Vec<Detection>> {
    if shape.len() != 3 || shape[0] != 1 {
        return unsupported_shape(shape, ModelOutputFormat::UltralyticsRaw);
    }
    let channels = usize::try_from(shape[1]).unwrap_or_default();
    let candidates = usize::try_from(shape[2]).unwrap_or_default();
    if channels < 5 || candidates == 0 || values.len() != channels * candidates {
        return unsupported_shape(shape, ModelOutputFormat::UltralyticsRaw);
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
    params: &ModelParams,
    transform: ImageTransform,
) -> Result<Vec<Detection>> {
    if shape.len() < 2 || shape.last() != Some(&6) || !values.len().is_multiple_of(6) {
        return unsupported_shape(shape, ModelOutputFormat::UltralyticsEndToEnd);
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

fn class_label(params: &ModelParams, class: f32) -> String {
    let index = class.round().max(0.0) as usize;
    params
        .labels
        .get(index)
        .cloned()
        .unwrap_or_else(|| format!("class_{index}"))
}

fn unsupported_shape<T>(shape: &[i64], format: ModelOutputFormat) -> Result<T> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::backend::ModelBackend;
    use crate::models::pipeline::{CompiledPipeline, ImageTensorPreProcessor, YoloPostProcessor};
    use image::DynamicImage;
    use std::sync::Arc;

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

    #[test]
    fn parses_ultralytics_raw_channel_major_output() {
        let params = ModelParams {
            labels: vec!["person".to_owned(), "car".to_owned()],
            output_format: ModelOutputFormat::UltralyticsRaw,
            ..ModelParams::default()
        };
        let values = [
            16.0, 24.0, // cx
            16.0, 24.0, // cy
            8.0, 8.0, // width
            8.0, 8.0, // height
            0.9, 0.1, // person score
            0.1, 0.8, // car score
        ];

        let detections = parse_ultralytics_raw(&[1, 6, 2], &values, &params, transform()).unwrap();

        assert_eq!(detections.len(), 2);
        assert_eq!(detections[0].label, "person");
        assert_eq!(detections[1].label, "car");
        assert_eq!(detections[0].x, 0.375);
        assert_eq!(detections[1].y, 0.625);
    }

    #[test]
    fn parses_yolo26_end_to_end_xyxy_output() {
        let params = ModelParams {
            labels: vec!["person".to_owned()],
            output_format: ModelOutputFormat::UltralyticsEndToEnd,
            ..ModelParams::default()
        };

        let detections = parse_ultralytics_end_to_end(
            &[1, 1, 6],
            &[8.0, 4.0, 24.0, 20.0, 0.9, 0.0],
            &params,
            transform(),
        )
        .unwrap();

        assert_eq!(detections.len(), 1);
        assert_eq!(detections[0].x, 0.25);
        assert_eq!(detections[0].y, 0.125);
        assert_eq!(detections[0].w, 0.5);
        assert_eq!(detections[0].h, 0.5);
    }

    #[test]
    fn yolo26_end_to_end_output_does_not_run_external_nms() {
        let detection = Detection {
            label: "person".to_owned(),
            confidence: 0.9,
            x: 0.25,
            y: 0.25,
            w: 0.5,
            h: 0.5,
        };
        let params = ModelParams::default();

        let output = finalize_detections(vec![detection.clone(), detection], &params);

        assert_eq!(output.len(), 2);
    }

    #[test]
    fn parses_each_image_from_a_batched_end_to_end_output() {
        let params = ModelParams {
            labels: vec!["person".to_owned(), "car".to_owned()],
            output_format: ModelOutputFormat::UltralyticsEndToEnd,
            ..ModelParams::default()
        };
        let values = [
            4.0, 4.0, 20.0, 20.0, 0.9, 0.0, 8.0, 8.0, 24.0, 24.0, 0.8, 1.0,
        ];

        let batches =
            parse_batched_output(&[2, 1, 6], &values, &params, &[transform(), transform()])
                .unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 1);
        assert_eq!(batches[0][0].label, "person");
        assert_eq!(batches[0][0].x, 0.125);
        assert_eq!(batches[1].len(), 1);
        assert_eq!(batches[1][0].label, "car");
        assert_eq!(batches[1][0].x, 0.25);
    }

    #[test]
    #[ignore = "requires VQL_YOLO26_ONNX"]
    fn real_yolo26_onnx_e2e() {
        let path = std::env::var_os("VQL_YOLO26_ONNX")
            .map(std::path::PathBuf::from)
            .expect("set VQL_YOLO26_ONNX to an exported YOLO26 ONNX model");
        let params = ModelParams::default();
        let backend = CompiledPipeline::new(
            Arc::new(ImageTensorPreProcessor::new(params.clone())),
            Arc::new(OrtRuntime::new(&path).expect("load YOLO ONNX model")),
            Arc::new(YoloPostProcessor::new(params)),
        );

        let output = backend
            .infer(vec![DynamicImage::new_rgb8(640, 480)])
            .expect("run YOLO ONNX inference");

        assert_eq!(output.len(), 1);
        assert!(output[0].iter().all(|detection| {
            (0.0..=1.0).contains(&detection.x)
                && (0.0..=1.0).contains(&detection.y)
                && (0.0..=1.0).contains(&detection.w)
                && (0.0..=1.0).contains(&detection.h)
        }));
    }
}
