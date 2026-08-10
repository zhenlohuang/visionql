use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use image::{DynamicImage, GenericImage, Rgb, RgbImage, imageops::FilterType};

use super::backend::ModelBackend;
use super::ort_backend::parse_batched_output;
use super::{Detection, ModelParams};
use crate::{ErrorCode, Result, VqlError};

#[derive(Debug, Clone)]
pub(super) struct TensorBatch {
    pub(super) name: String,
    pub(super) shape: Vec<i64>,
    pub(super) values: Vec<f32>,
}

#[derive(Debug)]
pub(super) struct RuntimeRequestBatch {
    pub(super) input: TensorBatch,
    pub(super) output_names: Vec<String>,
}

#[derive(Debug)]
pub(super) struct RuntimeResponseBatch {
    pub(super) outputs: BTreeMap<String, TensorBatch>,
}

impl RuntimeResponseBatch {
    fn output(&self, name: &str) -> Result<&TensorBatch> {
        self.outputs.get(name).ok_or_else(|| {
            VqlError::new(
                ErrorCode::Execution,
                format!("Runtime output '{name}' is missing"),
            )
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct ImageTransform {
    pub(super) original_width: f32,
    pub(super) original_height: f32,
    pub(super) input_width: f32,
    pub(super) input_height: f32,
    pub(super) scale: f32,
    pub(super) pad_x: f32,
    pub(super) pad_y: f32,
}

#[derive(Debug)]
pub(super) struct PreProcessContext {
    pub(super) transforms: Vec<ImageTransform>,
}

#[derive(Debug)]
pub(super) struct PreprocessedBatch {
    pub(super) request: RuntimeRequestBatch,
    pub(super) context: PreProcessContext,
}

pub(super) trait PreProcessor: Send + Sync + Debug {
    fn kind(&self) -> &str;
    fn process(&self, images: &[DynamicImage]) -> Result<PreprocessedBatch>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BatchingOwner {
    VisionQl,
    Service,
}

pub(super) trait RuntimeSession: Send + Sync + Debug {
    fn kind(&self) -> &str;
    fn batching_owner(&self) -> BatchingOwner;
    fn infer(&self, batch: RuntimeRequestBatch) -> Result<RuntimeResponseBatch>;
}

pub(super) trait PostProcessor: Send + Sync + Debug {
    fn kind(&self) -> &str;
    fn process(
        &self,
        response: RuntimeResponseBatch,
        context: &PreProcessContext,
    ) -> Result<Vec<Vec<Detection>>>;
}

pub(super) struct CompiledPipeline {
    pre_processor: Arc<dyn PreProcessor>,
    runtime: Arc<dyn RuntimeSession>,
    post_processor: Arc<dyn PostProcessor>,
}

impl CompiledPipeline {
    pub(super) fn new(
        pre_processor: Arc<dyn PreProcessor>,
        runtime: Arc<dyn RuntimeSession>,
        post_processor: Arc<dyn PostProcessor>,
    ) -> Self {
        Self {
            pre_processor,
            runtime,
            post_processor,
        }
    }

    pub(super) fn batching_owner(&self) -> BatchingOwner {
        self.runtime.batching_owner()
    }
}

impl Debug for CompiledPipeline {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CompiledPipeline")
            .field("pre_processor", &self.pre_processor.kind())
            .field("runtime", &self.runtime.kind())
            .field("post_processor", &self.post_processor.kind())
            .field("batching_owner", &self.batching_owner())
            .finish()
    }
}

impl ModelBackend for CompiledPipeline {
    fn infer(&self, images: Vec<DynamicImage>) -> Result<Vec<Vec<Detection>>> {
        if images.is_empty() {
            return Ok(Vec::new());
        }
        let preprocessed = self.pre_processor.process(&images)?;
        let response = self.runtime.infer(preprocessed.request)?;
        let output = self
            .post_processor
            .process(response, &preprocessed.context)?;
        if output.len() != images.len() {
            return Err(VqlError::new(
                ErrorCode::Execution,
                format!(
                    "compiled model pipeline returned {} rows for {} inputs",
                    output.len(),
                    images.len()
                ),
            ));
        }
        Ok(output)
    }
}

#[derive(Debug)]
pub(super) struct ImageTensorPreProcessor {
    params: ModelParams,
}

impl ImageTensorPreProcessor {
    pub(super) fn new(params: ModelParams) -> Self {
        Self { params }
    }
}

impl PreProcessor for ImageTensorPreProcessor {
    fn kind(&self) -> &str {
        "vision.image_tensor@1"
    }

    fn process(&self, images: &[DynamicImage]) -> Result<PreprocessedBatch> {
        let batch_size = images.len();
        let width = self.params.input_width as usize;
        let height = self.params.input_height as usize;
        let plane = width.checked_mul(height).ok_or_else(|| {
            VqlError::new(ErrorCode::Execution, "input tensor dimensions overflow")
        })?;
        let elements = batch_size
            .checked_mul(3)
            .and_then(|value| value.checked_mul(plane))
            .ok_or_else(|| VqlError::new(ErrorCode::Execution, "input tensor size overflow"))?;
        let mut input = vec![0_f32; elements];
        let mut transforms = Vec::with_capacity(batch_size);
        for (batch_index, image) in images.iter().enumerate() {
            let (pixels, scale, pad_x, pad_y) =
                letterbox(image, self.params.input_width, self.params.input_height);
            let offset = batch_index * 3 * plane;
            for (index, pixel) in pixels.pixels().enumerate() {
                input[offset + index] = f32::from(pixel[0]) / 255.0;
                input[offset + plane + index] = f32::from(pixel[1]) / 255.0;
                input[offset + 2 * plane + index] = f32::from(pixel[2]) / 255.0;
            }
            transforms.push(ImageTransform {
                original_width: image.width() as f32,
                original_height: image.height() as f32,
                input_width: self.params.input_width as f32,
                input_height: self.params.input_height as f32,
                scale,
                pad_x,
                pad_y,
            });
        }
        Ok(PreprocessedBatch {
            request: RuntimeRequestBatch {
                input: TensorBatch {
                    name: self.params.input_name.clone(),
                    shape: vec![
                        i64::try_from(batch_size).map_err(|_| {
                            VqlError::new(ErrorCode::Execution, "batch size is too large")
                        })?,
                        3,
                        i64::from(self.params.input_height),
                        i64::from(self.params.input_width),
                    ],
                    values: input,
                },
                output_names: vec![self.params.output_name.clone()],
            },
            context: PreProcessContext { transforms },
        })
    }
}

#[derive(Debug)]
pub(super) struct YoloPostProcessor {
    params: ModelParams,
}

impl YoloPostProcessor {
    pub(super) fn new(params: ModelParams) -> Self {
        Self { params }
    }
}

impl PostProcessor for YoloPostProcessor {
    fn kind(&self) -> &str {
        match self.params.output_format {
            super::ModelOutputFormat::UltralyticsEndToEnd => "vision.yolo_e2e@1",
            super::ModelOutputFormat::UltralyticsRaw => "vision.yolo_raw@1",
            super::ModelOutputFormat::XywhNormalized => "vision.xywh_normalized@1",
        }
    }

    fn process(
        &self,
        response: RuntimeResponseBatch,
        context: &PreProcessContext,
    ) -> Result<Vec<Vec<Detection>>> {
        let output = response.output(&self.params.output_name)?;
        parse_batched_output(
            &output.shape,
            &output.values,
            &self.params,
            &context.transforms,
        )
    }
}

fn letterbox(image: &DynamicImage, width: u32, height: u32) -> (RgbImage, f32, f32, f32) {
    let scale = (width as f32 / image.width() as f32).min(height as f32 / image.height() as f32);
    let resized_width = (image.width() as f32 * scale).round() as u32;
    let resized_height = (image.height() as f32 * scale).round() as u32;
    let resized = image
        .resize_exact(resized_width, resized_height, FilterType::Triangle)
        .to_rgb8();
    let pad_x = (width - resized_width) / 2;
    let pad_y = (height - resized_height) / 2;
    let mut output = RgbImage::from_pixel(width, height, Rgb([114, 114, 114]));
    output
        .copy_from(&resized, pad_x, pad_y)
        .expect("letterbox dimensions fit");
    (output, scale, pad_x as f32, pad_y as f32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_preprocessor_batches_nchw_and_keeps_row_context() {
        let params = ModelParams {
            input_width: 4,
            input_height: 2,
            ..ModelParams::default()
        };
        let processor = ImageTensorPreProcessor::new(params);
        let batch = processor
            .process(&[DynamicImage::new_rgb8(4, 2), DynamicImage::new_rgb8(2, 2)])
            .unwrap();

        assert_eq!(batch.request.input.shape, vec![2, 3, 2, 4]);
        assert_eq!(batch.request.input.values.len(), 48);
        assert_eq!(batch.context.transforms.len(), 2);
        assert_eq!(batch.context.transforms[1].pad_x, 1.0);
    }
}
