use std::sync::Arc;

use arrow::datatypes::DataType;
use fast_image_resize::images::Image as ResizeImage;
use fast_image_resize::{FilterType, PixelType, ResizeAlg, ResizeOptions, Resizer};
use image::{DynamicImage, GenericImage, Rgb, RgbImage};
use serde::Deserialize;

use super::super::pipeline::{
    ImageTransform, PreProcessContext, PreProcessor, PreprocessedBatch, TensorBatch, TensorContract,
};
use super::super::registry::{PreProcessorFactory, deserialize_processor_options};
use crate::catalog::{ModelType, ProcessorSpec};
use crate::{ErrorCode, Result, VqlError};

const KIND: &str = "vision.image_tensor@1";
const SUPPORTED_TYPES: &[ModelType] = &[ModelType::ObjectDetection];

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ResizeMode {
    #[default]
    Letterbox,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ColorSpace {
    #[default]
    Rgb,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum TensorLayout {
    #[default]
    Nchw,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ImageTensorOptions {
    #[serde(default = "default_input_name")]
    input_name: String,
    #[serde(default = "default_side")]
    width: u32,
    #[serde(default = "default_side")]
    height: u32,
    #[serde(default, rename = "resize")]
    _resize: ResizeMode,
    #[serde(default, rename = "color_space")]
    _color_space: ColorSpace,
    #[serde(default, rename = "layout")]
    _layout: TensorLayout,
}

impl ImageTensorOptions {
    fn parse(spec: &ProcessorSpec) -> Result<Self> {
        let options: Self = deserialize_processor_options("pre_processor.options", &spec.options)?;
        if options.input_name.is_empty() {
            return super::super::registry::invalid_option(
                "pre_processor.options.input_name",
                "must be a non-empty string",
            );
        }
        if options.width == 0 {
            return super::super::registry::invalid_option(
                "pre_processor.options.width",
                "must be a positive integer",
            );
        }
        if options.height == 0 {
            return super::super::registry::invalid_option(
                "pre_processor.options.height",
                "must be a positive integer",
            );
        }
        Ok(options)
    }
}

fn default_input_name() -> String {
    "images".to_owned()
}

const fn default_side() -> u32 {
    640
}

#[derive(Debug)]
pub(in crate::models) struct ImageTensorFactory;

impl PreProcessorFactory for ImageTensorFactory {
    fn kind(&self) -> &str {
        KIND
    }

    fn supported_types(&self) -> &[ModelType] {
        SUPPORTED_TYPES
    }

    fn validate(&self, spec: &ProcessorSpec) -> Result<()> {
        ImageTensorOptions::parse(spec).map(|_| ())
    }

    fn build(&self, spec: &ProcessorSpec) -> Result<Arc<dyn PreProcessor>> {
        Ok(Arc::new(ImageTensorPreProcessor::new(
            ImageTensorOptions::parse(spec)?,
        )))
    }
}

#[derive(Debug)]
struct ImageTensorPreProcessor {
    options: ImageTensorOptions,
    output_contract: TensorContract,
}

impl ImageTensorPreProcessor {
    fn new(options: ImageTensorOptions) -> Self {
        let output_contract = TensorContract {
            name: options.input_name.clone(),
            dtype: DataType::Float32,
            shape: vec![-1, 3, i64::from(options.height), i64::from(options.width)],
        };
        Self {
            options,
            output_contract,
        }
    }
}

impl PreProcessor for ImageTensorPreProcessor {
    fn kind(&self) -> &str {
        KIND
    }

    fn runtime_output(&self) -> &TensorContract {
        &self.output_contract
    }

    fn process(&self, images: &[DynamicImage]) -> Result<PreprocessedBatch> {
        let batch_size = images.len();
        let width = self.options.width as usize;
        let height = self.options.height as usize;
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
                letterbox(image, self.options.width, self.options.height)?;
            let offset = batch_index * 3 * plane;
            for (index, pixel) in pixels.pixels().enumerate() {
                input[offset + index] = f32::from(pixel[0]) / 255.0;
                input[offset + plane + index] = f32::from(pixel[1]) / 255.0;
                input[offset + 2 * plane + index] = f32::from(pixel[2]) / 255.0;
            }
            transforms.push(ImageTransform {
                original_width: image.width() as f32,
                original_height: image.height() as f32,
                input_width: self.options.width as f32,
                input_height: self.options.height as f32,
                scale,
                pad_x,
                pad_y,
            });
        }
        Ok(PreprocessedBatch {
            input: TensorBatch::from_f32(
                self.options.input_name.clone(),
                vec![
                    i64::try_from(batch_size).map_err(|_| {
                        VqlError::new(ErrorCode::Execution, "batch size is too large")
                    })?,
                    3,
                    i64::from(self.options.height),
                    i64::from(self.options.width),
                ],
                input,
                Some(vec!["C".to_owned(), "H".to_owned(), "W".to_owned()]),
            )?,
            context: PreProcessContext { transforms },
        })
    }
}

fn letterbox(image: &DynamicImage, width: u32, height: u32) -> Result<(RgbImage, f32, f32, f32)> {
    let scale = (width as f32 / image.width() as f32).min(height as f32 / image.height() as f32);
    let resized_width = (image.width() as f32 * scale).round() as u32;
    let resized_height = (image.height() as f32 * scale).round() as u32;
    let source = image.to_rgb8();
    let source = ResizeImage::from_vec_u8(
        image.width(),
        image.height(),
        source.into_raw(),
        PixelType::U8x3,
    )
    .map_err(|error| {
        VqlError::new(ErrorCode::Execution, "failed to prepare image resize input")
            .with_source(error)
    })?;
    let mut resized = ResizeImage::new(resized_width, resized_height, PixelType::U8x3);
    Resizer::new()
        .resize(
            &source,
            &mut resized,
            &ResizeOptions::new()
                .resize_alg(ResizeAlg::Convolution(FilterType::Bilinear))
                .use_alpha(false),
        )
        .map_err(|error| {
            VqlError::new(ErrorCode::Execution, "failed to resize model IMAGE input")
                .with_source(error)
        })?;
    let resized = RgbImage::from_raw(resized_width, resized_height, resized.into_vec())
        .ok_or_else(|| VqlError::new(ErrorCode::Internal, "resized RGB image is invalid"))?;
    let pad_x = (width - resized_width) / 2;
    let pad_y = (height - resized_height) / 2;
    let mut output = RgbImage::from_pixel(width, height, Rgb([114, 114, 114]));
    output
        .copy_from(&resized, pad_x, pad_y)
        .expect("letterbox dimensions fit");
    Ok((output, scale, pad_x as f32, pad_y as f32))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_preprocessor_batches_nchw_and_keeps_row_context() {
        let processor = ImageTensorPreProcessor::new(ImageTensorOptions {
            width: 4,
            height: 2,
            ..ImageTensorOptions::parse(&ProcessorSpec {
                kind: KIND.to_owned(),
                options: Default::default(),
            })
            .unwrap()
        });
        let batch = processor
            .process(&[DynamicImage::new_rgb8(4, 2), DynamicImage::new_rgb8(2, 2)])
            .unwrap();

        assert_eq!(batch.input.shape(), vec![2, 3, 2, 4]);
        assert_eq!(batch.input.as_f32("input").unwrap().len(), 48);
        assert_eq!(batch.context.transforms.len(), 2);
        assert_eq!(batch.context.transforms[1].pad_x, 1.0);
    }
}
