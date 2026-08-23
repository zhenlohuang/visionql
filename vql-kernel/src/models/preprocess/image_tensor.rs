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
const SUPPORTED_TYPES: &[ModelType] = &[ModelType::ObjectDetection, ModelType::ImageClassification];

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ResizeMode {
    #[default]
    Letterbox,
    #[serde(rename = "center_crop")]
    CenterCrop,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ColorSpace {
    #[default]
    Rgb,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum TensorLayout {
    #[default]
    Nchw,
    Nhwc,
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
    #[serde(default)]
    resize: ResizeMode,
    #[serde(default, rename = "color_space")]
    _color_space: ColorSpace,
    #[serde(default, rename = "layout")]
    layout: TensorLayout,
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
}

impl ImageTensorOptions {
    fn parse(spec: &ProcessorSpec) -> Result<Self> {
        let mut options: Self =
            deserialize_processor_options("pre_processor.options", &spec.options)?;
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
        match options.preprocess.as_deref() {
            Some(value) if value.eq_ignore_ascii_case("imagenet") => {
                options.mean = Some(vec![0.485, 0.456, 0.406]);
                options.std = Some(vec![0.229, 0.224, 0.225]);
                options.scale = Some(1.0 / 255.0);
                options.pad_value = Some(0.0);
            }
            Some(_) => {
                return super::super::registry::invalid_option(
                    "pre_processor.options.preprocess",
                    "must be the supported 'imagenet' preset",
                );
            }
            None => {}
        }
        let mean = options.mean.get_or_insert_with(|| vec![0.0; 3]);
        let std = options.std.get_or_insert_with(|| vec![1.0; 3]);
        if mean.len() != 3 || mean.iter().any(|value| !value.is_finite()) {
            return super::super::registry::invalid_option(
                "pre_processor.options.mean",
                "must contain three finite channel values",
            );
        }
        if std.len() != 3 || std.iter().any(|value| !value.is_finite() || *value == 0.0) {
            return super::super::registry::invalid_option(
                "pre_processor.options.std",
                "must contain three finite non-zero channel values",
            );
        }
        let scale = options.scale.get_or_insert(1.0 / 255.0);
        if !scale.is_finite() || *scale <= 0.0 {
            return super::super::registry::invalid_option(
                "pre_processor.options.scale",
                "must be a positive finite number",
            );
        }
        let pad_value = options.pad_value.get_or_insert(114.0);
        if !pad_value.is_finite() || !(0.0..=255.0).contains(pad_value) {
            return super::super::registry::invalid_option(
                "pre_processor.options.pad_value",
                "must be a finite pixel value between 0 and 255",
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
            shape: match options.layout {
                TensorLayout::Nchw => {
                    vec![-1, 3, i64::from(options.height), i64::from(options.width)]
                }
                TensorLayout::Nhwc => {
                    vec![-1, i64::from(options.height), i64::from(options.width), 3]
                }
            },
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

    fn output_bytes(&self, batch_size: usize) -> Result<usize> {
        batch_size
            .checked_mul(3)
            .and_then(|value| value.checked_mul(self.options.width as usize))
            .and_then(|value| value.checked_mul(self.options.height as usize))
            .and_then(|value| value.checked_mul(std::mem::size_of::<f32>()))
            .ok_or_else(|| {
                VqlError::new(ErrorCode::ResourceExhausted, "input tensor size overflow")
            })
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
        let mean = self.options.mean.as_deref().expect("validated image mean");
        let std = self.options.std.as_deref().expect("validated image std");
        let scale_factor = self.options.scale.expect("validated image scale");
        let pad_value = self.options.pad_value.expect("validated pad value").round() as u8;
        for (batch_index, image) in images.iter().enumerate() {
            let (pixels, scale, pad_x, pad_y) = match self.options.resize {
                ResizeMode::Letterbox => {
                    letterbox(image, self.options.width, self.options.height, pad_value)?
                }
                ResizeMode::CenterCrop => {
                    center_crop(image, self.options.width, self.options.height)
                }
            };
            let offset = batch_index * 3 * plane;
            for (index, pixel) in pixels.pixels().enumerate() {
                let normalized = [
                    (f32::from(pixel[0]) * scale_factor - mean[0]) / std[0],
                    (f32::from(pixel[1]) * scale_factor - mean[1]) / std[1],
                    (f32::from(pixel[2]) * scale_factor - mean[2]) / std[2],
                ];
                match self.options.layout {
                    TensorLayout::Nchw => {
                        input[offset + index] = normalized[0];
                        input[offset + plane + index] = normalized[1];
                        input[offset + 2 * plane + index] = normalized[2];
                    }
                    TensorLayout::Nhwc => {
                        let pixel_offset = offset + index * 3;
                        input[pixel_offset..pixel_offset + 3].copy_from_slice(&normalized);
                    }
                }
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
                {
                    let batch_size = i64::try_from(batch_size).map_err(|_| {
                        VqlError::new(ErrorCode::Execution, "batch size is too large")
                    })?;
                    match self.options.layout {
                        TensorLayout::Nchw => vec![
                            batch_size,
                            3,
                            i64::from(self.options.height),
                            i64::from(self.options.width),
                        ],
                        TensorLayout::Nhwc => vec![
                            batch_size,
                            i64::from(self.options.height),
                            i64::from(self.options.width),
                            3,
                        ],
                    }
                },
                input,
                Some(vec!["C".to_owned(), "H".to_owned(), "W".to_owned()]),
            )?,
            context: PreProcessContext { transforms },
        })
    }
}

fn center_crop(image: &DynamicImage, width: u32, height: u32) -> (RgbImage, f32, f32, f32) {
    let scale = (width as f32 / image.width() as f32).max(height as f32 / image.height() as f32);
    let resized_width = image.width() as f32 * scale;
    let resized_height = image.height() as f32 * scale;
    let pixels = image
        .resize_to_fill(width, height, image::imageops::FilterType::Triangle)
        .to_rgb8();
    (
        pixels,
        scale,
        -((resized_width - width as f32) / 2.0),
        -((resized_height - height as f32) / 2.0),
    )
}

fn letterbox(
    image: &DynamicImage,
    width: u32,
    height: u32,
    pad_value: u8,
) -> Result<(RgbImage, f32, f32, f32)> {
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
    let mut output = RgbImage::from_pixel(width, height, Rgb([pad_value; 3]));
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
        assert_eq!(
            processor.output_bytes(2).unwrap(),
            48 * std::mem::size_of::<f32>()
        );
        assert_eq!(batch.context.transforms.len(), 2);
        assert_eq!(batch.context.transforms[1].pad_x, 1.0);
    }
}
