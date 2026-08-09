use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, Int64Array, StringArray, StringBuilder,
    StructArray,
};
use arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use crate::catalog::{CatalogStore, TableProviderKind};
use crate::media::{DecodedFrame, MediaRuntime};
use crate::types::{image_storage_fields, parse_locator};
use crate::{ErrorCode, Result, VqlError};

#[derive(Debug)]
struct ToJpeg {
    signature: Signature,
    catalog: Arc<CatalogStore>,
    media: Arc<MediaRuntime>,
    fail_on_error: Arc<AtomicBool>,
}

impl ToJpeg {
    fn new(
        catalog: Arc<CatalogStore>,
        media: Arc<MediaRuntime>,
        fail_on_error: Arc<AtomicBool>,
    ) -> Self {
        let image = DataType::Struct(image_storage_fields());
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![image.clone()]),
                    TypeSignature::Exact(vec![image, DataType::Int64]),
                ],
                Volatility::Stable,
            ),
            catalog,
            media,
            fail_on_error,
        }
    }

    fn encode_row(
        &self,
        images: &StructArray,
        quality: Option<&Int64Array>,
        row: usize,
    ) -> Result<Option<Vec<u8>>> {
        if images.is_null(row) {
            return Ok(None);
        }
        let quality = quality
            .filter(|quality| !quality.is_null(row))
            .map_or(85, |quality| quality.value(row));
        if !(1..=100).contains(&quality) {
            return Err(VqlError::new(
                ErrorCode::InvalidOption,
                "TO_JPEG quality must be between 1 and 100",
            ));
        }
        let encoded = images
            .column(4)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| VqlError::new(ErrorCode::Internal, "IMAGE encoded field is invalid"))?;
        let dynamic = if !encoded.is_null(row) {
            image::load_from_memory(encoded.value(row)).map_err(|error| {
                VqlError::new(ErrorCode::Execution, "IMAGE encoded bytes are invalid")
                    .with_source(error)
            })?
        } else {
            self.load_reference(images, row)?
        };
        let mut output = Vec::new();
        image::codecs::jpeg::JpegEncoder::new_with_quality(&mut output, quality as u8)
            .encode_image(&dynamic)
            .map_err(|error| {
                VqlError::new(ErrorCode::Execution, "failed to encode IMAGE as JPEG")
                    .with_source(error)
            })?;
        Ok(Some(output))
    }

    fn load_reference(&self, images: &StructArray, row: usize) -> Result<image::DynamicImage> {
        let locators = images
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| VqlError::new(ErrorCode::Internal, "IMAGE locator field is invalid"))?;
        if locators.is_null(row) {
            return Err(VqlError::new(
                ErrorCode::Execution,
                "IMAGE has neither encoded bytes nor a locator",
            ));
        }
        let locator = parse_locator(locators.value(row))?;
        let table = self.catalog.table_at_revision(locator.table_revision)?;
        let path = resolved_media_path(&table.location, &locator.relative_path)?;
        match table.provider {
            TableProviderKind::Images => image::open(&path).map_err(|error| {
                VqlError::new(
                    ErrorCode::Execution,
                    format!("failed to decode image '{}': {error}", path.display()),
                )
                .with_source(error)
            }),
            TableProviderKind::Videos => {
                let frame = self
                    .media
                    .decode_frame(&path, locator.pts_ms.unwrap_or_default())?;
                decoded_image(frame)
            }
        }
    }
}

impl PartialEq for ToJpeg {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.catalog, &other.catalog)
            && Arc::ptr_eq(&self.media, &other.media)
            && Arc::ptr_eq(&self.fail_on_error, &other.fail_on_error)
    }
}

impl Eq for ToJpeg {}

impl Hash for ToJpeg {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (Arc::as_ptr(&self.catalog) as usize).hash(state);
        (Arc::as_ptr(&self.media) as usize).hash(state);
        (Arc::as_ptr(&self.fail_on_error) as usize).hash(state);
    }
}

impl ScalarUDFImpl for ToJpeg {
    fn name(&self) -> &str {
        "to_jpeg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let Some(images) = arrays
            .first()
            .and_then(|array| array.as_any().downcast_ref::<StructArray>())
        else {
            return exec_err!("TO_JPEG expects IMAGE as its first argument");
        };
        let quality = arrays
            .get(1)
            .map(|array| {
                array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "TO_JPEG quality must be BIGINT".to_owned(),
                    )
                })
            })
            .transpose()?;
        let mut output = BinaryBuilder::with_capacity(images.len(), images.len() * 512);
        for row in 0..images.len() {
            match self.encode_row(images, quality, row) {
                Ok(Some(bytes)) => output.append_value(bytes),
                Ok(None) => output.append_null(),
                Err(error) if error.code == ErrorCode::InvalidOption => {
                    return Err(datafusion::common::DataFusionError::Execution(
                        error.to_string(),
                    ));
                }
                Err(error) => {
                    self.media.record_decode_error();
                    if self.fail_on_error.load(Ordering::Relaxed) {
                        return Err(datafusion::common::DataFusionError::Execution(
                            error.to_string(),
                        ));
                    }
                    output.append_null();
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(output.finish())))
    }
}

pub(crate) fn to_jpeg_udf(
    catalog: Arc<CatalogStore>,
    media: Arc<MediaRuntime>,
    fail_on_error: Arc<AtomicBool>,
) -> ScalarUDF {
    ScalarUDF::new_from_impl(ToJpeg::new(catalog, media, fail_on_error))
}

pub(crate) fn materialize_encoded_images(
    catalog: Arc<CatalogStore>,
    media: Arc<MediaRuntime>,
    images: &StructArray,
    fail_on_error: bool,
) -> Result<ArrayRef> {
    let encoder = ToJpeg::new(catalog, media, Arc::new(AtomicBool::new(fail_on_error)));
    let mut encoded = BinaryBuilder::with_capacity(images.len(), images.len() * 512);
    let mut encoding = StringBuilder::with_capacity(images.len(), images.len() * 4);
    for row in 0..images.len() {
        match encoder.encode_row(images, None, row) {
            Ok(Some(bytes)) => {
                encoded.append_value(bytes);
                encoding.append_value("jpeg");
            }
            Ok(None) => {
                encoded.append_null();
                encoding.append_null();
            }
            Err(error) => {
                encoder.media.record_decode_error();
                if fail_on_error {
                    return Err(error);
                }
                encoded.append_null();
                encoding.append_null();
            }
        }
    }
    let mut columns = images.columns().to_vec();
    columns[4] = Arc::new(encoded.finish());
    columns[5] = Arc::new(encoding.finish());
    Ok(Arc::new(StructArray::new(
        crate::types::image_storage_fields(),
        columns,
        images.nulls().cloned(),
    )))
}

fn resolved_media_path(root: &str, relative: &str) -> Result<PathBuf> {
    let root = Path::new(root).canonicalize()?;
    let path = root.join(relative).canonicalize()?;
    if !path.starts_with(&root) {
        return Err(VqlError::new(
            ErrorCode::InvalidLocation,
            "media locator escapes its table location",
        ));
    }
    Ok(path)
}

fn decoded_image(frame: DecodedFrame) -> Result<image::DynamicImage> {
    let rgb = image::RgbImage::from_raw(frame.width, frame.height, frame.rgb).ok_or_else(|| {
        VqlError::new(
            ErrorCode::Execution,
            "decoded video frame has an invalid RGB buffer length",
        )
    })?;
    Ok(image::DynamicImage::ImageRgb8(rgb))
}
