use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, StringArray, StringBuilder, StructArray,
    UInt32Array, UInt64Array,
};
use arrow::record_batch::RecordBatch;

use crate::catalog::{CatalogStore, TableProviderKind};
use crate::media::{DecodedFrame, MediaRuntime};
use crate::types::{is_image_field, parse_locator};
use crate::{ErrorCode, Result, VqlError};

const DEFAULT_JPEG_QUALITY: u8 = 85;

struct ImageEncoder {
    catalog: Arc<CatalogStore>,
    media: Arc<MediaRuntime>,
}

impl ImageEncoder {
    fn new(catalog: Arc<CatalogStore>, media: Arc<MediaRuntime>) -> Self {
        Self { catalog, media }
    }

    fn encode_row(&self, images: &StructArray, row: usize) -> Result<Option<Vec<u8>>> {
        if images.is_null(row) {
            return Ok(None);
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
        } else if let Some(frame) = self.load_frame_buffer(images, row)? {
            decoded_image(frame)?
        } else {
            self.load_reference(images, row)?
        };
        let mut output = Vec::new();
        image::codecs::jpeg::JpegEncoder::new_with_quality(&mut output, DEFAULT_JPEG_QUALITY)
            .encode_image(&dynamic)
            .map_err(|error| {
                VqlError::new(ErrorCode::Execution, "failed to encode IMAGE as JPEG")
                    .with_source(error)
            })?;
        Ok(Some(output))
    }

    fn load_frame_buffer(&self, images: &StructArray, row: usize) -> Result<Option<DecodedFrame>> {
        let buffer_ids = images
            .column(8)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| {
                VqlError::new(ErrorCode::Internal, "IMAGE buffer_id field is invalid")
            })?;
        let buffer_slots = images
            .column(9)
            .as_any()
            .downcast_ref::<arrow::array::UInt32Array>()
            .ok_or_else(|| {
                VqlError::new(ErrorCode::Internal, "IMAGE buffer_slot field is invalid")
            })?;
        if buffer_ids.is_null(row) || buffer_slots.is_null(row) {
            return Ok(None);
        }
        self.media
            .resolve_buffered_frame(buffer_ids.value(row), buffer_slots.value(row))
            .map(Some)
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
                "IMAGE has neither encoded bytes, a frame buffer slot, nor a locator",
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

pub(crate) fn materialize_encoded_images(
    catalog: Arc<CatalogStore>,
    media: Arc<MediaRuntime>,
    images: &StructArray,
    fail_on_error: bool,
) -> Result<ArrayRef> {
    let encoder = ImageEncoder::new(catalog, media);
    let mut encoded = BinaryBuilder::with_capacity(images.len(), images.len() * 512);
    let mut encoding = StringBuilder::with_capacity(images.len(), images.len() * 4);
    for row in 0..images.len() {
        match encoder.encode_row(images, row) {
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
    columns[8] = Arc::new(UInt64Array::from(vec![None; images.len()]));
    columns[9] = Arc::new(UInt32Array::from(vec![None; images.len()]));
    Ok(Arc::new(StructArray::new(
        crate::types::image_storage_fields(),
        columns,
        images.nulls().cloned(),
    )))
}

pub(crate) fn materialize_batch_images(
    catalog: Arc<CatalogStore>,
    media: Arc<MediaRuntime>,
    batch: RecordBatch,
    fail_on_error: bool,
) -> Result<RecordBatch> {
    let mut columns = batch.columns().to_vec();
    for (index, field) in batch.schema().fields().iter().enumerate() {
        if !is_image_field(field) {
            continue;
        }
        let images = columns[index]
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| VqlError::new(ErrorCode::Internal, "IMAGE column is not StructArray"))?;
        columns[index] = materialize_encoded_images(
            Arc::clone(&catalog),
            Arc::clone(&media),
            images,
            fail_on_error,
        )?;
    }
    RecordBatch::try_new(batch.schema(), columns).map_err(|error| {
        VqlError::new(
            ErrorCode::Execution,
            "failed to materialize streaming IMAGE output",
        )
        .with_source(error)
    })
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
