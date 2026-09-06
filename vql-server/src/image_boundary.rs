use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, Int32Builder, StringArray, StringBuilder,
    StructArray, UInt32Array, UInt64Array,
};
use arrow::record_batch::RecordBatch;
use image::ImageReader;
use vql_kernel::{ErrorCode, Result, VqlError};

use crate::config::ServiceConfig;

pub fn sanitize_batch(
    batch: RecordBatch,
    config: &ServiceConfig,
    result_bytes: &AtomicUsize,
    enforce_total: bool,
) -> Result<RecordBatch> {
    let mut columns = batch.columns().to_vec();
    for (index, field) in batch.schema().fields().iter().enumerate() {
        if field
            .metadata()
            .get("ARROW:extension:name")
            .is_none_or(|name| name != "vql.image")
        {
            continue;
        }
        let images = columns[index]
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                VqlError::new(
                    ErrorCode::Internal,
                    "IMAGE Flight column is not a StructArray",
                )
            })?;
        columns[index] = sanitize_images(images, config)?;
    }
    let batch = RecordBatch::try_new(batch.schema(), columns)?;
    let batch_size = batch.get_array_memory_size();
    if batch_size > config.batch_bytes {
        return Err(VqlError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "Flight batch uses {batch_size} bytes and exceeds the {} byte limit",
                config.batch_bytes
            ),
        ));
    }
    if enforce_total {
        let previous = result_bytes.fetch_add(batch_size, Ordering::Relaxed);
        if previous.saturating_add(batch_size) > config.result_bytes {
            result_bytes.fetch_sub(batch_size, Ordering::Relaxed);
            return Err(VqlError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    "Flight result exceeds the {} byte limit",
                    config.result_bytes
                ),
            ));
        }
    }
    Ok(batch)
}

fn sanitize_images(images: &StructArray, config: &ServiceConfig) -> Result<ArrayRef> {
    let encoded = images
        .column(4)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| VqlError::new(ErrorCode::Internal, "IMAGE encoded field is invalid"))?;
    let mut output = BinaryBuilder::new();
    let mut encoding = StringBuilder::new();
    let mut widths = Int32Builder::new();
    let mut heights = Int32Builder::new();
    for row in 0..images.len() {
        if images.is_null(row) || encoded.is_null(row) {
            output.append_null();
            encoding.append_null();
            widths.append_null();
            heights.append_null();
            continue;
        }
        let source = encoded.value(row);
        let reader = ImageReader::new(Cursor::new(source))
            .with_guessed_format()
            .map_err(|error| {
                VqlError::new(ErrorCode::Execution, "Flight IMAGE encoding is invalid")
                    .with_source(error)
            })?;
        let image = reader.decode().map_err(|error| {
            VqlError::new(ErrorCode::Execution, "Flight IMAGE cannot be decoded").with_source(error)
        })?;
        let thumbnail = image.thumbnail(config.thumbnail_max_width, config.thumbnail_max_height);
        let width = i32::try_from(thumbnail.width()).map_err(|_| {
            VqlError::new(
                ErrorCode::ResourceExhausted,
                "Flight IMAGE width exceeds limits",
            )
        })?;
        let height = i32::try_from(thumbnail.height()).map_err(|_| {
            VqlError::new(
                ErrorCode::ResourceExhausted,
                "Flight IMAGE height exceeds limits",
            )
        })?;
        let mut bytes = Vec::new();
        image::codecs::jpeg::JpegEncoder::new_with_quality(&mut bytes, 80)
            .encode_image(&thumbnail)
            .map_err(|error| {
                VqlError::new(
                    ErrorCode::Execution,
                    "failed to encode Flight IMAGE thumbnail",
                )
                .with_source(error)
            })?;
        if bytes.len() > config.image_cell_bytes {
            return Err(VqlError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    "Flight IMAGE uses {} bytes and exceeds the {} byte cell limit",
                    bytes.len(),
                    config.image_cell_bytes
                ),
            ));
        }
        output.append_value(bytes);
        encoding.append_value("jpeg");
        widths.append_value(width);
        heights.append_value(height);
    }

    let rows = images.len();
    let uri = Arc::new(StringArray::from(vec![None::<&str>; rows])) as ArrayRef;
    let locator = Arc::new(StringArray::from(vec![None::<&str>; rows])) as ArrayRef;
    let pts_ms = Arc::clone(images.column(2));
    let frame_id = Arc::clone(images.column(3));
    let buffer_id = Arc::new(UInt64Array::from(vec![None; rows])) as ArrayRef;
    let buffer_slot = Arc::new(UInt32Array::from(vec![None; rows])) as ArrayRef;
    Ok(Arc::new(StructArray::new(
        vql_catalog::image_storage_fields(),
        vec![
            uri,
            locator,
            pts_ms,
            frame_id,
            Arc::new(output.finish()),
            Arc::new(encoding.finish()),
            Arc::new(widths.finish()),
            Arc::new(heights.finish()),
            buffer_id,
            buffer_slot,
        ],
        images.nulls().cloned(),
    )))
}

#[cfg(test)]
mod tests {
    use arrow::array::StructArray;
    use arrow::datatypes::Schema;
    use image::ImageFormat;
    use vql_catalog::image_field;

    use super::*;

    #[test]
    fn image_boundary_nulls_private_fields_and_bounds_dimensions() {
        let mut encoded = Cursor::new(Vec::new());
        image::DynamicImage::new_rgb8(1024, 256)
            .write_to(&mut encoded, ImageFormat::Jpeg)
            .unwrap();
        let images = StructArray::new(
            vql_catalog::image_storage_fields(),
            vec![
                Arc::new(StringArray::from(vec![Some("rtsp://camera/live")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("vql://media/v1/1/frame")])) as ArrayRef,
                Arc::new(arrow::array::Int64Array::from(vec![Some(1)])) as ArrayRef,
                Arc::new(arrow::array::UInt64Array::from(vec![Some(2)])) as ArrayRef,
                Arc::new(BinaryArray::from(vec![Some(encoded.get_ref().as_slice())])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("jpeg")])) as ArrayRef,
                Arc::new(arrow::array::Int32Array::from(vec![Some(1024)])) as ArrayRef,
                Arc::new(arrow::array::Int32Array::from(vec![Some(256)])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![Some(7)])) as ArrayRef,
                Arc::new(UInt32Array::from(vec![Some(8)])) as ArrayRef,
            ],
            None,
        );
        let schema = Arc::new(Schema::new(vec![image_field("image", false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(images)]).unwrap();
        let sanitized =
            sanitize_batch(batch, &ServiceConfig::default(), &AtomicUsize::new(0), true).unwrap();
        let images = sanitized
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(images.column(0).is_null(0));
        assert!(images.column(1).is_null(0));
        assert!(images.column(8).is_null(0));
        assert!(images.column(9).is_null(0));
        assert_eq!(
            images
                .column(6)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap()
                .value(0),
            512
        );
    }
}
