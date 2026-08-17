use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, StringArray, StringBuilder, StructArray,
    UInt32Array, UInt64Array,
};
use arrow::record_batch::RecordBatch;

use crate::catalog::{CatalogStore, TableProviderKind};
use crate::media::{DecodedFrame, MediaRuntime};
use crate::resources::{QueryBudget, QueryReservation};
use crate::types::{is_image_field, parse_locator};
use crate::{ErrorCode, Result, VqlError};

const DEFAULT_JPEG_QUALITY: u8 = 85;

struct ImageEncoder {
    catalog: Arc<CatalogStore>,
    media: Arc<MediaRuntime>,
    budget: Option<QueryBudget>,
}

impl ImageEncoder {
    fn new(
        catalog: Arc<CatalogStore>,
        media: Arc<MediaRuntime>,
        budget: Option<QueryBudget>,
    ) -> Self {
        Self {
            catalog,
            media,
            budget,
        }
    }

    fn encode_row(&self, images: &StructArray, row: usize) -> Result<Option<EncodedRow>> {
        if images.is_null(row) {
            return Ok(None);
        }
        let encoded = images
            .column(4)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| VqlError::new(ErrorCode::Internal, "IMAGE encoded field is invalid"))?;
        let (loaded, _decoded_reservation) = if !encoded.is_null(row) {
            let bytes = encoded.value(row);
            let dimensions = image::ImageReader::new(Cursor::new(bytes))
                .with_guessed_format()
                .map_err(|error| {
                    VqlError::new(ErrorCode::Execution, "IMAGE encoded format is invalid")
                        .with_source(error)
                })?
                .into_dimensions()
                .map_err(|error| {
                    VqlError::new(ErrorCode::Execution, "IMAGE encoded dimensions are invalid")
                        .with_source(error)
                })?;
            let reservation = self.reserve_decoded(dimensions.0, dimensions.1, 4)?;
            let image = image::load_from_memory(bytes).map_err(|error| {
                VqlError::new(ErrorCode::Execution, "IMAGE encoded bytes are invalid")
                    .with_source(error)
            })?;
            (image, reservation)
        } else if let Some((frame, reservation)) = self.load_frame_buffer(images, row)? {
            (decoded_image(frame)?, reservation)
        } else {
            self.load_reference(images, row)?
        };
        let mut output = BudgetedBuffer::new(self.budget.as_ref(), crate::QueryResource::Media)?;
        if let Err(error) =
            image::codecs::jpeg::JpegEncoder::new_with_quality(&mut output, DEFAULT_JPEG_QUALITY)
                .encode_image(&loaded)
        {
            if let Some(error) = output.take_reservation_error() {
                return Err(error);
            }
            return Err(
                VqlError::new(ErrorCode::Execution, "failed to encode IMAGE as JPEG")
                    .with_source(error),
            );
        }
        let (bytes, reservation) = output.into_parts();
        Ok(Some(EncodedRow {
            bytes,
            _reservation: reservation,
        }))
    }

    fn load_frame_buffer(
        &self,
        images: &StructArray,
        row: usize,
    ) -> Result<Option<(DecodedFrame, Option<QueryReservation>)>> {
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
        let buffer_id = buffer_ids.value(row);
        let buffer_slot = buffer_slots.value(row);
        let reservation = self.budget.as_ref().map(|budget| {
            self.media
                .buffered_frame_bytes(buffer_id, buffer_slot)
                .and_then(|bytes| budget.reserve(crate::QueryResource::Media, bytes))
        });
        let reservation = reservation.transpose()?;
        self.media
            .resolve_buffered_frame(buffer_id, buffer_slot)
            .map(|frame| Some((frame, reservation)))
    }

    fn load_reference(
        &self,
        images: &StructArray,
        row: usize,
    ) -> Result<(image::DynamicImage, Option<QueryReservation>)> {
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
            TableProviderKind::Images => {
                let (width, height) = image::image_dimensions(&path).map_err(|error| {
                    VqlError::new(
                        ErrorCode::Execution,
                        format!("failed to inspect image '{}': {error}", path.display()),
                    )
                    .with_source(error)
                })?;
                let reservation = self.reserve_decoded(width, height, 4)?;
                let image = image::open(&path).map_err(|error| {
                    VqlError::new(
                        ErrorCode::Execution,
                        format!("failed to decode image '{}': {error}", path.display()),
                    )
                    .with_source(error)
                })?;
                Ok((image, reservation))
            }
            TableProviderKind::Videos => {
                let metadata = self.media.probe(&path)?;
                let width = u32::try_from(metadata.width).map_err(|_| {
                    VqlError::new(ErrorCode::Execution, "video width must be non-negative")
                })?;
                let height = u32::try_from(metadata.height).map_err(|_| {
                    VqlError::new(ErrorCode::Execution, "video height must be non-negative")
                })?;
                let reservation = self.reserve_decoded(width, height, 3)?;
                let frame = self
                    .media
                    .decode_frame(&path, locator.pts_ms.unwrap_or_default())?;
                Ok((decoded_image(frame)?, reservation))
            }
        }
    }

    fn reserve_decoded(
        &self,
        width: u32,
        height: u32,
        bytes_per_pixel: usize,
    ) -> Result<Option<QueryReservation>> {
        let Some(budget) = &self.budget else {
            return Ok(None);
        };
        let bytes = (width as usize)
            .checked_mul(height as usize)
            .and_then(|value| value.checked_mul(bytes_per_pixel))
            .ok_or_else(|| {
                VqlError::new(
                    ErrorCode::ResourceExhausted,
                    "decoded IMAGE size exceeds platform limits",
                )
            })?;
        budget.reserve(crate::QueryResource::Media, bytes).map(Some)
    }
}

struct EncodedRow {
    bytes: Vec<u8>,
    _reservation: Option<QueryReservation>,
}

struct BudgetedBuffer {
    bytes: Vec<u8>,
    reservation: Option<QueryReservation>,
    reservation_error: Option<VqlError>,
}

impl BudgetedBuffer {
    fn new(budget: Option<&QueryBudget>, resource: crate::QueryResource) -> Result<Self> {
        Ok(Self {
            bytes: Vec::new(),
            reservation: budget
                .map(|budget| budget.reserve(resource, 0))
                .transpose()?,
            reservation_error: None,
        })
    }

    fn into_parts(self) -> (Vec<u8>, Option<QueryReservation>) {
        (self.bytes, self.reservation)
    }

    fn take_reservation_error(&mut self) -> Option<VqlError> {
        self.reservation_error.take()
    }
}

impl Write for BudgetedBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let size = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("buffer size exceeds platform limits"))?;
        if let Some(reservation) = &mut self.reservation
            && let Err(error) = reservation.try_resize(size)
        {
            let message = error.to_string();
            self.reservation_error = Some(error);
            return Err(std::io::Error::other(message));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) struct MaterializedImages {
    pub(crate) array: ArrayRef,
    pub(crate) reservations: Vec<QueryReservation>,
}

pub(crate) struct MaterializedBatch {
    pub(crate) batch: RecordBatch,
    pub(crate) reservations: Vec<QueryReservation>,
}

pub(crate) fn materialize_encoded_images(
    catalog: Arc<CatalogStore>,
    media: Arc<MediaRuntime>,
    images: &StructArray,
    fail_on_error: bool,
    budget: Option<QueryBudget>,
) -> Result<MaterializedImages> {
    let encoder = ImageEncoder::new(catalog, media, budget.clone());
    let mut encoded = BinaryBuilder::new();
    let mut encoding = StringBuilder::new();
    let mut output_reservation = budget
        .as_ref()
        .map(|budget| budget.reserve(crate::QueryResource::Arrow, 0))
        .transpose()?;
    for row in 0..images.len() {
        match encoder.encode_row(images, row) {
            Ok(Some(encoded_row)) => {
                if let Some(reservation) = &mut output_reservation {
                    reservation.try_grow(encoded_row.bytes.len())?;
                }
                encoded.append_value(&encoded_row.bytes);
                encoding.append_value("jpeg");
            }
            Ok(None) => {
                encoded.append_null();
                encoding.append_null();
            }
            Err(error) => {
                encoder.media.record_decode_error();
                if fail_on_error || error.code == ErrorCode::ResourceExhausted {
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
    Ok(MaterializedImages {
        array: Arc::new(StructArray::new(
            crate::types::image_storage_fields(),
            columns,
            images.nulls().cloned(),
        )),
        reservations: output_reservation.into_iter().collect(),
    })
}

pub(crate) fn materialize_batch_images(
    catalog: Arc<CatalogStore>,
    media: Arc<MediaRuntime>,
    batch: RecordBatch,
    fail_on_error: bool,
    budget: QueryBudget,
) -> Result<MaterializedBatch> {
    let mut columns = batch.columns().to_vec();
    let mut reservations = Vec::new();
    for (index, field) in batch.schema().fields().iter().enumerate() {
        if !is_image_field(field) {
            continue;
        }
        let images = columns[index]
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| VqlError::new(ErrorCode::Internal, "IMAGE column is not StructArray"))?;
        let materialized = materialize_encoded_images(
            Arc::clone(&catalog),
            Arc::clone(&media),
            images,
            fail_on_error,
            Some(budget.clone()),
        )?;
        columns[index] = materialized.array;
        reservations.extend(materialized.reservations);
    }
    let batch = RecordBatch::try_new(batch.schema(), columns).map_err(|error| {
        VqlError::new(
            ErrorCode::Execution,
            "failed to materialize streaming IMAGE output",
        )
        .with_source(error)
    })?;
    Ok(MaterializedBatch {
        batch,
        reservations,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::ResourceMetrics;
    use crate::types::{ImageRef, ImageRefBuilder};
    use image::ImageFormat;
    use tempfile::tempdir;

    fn encoded_image(width: u32, height: u32) -> StructArray {
        let mut bytes = Cursor::new(Vec::new());
        image::DynamicImage::new_rgb8(width, height)
            .write_to(&mut bytes, ImageFormat::Png)
            .unwrap();
        let mut images = ImageRefBuilder::with_capacity(1);
        images.append(ImageRef {
            uri: None,
            locator: None,
            pts_ms: None,
            frame_id: None,
            encoded: Some(bytes.into_inner()),
            encoding: Some("png".to_owned()),
            width: Some(width as i32),
            height: Some(height as i32),
            buffer_id: None,
            buffer_slot: None,
        });
        images.finish()
    }

    #[test]
    fn image_materialization_reserves_before_decode_and_releases_output() {
        let temp = tempdir().unwrap();
        let catalog = Arc::new(CatalogStore::open(&temp.path().join("catalog.db")).unwrap());
        let media = Arc::new(MediaRuntime::new());
        let metrics = Arc::new(ResourceMetrics::default());
        let budget = QueryBudget::new(64 * 64 * 4 - 1, Arc::clone(&metrics));

        let error = materialize_encoded_images(
            Arc::clone(&catalog),
            Arc::clone(&media),
            &encoded_image(64, 64),
            false,
            Some(budget),
        )
        .err()
        .unwrap();

        assert_eq!(error.code, ErrorCode::ResourceExhausted);
        assert_eq!(metrics.total_usage().current_bytes, 0);

        let metrics = Arc::new(ResourceMetrics::default());
        let budget = QueryBudget::new(1024 * 1024, Arc::clone(&metrics));
        let materialized =
            materialize_encoded_images(catalog, media, &encoded_image(64, 64), true, Some(budget))
                .unwrap();
        assert_eq!(materialized.array.len(), 1);
        assert!(metrics.usage(crate::QueryResource::Media).peak_bytes > 0);
        assert!(metrics.usage(crate::QueryResource::Arrow).current_bytes > 0);
        drop(materialized);
        assert_eq!(metrics.total_usage().current_bytes, 0);
    }
}
