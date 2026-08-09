use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use vql_kernel::{ErrorCode, Result, VqlError, is_image_storage};

pub(crate) fn print_batches(batches: &[RecordBatch]) -> Result<()> {
    if batches.is_empty() {
        println!("(no rows)");
        return Ok(());
    }
    let display = batches
        .iter()
        .map(summarize_images)
        .collect::<Result<Vec<_>>>()?;
    let table = pretty_format_batches(&display).map_err(|error| {
        VqlError::new(ErrorCode::Execution, "failed to render query result").with_source(error)
    })?;
    println!("{table}");
    Ok(())
}

fn summarize_images(batch: &RecordBatch) -> Result<RecordBatch> {
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if is_image_storage(field.data_type()) {
            let images = column
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    VqlError::new(ErrorCode::Internal, "IMAGE column is not a StructArray")
                })?;
            let uris = images
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| VqlError::new(ErrorCode::Internal, "IMAGE uri is not Utf8"))?;
            let widths = images
                .column(6)
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| VqlError::new(ErrorCode::Internal, "IMAGE width is not Int32"))?;
            let heights = images
                .column(7)
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| VqlError::new(ErrorCode::Internal, "IMAGE height is not Int32"))?;
            let summaries = (0..images.len())
                .map(|row| {
                    if images.is_null(row) {
                        None
                    } else {
                        let uri = if uris.is_null(row) {
                            "?"
                        } else {
                            uris.value(row)
                        };
                        let dimensions = if widths.is_null(row) || heights.is_null(row) {
                            "?x?".to_owned()
                        } else {
                            format!("{}x{}", widths.value(row), heights.value(row))
                        };
                        Some(format!("<image uri={uri} {dimensions}>"))
                    }
                })
                .collect::<Vec<_>>();
            fields.push(Arc::new(Field::new(field.name(), DataType::Utf8, true)));
            columns.push(Arc::new(StringArray::from(summaries)) as ArrayRef);
        } else {
            fields.push(Arc::clone(field));
            columns.push(Arc::clone(column));
        }
    }
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    RecordBatch::try_new(schema, columns).map_err(|error| {
        VqlError::new(ErrorCode::Execution, "failed to summarize IMAGE values").with_source(error)
    })
}
