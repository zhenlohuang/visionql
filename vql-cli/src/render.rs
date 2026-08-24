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

#[cfg(test)]
mod tests {
    use arrow::array::{
        ArrayRef, BinaryArray, Int64Array, StringArray, StructArray, UInt32Array, UInt64Array,
    };
    use arrow::buffer::NullBuffer;
    use vql_kernel::image_field;

    use super::*;

    #[test]
    fn image_columns_render_as_safe_text_summaries() {
        let image = image_field("image", true);
        let DataType::Struct(storage_fields) = image.data_type() else {
            panic!("IMAGE storage must be a Struct");
        };
        let images = StructArray::new(
            storage_fields.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("file:///one.png"), None])) as ArrayRef,
                Arc::new(StringArray::new_null(2)),
                Arc::new(Int64Array::new_null(2)),
                Arc::new(UInt64Array::new_null(2)),
                Arc::new(BinaryArray::new_null(2)),
                Arc::new(StringArray::new_null(2)),
                Arc::new(Int32Array::from(vec![Some(640), None])),
                Arc::new(Int32Array::from(vec![Some(480), None])),
                Arc::new(UInt64Array::new_null(2)),
                Arc::new(UInt32Array::new_null(2)),
            ],
            Some(NullBuffer::from(vec![true, false])),
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![image])),
            vec![Arc::new(images) as ArrayRef],
        )
        .unwrap();

        let rendered = summarize_images(&batch).unwrap();

        let values = rendered
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "<image uri=file:///one.png 640x480>");
        assert!(values.is_null(1));
        assert_eq!(rendered.schema().field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn scalar_columns_pass_through_and_empty_results_render() {
        let values = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        let batch = RecordBatch::try_from_iter(vec![("value", Arc::clone(&values))]).unwrap();

        let rendered = summarize_images(&batch).unwrap();

        assert!(Arc::ptr_eq(rendered.column(0), &values));
        print_batches(&[]).unwrap();
    }
}
