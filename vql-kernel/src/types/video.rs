use arrow::datatypes::{DataType, Field, Fields};
use std::collections::HashMap;
use std::sync::Arc;

pub fn video_field(name: impl Into<String>, nullable: bool) -> Field {
    let fields = Fields::from(vec![
        Arc::new(Field::new("uri", DataType::Utf8, true)),
        Arc::new(Field::new("locator", DataType::Utf8, true)),
        Arc::new(Field::new("duration_ns", DataType::Int64, true)),
        Arc::new(Field::new("fps", DataType::Float64, true)),
        Arc::new(Field::new("width", DataType::Int32, true)),
        Arc::new(Field::new("height", DataType::Int32, true)),
        Arc::new(Field::new("codec", DataType::Utf8, true)),
    ]);
    Field::new(name.into(), DataType::Struct(fields), nullable).with_metadata(HashMap::from([
        ("ARROW:extension:name".to_owned(), "vql.video".to_owned()),
        (
            "ARROW:extension:metadata".to_owned(),
            r#"{"version":1}"#.to_owned(),
        ),
    ]))
}
