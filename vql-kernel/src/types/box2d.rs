use arrow::datatypes::{DataType, Field, Fields};
use std::collections::HashMap;
use std::sync::Arc;

pub fn box2d_field(name: impl Into<String>, nullable: bool) -> Field {
    let fields = Fields::from(vec![
        Arc::new(Field::new("x", DataType::Float32, false)),
        Arc::new(Field::new("y", DataType::Float32, false)),
        Arc::new(Field::new("w", DataType::Float32, false)),
        Arc::new(Field::new("h", DataType::Float32, false)),
    ]);
    Field::new(name.into(), DataType::Struct(fields), nullable).with_metadata(HashMap::from([(
        "ARROW:extension:name".to_owned(),
        "vql.box2d".to_owned(),
    )]))
}
