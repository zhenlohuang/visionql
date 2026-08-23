use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields};

use super::box2d_field;

pub fn locator_field(name: impl Into<String>, nullable: bool) -> Field {
    let char_span = Field::new(
        "char_span",
        DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("start", DataType::Int32, false)),
            Arc::new(Field::new("end", DataType::Int32, false)),
        ])),
        true,
    );
    Field::new(
        name,
        DataType::Struct(Fields::from(vec![
            Arc::new(char_span),
            Arc::new(box2d_field("box", true)),
        ])),
        nullable,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locator_shape_and_nullability_are_stable() {
        let locator = locator_field("locator", true);
        assert!(locator.is_nullable());
        let DataType::Struct(fields) = locator.data_type() else {
            panic!("LOCATOR must use Arrow Struct storage");
        };
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "char_span");
        assert!(fields[0].is_nullable());
        assert_eq!(fields[1].name(), "box");
        assert!(fields[1].is_nullable());
    }
}
