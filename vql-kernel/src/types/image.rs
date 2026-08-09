use arrow::array::{
    ArrayRef, BinaryArray, Int32Array, Int64Array, StringArray, StructArray, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Fields};
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use std::collections::HashMap;
use std::sync::Arc;

use crate::{ErrorCode, Result, VqlError};

const EXTENSION_NAME: &str = "visionql.image";

pub fn image_storage_fields() -> Fields {
    Fields::from(vec![
        Arc::new(Field::new("uri", DataType::Utf8, true)),
        Arc::new(Field::new("locator", DataType::Utf8, true)),
        Arc::new(Field::new("pts_ms", DataType::Int64, true)),
        Arc::new(Field::new("frame_id", DataType::UInt64, true)),
        Arc::new(Field::new("encoded", DataType::Binary, true)),
        Arc::new(Field::new("encoding", DataType::Utf8, true)),
        Arc::new(Field::new("width", DataType::Int32, true)),
        Arc::new(Field::new("height", DataType::Int32, true)),
        Arc::new(Field::new("arena_id", DataType::UInt64, true)),
        Arc::new(Field::new("arena_slot", DataType::UInt32, true)),
    ])
}

pub fn image_field(name: impl Into<String>, nullable: bool) -> Field {
    Field::new(
        name.into(),
        DataType::Struct(image_storage_fields()),
        nullable,
    )
    .with_metadata(HashMap::from([
        ("ARROW:extension:name".to_owned(), EXTENSION_NAME.to_owned()),
        (
            "ARROW:extension:metadata".to_owned(),
            r#"{"version":1}"#.to_owned(),
        ),
    ]))
}

pub fn is_image_field(field: &Field) -> bool {
    field
        .metadata()
        .get("ARROW:extension:name")
        .is_some_and(|name| name == EXTENSION_NAME)
        && is_image_storage(field.data_type())
}

pub fn is_image_storage(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Struct(fields) if fields == &image_storage_fields())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageRef {
    pub uri: Option<String>,
    pub locator: Option<String>,
    pub pts_ms: Option<i64>,
    pub frame_id: Option<u64>,
    pub encoded: Option<Vec<u8>>,
    pub encoding: Option<String>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub arena_id: Option<u64>,
    pub arena_slot: Option<u32>,
}

impl ImageRef {
    pub fn referenced(
        uri: impl Into<String>,
        locator: impl Into<String>,
        width: Option<i32>,
        height: Option<i32>,
    ) -> Self {
        Self {
            uri: Some(uri.into()),
            locator: Some(locator.into()),
            pts_ms: None,
            frame_id: None,
            encoded: None,
            encoding: None,
            width,
            height,
            arena_id: None,
            arena_slot: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct ImageRefBuilder {
    values: Vec<ImageRef>,
}

impl ImageRefBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
        }
    }

    pub fn append(&mut self, value: ImageRef) {
        self.values.push(value);
    }

    pub fn finish(self) -> StructArray {
        let uri = StringArray::from(
            self.values
                .iter()
                .map(|value| value.uri.as_deref())
                .collect::<Vec<_>>(),
        );
        let locator = StringArray::from(
            self.values
                .iter()
                .map(|value| value.locator.as_deref())
                .collect::<Vec<_>>(),
        );
        let pts_ms = Int64Array::from(
            self.values
                .iter()
                .map(|value| value.pts_ms)
                .collect::<Vec<_>>(),
        );
        let frame_id = UInt64Array::from(
            self.values
                .iter()
                .map(|value| value.frame_id)
                .collect::<Vec<_>>(),
        );
        let encoded = BinaryArray::from(
            self.values
                .iter()
                .map(|value| value.encoded.as_deref())
                .collect::<Vec<_>>(),
        );
        let encoding = StringArray::from(
            self.values
                .iter()
                .map(|value| value.encoding.as_deref())
                .collect::<Vec<_>>(),
        );
        let width = Int32Array::from(
            self.values
                .iter()
                .map(|value| value.width)
                .collect::<Vec<_>>(),
        );
        let height = Int32Array::from(
            self.values
                .iter()
                .map(|value| value.height)
                .collect::<Vec<_>>(),
        );
        let arena_id = UInt64Array::from(
            self.values
                .iter()
                .map(|value| value.arena_id)
                .collect::<Vec<_>>(),
        );
        let arena_slot = UInt32Array::from(
            self.values
                .iter()
                .map(|value| value.arena_slot)
                .collect::<Vec<_>>(),
        );

        StructArray::new(
            image_storage_fields(),
            vec![
                Arc::new(uri) as ArrayRef,
                Arc::new(locator),
                Arc::new(pts_ms),
                Arc::new(frame_id),
                Arc::new(encoded),
                Arc::new(encoding),
                Arc::new(width),
                Arc::new(height),
                Arc::new(arena_id),
                Arc::new(arena_slot),
            ],
            None,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaLocator {
    pub table_revision: i64,
    pub relative_path: String,
    pub pts_ms: Option<i64>,
}

pub fn make_locator(table_revision: i64, relative_path: &str, pts_ms: Option<i64>) -> String {
    let encoded = utf8_percent_encode(relative_path, NON_ALPHANUMERIC);
    match pts_ms {
        Some(pts_ms) => format!("vql://media/v1/{table_revision}/{encoded}?pts_ms={pts_ms}"),
        None => format!("vql://media/v1/{table_revision}/{encoded}"),
    }
}

pub fn parse_locator(locator: &str) -> Result<MediaLocator> {
    let prefix = "vql://media/v1/";
    let rest = locator.strip_prefix(prefix).ok_or_else(|| {
        VqlError::new(
            ErrorCode::InvalidLocation,
            "media locator must start with vql://media/v1/",
        )
    })?;
    let (path, query) = rest.split_once('?').unwrap_or((rest, ""));
    let (revision, encoded_path) = path.split_once('/').ok_or_else(|| {
        VqlError::new(
            ErrorCode::InvalidLocation,
            "media locator is missing its source revision or path",
        )
    })?;
    let table_revision = revision.parse::<i64>().map_err(|error| {
        VqlError::new(
            ErrorCode::InvalidLocation,
            "media locator has an invalid source revision",
        )
        .with_source(error)
    })?;
    let relative_path = percent_decode_str(encoded_path)
        .decode_utf8()
        .map_err(|error| {
            VqlError::new(
                ErrorCode::InvalidLocation,
                "media locator path is not valid UTF-8",
            )
            .with_source(error)
        })?
        .into_owned();
    if relative_path.is_empty() {
        return Err(VqlError::new(
            ErrorCode::InvalidLocation,
            "media locator path cannot be empty",
        ));
    }
    let pts_ms = if query.is_empty() {
        None
    } else {
        let value = query.strip_prefix("pts_ms=").ok_or_else(|| {
            VqlError::new(
                ErrorCode::InvalidLocation,
                "media locator contains an unsupported query parameter",
            )
        })?;
        Some(value.parse::<i64>().map_err(|error| {
            VqlError::new(
                ErrorCode::InvalidLocation,
                "media locator has an invalid pts_ms",
            )
            .with_source(error)
        })?)
    };

    Ok(MediaLocator {
        table_revision,
        relative_path,
        pts_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locator_round_trips_paths_and_pts() {
        let locator = make_locator(42, "nested/门口 1.png", Some(1250));
        assert_eq!(
            parse_locator(&locator).unwrap(),
            MediaLocator {
                table_revision: 42,
                relative_path: "nested/门口 1.png".to_owned(),
                pts_ms: Some(1250),
            }
        );
    }

    #[test]
    fn image_field_has_standard_extension_metadata() {
        let field = image_field("image", false);
        assert!(is_image_field(&field));
        assert!(is_image_storage(field.data_type()));
        assert_eq!(
            field.metadata().get("ARROW:extension:metadata").unwrap(),
            r#"{"version":1}"#
        );
    }
}
