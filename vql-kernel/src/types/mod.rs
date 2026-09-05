mod box2d;
mod image;
mod locator;
mod video;

pub use box2d::box2d_field;
pub use image::{
    ImageRef, ImageRefBuilder, MediaLocator, image_field, image_storage_fields, is_image_field,
    is_image_storage, make_locator, parse_locator,
};
pub use locator::locator_field;
pub use video::video_field;

use crate::{Result, VqlError};
use arrow::datatypes::Field;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum VqlType {
    Arrow,
    Image,
    Video,
    Box2d,
    Audio,
    Mask,
}

pub fn logical_type_of(field: &Field) -> VqlType {
    match field
        .metadata()
        .get("ARROW:extension:name")
        .map(String::as_str)
    {
        Some("vql.image") => VqlType::Image,
        Some("vql.video") => VqlType::Video,
        Some("vql.box2d") => VqlType::Box2d,
        Some("vql.audio") => VqlType::Audio,
        Some("vql.mask") => VqlType::Mask,
        _ => VqlType::Arrow,
    }
}

pub fn audio_field(_name: impl Into<String>, _nullable: bool) -> Result<Field> {
    Err(VqlError::feature(
        "AUDIO is a reserved logical type and has no storage contract yet",
        "未排期",
    ))
}

pub fn mask_field(_name: impl Into<String>, _nullable: bool) -> Result<Field> {
    Err(VqlError::feature(
        "MASK is a reserved logical type and has no storage contract yet",
        "未排期",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorCode;

    #[test]
    fn future_types_fail_with_a_stable_code() {
        for error in [audio_field("audio", true), mask_field("mask", true)] {
            let error = error.unwrap_err();
            assert_eq!(error.code, ErrorCode::FeatureNotAvailable);
            assert_eq!(error.target_version.as_deref(), Some("未排期"));
        }
    }
}
