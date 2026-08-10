use image::DynamicImage;

use super::Detection;
use crate::Result;

pub(crate) trait ModelBackend: Send + Sync + std::fmt::Debug {
    fn infer(&self, images: Vec<DynamicImage>) -> Result<Vec<Vec<Detection>>>;
}

#[derive(Debug)]
pub(crate) struct MockBackend {
    label: String,
}

impl MockBackend {
    pub(crate) fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
        }
    }
}

impl ModelBackend for MockBackend {
    fn infer(&self, images: Vec<DynamicImage>) -> Result<Vec<Vec<Detection>>> {
        Ok(images
            .into_iter()
            .map(|image| {
                let confidence = if image.width() > 0 && image.height() > 0 {
                    0.9
                } else {
                    0.0
                };
                vec![Detection {
                    label: self.label.clone(),
                    confidence,
                    x: 0.25,
                    y: 0.25,
                    w: 0.5,
                    h: 0.5,
                }]
            })
            .collect())
    }
}
