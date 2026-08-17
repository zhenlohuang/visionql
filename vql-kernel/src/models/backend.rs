use arrow::array::ArrayRef;
use async_trait::async_trait;
use image::DynamicImage;
use tokio_util::sync::CancellationToken;

use super::postprocess::mock_detection_output;
use crate::Result;
use crate::resources::QueryBudget;

#[async_trait]
pub(crate) trait ModelBackend: Send + Sync + std::fmt::Debug {
    async fn infer(
        &self,
        images: Vec<DynamicImage>,
        cancel: CancellationToken,
        budget: &QueryBudget,
    ) -> Result<ArrayRef>;
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

#[async_trait]
impl ModelBackend for MockBackend {
    async fn infer(
        &self,
        images: Vec<DynamicImage>,
        _cancel: CancellationToken,
        _budget: &QueryBudget,
    ) -> Result<ArrayRef> {
        Ok(mock_detection_output(&self.label, images.len()))
    }
}
