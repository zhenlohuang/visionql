use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::FieldRef;
use tokio_util::sync::CancellationToken;

use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PyUdfHandle {
    entry: String,
}

impl PyUdfHandle {
    pub fn new(entry: impl Into<String>) -> Self {
        Self {
            entry: entry.into(),
        }
    }

    pub fn entry(&self) -> &str {
        &self.entry
    }
}

pub trait PythonUdfHost: Debug + Send + Sync + 'static {
    fn resolve(&self, entry: &str) -> Result<PyUdfHandle>;

    fn invoke(
        &self,
        handle: &PyUdfHandle,
        args: &[ArrayRef],
        expected: &FieldRef,
        cancel: &CancellationToken,
    ) -> Result<ArrayRef>;
}

pub type PythonUdfHostRef = Arc<dyn PythonUdfHost>;
