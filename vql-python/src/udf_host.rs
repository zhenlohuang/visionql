use std::collections::HashMap;
use std::sync::Mutex;

use arrow::array::{ArrayRef, make_array};
use arrow::datatypes::FieldRef;
use arrow_pyarrow::{FromPyArrow, ToPyArrow};
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use tokio_util::sync::CancellationToken;
use vql_kernel::{ErrorCode, PyUdfHandle, PythonUdfHost, Result, VqlError};

#[derive(Default)]
pub(crate) struct PyArrowUdfHost {
    functions: Mutex<HashMap<String, Py<PyAny>>>,
}

impl std::fmt::Debug for PyArrowUdfHost {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PyArrowUdfHost")
            .finish_non_exhaustive()
    }
}

impl PythonUdfHost for PyArrowUdfHost {
    fn resolve(&self, entry: &str) -> Result<PyUdfHandle> {
        let (module, function) = split_entry(entry)?;
        let callable = Python::attach(|py| -> PyResult<Py<PyAny>> {
            let callable = py.import(module)?.getattr(function)?;
            if !callable.is_callable() {
                return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                    "{entry} is not callable"
                )));
            }
            Ok(callable.unbind())
        })
        .map_err(|error| {
            VqlError::new(
                ErrorCode::Execution,
                format!("cannot resolve Python UDF '{entry}': {error}"),
            )
        })?;
        self.functions
            .lock()
            .map_err(|_| VqlError::new(ErrorCode::Internal, "Python UDF cache was poisoned"))?
            .insert(entry.to_owned(), callable);
        Ok(PyUdfHandle::new(entry))
    }

    fn invoke(
        &self,
        handle: &PyUdfHandle,
        args: &[ArrayRef],
        _expected: &FieldRef,
        cancel: &CancellationToken,
    ) -> Result<ArrayRef> {
        if cancel.is_cancelled() {
            return Err(VqlError::new(ErrorCode::QueryCancelled, "query cancelled"));
        }
        Python::attach(|py| -> Result<ArrayRef> {
            let functions = self
                .functions
                .lock()
                .map_err(|_| VqlError::new(ErrorCode::Internal, "Python UDF cache was poisoned"))?;
            let callable = functions.get(handle.entry()).ok_or_else(|| {
                VqlError::new(ErrorCode::Internal, "Python UDF handle is not resolved")
            })?;
            let mut python_args = Vec::with_capacity(args.len());
            for arg in args {
                python_args.push(arg.to_data().to_pyarrow(py).map_err(python_error)?);
            }
            let tuple = PyTuple::new(py, python_args).map_err(python_error)?;
            let result = callable.bind(py).call1(tuple).map_err(python_error)?;
            let data =
                arrow::array::ArrayData::from_pyarrow_bound(&result).map_err(python_error)?;
            Ok(make_array(data))
        })
    }
}

fn split_entry(entry: &str) -> Result<(&str, &str)> {
    entry.split_once(':').ok_or_else(|| {
        VqlError::new(
            ErrorCode::InvalidOption,
            "Python UDF entry must be 'module:function'",
        )
    })
}

fn python_error(error: PyErr) -> VqlError {
    VqlError::new(ErrorCode::Execution, format!("Python UDF raised: {error}"))
}
