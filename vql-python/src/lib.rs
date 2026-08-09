mod udf_host;

use std::path::PathBuf;
use std::sync::Arc;

use arrow_pyarrow::{IntoPyArrow, Table};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use vql_kernel::{Engine, EngineConfig, Session, Statement};

use udf_host::PyArrowUdfHost;

fn py_error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

#[pyclass(name = "Session")]
struct PySession {
    session: Session,
}

#[pymethods]
impl PySession {
    fn sql(&self, sql: &str) -> PyResult<PyQueryHandle> {
        Ok(PyQueryHandle {
            statement: self.session.sql(sql).map_err(py_error)?,
        })
    }

    fn run_script(&self, script: &str) -> PyResult<Vec<PyQueryHandle>> {
        self.session
            .run_script(script)
            .map(|statements| {
                statements
                    .into_iter()
                    .map(|statement| PyQueryHandle { statement })
                    .collect()
            })
            .map_err(py_error)
    }
}

#[pyclass(name = "QueryHandle")]
struct PyQueryHandle {
    statement: Statement,
}

#[pymethods]
impl PyQueryHandle {
    fn collect(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let batches = py.detach(|| self.statement.collect()).map_err(py_error)?;
        let schema = batches
            .first()
            .map(|batch| batch.schema())
            .unwrap_or_else(|| match &self.statement {
                Statement::Query(query) | Statement::Explain(query) | Statement::Set(query) => {
                    query.schema()
                }
                Statement::Ddl(result) => result
                    .batches()
                    .first()
                    .map(|batch| batch.schema())
                    .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty())),
            });
        Table::try_new(batches, schema)
            .map_err(py_error)?
            .into_pyarrow(py)
            .map(|table| table.unbind())
    }

    #[pyo3(signature = (n=20))]
    fn show(&self, py: Python<'_>, n: usize) -> PyResult<String> {
        let table = self.collect(py)?;
        let bound = table.bind(py);
        let sliced = bound.call_method1("slice", (0, n))?;
        Ok(sliced.str()?.to_string())
    }

    fn cancel(&self) {
        self.statement.cancel();
    }

    fn _repr_html_(&self, py: Python<'_>) -> PyResult<String> {
        let text = self.show(py, 20)?;
        let escaped = text
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;");
        Ok(format!("<pre class=\"visionql-result\">{escaped}</pre>"))
    }
}

#[pyfunction]
#[pyo3(signature = (catalog=None))]
fn connect(catalog: Option<PathBuf>) -> PyResult<PySession> {
    let config = catalog
        .map(|path| EngineConfig::default().with_catalog_path(path))
        .unwrap_or_default();
    let engine = Engine::new(config).map_err(py_error)?;
    let host = Arc::new(PyArrowUdfHost::default());
    let session = engine
        .session()
        .with_python_udf_host(host)
        .build()
        .map_err(py_error)?;
    Ok(PySession { session })
}

#[pymodule]
fn _visionql(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(connect, module)?)?;
    module.add_class::<PySession>()?;
    module.add_class::<PyQueryHandle>()?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
