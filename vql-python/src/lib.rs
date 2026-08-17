mod udf_host;

use std::path::PathBuf;
use std::sync::Arc;

use arrow_pyarrow::{IntoPyArrow, Table};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use vql_kernel::{Engine, EngineConfig, FrameDropReason, QueryResource, Session, Statement};

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

    fn metrics(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let Some(metrics) = self.statement.metrics() else {
            return Ok(None);
        };
        let output = PyDict::new(py);
        output.set_item("input_rows", metrics.input_rows())?;
        output.set_item("output_rows", metrics.output_rows())?;
        output.set_item("decode_frames", metrics.decode_frames())?;
        output.set_item("inference_rows", metrics.inference_rows())?;
        output.set_item("inference_batches", metrics.inference_batches())?;
        output.set_item("error_rows", metrics.error_rows())?;
        output.set_item("inference_p50_ms", metrics.inference_p50_ms())?;
        output.set_item("inference_p95_ms", metrics.inference_p95_ms())?;
        output.set_item("batch_histogram", metrics.batch_histogram())?;
        output.set_item("model_queue_p50_ms", metrics.model_queue_p50_ms())?;
        output.set_item("model_queue_p95_ms", metrics.model_queue_p95_ms())?;
        output.set_item("model_service_p50_ms", metrics.model_service_p50_ms())?;
        output.set_item("model_service_p95_ms", metrics.model_service_p95_ms())?;
        output.set_item("source_generation", metrics.source_generation())?;
        output.set_item("source_reconnects", metrics.source_reconnects())?;
        output.set_item("event_time_fallbacks", metrics.event_time_fallbacks())?;
        output.set_item("source_dropped_frames", metrics.source_dropped_frames())?;
        output.set_item("source_gap_duration_ms", metrics.source_gap_duration_ms())?;
        output.set_item("sampled_frames", metrics.sampled_frames())?;
        output.set_item("sampled_fps", metrics.sampled_fps())?;
        output.set_item("source_input_bytes", metrics.source_input_bytes())?;
        output.set_item("input_bitrate_bps", metrics.input_bitrate_bps())?;
        output.set_item("watermark_ms", metrics.watermark_ms())?;
        let dropped_ranges = metrics
            .dropped_frame_ranges()
            .into_iter()
            .map(|range| {
                let item = PyDict::new(py);
                let reason = match range.reason {
                    FrameDropReason::SourceOverrun => "source_overrun",
                    FrameDropReason::ResourceBudget => "resource_budget",
                    FrameDropReason::DecodeError => "decode_error",
                };
                item.set_item("reason", reason)?;
                item.set_item("count", range.count)?;
                item.set_item("first_event_time_ms", range.first_event_time_ms)?;
                item.set_item("last_event_time_ms", range.last_event_time_ms)?;
                Ok(item)
            })
            .collect::<PyResult<Vec<_>>>()?;
        output.set_item("dropped_frame_ranges", PyList::new(py, dropped_ranges)?)?;
        output.set_item("late_rows", metrics.late_rows())?;
        output.set_item("window_state_bytes", metrics.window_state_bytes())?;
        output.set_item("sink_retries", metrics.sink_retries())?;
        output.set_item("epoch_p50_ms", metrics.epoch_p50_ms())?;
        output.set_item("epoch_p95_ms", metrics.epoch_p95_ms())?;
        output.set_item("end_to_end_p50_ms", metrics.end_to_end_p50_ms())?;
        output.set_item("end_to_end_p95_ms", metrics.end_to_end_p95_ms())?;
        let resources = PyDict::new(py);
        for (name, resource) in [
            ("arrow", QueryResource::Arrow),
            ("media", QueryResource::Media),
            ("frame_buffer", QueryResource::FrameBuffer),
            ("model_tensor", QueryResource::ModelTensor),
            ("model_queue", QueryResource::ModelQueue),
            ("triton_payload", QueryResource::TritonPayload),
            ("window_state", QueryResource::WindowState),
            ("sink_buffer", QueryResource::SinkBuffer),
            ("device_memory", QueryResource::DeviceMemory),
        ] {
            let usage = metrics.resource_usage(resource);
            let item = PyDict::new(py);
            item.set_item("available", resource != QueryResource::DeviceMemory)?;
            item.set_item("current_bytes", usage.current_bytes)?;
            item.set_item("peak_bytes", usage.peak_bytes)?;
            resources.set_item(name, item)?;
        }
        output.set_item("resources", resources)?;
        Ok(Some(output.into_any().unbind()))
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
#[pyo3(signature = (catalog=None, query_memory_limit_bytes=None))]
fn connect(
    catalog: Option<PathBuf>,
    query_memory_limit_bytes: Option<usize>,
) -> PyResult<PySession> {
    let mut config = catalog
        .map(|path| EngineConfig::default().with_catalog_path(path))
        .unwrap_or_default();
    if let Some(limit) = query_memory_limit_bytes {
        config = config.with_query_memory_limit_bytes(limit);
    }
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
