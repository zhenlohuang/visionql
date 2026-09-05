# VisionQL Python Binding Design

> This document defines the synchronous `visionql` package and its PyO3 boundary. SQL and execution semantics belong to the [Kernel Design](./kernel.md).

## Boundary

`vql-python` owns:

- the PyO3 extension module and public Python classes;
- construction of an embedded `Engine` and `Session`;
- PyArrow result conversion;
- the in-process Python UDF host;
- Python exceptions and notebook representation.

It does not reimplement SQL planning, result schemas, cancellation, resource accounting, or model execution. DataFusion types do not appear in the public Python API.

The package and import name are `visionql`; the native module is `visionql._visionql`. The package re-exports `connect`, `Session`, `QueryHandle`, `VisionQLError`, `__version__`, and the pure-Python `visionql.images` helper module.

## Public API

```python
session = visionql.connect(
    catalog=None,
    session_memory_limit_bytes=None,
)

handle = session.sql("SELECT 1")
handles = session.run_script("SELECT 1; SELECT 2;")

table = handle.collect()
text = handle.show(n=20)
handle.cancel()
```

`visionql.images.decode_batch(images)` is the one pure-Python helper: it takes an `IMAGE` `StructArray`, reads its `encoded` field, and returns Pillow images (or `None` per null row). It raises when Pillow is absent, which keeps Pillow an optional dependency of the package.

`connect()` loads the same strict `EngineConfig` as the CLI. The optional `catalog` and `session_memory_limit_bytes` arguments override only those settings. It installs a Python UDF host before building the Session.

`Session.sql()` accepts one statement and returns a `QueryHandle`. Query and `EXPLAIN` statements are planned for later collection, while DDL and `SET` take effect before `sql()` returns. `run_script()` executes and collects every statement synchronously in script order. On success it returns one handle with a cached result per statement; on the first failure it raises an error instead of returning a partial list. An unbounded statement keeps `run_script()` attached until it stops, so callers should use `sql()` when they need an independently cancellable continuous handle. A chainable DataFrame API is not part of this binding; when it is introduced it must lower to the same kernel logical-plan contracts.

## Results and Execution

For an uncollected query or `EXPLAIN` statement, `QueryHandle` is a lazy host handle. `collect()` starts or drains execution and converts Arrow `RecordBatch` values into one `pyarrow.Table`; later calls reuse the cached kernel batches. Handles for DDL, `SET`, or statements returned by `run_script()` already contain materialized results. An empty result retains the kernel statement schema.

`show(n)` collects, slices to at most `n` rows, and returns the PyArrow text representation; `n` defaults to 20. `_repr_html_()` shows the same 20-row view inside an escaped `<pre>` block. The binding never implicitly fetches original media or renders notebook thumbnails; explicit encoding is required before media crosses the process boundary.

`cancel()` forwards to the kernel cancellation token. `collect()` releases the Python GIL while blocking in the kernel. `run_script()` is a synchronous Python call, and an invoked Python UDF necessarily runs under the GIL. A kernel failure becomes `VisionQLError`, a `RuntimeError` subclass. Its `code`, `symbol`, `message`, and `target_version` attributes preserve the [structured error contract](./error_codes.md); `str(error)` uses `[VQL-CCDDD] SYMBOL: message`.

## Arrow Boundary

Results use the Arrow C data interface through `arrow-pyarrow`; they are not copied through Python row objects.

Standard Arrow storage remains readable when PyArrow does not interpret VisionQL extension metadata. Process-local `IMAGE.buffer_id` and `IMAGE.buffer_slot` never cross this boundary. An `IMAGE` needed in Python carries encoded bytes and metadata.

## Python UDF Host

`CREATE FUNCTION ... LANGUAGE PYTHON AS 'module:function'` stores a Python entry point in the Catalog. Resolution imports the module, verifies that the attribute is callable, and caches the callable in the Session's Python UDF host.

Invocation is vectorized:

1. Kernel Arrow arrays cross into PyArrow arrays.
2. The callable receives one array per SQL argument.
3. Its result returns through the Arrow data interface.
4. The kernel validates the result against the declared field contract.

The entry point uses the `module:function` form and must resolve to a callable attribute. Resolution and invocation failures become structured execution errors. A Python call already running under the GIL cannot be interrupted; surrounding query execution observes cancellation after the call returns. Isolated worker processes and cancellation of already-running Python code belong to the [`vqld` Service Design](./vqld.md).

## Verification

Python-owned API tests run after `maturin develop --locked`:

```bash
python -m pytest -q vql-python/tests
```

They protect PyArrow collection, schema preservation, Session configuration overrides, notebook representation, vectorized Python UDFs, and encoded-image flow through a Python UDF. Kernel SQL owner tests and external-service scenarios are defined in the [Testing Design](./testing.md).
