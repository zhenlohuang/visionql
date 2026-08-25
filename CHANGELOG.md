# Changelog

All notable changes to VisionQL are documented in this file.

## [0.1.0] - 2026-08-25

VisionQL v0.1.0 is the first public release of the embedded batch and streaming engine for visual data.

### Highlights

- Query image directories, recorded video, and live RTSP streams through one SQL engine with native `IMAGE`, `VIDEO`, `BOX2D`, and `LOCATOR` types.
- Register callable, versioned Models with direct SQL invocation, resolve-time introspection, snapshot-pinned planning, and embedded ONNX Runtime or remote Triton execution.
- Use the task-shaped `VQL_CLASSIFY`, `VQL_EXTRACT`, and `VQL_DETECT` contracts. Release-managed YOLO26n artifacts execute IMAGE classification and detection.
- Compose event-time RTSP queries with watermarks and `TUMBLE` windows, return attached foreground results, or write acknowledged results to Kafka with bounded backpressure.
- Extend SQL with Catalog Functions and in-process Python UDFs, and collect results as PyArrow tables through the Python API.
- Persist Tables, Models, and Functions in the embedded SQLite Catalog and expose the Unity Catalog-compatible HTTP boundary.
- Configure the embedded runtime through strict schema-v1 `$VQL_HOME/config.toml` settings and use the focused `vql shell` and `vql run` CLI surface.
- Receive stable public errors in the `[VQL-CCDDD] SYMBOL: message` format across Rust, CLI, and Python host boundaries.

### Scope boundaries

- `VQL_EXTRACT` execution and the STRING overload of `VQL_CLASSIFY` are typed but return `FEATURE_NOT_AVAILABLE` in v0.1.0.
- `vqld`, Flight SQL, authentication, durable query recovery, the DataFrame API, and Workbench are planned for v0.2.
- Embeddings, vector search, and Parquet/Lance persistence are planned for v0.3.
- RTSP execution is attached, best-effort, and non-replayable; v0.1.0 does not promise exactly-once delivery or restart recovery.

[0.1.0]: https://github.com/zhenlohuang/visionql/releases/tag/v0.1.0
