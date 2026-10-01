# Changelog

All notable changes to VisionQL are documented in this file.

## [Unreleased]

## [0.3.0] - 2026-10-01

VisionQL v0.3.0 adds Visual SQL Workbench for managing Catalog objects and persistent Jobs, authoring batch and streaming SQL, and inspecting multimodal results through the public Flight SQL service.

### Highlights

- Add an independent browser frontend and loopback Workbench backend for one `vqld` endpoint, with connection settings, optional service credentials and TLS trust, and credentials retained in backend memory.
- Author SQL in multiple named local drafts with syntax highlighting, formatting, selected/current-statement execution, `EXPLAIN`, and persistent local execution history.
- Run bounded SQL or preview attached streams in a rolling 500-row buffer, with exact cancellation and one active execution per browser Session.
- Browse namespace-aware Catalog objects, inspect formatted `SHOW CREATE` definitions, manage Tables, Models, and Functions, and select exact Model versions.
- Submit, search, inspect, load as a draft, and stop persistent Jobs through public SQL while keeping registered definitions immutable.
- Inspect typed Arrow tables and JSON, render bounded `IMAGE` thumbnails with configurable `BOX2D`, label, and confidence overlays, and export client-side thumbnail crops.
- Add browser acceptance against built Workbench and shipped `vqld`, including real-model detection and IMAGE/BOX2D parity with the Python/notebook path.
- Reorganize the user guides and generate the SQL function reference from implementation metadata.

### Changed

- Make `SHOW CREATE MODEL <name>` return the latest live version's complete declaration, and support `VERSION '<version>'` for an exact version. Model results append the actual `version`; Table and Function results retain their existing schema. Workbench reads these definitions directly for object and version selections.
- Preserve escaped strings and quoted identifiers when splitting SQL scripts, so returned Model DDL and quoted version names can be replayed.
- Rename persistent Catalog Queries to Jobs throughout SQL, public Arrow results (`job_id`), Catalog APIs and storage, Workbench, examples, and current documentation. The lifecycle commands are `SUBMIT JOB`, `SHOW JOBS`, `DESCRIBE JOB`, and `STOP JOB`; the previous `QUERY` commands are no longer accepted.
- Existing v0.2 persistent Query records are not automatically migrated into Job storage. Before upgrading, preserve their original SQL, stop active Queries with v0.2, shut down `vqld`, and back up `VQL_HOME`. Resubmit them with `SUBMIT JOB` after upgrading; they receive new Job identities. See the [upgrade instructions](docs/user_guide/installation.md#upgrade-from-v02).

### Fixed

- Preserve the declared polygon list field in `POLYGON` and `ST_POLYGON` results, allowing spatial SQL to execute without an Arrow result-type mismatch.

### Scope boundaries

- Workbench connects to one single-node, one-principal `vqld` service. Drafts and execution history remain local to the browser; shared workspaces and synchronized history are not included.
- Results use bounded Flight thumbnails. Crop export operates on the returned thumbnail; original-media retrieval is not included.
- Persistent Jobs retain the live-source recovery contract from v0.2: fresh in-memory window state and an explicit restart gap, without checkpoints, `PAUSE` / `RESUME`, replay, or exactly-once delivery.
- The Python API, CLI, daemon, and Workbench are distributed as source. This release does not publish a PyPI package or platform-specific binaries.

## [0.2.0] - 2026-09-06

VisionQL v0.2.0 adds the smallest useful single-node service for remote SQL and client-independent continuous queries while preserving the embedded v0.1 engine.

### Highlights

- Add the single-node `vqld` host with Arrow Flight SQL Sessions, direct and prepared statement execution, opaque execution tickets, exact cancellation, health endpoints, and aggregate metrics.
- Add Catalog-backed persistent Queries through `SUBMIT QUERY`, `SHOW QUERIES`, `DESCRIBE QUERY`, and `STOP QUERY`, including atomic definition-generation pinning and compare-and-swap status transitions.
- Recover active live-source Queries after daemon restart with the same Query identity, an explicit restart gap, and honest in-memory window-state reset reporting.
- Convert process-local `IMAGE` values to bounded encoded thumbnails at the Flight boundary and reject unsupported service-hosted Python Functions explicitly.
- Add owner tests and a real Flight SQL, RTSP, and Kafka system journey for attached execution, cancellation, client-independent delivery, and recovery.
- Let `vql shell` select either its existing embedded Session or a `vqld` Flight SQL endpoint, including flag- or environment-supplied service credentials, TLS trust configuration, exact remote cancellation, and structured VQL error preservation.

### Scope boundaries

- `vqld` is a single-node, one-principal service. Non-loopback Flight access requires TLS and one configured service token; the HTTP health and metrics surface remains loopback-only.
- Restart recovery resumes RTSP Queries from the current live position with fresh in-memory window state and an explicit gap. v0.2.0 does not provide serialized checkpoints, `PAUSE` / `RESUME`, replay, or exactly-once delivery.
- Service-side Python Functions, multi-user authorization, original-media Flight tickets, the DataFrame API, and Workbench are not part of v0.2.0.

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
- A minimal single-node `vqld` Flight SQL host with Catalog-backed, client-independent persistent Queries is planned for v0.2. Checkpoints, multi-user authorization, and the DataFrame API are not part of that release.
- A focused Workbench client for bounded visual SQL results is planned for v0.3.
- RTSP execution is attached, best-effort, and non-replayable; v0.1.0 does not promise exactly-once delivery or restart recovery.

[Unreleased]: https://github.com/zhenlohuang/visionql/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/zhenlohuang/visionql/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/zhenlohuang/visionql/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/zhenlohuang/visionql/releases/tag/v0.1.0
