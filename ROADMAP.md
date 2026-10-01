# VisionQL Roadmap

VisionQL brings one query model to images, recorded video, and live streams. Releases are organized by capability, without fixed dates. See the [PRD](./docs/prd.md), [high-level design](./docs/high_level_design.md), and [later-version proposals](./docs/proposals/README.md) for details.

Status: ✅ Complete · 🚧 In progress · 📋 Planned

Statuses and checkboxes track implementation and acceptance in the current source. The latest published release is v0.2.0; v0.3 is in development. See the [Changelog](./CHANGELOG.md) for released and unreleased changes and the [README](./README.md) for current setup instructions.

## v0.1 — Embedded batch and streaming MVP ✅

**Goal:** Run visual queries locally through the embedded engine, SQL shell, and Python API, without a VisionQL service.

- [x] Query image and video directories with native `IMAGE`, `VIDEO`, and `BOX2D` types.
- [x] Run direct, version-bound Model calls with minimal `CREATE MODEL`, resolve-time introspection, local ONNX or remote Triton execution, and callable-namespace lifecycle statements.
- [x] Provide generic `VECTOR`/`TENSOR` signatures and inferred constant-only Function parameters.
- [x] Provide task-shaped `VQL_CLASSIFY`, `VQL_EXTRACT`, and `VQL_DETECT` contracts with shared `LOCATOR` provenance; release-managed YOLO26n artifacts execute the IMAGE classification and detection overloads, while unbacked IMAGE/STRING overloads return `FEATURE_NOT_AVAILABLE`.
- [x] Extend queries with SQL functions and in-process Python UDFs.
- [x] Analyze recorded video and live RTSP streams with the same SQL, including event time, watermarks, `TUMBLE` windows, and reconnects.
- [x] Return foreground query results directly or write them to Kafka tables with bounded backpressure.
- [x] Explore interactively in the SQL shell with multiline input, history, and `\q` exit; run scripts with `vql run`; or use Python through `sess.sql()` and Arrow.
- [x] Reduce media processing with column, frame-sampling, and time-predicate pushdown.

## v0.2 — Single-node service ✅

**Goal:** Keep an explicitly submitted continuous query running independently of its client through the smallest useful single-node service.

- [x] Run bounded and attached continuous SQL through a tested Arrow Flight SQL subset in `vqld`.
- [x] Connect the existing `vql shell` front end to either its embedded Session or one `vqld` Flight SQL endpoint.
- [x] Submit, inspect, and stop persistent continuous Table writes explicitly.
- [x] Persist submitted Queries in the Catalog and keep them running after client disconnect.
- [x] Rediscover Catalog-backed Queries from another Session and restart active RTSP Queries from the live position with fresh window state and an explicit restart gap.
- [x] Bind both surfaces to loopback by default; require TLS and one configured service credential for non-loopback Flight access while keeping HTTP loopback-only.
- [x] Return one bounded `IMAGE` thumbnail representation and expose per-query state through SQL.
- [x] Validate the complete service path with one selected Flight SQL or ADBC client integration.

## v0.3 — Visual SQL Workbench 🚧

**Goal:** Provide one browser workspace for managing VisionQL objects and persistent Queries, authoring batch or streaming SQL, and understanding multimodal results. The [Workbench implementation](./vql-workbench/README.md) uses an independent frontend and loopback backend, with its public-protocol boundary defined by the [Workbench Design](./docs/design/workbench.md).

**Implemented:**

- [x] Connect to one `vqld` endpoint through public Arrow Flight SQL, with visible endpoint and Session state, connection settings, optional service credential and TLS CA, and documentation access. Keep connection credentials in backend memory.
- [x] Author SQL in multiple local draft tabs with syntax highlighting, formatting, full-buffer or selected/current-statement execution, public `EXPLAIN`, and one active server execution per browser Session. Rename drafts and restore their names and SQL after browser reloads.
- [x] Run bounded statements to completion, preview one attached continuous statement in a rolling buffer of the latest 500 rows, and cancel exactly the active execution, including when its browser result stream disappears.
- [x] Browse a Catalog → Schema → Tables / Models / Functions tree across available namespaces and open formatted, read-only `SHOW CREATE` DDL in the main workspace. Preserve the original returned SQL for copying.
- [x] Manage Tables through public `CREATE TABLE`, `SHOW TABLES`, `DESCRIBE TABLE`, `SHOW CREATE TABLE`, and `DROP TABLE` actions.
- [x] Manage Models through public `CREATE MODEL`, `SHOW`/`DESCRIBE`, `RESOLVE MODEL`, `ALTER MODEL`, and `DROP MODEL` actions; manage Functions through public `CREATE FUNCTION`, `SHOW`/`DESCRIBE`, `SHOW CREATE FUNCTION`, and `DROP FUNCTION` actions.
- [x] Inspect the latest live Model version with `SHOW CREATE MODEL <name>` or an exact version with `VERSION '<version>'`. Display the actual version, mark the default, and switch version DDL from both Model and callable entries.
- [x] Submit, list, search, refresh, inspect, load as a new draft, and stop persistent Queries in Workspace → Jobs through `SUBMIT QUERY`, `SHOW JOBS`, `DESCRIBE QUERY`, and `STOP QUERY`. Keep attached and persistent execution visibly distinct and registered Query definitions immutable.
- [x] Render Arrow results as a typed table or JSON presentation with elapsed time, row or stream status, local pagination, and no SQL rewriting.
- [x] Render returned `IMAGE` thumbnails with configurable `BOX2D`, label, and confidence overlays; inspect nested row values and export a client-side thumbnail crop without fetching original media.
- [x] Keep up to 500 local execution-history records with load, rerun, copy, edit, and clear actions. Keep results, media, connection credentials, and persistent Query state outside local history.
- [x] Display structured VQL errors without parsing message text and distinguish them from connectivity or Workbench-local failures.
- [x] Validate browser journeys against the built Workbench and shipped `vqld` using public protocols: namespace Catalog navigation, Table creation, Model version selection, Jobs submission/inspection/Stop, keyboard execution, thumbnails, exact cancellation, Session expiry, Host/Origin checks, and narrow-screen layouts.

**Remaining acceptance:**

- [ ] Validate the same visual query against Workbench and the Python/notebook path, including returned detection values and IMAGE/BOX2D interpretation.
- [ ] Finish reconciling Workbench component-design and acceptance documentation with the implemented editor, Catalog, and Jobs workflows, including removal of obsolete prototype references.

## Future directions

These capabilities are not yet scheduled:

- Lower-cost queries through model cascades, inference reuse, cost estimates, sampling, and ROI cropping.
- Semantic visual retrieval through embeddings, vector indexes, and a defined vector-similarity SQL contract.
- More streaming workloads through tracking, additional window types, VLM predicates, Kafka input, and cross-stream joins.
- Agent and workflow integrations through MCP and reusable scenario packages.
- General semantic image classification, text/document execution for the AI built-ins, and user-defined image-classification capability Models.
- Persistent window checkpoints, `PAUSE`/`RESUME`, and recovery of open window state.
- Multi-user identity, relation-level authorization, audit, and a network Unity Catalog API.
- Shared Workbench spaces, server-synchronized history, dashboards, and collaborative query editing.
- Original-media Flight tickets, server-side crop export, broader BI/JDBC compatibility, and multiple image transport modes.
- Service-side Python UDF workers and a chainable DataFrame API.
- Cluster deployment, exactly-once delivery, multi-tenancy, and WASM UDFs.
- Edge-to-cloud query partitioning and edge fleet management.
- Reproducible datasets, benchmarks, and performance regression gates.
