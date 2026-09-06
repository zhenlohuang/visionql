# VisionQL Roadmap

VisionQL brings one query model to images, recorded video, and live streams. Releases are organized by capability, without fixed dates. See the [PRD](./docs/prd.md), [high-level design](./docs/high_level_design.md), and [later-version proposals](./docs/proposals/README.md) for details.

Status: ✅ Complete · 🚧 In progress · 📋 Planned

## v0.1 — Embedded batch and streaming MVP ✅

**Goal:** Install VisionQL with `pip` and run visual queries locally, without a VisionQL service.

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
- [x] Submit, inspect, and stop persistent continuous Table writes explicitly.
- [x] Persist submitted Queries in the Catalog and keep them running after client disconnect.
- [x] Rediscover Catalog-backed Queries from another Session and restart active RTSP Queries from the live position with fresh window state and an explicit restart gap.
- [x] Bind both surfaces to loopback by default; require TLS and one configured service credential for non-loopback Flight access while keeping HTTP loopback-only.
- [x] Return one bounded `IMAGE` thumbnail representation and expose per-query state through SQL.
- [x] Validate the complete service path with one selected Flight SQL or ADBC client integration.

## v0.3 — Workbench 📋

**Goal:** Make bounded visual SQL results understandable in a focused browser client defined by the [Workbench Design](./docs/design/workbench.md).

- [ ] Connect to one `vqld` endpoint through its public Arrow Flight SQL profile.
- [ ] Run and cancel one bounded SQL statement at a time.
- [ ] Render ordinary columns, bounded `IMAGE` thumbnails, and `BOX2D` overlays.
- [ ] Display structured VQL errors without parsing message text.
- [ ] Validate the same anchor query against the Python/notebook path.

## Future directions

These capabilities are not yet scheduled:

- Lower-cost queries through model cascades, inference reuse, cost estimates, sampling, and ROI cropping.
- More streaming workloads through tracking, additional window types, VLM predicates, Kafka input, and cross-stream joins.
- Agent and workflow integrations through MCP and reusable scenario packages.
- General semantic image classification, text/document execution for the AI built-ins, and user-defined image-classification capability Models.
- Persistent window checkpoints, `PAUSE`/`RESUME`, and recovery of open window state.
- Multi-user identity, relation-level authorization, audit, and a network Unity Catalog API.
- Original-media Flight tickets, broader BI/JDBC compatibility, and multiple image transport modes.
- Service-side Python UDF workers and a chainable DataFrame API.
- Cluster deployment, exactly-once delivery, multi-tenancy, and WASM UDFs.
- Edge-to-cloud query partitioning and edge fleet management.
- Reproducible datasets, benchmarks, and performance regression gates.
