# VisionQL Roadmap

VisionQL brings one query model to images, recorded video, and live streams. Releases are organized by capability, without fixed dates. See the [PRD](./docs/prd.md), [v0.1 system design](./docs/design.md), and [later-version proposals](./docs/proposals/README.md) for details.

Status: ✅ Complete · 🚧 In progress · 📋 Planned

## v0.1 — Embedded batch and streaming MVP 🚧

**Goal:** Install VisionQL with `pip` and run visual queries locally, without a VisionQL service.

- [x] Query image and video directories with native `IMAGE`, `VIDEO`, and `BOX2D` types.
- [x] Run typed object detection with `CREATE MODEL`, `RESOLVE MODEL`, and `IMAGE_DETECTION` using local ONNX or remote Triton models.
- [x] Extend queries with SQL functions and in-process Python UDFs.
- [x] Analyze recorded video and live RTSP streams with the same SQL, including event time, watermarks, `TUMBLE` windows, and reconnects.
- [x] Send results to the console or Kafka with bounded backpressure.
- [x] Explore interactively in the SQL shell with multiline input, history, and `\q` exit; run scripts with `vql run`; or use Python through `sess.sql()` and Arrow.
- [x] Reduce media processing with column, frame-sampling, and time-predicate pushdown.

## v0.2 — Service and Workbench 📋

**Goal:** Keep queries running independently of clients and make VisionQL accessible from browsers and standard data tools.

- [ ] Run VisionQL as a single-node `vqld` service over Arrow Flight SQL.
- [ ] Secure shared access with TLS, authentication, and table- and stream-level authorization.
- [ ] Submit, inspect, pause, resume, stop, checkpoint, and recover durable queries.
- [ ] Access thumbnails and authorized original media through the Flight protocol.
- [ ] Isolate Python UDFs in worker processes and expose operational metrics through Prometheus.
- [ ] Build equivalent queries with a Python DataFrame API in embedded or service mode.
- [ ] Use Workbench to write SQL, inspect multimodal and live results, browse the Catalog, and operate queries.

## v0.3 — Cross-modal search and persistence 📋

**Goal:** Search images by text or image and reuse persisted inference results.

- [ ] Generate typed image and text embeddings with an isolated Transformers Runtime.
- [ ] Search `VECTOR(n)` values with exact `<->` Top-K queries.
- [ ] Store and restore multimodal results in Parquet and Lance, including native `IMAGE` and vector columns.
- [ ] Accelerate Top-K search with explicit HNSW indexes backed by Lance.

## Future directions

These capabilities are not yet scheduled:

- Lower-cost queries through model cascades, inference reuse, cost estimates, sampling, and ROI cropping.
- More streaming workloads through tracking, additional window types, VLM predicates, Kafka input, and cross-stream joins.
- Agent and workflow integrations through MCP and reusable scenario packages.
- Cluster deployment, exactly-once delivery, multi-tenancy, auditing, and WASM UDFs.
- Edge-to-cloud query partitioning and edge fleet management.
- Reproducible datasets, benchmarks, and performance regression gates.
