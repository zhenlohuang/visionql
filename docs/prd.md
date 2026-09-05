# VisionQL Product Requirements Document

> VisionQL is a unified batch and streaming engine for multimodal data. It lets users query and process images, recorded video, and live video streams through SQL.

---

## 1. Product in One Sentence

**VisionQL is a data engine for Physical AI.**

Traditional databases answer questions about structured business records. VisionQL makes the visual world captured by cameras and video files queryable. Engineers can analyze images, recordings, and live streams with SQL, without assembling one-off Python pipelines or operating GPU inference jobs themselves.

- Images and recorded video are bounded datasets and run as batch workloads, much like Spark batch jobs.
- RTSP video is an unbounded dataset and runs as a streaming workload, much like Flink or Spark Structured Streaming.
- Both modes use the same SQL semantics.

---

## 2. Why This Product Should Exist

### 2.1 Market and Technology Context

1. **Visual data is growing faster than the systems built to process it.** An estimated 80–90% of newly created enterprise data is unstructured, including images, surveillance footage, dashcam recordings, and live feeds. Physical AI systems such as autonomous vehicles and robots add a constant stream of first-person video. Their data loops—finding corner cases and assembling training or evaluation sets—need infrastructure designed for visual data. Most existing data platforms can store a file path, but cannot understand or query what appears in the file.

2. **Vision models are now useful as query operators.** Detection, tracking, OCR, and vision-language models can answer questions such as “How many people are in this frame?” or “Which clips show an intrusion into the restricted area?” What is missing is a coherent data system that connects models, media, and queries.

3. **A declarative interface can remove substantial engineering work.** Data processing moved from hand-written MapReduce jobs to Hive and Spark SQL because declarative languages and optimizers made common workloads easier to build and improve. Visual processing is still dominated by custom OpenCV and PyTorch scripts. It is ready for the same kind of abstraction.

### 2.2 Where Existing Approaches Fall Short

| Approach | Limitation |
|---|---|
| **Spark / Flink** | Designed primarily for structured data. Vision logic usually lives inside opaque UDFs, which prevents sampling and time-range pushdown and makes inference reuse difficult. Users still manage video decoding, GPU scheduling, and model batching. |
| **Custom Python pipelines** (OpenCV + PyTorch + Celery/Airflow) | Tend to become disposable scripts with no shared optimizer, incremental execution model, or clear fault semantics. Batch and streaming paths often diverge, and analysts cannot participate directly. |
| **Research systems** (EvaDB, BlazeIt, VIVA, and others) | Demonstrate that SQL over video is viable and that model cascades and frame sampling can reduce cost. Most remain single-node, batch-oriented prototypes without a complete production streaming story. |
| **Multimodal data frameworks** (Daft, Ray Data, LanceDB) | Strong at storage or parallel processing, but positioned as general-purpose layers. Vision-native operators, SQL, and streaming are usually incomplete as a combined experience. |
| **Cloud vision APIs** (Rekognition, Alibaba Cloud Vision AI, and others) | Easy to call, but users have limited control over models and execution. Complex queries are difficult to compose, private models may not be supported, cost scales with processed frames, and data may have to leave the customer environment. |

**Product opportunity:** few systems combine vision-native operators, declarative SQL, unified batch and streaming semantics, and explainable optimization. Better models, more accessible GPUs, and rapidly growing video archives create a clear opening for VisionQL.

### 2.3 Target Users and Jobs to Be Done

**Primary user:** a data or ML engineer who currently builds visual-processing pipelines with Python, media libraries, model runtimes, and separate batch or streaming infrastructure. v0.1 and v0.2 optimize for this user. Analysts, platform teams, and agent developers are potential later users; they do not add requirements to the first service release.

**Anchor workflow:** register entrance-camera recordings and an RTSP feed, run the same people-detection and per-minute aggregation logic over both, and write the live result to Kafka. This workflow is useful in security, retail, and moderation contexts without requiring VisionQL to ship a vertical application.

Other promising uses include robotics data selection, media tagging, industrial inspection, and visual retrieval. They remain market hypotheses until a design partner runs them end to end; their union is not the product scope.

### 2.4 Customer Value

1. **Faster development.** A task such as “calculate the people count every minute and publish it to Kafka” should shrink from hundreds of lines of Python and deployment configuration to a small SQL script. Analysts can participate without learning the model stack.

2. **Lower GPU cost through query optimization.** Inference is usually the most expensive part of a visual query. A declarative plan gives VisionQL room to reduce that work:

   - Push sampling into the media source so a minute-level aggregate does not infer on every frame at 30 fps.
   - Push time predicates into decoding so only the requested interval or keyframe neighborhood is read.

   Opaque UDFs make these optimizations difficult. Representing model calls explicitly in the query plan is VisionQL's primary technical advantage over hand-built pipelines.

3. **One logical workflow for batch and streaming.** A query can be validated against recorded video before it is pointed at a live feed, reducing duplicated implementations and semantic drift.

4. **Reusable visual data assets.** Models, sources, and query outputs are catalog objects with traceable provenance and controllable access.

5. **Data stays near its source by default.** From v0.1 onward, the engine can run beside the data instead of requiring video uploads. This is important for privacy and regulatory obligations such as PIPL and GDPR.

6. **A natural visual tool for agents.** An agent can translate natural language into SQL and return constrained, auditable results from images, recordings, or live feeds. The existing text-to-SQL ecosystem lowers integration cost.

### 2.5 Risks

| Risk | Why it matters | Mitigation |
|---|---|---|
| **Inference remains expensive** | Even a 10× improvement can leave full analysis of a large archive costly | Let users control inference volume explicitly through sample rates; push sampling into the source; pursue additional cost optimizations only after measuring real workloads |
| **Results are probabilistic** | A detector can miss or falsely report an object, so `COUNT(*)` no longer represents an indisputable fact | Keep confidence and thresholds visible in the query; decide whether confidence-aware aggregation primitives are needed after user research |
| **SQL cannot express every vision workflow** | Calibration and complex multi-object association do not fit naturally into SQL | Do not force all logic into SQL; use UDFs, custom models, or ordinary host-language composition for complex processing |
| **Connector and model coverage takes time** | Product usefulness depends on supported sources, models, and scenario templates | Start with object detection plus RTSP, object storage, and Kafka provider tables; make one security or moderation workflow complete before expanding |
| **Large platforms may add similar features** | Databricks and cloud vendors can extend their multimodal offerings | Differentiate through unified batch and streaming behavior, vision-native optimization, and an open-source ecosystem |

---

## 3. Product Experience

### 3.1 Core Abstractions

VisionQL uses one unifying model: **visual data is represented as relations made up of frames.**

| Abstraction | Meaning |
|---|---|
| **Multimodal type system** | Extends standard SQL with `IMAGE`, `VIDEO`, `BOX2D`, `VECTOR(n)`, `TENSOR(dtype, dims...)`, and nested `STRUCT` / `ARRAY` types. `VECTOR` and `TENSOR` are available at generic Model boundaries in v0.1. |
| **Table** | A relation plus provider capabilities. Image/video tables are bounded and readable; RTSP tables are unbounded and readable; Kafka tables are writable. |
| **Model** | A versioned callable with an immutable persisted interface. `TYPE` selects a capability preset, while an explicit signature exposes a generic tensor model. `FROM`, optional `USING`, and flat `OPTIONS` declare one version; `RESOLVE MODEL` introspects and pins its execution contract. A query calls the Model identifier directly, and planning copies the selected version into its immutable definition snapshot. |
| **Function** | User-defined computation: a SQL expression function or a batched Python function. Function DDL reuses DataFusion's grammar and registry. A SQL function may wrap a typed inference call as an alias or preset. |
| **Window** | A streaming aggregation boundary. `TUMBLE` is a time-bucketing scalar function used in `GROUP BY`; in batch mode it behaves as an ordinary time-bucketed aggregate. |

The key contract is simple: **all data endpoints are Tables.** Provider capabilities decide whether a Table can be read or written and whether a read terminates. Bounded and unbounded reads retain aligned relational and windowing semantics.

### 3.2 The Five-Minute Journey

```bash
pip install visionql
vql shell               # interactive SQL, or import visionql in Python
```

The first-run task calculates the average and peak number of people per minute in a directory of entrance-camera recordings. It is entirely local and needs no camera, Kafka cluster, or other service.

```sql
-- 1. Register a video directory as a frame table sampled at 5 fps.
CREATE TABLE entrance_videos
USING VIDEOS
LOCATION './recordings/entrance/'
OPTIONS (fps = 5);

-- 2. Register one callable Model; graph metadata supplies its tensor contract.
CREATE MODEL yolo26n TYPE OBJECT_DETECTION
FROM './models/yolo26n.onnx';

-- Download/cache/validate explicitly; this may be slow.
RESOLVE MODEL yolo26n;

-- 3. Count people in each frame and aggregate by minute.
SELECT TUMBLE(ts, INTERVAL '1' MINUTE) AS window_start,
       AVG(person_cnt) AS avg_people,
       MAX(person_cnt) AS peak_people
FROM (
  SELECT ts,
         CARDINALITY(yolo26n(frame,
           classes => ['person'], min_confidence => 0.6
         )) AS person_cnt
  FROM entrance_videos
)
GROUP BY 1;
-- CARDINALITY is DataFusion's native array-size function.
```

The workflow stays below 15 non-comment SQL lines and requires neither inference code nor a deployed service. The example ONNX artifact is exported with the metadata needed for resolution from the official `Ultralytics/YOLO26` checkpoint. The interval from `pip install` to the first result must remain under five minutes, which is the TTFV definition used in Section 7 and Acceptance Scenario A.

The same logic can later run against a readable RTSP table declared with `CREATE TABLE ... USING RTSP`, then publish continuously with `INSERT INTO` a writable Kafka table. This is Acceptance Scenario B and the practical meaning of batch–stream unification. Attached streaming queries run in the foreground for development; production lifecycle behavior is defined in Section 3.5.

### 3.3 SQL Surface

#### 3.3.1 Registering Sources

```sql
-- Batch: an image directory becomes a table with one image per row.
CREATE TABLE product_photos
USING IMAGES
LOCATION 's3://bucket/photos/'
OPTIONS (recursive = true);
-- schema: (uri STRING, image IMAGE, width INT, height INT, captured_at TIMESTAMP, ...)

-- Batch: a video directory becomes a frame table sampled at the declared fps.
CREATE TABLE traffic_videos
USING VIDEOS
LOCATION 's3://bucket/dashcam/2026/07/'
OPTIONS (fps = 1);
-- schema: (uri STRING, ts TIMESTAMP, frame IMAGE, frame_id BIGINT, duration DOUBLE, ...)
-- File attributes such as uri and duration are repeated on frame rows.
-- frame is decoded only when the query consumes it.
-- Register another logical table over the same directory to use a different sample rate.

-- Streaming: register one readable, unbounded RTSP table.
CREATE TABLE cam_entrance
USING RTSP
OPTIONS (
  url        = 'rtsp://10.0.0.15:554/main',
  fps        = 5,                        -- sample on demand instead of ingesting at full frame rate
  event_time = 'capture_time',
  watermark  = '2 seconds'
);
-- schema: (ts TIMESTAMP, frame IMAGE, frame_id BIGINT, source STRING)
```

#### 3.3.2 Registering Models

A **MODEL is a versioned callable**. A capability `TYPE` expands into a persisted interface; an explicit `(parameter type, ...) RETURNS type` signature exposes generic tensor models. `FROM` records one version's provenance, optional `USING` selects a Runtime when the source is ambiguous, and flat `OPTIONS` provide only facts resolution cannot inspect.

| Model `TYPE` | Direct-call interface | Canonical result | Availability |
|---|---|---|---|
| `OBJECT_DETECTION` | `model(IMAGE [, classes => CONST ARRAY<STRING>, min_confidence => CONST FLOAT])` | `ARRAY<STRUCT<label STRING, confidence FLOAT, box BOX2D>>` | v0.1 |

`VQL_*` is reserved case-insensitively for release-managed convenience functions. Models use their own names directly. The v0.1 functions are named by task rather than modality and keep one return schema across their input overloads:

| Function | Task contract | v0.1 execution |
|---|---|---|
| `VQL_CLASSIFY(input, categories [, output_mode => ..., min_score => ...])` | Whole-input judgment returning `ARRAY<STRUCT<label, score>>`; `single` is a forced choice and `multi` applies a score threshold | IMAGE through release-managed YOLO26n classification; STRING is typed but unavailable |
| `VQL_EXTRACT(input, fields)` | Ordered named-field extraction whose constant `MAP<STRING, STRUCT<question, list>>` request derives a typed STRUCT result; every answer may carry `LOCATOR` provenance | IMAGE and STRING are typed but unavailable |
| `VQL_DETECT(input [, classes => ..., min_score => ...])` | Instance discovery returning `ARRAY<STRUCT<label, score, locator LOCATOR>>` | IMAGE through release-managed YOLO26n detection |

`LOCATOR` is `STRUCT<char_span STRUCT<start INT, end INT>, box BOX2D>` with nullable members. Text spans use zero-based Unicode code-point offsets with an exclusive end. IMAGE task results use pixel boxes with a top-left origin. Every task score is in `[0, 1]`, sorted descending, and comparable only within one call.

These are capability types rather than broad framework labels such as CV or LLM. The same physical bundle may be registered under multiple compatible Model interfaces, while cache and session reuse remain internal optimizations.

```sql
-- Local ONNX object detection.
CREATE MODEL yolo26n TYPE OBJECT_DETECTION
FROM 'file:///models/yolo26n.onnx';

RESOLVE MODEL yolo26n;
```

`CREATE MODEL` performs local declaration validation and writes the unresolved definition to the Catalog. It never downloads an artifact or contacts a service. `RESOLVE MODEL <name> [VERSION '<version>']` is the explicit slow boundary: it downloads and atomically caches remote artifacts, verifies hashes, resolves local files, or validates service metadata, then writes the resolved execution contract back to the Catalog. The bare form is unambiguous only while the Model has one live version. Planning a query against an unresolved Model fails with an instruction to run `RESOLVE MODEL`.

`SHOW MODELS` exposes aggregate identity, interface, live version count, default, and comment. `SHOW MODEL VERSIONS name` exposes status, volatility, fingerprint, creation time, and default marker.

`.onnx` and `triton+http(s)` sources have permanent Runtime defaults. ONNX resolution inspects names, shapes, layout, output metadata, and labels; ambiguous facts fail while naming the exact fallback option. A Triton URI carries the served model and optional routing version; service-backed versions are always volatile.

`OPTIONS` contains result-affecting artifact, Runtime, input, or output facts, not deployment policy. Device placement, replicas, queue capacity, batch size, maximum wait, concurrency, timeout, and credentials belong to scheduler configuration or the secret provider. Unknown options fail rather than being silently retained.

`FROM` identifies an artifact bundle or endpoint. ONNX graphs, explicitly classified `.pt`/`.pth` artifacts, Safetensors bundles, and GGUF bundles are inputs to compatible Runtimes; a file suffix is not a generic execution strategy. Remote bundles are pinned and content-addressed where possible. VisionQL neither infers a task, label map, tensor contract, or processor from filenames and shapes nor requires a public Profile, Adapter, or `vql-manifest.json`.

The normal embedded open-source integration path is intentionally short: pin the source revision or digest, select a compatible Runtime, declare its typed input/output options, resolve it, and pass one real-model conformance fixture. Common tensor contracts should require only Model DDL; a new reusable tensor family adds one narrow internal processor implementation. Service Runtimes expose the canonical typed capability rather than their internal tensor layout. Arbitrary repository code runs only in an isolated `TRANSFORMERS` worker or behind a service—not inside `vql-kernel`.

#### 3.3.3 Calling Models and Registering User Functions

Inference calls the Model identifier directly. Required domain arguments are positional; optional semantic arguments and the reserved version selector use DataFusion's `=>` named-argument notation:

```sql
SELECT yolo26n(image,
  classes => ['person'],
  min_confidence => 0.5
) AS detections
FROM product_photos;
```

Planning resolves the call target and `version =>` from the Catalog snapshot and copies the resolved version into the inference node; Model identity never becomes row data. A bare call binds the published default. Required domain arguments may be arbitrary row expressions. Semantic arguments must be named constants. Dynamic model selection, duplicate or unknown arguments, unresolved or unknown versions, and type mismatches are planning errors.

`CREATE FUNCTION` is reserved for genuine user-defined computation:

```sql
CREATE FUNCTION fahrenheit(DOUBLE)
RETURNS DOUBLE
RETURN $1 * 1.8 + 32;

CREATE FUNCTION blur_score(img IMAGE) RETURNS FLOAT
LANGUAGE PYTHON AS 'myops.quality:blur_score';
```

VisionQL reuses DataFusion's PostgreSQL-style `CREATE FUNCTION` grammar, `CreateFunction`, `FunctionFactory`, named arguments, and UDF registry. VisionQL adds durable Catalog storage and reconstructs UDFs from each query definition snapshot; session registration is not the source of truth. SQL functions expand during planning, and Python functions use the batched Arrow ABI. A reusable inference alias or parameter preset may be an ordinary SQL expression function whose expanded body still becomes an explicit `Inference` node.

Creation-time expansion infers wrapper parameters that flow into constant-only Model positions. Those parameters are displayed with `CONST` in introspection and rejected at the wrapper call site when a row expression is supplied. The constraint propagates through nested SQL wrappers.

`RETURNS` is required for Python functions. A SQL expression function may omit it when DataFusion can infer the body type; otherwise registration asks for an explicit return type.

#### 3.3.4 Locating People in Video or a Stream

Detection returns an array. `UNNEST` turns each element into a relational row using the BigQuery-style implicit correlation form `FROM t, UNNEST(expr) AS x`.

```sql
-- Live stream: emit one row per detected person.
SELECT ts,
       det.box,
       det.confidence
FROM cam_entrance,
     UNNEST(yolo26n(frame)) AS det
WHERE det.label = 'person'
  AND det.confidence > 0.6;
```

```sql
-- Recorded video: the frame table uses the same query shape.
SELECT f.uri, f.ts, det.box
FROM traffic_videos AS f,
     UNNEST(yolo26n(f.frame)) AS det
WHERE det.label = 'person';
```

#### 3.3.5 Writing Results

```sql
-- Publish a continuous query to Kafka.
INSERT INTO people_per_minute SELECT ...;
```

#### 3.3.6 Rules for an Implementable SQL Dialect

Every extension must map to a mature extension point in the columnar query engine. VisionQL does not require a fork of that engine.

1. **Extensions reduce to two standard mechanisms.**
   - Per-Model typed markers are extracted into explicit `Inference` nodes. Ordinary DataFusion functions cover array operations such as `CARDINALITY`; SQL expression functions expand during planning.
   - VQL DDL updates the Catalog or runtime. Provider-table DDL, `CREATE MODEL`, `CREATE FUNCTION`, and `RESOLVE MODEL` do not enter the relational plan. A video table expands frames inside its scan operator at the fps declared by the table.
2. **No lambdas or higher-order functions.** The object-detection capability owns label and confidence filtering, so native `CARDINALITY` can count its result without another VisionQL-specific function.
3. **`UNNEST` is the only row-expansion mechanism.** `FROM t, UNNEST(expr) AS x` maps to the engine's native unnest node without requiring general lateral joins.
4. **`TUMBLE(event_time, interval)` keeps the same shape in both modes.** Batch lowers it to ordinary time bucketing and aggregation. Streaming adds window state and watermark handling to the same logical plan. ANSI analytic windows remain available through `OVER (...)` and the named `WINDOW` clause for bounded queries.
5. **Every future custom operator needs a function equivalent.** This keeps a portable fallback when dialect syntax is unavailable, without reserving an operator before its feature is implemented.
6. **Multimodal types use standard columnar storage.** `IMAGE` and `VIDEO` are metadata-bearing binary or struct columns, `BOX2D` is a struct, and `VECTOR(n)`/`TENSOR(dtype, dims...)` use Arrow's canonical `arrow.fixed_shape_tensor` extension. Their names exist in DDL and documentation; the underlying engine needs no custom type kernel.

### 3.4 Python API

The v0.1 Python package provides `sess.sql()`, Arrow result exchange, notebook display, and Python UDF registration. SQL is the only normative query-construction surface. A chainable DataFrame API remains a candidate direction and will receive its own proposal only after real Python workflows establish the required composition, streaming, and result-lifecycle semantics.

### 3.5 Product Forms and Deployment

VisionQL must satisfy three competing conditions:

- Batch exploration should begin immediately after `pip install`, without a cluster.
- Continuous queries need a long-lived process for lifecycle ownership, resident model sessions, restart orchestration, and GPU sharing.
- High-volume video should stay close to the camera. Centralizing many feeds creates material network and compliance costs, while query results are comparatively small.

One deployment form cannot serve all three well. VisionQL therefore uses **one engine kernel with two hosts**, sharing SQL and Catalog semantics. Cluster designs remain out of scope until these forms are validated.

| Form | Packaging | Intended use | Release |
|---|---|---|---|
| Embedded `visionql` | pip package embedded in-process, similar to DuckDB | Notebook exploration, batch jobs, CI regression, and foreground streaming during development | v0.1 (MVP) |
| Service `vqld` | Single-node daemon built by `vql-server`; Catalog, model runtime, and streaming runtime live in one binary | Run attached queries remotely and keep explicitly submitted continuous queries alive independently of clients | v0.2 |

The CLI executable is `vql` (`vql shell`, `vql run`), paired with daemon `vqld`. In the interactive shell, `\q` or Ctrl-D exits. The pip package and Python import remain `visionql`.

**Lifecycle and protocol contracts:**

1. **Validate locally, then submit explicitly.** From v0.1, `vql run job.sql` executes batch and streaming queries in the foreground and always attaches them to the client; an ordinary unbounded statement must be last in the script. In v0.2, `SUBMIT QUERY <name> AS INSERT INTO ...` is the public SQL statement for creating a persistent job. An ordinary unbounded statement never becomes detached implicitly.
2. **The service owns a minimal persistent lifecycle.** Explicitly submitted jobs survive client disconnects and expose `SHOW QUERIES`, `DESCRIBE QUERY`, and `STOP QUERY`. The service stores normalized SQL, semantic Session settings, and pinned opaque Catalog generations. `PAUSE`, `RESUME`, serialized window checkpoints, and transparent state migration are not part of v0.2.
3. **Restart is honest rather than transparent.** After a daemon restart, an active RTSP job is replanned against its pinned Catalog generations and reconnects at the live position with fresh in-memory window state. The unavailable interval and discarded open windows are reported as a restart gap. If the historical snapshot can no longer be prepared by the running engine, the job becomes `FAILED` and must be resubmitted.
4. **Clients use a narrow standard protocol.** The service implements the Arrow Flight SQL operations required by one selected Flight SQL or ADBC client integration. Broader JDBC, BI metadata, and vendor capability coverage follows demonstrated client demand.
5. **Remote exposure is explicit.** `vqld` listens on loopback by default. A non-loopback listener requires TLS and one configured service token mapped to a single principal. Multi-user identity and relation-level authorization are later capabilities.
6. **First launch has no mandatory external service.** Catalog and model runtime are built in. Kafka and object storage are optional integrations.

### 3.6 Execution Requirements

The implementation details live in the [High-Level Design](./high_level_design.md) and its component designs, but the following constraints are required for the product experience above:

1. **Optimizer:** in the initial scope, push only user-declared fps/time ranges and decoding, plus query-local common-expression elimination for deterministic typed inference calls.
2. **Frame path:** decoded frames are large. `IMAGE` should remain a reference or compressed value through most of the plan, with decoding deferred until inference or result encoding.
3. **GPU-aware scheduling:** automatic batching, operator/model co-location, and backpressure.
4. **Streaming semantics:** event time, watermarks, and reconnect behavior. RTSP is non-replayable and therefore best-effort; outages and dropped frames must appear as gaps rather than fabricated data.
5. **Result boundary:** bounded results cross host and service boundaries as Arrow values; continuous writes use the Kafka provider contract.
6. **Observability:** query lifecycle, source health, last event time, restart gaps, and errors are available through `SHOW QUERIES` and `DESCRIBE QUERY`. Prometheus, when enabled, exposes only process and aggregate operational metrics without `query_id` labels.
7. **Inference and code functions:** calls through persisted Model interfaces become explicit `Inference` nodes so the engine can manage accelerators, batching, and backpressure. SQL expression and Python Function DDL reuse DataFusion's function extension points as ordinary UDFs. In embedded mode, Python UDFs run in the host process and exchange Arrow batches. v0.2 service execution rejects Python Functions with `FEATURE_NOT_AVAILABLE`; an isolated service worker is unscheduled. The kernel never embeds a Python interpreter.

### 3.7 Non-Functional Requirements

| Area | Requirement |
|---|---|
| **Fault behavior** | RTSP is non-replayable and best-effort; gaps are reported, never invented. Reconnect automatically. In v0.2, persistent jobs restart from the current live position with fresh window state and an explicit restart gap; they do not claim transparent state recovery. |
| **Error semantics** | A single decode or inference failure produces NULL for that row. Optional strict mode is `on_error = 'fail'`. Model false positives and false negatives are not engine errors; users manage them with explicit thresholds. |
| **Security and privacy** | Data stays in its domain by default. Pin and hash model sources. `vqld` binds to loopback by default; non-loopback use requires TLS and one configured service token. Multi-user relation authorization and service-side Python execution are outside v0.2. |
| **Compatibility** | VisionQL v0.1 is the first public release. Before 1.0, minor releases may revise SQL and Catalog contracts; patch releases preserve the documented public contract. The current SQLite schema is initialized directly and has no legacy migration chain. `EXPLAIN` text is not a stable API before 1.0. |

### 3.8 Workbench

General SQL clients do not render `IMAGE`, `BOX2D`, or detection arrays naturally. v0.3 therefore adds a focused Workbench client after v0.2 validates the service protocol with one selected Flight SQL or ADBC integration.

Workbench connects to one `vqld` endpoint, runs and cancels one bounded SQL statement at a time, renders ordinary columns and bounded thumbnails with `BOX2D` overlays, and displays structured VQL errors. It does not add Catalog browsing, live-stream preview, persistent-job operations, saved queries, dashboards, cost views, or original-media access in v0.3.

Workbench remains an independent public-protocol client; `vql-kernel` and `vqld` do not gain Workbench-only APIs. See the [Workbench Design](./design/workbench.md) for the client and media boundaries.

---

## 4. Scope and Version Boundaries

VisionQL is a query and processing engine, not a complete vertical application.

- It does not train models or provide a labeling platform. It can find and export training examples, including corner cases.
- It is not a video management system or media server. It connects to existing RTSP and object-storage systems.
- It is not an end-user security or moderation application. Scenario packages may include models, SQL templates, and dashboards, but applications remain separate.

**v0.1 is a single-node, embedded batch-and-streaming release for images, recorded video, and RTSP:**

- `IMAGE`, `VIDEO`, `BOX2D`, `VECTOR(n)`, `TENSOR(dtype, dims...)`, nested types, and `UNNEST`. `VECTOR` and `TENSOR` serve generic Model boundaries.
- Image and video directory tables. Video is expanded by the table's declared fps.
- `OBJECT_DETECTION` capability Models, generic ONNX signatures, named immutable versions, and direct `<model>(...)` calls. `VQL_CLASSIFY`, `VQL_EXTRACT`, and `VQL_DETECT` provide the complete task-shaped v0.1 AI function surface. Installed YOLO26n ImageNet classification and COCO detection artifacts execute the IMAGE classify/detect overloads; field extraction and STRING overloads return `FEATURE_NOT_AVAILABLE`. Local `ONNX_RUNTIME` uses VisionQL-owned processors, while remote `TRITON_INFERENCE_SERVER` owns its complete pre/post-processing pipeline behind the same canonical typed result. `CREATE FUNCTION` provides DataFusion-backed SQL expression and in-process Python UDFs.
- One RTSP provider table with event time, watermarks, reconnect handling, and best-effort delivery; `TUMBLE` uses a bounded allowlist of `COUNT/SUM/AVG/MIN/MAX` over persistable scalar types.
- Foreground SELECT results return directly; Kafka is a writable provider table for continuous output.
- Embedded pip package, SQL shell, `vql run job.sql`, and Python library with `sess.sql()`, Arrow results, notebook display, and UDF registration. Batch and streaming queries run in the foreground and stay attached to the client. A chainable DataFrame API is unscheduled.
- Configuration, Catalog, shell history, and cache live under `VQL_HOME` (default `$HOME/.vql`). `vql-catalog` uses SQLite at exactly `$VQL_HOME/catalog/vql.db`; its backend port is the future MySQL/PostgreSQL boundary. `$VQL_HOME/config.toml` selects SQLite settings and the Session memory limit. SQL defaults to `vql.default`.
- Explicit frame-sampling pushdown as the first optimizer feature.

RTSP remains non-replayable and best-effort; outages and drops appear as gaps. Scenario B uses recorded-video batch output as the trusted reference for the streaming result, but both paths are required before v0.1 is complete.

**v0.2 adds the minimal `vqld` service.** It hosts attached queries over a tested Arrow Flight SQL subset and keeps explicitly submitted continuous writes running after their clients disconnect. Persistent jobs store SQL, semantic settings, and pinned opaque Catalog generations. After daemon restart, active RTSP jobs restart from the live position with fresh window state and an explicit gap. v0.2 has no serialized checkpoints, `PAUSE`/`RESUME`, multi-user relation authorization, UC HTTP surface, service-side Python UDF worker, original-media ticket, DataFrame API, or Workbench. The exact host and lifecycle contracts are defined in the [`vqld` Service Design](./design/vqld.md).

**v0.3 adds Workbench.** The independent browser client uses only the public `vqld` protocol to execute and cancel one bounded statement, render tables with thumbnail and `BOX2D` results, and present structured errors. It does not add a parallel query, Catalog, job-control, or media API.

Everything else remains intentionally undefined. Candidate directions live in the [Roadmap](../ROADMAP.md) and will be scheduled only after earlier releases produce real feedback.

**Acceptance scenarios:**

- **Scenario A — first value without external services (v0.1):** run locally in a Python host. Register an image directory, use a Python UDF to reject blurry images, call a local ONNX Model directly to select images containing a target object, and display the result in the Python session. An in-process UDF requires a notebook or REPL; `vql shell` must direct the user to a Python host. The pure-SQL first-run path in Section 3.2 runs in the shell. Both paths must produce a first result within five minutes of `pip install`.
- **Scenario B — batch/stream parity (v0.1):** start with the per-minute people-count query in Section 3.2, run it over recorded video, then point the same logic at an RTSP table and use `vql run` to publish to a Kafka table. With the same model and sample rate, assert equivalent results. Inspect `UNNEST` output through an ordinary foreground SELECT. Batch is the trusted reference for the streaming comparison.
- **Scenario C — client-independent service execution (v0.2):** submit the Scenario B RTSP-to-Kafka write through `vqld`, disconnect the client, reconnect and inspect the same job, then stop it explicitly. Restart `vqld` during a second run and verify that the job reconnects at the live position, reports the unavailable interval and reset window state, and never claims replay or exactly-once delivery. Run the scenario through one selected Flight SQL or ADBC client integration.
- **Scenario D — bounded visual inspection (v0.3):** connect Workbench to `vqld`, run the bounded image query from Scenario A, render each returned thumbnail with its `BOX2D` overlays, cancel one active query by its server query ID, and display one structured VQL error. The SQL and returned values must match the Python/notebook path.

Scenario A proves that first use is simple. Scenario B proves the differentiated end-to-end streaming capability. Scenario C proves the only new product value required from the first service release: execution independent of a client process. Scenario D proves that Workbench improves visual inspection without creating a second execution contract.

## 5. Roadmap Summary

| Release | Theme | Core deliverables |
|---|---|---|
| **v0.1 (MVP)** | Single-node batch and streaming | Embedded pip package, SQL, CLI, image/video/RTSP/Kafka provider tables, object detection, Python UDFs, `TUMBLE`, attached continuous execution, Acceptance Scenarios A and B |
| **v0.2** | Minimal single-node service | `vqld`, a tested Flight SQL subset, explicit `SUBMIT QUERY`, client-independent jobs, honest restart-from-live behavior, one-principal remote security, and Acceptance Scenario C |
| **v0.3** | Workbench | Independent browser client, bounded SQL execution and cancellation, thumbnail and `BOX2D` rendering, structured errors, and Acceptance Scenario D |

Persistent window checkpoints, pause/resume, multi-user authorization, broader BI compatibility, original-media tickets, service-side Python UDFs, the DataFrame API, cost optimization, clustering, and edge coordination remain future candidates. The complete delivery and acceptance plan is maintained in the [Roadmap](../ROADMAP.md).

## 6. Business Model

VisionQL will use an Apache-2.0 open-source engine to establish adoption and a common visual SQL ecosystem. The kernel, embedded and service forms, and complete SQL semantics remain open source so individuals and small teams can use the full product. Commercial offerings will focus on operating VisionQL at production scale—enterprise governance, managed services, and related needs—after the open-source releases validate product value.

The first users will be two or three design partners working with the team on one focused scenario, selected between security/campuses and content moderation. The open-source launch will include a runnable example for that scenario.

## 7. Success Metrics

**North-star metric: successful visual-query workflows used each week.** A bounded workflow counts when its result is collected or written; a continuous workflow counts when its sink acknowledges output. Video hours and inference calls are workload and cost diagnostics, not measures of user value.

| Dimension | Metric |
|---|---|
| Activation | Time to first value is under 5 minutes from `pip install`, with no external service; measured through Section 3.2 and Scenario A |
| Efficiency | The standard per-minute people-count task uses fewer than 30 lines of code and goes from zero to running in under 30 minutes |
| Cost | On workloads where sampling is valid, GPU time falls approximately in proportion to the declared sample-rate reduction versus full-frame inference |
| Correctness | With the same model and sample rate, window aggregates match a hand-built baseline pipeline |
| Adoption | Within 90 days of open-source launch, at least 3 real external scenarios run end to end and at least 1 design partner carries production traffic |
| Service value | A submitted v0.2 job continues after client disconnect, can be rediscovered and stopped, and reports an honest gap after daemon restart |
| Retention | Weekly successful workflows grow among design partners; repeated workflows matter more than raw processed hours |

## 8. Open Questions

1. **Initial packaging:** should the entrance-camera anchor workflow first be presented for security/campuses or content moderation? The answer changes examples and design-partner outreach, but not the engine or v0.2 service scope.
2. **SQL compatibility:** provider-table DDL follows Spark/Databricks `USING ... OPTIONS (...)`; remaining type names, functions, and errors should converge only where doing so preserves VisionQL's typed inference and streaming semantics.
3. **Confidence-aware aggregation:** should VisionQL eventually provide interval estimates or other dedicated primitives, or should users continue to set thresholds explicitly?
4. **Service client profile:** which existing Flight SQL or ADBC client should become the single v0.2 compatibility target? Broader protocol coverage follows measured client demand.

---

## Appendix: VQL Syntax

| Syntax | Category | Purpose |
|---|---|---|
| `CREATE TABLE ... USING IMAGES/VIDEOS/RTSP/KAFKA OPTIONS (...)` | DDL | Register a readable or writable provider table |
| `CREATE MODEL ... { TYPE ... \| (...) RETURNS ... } ... [OPTIONS (...)]` | DDL | Store a fast, unresolved Model aggregate with an immutable interface and first version |
| `ALTER MODEL ... ADD\|DROP VERSION / SET DEFAULT_VERSION / SET COMMENT / RENAME TO` | DDL | Mutate a versioned Model aggregate without changing its persisted interface |
| `RESOLVE MODEL <name> [VERSION '<version>']` | DDL | Perform the potentially slow artifact download/cache or service validation step for one version |
| `<model>(arguments [, version => '<version>', named semantic arguments])` | Typed inference | Call a Model directly; planning resolves the call target and version and extracts an `Inference` node |
| `CREATE FUNCTION ... RETURN <expression> / LANGUAGE PYTHON AS '<entry>'` | DDL | Register a DataFusion-backed SQL expression or batched Python function |
| `TUMBLE(ts, interval)` | Time bucket | Define a tumbling window for batch or streaming `GROUP BY` |
| `UNNEST(expr) AS x` | Relational | Expand an array of detections into rows |
| `CARDINALITY(array)` | DataFusion scalar function | Count detections after an object-detection Model applies its named filters |
| `SUBMIT QUERY name AS INSERT INTO ...` | Operations | Create a persistent Table-write job explicitly in v0.2; ordinary unbounded SQL stays attached |
| `SHOW QUERIES / DESCRIBE QUERY / STOP QUERY` | Operations | Inspect and stop persistent queries in v0.2 |
| `EXPLAIN` | Operations | Show the query plan |
