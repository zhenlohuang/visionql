# VisionQL Product Requirements Document

> VisionQL is a unified batch and streaming engine for multimodal data. It lets users query and process images, recorded video, and live video streams through SQL or a DataFrame API.

---

## 1. Product in One Sentence

**VisionQL is a data engine for Physical AI.**

Traditional databases answer questions about structured business records. VisionQL makes the visual world captured by cameras and video files queryable. Engineers and analysts can analyze images, recordings, and live streams with one SQL statement or a few lines of DataFrame code, without assembling one-off Python pipelines or operating GPU inference jobs themselves.

- Images and recorded video are bounded datasets and run as batch workloads, much like Spark batch jobs.
- RTSP video is an unbounded dataset and runs as a streaming workload, much like Flink or Spark Structured Streaming.
- Both modes use the same SQL and DataFrame semantics.

---

## 2. Why This Product Should Exist

### 2.1 Market and Technology Context

1. **Visual data is growing faster than the systems built to process it.** An estimated 80–90% of newly created enterprise data is unstructured, including images, surveillance footage, dashcam recordings, and live feeds. Physical AI systems such as autonomous vehicles and robots add a constant stream of first-person video. Their data loops—finding corner cases and assembling training or evaluation sets—need infrastructure designed for visual data. Most existing data platforms can store a file path, but cannot understand or query what appears in the file.

2. **Vision models are now useful as query operators.** Detection, tracking, OCR, vision-language models, and vector search can answer questions such as “How many people are in this frame?” or “Which clips show an intrusion into the restricted area?” What is missing is a coherent data system that connects models, media, and queries.

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

**Primary users, in priority order:**

1. **Data and ML engineers** who build visual-processing pipelines today. They are the core users of the first release and primarily consume VisionQL as a pip-installable library.
2. **Data analysts** who know SQL but not PyTorch. Starting in v0.2, they can connect to the authenticated `vqld` service from Workbench or a BI tool without a local installation.
3. **Platform teams** building an internal visual analytics platform with multi-tenancy, governance, and cost controls. They are also the likely buyers of a future enterprise offering.
4. **AI application and agent developers** who need a visual query tool for cameras and video libraries. Existing text-to-SQL systems can generate auditable, permission-scoped VisionQL queries.

**Representative use cases:**

| Use case | Mode | Typical questions |
|---|---|---|
| Security and smart campuses | Streaming | People per minute, boundary-crossing alerts, loitering, cross-camera trajectories |
| Retail traffic analysis | Streaming | Store entries, movement heatmaps, dwell time, queue length |
| Autonomous driving and robotics data loops | Batch | Find corner cases such as rain + crossing pedestrian + occlusion in large driving or teleoperation archives |
| Media and content platforms | Batch | Tag videos, find people or scenes, extract highlights, retrieve clips by text |
| Content moderation | Batch + streaming | Apply the same policy to live feeds and historical backfills |
| Industrial inspection | Streaming | Detect defects, aggregate yield by minute, retain evidence frames |
| Drone and infrastructure inspection | Batch | Find defects, compare inspections over time, export work-order evidence |
| Sports analytics | Batch | Track players, analyze formations, retrieve events such as shots or fouls |
| AI agents and assistants | Batch + streaming | Answer questions such as “Has anyone remained in the warehouse for more than ten minutes?” and support visual RAG |

Content moderation illustrates the value of a unified engine particularly well: one SQL query can backfill a file table and then monitor a live stream. VisionQL may eventually support many industries, but the initial launch will make one scenario excellent. The choice will be driven by pain, data volume, and willingness to pay; see Open Question 1.

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
| **SQL cannot express every vision workflow** | Calibration and complex multi-object association do not fit naturally into SQL | Do not force all logic into SQL; use UDFs, custom models, and the DataFrame API for complex processing |
| **Connector and model coverage takes time** | Product usefulness depends on supported sources, models, and scenario templates | Start with object detection plus RTSP, object storage, and Kafka Sink; add embedding search in v0.3; make one security or moderation workflow complete before expanding |
| **Large platforms may add similar features** | Databricks and cloud vendors can extend their multimodal offerings | Differentiate through unified batch and streaming behavior, vision-native optimization, and an open-source ecosystem |

---

## 3. Product Experience

### 3.1 Core Abstractions

VisionQL uses one unifying model: **visual data is represented as relations made up of frames.**

| Abstraction | Meaning |
|---|---|
| **Multimodal type system** | Extends standard SQL with `IMAGE`, `VIDEO`, `BOX2D`, `VECTOR(n)` (enabled for embedding search in v0.3), and nested `STRUCT` / `ARRAY` types |
| **Table** | A bounded dataset. An image directory is one row per image. A video directory is expanded at its declared sample rate into one row per frame. |
| **Stream** | An unbounded frame relation such as `(ts TIMESTAMP, frame IMAGE, ...)`, with event-time and watermark semantics |
| **Model** | A typed inference capability. `TYPE` fixes its built-in SQL function and canonical Arrow result; the definition selects an artifact or endpoint, Runtime, PreProcessor, and PostProcessor. A query names the Model through the built-in function's constant `model` argument, and planning copies the resolved definition into an immutable Query Manifest. |
| **Function** | User-defined computation: a SQL expression function or a batched Python function. Function DDL reuses DataFusion's grammar and registry. A SQL function may wrap a typed inference call as an alias or preset. |
| **Window** | A streaming aggregation boundary. `TUMBLE` is a time-bucketing scalar function used in `GROUP BY`; in batch mode it behaves as an ordinary time-bucketed aggregate. |
| **Sink** | A destination such as Console, Kafka, Parquet, or Lance |

The key contract is simple: **tables and streams use the same query language.** A table query terminates; a stream query continues. Their relational and windowing semantics remain aligned.

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
WITH (fps = 5);

-- 2. Register one typed inference capability.
-- TYPE fixes the IMAGE_DETECTION SQL interface and result schema.
CREATE MODEL yolo26n
TYPE OBJECT_DETECTION
FROM './models/yolo26n.onnx'
WITH (
  runtime.kind = 'onnxruntime',
  pre_processor.kind = 'vision.image_tensor@1',
  pre_processor.options = {
    input_name = 'images', width = 640, height = 640, resize = 'letterbox'
  },
  post_processor.kind = 'vision.yolo_e2e@1',
  post_processor.options = {output_name = 'output0', labels = 'coco80'}
);

-- 3. Count people in each frame and aggregate by minute.
SELECT TUMBLE(ts, INTERVAL '1' MINUTE) AS window_start,
       AVG(person_cnt) AS avg_people,
       MAX(person_cnt) AS peak_people
FROM (
  SELECT ts,
         CARDINALITY(IMAGE_DETECTION(
           'yolo26n', frame,
           classes => ['person'], min_confidence => 0.6
         )) AS person_cnt
  FROM entrance_videos
)
GROUP BY 1;
-- CARDINALITY is DataFusion's native array-size function.
```

The workflow stays below 30 non-comment SQL lines and requires neither inference code nor a deployed service. The example ONNX artifact is exported explicitly from the official `Ultralytics/YOLO26` checkpoint; preparing a model is separate from query logic. The interval from `pip install` to the first result must remain under five minutes, which is the TTFV definition used in Section 7 and Acceptance Scenario A.

The same logic can later run against live video: replace the table in `FROM` with an RTSP stream registered by `CREATE STREAM`, then use `CREATE SINK` and `INSERT INTO` to publish continuously to Kafka. This is Acceptance Scenario B and the practical meaning of batch–stream unification. Attached streaming queries run in the foreground for development; production lifecycle behavior is defined in Section 3.5.

### 3.3 SQL Surface

#### 3.3.1 Registering Sources

```sql
-- Batch: an image directory becomes a table with one image per row.
CREATE TABLE product_photos
USING IMAGES
LOCATION 's3://bucket/photos/'
WITH (recursive = true);
-- schema: (uri STRING, image IMAGE, width INT, height INT, captured_at TIMESTAMP, ...)

-- Batch: a video directory becomes a frame table sampled at the declared fps.
CREATE TABLE traffic_videos
USING VIDEOS
LOCATION 's3://bucket/dashcam/2026/07/'
WITH (fps = 1);
-- schema: (uri STRING, ts TIMESTAMP, frame IMAGE, frame_id BIGINT, duration DOUBLE, ...)
-- File attributes such as uri and duration are repeated on frame rows.
-- frame is decoded only when the query consumes it.
-- Register another logical table over the same directory to use a different sample rate.

-- Streaming: register one RTSP camera.
CREATE STREAM cam_entrance
FROM 'rtsp://10.0.0.15:554/main'
WITH (
  fps        = 5,                        -- sample on demand instead of ingesting at full frame rate
  event_time = 'capture_time',
  watermark  = INTERVAL '2' SECOND
);
-- schema: (ts TIMESTAMP, frame IMAGE, frame_id BIGINT, source STRING)
```

#### 3.3.2 Registering Models

A **MODEL is one typed inference capability**. Its `TYPE` fixes the callable SQL interface and canonical result; its source and `WITH` options describe how an artifact or endpoint implements that interface.

| Model `TYPE` | Built-in SQL function | Canonical result | Availability |
|---|---|---|---|
| `OBJECT_DETECTION` | `IMAGE_DETECTION` over `IMAGE` | `ARRAY<STRUCT<label STRING, confidence FLOAT, box BOX2D>>` | v0.1 |
| `IMAGE_CLASSIFICATION` | `IMAGE_CLASSIFICATION` over `IMAGE` | `ARRAY<STRUCT<label STRING, score FLOAT>>` | Roadmap-gated |
| `IMAGE_EMBEDDING(n)` | `IMAGE_EMBEDDING` over `IMAGE` | `VECTOR(n)` | v0.3 |
| `TEXT_EMBEDDING(n)` | `TEXT_EMBEDDING` over `STRING` | `VECTOR(n)` | v0.3 |
| `TEXT_GENERATION` | `TEXT_GENERATION` over `STRING` | `STRING` | Roadmap-gated |

These five typed-inference function names are reserved case-insensitively, including Roadmap-gated markers. `CREATE FUNCTION` cannot redefine them.

These are capability types rather than broad framework labels such as CV or LLM. The same physical bundle may be registered under multiple compatible types—for example, separate CLIP image- and text-embedding Models—while cache and session reuse remain internal optimizations.

```sql
-- Local ONNX object detection.
CREATE MODEL yolo26n
TYPE OBJECT_DETECTION
FROM 'file:///models/yolo26n.onnx'
WITH (
  runtime.kind = 'onnxruntime',

  pre_processor.kind = 'vision.image_tensor@1',
  pre_processor.options = {
    input_name = 'images',
    width = 640,
    height = 640,
    resize = 'letterbox',
    color_space = 'rgb',
    layout = 'nchw'
  },

  post_processor.kind = 'vision.yolo_e2e@1',
  post_processor.options = {
    output_name = 'output0',
    box_format = 'xyxy',
    labels = 'coco80'
  }
);
```

Model options are deliberately namespaced:

| Namespace | Responsibility |
|---|---|
| `runtime.*` | Select the implementation with `runtime.kind` and, for a service, its wire contract with `runtime.protocol`; binding fields such as a served model and immutable version live here too |
| `pre_processor.*` | Select a versioned PreProcessor and provide all implementation-specific values in one `pre_processor.options = {...}` object |
| `post_processor.*` | Select a versioned PostProcessor and provide all implementation-specific values in one `post_processor.options = {...}` object |

`runtime.kind` is an implementation name such as `onnxruntime`, `triton`, `transformers`, `vllm`, `sglang`, or `llama_cpp`. `runtime.protocol` is orthogonal: examples include `kserve_v2_http`, `kserve_v2_grpc`, and `openai`. Thus `runtime.kind = 'sglang'` with `runtime.protocol = 'openai'` still selects the SGLang integration; `openai` is never a Runtime kind. Every type, Runtime, protocol, and processor combination is version-gated and validated before a usable Model is stored.

The `WITH` clause contains result-affecting implementation data, not deployment policy. Device placement, replicas, queue capacity, batch size, maximum wait, concurrency, timeout, and credentials belong to internal RuntimeConfig or the secret provider. Flat processor fields such as `pre_processor.width` are invalid. Unknown namespaces and unknown processor options fail rather than being silently retained.

`FROM` identifies an artifact bundle or endpoint. ONNX graphs, explicitly classified `.pt`/`.pth` artifacts, Safetensors bundles, and GGUF bundles are inputs to compatible Runtimes; a file suffix is not a generic execution strategy. Remote bundles are pinned and content-addressed where possible. VisionQL neither infers a task, label map, tensor contract, or processor from filenames and shapes nor requires a public Profile, Adapter, or `visionql-manifest.json`.

The normal open-source integration path is intentionally short: pin the source revision or digest, select a compatible Runtime, select compatible PreProcessor and PostProcessor kinds, declare only their typed options, and pass one real-model conformance fixture. Common tensor contracts should require only Model DDL; a new reusable tensor family adds one narrow processor implementation. Arbitrary repository code runs only in an isolated `transformers` worker or behind Triton, vLLM, SGLang, or llama.cpp—not inside `vql-kernel`.

#### 3.3.3 Calling Models and Registering User Functions

Inference uses the fixed built-in function owned by the Model type. Required arguments are positional: the Model name first, followed by the type-specific domain input. Optional semantic arguments use DataFusion's `=>` named-argument notation:

```sql
SELECT IMAGE_DETECTION(
  'yolo26n',
  image,
  classes => ['person'],
  min_confidence => 0.5
) AS detections
FROM product_photos;
```

In v0.1, the first argument must be a non-NULL string literal naming a Model. Planning resolves it from the Catalog, validates that its Model type matches the built-in function, and stores the resolved Model in the Query Manifest; the string never becomes a per-row Arrow value or Runtime request field. Required domain arguments such as `image` or `prompt` may be arbitrary row expressions. Optional semantic arguments such as `classes` and thresholds must be named constants and follow all positional arguments. Dynamic model selection, prepared Model parameters, duplicate or unknown arguments, and type mismatches are planning errors.

`CREATE FUNCTION` is reserved for genuine user-defined computation:

```sql
CREATE FUNCTION fahrenheit(DOUBLE)
RETURNS DOUBLE
RETURN $1 * 1.8 + 32;

CREATE FUNCTION blur_score(img IMAGE) RETURNS FLOAT
LANGUAGE PYTHON AS 'myops.quality:blur_score';
```

VisionQL reuses DataFusion's PostgreSQL-style `CREATE FUNCTION` grammar, `CreateFunction`, `FunctionFactory`, named arguments, and UDF registry. VisionQL adds durable Catalog storage and reconstructs the UDF for each Query Manifest; session registration is not the source of truth. SQL functions expand during planning, and Python functions use the batched Arrow ABI. A reusable inference alias or parameter preset may be an ordinary SQL expression function whose expanded body still becomes an explicit `Inference` node.

`RETURNS` is required for Python functions. A SQL expression function may omit it when DataFusion can infer the body type; otherwise registration asks for an explicit return type.

#### 3.3.4 Locating People in Video or a Stream

Detection returns an array. `UNNEST` turns each element into a relational row using the BigQuery-style implicit correlation form `FROM t, UNNEST(expr) AS x`.

```sql
-- Live stream: emit one row per detected person.
SELECT ts,
       det.box,
       det.confidence
FROM cam_entrance,
     UNNEST(IMAGE_DETECTION('yolo26n', frame)) AS det
WHERE det.label = 'person'
  AND det.confidence > 0.6;
```

```sql
-- Recorded video: the frame table uses the same query shape.
SELECT f.uri, f.ts, det.box
FROM traffic_videos AS f,
     UNNEST(IMAGE_DETECTION('yolo26n', f.frame)) AS det
WHERE det.label = 'person';
```

#### 3.3.5 Cross-Modal Retrieval

> Embeddings and vector search arrive in v0.3. This section fixes their SQL semantics in advance.

```sql
-- Return the 20 images most similar to the text prompt.
SELECT uri, image
FROM product_photos
ORDER BY IMAGE_EMBEDDING('clip_image', image)
         <-> TEXT_EMBEDDING('clip_text', 'a worker wearing a red helmet')
LIMIT 20;
```

A vector column can be indexed with `CREATE INDEX ... USING HNSW`. When an index is present, `ORDER BY <-> LIMIT` is rewritten to approximate nearest-neighbor search. `<->` is shorthand for `L2_DISTANCE(a, b)`.

#### 3.3.6 Writing Results

```sql
-- Publish a continuous query to Kafka.
INSERT INTO people_per_minute SELECT ...;

-- Retain evidence frames with the alert.
INSERT INTO evidence  -- a Lance/Parquet table; native IMAGE storage arrives in v0.3
SELECT ts, frame, det.box
FROM cam_entrance,
     UNNEST(IMAGE_DETECTION('yolo26n', frame)) AS det
WHERE det.label = 'person' AND det.confidence > 0.9;
```

#### 3.3.7 Rules for an Implementable SQL Dialect

Every extension must map to a mature extension point in the columnar query engine. VisionQL does not require a fork of that engine.

1. **Extensions reduce to two standard mechanisms.**
   - Type-owned inference markers such as `IMAGE_DETECTION` are extracted into explicit `Inference` nodes. Ordinary DataFusion functions cover array operations (`CARDINALITY`) and vector predicates (`L2_DISTANCE`); SQL expression functions expand during planning.
   - VQL DDL updates the Catalog or runtime. `CREATE STREAM/MODEL/FUNCTION/SINK` does not enter the relational plan. A video table expands frames inside its scan operator at the fps declared by the table.
2. **No lambdas or higher-order functions.** `IMAGE_DETECTION` owns label and confidence filtering, so native `CARDINALITY` can count its result without another VisionQL-specific function.
3. **`UNNEST` is the only row-expansion mechanism.** `FROM t, UNNEST(expr) AS x` maps to the engine's native unnest node without requiring general lateral joins.
4. **`TUMBLE` keeps the same shape in both modes.** Batch lowers it to ordinary time bucketing and aggregation. Streaming adds window state and watermark handling to the same logical plan. `WINDOW` remains reserved for ANSI analytic functions whose output cardinality does not change.
5. **Every custom operator has a function equivalent.** Operators such as `<->` normalize to functions, leaving a portable fallback when dialect syntax is unavailable.
6. **Multimodal types use standard columnar storage.** `IMAGE` and `VIDEO` are metadata-bearing binary or struct columns, `BOX2D` is a struct, and `VECTOR(n)` is a fixed-size float list. Their names exist in DDL and documentation; the underlying engine needs no custom type kernel.

### 3.4 Python DataFrame API (v0.2)

SQL and the DataFrame API build the same logical plan. SQL remains the primary interface; the DataFrame API gives engineers a programmable way to assemble more complex workflows.

In v0.1, the Python package provides `sess.sql()`, Arrow result exchange, rich notebook display, and Python UDF registration. The chainable API arrives in v0.2 after the logical plan has been validated by real v0.1 queries; exposing it earlier would freeze an immature internal representation as a public contract.

The target API looks like this. Embedding and Lance output are v0.3 capabilities:

```python
import visionql as vq

sess = vq.connect()

# Streaming equivalent of Section 3.2: publish average and peak counts by minute.
counts = (
    sess.stream("cam_entrance")
        .with_column("person_cnt",
                     vq.fn("cardinality")(
                         vq.fn("image_detection")(
                             "yolo26n", vq.col("frame"),
                             classes=["person"], min_confidence=0.6,
                         )
                     ))
        .window(vq.tumble("1 minute"))
        .agg(avg_people=vq.avg("person_cnt"), peak_people=vq.max("person_cnt"))
)
counts.write.kafka("broker:9092", topic="people-count").start()

# Batch: tag an image directory and persist the result.
(
    sess.table("product_photos")
        .with_column("tags", vq.fn("image_detection")(
            "yolo26n", vq.col("image")
        ))
        .with_column("embedding", vq.fn("image_embedding")(
            "clip_image", vq.col("image")
        ))
        .write.lance("s3://bucket/photo_index/")
)
```

The APIs can be mixed: `sess.sql(...)` returns a DataFrame.

### 3.5 Product Forms and Deployment

VisionQL must satisfy three competing conditions:

- Batch exploration should begin immediately after `pip install`, without a cluster.
- Continuous queries need a long-lived process for state, recovery, resident model sessions, and GPU sharing.
- High-volume video should stay close to the camera. A 1080p stream is roughly 4 Mbps; centralizing dozens of feeds creates material network and compliance costs, while query results are often only kilobytes.

One deployment form cannot serve all three well. VisionQL therefore uses **one engine kernel with two hosts**, sharing SQL and Catalog semantics. Cluster designs remain out of scope until these forms are validated.

| Form | Packaging | Intended use | Release |
|---|---|---|---|
| Embedded `visionql` | pip package embedded in-process, similar to DuckDB | Notebook exploration, batch jobs, CI regression, and foreground streaming during development | v0.1 (MVP) |
| Service `vqld` | Single-node daemon built by `vql-server`; Catalog, model runtime, and streaming runtime live in one binary | Long-running streams, durable jobs and recovery, shared clients, Workbench, and BI access | v0.2 |

The CLI executable is `vql` (`vql shell`, `vql run`), paired with daemon `vqld`. The pip package and Python import remain `visionql`.

**Lifecycle and protocol contracts:**

1. **Validate in a notebook, then run the same script.** From v0.1, `vql run job.sql` executes batch and streaming queries in the foreground and always attaches them to the client. In v0.2, `vql submit job.sql [--name <job>]` executes the DDL statements and wraps the script's single unbounded Sink statement as `SUBMIT QUERY`; the filename supplies the default job name. `SUBMIT QUERY <name> AS INSERT INTO ...` is the public protocol statement used by CLI and Workbench. An ordinary unbounded SQL statement never becomes detached implicitly after an upgrade.
2. **The service owns durable queries in v0.2.** Foreground v0.1 streams stop with the client. Explicitly submitted jobs gain a name, state, recovery, query-level metrics, and `SHOW/DESCRIBE QUERY`, `PAUSE`, `RESUME`, and `STOP`. `DESCRIBE QUERY` exposes dependencies. Dropping an object referenced by a running job fails with a structured error listing the dependent jobs.
3. **Clients use standard columnar protocols.** The service speaks Arrow Flight SQL. Python, BI tools, and third-party applications connect through Flight SQL, ADBC, or JDBC; there is no private client protocol.
4. **First launch has no mandatory external service.** Catalog and model runtime are built in. Kafka, object storage, and Kubernetes are optional integrations.
5. **Workbench ships with the service.** It is a separate lightweight project and a normal Flight SQL client. This both enforces the public protocol boundary and validates that the protocol covers a real visual client.

### 3.6 Execution Requirements

The implementation details live in [System Design](./design.md), but the following constraints are required for the product experience above:

1. **Optimizer:** in the initial scope, push only user-declared fps/time ranges and decoding, plus query-local common-expression elimination for deterministic typed inference calls.
2. **Frame path:** decoded 1080p RGB is about 6 MB per frame; one 5 fps stream produces about 30 MB/s. `IMAGE` should remain a reference or compressed value through most of the plan, with decoding deferred until inference or persistence.
3. **GPU-aware scheduling:** automatic batching, operator/model co-location, and backpressure.
4. **Streaming semantics:** event time, watermarks, and reconnect behavior. RTSP is non-replayable and therefore best-effort; outages and dropped frames must appear as gaps rather than fabricated data.
5. **Storage:** columnar multimodal output, with Parquet and Lance plus video references in v0.3.
6. **Observability:** per-query inference count and latency; from v0.2, a Prometheus endpoint powers operational and Workbench cost views.
7. **Inference and code functions:** type-owned model calls become explicit `Inference` nodes so the engine can manage accelerators, batching, and backpressure. SQL expression and Python Function DDL reuse DataFusion's function extension points as ordinary UDFs. In embedded mode, Python UDFs run in the host process and exchange Arrow batches. In the v0.2 service, Python UDFs run in isolated worker processes over Arrow IPC. The kernel never embeds a Python interpreter and needs Python only when a Python function is registered.

### 3.7 Non-Functional Requirements

| Area | Requirement |
|---|---|
| **Performance (MVP baseline)** | On one machine with one consumer GPU: at least 8 concurrent 1080p@5fps streams running lightweight detection plus window aggregation; batch scans should saturate hardware with decoding as the bottleneck; interactive metadata and persisted-result queries must achieve P95 < 1s |
| **Fault behavior** | RTSP is non-replayable and best-effort; gaps are reported, never invented. Reconnect automatically. From v0.2, durable service jobs recover after restart without losing Catalog state. |
| **Error semantics** | A single decode or inference failure produces NULL for that row and increments query error metrics. Alert when the failure rate crosses a threshold. Optional strict mode is `on_error = 'fail'`. Model false positives and false negatives are not engine errors; users manage them with explicit thresholds. |
| **Security and privacy** | Data stays in its domain by default. Pin and hash model sources. The v0.2 service adds TLS, authentication, relation-level authorization, and out-of-process Python UDFs. |
| **Compatibility** | VisionQL v0.1 has not been released, so pre-release SQL and Catalog definitions carry no compatibility guarantee. Compatibility and automatic Catalog migration begin with released versions. `EXPLAIN` text and internal metric names are not stable APIs before 1.0. |

### 3.8 Workbench

Workbench ships with `vqld` in v0.2 as a separate lightweight project under `vql-workbench/`. Its SPA and small backend connect to `vqld` as standard Arrow Flight SQL clients and use no private engine API.

Generic SQL clients can connect through JDBC or ADBC, but typically render `IMAGE` as binary and detection structs as text. Visual query development needs inline images, bounding boxes, and recent stream results. Workbench focuses on multimodal result inspection and query operations rather than general BI.

| Capability | Contract | Release |
|---|---|---|
| SQL editor and execution | VQL highlighting, Catalog-aware completion, multi-statement scripts, and history. Prepared schema metadata identifies statement type and boundedness. Interactive row limits are applied at transport, not by rewriting SQL. | v0.2 |
| Result inspection | Paginated table; inline `IMAGE` thumbnails; click-through via a locator-backed Flight ticket with reauthorization; `BOX2D` overlays; client-side confidence filtering; collapsed `VECTOR`. Original-frame lookup is available only for persistent sources. Live preview guarantees thumbnails; evidence that needs later lookup must first be persisted. | v0.2 |
| Live result preview | Rolling view of the most recent N rows from an unbounded SELECT; closing the page cancels the preview query | v0.2 |
| Catalog browser | Browse tables, streams, models, functions, and Sinks with schema and DDL | v0.2 |
| Continuous-query operations | Submit durable jobs through public SQL; show definition, state, inference volume, latency, dropped frames, and disconnections; expose `PAUSE`, `RESUME`, and `STOP` | v0.2 |
| Cost view | Read the engine's Prometheus-format endpoint and show actual GPU time and inference calls per query without requiring a Prometheus server | v0.2 |

Workbench follows three rules:

1. Every operation uses SQL or a standard protocol: `SHOW`, `SUBMIT QUERY`, `DESCRIBE QUERY`, `PAUSE`, `RESUME`, `STOP`, versioned capabilities, statement metadata, structured errors, and Prometheus-format metrics.
2. Workbench is stateless. The engine authenticates users; saved queries remain in browser storage; the process can restart or scale freely.
3. Workbench is released independently. The engine does not depend on it, and compatibility follows the SQL and Flight SQL stability policy in Section 3.7.

See the [Workbench proposal](./proposals/2026-08-05-workbench.md) for the technical design.

---

## 4. Scope and Version Boundaries

VisionQL is a query and processing engine, not a complete vertical application.

- It does not train models or provide a labeling platform. It can find and export training examples, including corner cases.
- It is not a video management system or media server. It connects to existing RTSP and object-storage systems.
- It is not an end-user security or moderation application. Scenario packages may include models, SQL templates, and dashboards, but applications remain separate.

**v0.1 is a single-node, embedded batch-and-streaming release for images, recorded video, and RTSP:**

- `IMAGE`, `VIDEO`, `BOX2D`, nested types, and `UNNEST`; `VECTOR` waits for v0.3.
- Image and video directory tables. Video is expanded by the table's declared fps.
- One `OBJECT_DETECTION` Model type called through `IMAGE_DETECTION('<model>', image, ...)`; local ONNX Runtime and remote Triton KServe V2 HTTP execution share the same typed pipeline. `CREATE FUNCTION` provides DataFusion-backed SQL expression and in-process Python UDFs.
- One RTSP source with event time, watermarks, reconnect handling, and best-effort delivery; `TUMBLE` uses a bounded allowlist of `COUNT/SUM/AVG/MIN/MAX` over persistable scalar types.
- Console Sink for foreground debugging and Kafka Sink for continuous output. Parquet and Lance arrive together in v0.3.
- Embedded pip package, SQL shell, `vql run job.sql`, and Python library with `sess.sql()`, Arrow results, notebook display, and UDF registration. Batch and streaming queries run in the foreground and stay attached to the client. The chainable DataFrame API arrives in v0.2.
- Catalog, shell history, and cache live under `VQL_HOME` (default `$HOME/.vql`). The SQLite Catalog is exactly `$VQL_HOME/catalog/vql.db`. Repository development uses `VQL_HOME=./data/.vql`; datasets live separately under `./data/datasets/`.
- Explicit frame-sampling pushdown as the first optimizer feature.

RTSP remains non-replayable and best-effort; outages and drops appear as gaps. Scenario B uses recorded-video batch output as the trusted reference for the streaming result, but both paths are required before v0.1 is complete.

**v0.2 adds the `vqld` service**, Flight SQL, TLS/authentication, relation-level authorization, durable job management and recovery, the Python DataFrame API, and Workbench.

**v0.3 adds cross-modal retrieval and persisted results:** `IMAGE_EMBEDDING(n)` / `TEXT_EMBEDDING(n)`, their fixed `IMAGE_EMBEDDING` / `TEXT_EMBEDDING` calls, `VECTOR(n)`, brute-force `<->` TopK, Parquet and Lance with native `IMAGE` and vector columns, and HNSW indexing. Parquet and Lance ship together against one output, CTAS, and logical-type recovery contract so `IMAGE` storage is designed only once.

Everything else remains intentionally undefined. Candidate directions live in the [Roadmap](../ROADMAP.md) and will be scheduled only after earlier releases produce real feedback.

**Acceptance scenarios:**

- **Scenario A — first value without external services (v0.1):** run locally in a Python host. Register an image directory, use a Python UDF to reject blurry images, call `IMAGE_DETECTION` with a local ONNX Model to select images containing a target object, and display the result in the Python session. An in-process UDF requires a notebook or REPL; `vql shell` must direct the user to a Python host. The pure-SQL first-run path in Section 3.2 runs in the shell. Both paths must produce a first result within five minutes of `pip install`.
- **Scenario B — batch/stream parity (v0.1):** start with the per-minute people-count query in Section 3.2, run it over recorded video, then point the same logic at RTSP and use `vql run` to publish to Kafka. With the same model and sample rate, assert equivalent results. Use Console Sink during debugging to inspect `UNNEST` output. Batch is the trusted reference for the streaming comparison.

Scenario A proves that first use is simple. Scenario B proves the differentiated end-to-end streaming capability.

## 5. Roadmap Summary

| Release | Theme | Core deliverables |
|---|---|---|
| **v0.1 (MVP)** | Single-node batch and streaming | Embedded pip package, SQL, CLI, image/video directory tables, one RTSP source, object detection, Python UDFs, `TUMBLE`, Console and Kafka Sinks, attached continuous execution, Acceptance Scenarios A and B |
| **v0.2** | Service and visual client | `vqld` with Flight SQL, TLS/authentication, relation-level authorization, explicit `SUBMIT QUERY`, durable jobs and recovery, Python DataFrame API, Workbench |
| **v0.3** | Cross-modal retrieval and persistence | `IMAGE_EMBEDDING(n)` / `TEXT_EMBEDDING(n)`, `VECTOR(n)`, brute-force `<->` TopK, Parquet and Lance with native multimodal columns, HNSW |

Cost optimization, clustering, multi-tenancy, and edge coordination remain future candidates. The complete delivery and acceptance plan is maintained in the [Roadmap](../ROADMAP.md).

## 6. Business Model

VisionQL will use an Apache-2.0 open-source engine to establish adoption and a common visual SQL ecosystem. The kernel, embedded and service forms, and complete SQL semantics remain open source so individuals and small teams can use the full product. Commercial offerings will focus on operating VisionQL at production scale—enterprise governance, managed services, and related needs—after the open-source releases validate product value.

The first users will be two or three design partners working with the team on one focused scenario, selected between security/campuses and content moderation. The open-source launch will include a runnable example for that scenario.

## 7. Success Metrics

**North-star metric: hours of video processed through VisionQL each week**, with batch and streaming normalized into one measure.

| Dimension | Metric |
|---|---|
| Activation | Time to first value is under 5 minutes from `pip install`, with no external service; measured through Section 3.2 and Scenario A |
| Efficiency | The standard per-minute people-count task uses fewer than 30 lines of code and goes from zero to running in under 30 minutes |
| Cost | On workloads where sampling is valid, GPU time falls approximately in proportion to the declared sample-rate reduction versus full-frame inference |
| Correctness | With the same model and sample rate, window aggregates match a hand-built baseline pipeline |
| Adoption | Within 90 days of open-source launch, at least 3 real external scenarios run end to end and at least 1 design partner carries production traffic |
| Retention | Streaming queries remain online for more than 30 days on average, and weekly active queries grow among design partners |

## 8. Open Questions

1. **Initial vertical:** security/campuses or content moderation? Security favors private deployment and has stronger willingness to pay but longer channel dependencies. Moderation is more cloud-native, has shorter buying paths, and often larger data volume. The answer determines early connector and scenario investment.
2. **SQL compatibility:** how closely should type names, functions, and error codes follow PostgreSQL conventions? The decision affects compatibility with existing tools.
3. **Confidence-aware aggregation:** should VisionQL eventually provide interval estimates or other dedicated primitives, or should users continue to set thresholds explicitly?
4. **`IMAGE` transport:** the direction is fixed—return a thumbnail plus a reference by default, never inline original bytes implicitly. The reference includes a display-only sanitized `uri` and an opaque, version-bound `locator`. Original media is fetched through a Flight ticket/DoGet and reauthorized at dereference time. Locators apply only to persistent data. Live frames guarantee thumbnails only; anything requiring later retrieval must first be written by an evidence-retention query. Before the v0.2 Flight schema freezes, Workbench, Python, and BI client testing must determine thumbnail dimensions, inline-byte limits, and locator TTL. See the [Workbench proposal](./proposals/2026-08-05-workbench.md) §3.2.

---

## Appendix: VQL Syntax

| Syntax | Category | Purpose |
|---|---|---|
| `CREATE STREAM ... FROM 'rtsp://...'` | DDL | Register a video stream |
| `CREATE TABLE ... USING IMAGES/VIDEOS` | DDL | Register a directory as a table |
| `CREATE MODEL ... TYPE ... FROM ... WITH (...)` | DDL | Register a typed inference capability with namespaced Runtime and processor options |
| `IMAGE_DETECTION('<model>', image [, named options])` | Typed inference | Call an `OBJECT_DETECTION` Model; planning resolves the first positional argument and extracts an `Inference` node |
| `CREATE FUNCTION ... RETURN <expression> / LANGUAGE PYTHON AS '<entry>'` | DDL | Register a DataFusion-backed SQL expression or batched Python function |
| `CREATE SINK` | DDL | Declare a result destination |
| `CREATE INDEX ... USING HNSW` | DDL | Add a vector index; rewrite `ORDER BY <-> LIMIT` to ANN in v0.3 |
| `TUMBLE(ts, interval)` | Time bucket | Define a tumbling window for batch or streaming `GROUP BY` |
| `UNNEST(expr) AS x` | Relational | Expand an array of detections into rows |
| `CARDINALITY(array)` | DataFusion scalar function | Count detections after `IMAGE_DETECTION` applies its named filters |
| `<->` (equivalent to `L2_DISTANCE`) | Vector | Cross-modal similarity in v0.3 |
| `SUBMIT QUERY name AS INSERT INTO ...` | Operations | Create a durable Sink job explicitly in v0.2; CLI entry point is `vql submit job.sql`; ordinary unbounded SQL stays attached |
| `SHOW/DESCRIBE QUERY / PAUSE / RESUME / STOP` | Operations | Inspect and manage durable queries |
| `EXPLAIN` | Operations | Show the query plan |

## Changelog

| Date | Change |
|---|---|
| 2026-08-15 | Merged embedded batch and streaming into v0.1 and renumbered later releases |
| 2026-08-10 | Defined type-owned inference calls, namespaced Runtime and processor options, and DataFusion-backed SQL/Python Functions |
| 2026-08-07 | Initial product design |
