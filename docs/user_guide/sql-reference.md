# VQL SQL reference

This reference describes the SQL extensions in the current source checkout. See the [Roadmap](../../ROADMAP.md) for release scope and [Installation and configuration](installation.md) for setup.

- [SQL conventions](#sql-conventions)
- [Visual and tensor types](#visual-and-tensor-types)
- [CREATE TABLE](#create-table)
- [CREATE MODEL](#create-model)
- [RESOLVE MODEL](#resolve-model)
- [ALTER MODEL](#alter-model)
- [CREATE FUNCTION](#create-function)
- [Inspect and drop Catalog objects](#inspect-and-drop-catalog-objects)
- [Functions](#functions)
- [TUMBLE and streaming queries](#tumble-and-streaming-queries)
- [Session settings](#session-settings)
- [EXPLAIN](#explain)
- [Persistent Jobs](#persistent-jobs)

## SQL conventions

VQL uses DataFusion for relational SQL and adds provider Tables, typed Models, visual functions, streaming windows, and persistent Jobs. `SELECT`, `WITH`, `VALUES`, and `INSERT INTO ... SELECT` use the same engine in the CLI, Python, and `vqld`; execution availability depends on the host as described below.

Syntax blocks use `[optional]`, `{alternative | alternative}`, and `...` as notation. SQL examples use single quotes for strings. Relational identifiers follow DataFusion rules: unquoted names fold to lowercase and double quotes preserve case. Catalog object names in VQL extension DDL normalize to lowercase even when quoted; Model version strings are case-sensitive.

Tables occupy the relation namespace. Models and Functions share a callable namespace. Persistent Jobs have a separate namespace. Unqualified names resolve in the host's active Catalog and Schema, initially `vql.default`. `VQL_*`, `__VQL_*`, and the `vql.builtin` schema are reserved.

The examples below run from the repository root after [sample setup](installation.md#sample-data-and-models). Each example states any additional prerequisites; syntax blocks are templates rather than scripts.

## Visual and tensor types

| Type | Meaning |
|---|---|
| `IMAGE` | An image or sampled frame with dimensions and media metadata |
| `VIDEO` | A video reference with duration, fps, dimensions, and codec |
| `BOX2D` | `STRUCT<x FLOAT, y FLOAT, w FLOAT, h FLOAT>`; normalized top-left-origin coordinates |
| `POINT2D` | `STRUCT<x FLOAT, y FLOAT>` used by spatial functions |
| `POLYGON` | A list of POINT2D values |
| `VECTOR(n)` | One fixed-length FLOAT32 tensor value per row at a generic Model boundary |
| `TENSOR(dtype, dims...)` | One fixed-shape tensor value per row at a generic Model boundary |
| `LOCATOR` | Optional character span or pixel-coordinate box attached to a task-function answer |

Generic tensor boundaries support `FLOAT32`, `FLOAT64`, `INT8`, `INT16`, `INT32`, `INT64`, and `UINT8` elements. `FLOAT16`, STRING tensor elements, and MODEL-valued parameters are unavailable. `AUDIO` and `MASK` are reserved types without execution support. These are typed boundaries and result representations; they do not imply every type is accepted in every SQL DDL position.

`image.width`, `image.height`, and box fields use Struct access. `BOX_CENTER(box)` or `box.center` yields a point. Results are Arrow values; the Python host materializes IMAGE bytes, while Flight SQL transports bounded JPEG thumbnails. The box from an OBJECT_DETECTION Model is normalized; a task-function `LOCATOR.box` uses pixels. Character spans use zero-based Unicode code-point offsets with an exclusive end.

## CREATE TABLE

```text
CREATE TABLE <name> [(<column> <type> [NOT NULL], ...)]
USING {IMAGES | VIDEOS | RTSP | KAFKA}
[LOCATION '<directory>']
[OPTIONS (<key> = <value>, ...)]
```

Register a provider-backed Table. IMAGES and VIDEOS require `LOCATION`; RTSP and KAFKA use endpoint options and reject `LOCATION`. An explicit column list is supported for writable providers such as KAFKA. Unknown and duplicate options fail before the definition is committed. `CREATE TABLE AS SELECT` is unavailable.

### Provider options

| Provider | Option | Default / requirement |
|---|---|---|
| IMAGES | `recursive` | `false`; recurse into subdirectories when `true` |
| VIDEOS | `recursive` | `false` |
| VIDEOS | `fps` | Omitted: native frame rate; explicit value must be greater than 0 and at most 120 |
| VIDEOS | `start_time` | Optional RFC 3339 timestamp used as the event-time origin |
| RTSP | `url` | Required RTSP endpoint; embedded credentials and query parameters are rejected |
| RTSP | `fps` | `5`; greater than 0 and at most 120 |
| RTSP | `event_time` | `'capture_time'` or `'ingest_time'`; default `'capture_time'` |
| RTSP | `watermark` | `'2 seconds'`; non-negative duration in milliseconds, seconds, or minutes |
| RTSP | `transport` | `'tcp'` or `'udp'`; default `'tcp'` |
| KAFKA | `bootstrap_servers`, `topic` | Required |
| KAFKA | `format` | `'json'`; other formats are unavailable |
| KAFKA | `credential_ref` | Optional opaque reference resolved by a host-installed SecretProvider |
| KAFKA | `delivery_timeout_ms` | `30000`; range 1–3600000 |
| KAFKA | `buffer_capacity` | `1024`; range 1–100000 |

### Readable columns

| Provider | Columns |
|---|---|
| IMAGES | `uri`, `image IMAGE`, `width`, `height`, `captured_at` |
| VIDEOS | `uri`, `ts`, `pts_ms`, `frame_id`, `frame IMAGE`, `duration`, `fps`, `width`, `height`, `codec` |
| RTSP | `ts`, `frame IMAGE`, `frame_id`, `source` |

KAFKA is an output Table. Write to it with `INSERT INTO <table> SELECT ...`. A declared output schema is checked during planning. Each output row becomes one acknowledged JSON record; partial delivery and duplicate rows after replay are possible. Secret references require embedding-host configuration; the local Compose broker uses plaintext and needs no reference.

### Example

After fetching the sample images:

```sql
CREATE TABLE sample_images
USING IMAGES
LOCATION './data/datasets/images/coco128/images'
OPTIONS (recursive = true);

SELECT uri, width, height
FROM sample_images
ORDER BY uri
LIMIT 5;
```

See the [streaming and Jobs tutorial](../../examples/README.md#rtsp-streaming-and-persistent-jobs) for RTSP input and Kafka output.

## CREATE MODEL

```text
CREATE MODEL [IF NOT EXISTS] <name>
{TYPE OBJECT_DETECTION | (<parameter> <type>, ...) RETURNS <type>}
[VERSION '<version>'] FROM '<source>'
[USING <runtime>]
[OPTIONS (<key> = <constant>, ...)]
[COMMENT '<comment>']
```

Declare a versioned callable with an immutable interface. Creation validates local declaration facts and stores an unresolved version; it performs no artifact download or service call. The initial version name defaults to `'v1'`. Call the Model only after `RESOLVE MODEL` succeeds.

`TYPE OBJECT_DETECTION` exposes `model(image IMAGE, classes => CONST ARRAY<STRING>?, min_confidence => CONST FLOAT?)`, returning `ARRAY<STRUCT<label STRING, confidence FLOAT, box BOX2D>>`. `IMAGE_CLASSIFICATION` and `TEXT_GENERATION` Model presets are unavailable. Generic signatures support embedded ONNX tensor models with IMAGE, numeric scalar, VECTOR, and TENSOR parameters.

### Sources and options

| Source | Runtime / resolution |
|---|---|
| Local `.onnx` or `file://` artifact | `ONNX_RUNTIME`; hashed in place |
| `hf://owner/repository@<commit>/model.onnx` | `ONNX_RUNTIME`; pinned artifact downloaded into the cache |
| HTTP(S) `.onnx` URL | `ONNX_RUNTIME`; requires `OPTIONS (sha256 = '<digest>')` |
| `triton+http(s)://host/model[@server_version]` | `TRITON_INFERENCE_SERVER`; validates a canonical detection service, always volatile |

Unambiguous sources infer the Runtime; otherwise `USING` is required. Flat options supply facts such as `sha256`, `input_name`, `layout`, `image_size`, `preprocess`, `format`, `labels`, `box_format`, and `output_name`. Resolution uses graph metadata and reports missing fallback facts; unknown options are rejected. Generic multi-input Models prefix processing options with the parameter name. See the [Model contract](../design/kernel.md#typed-model-contract) for the complete processing rules.

### Example

Export the detector with the [sample model commands](installation.md#sample-data-and-models), and create `sample_images` as above:

```sql
CREATE MODEL yolo TYPE OBJECT_DETECTION
FROM './data/models/yolo26n.onnx'
OPTIONS (
  image_size = 640,
  format = 'yolo_e2e',
  labels = 'coco80',
  box_format = 'xyxy'
);

RESOLVE MODEL yolo;

SELECT f.uri, det.label, det.confidence, det.box
FROM sample_images AS f,
     UNNEST(yolo(f.image,
       classes => ['person'],
       min_confidence => 0.5
     )) AS u(det)
LIMIT 5;
```

Call the Model identifier directly. Required inputs are positional row expressions. Optional semantic arguments and `version => 'v1'` follow them as named planning-time constants. The selected version is pinned for the running statement. Unknown, duplicate, or nonconstant semantic arguments fail planning. `UNNEST` turns the detection array into rows. `box` uses normalized coordinates; its fields are `x`, `y`, `w`, and `h`.

For OBJECT_DETECTION, omitted `classes` selects all classes and `min_confidence` defaults to `0.25`, within `[0,1]`. An omitted version selects the resolved default version. The result uses `confidence` and normalized `box`; the task-shaped `VQL_DETECT` result instead uses `score` and a pixel-coordinate `locator.box`.

## RESOLVE MODEL

```text
RESOLVE MODEL <name> [VERSION '<version>']
```

Resolve an existing declaration before calling it. Resolution reads local artifacts, downloads and verifies remote artifacts, or validates service metadata, then persists the version's execution contract. Local artifacts remain at their original paths; remote ONNX artifacts use the content-addressed model cache. `HF_TOKEN` authenticates private Hugging Face downloads and is never stored in the Catalog.

The bare form requires exactly one live version. With multiple versions, specify `VERSION`. Resolved versions are immutable. Resolving the initial declaration establishes its first default; added versions need an explicit `SET DEFAULT_VERSION` before becoming the default.

```sql
RESOLVE MODEL yolo;
RESOLVE MODEL yolo VERSION 'v1';
```

These statements require the `yolo` declaration from [CREATE MODEL](#create-model).

## ALTER MODEL

```text
ALTER MODEL <name> ADD VERSION [IF NOT EXISTS] '<version>'
  FROM '<source>' [USING <runtime>] [OPTIONS (<key> = <constant>, ...)]
ALTER MODEL <name> DROP VERSION '<version>'
ALTER MODEL <name> SET DEFAULT_VERSION = '<version>'
ALTER MODEL <name> SET COMMENT = '<comment>'
ALTER MODEL <name> RENAME TO <new_name>
```

Added versions inherit the existing interface and start unresolved. Resolve them explicitly before selecting them as the default. `SET DEFAULT_VERSION` accepts only a live resolved version. Dropping the default version or the last live version fails; move the default first or drop the whole Model. A dropped version name may later be reused. The case-insensitive version name `default` is reserved.

Given the resolved `yolo` above, this example reuses its artifact under a second version name:

```sql
ALTER MODEL yolo ADD VERSION 'candidate'
FROM './data/models/yolo26n.onnx'
OPTIONS (image_size = 640, format = 'yolo_e2e', labels = 'coco80', box_format = 'xyxy');
RESOLVE MODEL yolo VERSION 'candidate';
ALTER MODEL yolo SET DEFAULT_VERSION = 'candidate';
SHOW MODEL VERSIONS yolo;
```

## CREATE FUNCTION

```text
CREATE FUNCTION <name>([<parameter_name>] <type>, ...)
  [RETURNS <type>] RETURN <expression>
CREATE FUNCTION <name>([<parameter_name>] <type>, ...)
  RETURNS <type> LANGUAGE PYTHON AS '<module>:<function>'
```

SQL expression Functions persist in the Catalog and expand during planning. Positional parameters can be referenced as `$1`, `$2`, and so on. `RETURNS` may be omitted when the body type can be derived. A Function can wrap a Model call while preserving visible inference in the plan. Parameters used in a Model's constant-only argument positions must also be constant at the Function call site.

```sql
CREATE FUNCTION add_one(INT) RETURNS INT RETURN $1 + 1;
SELECT add_one(41) AS answer;
```

With the resolved `yolo` Model from [CREATE MODEL](#create-model):

```sql
CREATE FUNCTION detect_people(IMAGE)
RETURN yolo($1, classes => ['person'], min_confidence => 0.5);

SELECT uri, detect_people(image) AS detections
FROM sample_images
LIMIT 5;
```

Python Functions require an explicit `RETURNS` type and a Python host. The callable receives Arrow arrays in batches and returns one equal-length compatible Arrow array. Declarations can be inspected through other hosts, but execution in the CLI, `vqld`, or Workbench returns `PYTHON_HOST_REQUIRED`. See [Python installation](installation.md#python-api) and the [Python example](../../examples/python/image_filtering.py). `OR REPLACE`, `TEMPORARY`, aggregate Functions, and table Functions are unavailable.

## Inspect and drop Catalog objects

```text
SHOW {TABLES | MODELS | FUNCTIONS}
SHOW MODEL VERSIONS <name>
DESCRIBE [TABLE | MODEL | FUNCTION] <name>
DESC [TABLE | MODEL | FUNCTION] <name>
SHOW CREATE {TABLE | FUNCTION} <name>
SHOW CREATE MODEL <name> [VERSION '<version>']
DROP {TABLE | MODEL | FUNCTION} <name>
```

`SHOW` lists objects in the active namespace. `DESCRIBE` inspects a Table schema or callable interface; omitting the kind means TABLE. `SHOW CREATE` returns sanitized canonical declaration SQL, with credential references redacted. `DROP` removes the object from new planning snapshots; it does not delete source media or model artifacts.

Bare `SHOW CREATE MODEL` selects the most recently added live version, independently of the default. `VERSION` selects an exact case-sensitive version. Both return `object_name`, `object_type`, `create_sql`, and `version`, and include an explicit `VERSION` in the declaration. Missing Models or versions return `NOT_FOUND`. Resolution state and the default marker are shown by `SHOW MODEL VERSIONS`, rather than replayed by `SHOW CREATE`.

```sql
SHOW TABLES;
DESCRIBE TABLE sample_images;
SHOW CREATE TABLE sample_images;
SHOW MODELS;
DESCRIBE MODEL yolo;
SHOW MODEL VERSIONS yolo;
SHOW CREATE MODEL yolo VERSION 'v1';
```

These examples require the Table and Model created in the preceding sections.

## Functions

The [built-in function reference](sql-functions.md) covers AI classification, detection and extraction, spatial operations, and `TUMBLE`, including public syntax, arguments, result types, availability, and SQL examples.

Built-in AI functions do not require a Catalog Model. Install their artifacts using [sample setup](installation.md#sample-data-and-models). Catalog Models and user-defined Functions use the declarations above.

## TUMBLE and streaming queries

Use [TUMBLE](sql-functions.md#tumble) to group timestamps into event-time buckets.

Bounded input uses ordinary aggregation. On RTSP input, a grouped `TUMBLE` aggregation emits a window only after the watermark reaches its end. Supported continuous aggregates are `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX` over scalar inputs and group keys. Continuous plans accept projection, filtering, `UNNEST`, scalar functions, typed inference, and one TUMBLE aggregation; unsupported shapes, DISTINCT, and media-valued aggregate state fail planning. RTSP delivery is best-effort with no replay.

Run an unbounded SELECT as an attached stream in Workbench or incrementally in the shell. Cancel the active execution before starting another statement in the same Session. In embedded CLI execution, the first Ctrl-C drains admitted work and flushes an attached write; a second cancels immediately. A remote shell cancels its active Flight execution. An unbounded statement must be last in a `vql run` script. Use [persistent Jobs](#persistent-jobs) when a continuous write must survive client disconnection.

## Session settings

```sql
SET vql.on_error = 'null';
SET vql.on_error = 'fail';
```

`'null'` is the default: row-level processing failures yield NULL. `'fail'` turns them into hard query errors. SQL NULL input and successful empty detections remain ordinary results. Unsupported settings or values return `INVALID_OPTION`; this setting does not hide SQL, type, declaration, or resolution errors.

Configuration and resource limits belong to the host's [runtime configuration](installation.md#runtime-configuration), rather than SQL SET statements.

## EXPLAIN

```text
EXPLAIN <query>
EXPLAIN INSERT INTO <table> <query>
```

Inspect the plan without opening data sources, resolving artifacts, or contacting inference services. VQL annotations include bounded or continuous mode, source pushdowns, stream topology, inference semantics, and Table-write placement. Models referenced by the query must already be resolved. `EXPLAIN ANALYZE` is rejected because it would execute the query.

With the `sample_images` Table:

```sql
EXPLAIN SELECT uri, width, height FROM sample_images LIMIT 5;
```

Hard failures use `[VQL-CCDDD] SYMBOL: message`, for example `[VQL-42001] INVALID_SQL: expected a statement`. Python raises `visionql.VisionQLError` with `code`, `symbol`, `message`, and `target_version` attributes. See the [error registry](../design/error_codes.md) for stable identifiers.

## Persistent Jobs

```text
SUBMIT JOB <name> AS INSERT INTO <writable_table> SELECT ...
SHOW JOBS
DESCRIBE JOB '<job_id>'
STOP JOB '<job_id>'
```

These statements require `vqld`, through a remote shell, Workbench, or another Flight SQL client. A persistent Job contains exactly one unbounded `INSERT INTO ... SELECT` targeting a writable Table. Submission returns a bounded acknowledgement with the generated `job_id`; use that ID for inspect and Stop operations. The name must be unique among non-terminal Jobs in its namespace.

Jobs survive client disconnection. After daemon restart, active RTSP Jobs resume at the current live position with fresh window state and a reported restart gap. There is no replay or exactly-once guarantee. Definitions are immutable: loading a Job's SQL creates a separate draft, and resubmitting creates a new Job. Redacted literals must be replaced before reusing displayed SQL. `PAUSE JOB` and `RESUME JOB` are unavailable.

See the complete [RTSP and Kafka example](../../examples/README.md#rtsp-streaming-and-persistent-jobs), including source and output Table setup. The [service design](../design/vqld.md#persistent-job-sql) defines the result fields and lifecycle states.
