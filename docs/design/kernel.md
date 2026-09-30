# VisionQL Kernel Design

> This document defines the `vql-kernel` planning and execution contracts. System boundaries are defined by the [High-Level Design](../high_level_design.md); Catalog persistence, host behavior, and verification policy are defined by their dedicated design documents.

## Kernel Boundary

`vql-kernel` exposes `Engine`, `Session`, configuration, statements, query results, cancellation, and host-injection traits. It owns no process signals, terminal behavior, PyO3 objects, network listeners, or global singleton state.

The host supplies `EngineConfig`, a secret provider, and an optional Python UDF host. The kernel owns the SQL entry point and all semantics below it.

## Logical Planning and Physical Compilation

### DataFusion Logical Plan and VQL Metadata

Standard relational work uses DataFusion `LogicalPlan` nodes. VisionQL adds an extension node only when model inference or a provider-specific table write cannot be represented faithfully by a standard node.

| Extension node | Plan text | Logical behavior | Physical implementation |
|---|---|---|---|
| Inference | `InferenceNode` | Append a model result column to an input relation | [`InferenceExec`](#inferenceexec-scheduling-and-failure) |
| Table write | `SinkWrite` | Internal write node for a writable Catalog Table | `SinkExec` or streaming Kafka writer |

Those four names are part of the contract: plan-shape tests assert on `InferenceNode`, `InferenceExec`, `SinkWrite`, and `SinkExec` in `EXPLAIN` text.

A video table expands into frames inside the `USING VIDEOS` scan at the fps declared by the table. It is not a separate logical node or table-valued function.

`TUMBLE` is a VisionQL time-bucket UDF represented as an ordinary scalar expression plus a DataFusion `Aggregate`. For a continuous query, planning extracts the aggregate's inputs and output template into a side `TumblePlan`; it does not add a `TumbleAggregate` extension node.

The planned statement keeps the DataFusion plan together with only the metadata required by attached execution:

```text
PlannedStatement {
  dataframe: DataFrame,           // the planned logical template
  stream_name: Option<String>,    // the single RTSP relation, when the plan is unbounded
  stream_skip: usize,             // top-level OFFSET, stripped from the template
  stream_fetch: Option<usize>,    // top-level LIMIT, stripped from the template
  tumble: Option<TumblePlan>,
}
```

A top-level `LIMIT`/`OFFSET` over a stream is removed from the template and applied by the coordinator, so a bounded prefix of an unbounded query runs the same plan as the continuous form.

### Planning Pipeline

```text
SQL
  → one Catalog definition snapshot
  → syntax normalization against that snapshot
  → DataFusion SQL planning (names, types, Functions, UDFs)
  → streamability validation
  → top-level LIMIT/OFFSET extraction for a stream
  → inference extraction and deduplication
  → EXPLAIN annotation
  → TumblePlan extraction for a stream
  → DataFusion optimization and physical planning at execution time
  → bounded execution or attached epoch execution
```

Streamability is validated before inference extraction, so an unsupported continuous shape is rejected without resolving Models. DataFusion's own optimizer and the providers' `scan` pushdown run later, when the planned `DataFrame` is compiled — for a continuous query, once per epoch.

Planning reads only the Catalog and lightweight metadata. Object listing, model download, service metadata validation, video probing, and network connection never happen implicitly during planning: model materialization belongs exclusively to `RESOLVE MODEL`, while ordinary execution opens only the already-resolved binding. `EXPLAIN` and completion cannot trigger expensive I/O.

### Immutable Query Definition Snapshot

Planning opens one Catalog transaction and constructs a `DefinitionSnapshot` containing the current Tables, Models, and Functions in `vql.default`. RTSP and Kafka definitions are Tables with provider capabilities. A query-specific DataFusion session is populated from that snapshot. Resolved Model specifications are copied into `InferenceNode` extension nodes, while selected provider configurations are copied into the attached result handle or internal write target.

The planned `DataFrame` and those copied specifications are the execution source of truth. Replacing or dropping a Catalog definition affects newly planned queries but does not replan a running query. Opaque Catalog generations support exact historical snapshot lookup, but the Kernel owns no user-facing revision lifecycle, persistent Query identity, Manifest store, lease manager, or Manifest garbage collector.

A persistent Query object stored by `vql-catalog` contains normalized SQL, semantic settings, and opaque Catalog generations. On restart, `vqld` asks the Kernel to prepare against a snapshot loaded from those generations. The Query does not enter `DefinitionSnapshot` and is not a serialized kernel plan or a prerequisite for foreground embedded execution.

### Allowlist for Unbounded Plans

VisionQL validates unbounded plans against an allowlist instead of assuming an arbitrary DataFusion plan can run forever.

Allowed shapes:

- one RTSP provider table;
- Projection, Filter, `UNNEST`, built-in scalar functions, and `InferenceNode`;
- at most one `TUMBLE` aggregate;
- a stateless SELECT preview or one Kafka table write;
- Projection, Filter, and one table write after the window.

Rejected shapes:

| Plan shape | Why it is unsafe | Suggested rewrite |
|---|---|---|
| Global or grouped aggregate without a window | Input never ends | Add `TUMBLE` |
| Unbounded `ORDER BY` / TopK | Requires unbounded state or end-of-input | Bound it by a window or use a batch query |
| Unbounded `DISTINCT` | State cannot be reclaimed | Use an allowlisted aggregate or a batch query |
| JOIN or multi-source UNION | Multi-source watermark and consistency are undefined here | Split into independent queries |
| `OVER` analytic window | No bounded-state rule exists | Use a time-window aggregate |
| Aggregate or UDAF outside the streaming allowlist | Bounded memory and media lifetime cannot be guaranteed | Choose a supported aggregate or batch execution |
| More than one scan, or a scan that is not the single RTSP table | Multi-source progress is undefined here | Split the work into independent queries |
| `LIMIT` / `OFFSET` below the top level | The coordinator can only apply one bounded prefix | Move the limit to the outermost query |
| `EXPLAIN ANALYZE` | `EXPLAIN` is side-effect free and must not start a continuous query | Use `EXPLAIN` without `ANALYZE` |

A top-level `LIMIT` is not a rejection: it turns the statement into a bounded prefix of the stream and `EXPLAIN` reports `mode=bounded`.

[`TUMBLE` State](#tumble-state) defines the aggregate and type allowlist. A validation error must identify the first unsupported node or aggregate, point to its SQL fragment, and offer a viable rewrite; a raw DataFusion error is not sufficient.

---

## Epoch-Based Streaming

### Why Epochs

Streaming input enters the engine as short micro-batches. The RTSP source closes an epoch when its event-time span reaches 200 ms or it contains 64 sampled rows, whichever happens first; at most four closed epochs are buffered toward the coordinator. An epoch carries data and control state separately:

```rust
struct StreamEpoch {
    epoch_id: u64,
    batches: Vec<RecordBatch>,
    watermark_ms: Option<i64>,
    frame_lease: Option<FrameBufferLease>,
}
```

Event time is UTC milliseconds throughout, so the watermark is a plain `i64`.

`batches` may be empty. None of the other fields is encoded as a hidden row, so a Filter that removes every row still cannot stall source progress, watermarks, or frame-buffer reclamation.

### Epoch Execution Order

```mermaid
sequenceDiagram
    participant S as Stream source
    participant C as Epoch coordinator
    participant D as DataFusion fragment
    participant W as TUMBLE state
    participant K as Table writer

    S->>C: StreamEpoch(data, progress, watermark, frame lease)
    C->>D: bind epoch batches and run bounded fragment
    D-->>C: filtered and inferred RecordBatches
    C->>W: apply data for this epoch
    C->>W: advance watermark after data completes
    W-->>C: results for closed windows
    C->>K: write and await acknowledgement
    K-->>C: ack
    C->>C: mark epoch complete
    C->>S: release frame lease / advance committable progress
```

The sequence is strict:

1. Epochs from one source are applied serially by `epoch_id`; decode, preprocessing, and inference may still run in parallel within the data fragment.
2. The coordinator passes a watermark to stateful operators only after every output for that epoch has completed.
3. An epoch completes only after state updates and all newly closed-window writes have been acknowledged by the Table writer.
4. Cancellation stops the active DataFusion stream, model requests, and Table writes before releasing the frame lease.
5. This design has one source and one partition, so it does not merge watermark frontiers.

The coordinator retains the planned DataFusion logical template, not a compiled `ExecutionPlan` or a separate epoch-plan abstraction. For each epoch it:

1. replaces the stream `TableScan` with a single-partition `MemTable` containing that epoch's batches;
2. calls DataFusion execution on the rebound `DataFrame`, which builds a fresh physical tree;
3. drains the bounded fragment before advancing the watermark;
4. for `TUMBLE`, applies projected aggregate inputs to process-local `TumbleState`, then binds closed-window rows into the planned output template;
5. waits for output or writable-table acknowledgement before releasing the frame lease.

Stateful window data and the epoch control plane remain outside DataFusion. No `EpochPlanTemplate`, `EpochInputExec`, `reset_state`, or physical-plan reuse API is required.

### Backpressure and Frame Loss

Backpressure travels upstream from `Table writer → state → data fragment → source buffer`. Every buffer has a hard capacity.

- Replayable sources wait when capacity is unavailable.
- When the RTSP source-to-coordinator queue is full, sampled frames that have not entered an epoch may be dropped and recorded as `source_overrun` with their count and range.
- Once a row enters an epoch, overload cannot discard it silently. Exhausting the budget fails the query.
- Downstream bounded queues propagate backpressure rather than inventing additional drop policies.

### `TUMBLE` State

In streaming mode, `TumbleState` stores process-local scalar and Arrow-compatible state rather than retaining DataFusion `Accumulator` instances:

```text
key = (window_start, group_key)
value = aggregate_states
```

- Windows are `[start, end)`. Time is UTC milliseconds anchored at the Unix epoch.
- The interval is a positive fixed duration; calendar intervals are unsupported. A streaming `TUMBLE` must group by the RTSP event-time column `ts` directly, and that column must be a non-null `TIMESTAMP(ms)`. A nullable or derived event-time expression must be filtered or materialized into a bounded query first.
- Exactly one `TUMBLE` expression may appear in `GROUP BY`, and the plan may contain at most one aggregate node. Every additional group key must be a persistable scalar type.
- `DISTINCT`, `FILTER`, `ORDER BY`, and explicit NULL treatment inside a streaming aggregate are rejected; filter rows before the aggregate instead.
- The extracted `TumblePlan` defines aggregate inputs and output expressions. An accumulator is temporary: merge the previous process-local state, process one epoch, call `state()`, and discard it.
- Advance the watermark after data processing. Emit and delete a window when `window_end <= watermark`.
- Rows where `event_time < current_watermark` are dropped and increment the query's `late_rows` counter. `allowed_lateness` is not supported.
- Stopping a query does not emit windows that have not closed.
- State and group keys cannot contain `buffer_id` or `buffer_slot`. Convert media to a persistent locator or encoded value first. `IMAGE`, `VIDEO`, and `TENSOR` are rejected in window state; materialize a bounded result before aggregating them.
- Batch mode lowers `TUMBLE` to time bucketing and ordinary aggregation. Differential batch/stream tests cover NULL, grouping, overflow, and final values for each allowlisted aggregate.

Window state is charged to the Session memory pool through its own `MemoryConsumer` and additionally capped at 64 MiB, or at the Session limit when that is smaller. Exceeding the cap fails the query rather than spilling.

State lives only for the process lifetime and is not serialized or restored after restart. A persistent v0.2 Query restarts with empty `TumbleState` at the current live-source position and reports the discarded open windows as part of its restart gap.

The streaming aggregate allowlist is:

- `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX` over persistable scalar Arrow inputs and group keys;
- no `DISTINCT`, `ARRAY_AGG`, `STRING_AGG`, approximate aggregates, ordered aggregates, UDAFs, or aggregation over `IMAGE`, `VIDEO`, Binary, or complex values containing process-local media slots.

---

## Multimodal Types and Media Lifetime

### Arrow Representation

VQL logical types use standard Arrow storage and field metadata.

| VQL type | Arrow storage type | Contract |
|---|---|---|
| `IMAGE` | `Struct`, defined in [Three `IMAGE` Payload Forms](#three-image-payload-forms) | `ARROW:extension:name=vql.image` |
| `VIDEO` | `Struct<uri, locator, duration_ns, fps, width, height, codec>` | `uri` is display-only; `locator` is used for reauthorized reads; a full video is never inlined |
| `BOX2D` | `Struct<x: Float32, y: Float32, w: Float32, h: Float32>` | Top-left origin and normalized `[0,1]` coordinates |
| `POINT2D` | `Struct<x: Float32, y: Float32>` | Internal logical type for spatial functions |
| `POLYGON` | `List<POINT2D>` | Normalized two-dimensional polygons only |
| `VECTOR(n)` / `TENSOR(dtype, dims...)` | Non-nullable `FixedSizeList` with `arrow.fixed_shape_tensor` extension metadata | One fixed-shape value per row; the Field records element dtype, shape, and optional dimension names |
| Detection result | `List<Struct<label: Utf8, confidence: Float32, box: BOX2D>>` | One list per frame; `UNNEST` produces rows |
| `LOCATOR` | `Struct<char_span: Struct<start: Int32, end: Int32>?, box: BOX2D?>` | Task-function provenance. `char_span` uses zero-based Unicode code-point offsets with an exclusive end; an IMAGE locator's box uses pixel coordinates with a top-left origin |
| Task detection result | `List<Struct<label: Utf8, score: Float32, locator: LOCATOR?>>` | Sorted by descending score; `UNNEST` produces instances |
| `AUDIO` / `MASK` | Reserved logical types | Registration and execution return an unsupported-feature error |

Every `IMAGE` field carries `ARROW:extension:name=vql.image` and `ARROW:extension:metadata={"version":1}`. An unaware client still sees a standard Arrow Struct.

### Three `IMAGE` Payload Forms

```text
IMAGE storage := Struct {
  uri: Utf8?,                 # sanitized, display-only; never used to read or authorize
  locator: Utf8?,             # versioned vql:// media locator
  pts_ms: Int64?,             # frame time within a video; NULL for an image
  frame_id: UInt64?,          # frame identity in a live ring buffer
  encoded: Binary?,           # JPEG/PNG or thumbnail bytes
  encoding: Utf8?,
  width: Int32?,
  height: Int32?,
  buffer_id: UInt64?,         # process-local only
  buffer_slot: UInt32?        # process-local only
}
```

| Form | Valid fields | Where it is used |
|---|---|---|
| Reference | `uri`, `locator`, `pts_ms`, metadata; `locator` is non-null | Table scans, video expansion, and most operator boundaries |
| Frame buffer | `buffer_id`, `buffer_slot`, metadata | Within one epoch, between decoding and pixel consumers |
| Encoded | `encoded`, `encoding`, metadata | Python and other process boundaries, including explicit Kafka output |

Invariants:

1. `buffer_id` and `buffer_slot` never cross a process boundary, reach persistent storage, or enter the Catalog.
2. `uri` is sanitized and display-only. It may be logged or exported but is never used by the runtime to read media.
3. `locator` is an opaque `vql://media/v1/...` value bound to an internal source generation and frame coordinates. The generation is not a Catalog revision exposed through SQL. Resolution accepts only registered sources and reauthorizes as the current caller.
4. A live RTSP frame is not replayable and has no durable `locator`. Encode or persist it before later retrieval.
5. Field metadata distinguishes original encoded bytes from thumbnail bytes.

### Epoch Frame Buffer

Sampled RTSP frames enter the current epoch's `FrameBuffer`; the RecordBatch carries only the slot. The coordinator releases `FrameBufferLease` only after the data fragment, state handling, and egress encoding have all completed.

The lease is independent of row survival:

- filtering one row or the whole batch does not leak a frame;
- asynchronous inference retains the lease until it completes;
- cancellation stops consumers before releasing the epoch;
- frame-buffer references never survive into another epoch or a window state.

Batch video normally fuses read, decode, and preprocessing inside `InferenceExec`. A short-lived frame buffer is needed only when several pixel consumers share one frame.

### NULL and Row-Level Failure

- Decode failure leaves the media reference and metadata intact, but any pixel-dependent result is NULL.
- Inference failure makes the model result NULL while preserving input columns.
- `SET vql.on_error = 'fail'` terminates on the first row-level failure.

---

## SQL, Models, and Functions

### Parser Boundary

VQL reuses the sqlparser-rs tokenizer and DataFusion SQL AST, with a dedicated parser only for VisionQL extensions:

1. Split a complete script while respecting strings, comments, and quoted identifiers.
2. Send provider-table DDL, `CREATE MODEL`, `RESOLVE MODEL`, and VisionQL operational statements to the VQL DDL parser.
3. Send SELECT, INSERT, standard DDL, and `CREATE FUNCTION` through the DataFusion-supported grammar. A VisionQL `FunctionFactory` validates and persists supported Function definitions.
4. Normalize constructs such as `.center`, `TUMBLE`, and typed inference markers at the AST or logical-plan layer.
5. Pass normalized relational expressions to the DataFusion planner interface.

Extension statements cannot rely only on a `Dialect` hook; the VQL parser needs golden tests. Catalog object names in VQL extension statements are case-insensitive and normalize to lowercase even when quoted. Other relational identifiers follow DataFusion SQL rules: unquoted identifiers fold to lowercase, double-quoted identifiers preserve case, and string literals use single quotes.

| Statement | Behavior |
|---|---|
| `CREATE TABLE ... USING IMAGES/VIDEOS/RTSP` | Create a readable provider table as defined by [Table Providers](#table-providers) |
| `CREATE TABLE ... USING KAFKA` | Create a writable Kafka table as defined by [Kafka Table](#kafka-table) |
| `CREATE MODEL ... { TYPE ... \| (...) RETURNS ... } FROM ... [USING ...] [OPTIONS (...)]` | Store an unresolved Model with its immutable interface and first named version, without network I/O |
| `ALTER MODEL ... ADD\|DROP VERSION`, `SET DEFAULT_VERSION`, `SET COMMENT`, `RENAME TO` | Mutate the versioned Model aggregate through Catalog compare-and-swap |
| `RESOLVE MODEL <name> [VERSION '...']` | Introspect and pin one immutable version; bare form requires exactly one live version |
| `CREATE FUNCTION ... RETURN <expression>` | Create a DataFusion-backed SQL expression function |
| `CREATE FUNCTION ... LANGUAGE PYTHON AS 'module:function'` | Create a batched Python function; executable only from a Python host |
| `SET vql.on_error = 'null' \| 'fail'` | Switch row-level failures between NULL results and hard errors for this Session, returning the new value as a one-column result |

`DROP`, `SHOW`, `DESCRIBE`, and `SHOW CREATE` use the same VQL DDL path. `SHOW CREATE` must be sanitized and parseable. Statements outside the [documented scope](../high_level_design.md#scope) fail without registering placeholders.


### Typed Model Contract

A MODEL is a versioned callable backed by an artifact or endpoint. Capability form expands `TYPE` into a persisted interface; explicit-signature form declares tensor-mappable parameters and return type directly. The interface is model-level and immutable.

| Model `TYPE` | Callable interface | Canonical result |
|---|---|---|
| `OBJECT_DETECTION` | `model(image IMAGE, classes => CONST ARRAY<STRING>?, min_confidence => CONST FLOAT?)` | `ARRAY<STRUCT<label STRING, confidence FLOAT, box BOX2D>>` |

`IMAGE_CLASSIFICATION` and text-generation Model presets remain unscheduled. Generic Model signatures expose `IMAGE`, numeric scalars, `VECTOR(n)`, `TENSOR(dtype, dims...)`, and structured tensor output without introducing generic selector functions. Supported tensor elements are `FLOAT32`, `FLOAT64`, `INT8`, `INT16`, `INT32`, `INT64`, and `UINT8`; `STRING`, `FLOAT16`, and `MODEL` are rejected at an embedded generic boundary. The release-managed `VQL_CLASSIFY`, `VQL_EXTRACT`, and `VQL_DETECT` contracts are specified separately under [Built-in Functions](#built-in-functions).

Generic signatures use embedded ONNX Runtime. `RESOLVE MODEL` binds declared parameters to graph inputs positionally unless `<parameter>.input_name` overrides the binding, matches structured outputs by field name, validates one dynamic leading batch axis plus static per-row shapes, and persists the resolved tensor contracts. Numeric scalar parameters map only to graph inputs shaped `[N]`. An `IMAGE` parameter requires exactly one processing form: either the `imagenet` preset or the complete inline set `mean`, `std`, `scale`, `resize`, and `pad_value`; incomplete inline processing or mixing the two forms fails resolution. Multi-input options use a parameter-name prefix such as `image.preprocess`, while a single-input Model may use flat keys. Execution compacts rows for which every argument is non-NULL, preprocesses IMAGE inputs, runs all graph inputs and outputs together, and scatters results back so any row with a NULL or failed argument remains NULL.

One source bundle may be registered under multiple compatible Model interfaces; artifact-cache or Runtime-session reuse is an internal optimization.

The Model identifier is the call target. Required domain inputs are positional; semantic arguments and the reserved `version =>` selector are named constants:

```sql
SELECT yolo(image,
  classes => ['person'],
  min_confidence => 0.5
)
FROM photos;
```

Planning enforces these rules:

- The call target resolves from the statement's immutable definition snapshot and the selected version is copied into `InferenceNode`; Model identity never enters an Arrow batch.
- Required positional parameters may be row expressions and are type-checked against the persisted interface.
- Optional interface-owned semantic arguments such as `classes`, thresholds, and generation controls use `name => constant` notation after all positional arguments. The VQL normalizer binds them against the persisted interface, fills omitted defaults, and emits a fully ordered marker before DataFusion type planning.
- Every Model registers a volatile typed marker so DataFusion cannot fold or CSE it before extraction. Per-invocation deduplication requires an immutable version, a deterministic resolved interface, and deterministic inputs.
- Unknown, duplicate, nonconstant semantic arguments, nonconstant versions, and unknown versions fail planning.

Model declarations use one flat, deny-unknown option surface:

```text
CREATE MODEL identifier
  { TYPE capability | (parameter type [, ...]) RETURNS type }
  [VERSION 'version'] FROM string_literal
  [USING runtime_identifier]
  [OPTIONS (option = constant_value [, ...])]
```

`.onnx` and `mock://` infer `ONNX_RUNTIME`; `triton+http(s)://host/model[@server_version]` infers `TRITON_INFERENCE_SERVER`. Other sources require `USING`. Flat deny-unknown `OPTIONS` are classified as artifact, Runtime, input, or output facts. ONNX resolution discovers tensor names, layout, static image size, output convention, and labels from graph structure and metadata; when a fact is undecidable, the error names the exact fallback option.

`CREATE MODEL` is deliberately fast. It validates only facts available locally—the `TYPE`/Runtime pairing, source shape, option schema, and embedded processor options—and commits an unresolved definition without downloading or contacting a service. `RESOLVE MODEL` is the explicit potentially slow operation. For an embedded artifact it resolves the source, streams remote bytes to a temporary file, checks cancellation and checksum while downloading, atomically installs a content-addressed cache entry, and persists the resolved path/hash. For a service it contacts the endpoint, validates the typed service contract, and persists the binding. A query cannot plan against an unresolved Model.

Resolved versions are immutable. Version names are case-sensitive string identities; `default` is reserved case-insensitively. The first version is `v1` unless named explicitly; successful resolution of that exact initial declaration establishes the first default. Dropping and reusing its name does not recreate that publication right, and added versions never auto-publish. A service-backed version is always volatile because the service can change weights behind routing metadata.

`ALTER MODEL ... ADD VERSION` inherits the persisted interface and requires a new explicit version name. `SET DEFAULT_VERSION` accepts only a live resolved version. Dropping the default or last live version fails with guidance to move the default or use `DROP MODEL`; dropping another version removes it from new snapshots and permits a later explicit reuse of its name. Bare `RESOLVE MODEL` is accepted only when exactly one live version exists, so concurrent additions cannot redirect an operator's request.

`SHOW MODELS` exposes aggregate identity, interface, live version count, default, and comment. `SHOW MODEL VERSIONS` exposes per-version state, volatility, fingerprint, creation time, and default marker. `DESCRIBE MODEL` renders the callable interface.

`SHOW CREATE MODEL <name> [VERSION '<version>']` returns one sanitized, independently parseable `CREATE MODEL` definition with the persisted interface, explicit version name (including `v1`), source, Runtime, options, and current aggregate comment. The bare form selects the most recently added live version in Catalog insertion order, independently of version-name ordering, resolution status, and the default publication pointer. Dropping that version exposes the preceding live version; reusing a dropped name adds a new latest version. The explicit form matches a case-sensitive version identity and returns `NOT_FOUND` if the Model or version does not exist. Both forms use the statement's Catalog snapshot, including prepared statements. The result fields are `object_name`, `object_type`, `create_sql`, and `version`, all non-null UTF-8 strings. Table and Function `SHOW CREATE` retain their existing three-field results. Model DDL describes the selected declaration; resolved runtime state and default publication remain visible through `SHOW MODEL VERSIONS` and are not replayed by the returned `CREATE MODEL`.

The declaration fingerprint includes the persisted interface, version name, source, Runtime, and user options. The resolved semantic fingerprint additionally includes the resolved source/hash and derived execution contract. Scheduler configuration does not enter semantic identity.

### User-defined Functions and DataFusion Reuse

`CREATE FUNCTION` supports only genuine user-defined computation:

| Syntax | Planning and execution |
|---|---|
| `CREATE FUNCTION ... RETURN <expression>` | Persist a normalized SQL expression function and expand it hygienically during planning with a recursion-depth check |
| `CREATE FUNCTION ... LANGUAGE PYTHON AS 'module:function'` | Persist a batched Arrow ABI; executable only from a Python host |

The statement router uses DataFusion's PostgreSQL-style function grammar, `CreateFunction` representation, named-argument support, and UDF registry. A VisionQL `FunctionFactory` validates the supported language or body, constructs the UDF, and persists the normalized definition. Planning recreates equivalent DataFusion UDFs from the definition snapshot, so session-local registration is never durable state.

Python functions require an explicit `RETURNS` type. SQL expression functions may omit it when DataFusion can derive the body type from positional parameter types and registered built-ins. This permits a compact inference preset such as `CREATE FUNCTION detect_people(IMAGE) RETURN yolo($1, classes => ['person'])`; macro expansion still exposes the typed inference marker to the planner.

Function creation expands the body once and records parameters that flow into a Model's constant-only semantic or version position. Nested wrappers propagate the same constraint. Call sites reject nonconstant values before expansion with the parameter name and position; `SHOW FUNCTIONS` and `DESCRIBE FUNCTION` render inferred parameters with the display-only `CONST` marker.

Model references in SQL functions are late-bound at body expansion. Dropping a Model does not cascade to Functions; a later call fails planning if the referenced Model is absent or unresolved, and `DESCRIBE FUNCTION` reports the reference status. `MODEL` is not a SQL parameter type because Model identity is legal only in call position.

A Python function is invoked through the injected host as one Arrow array per argument, and its result must be an equal-length, type-compatible array. `IMAGE` is materialized into encoded form before it crosses the boundary. Row-at-a-time callbacks are not supported, and a Python function reached without an installed host fails with `PYTHON_HOST_REQUIRED`. The [Python Binding Design](./python_binding.md#python-udf-host) defines the Python-side contract.

Model inference uses a defensive typed `ScalarUDF` marker only as a DataFusion planning bridge; execution always extracts it into `InferenceNode`. A SQL expression function may wrap a Model call, and expansion exposes the same marker.

### Syntax Normalization

| VQL form | Normalized plan form |
|---|---|
| `box.center` | `BOX_CENTER(box)` |
| `TUMBLE(ts, interval)` | VisionQL time-bucket scalar expression and DataFusion Aggregate; continuous planning also extracts a side `TumblePlan` |
| `FROM t, UNNEST(expr)` | Native DataFusion unnest node; the only row-expansion mechanism |
| `CREATE ...` | Catalog or runtime operation, absent from the relational plan |

Inference-call parameters such as `classes` and `min_confidence` are owned by the persisted Model interface and filter elements within one detection result. They are not processor DDL options and are not converted into a row-level Filter that could discard the frame.

### Built-in Functions

Public generic inference selectors are absent. Model names remain direct call targets. The v0.1 release-managed AI surface contains exactly `VQL_CLASSIFY`, `VQL_EXTRACT`, and `VQL_DETECT`; neither Models nor Functions may claim any `VQL_*` name, and `__VQL_*` is reserved for internal planning markers. A function name fixes one task shape and one return schema across modalities. Every argument except `input` is a planning-time constant; required domain arguments remain positional and optional arguments use `name => value`.

| Function | Signature | Contract |
|---|---|---|
| `VQL_CLASSIFY` | `(input IMAGE\|STRING, categories CONST ARRAY<STRING> [, output_mode => CONST STRING, min_score => CONST FLOAT]) -> ARRAY<STRUCT<label STRING, score FLOAT>>` | `categories` is non-empty and distinct. `single` is the default and returns exactly the highest-scoring requested category; `multi` returns categories at or above `min_score`, default `0.25`. IMAGE uses the installed YOLO26n-cls classifier; STRING returns `FEATURE_NOT_AVAILABLE` with target `未排期` |
| `VQL_EXTRACT` | `(input IMAGE\|STRING, fields CONST MAP<STRING, STRUCT<question STRING, list BOOLEAN>>) -> STRUCT<requested fields>` | The ordered constant map derives one lowercase result field per request. A scalar field returns `STRUCT<value STRING?, score FLOAT?, locator LOCATOR?>`; `list = true` returns an array of that answer struct. A bare STRING descriptor is shorthand for `STRUCT(question => value, list => false)`. Both overloads return `FEATURE_NOT_AVAILABLE` with target `未排期` |
| `VQL_DETECT` | `(input IMAGE [, classes => CONST ARRAY<STRING>, min_score => CONST FLOAT]) -> ARRAY<STRUCT<label STRING, score FLOAT, locator LOCATOR?>>` | IMAGE execution uses the installed YOLO26n detector. Results are sorted by descending score; unknown class filters produce no matches; no detections is an empty array |
| `BOX_CENTER` | `(BOX2D) -> POINT2D` | Function form of `box.center` |
| `POLYGON` / `ST_POLYGON` | `(STRING) -> POLYGON` | Parse constants during planning; require closure, finite values, and `[0,1]` coordinates |
| `ST_CONTAINS` | `(POLYGON, POINT2D) -> BOOLEAN` | Boundary points count as contained |

The AI functions bind separate release-owned inference identities. `VQL_CLASSIFY` uses `vql.builtin.yolo26n-cls@v0.1` at `$VQL_HOME/models/yolo26n-cls.onnx`; `VQL_DETECT` uses `vql.builtin.yolo26n@v0.1` at `$VQL_HOME/models/yolo26n.onnx`. Install them with `python scripts/export_yolo26.py --task classify --install` and `python scripts/export_yolo26.py --task detect --install`. Each identity is an internal model definition, not a Catalog Model, and therefore has no DDL, object revision, `SHOW` row, or user-selectable version. Planning resolves only the artifact required by the invoked function and returns `INVALID_LOCATION` with the corresponding install command when it is absent. `VQL_EXTRACT` has no release-owned execution identity until a conforming field-extraction backend is scheduled.

All three functions use `score` in `[0, 1]`, sorted descending and comparable only within one call. SQL NULL input produces a NULL result without resolving an implementation. Row processing failure produces NULL unless `vql.on_error = 'fail'`; successful absence is an empty array or a scalar answer with `value NULL`. Function markers are planning-only and executable overloads are extracted into `InferenceNode`; scalar marker execution is an internal error. Calls to the retired detection-shaped `VQL_EXTRACT` form fail with `INVALID_ARGUMENT` and a `VQL_DETECT` rewrite hint.

The public extraction literal uses standard MAP and STRUCT syntax. Request order fixes return-field order and participates in call identity:

```sql
VQL_EXTRACT(document, MAP {
  'title': 'What is the title?',
  'line_items': STRUCT('List the line items' AS question, TRUE AS list)
})
```

---

## Table Providers

Data ingress and egress use narrow connector traits: readable providers scan Tables and writable providers accept rows.

### Minimum Schemas

| Source | Minimum columns |
|---|---|
| IMAGES | `uri STRING, image IMAGE, width INT, height INT, captured_at TIMESTAMP` |
| VIDEOS frame table | `uri STRING, ts TIMESTAMP, pts_ms BIGINT, frame_id BIGINT UNSIGNED, frame IMAGE, duration DOUBLE, fps DOUBLE, width INT, height INT, codec STRING` |
| RTSP Table | `ts TIMESTAMP NOT NULL, frame IMAGE, frame_id BIGINT, source STRING` |

Unreadable optional metadata becomes NULL. `uri`, media values, and RTSP `ts/frame_id/source` are non-null. Provider options may append partition columns but cannot change the meaning of base columns.

### Image and Video Directory Tables

`CREATE TABLE ... USING IMAGES/VIDEOS` registers a local directory as an external table. An image contributes one row; a video is expanded into frame rows inside the scan at the table's fps.

The [Catalog Design](./catalog.md#tables-and-provider-capabilities) defines the accepted options and their validation. This section defines what the kernel does with them.

Provider behavior:

- Both providers implement `TableProvider`. Planning returns schema and statistics; listing and reading begin in `execute()`.
- Canonicalize and validate the local directory and `recursive` option at DDL time. Listing and reading occur during execution through the local `object_store` provider.
- `uri`, size, and modification time come from local listing. Probe dimensions, duration, and codec only when projected.
- `IMAGE` and `frame` leave the scan as references; scanning never decodes pixels.

Video expansion rules:

- The scan emits `uri`, `ts`, `pts_ms`, `frame_id`, reference-form `frame`, and file constants such as `duration`.
- A different sample rate requires another logical table over the same directory; no media is copied.
- `fps` is an explicit semantic target. Sampling uses PTS, not frame ordinal, and therefore supports variable frame rate.
- `pts_ms` is relative media time; `ts` is event time. Prefer trusted `start_time + pts`, then a table-level `start_time`. If neither exists, synthesize from the Unix epoch and mark `synthetic_event_time`.
- Push a time predicate into the scan's `time_range` and restrict sampling to it. The provider reports such a filter as inexact, so DataFusion still evaluates the exact predicate.
- The scan lists files, reads container timestamps once per file, and selects sampled PTS values; it probes duration, fps, dimensions, and codec only when one of those columns is projected.
- Decoding is a separate per-frame call made by a pixel consumer, keyed by path and `pts_ms`. A metadata-only query creates frame coordinates and reads zero frames, and `MediaRuntime` counters exist so tests can assert that.

The decoder itself is a media-runtime detail with two interchangeable backends. The default `ffmpeg-native` cargo feature links FFmpeg 8 development libraries; when that feature is off or the native decoder cannot initialize, `MediaRuntime` falls back to an `ffmpeg`/`ffprobe` subprocess decoder, and each probe, timestamp scan, and decode retries on the fallback. `CREATE TABLE ... USING VIDEOS` checks backend availability first and fails with `FEATURE_NOT_AVAILABLE` when neither backend is usable, so a host without FFmpeg never registers a table that cannot be read.

### RTSP Table

`CREATE TABLE ... USING RTSP OPTIONS (...)` creates one readable, unbounded Table. RTSP is non-replayable, so delivery is best-effort; frames lost to a crash, drop, or pause cannot be recovered. The UC boundary reports it as an `EXTERNAL` table with explicit VisionQL capability properties, not as a UC-managed `STREAMING_TABLE`.

Ingestion:

- FFmpeg demux and decode run on controlled worker threads, away from the async executor.
- Prefer TCP interleaved transport; allow UDP by configuration.
- Inter-frame codecs normally require decode at the source frame rate before sampling. `fps=5` reduces frame-buffer, preprocessing, and inference work, not necessarily decode work.
- Sampled frames enter the current epoch frame buffer. A row or time threshold closes the epoch.

Event time and reconnect behavior:

- At query-run start, pair UTC time with a monotonic clock in `IngestClock`; derive later ingest time from monotonic elapsed time so reconnects and wall-clock rollback cannot move it backward.
- Every initial connection or reconnect starts a new `source_generation`. `ingest_time` uses the process-wide `IngestClock`. For `capture_time`, the first decoded PTS in a generation is anchored to that frame's ingest instant and later timestamps advance by relative PTS; VisionQL does not claim absolute camera or RTCP wall-clock time.
- Missing or backward PTS, excessive drift from the ingest clock, or a mapped timestamp behind the current watermark falls back to ingest time and increments `event_time_fallback_total` with the reason.
- Watermark is `max_seen_event_time - watermark_delay`, never decreases, and considers every source frame with a valid timestamp—not only sampled rows. Apply an epoch's watermark only after its data completes.
- Reconnect with exponential backoff from 1s to 30s. Freeze the watermark during the outage; never invent progress from local wall time.
- Reconnected ingest time preserves the real outage gap and may close windows without fabricating rows. The attached query keeps retrying until cancellation or an unrecoverable error; an outage alone does not emit the final open window.

### Kafka Table

Declare the destination independently from the query:

```sql
CREATE TABLE people_per_minute (
  window_start TIMESTAMP,
  people BIGINT
)
USING KAFKA
OPTIONS (
  bootstrap_servers = '127.0.0.1:9092',
  topic = 'people-per-minute',
  format = 'json',
  credential_ref = 'secret://kafka/producer',
  delivery_timeout_ms = 30000,
  buffer_capacity = 1024
);

INSERT INTO people_per_minute
SELECT window_start, COUNT(*) AS people
FROM detections
GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE);
```

Three of those stored options drive write execution: `credential_ref` is resolved by the host-supplied `SecretProvider` on the first write, `delivery_timeout_ms` bounds connection, request, and delivery waits, and `buffer_capacity` bounds in-flight row deliveries for one table-write execution across every DataFusion partition — reaching it stops pulling upstream until a delivery completes. VisionQL never creates or alters the topic, and SASL/TLS details come from the provider rather than from DDL.

`CREATE TABLE` only validates and stores metadata; it performs no network I/O. Planning `INSERT INTO` validates the query output against a declared schema when columns are present. Execution resolves `credential_ref` through `EngineConfig::with_secret_provider`, connects lazily, emits one Kafka record per output row with no key, uses `acks=all`, and waits for every record in a batch to be acknowledged before that batch completes. A referenced table fails before connecting when the host did not install a provider or resolution fails. The producer is closed with the configured timeout when the foreground query finishes, fails, is cancelled, or its result stream is dropped. The attached coordinator disables producer retries and fails the query on delivery failure or timeout. A batch can therefore be partially visible after an error or cancellation, and replay can duplicate rows; transactions and exactly-once delivery are outside this contract.

The transport uses `rust-rdkafka` with a statically built `librdkafka` and vendored OpenSSL. Public host integration remains client-neutral: `SecretProvider` returns VisionQL-owned `KafkaAuthentication` and `KafkaTlsConfig` values, which the connector translates into TLS/mTLS, SASL/PLAIN, SCRAM-SHA-256/512, or static OAUTHBEARER client configuration. The producer disables automatic topic creation, idempotence, and client retries to preserve that contract; future transactional delivery can use librdkafka without changing the public authentication boundary.

The JSON value contract is:

- Preserve query output names and order as JSON object fields; duplicate names are rejected during planning and nulls are explicit.
- Encode booleans and finite numbers as JSON scalars; encode non-finite floats as `null`. Encode temporal values as ISO-8601 strings at their Arrow precision. Lists, maps, and ordinary structs retain their JSON shape.
- Project a top-level `IMAGE` to `uri`, `locator`, `pts_ms`, `frame_id`, `encoding`, `width`, and `height`. Strip URI user information, query, and fragment. Never emit `encoded`, `buffer_id`, or `buffer_slot`.
- Reject raw binary output and nested `IMAGE` values during planning; callers must make any intended binary representation explicit as text.

These rules are fixed by exact wire-format tests. The globally bounded in-flight set propagates Kafka backpressure upstream, while cancellation interrupts connection and delivery waits. For an RTSP query, `OFFSET` and `LIMIT` are applied before the acknowledged Table write, including closed `TUMBLE` output, so rows outside the visible query result are never published.

---

## Optimizer and `EXPLAIN`

### Rule Order

| Order | Rule | Purpose |
|---|---|---|
| R1 | SQL expression-function expansion, named-argument binding, and type checking | Establish valid semantics before inference extraction |
| R2 | Resolve and extract typed inference calls; deduplicate and constant-lift only deterministic or stable-within-query calls | Make inference schedulable without changing volatile call count or order |
| R3 | Native DataFusion rules | Ordinary predicate, projection, constant, and relational optimization |
| R4 | Projection pushdown into the provider scan | A scan that does not produce the media column never lists or decodes pixels |
| R5 | Time-predicate pushdown into `USING VIDEOS` | Read only the requested interval; the provider reports the filter as inexact and DataFusion keeps the exact predicate |
| R6 | Table-declared sampling inside the scan | The table's `fps` is applied by PTS during expansion, not as a query-level rewrite |

R3 through R6 run when the planned `DataFrame` is compiled, after inference extraction. An `InferenceNode` blocks predicate pushdown through its own output column, so a filter over a model result cannot be moved below the inference that produces it.

Window size never implies a sample rate. User-declared fps is part of result semantics.

### Extracting Inference

After expanding SQL expression functions, the planner scans Projection, Filter, and aggregate inputs for per-Model markers:

1. Resolve the call target and constant semantic/version arguments from the snapshot, require a resolved selected version, and copy its resolved specification into `InferenceNode`.
2. Replace each marker with an internal column reference and insert `InferenceNode` at the earliest point where every domain input exists and semantics remain unchanged.
3. Deduplicate only when the Model semantic fingerprint, all domain input expressions, and semantic arguments match exactly, the selected version is immutable, the interface is deterministic, and every input expression is deterministic. Preserve every volatile invocation and its order.
4. Evaluate a constant-domain-input inference call once as a query-init expression only under the same determinism rule.
5. Never share raw Runtime output across different resolved processor contracts; only canonical, semantically identical inference results are shareable.

### `EXPLAIN`

`EXPLAIN` prepends a VisionQL header to DataFusion's own logical and physical plans. The header is line-oriented and stable enough to assert on:

| Line | Emitted when | Content |
|---|---|---|
| `VisionQLPlan mode=bounded\|continuous` | Always | A stream with a top-level `LIMIT` reports `bounded`, because it runs as a bounded prefix |
| `Source RTSP name=… fps=… event_time=… watermark_delay_ms=… transport=… projection_pushdown=… time_range_pushdown=…` | The plan reads an RTSP table | The resolved stream configuration |
| `Source bounded projection_pushdown=… time_range_pushdown=…` | Otherwise | Pushdown available to bounded providers |
| `Topology RTSPSource -> EpochCoordinator [-> Inference] [-> TumblePlan] -> Watermark` | The plan reads an RTSP table | Epoch topology, including whether inference and window state participate |
| `Inference model=… identity=… runtime=… protocol=… pipeline=pre -> runtime -> post batching_owner=… volatile=… dedup=… decode=… image_payload=…` | Once per inference node | Resolved Model identity and execution shape, whether decode is required, and whether the input arrives as a locator/encoded value or a frame-buffer reference |
| `Unsupported node=…; suggestion=…` | An unbounded plan violates the allowlist | The first offending node and a viable rewrite |
| `VisionQLWrite name=… provider=KAFKA topology_append=TableWrite` | `EXPLAIN INSERT INTO <kafka table> …` | The write target appended to the topology |

`EXPLAIN ANALYZE` is rejected with `INVALID_SQL` because `EXPLAIN` is side-effect free and must never start a continuous query or contact a Runtime. `EXPLAIN` describes work; it does not invent uncalibrated GPU-time or cost estimates.

---

## Model Runtime and Inference

### Compiled Pipeline and Interfaces

Every resolved Model call selects one of two execution modes. Embedded Runtimes use the VisionQL-owned tensor pipeline:

```text
Model-interface input RecordBatch
  → PreProcessor
  → RuntimeRequestBatch
  → RuntimeSession
  → RuntimeResponseBatch
  → PostProcessor
  → Model-interface canonical Arrow result
```

Service Runtimes own the full model-facing pipeline:

```text
Model-interface canonical input
  → VisionQL transport codec
  → typed service request
  → service-owned preprocessing → inference → postprocessing
  → typed service response
  → VisionQL canonical Arrow conversion
```

The Runtime factory owns declaration validation, the explicit resolve operation, and construction for its execution mode:

```rust
trait RuntimeFactory {
    fn validate_declaration(&self, model: &ModelDef) -> Result<()>;
    async fn resolve(
        &self,
        model: &ModelDef,
        cache_dir: &Path,
        cancel: CancellationToken,
    ) -> Result<RuntimeResolution>;
    fn build_embedded(...) -> Result<Arc<dyn RuntimeSession>>;
    fn build_service(...) -> Result<Arc<dyn ModelBackend>>;
}
```

`Engine` owns one crate-private `PipelineRegistry`. Runtime selection, DDL validation, resolution, and backend compilation all use the same Runtime factory. Embedded execution additionally uses internal processor factories:

```rust
trait PreProcessorFactory: Send + Sync {
    fn kind(&self) -> &str;
    fn supported_types(&self) -> &[ModelType];
    fn validate(&self, options: &BTreeMap<String, Value>) -> Result<()>;
    fn build(&self, spec: &ProcessorSpec) -> Result<Arc<dyn PreProcessor>>;
}

```

`PostProcessorFactory` follows the PreProcessor factory shape. The registry internally registers `vision.image_tensor@1`, `vision.yolo_e2e@1`, `vision.yolo_raw@1`, and `vision.xywh_normalized@1` for embedded ONNX execution, plus the public Runtime IDs `onnx-runtime` and `triton-inference-server`. The known but unscheduled Runtime IDs `transformers`, `vllm`, `sglang`, and `llama-cpp` are registered as rejecting factories, so their stable `FEATURE_NOT_AVAILABLE` responses do not depend on an unrelated fallback branch. Processor IDs remain internal implementation details; no public registration API ships until an independent embedded Runtime requires one.

Each factory owns a serde option type with unknown fields denied. A PreProcessor receives only its input options; a PostProcessor receives only decoding and result-construction options; a Runtime receives only source, protocol, and binding options. Deserialization failures are rendered through the existing `INVALID_OPTION` contract with the complete option path. Options are deserialized once while compiling a pipeline, not once per input batch.

For embedded execution, resolution and compilation validate every adjacent contract: Model-interface input against PreProcessor input, PreProcessor output against Runtime input, Runtime output against PostProcessor input, and PostProcessor output against the canonical Model-interface result. For service execution, `RESOLVE MODEL` validates the service's typed request and response contract. No component may rely on an unchecked tensor name, dtype, shape, or response field.

Runtime tensors use Arrow's canonical `arrow.fixed_shape_tensor` extension type instead of a private dtype-and-buffer enum. The outer Arrow array length is the batch dimension; each slot is one equal-shape tensor backed by a non-nullable `FixedSizeList`, while the extension `Field` records element dtype, per-row shape, optional dimension names, and layout permutation. `TensorBatch` therefore carries the `FieldRef` together with its array so extension metadata cannot be separated from the buffer. Concrete batches contain only positive fixed dimensions after the batch axis; wildcard dimensions remain a contract-only concept.

The image PreProcessor emits `Float32` tensors with `C`, `H`, and `W` dimension names. ONNX Runtime borrows the contiguous Arrow values buffer for input execution; runtime output is wrapped back into a fixed-shape tensor before contract validation and PostProcessor execution. This raw tensor path is embedded-only. A Triton service receives encoded image bytes and returns canonical detections; a raw FP32 Triton model is rejected because it would move service-owned pre/post-processing back into VQL.

A CV PreProcessor resolves and decodes `IMAGE`, converts color and dtype, resizes/crops/pads, normalizes, changes layout, and constructs named tensor batches. It returns row-aligned context such as original dimensions and letterbox transforms.

A PostProcessor converts Runtime output to the canonical Arrow result. Detection implementations decode tensors, apply activation or NMS only when required, resolve labels, and restore coordinates. Capability presets define their semantic-argument allowlist; processors may consume only that allowlist.

The compiled embedded pipeline or service backend plus scheduler forms one cache entry keyed by the resolved Model semantic fingerprint. `ModelRuntime` removes entries whose fingerprints are no longer present in the Catalog head; in-flight queries retain their snapshot-owned `Arc`, while removing the cache owner closes the scheduler queue and releases the Runtime session after the last query finishes. Dropping, re-resolving, and recreating a Model therefore cannot reuse an obsolete session.

The implementation keeps embedded processor semantics separate while allowing a service Runtime to own its complete typed protocol:

```text
models/
  registry.rs                 # factory traits and PipelineRegistry
  preprocess/image_tensor.rs  # vision.image_tensor@1
  postprocess/yolo.rs         # YOLO decoding, NMS, coordinate restore, Arrow output
  ort_backend.rs              # ONNX Runtime session and graph-contract validation
  triton_backend.rs           # typed KServe V2 service codec and metadata validation
```

### Runtime Registry and Batching Ownership

A Runtime loads an artifact or binds a service endpoint. The persisted Model interface fixes call semantics; the Runtime owns how that interface is executed.

| `USING` Runtime | Source | Execution ownership | Batching owner | Delivery |
|---|---|---|---|---|
| `ONNX_RUNTIME` | Local, cached HTTP(S), or pinned Hugging Face ONNX artifact | VisionQL PreProcessor → ONNX Runtime → VisionQL PostProcessor | VisionQL queues requests; ONNX Runtime executes tensor batches | Implemented |
| `TRITON_INFERENCE_SERVER` | `triton+http(s)://host/model[@server_version]` | Triton owns preprocessing, inference, and postprocessing; VisionQL owns the typed KServe V2 codec | Triton owns model instances and dynamic batching; VisionQL owns bounded concurrency and backpressure | Implemented over HTTP; always volatile |

ONNX Runtime validates graph input/output names, dtypes, and static dimensions against the compiled processor contracts when the session is built. Its blocking `run` executes through Tokio's blocking pool and retains one session mutex because VisionQL-owned batching already serializes calls per session.

`RESOLVE MODEL` validates that Triton exposes a canonical `image` BYTES input and `detections` BYTES output, each with one dynamic batch dimension. Inference sends encoded images and receives one JSON detection list per row; VisionQL validates normalized confidence and box values and converts them to the canonical Arrow result. Raw tensor models are rejected. Cancellation drops the in-flight HTTP future. VisionQL does not manage Triton repositories or deployments.

Each Runtime reports whether batching is VisionQL-owned or service-owned. VisionQL does not place a second dynamic-batching queue in front of Triton; it applies bounded concurrency, cancellation, and backpressure around service calls. Additional Runtime families require their own versioned proposal and are not part of this design.

### ONNX Artifacts and Open-source Models

`FROM` preserves the Runtime's raw location. `ONNX_RUNTIME` accepts a local path, `file://` path, pinned `hf://owner/repository@revision[/artifact.onnx]`, or an HTTP(S) `.onnx` URL accompanied by `OPTIONS.sha256`. `TRITON_INFERENCE_SERVER` accepts the `triton+http(s)` form above. A resolver never guesses task or label semantics.

The embedded artifact is an ONNX graph whose metadata carries discoverable tensor, output-format, image-size, and label facts. Flat options override or provide only facts that inspection cannot decide. Other weight formats and arbitrary repository code require a future Runtime or an external inference service.

Remote ONNX files are downloaded to a temporary path and atomically installed under `$VQL_HOME/cache/models/<sha256>/<filename>`, with a source index mapping the declared location to the verified digest. Local files remain in place and endpoints create no cache entry. The Catalog stores source identity and the resolved digest rather than model bytes. Offline execution uses local files or a prewarmed cache. `HF_TOKEN` is read only while resolving private Hugging Face artifacts and is never persisted.

Integrating an embedded open-source model follows five steps:

1. Pin its source revision or SHA-256 digest.
2. Export or select an ONNX graph compatible with `ONNX_RUNTIME`.
3. Attach standard metadata during export and declare only required fallback options.
4. Run `RESOLVE MODEL` to materialize and validate it.
5. Pass processor contract tests and one real-model conformance fixture.

A familiar ONNX model family should need only Model DDL. A new reusable embedded tensor layout adds one narrow internal processor implementation and focused fixtures. A service Runtime instead owns all model-specific preprocessing and postprocessing and exposes the typed capability contract.

### `InferenceExec`, Scheduling, and Failure

The physical operator evaluates only domain arguments into a temporary Arrow `RecordBatch`; the resolved Model and constant semantic arguments remain in its immutable spec:

```text
domain Arrow values
  → materialize/decode as required
  → embedded pipeline or typed service codec
  → local scheduler or bounded remote submission
  → embedded RuntimeSession or service-owned pipeline
  → canonical Arrow conversion
  → nullable canonical Arrow result column
```

The compiled pipeline carries a stable row identifier. It must produce exactly one value, NULL, or row error for every input row and restore input order even when a remote service completes requests out of order. Batch video usually needs no frame buffer because read, decode, and preprocessing can be fused inside `InferenceExec`.

For VisionQL-owned batching, each compiled pipeline owns one bounded Tokio mpsc queue with per-request oneshot responses. Requests are handled FIFO and dispatch when the batch reaches 16 rows or the oldest request has waited 5 ms. The queue holds at most 64 requests; a full queue awaits capacity and propagates backpressure. Queue submission and response waits are cancellation-aware.

Service-owned batching bypasses the VisionQL batching queue. Each session instead owns a semaphore for bounded concurrent in-flight requests so the service can observe overlapping submissions and apply its own dynamic batching; awaiting a permit is VisionQL's backpressure boundary. The scheduler module centralizes the current limits—16 rows per VisionQL batch, 5 ms maximum wait, 64 queued requests, and 4 concurrent service requests—rather than scattering literals across Runtime construction.

NULL domain inputs produce NULL results. A row-level preprocessing or post-processing error follows `vql.on_error`: NULL by default or query failure in strict mode. A Runtime failure affecting a whole batch is attributed to every affected row before the same policy is applied. Cancelled queued requests are skipped, submission and response waits stop promptly, and late results are ignored. Service-owned calls receive the caller's cancellation token; the current batched embedded backend call is allowed to finish before its buffers are released.

All preprocessing tensors, encoded request payloads, Runtime queues, and post-processing buffers reserve memory through the engine pool. Prompt and payload size limits are checked before allocation. Runtime identity and batching ownership remain plan annotations.


---

## Resources and Performance

### Unified Resource Budget

Each Session receives one host-memory budget, set by `kernel.session.memory_limit` in `$VQL_HOME/config.toml` and defaulting to 512 MiB. Every query, asynchronous task, and retained result owned by that Session shares its DataFusion `MemoryPool` capacity; cloned Session handles share the same pool, while separately built Sessions receive independent pools. The limit covers these resources:

| Resource | Behavior at the limit |
|---|---|
| Arrow batches and operator state | Use DataFusion memory management; custom state without spill support fails explicitly |
| Local-file prefetch and compressed bytes | Reduce concurrency and read-ahead |
| Decoded frame buffer | Backpressure batch sources; for RTSP, drop only the oldest sampled frame before epoch admission |
| Tensor buffers and inference queues | Bounded queues; submitters await capacity |
| `TUMBLE` state | No spill; fail with guidance to reduce group-key cardinality or shorten the window |
| Table-write buffers | Apply backpressure; fail after timeout according to query policy |

The Session limit is not an Engine-wide or process-RSS limit. Catalog internals, on-disk model cache contents, third-party allocations outside the reservation system, and aggregate memory across separately built Sessions are outside it. When a reservation would exceed the limit, the requesting query fails with `RESOURCE_EXHAUSTED`; existing queries retain their reservations, and every reservation is released with its owning asynchronous work or result handle.

### Performance Measurement

The PRD does not set hardware-specific throughput or latency targets. Capacity depends on the Model, Runtime, accelerator, codec, GOP, source transport, sampling policy, and query shape. Engineering benchmarks therefore report four rates separately:

| Measure | What it tests |
|---|---|
| Input bitrate | Network and demux capacity |
| Decode rate | Full decode work required by the source codec |
| Sampled output rate | Frame buffer, preprocessing, and query input after sampling |
| Inference rate | Model work remaining after sampling and query filtering |

A benchmark records the measured rates, window latency, and frame-drop rate together with its complete workload and hardware configuration. Thresholds belong to benchmark plans and release evidence, not to the product requirements contract.

### Error Classes

| Class | Example | Default behavior |
|---|---|---|
| Row data error | Corrupt frame, one failed inference | Write NULL and continue |
| Query semantic error | Type mismatch, unbounded sort, unavailable feature | Fail planning before starting runtime work |
| Resource error | Memory/device exhaustion, excessive state | Fail query and release every lease |
| External-system error | RTSP disconnect, Kafka unavailable | Retry by connector policy; eventually fail or remain Disconnected |
| Engine defect | Broken invariant, frame-buffer bounds violation | Fail immediately with diagnostics; never downgrade to NULL |

Stable identifiers are separate from prose messages. Clients react to identifiers, never error-string matching. Every kernel error follows the [`VQL-CCDDD` registry](./error_codes.md) and renders as `[VQL-CCDDD] SYMBOL: message`.

`ErrorCode::as_str()` returns the identifier, while `ErrorCode::symbol()` returns the readable symbolic name. Known errors retain their identifier when they pass through DataFusion or an asynchronous execution boundary. Adding an identifier is additive; removing, reusing, or changing the meaning of an identifier or symbol is a breaking contract change. Owner tests assert on identifiers and symbols directly.

---

## Security and Privacy

- `vql-kernel` listens on no network port by default.
- Outbound connections occur only for user-declared endpoint Models, Kafka, RTSP, and model download.
- Model bundles are pinned by immutable revision and complete digest where the source permits it and are verified at load time.
- Catalog output, logs, and `SHOW CREATE` sanitize URIs and secret references.
- Execution uses the immutable definition snapshot and resolved specifications captured during planning; no execution identity is persisted.


---

## Related Designs

- [High-Level Design](../high_level_design.md)
- [Catalog Design](./catalog.md)
- [CLI Design](./cli.md)
- [Python Binding Design](./python_binding.md)
- [Testing Design](./testing.md)
