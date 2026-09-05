# VisionQL `vqld` Service Design

> This document defines the v0.2 `vql-server` crate and its `vqld` daemon. It extends the embedded contracts in the [High-Level Design](./high_level_design.md), [Kernel Design](./kernel.md), [Catalog Design](./catalog.md), and [Error Code Design](./error_codes.md) without moving SQL or Catalog semantics into the service host.

## Scope

`vqld` is the single-node network host for VisionQL. It owns Arrow Flight SQL, TLS, authentication, authorization enforcement, logical client sessions, active-query registration, durable jobs, checkpoints, recovery, health endpoints, operational metrics, and out-of-process Python UDF supervision.

The service preserves these boundaries:

- `vql-kernel` owns parsing, statement classification, immutable definition snapshots, logical planning, bounded execution, the epoch coordinator, cancellation tokens, media conversion, model execution, and checkpointable window-state semantics.
- `vql-catalog` owns Catalog, Schema, Table, Model, and Function definitions, provider capabilities, object revisions, snapshots, SQLite persistence, and Unity Catalog-compatible wire models. It does not own QueryJobs or checkpoints.
- `vql-server` depends on the public APIs of `vql-kernel` and `vql-catalog`; neither lower crate depends on Flight, gRPC, authentication middleware, Prometheus, or service persistence.
- Workbench, CLI service mode, ADBC, and JDBC are independent clients. They use public Flight SQL, public SQL, the authenticated Unity Catalog-compatible API, and the metrics endpoint. There is no Workbench-only management RPC.

The v0.2 service does not provide clustering, high availability, distributed workers, exactly-once delivery, replay for RTSP, an audit log, a general media server, or a multi-tenant Python security sandbox.

## Service Boundary

The `vql-server` package builds the `vqld` binary and composes four service-owned control planes around one embedded `Engine`:

| Control plane | Ownership |
|---|---|
| Session registry | Handshake tokens, expiry, Session settings, prepared statements |
| Query registry | Per-execution query IDs, Flight endpoints, cancellation, attached lifecycle |
| Job store | QueryJobs, Query Manifests, dependency records, state transitions, retention |
| Checkpoint store | Versioned window state, watermark, source progress, sink acknowledgement boundary |

`vqld` uses `$VQL_HOME/catalog/vql.db` through `vql-catalog` and stores service state separately:

```text
$VQL_HOME/
├── catalog/
│   └── vql.db
└── service/
    ├── vqld.db
    └── checkpoints/
        └── <query_id>/
            └── <checkpoint_id>.arrow
```

Only one `vqld` process may write one `VQL_HOME`. The daemon holds an instance lock for its lifetime. Job metadata and checkpoint pointers are transactional in `service/vqld.db`. A checkpoint blob is written to a temporary file, synchronized, atomically renamed, and only then selected by a database transaction. Unreferenced temporary or superseded blobs are removed by service garbage collection.

### Public Network Surfaces

| Surface | Contract |
|---|---|
| Arrow Flight SQL | Queries, updates, prepared statements, metadata, cancellation, and media tickets |
| Unity Catalog-compatible HTTP API | Authenticated Catalog, Schema, and Table metadata and mutations under `/api/2.1/unity-catalog` |
| `/health/live` | Process liveness only; returns no configuration or dependency detail |
| `/health/ready` | Readiness of the Catalog, job store, and Flight listener; returns no sensitive detail |
| `/metrics` | Prometheus text format for an authenticated monitoring principal |

Flight and HTTP listeners use TLS outside loopback development. Every surface except the two minimal health checks authenticates its caller and resolves the same principal. `vqld` must not mount `vql_catalog::uc::http::router` directly: the service wraps the UC operations with authentication, authorization, and the same Catalog-mutation coordinator used by SQL DDL.

## Kernel Host Contract

The existing `Session::sql` behavior remains the embedded convenience API. The service adds an additive kernel-owned prepare/execute boundary so `vql-server` never imports a private VQL AST, parses the first SQL keyword, or calls `Session::sql` merely to discover a schema.

Conceptually, the public host API provides:

```text
Session::prepare(sql, principal, session_settings) -> PreparedStatement
PreparedStatement::statement_info() -> StatementInfo
PreparedStatement::result_schema() -> Arrow Schema
PreparedStatement::execute_query() -> QueryHandle
PreparedStatement::execute_update() -> UpdateResult
PreparedStatement::query_manifest() -> QueryManifestInput
```

The concrete Rust API may use different method names, but it must preserve these contracts:

- Preparation parses, classifies, authorizes, captures one immutable `DefinitionSnapshot`, and plans query-shaped statements. It performs no Catalog mutation, DML write, model resolution, connector execution, or Python invocation.
- DDL preparation validates syntax and authorization intent. Provider validation and the Catalog transaction occur only during `execute_update`.
- A prepared statement owns its classification, result schema, captured Session settings, definition snapshot, and logical plan. The service receives stable host types rather than DataFusion or private parser types.
- `SET` executes as a Session mutation. A successful prepare captures the current setting generation, so later `SET` statements affect later preparations but do not alter an already prepared or running statement.
- One logical Session may have multiple prepared statements and concurrent executions. Cancellation is always routed through the QueryHandle registered for a specific `query_id`; the embedded single-active-query interrupt helper is not a service query registry.
- Preparing `SUBMIT QUERY` produces a manifest input but does not create a job. The job-store transaction occurs only when the persistent-submission statement executes.

## Flight SQL Contract

`vql-server` implements statement query/update, prepared statements, `GetSchema`, `GetCatalogs`, `GetDbSchemas`, `GetTables`, `GetTableTypes`, `GetSqlInfo`, `PollFlightInfo`, and `CancelFlightInfo`.

Bounded and unbounded statement-query results use `DoGet`. An unbounded query returns consumable `FlightInfo` as soon as its schema and endpoint are registered; it does not wait for query completion. Bounded updates use statement-update and return their affected-row count only after completion.

### Protocol Discovery

Vendor SqlInfo IDs start at the specification-reserved value 10000:

| ID | Type | Meaning |
|---|---|---|
| 10000 | string | VQL client-protocol version |
| 10001 | string | VQL SQL-dialect version |
| 10002 | string | VQL `IMAGE` extension version |
| 10003 | list<string> | VQL capability names |

Values for IDs 10000 and 10001 are decimal `MAJOR.MINOR` strings. Clients parse each component as an integer and never compare the strings lexicographically. The value for ID 10002 is the integer version string `1`. The numeric SqlInfo ID is the wire identity; the descriptions above are not additional string keys.

The capability set publishes only implemented behavior and includes `unbounded_do_get`, `poll_flight_info`, `cancel_flight_info`, `statement_info_v1`, `image_thumbnail_mode`, `frame_at_v1`, and `query_control_v1` when available. Clients ignore unknown capabilities.

### Statement Classification

Every prepared result schema, including a zero-column update schema, carries:

```text
vql.statement_info.version = 1
vql.statement_info.kind = query | update | ddl | persistent_submission
vql.statement_info.query_mode = bounded | unbounded | not_applicable
vql.statement_info.result_mode = bounded | unbounded | none
vql.statement_info.side_effect = read_only | write
```

`vql.statement_info.query_mode` describes the submitted computation. `vql.statement_info.result_mode` describes the Flight result lifetime. This keeps a durable continuous job distinct from the bounded row that confirms its submission.

| Statement | kind | query mode | result mode | side effect | Transport |
|---|---|---|---|---|---|
| Bounded `SELECT`, `SHOW`, or `DESCRIBE` | `query` | `bounded` | `bounded` | `read_only` | statement-query plus `DoGet` |
| Ordinary unbounded `SELECT` | `query` | `unbounded` | `unbounded` | `read_only` | attached `DoGet` data stream |
| Bounded `INSERT` or DML | `update` | `bounded` | `none` | `write` | statement-update |
| Ordinary unbounded `INSERT INTO ... SELECT ...` | `query` | `unbounded` | `unbounded` | `write` | attached `DoGet` status stream |
| `SUBMIT QUERY ...` | `persistent_submission` | `unbounded` | `bounded` | `write` | statement-query plus one job row |
| Catalog DDL, `SET`, or job control | `ddl` | `not_applicable` | `none` | `write` | statement-update |

The attached unbounded `INSERT` status schema is:

```text
query_id Utf8,
lifecycle Utf8,
state Utf8,
definition_revision Utf8,
updated_at Timestamp(Nanosecond, UTC)
```

`lifecycle` is `attached`. The stream emits one row when execution starts and one row for each state change, then remains open until completion, cancellation, Session expiry, or failure.

Every statement-query sets `FlightInfo.app_metadata` to UTF-8 JSON `VqlFlightInfoV1`:

```json
{
  "version": 1,
  "query_id": "...",
  "statement_kind": "query",
  "query_mode": "unbounded",
  "result_mode": "unbounded"
}
```

The prepared-schema metadata is the classification authority. Clients ignore unknown JSON fields and never extract a query ID from ticket contents.

### Sessions, Execution, and Cancellation

Handshake returns a cryptographically random logical Session token. Clients send it on every RPC, and the server validates expiry and principal binding before selecting a kernel Session. A gRPC channel that completed Handshake is neither a Session identity nor authorization and may be reused across Sessions.

Each execution receives a server-generated UUID and an independent query-registry entry. A prepared statement may be executed more than once, producing a different query ID each time. `PollFlightInfo` and `CancelFlightInfo` resolve only through this registry.

An attached execution has a bounded interval in which its first `DoGet` must attach. Once attached, dropping its last `DoGet` response stream cancels its QueryHandle. Losing an unrelated RPC or the underlying reusable channel does not cancel another execution. Session expiry cancels all attached executions and drops prepared statements. Durable jobs are independent of their submitting Session and are cancelled only through job-control SQL.

Scripts prepare and execute statements sequentially because later statements may depend on earlier DDL. No client or server claims whole-script semantic preflight.

### Structured Errors

Failures use a standard gRPC status for the broad class and carry a Protobuf-encoded `vql.protocol.v1.VqlErrorV1` in `vql-error-bin` trailing metadata:

```proto
message VqlErrorV1 {
  uint32 version = 1;
  string code = 2;
  string message = 3;
  optional string hint = 4;
  optional uint64 source_start = 5;
  optional uint64 source_end = 6;
  optional string query_id = 7;
  bool retryable = 8;
  string symbol = 9;
  optional string target_version = 10;
}
```

`code`, `symbol`, `target_version`, source span, `query_id`, and `retryable` are structured fields. `message` and `hint` are human-readable. The source span is a half-open byte range `[source_start, source_end)` in the current statement's UTF-8 text and is omitted when no reliable position exists.

The owning boundary assigns retryability to the error instance; clients never derive it from `code` alone. Parser and option errors map to `INVALID_ARGUMENT`, missing objects to `NOT_FOUND`, conflicts to `ALREADY_EXISTS` or `FAILED_PRECONDITION`, cancellation to `CANCELLED`, resource exhaustion to `RESOURCE_EXHAUSTED`, authorization failures to `PERMISSION_DENIED`, unavailable external dependencies to `UNAVAILABLE`, and engine defects to `INTERNAL`. A service authorization condition receives its own append-only VQL identifier before the protocol is released rather than reusing an input-error identifier.

Clients that do not understand the extension can still consume the gRPC status. Workbench reads the envelope instead of parsing messages and adds `statement_index` itself for multi-statement scripts.

## `IMAGE` Flight Transport

A Session controls result representation with SQL settings:

```sql
SET vql.result.image_mode = 'reference';
SET vql.result.image_mode = 'thumbnail';
SET vql.result.image_mode = 'inline';
SET vql.result.thumbnail_max_edge = 256;
SET vql.result.thumbnail_quality = 75;
```

The service validates thumbnail dimensions, quality, per-cell bytes, per-batch bytes, and total query-result bytes against configured bounds. Exceeding a bound returns `RESOURCE_EXHAUSTED`; the service never silently changes the requested representation.

All modes retain the `vql.image` Arrow storage schema and extension version. Before a batch crosses Flight, `buffer_id` and `buffer_slot` are always NULL.

| Mode | Persistent image or video frame | Live RTSP frame |
|---|---|---|
| `reference` | Sanitized `uri`, locator, coordinates, dimensions; no encoded bytes | Sanitized metadata and frame ID; no locator or encoded bytes |
| `thumbnail` | Reference fields plus bounded JPEG thumbnail | Bounded JPEG thumbnail and metadata; no locator |
| `inline` | Original encoded content when available, otherwise one bounded encode | Current frame encoded before its epoch lease is released; no locator |

Field metadata marks non-null `encoded` values as `content_kind=thumbnail` or `content_kind=original`. A live RTSP frame has no durable locator and cannot be opened after its Flight batch has been consumed. Evidence requiring later access must first be written to a persistent Table.

### Original-media Tickets

Original-media lookup is a locator-backed Flight `DoGet`, not a SQL scalar function. `FRAME_AT` is the client-facing operation name for a versioned media ticket containing an opaque `IMAGE.locator` and an optional replacement `pts_ms`.

The operation accepts only persistent image and video locators. `vqld` rejects display URIs and live frame IDs, parses only supported locator versions, resolves the embedded Table generation, and reauthorizes the current principal on every read. A replacement PTS may select only within the same authorized video object and source generation. Local persistent providers are supported in v0.2; remote object-store providers require their own credential and authorization contract.

Locators contain no credentials and are not bearer authorization. The server returns a stable error for malformed locators, unavailable source generations, out-of-range PTS values, denied access, and unavailable media.

## Public Query and Job SQL

The service adds these statements to the kernel-owned VQL parser:

```sql
SUBMIT QUERY people_per_minute AS
INSERT INTO people_sink SELECT ...;

SHOW QUERIES;
DESCRIBE QUERY '<query_id>';
SHOW QUERY DEPENDENCIES '<query_id>';
PAUSE QUERY '<query_id>';
RESUME QUERY '<query_id>';
STOP QUERY '<query_id>';
```

`SUBMIT QUERY` accepts exactly one unbounded `INSERT INTO <table> SELECT ...` whose destination has writable capability. The name is a normalized SQL identifier and is unique among the principal's non-terminal jobs. State changes use only the server UUID; names are display and filtering metadata.

`SUBMIT QUERY` returns one bounded Arrow row after manifest construction and the job-store transaction commit:

```text
query_id Utf8,
name Utf8,
state Utf8,
definition_revision Utf8
```

`definition_revision` is the lowercase SHA-256 digest of the canonical `QueryManifestV1` payload. It is immutable for the job lifetime. Attached queries use the fingerprint of their process-local prepared definition.

Minimum result columns are:

```text
SHOW QUERIES:
  query_id, name, lifecycle, state, mode, source_kind, source_health,
  delivery_semantics, last_event_time, started_at, updated_at,
  definition_revision, error_code, error_message

DESCRIBE QUERY <id>:
  query_id, name, lifecycle, state, owner, sql_redacted,
  created_at, started_at, updated_at, definition_revision,
  checkpoint_id, checkpoint_at, error_code, error_message

SHOW QUERY DEPENDENCIES <id>:
  query_id, object_kind, object_id, object_name,
  revision_id, semantic_fingerprint
```

Later protocol versions may append columns. Clients select fields by name and preserve unknown state strings. Attached queries have `name=NULL, lifecycle=attached`; QueryJobs have `lifecycle=persistent`. Job-history retention is bounded by service configuration, while non-terminal jobs are never evicted.

## QueryJob Persistence and Catalog Coordination

`QueryJob` is a `vql-server` domain object stored in `service/vqld.db`, not a `vql-catalog` object and not part of a Catalog namespace or Unity Catalog response. It contains the query ID, owner, normalized name, manifest bytes protected by service-store filesystem permissions, redacted SQL projection, state, timestamps, checkpoint pointer, definition revision, last error, and dependency rows.

`vqld` serializes `SUBMIT QUERY` and definition-invalidating Catalog mutations through one in-process mutation coordinator while holding the exclusive instance lock. Both Flight SQL DDL and the authenticated UC HTTP adapter use this coordinator. Dropping a Table, Model, Model version, or Function required by a non-terminal job fails with a structured error listing the dependent query IDs. Moving an unrelated Model default does not invalidate a manifest that pinned an explicit resolved version.

The Catalog's append-only object revisions remain definition history, not job storage. A Query Manifest copies every definition required for rehydration. Service dependency records prevent invalidating mutations while the job is non-terminal, and service-owned artifact leases prevent cached model bytes from being collected. Authorization is still checked against the current principal and policy; neither a copied definition nor a lease grants access.

## Query Manifest and Rehydration

`QueryManifestV1` is a versioned, deterministic service DTO derived from a prepared persistent submission. It records:

- normalized SQL and the writable sink;
- every readable and writable Table identity, generation, revision, provider definition, and canonical Arrow schema;
- every referenced Model and Function identity, revision, semantic fingerprint, and resolved specification;
- Model artifact digests or service bindings, Runtime and processor contracts, canonical schemas, and determinism;
- the submitting principal and authorization-relevant object identities;
- normalized Session settings that affect semantics;
- the logical-plan fingerprint, state-schema fingerprints, codec versions, and engine state-format version.

The manifest does not serialize a DataFusion `LogicalPlan`, physical plan, `DataFrame`, process-local frame reference, credential value, or compiled model session. On start or recovery, the kernel rehydrates a prepared execution from the manifest-owned definitions, not from current Catalog heads, and verifies that the reconstructed logical-plan fingerprint matches the manifest. A mismatch fails with `RECOVERY_INCOMPATIBLE`; it never falls back to planning against newer definitions.

`RESUME` therefore rehydrates the original manifest without adopting later Catalog changes. To use changed definitions, the caller stops the old job and submits a new one with a new query ID.

## Window Checkpoint ABI

The kernel exposes a versioned checkpoint form for its existing process-local `TumbleState`. Each checkpointable aggregate codec defines its aggregate kind, input types, normalized state fields, update and evaluation rules, byte accounting, serialization, and restore behavior.

A checkpoint snapshot is non-destructive. It copies the current watermark and open-window state without calling `state()` on a retained active accumulator; the embedded coordinator continues to use temporary accumulators and stored `ScalarValue` state. Serialized window keys are canonically sorted, Arrow schemas are fingerprinted, and every operator record includes the operator ID, aggregate kind, codec version, state-schema fingerprint, and engine state-format version.

Codec tests cover deterministic schema and ordering, non-destructive snapshot, restore round-trip, compatibility rejection, NULL and overflow behavior, and resource accounting. A codec or engine-state format change requires a migration or replay plan; incompatible recovery fails rather than attempting best-effort deserialization.

## Checkpoint and Recovery Semantics

RTSP is non-replayable. For it, a successful checkpoint preserves window state and watermark only through that checkpoint. Frames admitted after the latest checkpoint and before a crash cannot be reconstructed and are reported as a source gap. The service never claims that all post-checkpoint state survives or that RTSP output is exactly once or at least once.

The coordinator selects a completed epoch as a periodic checkpoint boundary based on elapsed time or state growth. At a selected boundary it:

1. applies the epoch to normalized window state and produces any closed-window output;
2. writes output to the destination Table and waits for the provider's acknowledgement contract;
3. takes a non-destructive checkpoint snapshot containing the manifest identity, plan fingerprint, watermark, open-window state, codec versions, and acknowledged delivery boundary;
4. atomically installs the checkpoint blob and commits its job-store pointer;
5. releases the epoch's frame lease and admits the next epoch.

If checkpoint persistence fails after sink acknowledgement, the coordinator stops admitting epochs and retries within policy or marks the job `FAILED`; it does not continue with an uncheckpointable state. A crash still restores the previous successful checkpoint, so any later RTSP contribution becomes part of the reported gap.

`PAUSE` creates a mandatory checkpoint barrier: stop source admission, finish the current admitted epoch, wait for sink acknowledgement, persist the checkpoint, and only then commit `PAUSED`. A failed pause checkpoint moves the job to `FAILED` with the last successful checkpoint retained.

Delivery follows the source and sink contracts:

- RTSP-to-Kafka is best-effort. The current Kafka writer waits for acknowledgements but can leave a partially visible batch after failure, and RTSP cannot replay missing rows.
- A future replayable source may provide at-least-once delivery only after its source progress participates in the same checkpoint boundary.
- Exactly-once requires a transactional sink commit coordinated with checkpoint publication and is outside v0.2.

On recovery, `vqld` restores the newest successful checkpoint when one exists, creates a new `IngestClock`, reconnects RTSP at the live position with a new source generation, and applies the kernel time-continuity gate. A job that crashed before its first checkpoint starts from the manifest and records the unavailable RTSP interval as a source gap. If the first ingest time is below a recovered watermark, the job fails with `TIME_DISCONTINUITY`; the service does not clamp time or mark all future frames late.

## QueryJob State Machine

```text
SUBMITTED → STARTING → RUNNING → PAUSING → PAUSED
                │          │                    │
                └──────────┴────→ RECOVERING ←──┘
                                      │
                                      └────→ RUNNING

Any non-terminal state ─→ STOPPING ─→ STOPPED
STARTING / RUNNING / PAUSING / RECOVERING ─→ FAILED
FAILED ─→ STOPPING ─→ STOPPED
```

- `PAUSE` is valid for `RUNNING`; repeating it for `PAUSING` or `PAUSED` is idempotent.
- `RESUME` is valid for `PAUSED`; it reauthorizes dependencies and enters `RECOVERING`. Repeating it for `RECOVERING` or `RUNNING` is idempotent.
- `STOP` is terminal, idempotent, releases runtime and artifact leases, and retains bounded job history.
- Revoked authorization fails the next execution-start, epoch-boundary, resume, or media-read check. A lease never bypasses revocation.

Startup normalizes every persisted non-terminal state:

| Persisted state | Startup action |
|---|---|
| `SUBMITTED` | Enter `STARTING` and launch from the manifest |
| `STARTING` | Launch from the manifest, or enter `RECOVERING` when a checkpoint exists |
| `RUNNING`, `PAUSING`, `RECOVERING` | Enter `RECOVERING`; restore the last successful checkpoint when present, otherwise launch from the manifest and record a source gap |
| `PAUSED` | Remain paused with no source or model runtime allocated |
| `STOPPING` | Complete cleanup and commit `STOPPED` |
| `FAILED`, `STOPPED` | Preserve terminal history and allocate no runtime resources |

No committed `SUBMITTED` or `STARTING` job may remain stranded after restart.

## Security and Authorization

- Authenticated identities map to Catalog principals. The Session token is opaque, short-lived, bound to one principal, and invalidated on logout or expiry.
- The service authorizer evaluates read, write, definition-mutation, job-control, and media-read actions. Catalog list filtering is not authorization.
- Query preparation, execution start, each durable epoch boundary, resume, job control, UC mutations, and media dereference enforce authorization against current policy.
- `IMAGE.uri` is sanitized display data. `IMAGE.locator` is an opaque media coordinate, contains no credential, and is never accepted as authorization.
- Raw credentials, Authorization headers, cookies, URI userinfo, signed query parameters, Session tokens, and resolved secret values never enter manifests, errors, metrics, or public logs.
- Python UDFs run in supervised worker processes with time, memory, dependency, and cancellation limits. A worker is isolated from `vqld` but is not presented as a hostile multi-tenant sandbox.
- v0.2 has no audit log. Execution context still retains principal, query ID, manifest identity, and resolved object fingerprints so a later audit design can consume explicit provenance.

## Metrics and Health

The Prometheus endpoint exposes process, Session, active-query, inference, source, sink, checkpoint, recovery, and gap metrics. Names follow Prometheus base-unit and suffix conventions. Missing data is absent, never reported as zero.

Per-query series may carry `query_id` only while a query is active or within a bounded post-terminal metrics-retention window. Aggregate counters omit query IDs. The service caps retained per-query series so terminal-job history cannot create unbounded Prometheus cardinality. Workbench uses query-level series for active previews and public job SQL for retained historical state.

Liveness means the process event loop responds. Readiness requires the Catalog and job store to open, migrations to complete, the instance lock to be held, and the Flight listener to accept work. External RTSP, Kafka, model, or user Table failures affect their queries and metrics but do not make the whole daemon unready.

## Testing and Acceptance

Kernel owner tests verify:

- prepare performs no Catalog mutation, DML write, model resolution, connector execution, or Python invocation;
- statement classification and result schemas are identical across embedded and service hosts;
- prepared settings and definition snapshots do not change after later `SET` or Catalog mutations;
- manifest construction and rehydration preserve the logical-plan fingerprint;
- checkpoint snapshots are deterministic, non-destructive, restorable, version-checked, and resource-accounted;
- per-QueryHandle cancellation remains independent when one Session runs concurrent statements.

Service protocol tests verify every metadata RPC, SqlInfo ID, capability, statement transport mapping, prepared statement, `VqlFlightInfoV1`, attached write-status stream, attach timeout, disconnect cancellation, Session isolation, structured error field, and unknown-field fallback.

Security tests exercise Flight and UC authentication, list filtering, direct unauthorized reads and writes, destructive Catalog mutations with job dependencies, Session-token replay, locator tampering, source-generation substitution, PTS range changes, revocation, redaction, and metrics access.

Recovery tests kill `vqld` after each durable boundary: job commit, `SUBMITTED`, `STARTING`, epoch admission, state update, sink acknowledgement, checkpoint blob rename, checkpoint pointer commit, `PAUSING`, and `STOPPING`. They verify startup normalization, last-checkpoint restoration, explicit RTSP gaps, no invented rows, forced pause checkpoints, incompatibility failure, and cleanup of unreferenced checkpoint files.

Media tests cover all three `IMAGE` modes, NULL process-local buffer fields at Flight boundaries, content-kind metadata, byte limits, persistent locator reads, rejected live-frame dereference, current-principal authorization, and unavailable source generations.

## References

- [Apache Arrow Flight SQL specification](https://arrow.apache.org/docs/format/FlightSql.html)
- [Workbench proposal](./proposals/2026-08-05-workbench.md)
