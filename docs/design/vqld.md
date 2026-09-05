# VisionQL `vqld` Service Design

> This document defines the v0.2 `vql-server` crate and its `vqld` daemon. It extends the embedded contracts in the [High-Level Design](../high_level_design.md), [Kernel Design](./kernel.md), [Catalog Design](./catalog.md), and [Error Code Design](./error_codes.md) without moving SQL or Catalog semantics into the service host.

## Scope

`vqld` is a single-node network host for VisionQL. Its v0.2 product task is narrow: run the same bounded and continuous SQL as the embedded engine, and keep an explicitly submitted continuous Table write alive independently of the client that submitted it.

The v0.2 acceptance path is:

1. submit one RTSP-to-Kafka query through `vqld`;
2. disconnect the client while the query keeps running;
3. reconnect, find the same job, and inspect or stop it;
4. restart `vqld` and observe the active job reconnect at the live RTSP position with a reported restart gap and fresh window state.

The service owns a tested Arrow Flight SQL profile, logical client Sessions, attached-query registration, a persistent job repository, process health, and aggregate operational metrics.

The v0.2 service does not provide serialized window checkpoints, `PAUSE` or `RESUME`, transparent state recovery, multi-user or relation-level authorization, a network Unity Catalog API, original-media lookup, Workbench, a chainable DataFrame API, service-side Python UDF execution, complete BI/JDBC compatibility, clustering, high availability, distributed workers, replay for RTSP, or exactly-once delivery.

## Ownership Boundaries

- `vql-kernel` owns parsing, statement classification, immutable definition snapshots, planning, bounded execution, the epoch coordinator, cancellation, media conversion, and model execution.
- `vql-catalog` owns Catalog, Schema, Table, Model, and Function definitions, immutable object revisions, provider capabilities, SQLite persistence, and Unity Catalog-compatible wire models.
- `vql-server` owns network transport, logical Sessions, attached query IDs, persistent job lifecycle, restart orchestration, and service-level security.
- `vql-server` depends only on public APIs of `vql-kernel` and `vql-catalog`; neither lower crate depends on Flight, gRPC, authentication middleware, or service persistence.
- CLI, Python, and third-party clients use public Flight SQL and SQL. No client receives a private engine or job-management RPC.

`vqld` composes three service-owned control planes around one embedded `Engine`:

| Control plane | Ownership |
|---|---|
| Session registry | Authenticated logical Sessions, expiry, settings, and prepared statements required by the tested client profile |
| Query registry | Per-execution query IDs, Flight endpoints, attached lifecycle, and exact cancellation |
| Job repository | Persistent query SQL, pinned Catalog generations, state, restart gaps, errors, and bounded history |

## Local State and Process Model

`vqld` uses `$VQL_HOME/catalog/vql.db` through `vql-catalog` and stores service state separately:

```text
$VQL_HOME/
├── catalog/
│   └── vql.db
└── service/
    └── vqld.db
```

Only one `vqld` process may write one `VQL_HOME`. The daemon holds an instance lock for its lifetime. A job transaction is committed before launch, so every accepted submission is discoverable after restart. Terminal history is retained under a configured count or age bound; active jobs are never evicted.

The Catalog remains the source of truth for definitions. A non-terminal job records the exact opaque generations from its definition snapshot. The append-only Catalog already retains those historical generations, while live heads may still be changed or dropped for new queries. Such a mutation does not stop or retarget a persistent job; job termination remains explicit. The service does not copy full Table, Model, or Function definitions into a second manifest store and does not globally serialize Catalog mutations with job submission.

## Public Network Surfaces

| Surface | Contract |
|---|---|
| Arrow Flight SQL | Query and update execution, the metadata needed by the selected clients, and per-query cancellation |
| `/health/live` | Process liveness only; returns no configuration or dependency detail |
| `/health/ready` | Catalog, job repository, instance lock, and Flight-listener readiness |
| `/metrics` | Optional process and aggregate service metrics without per-query labels |

`vqld` binds to loopback by default. A non-loopback listener requires TLS and one configured service credential mapped to one Catalog principal. Every non-health request must authenticate. v0.2 does not expose the Unity Catalog-compatible HTTP router over the network.

## Kernel Host Contract

The existing `Session::sql` behavior remains the embedded convenience API. The service adds an additive kernel-owned prepare/execute boundary so `vql-server` does not import a private VQL AST, infer behavior from the first SQL keyword, or execute a statement merely to discover its schema.

Conceptually, the public host API provides:

```text
Session::prepare(sql, principal, session_settings) -> PreparedStatement
Session::prepare_pinned(sql, principal, session_settings, generations) -> PreparedStatement
PreparedStatement::statement_info() -> StatementInfo
PreparedStatement::result_schema() -> Arrow Schema
PreparedStatement::definition_generations() -> GenerationSet
PreparedStatement::execute_query() -> QueryHandle
PreparedStatement::execute_update() -> UpdateResult
```

The concrete Rust API may use different names, but it preserves these contracts:

- Preparation parses, classifies, associates the configured principal, captures one immutable `DefinitionSnapshot`, and plans query-shaped statements.
- Preparation performs no Catalog mutation, DML write, model resolution, connector execution, or Python invocation.
- DDL preparation validates syntax and authorization intent; provider validation and the Catalog transaction occur during execution.
- A prepared statement owns its classification, result schema, Session settings, definition snapshot, opaque definition generations, and logical plan.
- `prepare_pinned` loads one historical snapshot from an exact set of Catalog generations and resolves names only inside that snapshot. Missing or incompatible generations fail; it never substitutes current heads.
- One logical Session may have multiple prepared statements and concurrent executions. Cancellation is routed through the `QueryHandle` registered for one `query_id`.
- Preparing `SUBMIT QUERY` captures the normalized SQL, semantic settings, and definition generations but creates no job until execution commits the job transaction.

## Flight SQL Profile

v0.2 implements the smallest Flight SQL profile required by one selected Flight SQL or ADBC client integration. Its compatibility test, not a speculative capability matrix, defines the required metadata calls and prepared-statement behavior. Unsupported optional Flight SQL operations return a standard unimplemented status.

The required product behavior is:

- bounded queries return Arrow batches through `DoGet`;
- ordinary unbounded queries return an attached `DoGet` stream;
- updates execute once and return their affected-row result after completion;
- every execution receives a server-generated query ID;
- the client can cancel that exact query ID without affecting other executions;
- scripts prepare and execute statements sequentially because later statements may depend on earlier DDL.

Prepared result-schema metadata carries only the classification clients need:

```text
vql.statement_info.version = 1
vql.statement_info.kind = query | update | persistent_submission
vql.statement_info.query_mode = bounded | unbounded | not_applicable
vql.statement_info.result_mode = bounded | unbounded | none
```

`SUBMIT QUERY` is an unbounded computation with a bounded result. Its returned row confirms persistence; it is not the result stream of the submitted computation.

An attached execution has a bounded interval in which its first result stream must attach. Dropping its last result stream cancels its kernel `QueryHandle`. Losing an unrelated RPC or channel does not cancel another query. Persistent jobs are independent of the submitting Session.

Failures use standard gRPC status classes and the versioned structured VQL error representation defined by the [Error Code Design](./error_codes.md). Clients do not parse prose messages or ticket contents to discover a query ID.

## `IMAGE` Flight Boundary

The initial service has one bounded network representation for a projected `IMAGE`: its existing Arrow storage struct contains a server-generated JPEG thumbnail plus sanitized metadata. The server configuration bounds thumbnail dimensions, encoded cell bytes, batch bytes, and total result bytes. Original encoded media is never inserted implicitly.

Before an `IMAGE` crosses Flight:

- `buffer_id` and `buffer_slot` are NULL;
- URI user information, signed query parameters, and credentials are removed;
- the service-only locator is NULL;
- live RTSP and persistent media use the same thumbnail representation.

v0.2 has no `image_mode` SQL setting, `FRAME_AT` ticket, original-media `DoGet`, PTS substitution, or locator TTL. A query that needs durable evidence writes it to a persistent Table through the normal provider contract.

## Persistent Query SQL

The service adds these statements to the kernel-owned VQL parser:

```sql
SUBMIT QUERY people_per_minute AS
INSERT INTO people_sink SELECT ...;

SHOW QUERIES;
DESCRIBE QUERY '<query_id>';
STOP QUERY '<query_id>';
```

`SUBMIT QUERY` accepts exactly one unbounded `INSERT INTO <table> SELECT ...` whose destination has writable capability. The normalized name is unique among non-terminal jobs. State-changing operations use the server-generated query ID; the name is display and filtering metadata.

`SUBMIT QUERY` returns one bounded Arrow row after the job transaction commits:

```text
query_id Utf8,
name Utf8,
state Utf8
```

The minimum inspection columns are:

```text
SHOW QUERIES:
  query_id, name, state, source_health, last_event_time,
  started_at, updated_at, restart_gap_count,
  error_code, error_message

DESCRIBE QUERY <id>:
  query_id, name, state, sql_redacted,
  created_at, started_at, updated_at,
  last_restart_at,
  restart_gap_started_at, restart_gap_ended_at,
  last_restart_reset_window_state,
  error_code, error_message
```

Later protocol versions may append columns. Clients select fields by name and preserve unknown state strings.

`source_health` is NULL before a source starts and otherwise reports `connected` or `reconnecting`; clients preserve unknown later values. `restart_gap_count` increments once whenever daemon startup relaunches a previously active job. `last_event_time` is the newest event time accepted by the current process and is NULL before the first row.

## Job Record and Restart

A service-owned job record contains:

- query ID and normalized name;
- normalized SQL and a redacted display projection;
- the configured principal identifier;
- semantic Session settings;
- pinned opaque Catalog generations;
- lifecycle state, timestamps, restart-gap summary, and last error.

It does not contain a serialized DataFusion logical or physical plan, copied Catalog definitions, compiled model session, process-local frame reference, credential value, checkpoint blob, state-schema fingerprint, codec version, or engine state-format ABI.

On startup, every persisted `STARTING` or `RUNNING` job is normalized to `STARTING`. The service then:

1. loads the pinned generations from the Catalog;
2. rebuilds the prepared statement from normalized SQL and saved semantic settings;
3. prepares the SQL against that historical snapshot and fails if the running engine no longer supports it;
4. creates a new ingest clock and empty in-memory window state;
5. reconnects RTSP at the live position;
6. records the unavailable interval and whether open-window state was discarded;
7. enters `RUNNING`, or `FAILED` if rehydration or launch cannot complete.

The public states are `STARTING`, `RUNNING`, `STOPPED`, and `FAILED`. `STOP QUERY` commits an internal stop-request flag, cancels the active handle, releases runtime resources, and then commits `STOPPED`. Startup completes a recorded stop request without relaunching the job. Repeating `STOP QUERY` for a terminal job is idempotent. Transient cleanup steps and the stop-request flag do not create additional public states.

RTSP remains non-replayable. Restart loses unavailable source frames and open-window contributions; a Kafka write that was in flight at the crash may be absent or partially visible. `vqld` reports the gap and never describes this behavior as exactly once, at least once, or transparent recovery.

`restart_gap_started_at` is the last durably recorded event time before shutdown when one is available; otherwise it is NULL. `restart_gap_ended_at` is the first event time accepted from the replacement source connection. `last_restart_reset_window_state` is true when the query contains stateful streaming operators, because v0.2 cannot inspect the lost process memory after a crash.

## Security

- The configured credential maps to one principal; v0.2 has no user directory, role model, per-relation policy, or delegated identity.
- A logical Session is opaque, short-lived, and bound to that principal.
- Raw credentials, authorization metadata, URI user information, signed query parameters, and resolved secret values never enter job records, errors, metrics, or public logs.
- Query execution uses the normal secret-provider boundary at runtime; secrets are not persisted with a job.
- Service execution of a query that depends on a Python Function returns `FEATURE_NOT_AVAILABLE`. Embedded Python UDF behavior is unchanged.

## Metrics and Health

`SHOW QUERIES` and `DESCRIBE QUERY` are the product interfaces for query state, source health, last event time, restart gaps, and errors.

The optional Prometheus endpoint contains process and aggregate Session, query, inference, source, and sink metrics. It does not use `query_id` or job names as labels and is not a retained job-history API.

Liveness means the process event loop responds. Readiness requires the Catalog and job repository to open, migrations to complete, the instance lock to be held, and the Flight listener to accept work. RTSP, Kafka, model, or user Table failures affect their queries but do not make the daemon unready.

## Testing and Acceptance

Kernel owner tests verify:

- prepare has no Catalog mutation, DML write, model resolution, connector execution, or Python invocation;
- statement classification and result schemas match embedded semantics;
- prepared settings and definition snapshots do not change after later `SET` or Catalog mutations;
- per-`QueryHandle` cancellation remains independent for concurrent statements;
- service-mode Python Functions fail before execution begins.

Service tests verify:

- the exact Flight SQL operations required by the selected client integration;
- bounded and attached-unbounded execution, Session isolation, query IDs, precise cancellation, and structured errors;
- the single thumbnail representation and removal of process-local or credential-bearing fields;
- atomic job creation, discovery after disconnect, bounded history, and idempotent stop;
- restart of active RTSP jobs from the live position with fresh windows and an explicit gap;
- missing generations or preparation incompatibility cause failure without falling back to current Catalog heads;
- loopback defaults and rejection of non-loopback startup without TLS and a configured credential;
- aggregate metrics contain no per-query labels.

The end-to-end acceptance test runs the PRD Scenario C through the selected Flight SQL or ADBC client integration.

## Deferred Capabilities

The following capabilities require their own validated product need and design before entering a release:

- serialized window checkpoints, partial-window restoration, `PAUSE`, and `RESUME`;
- multi-user identity, relation-level authorization, audit, and a network UC API;
- original-media tickets, locator-based rereads, and multiple image transport modes;
- complete BI/JDBC metadata compatibility and a public capability matrix;
- service-side Python UDF workers;
- a chainable DataFrame API and Workbench-specific RPCs;
- clustering, high availability, distributed execution, and stronger delivery guarantees.

## References

- [Apache Arrow Flight SQL specification](https://arrow.apache.org/docs/format/FlightSql.html)
- [Workbench Design](./workbench.md)
