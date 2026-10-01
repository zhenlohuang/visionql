# VisionQL `vqld` Service Design

> This document defines the v0.2 `vql-server` crate and its `vqld` daemon. It extends the embedded contracts in the [High-Level Design](../high_level_design.md), [Kernel Design](./kernel.md), [Catalog Design](./catalog.md), and [Error Code Design](./error_codes.md) without moving SQL or Catalog semantics into the service host.

## Scope

`vqld` is a single-node network host for VisionQL. Its v0.2 product task is narrow: run the same bounded and continuous SQL as the embedded engine, and keep an explicitly submitted continuous Table write alive independently of the client that submitted it.

The v0.2 acceptance path is:

1. submit one RTSP-to-Kafka Job through `vqld`;
2. disconnect the client while the Job keeps running;
3. reconnect, find the same persistent Job, and inspect or stop it;
4. restart `vqld` and observe the active Job reconnect at the live RTSP position with a reported restart gap and fresh window state.

The service owns a tested Arrow Flight SQL profile, logical client Sessions, attached-execution registration, the persistent Job controller, process health, and aggregate operational metrics. The Catalog persists Job objects in the same backend as Table, Model, and Function definitions.

The v0.2 service does not provide serialized window checkpoints, `PAUSE` or `RESUME`, transparent state recovery, multi-user or relation-level authorization, a network Unity Catalog API, original-media lookup, Workbench, a chainable DataFrame API, service-side Python UDF execution, complete BI/JDBC compatibility, clustering, high availability, distributed workers, replay for RTSP, or exactly-once delivery.

## Ownership Boundaries

- `vql-kernel` owns parsing, statement classification, immutable definition snapshots, planning, bounded execution, the epoch coordinator, cancellation, media conversion, and model execution.
- `vql-catalog` owns Catalog, Schema, Table, Model, Function, and persistent Job objects; immutable definition revisions; mutable Job status; provider capabilities; SQLite persistence; and Unity Catalog-compatible wire models.
- `vql-server` owns network transport, logical Sessions, attached execution IDs, the persistent Job controller, restart orchestration, and service-level security.
- `vql-server` depends only on public APIs of `vql-kernel` and `vql-catalog`; neither lower crate depends on Flight, gRPC, or authentication middleware, and the Catalog Job contract contains no service controller or runtime types.
- CLI, Python, and third-party clients use public Flight SQL and SQL. No client receives a private engine or Job-management RPC.

`vqld` coordinates three control planes around one embedded `Engine`:

| Control plane | Ownership |
|---|---|
| Session registry | Authenticated logical Sessions, expiry, settings, and prepared statements required by the tested client profile |
| Execution registry | Service-owned per-execution IDs, Flight endpoints, attached lifecycle, and exact cancellation |
| Job controller | Catalog-backed Job objects, active kernel handles, lifecycle transitions, restart orchestration, and bounded terminal history |

## Local State and Process Model

`vqld` stores every durable Job definition and status through `vql-catalog` in the existing Catalog database. It has no separate service database:

```text
$VQL_HOME/
└── catalog/
    └── vql.db    # definitions and persistent Job objects
```

Only one `vqld` process may write one `VQL_HOME`. The daemon holds an instance lock for its lifetime. Job creation is committed before launch, so every accepted submission is discoverable after restart. Terminal Job history is retained under a configured count or age bound; active Jobs are never evicted.

A persistent Job is a first-class Catalog object kind with an operational lifecycle and a Job-only namespace. It belongs to the Catalog and Schema active when `SUBMIT JOB` is prepared. Ordinary bounded and attached-unbounded executions are not Catalog objects. Job names do not collide with Table or callable names, and the stable Catalog object ID is its public `job_id` and lifecycle identity.

The Catalog stores an immutable Job definition, dependency rows for the exact opaque generations from its definition snapshot, and a separately mutable Job status. Job status uses compare-and-swap rather than creating an append-only object revision for every runtime update. Jobs do not enter `DefinitionSnapshot`, definition-name resolution, or the Unity Catalog-compatible API.

Job creation validates every pinned generation and inserts the definition, dependencies, and initial `STARTING` status in one Catalog transaction. The append-only definition history retains those generations while live heads may still be changed or dropped for new executions. Such a mutation does not stop or retarget a persistent Job; termination remains explicit. The service does not copy full Table, Model, or Function definitions into another store and does not write the Catalog database directly.

## Public Network Surfaces

| Surface | Contract |
|---|---|
| Arrow Flight SQL | Query and update execution, the metadata needed by the selected clients, and per-execution cancellation |
| `/health/live` | Process liveness only; returns no configuration or dependency detail |
| `/health/ready` | Catalog including persistent Job storage, instance lock, and Flight-listener readiness |
| `/metrics` | Optional process and aggregate service metrics without per-query labels |

`vqld` binds both surfaces to loopback by default. A non-loopback Flight listener requires TLS and one configured service credential mapped to one Catalog principal. The HTTP health and metrics listener remains loopback-only because v0.2 does not provide HTTP TLS termination. Every non-health request must authenticate. v0.2 does not expose the Unity Catalog-compatible HTTP router over the network.

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
- One logical Session may have multiple prepared statements and concurrent executions. Cancellation is routed through the `QueryHandle` registered for one `execution_id`.
- Preparing `SUBMIT JOB` captures the normalized SQL, semantic settings, and definition generations but creates no Job object until execution commits the Catalog transaction.

## Flight SQL Profile

v0.2 implements the smallest Flight SQL profile required by one selected Flight SQL or ADBC client integration. Its compatibility test, not a speculative capability matrix, defines the required metadata calls and prepared-statement behavior. Unsupported optional Flight SQL operations return a standard unimplemented status.

`vql shell --endpoint <URI>` is a first-party client of this same public profile. Its `FlightBackend` performs one authenticated handshake, keeps the returned logical Session across sequential statements, routes execution from prepared schema metadata, and never imports `vql-server` internals. Omitting `--endpoint` selects the shell's independent embedded backend.

The required product behavior is:

- bounded queries return Arrow batches through `DoGet`;
- ordinary unbounded queries return an attached `DoGet` stream;
- updates execute once and return their affected-row result after completion;
- every attached execution receives a server-generated execution ID;
- the client can cancel that exact execution ID without affecting other executions;
- scripts prepare and execute statements sequentially because later statements may depend on earlier DDL.

Prepared result-schema metadata carries only the classification clients need:

```text
vql.statement_info.version = 1
vql.statement_info.kind = query | update | persistent_submission
vql.statement_info.query_mode = bounded | unbounded | not_applicable
vql.statement_info.result_mode = bounded | unbounded | none
```

`SUBMIT JOB` is an unbounded computation with a bounded result. Its returned row confirms persistence; it is not the result stream of the submitted computation.

An attached execution has a bounded interval in which its first result stream must attach. Dropping its last result stream cancels its kernel `QueryHandle`. Losing an unrelated RPC or channel does not cancel another execution. Persistent Jobs are independent of the submitting Session.

Failures use standard gRPC status classes and the versioned structured VQL error representation defined by the [Error Code Design](./error_codes.md). Clients do not parse prose messages or ticket contents to discover an execution ID.

## `IMAGE` Flight Boundary

The initial service has one bounded network representation for a projected `IMAGE`: its existing Arrow storage struct contains a server-generated JPEG thumbnail plus sanitized metadata. The server configuration bounds thumbnail dimensions, encoded cell bytes, batch bytes, and total result bytes. Original encoded media is never inserted implicitly.

Before an `IMAGE` crosses Flight:

- `buffer_id` and `buffer_slot` are NULL;
- URI user information, signed query parameters, and credentials are removed;
- the service-only locator is NULL;
- live RTSP and persistent media use the same thumbnail representation.

v0.2 has no `image_mode` SQL setting, `FRAME_AT` ticket, original-media `DoGet`, PTS substitution, or locator TTL. A query that needs durable evidence writes it to a persistent Table through the normal provider contract.

## Persistent Job SQL

The service adds these statements to the kernel-owned VQL parser:

```sql
SUBMIT JOB people_per_minute AS
INSERT INTO people_sink SELECT ...;

SHOW JOBS;
DESCRIBE JOB '<job_id>';
STOP JOB '<job_id>';
```

`SUBMIT JOB` accepts exactly one unbounded `INSERT INTO <table> SELECT ...` whose destination has writable capability. The normalized name is unique among non-terminal Job objects. State-changing operations use the server-generated `job_id`; the name is display and filtering metadata.

`SUBMIT JOB` returns one bounded Arrow row after the Job transaction commits:

```text
job_id Utf8,
name Utf8,
state Utf8
```

The minimum inspection columns are:

```text
SHOW JOBS:
  job_id, name, state, source_health, last_event_time,
  started_at, updated_at, restart_gap_count,
  error_code, error_message

DESCRIBE JOB <id>:
  job_id, name, state, sql_redacted,
  created_at, started_at, updated_at,
  last_restart_at,
  restart_gap_started_at, restart_gap_ended_at,
  last_restart_reset_window_state,
  error_code, error_message
```

Later protocol versions may append columns. Clients select fields by name and preserve unknown state strings.

`source_health` is NULL before a source starts and otherwise reports `connected` or `reconnecting`; clients preserve unknown later values. `restart_gap_count` increments once whenever daemon startup relaunches a previously active Job. `last_event_time` is the newest event time accepted by the current process and is NULL before the first row.

## Job Object and Restart

A Catalog-managed Job contains an immutable definition:

- job ID, normalized name, principal identifier, and creation time;
- normalized SQL and a redacted display projection;
- semantic Session settings;
- pinned opaque Catalog generations.

Its mutable status contains:

- public lifecycle state and a monotonic `status_version` used for compare-and-swap;
- the durable internal stop-request flag;
- source health, last event time, and lifecycle timestamps;
- restart-gap count and latest gap summary;
- the last structured error.

It does not contain a serialized DataFusion logical or physical plan, copied Catalog definitions, compiled model session, process-local frame reference, credential value, checkpoint blob, state-schema fingerprint, codec version, or engine state-format ABI.

`vql-catalog` owns Job validation, atomic persistence, status compare-and-swap, dependency integrity, listing, and terminal-history pruning. `vql-server` owns the allowed lifecycle transitions and all execution behavior. Runtime tasks update status against the expected `status_version`, so a late completion or failure cannot overwrite a concurrent stop request.

On startup, the Job controller first completes every recorded stop request as `STOPPED` without launching it. It loads every other persisted `STARTING` or `RUNNING` Job, normalizes it to `STARTING`, and then:

1. loads the pinned generations from the Catalog;
2. rebuilds the prepared statement from normalized SQL and saved semantic settings;
3. prepares the SQL against that historical snapshot and fails if the running engine no longer supports it;
4. creates a new ingest clock and empty in-memory window state;
5. reconnects RTSP at the live position;
6. records the unavailable interval and whether open-window state was discarded;
7. enters `RUNNING`, or `FAILED` if rehydration or launch cannot complete.

The public states are `STARTING`, `RUNNING`, `STOPPED`, and `FAILED`. `STOP JOB` first commits the stop-request flag with compare-and-swap, then the controller cancels the active handle, releases runtime resources, and commits `STOPPED`. Repeating `STOP JOB` for a terminal Job is idempotent. Transient cleanup steps and the stop-request flag do not create additional public states.

RTSP remains non-replayable. Restart loses unavailable source frames and open-window contributions; a Kafka write that was in flight at the crash may be absent or partially visible. `vqld` reports the gap and never describes this behavior as exactly once, at least once, or transparent recovery.

`restart_gap_started_at` is the last durably recorded event time before shutdown when one is available; otherwise it is NULL. `restart_gap_ended_at` is the first event time accepted from the replacement source connection. `last_restart_reset_window_state` is true when the query contains stateful streaming operators, because v0.2 cannot inspect the lost process memory after a crash.

## Security

- The configured credential maps to one principal; v0.2 has no user directory, role model, per-relation policy, or delegated identity.
- The Flight SQL Basic username is a compatibility field, not an identity selector. `vqld` authenticates the credential and always supplies the server-configured principal to the Kernel.
- A logical Session is opaque, short-lived, and bound to that principal.
- Raw credentials, authorization metadata, URI user information, signed query parameters, and resolved secret values never enter Job objects, errors, metrics, or public logs.
- Job execution uses the normal secret-provider boundary at runtime; secrets are not persisted with a Job.
- Service execution of a query that depends on a Python Function returns `FEATURE_NOT_AVAILABLE`. Embedded Python UDF behavior is unchanged.

## Metrics and Health

`SHOW JOBS` and `DESCRIBE JOB` are the product interfaces for Job state, source health, last event time, restart gaps, and errors.

The optional Prometheus endpoint contains process and aggregate Session, query, inference, source, and sink metrics. `vqld_persistent_jobs_active` reports the active Job count. It does not use `job_id` or Job names as labels and is not a retained Job-history API.

Liveness means the process event loop responds. Readiness requires the Catalog, including Job storage, to open, migrations to complete, the instance lock to be held, and the Flight listener to accept work. RTSP, Kafka, model, or user Table failures affect their queries but do not make the daemon unready.

## Testing and Acceptance

Kernel owner tests verify:

- prepare has no Catalog mutation, DML write, model resolution, connector execution, or Python invocation;
- statement classification and result schemas match embedded semantics;
- prepared settings and definition snapshots do not change after later `SET` or Catalog mutations;
- per-`QueryHandle` cancellation remains independent for concurrent statements;
- service-mode Python Functions fail before execution begins.

Catalog owner tests verify:

- Job creation validates and pins every definition generation in the same transaction;
- ordinary executions create no Job object, and Jobs never enter definition snapshots or the UC API;
- status compare-and-swap prevents stale runtime writers from overwriting a stop request;
- terminal-history pruning never evicts an active Job or its pinned dependencies.

Service tests verify:

- the exact Flight SQL operations required by the selected client integration;
- bounded and attached-unbounded execution, Session isolation, execution IDs, precise cancellation, and structured errors;
- the single thumbnail representation and removal of process-local or credential-bearing fields;
- atomic Job creation, discovery after disconnect, bounded history, and idempotent stop;
- restart of active RTSP Jobs from the live position with fresh windows and an explicit gap;
- missing generations or preparation incompatibility cause failure without falling back to current Catalog heads;
- loopback defaults, rejection of non-loopback Flight startup without TLS and a configured credential, and rejection of a non-loopback HTTP listener;
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
