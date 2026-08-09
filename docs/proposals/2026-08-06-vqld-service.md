---
created_at: 2026-08-06
status: draft
target_version: v0.3
updated_at: 2026-08-09
---

# vqld Service: Flight SQL, Durable Jobs, and Recovery

## Summary

The v0.3 service-deployment and job-management feature introduces the `vqld` daemon, built by `vql-server`. Its only public surfaces are Arrow Flight SQL, public SQL system statements, health checks, and a Prometheus metrics endpoint. This proposal fixes the public Flight SQL contract, including capability negotiation, statement classification, and structured errors; the media protocol required by Workbench; the minimum system-query schemas; durable jobs created by `SUBMIT QUERY`; checkpoint and recovery behavior; and the continuous-query state machine.

## Motivation and Scope

The embedded v0.1 kernel does not listen on a port. In v0.3, `vqld` owns networking, TLS and authentication, durable job management, and recovery. Every client, including CLI `--server`, Workbench, ADBC, and JDBC, uses the contract in this document. The engine exposes no private management API. Any service capability must be implementable by an independent client against this public contract, consistent with ADR-010 and ADR-014 reserved in [design.md](../design.md). The additional service security boundary is defined below.

## Detailed Design

### Flight SQL Contract

`vql-server` implements the following public capabilities in v0.3:

- Flight SQL statement query/update, prepared statements, `GetSchema`, `GetCatalogs`, `GetDbSchemas`, `GetTables`, `GetTableTypes`, `GetSqlInfo`, and `PollFlightInfo` for long-running queries.
- Both bounded and unbounded statement-query results return Arrow batches through `DoGet`. An unbounded query returns consumable `FlightInfo` as soon as its schema and endpoint are ready, without waiting for completion. Cancellation uses `CancelFlightInfo`; a disconnected client also triggers the server cancellation token.
- Handshake returns a logical session token. `SET` values and temporary execution state belong to that session, not to the underlying gRPC channel. Channels may be reused, but clients must include the token on every RPC and the server must validate it before selecting a Session. A channel that once completed Handshake is not authorization.
- `SHOW STREAMS/MODELS/FUNCTIONS/SINKS` and `DESCRIBE` are public SQL. There are no private catalog RPCs.
- ADBC and JDBC use Flight SQL drivers rather than a second query or catalog protocol. A driver that does not understand `visionql.image` can still read its standard Struct storage type.
- Vendor `GetSqlInfo` fields publish the VisionQL client-protocol version, SQL-dialect version, `visionql.image` extension version, and capability names for independently released clients.

Vendor SqlInfo IDs are fixed starting at the specification-reserved value 10000:

| ID | Type | Meaning |
|---|---|---|
| 10000 | string | `visionql_protocol_version` |
| 10001 | string | `sql_dialect_version` |
| 10002 | string | `visionql_image_version` |
| 10003 | list<string> | Capability names |

`visionql_protocol_version` and `sql_dialect_version` are decimal `MAJOR.MINOR` strings. Clients parse them as two integers and must not compare them lexicographically. `visionql_image_version` is currently the integer version string `1`.

The capability set publishes at least `unbounded_do_get`, `poll_flight_info`, `cancel_flight_info`, `statement_info_v1`, `image_thumbnail_mode`, `frame_at_v1`, and `query_control_v1` when supported. Clients must ignore unknown capabilities.

After prepare succeeds, every prepared statement returns these stable fields in result-schema metadata. Update and DDL statements carry the same metadata on a zero-column schema:

```text
visionql.statement_info.version = 1
visionql.statement.kind = query | update | ddl | persistent_submission
visionql.query.mode = bounded | unbounded | not_applicable
visionql.statement.side_effect = read_only | write
```

`kind` selects the transport the client must use; it is not equivalent to the first SQL keyword:

| Statement | kind | mode | side_effect | Result |
|---|---|---|---|---|
| Bounded `SELECT`, `SHOW`, or `DESCRIBE` | `query` | `bounded` | `read_only` | statement-query / `DoGet` data |
| Ordinary unbounded `SELECT` | `query` | `unbounded` | `read_only` | Attached `DoGet` data stream |
| Bounded `INSERT` or DML | `update` | `bounded` | `write` | statement-update; affected rows after completion |
| Ordinary unbounded `INSERT INTO ... SELECT ...` | `query` | `unbounded` | `write` | Attached `DoGet` status stream |
| `SUBMIT QUERY ...` | `persistent_submission` | `unbounded` | `write` | statement-query; one durable-job row |
| Catalog DDL, `SET`, or job control | `ddl` | `not_applicable` | `write` | statement-update; metadata remains on a zero-column result schema |

The attached unbounded `INSERT` status schema is fixed as `query_id Utf8, lifecycle Utf8, state Utf8, definition_revision Utf8, updated_at Timestamp(Nanosecond, UTC)`. `lifecycle` is always `attached`. The stream emits one row at startup and one for each state change, and `DoGet` remains open until the query completes or is cancelled.

Every statement-query sets `FlightInfo.app_metadata` to UTF-8 JSON `VisionqlFlightInfoV1`, containing at least `{"version":1,"query_id":"...","statement_kind":"...","query_mode":"..."}`. Kind and mode must match the prepared-schema metadata, which remains the classification authority. Clients ignore unknown fields and never extract a query ID from ticket contents.

Prepare only parses, authorizes, and plans; it does not execute DDL, DML, or external I/O. This metadata lets clients choose statement-query or statement-update, identify unbounded execution, and choose cancellation behavior without copying the VQL parser. Because later script statements may depend on earlier DDL, clients prepare and execute sequentially and must not claim a global semantic preflight before the script begins.

Query failures use standard gRPC status for the broad class and carry a Protobuf-encoded `visionql.protocol.v1.VisionqlErrorV1` in `visionql-error-bin` trailing metadata:

```proto
message VisionqlErrorV1 {
  uint32 version = 1;          // fixed at 1
  string code = 2;
  string message = 3;
  optional string hint = 4;
  optional uint64 source_start = 5;
  optional uint64 source_end = 6;
  optional string query_id = 7;
  bool retryable = 8;
}
```

`code`, source span, and `retryable` are protocol fields; `message` and `hint` are human-readable. The source span is a half-open byte range `[source_start, source_end)` within the current statement's UTF-8 text and is omitted when no reliable location exists. Flight clients unaware of the extension can still consume the standard status. Workbench must read the envelope instead of parsing error strings and adds `statement_index` itself for multi-statement scripts.

The first v0.3 service release includes TLS, authentication, table/stream-level authorization, and the public job SQL `SUBMIT QUERY`, `SHOW/DESCRIBE QUERY`, `SHOW QUERY DEPENDENCIES`, and `PAUSE/RESUME/STOP`. There is no Workbench-only management RPC.

Unbounded-statement lifecycle is explicit:

- Ordinary unbounded `SELECT` and `INSERT INTO <sink> SELECT ...` attach to the current Flight session. Results or status continue through Flight and terminate on client cancellation, session expiry, or connection loss. They do not create a QueryJob, and a service upgrade must not silently change the lifecycle of the same SQL.
- Only explicit `SUBMIT QUERY <name> AS INSERT INTO <sink> SELECT ...` creates a durable job. Its statement-query result is one Arrow row with `query_id Utf8, name Utf8, state Utf8, definition_revision Utf8`. It returns as soon as planning and the Catalog transaction complete. `vqld` then manages the job independently of the Flight request.
- The current Flight request waits for bounded SELECT and INSERT completion. Console Sink cannot be the target of a durable service job.

### Media Protocol Required by Workbench

A Flight session supports three `IMAGE` result modes and locator-based frame retrieval:

```sql
SET vql.result.image_mode = 'reference'; -- default
SET vql.result.image_mode = 'thumbnail'; -- Workbench default; edge and quality are separate options
SET vql.result.image_mode = 'inline';    -- original encoded content, protected by a result-byte limit
```

All three modes use the same `visionql.image` Arrow storage schema. Only the presence and semantic marker of `encoded` change.

On-demand original-media reads use the PRD-defined function:

```sql
-- Open the frame identified by the locator.
SELECT TO_JPEG(FRAME_AT($1), 90);

-- Select another timestamp in the same video object.
SELECT TO_JPEG(FRAME_AT($1, $2), 90);
-- $1 = IMAGE.locator, $2 = target pts_ms
```

`FRAME_AT(locator STRING [, pts_ms BIGINT]) -> IMAGE` is an I/O-bearing built-in planned as `MediaFetchExec`. The locator already includes the current frame PTS; the optional second argument selects another timestamp only within the same authorized video object. The function rejects display URIs, parses only versioned locators, and reauthorizes the current principal against the embedded source revision and range. File and object-store references are rereadable under the locator invariants in [design.md](../design.md) §6.2. A live RTSP frame is available only while it remains in the server's bounded compressed-GOP ring. The ring evicts the oldest GOP by total bytes and TTL, is charged to the service resource budget, and does not affect query semantics. See the [Workbench proposal](./2026-08-05-workbench.md) for client behavior.

### Minimum System-query Schemas

To keep Workbench from parsing logs, v0.3 fixes these minimum columns. Later versions may append columns:

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

Job-control syntax is fixed as:

```sql
SUBMIT QUERY people_per_minute AS
INSERT INTO people_sink SELECT ...;
DESCRIBE QUERY '<query_id>';
SHOW QUERY DEPENDENCIES '<query_id>';
PAUSE QUERY '<query_id>';
RESUME QUERY '<query_id>';
STOP QUERY '<query_id>';
```

`query_id` is a server-generated UUID string. `SUBMIT QUERY` provides the durable job name, which is unique among the owner's non-terminal jobs. Attached queries have `name=NULL, lifecycle=attached`; durable jobs have `lifecycle=persistent`. Names are only for display and filtering and cannot replace IDs in state changes.

Query metrics do not have a system-SQL channel. In addition to [design.md](../design.md) §12.3, v0.3 exposes a Prometheus endpoint with labels such as `query_id`, plus checkpoint duration and size, last successful epoch, and recovery count. This matches the PRD §3.8 cost panel. Metric names may evolve before v1.0 but must follow Prometheus naming and unit-suffix conventions; clients must not guess units from arbitrary strings.

### Durable-job Checkpoints and Recovery

v0.3 provides checkpoints and crash recovery for jobs created by `SUBMIT QUERY`. RTSP is not replayable, so the goal is not data replay. The contract is: **accumulated window state and watermark survive a crash; recovery resumes from the live position and reports the gap honestly.**

The coordinator selects a completed epoch as a checkpoint boundary based on elapsed time or state growth. Each boundary uses this protocol:

1. Starting from the state restored from the current checkpoint, apply the epoch and produce closed-window output.
2. Write output to the Sink and wait for every write acknowledgement.
3. Atomically persist a new checkpoint containing the logical-plan hash, Catalog definition snapshot, watermark, normalized window state, every `WindowStateCodec` version, and the Sink delivery sequence.

The checkpoint writes the normalized Arrow state defined in [design.md](../design.md) §5.4 and records operator ID, state-schema fingerprint, codec version, and engine state-format version. It does not call `state()` on the active accumulator. Recovery requires those fields to match the definition snapshot. Incompatibility moves the job to `state=FAILED, error_code=RECOVERY_INCOMPATIBLE`; it must not attempt best-effort deserialization.

Recovery behavior is:

- Restore window state and watermark from the newest successful checkpoint. Reconnect RTSP at the live position with a new `source_generation` and pass the time-continuity gate in [design.md](../design.md) §8.3. Create a new `IngestClock` after process recovery. If its first ingest time is below the recovered watermark, fail with `TIME_DISCONTINUITY`; do not mark every future frame late or clamp the clock to the watermark.
- Window results written after the checkpoint and before the crash may be emitted again. Delivery of acknowledged output is therefore at least once.
- Source data during crashes or disconnections cannot be recovered. Report the resulting gap through metrics and gap records without inventing rows.

Offset-based replay for replayable sources such as a future Kafka frame source is not scheduled. `SourceProgress` already belongs to the same checkpoint boundary, so that work extends offset-commit ordering without changing this structure. Exactly-once delivery is also unscheduled and would require checkpoint and transactional Sink commit in the same barrier; this protocol does not pretend to provide it.

### Continuous-query State Machine

```text
SUBMITTED → STARTING → RUNNING ⇄ PAUSED
                 │         │
                 ├────→ RECOVERING ───→ RUNNING
                 └────→ FAILED
RUNNING / PAUSED / FAILED → STOPPED
```

- `PAUSE` stops the source after the current epoch reaches a consistency boundary. Pausing RTSP creates an unrecoverable gap.
- `RESUME` continues only from the original definition snapshot and never replans implicitly. To adopt a new Function or Model revision, stop the old job and explicitly submit the original SQL as a new job, which receives a new `query_id` and definition snapshot.
- `STOP` is terminal. It releases model, source, and state resources while retaining job history.
- On service startup, restore jobs in `RUNNING` or `RECOVERING`: recover window state from a checkpoint and reconnect RTSP at the live position.

### Catalog Object and Security Boundary

The service adds one object to the Catalog objects in [design.md](../design.md) §7.2:

| Object | Key contents |
|---|---|
| QueryJob | Name, SQL, definition snapshot, state, checkpoint location, owner |

A `SUBMIT QUERY` name is immutable and unique within the owner's non-terminal jobs. State changes use only the server UUID. Durable jobs and checkpoints hold revision leases according to [design.md](../design.md) §4.3.

In addition to the embedded security constraints in [design.md](../design.md) §13, the service requires:

- Flight SQL uses TLS, and authenticated identities map to Catalog principals.
- Both query planning and media dereference enforce table/stream-level authorization. Hiding an object from a catalog list is insufficient. Revocation takes effect immediately and is not bypassed by a lease.
- `FRAME_AT` accepts only locators for registered sources. By default, the server blocks cloud metadata addresses, link-local addresses, and targets not explicitly allowed by configuration.
- `IMAGE.uri` is a sanitized display value. `IMAGE.locator` is the media location that can be reauthorized. Neither contains underlying credentials.
- Python UDFs run in an out-of-process worker with timeout, memory, and dependency controls. The worker is not a multi-tenant security sandbox.
- v0.3 has no audit log, but execution context retains principal, query ID, and object-revision fields so provenance can be added later.

## Relationship to the System Design

- `vqld` is built on the process-agnostic kernel in [design.md](../design.md) §11.1; the `vql-server` crate boundary is in §14.
- `IMAGE` transport follows the three payload states and locator invariants in §6.2, which also defines the fixed error-code set.
- Checkpoint boundaries use the epoch consistency boundary in §5. The recovery ABI for window state is `WindowStateCodec` in §5.4.
- Definition snapshots and revision leases follow §4.3 and §7.2.
- At-least-once delivery corresponds to ADR-008 in §15.

## Testing and Acceptance

Test all specified metadata RPCs, statement/prepared transport mappings, `statement_info_v1`, FlightInfo query IDs, attached unbounded status streams, disconnect cancellation, per-RPC session isolation, Protobuf error envelopes, `IMAGE` storage schema and version, TLS/authentication/authorization, `SUBMIT QUERY`, query detail/dependency/control SQL, all three `IMAGE` modes, locator tampering/revocation/expiry, `FRAME_AT`, and capability negotiation. Kill the process at every boundary before and after Sink acknowledgement and checkpoint persistence. Verify that normalized window state is not lost, acknowledged output can only be duplicated, and codec/version incompatibility fails safely.

## Open Questions

| Question | Evidence required | Deadline |
|---|---|---|
| `IMAGE` thumbnail and inline limits, and locator TTL | Bandwidth, usability, revocation, and expiry tests across Workbench, Python, and BI clients; reference-by-default and the `uri`/locator split are already fixed | Before the v0.3 Flight schema freeze |

## References

- [Apache Arrow Flight SQL specification](https://arrow.apache.org/docs/format/FlightSql.html)

## Changelog

| Date | Change |
|---|---|
| 2026-08-06 | Extracted from engine design v0.5.0 §5.6–§5.7 and §11.4–§11.6 |
| 2026-08-09 | Converted metadata to front matter, adopted date-based naming, and translated to English |
