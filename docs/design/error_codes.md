# Error Code Design

> This document defines the stable error identifiers shared by the kernel, Catalog, CLI, and Python binding. Component documents define where errors originate and how each host transports them.

## Public Contract

Every VisionQL error has four layers:

| Layer | Example | Contract |
|---|---|---|
| Identifier | `VQL-42001` | Stable machine-readable identity; clients branch on this value |
| Symbol | `INVALID_SQL` | Stable readable name for code and diagnostics |
| Message | `expected a statement` | Human-readable context; wording is not an API |
| Metadata | `target_version = "v0.2"` | Structured fields defined for a particular error |

The identifier format is `VQL-CCDDD`:

- `VQL-` is the product namespace.
- `CC` is a two-character error class aligned with established SQLSTATE classes where the semantics match.
- `DDD` is a VisionQL-owned condition within that class.
- The five-character suffix uses uppercase ASCII letters and digits. The complete identifier is always nine characters.

The SQLSTATE alignment makes broad classes recognizable without claiming that a `VQL-` identifier is itself a SQLSTATE value. VisionQL identifiers are the public contract; error enum ordinals, Rust source types, DataFusion messages, SQLite result codes, HTTP statuses, and process exit codes are not substitutes for them.

| Class | VisionQL domain |
|---|---|
| `02` | Lookup result |
| `0A` | Unsupported feature |
| `22` | Invalid input or option |
| `23` | Object identity or state conflict |
| `42` | SQL syntax or semantics |
| `53` | Insufficient resources |
| `55` | Missing execution prerequisite |
| `57` | Operator intervention |
| `58` | Catalog or execution-system failure |
| `XX` | Internal engine defect |

## Registry

| Identifier | Symbol | Meaning |
|---|---|---|
| `VQL-0A001` | `FEATURE_NOT_AVAILABLE` | Valid VisionQL surface whose implementation is unavailable; carries `target_version` and creates no Catalog object |
| `VQL-02001` | `NOT_FOUND` | Requested Catalog object or required entity does not exist |
| `VQL-22001` | `INVALID_ARGUMENT` | Function or public API argument violates its contract |
| `VQL-22002` | `INVALID_OPTION` | DDL, Runtime, or configuration option is invalid |
| `VQL-22003` | `INVALID_LOCATION` | Table, model, media, or artifact location is unusable |
| `VQL-23001` | `ALREADY_EXISTS` | An object of the requested kind already exists |
| `VQL-23002` | `NAME_CONFLICT` | Another callable kind or a reserved built-in owns the name |
| `VQL-23003` | `FAILED_PRECONDITION` | Current Catalog state or generation does not permit the operation |
| `VQL-42001` | `INVALID_SQL` | SQL syntax, semantic allowlist, or statement shape is invalid |
| `VQL-53001` | `RESOURCE_EXHAUSTED` | A Session reservation, bounded queue, or state cap was exceeded |
| `VQL-55001` | `PYTHON_HOST_REQUIRED` | Execution reached a Python Function without an installed Python host |
| `VQL-57001` | `QUERY_CANCELLED` | The query observed graceful stop or immediate cancellation |
| `VQL-58001` | `CATALOG_ERROR` | Catalog persistence or consistency failed |
| `VQL-58002` | `EXECUTION_ERROR` | Runtime, connector, media, model, or result-rendering execution failed |
| `VQL-XX001` | `INTERNAL_ERROR` | A VisionQL invariant was broken |

The registry is append-only. An identifier and symbol are never reassigned, and changing their meaning is a breaking contract change. New conditions receive a new identifier in the narrowest applicable class. External-system errors are translated at their owning boundary instead of leaking SQLite, DataFusion, FFmpeg, ONNX Runtime, Triton, Kafka, operating-system, or Python exception identities as VisionQL codes.

An identifier does not declare retryability. For example, `VQL-23003` may ask the caller to retry a compare-and-swap operation, while another failed precondition requires a state change. Clients use the identifier together with operation-specific documentation and structured metadata.

## Host Representation

Rust exposes `VqlError.code: ErrorCode`. `ErrorCode::as_str()` returns the identifier and `ErrorCode::symbol()` returns the readable name. `VqlError` retains the message, optional `target_version`, and a private source chain for diagnostics.

CLI and plain-text boundaries render:

```text
[VQL-42001] INVALID_SQL: expected a statement
```

The Python binding raises `visionql.VisionQLError`, a `RuntimeError` subclass with `code`, `symbol`, `message`, and `target_version` attributes. Its string form uses the same CLI representation.

The Catalog domain uses the same identifiers and text format. The Unity Catalog-compatible HTTP API is a protocol adapter: its `{ "error_code", "message" }` response retains the error names and HTTP statuses required by the pinned Unity Catalog contract. This wire-level compatibility mapping does not change the underlying `CatalogError` identifier.

## Failure Policy

Row-local decode, preprocessing, inference, and post-processing failures produce NULL by default and therefore do not expose a statement error code. `SET vql.on_error = 'fail'` promotes the failure to the applicable statement error.

Hard failures preserve the most specific VisionQL identifier across asynchronous, DataFusion, connector, and host boundaries. Wrapping an error may add source diagnostics but must not replace `RESOURCE_EXHAUSTED`, `QUERY_CANCELLED`, or another known condition with `EXECUTION_ERROR`.

Messages must name the failed subject and actionable constraint without including credentials, signed URL parameters, URI userinfo, secret values, or internal backtraces. Hosts may log the private source chain according to their diagnostic policy, but public rendering contains only the identifier, symbol, safe message, and defined metadata.
