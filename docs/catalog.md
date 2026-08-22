# VisionQL Catalog Design

> This document defines the `vql-catalog` domain, persistence, snapshot, and Unity Catalog-compatible API contracts. The [High-Level Design](./high_level_design.md) defines its place in the system; the [Kernel Design](./kernel.md) defines how planned statements consume snapshots.

## Boundary

`vql-catalog` owns:

- Catalog, Schema, Table, Model, and Function definitions;
- namespace resolution and provider capability metadata;
- transactional mutations and immutable definition snapshots;
- the `CatalogBackend` storage port and SQLite implementation;
- Unity Catalog wire models and HTTP translation.

It does not own media bytes, model artifacts, resolved credentials, connector sessions, query execution, or durable jobs. It has no dependency on DataFusion, media/model runtimes, Kafka, PyO3, or CLI behavior.

## Namespace and Objects

The default namespace is `vql.default`:

- `vql` is the default Catalog;
- `default` is the default Schema;
- an unqualified SQL name such as `photos` resolves as `vql.default.photos`.

Every relation provider occupies the Table namespace. Models and Functions have separate namespaces. Object names are case-insensitive and normalize to lowercase, including names written as quoted SQL identifiers. `RESOLVE MODEL detector` is the explicit transition from a Model declaration to a resolved execution contract.

| Object | Stored definition |
|---|---|
| Table | Provider variant with its options, location or sanitized endpoint, optional credential reference, comment/properties/owner metadata, and the Arrow schema; capabilities are derived from the provider rather than stored |
| Model | Type, raw `FROM` location, Runtime kind, Runtime-scoped `WITH` options, declaration fingerprint, plus an optional resolved specification holding the resolved source, artifact hash, execution mode, semantic fingerprint, and volatility |
| Function | Parameter names and types, return type, normalized SQL-macro expression or Python `module:function` entry point, and a semantic fingerprint |

Typed inference calls are query syntax, not Function objects. Their constant Model names resolve from the statement's immutable definition snapshot.

## Tables and Provider Capabilities

Provider metadata determines whether a Table is readable, writable, bounded, and durable.

| Provider | Readable | Writable | Bounded | Durable |
|---|---:|---:|---:|---:|
| `IMAGES` | yes | no | yes | yes |
| `VIDEOS` | yes | no | yes | yes |
| `RTSP` | yes | no | no | no |
| `KAFKA` | no | yes | no | no |
| `EXTERNAL` | no | no | yes | yes |

`EXTERNAL` exists only for Tables registered through the Unity Catalog API with no `visionql.provider` property. It holds an optional data-source format and storage location, and it declares no readable or writable capability, so it carries metadata without describing a data path.

RTSP and Kafka appear in the Unity Catalog API as `EXTERNAL` Tables with `visionql.provider` and `visionql.{readable,writable,bounded,durable}` properties. They are not Unity Catalog `STREAMING_TABLE` objects because VisionQL does not implement that managed lifecycle.

The SQL surface follows the [Spark data-source table shape](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-datasource.html):

```sql
CREATE TABLE photos
USING IMAGES
LOCATION '/data/photos'
OPTIONS (recursive = true);

CREATE TABLE clips
USING VIDEOS
LOCATION '/data/clips'
OPTIONS (fps = 2, start_time = '2026-01-01T00:00:00Z');

CREATE TABLE camera
USING RTSP
OPTIONS (
  url = 'rtsp://camera.example/live',
  fps = 5,
  event_time = 'capture_time',
  watermark = '2 seconds',
  transport = 'tcp'
);

CREATE TABLE events (
  window_start TIMESTAMP,
  people BIGINT
)
USING KAFKA
OPTIONS (
  bootstrap_servers = 'broker:9092',
  topic = 'people-per-minute',
  format = 'json',
  credential_ref = 'secret://kafka/producer'
);
```

Directory providers take their root from `LOCATION` and only tuning options from `OPTIONS`; endpoint providers take everything from `OPTIONS` and reject `LOCATION`. Every option is validated before commit, so a stored definition is always one the providers can execute.

| Provider | Option | Contract |
|---|---|---|
| `IMAGES`, `VIDEOS` | `LOCATION` | Required absolute path to an existing, readable local directory, canonicalized at DDL time |
| `IMAGES`, `VIDEOS` | `recursive` | Optional boolean; defaults to false |
| `VIDEOS` | `fps` | Optional sampling target; defaults to 1, must be greater than 0 and at most 120 |
| `VIDEOS` | `start_time` | Optional RFC 3339 timestamp anchoring frame time to real event time; without it the frame timestamp is synthesized from the Unix epoch and the field carries `visionql.synthetic_event_time` |
| `RTSP` | `url` | Required absolute `rtsp://` URL with a host. Embedded credentials, query strings, and fragments are rejected so they cannot reach storage |
| `RTSP` | `fps` | Optional sampling target; defaults to 5, must be greater than 0 and at most 120 |
| `RTSP` | `event_time` | Optional `'capture_time'` (default) or `'ingest_time'` |
| `RTSP` | `watermark` | Optional non-negative delay written as a number plus `milliseconds`/`seconds`/`minutes` (or `ms`/`s`/`m`); defaults to `2 seconds` |
| `RTSP` | `transport` | Optional `'tcp'` (default) or `'udp'` |
| `KAFKA` | `bootstrap_servers` | Required comma-separated `host:port` endpoints, bracketing IPv6 literals; URI schemes and credentials are rejected |
| `KAFKA` | `topic` | Required existing topic name; 1–249 ASCII letters, digits, `.`, `_`, or `-`, excluding `.` and `..` |
| `KAFKA` | `format` | Optional, defaults to `json`; every other format is rejected |
| `KAFKA` | `credential_ref` | Optional opaque reference of at most 1024 characters. Only the reference is stored; resolution belongs to execution |
| `KAFKA` | `delivery_timeout_ms` | Optional; defaults to 30,000 and accepts 1–3,600,000 |
| `KAFKA` | `buffer_capacity` | Optional; defaults to 1,024 and accepts 1–100,000 |

How the engine acts on these values — sampling, watermarks, reconnects, delivery, and backpressure — belongs to the [Kernel Design](./kernel.md#table-providers).

Foreground results are a host concern; there is no Console Table provider.

## Catalog Location

`VQL_HOME` is the host-wide local root, not a Catalog-owned directory. Resolution order is an explicit host-supplied `EngineConfig`, a non-empty `VQL_HOME`, then `$HOME/.vql`. If `HOME` is unavailable, the fallback is `.vql` in the current directory. Repository development uses `VQL_HOME=./data/.vql` so tests and examples do not modify user state.

```text
$VQL_HOME/
├── config.toml
├── catalog/
│   └── vql.db
├── history
└── cache/
    └── models/
```

The Catalog owns only `catalog/vql.db` in this layout. Hosts own `config.toml` loading and history; the model runtime owns `cache/models`. `config.toml` is optional and uses schema `version = 1`. Relative paths resolve from `VQL_HOME`, and an explicit Rust or Python host configuration may override the SQLite path before constructing `CatalogStore`.

## Persistence and Snapshots

`CatalogBackend` is the storage port. `CatalogStore` provides default-namespace operations over any `Arc<dyn CatalogBackend>`. The `sqlite` feature implements the port at `$VQL_HOME/catalog/vql.db`. Other backends must preserve the same domain, transaction, snapshot, and wire-translation contracts.

Every definition mutation commits atomically. The SQLite backend keeps `catalogs` and `schemas` rows for the namespace, an append-only `revisions` log holding each definition as JSON plus its Arrow IPC schema, and an `objects` table pointing every live `(catalog, schema, kind, name)` at its head revision. Revision rows are never rewritten; `DROP` appends a tombstone revision and removes the object head so new planning cannot resolve the name. Table generations referenced by media locators remain identifiable, but a new media read must still reauthorize against current Catalog state.

A reader captures one `DefinitionSnapshot` containing the current Tables, Models, and Functions. The snapshot is immutable and detached from the store, so later mutations are invisible to everything already holding one. That is the whole consistency contract the Catalog offers; how a planned statement pins and consumes a snapshot belongs to the [Kernel Design](./kernel.md#immutable-query-definition-snapshot).

Internal revisions support consistent execution and compare-and-swap Model resolution. They are not a public object-versioning API. SQLite initializes the current schema directly and defines no legacy import or migration chain.

Schemas are stored as Arrow IPC. Secrets, tokens, signed URLs, and plaintext credentials are never persisted; definitions contain only opaque secret references. Catalog output and `SHOW CREATE` sanitize endpoint information and secret references.

## Unity Catalog API

Wire models and paths follow the [Unity Catalog OpenAPI v0.6.0](https://github.com/unitycatalog/unitycatalog/blob/v0.6.0/api/all.yaml) core metadata surface under `/api/2.1/unity-catalog`:

| Object | Operations |
|---|---|
| Catalog | create, get, list, update, delete |
| Schema | create, get, list, update, delete |
| Table | create, get, list, delete |

List operations use `max_results`, `page_token`, and `next_page_token`. Errors use the UC `{ "error_code", "message" }` envelope and corresponding HTTP statuses. The Axum router is available as `vql_catalog::uc::http::router` when the `http` feature is enabled.

Credentials, grants, volumes, temporary table credentials, and registered-model versions are outside this compatibility surface.
