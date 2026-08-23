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

Every relation provider occupies the Table namespace. Models and Functions share one callable namespace, while their Catalog kinds and lifecycle remain distinct. SQLite stores callable identity in a table whose primary key is `(catalog_name, schema_name, name)` and records the owning kind separately, so the database constraint prevents concurrent creates or renames from claiming one callable name. Object names are case-insensitive and normalize to lowercase, including names written as quoted SQL identifiers. `VQL_*` and the `vql.builtin` schema are release-managed reservations.

| Object | Stored definition |
|---|---|
| Table | Provider variant with its options, location or sanitized endpoint, optional credential reference, comment/properties/owner metadata, and the Arrow schema; capabilities are derived from the provider rather than stored |
| Model | Immutable expanded interface, comment, live named versions, the initial version declaration fingerprint, and one optional default pointer. Each version stores raw source, Runtime, flat options, declaration fingerprint, creation time, and an optional resolved contract with source/hash, execution, semantic fingerprint, and volatility |
| Function | Parameter names and types, inferred constant-only parameter names, return type, normalized SQL-macro expression or Python `module:function` entry point, and a semantic fingerprint |

Model calls are query syntax, not Function objects. The call target and selected version resolve from the statement's immutable definition snapshot.

### Callable Objects and Model Versions

Callable object identity is case-insensitive, but Model version names are case-sensitive strings. The version name `default` is reserved case-insensitively because `default_version` is the aggregate's publication pointer, not a version alias.

`CREATE MODEL` persists the expanded interface and an initial version declaration. Later `ADD VERSION` mutations inherit that exact interface and cannot re-expand a capability preset or declare another signature. Resolving a version pins its execution contract; later revisions of that Model must preserve the resolved entry byte-for-byte. Local and cached artifacts are immutable by digest. Service-backed versions are recorded as volatile because their routing version cannot prove that the server will retain the same weights.

Resolving the exact initial declaration establishes the first default. An added version never publishes itself, and dropping then reusing the initial version name does not restore that right. Moving the default is an explicit atomic mutation that may target only a live resolved version. The default version and the last live version cannot be dropped; callers must publish another version first or drop the whole Model.

Function bodies store computation rather than Model ownership. Model references in SQL functions resolve from the query snapshot when the body expands, so dropping a referenced Model does not cascade into Function deletion. Function definitions also persist parameters inferred to occupy constant-only Model positions.

The `vql.builtin` schema is reserved for release-managed identities. User DDL cannot create, alter, rename, or drop objects in that schema. The v0.1 YOLO26 classifier and detector used by `VQL_CLASSIFY` and `VQL_EXTRACT` are kernel-owned rather than Catalog Models, so they do not appear in Catalog snapshots, Unity Catalog responses, `SHOW MODELS`, or internal object-revision history.

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

Every definition mutation commits atomically. The SQLite backend keeps `catalogs` and `schemas` rows for the namespace, an append-only `object_revisions` log holding each definition as JSON plus its Arrow IPC schema, and an `objects` table pointing every live `(catalog, schema, kind, name)` at its head generation. Each newly created Table, Model, or Function starts at object revision 1; only mutations of that same live object increment its revision. A separate global generation is an opaque storage key used for exact historical lookup and compare-and-swap, never a SQL or Unity Catalog field. Revision rows are never rewritten; `DROP` appends a tombstone revision and removes the object head so new planning cannot resolve the name. Model mutations compare-and-swap the opaque head generation and verify that every already-resolved version is carried forward byte-for-byte. Dropping a version removes it from the live head while history retains it; a later explicit add may reuse the version name.

A reader captures one `DefinitionSnapshot` containing current Tables, Model aggregates, and Functions. The snapshot is immutable and detached from the store, so later mutations—including moving a Model default—are invisible to everything already holding one. That is the whole consistency contract the Catalog offers; how a planned statement pins and consumes a snapshot belongs to the [Kernel Design](./kernel.md#immutable-query-definition-snapshot).

Object revisions and storage generations are internal Catalog state and do not appear in DDL results or `SHOW` output. Public Model versions are named entries inside the Model aggregate and are independent of internal object revisions. SQLite initializes the current schema directly and defines no legacy import or migration chain.

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
