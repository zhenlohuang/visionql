# VisionQL v0.1 Catalog Design

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

Every relation provider occupies the Table namespace. Models and Functions have separate namespaces. v0.1 object names are case-insensitive and normalize to lowercase, including names written as quoted SQL identifiers. `RESOLVE MODEL detector` is the explicit transition from a Model declaration to a resolved execution contract.

| Object | Stored definition |
|---|---|
| Table | Provider, capabilities, location or sanitized endpoint, options, Arrow schema, internal revision, credential reference |
| Model | Type, raw `FROM` location, Runtime and Runtime-scoped `WITH` options, plus an optional resolved artifact or endpoint contract |
| Function | DataFusion signature, normalized SQL expression or Python entry point, volatility, implementation digest |

Typed inference calls are query syntax, not Function objects. Their constant Model names resolve from the statement's immutable definition snapshot.

## Tables and Provider Capabilities

Provider metadata determines whether a Table is readable, writable, bounded, and durable.

| Provider | Readable | Writable | Bounded | Durable |
|---|---:|---:|---:|---:|
| `IMAGES` | yes | no | yes | yes |
| `VIDEOS` | yes | no | yes | yes |
| `RTSP` | yes | no | no | no |
| `KAFKA` | no | yes | no | no |

RTSP and Kafka appear in the Unity Catalog API as `EXTERNAL` Tables with `visionql.provider` and `visionql.{readable,writable,bounded,durable}` properties. They are not Unity Catalog `STREAMING_TABLE` objects because VisionQL does not implement that managed lifecycle.

The SQL surface follows the [Spark data-source table shape](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-datasource.html):

```sql
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

Every definition mutation commits atomically. The backend stores current object heads and append-only internal revisions. `DROP` writes a tombstone and prevents new planning. Table generations referenced by media locators remain identifiable, but a new media read must still reauthorize against current Catalog state.

Planning captures one `DefinitionSnapshot` containing the current Tables, Models, and Functions. A query-specific DataFusion session is populated from that snapshot, and resolved Model and provider specifications are copied into the planned statement. Later mutations affect new plans only.

Internal revisions support consistent execution and compare-and-swap Model resolution. They are not a public object-versioning API. SQLite initializes the current schema directly; v0.1 defines no legacy import or migration chain.

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
