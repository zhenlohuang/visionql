# VisionQL Catalog

`vql-catalog` is the catalog boundary shared by the embedded engine and future services. It owns catalog objects, immutable definition snapshots, backend ports, the SQLite backend, and the Unity Catalog-compatible wire API. Execution, media I/O, model loading, and Kafka connections remain in `vql-kernel`.

## Namespace

The default namespace is `vql.default`:

- `vql` is the default catalog;
- `default` is the default schema;
- an unqualified SQL name such as `photos` resolves as `vql.default.photos`.

Users therefore write `SELECT * FROM photos`; the fully qualified name is primarily an API and interoperability identity.

Models and Functions remain first-class VisionQL definitions in the same default namespace. `RESOLVE MODEL detector` remains the explicit model-resolution statement.

## Tables and providers

VisionQL exposes every relation endpoint as a Table. Provider and capability metadata determine how it can be used.

| Provider | Readable | Writable | Bounded | Durable |
|---|---:|---:|---:|---:|
| `IMAGES` | yes | no | yes | yes |
| `VIDEOS` | yes | no | yes | yes |
| `RTSP` | yes | no | no | no |
| `KAFKA` | no | yes | no | no |

RTSP and Kafka are returned through the Unity Catalog API as `EXTERNAL` tables with `visionql.provider` and `visionql.{readable,writable,bounded,durable}` properties. They are not reported as Unity Catalog `STREAMING_TABLE` objects: that type implies a managed streaming-table lifecycle that VisionQL does not implement.

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

INSERT INTO events
SELECT TUMBLE(ts, INTERVAL '1' MINUTE), COUNT(*)
FROM camera
GROUP BY 1;
```

There is no Console table. Foreground `SELECT` results are printed directly by the CLI.

## Unity Catalog API

The wire models and paths are pinned to the released [Unity Catalog OpenAPI v0.6.0](https://github.com/unitycatalog/unitycatalog/blob/v0.6.0/api/all.yaml) core metadata surface under `/api/2.1/unity-catalog`:

| Object | Operations |
|---|---|
| Catalog | create, get, list, update, delete |
| Schema | create, get, list, update, delete |
| Table | create, get, list, delete |

List operations use UC `max_results`, `page_token`, and `next_page_token`. Errors use the UC `{ "error_code", "message" }` envelope and corresponding HTTP statuses. The Axum router is available as `vql_catalog::uc::http::router` when the `http` feature is enabled.

This is an explicitly bounded compatibility claim: credentials, grants, volumes, temporary table credentials, registered-model versions, and other Unity Catalog resources are not implemented by this module yet.

## Backend boundary

`CatalogBackend` is the storage port. `CatalogStore` supplies default-namespace conveniences and accepts any `Arc<dyn CatalogBackend>`. The current `sqlite` feature implements that port at `$VQL_HOME/catalog/vql.db`; future MySQL and PostgreSQL implementations should implement the same port without changing SQL, snapshots, or the UC translation layer.

Because v0.1 is unreleased, SQLite initializes the current schema directly. There is no legacy-schema import, catalog format version, or migration chain.

Every definition mutation is transactional. The backend stores current object heads plus append-only internal revisions so planning can capture one immutable snapshot and model resolution can use compare-and-swap updates. Revisions are internal execution consistency data, not a public UC versioning API.
