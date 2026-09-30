<div align="center">
  <h1>VisionQL — A Data Engine for Physical AI</h1>
  <p>
    <a href="https://github.com/zhenlohuang/visionql/actions/workflows/ci.yml">
      <img alt="CI" src="https://github.com/zhenlohuang/visionql/actions/workflows/ci.yml/badge.svg?branch=main">
    </a>
    <img alt="Version 0.2.0" src="https://img.shields.io/badge/version-0.2.0-6f42c1">
    <img alt="Rust 1.88 or newer" src="https://img.shields.io/badge/Rust-1.88%2B-black?logo=rust">
    <img alt="Python 3.10 or newer" src="https://img.shields.io/badge/Python-3.10%2B-3776AB">
    <a href="LICENSE">
      <img alt="Apache-2.0 license" src="https://img.shields.io/badge/license-Apache--2.0-blue">
    </a>
  </p>
  <p>
    <a href="#quick-start">Quick start</a> ·
    <a href="#examples">Examples</a> ·
    <a href="#python-api">Python</a> ·
    <a href="#architecture">Architecture</a> ·
    <a href="ROADMAP.md">Roadmap</a> ·
    <a href="CHANGELOG.md">Changelog</a>
  </p>
</div>

## What is VisionQL

VisionQL is a unified batch and streaming engine for querying and processing multimodal data. With SQL, users can work with images, video files, and live video streams through the same query model. A chainable DataFrame API remains a future candidate rather than a scheduled contract.

> [!IMPORTANT]
> VisionQL v0.2.0 adds the single-node `vqld` service, Arrow Flight SQL access, and Catalog-backed persistent continuous Queries to the embedded batch and streaming engine introduced in v0.1. The focused Workbench visual SQL client remains planned for v0.3. See the [Roadmap](ROADMAP.md) for exact version boundaries and the [Changelog](CHANGELOG.md) for release notes.

## Why VisionQL

Physical AI systems continuously produce camera, vehicle, and robot data. VisionQL turns decoding, sampling, inference, and aggregation into a declarative query plan:

- **Replace one-off pipelines with queries.** Images become rows and sampled video frames become time-aware relations. Compose visual inference with familiar filters, joins, `UNNEST`, and aggregations instead of rebuilding orchestration for every question.
- **Optimize inference, not just SQL.** Model calls stay visible in the plan rather than hiding inside black-box UDFs. VisionQL can push down frame sampling and time predicates, batch inference, and avoid decoding columns the query never reads.
- **Develop on history, move to live data.** One query model covers bounded image/video data and unbounded RTSP camera streams, with attached execution or explicit persistent Table writes.
- **Keep execution close to the data.** Run the engine in-process or host it through the single-node `vqld` service without requiring media uploads, a scheduler, or a control plane.

The [PRD](docs/prd.md) covers target users, representative Physical AI workflows, product boundaries, and the longer-term batch/stream value proposition.

## Quick start

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install), installed via `rustup`; the repository selects its pinned toolchain automatically.
- Python 3.10 or newer.
- [FFmpeg 8](https://ffmpeg.org/download.html), including development libraries for the default native video build and `ffmpeg` / `ffprobe` on `PATH` for sample preparation.
- A C/C++ build toolchain and `make` for the bundled `librdkafka` build. Kafka TLS uses vendored OpenSSL and does not require a system `librdkafka` installation.

### Package availability

VisionQL v0.2.0 is currently distributed as source through the
[GitHub release](https://github.com/zhenlohuang/visionql/releases/tag/v0.2.0). The Python package has
not yet been published to PyPI. Build the repository from source for the Python API, standalone
`vql` CLI, examples, and system-test assets.

### Build from source

```bash
git clone https://github.com/zhenlohuang/visionql.git
cd visionql

export VQL_HOME="$PWD/data/.vql"
python -m venv .venv
source .venv/bin/activate
cargo build --workspace --locked
python scripts/fetch_datasets.py
```

Open the SQL shell:

```bash
cargo run -q -p vql-cli -- shell
```

Then run a query against the downloaded COCO sample:

```sql
CREATE TABLE sample_images
USING IMAGES
LOCATION './data/datasets/images/coco128/images'
OPTIONS (recursive = true);

SELECT uri, width, height
FROM sample_images
ORDER BY uri
LIMIT 5;
```

VisionQL persists the table definition, so a new shell session can query `sample_images` without registering it again.

### Build the container image

The image contains both `vqld` and the `vql` CLI, persists local state under
`/var/lib/visionql`, and starts `vqld` by default:

```bash
docker build -t visionql .
docker run --rm visionql vql --version
docker run --rm -it \
  --mount source=visionql-home,target=/var/lib/visionql \
  visionql vql shell
```

The daemon keeps its secure listener defaults inside the container. To publish Flight SQL outside
the container, mount a certificate and key, set `VQLD_SERVICE_TOKEN`, and explicitly bind
`VQLD_FLIGHT_ADDR` to a non-loopback address. The HTTP health endpoint remains loopback-only and is
used by the image health check.

### Run `vqld`

The single-node service listens for Arrow Flight SQL on `127.0.0.1:6031` and exposes health and aggregate metrics on `127.0.0.1:6032`:

```bash
export VQL_HOME="$PWD/data/.vql"
cargo run -p vql-server --bin vqld

curl http://127.0.0.1:6032/health/live
curl http://127.0.0.1:6032/health/ready
```

Use a Flight SQL client to prepare and execute ordinary SQL or manage a persistent continuous Table write:

```bash
cargo run -q -p vql-cli -- shell --endpoint http://127.0.0.1:6031
```

The same shell front end now has two execution backends: without `--endpoint` it embeds `vql-kernel::Session`; with `--endpoint` it executes through Flight SQL against `vqld`. Loopback development needs no client environment variables. For a secured endpoint, pass `--token <token>`; if the flag is absent, the shell falls back to `VQLD_SERVICE_TOKEN`. The daemon maps that credential to its configured principal. Use `--tls-ca <ca.pem>` when the server certificate requires an additional CA. Prefer the environment fallback when the token must not appear in process arguments.

```sql
SUBMIT QUERY people_per_minute AS
INSERT INTO people_sink SELECT ... FROM cam_entrance;

SHOW JOBS;
DESCRIBE QUERY '<query_id>';
STOP QUERY '<query_id>';
```

Loopback development accepts a Flight SQL handshake with an empty credential and maps it to the daemon's configured principal, `service` by default. Logical Sessions expire after 15 idle minutes by default. For non-loopback Flight access, configure `VQLD_SERVICE_TOKEN`, `--tls-cert`, and `--tls-key`; startup rejects an externally bound Flight listener without all three security inputs. The HTTP health and metrics listener remains loopback-only because v0.2 does not terminate TLS for that surface. Run `vqld --help` for Session and attach timeouts, result limits, Query-history retention, and listener options.

## Examples

### Typed visual inference

The v0.1 contract registers a Model and calls that Model by name. Its persisted interface fixes the typed arguments and result, while inference remains visible to the optimizer.

```sql
CREATE MODEL yolo TYPE OBJECT_DETECTION
FROM 'file:///models/yolo.onnx';

RESOLVE MODEL yolo;

SELECT uri,
       yolo(image,
         classes => ['person'],
         min_confidence => 0.5
       ) AS detections
FROM sample_images;
```

`CREATE MODEL` is local and free of I/O. An `.onnx` source selects ONNX Runtime; other unambiguous source schemes have fixed Runtime defaults, and `USING` is required otherwise. `RESOLVE MODEL` introspects the graph, downloads and verifies remote artifacts when needed, and publishes the first version. Flat `OPTIONS` supply only facts introspection cannot determine, such as `sha256`, `format`, `labels`, or a dynamic `image_size`.

Use `SHOW MODELS`, `SHOW MODEL VERSIONS yolo`, and `DESCRIBE MODEL yolo` to inspect the aggregate, versions, and callable interface. `SHOW CREATE TABLE|MODEL|FUNCTION <name>` returns sanitized canonical DDL; credential references are redacted.

`SHOW CREATE MODEL yolo` returns a complete `CREATE MODEL` declaration for the most recently added live version, independently of the default version. Use `SHOW CREATE MODEL yolo VERSION 'v1'` to inspect an exact version. Both include an explicit `VERSION` in the DDL and the actual `version` in the result; missing Models or versions return `NOT_FOUND`. `SHOW MODEL VERSIONS yolo` lists version state and the default marker.

### Built-in AI functions

The v0.1 built-in AI surface is organized by task shape: `VQL_CLASSIFY` judges a whole input,
`VQL_EXTRACT` extracts user-named fields, and `VQL_DETECT` discovers instances. Install the
release-managed YOLO26n ImageNet classifier and COCO detector under the active `VQL_HOME`:

```bash
python scripts/export_yolo26.py --task classify --install
python scripts/export_yolo26.py --task detect --install
```

Then use either function without creating or resolving a Catalog Model:

```sql
SELECT uri,
       VQL_CLASSIFY(image, ['football_helmet']) AS categories,
       VQL_DETECT(image,
         classes => ['person'],
         min_score => 0.5
       ) AS detections
FROM sample_images;
```

`VQL_CLASSIFY` also reserves a STRING overload. `VQL_EXTRACT` accepts IMAGE or STRING plus a
planning-time field map such as
`MAP {'title': 'What is the title?', 'items': STRUCT('List the items' AS question, TRUE AS list)}`.
Those overloads currently return `FEATURE_NOT_AVAILABLE`; IMAGE execution is available for
`VQL_CLASSIFY` and `VQL_DETECT`.

### Model sources

| Source | Example | Behavior |
|---|---|---|
| Local ONNX artifact | `'./model.onnx'` or `'file:///models/model.onnx'` | `RESOLVE MODEL` hashes it in place; no cache copy |
| Hugging Face artifact | `'hf://owner/repository@<commit>/model.onnx'` | `RESOLVE MODEL` downloads and caches it by digest |
| Inference service | `'triton+https://triton-prod:8000/detector@42'` | `RESOLVE MODEL` validates the service contract; service versions are volatile |

```sql
CREATE MODEL production_detector TYPE OBJECT_DETECTION
FROM 'triton+https://triton-prod:8000/detector@42';

RESOLVE MODEL production_detector;
```

`ONNX_RUNTIME` and `TRITON_INFERENCE_SERVER` are the v0.1 Runtime paths. ONNX resolution derives tensor binding and processing from graph metadata plus flat `OPTIONS`; Triton owns preprocessing and postprocessing and exposes the canonical capability result.

Set `HF_TOKEN` when resolving a private Hugging Face bundle. The resolver reads it from the process environment only during `RESOLVE MODEL`; it is never persisted in the Catalog or visible Model DDL.

### RTSP streaming

The v0.1 streaming path registers one live camera and runs an attached query until the client cancels it. FFmpeg decodes on a controlled worker, sampling uses event time, watermarks advance outside data rows, and disconnects retry with exponential backoff.

```sql
CREATE TABLE cam_entrance
USING RTSP
OPTIONS (
  url = 'rtsp://10.0.0.15:554/main',
  fps = 5,
  event_time = 'capture_time',
  watermark = '2 seconds',
  transport = 'tcp'
);

SELECT ts, frame_id, source, frame
FROM cam_entrance;

SELECT TUMBLE(ts, INTERVAL '10' SECOND) AS window_start,
       COUNT(*) AS frames,
       MIN(frame_id) AS first_frame,
       MAX(frame_id) AS last_frame
FROM cam_entrance
GROUP BY 1;
```

The shell and `vql run` print unbounded results incrementally. During embedded execution, the first Ctrl-C stops source intake, drains admitted epochs, and flushes an attached table write; press Ctrl-C again while that shutdown is in progress to cancel immediately. A remote shell instead cancels the exact active Flight execution. An unbounded statement must be last in a `vql run` script so stopping it cannot start later SQL. `Projection`, `Filter`, `UNNEST`, scalar functions, typed inference, and one `TUMBLE` aggregate are accepted. Streaming windows support `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`; they close only after the watermark reaches the window end. `DISTINCT`, media-valued state, and unsupported unbounded plan shapes are rejected during planning. Live frames use epoch-scoped frame buffers internally and are encoded before crossing the result boundary. Cataloged endpoints currently reject embedded credentials and query parameters so secrets cannot be persisted accidentally.

Kafka output is a writable table declared with `CREATE TABLE ... USING KAFKA`. Authentication uses an opaque `credential_ref`; embedding hosts install a `SecretProvider` on `EngineConfig`, and resolved credentials never enter the Catalog or SQL text. `KafkaAuthentication` and `KafkaTlsConfig` are VisionQL-owned public types supporting TLS/mTLS, SASL/PLAIN, SCRAM-SHA-256/512, and static OAUTHBEARER tokens without exposing the internal Kafka client. The repository's local Compose service uses plaintext Kafka and does not require a reference.

## Python API

The example below reuses the `sample_images` table and `VQL_HOME` from [Quick start](#quick-start). Build the Python extension into an active virtual environment with [Maturin](https://www.maturin.rs/):

```bash
export VQL_HOME="$PWD/data/.vql"
source .venv/bin/activate
python -m pip install "maturin>=1.9,<2"

cd vql-python
maturin develop --locked
cd ..
```

The synchronous API returns a `QueryHandle`; `collect()` materializes a PyArrow table.

```python
import visionql

session = visionql.connect()
result = session.sql("""
    SELECT uri, width, height
    FROM sample_images
    ORDER BY uri
    LIMIT 5
""")

table = result.collect()
print(table)
```

Python-hosted UDFs receive and return Arrow arrays in batches. They require the Python host and are not available in the standalone CLI.

## CLI

```text
vql shell [--endpoint URI] [--token TOKEN] [--tls-ca PEM]
vql run <script.sql>
```

SQL `EXPLAIN` adds bounded/continuous mode, source pushdowns, stream topology, resolved inference semantics, and Table-write placement without opening sources or probing models and services. Run it through the shell or a SQL script like any other statement. During source development, replace `vql` with `cargo run -p vql-cli --`. Set `VQL_LOG_LEVEL=debug` when diagnosing execution.

In `vql shell`, enter `\q` on its own line or press Ctrl-D to exit. The shell uses the embedded engine unless `--endpoint` selects `vqld`. Ctrl-C clears pending input at the prompt; embedded unbounded queries support graceful stop followed by immediate cancellation, while a remote active query is cancelled by its Flight execution ID.

## Runtime state

VisionQL keeps configuration and local state under `VQL_HOME`, which defaults to `$HOME/.vql`. The optional `$VQL_HOME/config.toml` is loaded by the CLI and Python host; missing settings use the defaults below. The Catalog makes table, model, and function definitions reusable across sessions; planning takes one immutable definition snapshot so later DDL cannot change a running query.

```toml
version = 1

[log]
level = "info"

[catalog]
backend = "sqlite"

[catalog.sqlite]
path = "catalog/vql.db"

[kernel.session]
memory_limit = "512 MiB"
```

Relative paths are resolved from `VQL_HOME`. Configuration is strict: an unsupported version, backend, field, log level, or memory unit stops startup instead of being ignored.

| State or setting | Default and behavior |
|---|---|
| Configuration | `$VQL_HOME/config.toml`; optional, schema `version = 1` |
| Catalog | SQLite backend at `$VQL_HOME/catalog/vql.db`; Python and Rust hosts may explicitly override the path |
| Shell history | `$VQL_HOME/history` |
| Model cache | `$VQL_HOME/cache/models/`; local `file://` models stay at their source path |
| Session memory | 512 MiB shared by every query and retained result in one Session; Python may override it with `connect(session_memory_limit_bytes=...)` |
| `HF_TOKEN` | Authenticates private `hf://` downloads |
| `VQL_LOG_LEVEL` | Overrides `log.level`; accepts `error`, `warn`, `info`, `debug`, or `trace` |

An explicit host-side Catalog override changes only the SQLite Catalog path; configuration, shell history, and the model cache remain under the same `VQL_HOME`.

## Errors

Hard failures have a stable `VQL-CCDDD` identifier, a readable symbol, and a diagnostic message:

```text
[VQL-42001] INVALID_SQL: expected a statement
```

CLI output uses that form. Python raises `visionql.VisionQLError`, a `RuntimeError` subclass whose `code`, `symbol`, `message`, and `target_version` attributes avoid error-string matching. See the [Error Code Design](docs/design/error_codes.md) for the registry and compatibility rules.

## Architecture

[![VisionQL architecture](docs/assets/visionql-architecture.svg)](docs/architecture.html)

At component level, `vql-cli` either connects to `vqld` over Flight SQL or embeds `vql-kernel`
directly. `vql-python` is a second embedded host through PyO3. `vqld` delegates SQL planning and
execution to `vql-kernel`, while retaining ownership of service Sessions and persistent Query
lifecycle.

`vql-kernel` resolves definitions and immutable snapshots through `vql-catalog`. Both the kernel and
`vqld` use the Catalog-owned Query contract; `vql-catalog` persists definitions and Query status in
`$VQL_HOME/catalog/vql.db`, so there is no separate service database.

`vql-workbench` is an independent browser client. It connects only through `vqld`'s public Flight
SQL boundary and does not import `vql-kernel` or `vql-catalog`.

Open the [self-contained architecture diagram](docs/architecture.html) locally to inspect it at full
size or export PNG/PDF. See the [High-Level Design](docs/high_level_design.md) and focused component
designs below for normative contracts.

## Documentation

| Document | Use it for |
|---|---|
| [Examples](examples/README.md) | End-to-end SQL, Python, and notebook workflows |
| [Product requirements](docs/prd.md) | Product value, public semantics, and version scope |
| [High-level design](docs/high_level_design.md) | System boundaries, data paths, invariants, and dependency direction |
| [Kernel design](docs/design/kernel.md) | Planning, streaming, media, inference, resources, and security |
| [Catalog design](docs/design/catalog.md) | Namespaces, definition and persistent Query objects, snapshots, providers, backends, and UC API |
| [`vqld` service design](docs/design/vqld.md) | v0.2 Flight SQL host, Catalog-backed persistent Queries, and honest restart-from-live behavior |
| [Workbench design](docs/design/workbench.md) | v0.3 browser SQL client, visual results, Catalog management, and persistent Query management |
| [Error code design](docs/design/error_codes.md) | Stable identifiers, symbols, host representation, and extension rules |
| [CLI design](docs/design/cli.md) | Shell, script execution, rendering, and signal behavior |
| [Python binding design](docs/design/python_binding.md) | PyO3 API, PyArrow results, and Python UDFs |
| [Testing design](docs/design/testing.md) | Test ownership, system scenarios, fixtures, and Compose services |
| [Roadmap](ROADMAP.md) | Delivered and planned capabilities by version |
| [Proposals](docs/proposals/README.md) | Focused designs for later features |
| [Datasets](data/datasets/README.md) / [models](data/models/README.md) | Sample provenance and ONNX export contract |
| [Contributing](CONTRIBUTING.md) | Development workflow, tests, and pull request expectations |
| [Code of Conduct](CODE_OF_CONDUCT.md) | Community standards and private conduct reporting |
| [Security](SECURITY.md) | Supported versions and private vulnerability reporting |

## Development

The workspace declares Rust 1.88 as its minimum supported version and pins Rust 1.91.1 for repository development. Run the gates from the root:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

Run kernel-owned SQL contracts directly with:

```bash
cargo test -p vql-kernel --test slt --locked
```

Cases are grouped under `vql-kernel/tests/slt/{ddl,connectors,functions,models}`. The harness
generates its fixtures and uses `mock://` models, so the default workspace suite never depends on
downloaded data or models. Real data, models, RTSP, and Kafka remain in the feature-gated
`vql-testing` system suite.

Development and system-test services are defined in the root Compose file. Start an individual service by name, or use the system-test wrapper to select dependencies automatically:

```bash
docker compose up -d mediamtx
scripts/run-system-tests.sh
```

Install the Git hooks with [pre-commit](https://pre-commit.com/):

```bash
python -m pip install pre-commit
pre-commit install
pre-commit run --all-files
pre-commit run --hook-stage pre-push --all-files
```

The feature-gated system suite owns real datasets, models, RTSP, and Kafka. Run the real-image
task-shaped inference target after installing the system fixtures with:

```bash
VQL_SYSTEM_TEST=1 \
  cargo test -p vql-testing \
  --features system-tests \
  --test image \
  --locked
```

The complete strict suite runs the image, video, Catalog Model, RTSP, Kafka, and containerized
`vqld` targets through `scripts/run-system-tests.sh`. When one or more `--test` targets are
selected, the wrapper starts only their required Compose services.

## Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, tests, and review expectations. Report vulnerabilities privately according to [SECURITY.md](SECURITY.md).

## License

VisionQL is licensed under the [Apache License 2.0](LICENSE).
