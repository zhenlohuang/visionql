<div align="center">
  <h1>VisionQL — A Data Engine for Physical AI</h1>
  <p>
    <a href="https://github.com/zhenlohuang/visionql/actions/workflows/ci.yml">
      <img alt="CI" src="https://github.com/zhenlohuang/visionql/actions/workflows/ci.yml/badge.svg?branch=main">
    </a>
    <img alt="Version 0.1.0" src="https://img.shields.io/badge/version-0.1.0-6f42c1">
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
    <a href="ROADMAP.md">Roadmap</a>
  </p>
</div>

## What is VisionQL

VisionQL is a unified batch and streaming engine for querying and processing multimodal data. With SQL or the DataFrame API, users can work with images, video files, and live video streams through the same query model.

> [!IMPORTANT]
> VisionQL v0.1 is pre-release. Image sets, historical video, typed inference, RTSP ingestion, streaming `TUMBLE`, and Kafka output are implemented. Attached continuous-query foreground execution remains in progress; `vqld`, Workbench, and vector search follow in later releases. See the [Roadmap](ROADMAP.md) for exact delivery status and version boundaries.

## Why VisionQL

Physical AI systems continuously produce camera, vehicle, and robot data. VisionQL turns decoding, sampling, inference, and aggregation into a declarative query plan:

- **Replace one-off pipelines with queries.** Images become rows and sampled video frames become time-aware relations. Compose visual inference with familiar filters, joins, `UNNEST`, and aggregations instead of rebuilding orchestration for every question.
- **Optimize inference, not just SQL.** Model calls stay visible in the plan rather than hiding inside black-box UDFs. VisionQL can push down frame sampling and time predicates, batch inference, and avoid decoding columns the query never reads.
- **Develop on history, move to live data.** v0.1 uses one query model for bounded image/video data and unbounded RTSP camera streams, with attached execution and Kafka output.
- **Keep execution close to the data.** The v0.1 engine runs in-process and does not require media uploads, a scheduler, or a control plane.

The [PRD](docs/prd.md) covers target users, representative Physical AI workflows, product boundaries, and the longer-term batch/stream value proposition.

## Quick start

### Prerequisites

- [Rustup](https://rustup.rs/); the repository selects its pinned toolchain automatically.
- Python 3.10 or newer.
- [FFmpeg 8](https://ffmpeg.org/download.html), including development libraries for the default native video build and `ffmpeg` / `ffprobe` on `PATH` for sample preparation.

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
WITH (recursive = true);

SELECT uri, width, height
FROM sample_images
ORDER BY uri
LIMIT 5;
```

VisionQL persists the table definition, so a new shell session can query `sample_images` without registering it again.

## Examples

### Typed visual inference

The v0.1 contract registers one typed Model and calls the fixed built-in function selected by its `TYPE`. Inference remains visible to the optimizer; SQL/Python user functions use DataFusion's separate function extension path.

```sql
CREATE MODEL yolo
TYPE OBJECT_DETECTION
FROM 'file:///models/yolo.onnx'
USING ONNX_RUNTIME
WITH (
  input = {
    name = 'images', width = 640, height = 640, resize = 'letterbox'
  },
  output = {name = 'output0', format = 'yolo_e2e', labels = 'coco80'}
);

RESOLVE MODEL yolo;

SELECT uri,
       IMAGE_DETECTION(
         'yolo',
         image,
         classes => ['person'],
         min_confidence => 0.5
       ) AS detections
FROM sample_images;
```

`USING` selects the Runtime, and `WITH` is interpreted only by that Runtime. `CREATE MODEL` is a fast local declaration: it does not download an artifact or contact a service. `RESOLVE MODEL` performs the potentially slow download, cache installation, checksum verification, or service metadata validation. Queries reject a Model that has not been resolved.

Use `SHOW MODELS` to inspect each declaration's `UNRESOLVED` or `RESOLVED` status.

### Model sources

| Source | Example | Behavior |
|---|---|---|
| Local ONNX artifact | `'./model.onnx'` or `'file:///models/model.onnx'` | `RESOLVE MODEL` hashes it in place; no cache copy |
| Hugging Face artifact | `'hf://owner/repository@<commit>/model.onnx'` | `RESOLVE MODEL` downloads and caches it by digest |
| Inference service | `'http://triton-prod:8000'` | `RESOLVE MODEL` validates the Runtime-specific service contract |

```sql
CREATE MODEL production_detector
TYPE OBJECT_DETECTION
FROM 'http://triton-prod:8000'
USING TRITON_INFERENCE_SERVER
WITH (model = 'detector', version = '42');

RESOLVE MODEL production_detector;
```

The Runtime registry is intentionally explicit. `ONNX_RUNTIME` and `TRITON_INFERENCE_SERVER` are the v0.1 paths; `TRANSFORMERS`, `VLLM`, `SGLANG`, and `LLAMA_CPP` remain roadmap-gated. Embedded ONNX execution derives its VisionQL-owned pre/post-processing pipeline from `WITH.input` and `WITH.output`. A Triton service instead owns the complete preprocessing, inference, and postprocessing path: VisionQL sends encoded images and accepts only the canonical typed detection result, never service-specific raw tensors.

Set `HF_TOKEN` when resolving a private Hugging Face bundle. Credentials are provided through secret configuration and never persisted in visible Model DDL.

### RTSP streaming

The v0.1 streaming path registers one live camera and runs an attached query until the client cancels it. FFmpeg decodes on a controlled worker, sampling uses event time, watermarks advance outside data rows, and disconnects retry with exponential backoff.

```sql
CREATE STREAM cam_entrance
FROM 'rtsp://10.0.0.15:554/main'
WITH (
  fps = 5,
  event_time = 'capture_time',
  watermark = INTERVAL '2' SECOND,
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

The shell and `vql run` print unbounded results incrementally; Ctrl-C cancels the attached query. `Projection`, `Filter`, `UNNEST`, scalar functions, typed inference, and one `TUMBLE` aggregate are accepted. Streaming windows support `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`; they close only after the watermark reaches the window end. `DISTINCT`, media-valued state, and unsupported unbounded plan shapes are rejected during planning. Live frames use epoch-scoped frame buffers internally and are encoded before crossing the result boundary. Cataloged endpoints currently reject embedded credentials and query parameters so secrets cannot be persisted accidentally.

Kafka Sink authentication is configured with an opaque `credential_ref`; embedding hosts install a `SecretProvider` on `EngineConfig`, and resolved credentials never enter the Catalog or SQL text. The repository's local Compose profile uses plaintext Kafka and does not require a reference.

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
vql [--catalog PATH] shell
vql [--catalog PATH] run <script.sql>
vql [--catalog PATH] explain <query-or-file>
```

During source development, replace `vql` with `cargo run -p vql-cli --`. Set `VQL_LOG` to a `tracing` filter such as `info` or `vql_kernel=debug` when diagnosing execution.

## Runtime state

VisionQL keeps local state under `VQL_HOME`, which defaults to `$HOME/.vql`. The Catalog makes table, model, and function definitions reusable across sessions; every planned query receives an immutable Query Manifest.

| State or setting | Default and behavior |
|---|---|
| Catalog | `$VQL_HOME/catalog/vql.db`; override with `VQL_CATALOG` or `--catalog PATH` |
| Shell history | `$VQL_HOME/history` |
| Model cache | `$VQL_HOME/cache/models/`; local `file://` models stay at their source path |
| `HF_TOKEN` | Authenticates private `hf://` downloads |
| `VQL_LOG` | Configures Rust tracing; default is off |

A catalog override changes only the SQLite path; shell history and the model cache remain under the same `VQL_HOME`.

## Architecture

```mermaid
flowchart LR
    HOSTS["CLI / Python"] --> API["Engine / Session"]
    API --> FRONTEND["VQL SQL + Catalog"]
    FRONTEND --> PLAN["Planner + DataFusion"]
    PLAN --> RUNTIME["Media + Model Runtimes"]
    RUNTIME --> ARROW["Arrow RecordBatches"]
    ARROW --> HOSTS
```

The CLI and Python hosts share `vql-kernel`, which owns SQL planning, the Catalog and Query Manifests, DataFusion execution, epoch-driven RTSP ingestion, media decoding, and model inference. For design rationale—including lazy media decoding, optimizer-visible inference, and the batch/stream boundary—read the [system design](docs/design.md).

## Documentation

| Document | Use it for |
|---|---|
| [Examples](examples/README.md) | End-to-end SQL, Python, and notebook workflows |
| [Product requirements](docs/prd.md) | Product value, public semantics, and version scope |
| [System design](docs/design.md) | Engine architecture, contracts, and extension boundaries |
| [Roadmap](ROADMAP.md) | Delivered and planned capabilities by version |
| [Proposals](docs/proposals/README.md) | Focused designs for later features |
| [Integration tests](vql-testing/README.md) | Sqllogictest cases, fixtures, and Compose services |
| [Datasets](data/datasets/README.md) / [models](data/models/README.md) | Sample provenance and ONNX export contract |
| [Contributing](CONTRIBUTING.md) | Development workflow, tests, and pull request expectations |
| [Security](SECURITY.md) | Supported versions and private vulnerability reporting |

## Development

The workspace declares Rust 1.88 as its minimum supported version and pins Rust 1.91.1 for repository development. Run the gates from the root:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

Run the integration suite directly with:

```bash
cargo test -p vql-testing --test sql --locked
```

Cases are grouped under `vql-testing/tests/cases/{ddl,functions,scenarios}` as one-purpose
sqllogictest files. Each file gets an isolated Engine and catalog. Real-data cases run against
`data/datasets` and `data/models/yolo26n.onnx`, and report an ignored test when a required fixture
is absent.

Optional development and system-test services use profiles in the root Compose file. For example:

```bash
docker compose --profile rtsp up -d mediamtx
scripts/run-integration-tests.sh
```

Install the Git hooks with [pre-commit](https://pre-commit.com/):

```bash
python -m pip install pre-commit
pre-commit install
pre-commit run --all-files
pre-commit run --hook-stage pre-push --all-files
```

The real-model mixed-size batch scenario reads `data/models/yolo26n.onnx` directly. It runs when
the integration fixtures are available and otherwise reports a skip:

```bash
VQL_TEST_CASE=scenarios/mixed_size_images \
  cargo test -p vql-testing --test sql --locked
```

## Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, tests, and review expectations. Report vulnerabilities privately according to [SECURITY.md](SECURITY.md).

## License

VisionQL is licensed under the [Apache License 2.0](LICENSE).
