<div align="center">
  <h1>VisionQL — A Data Engine for Physical AI</h1>
  <p>
    <a href="https://github.com/zhenlohuang/visionql/actions/workflows/ci.yml">
      <img alt="CI" src="https://github.com/zhenlohuang/visionql/actions/workflows/ci.yml/badge.svg?branch=main">
    </a>
    <a href="CHANGELOG.md">
      <img alt="Released v0.2.0" src="https://img.shields.io/badge/release-v0.2.0-6f42c1">
    </a>
    <img alt="Rust 1.88 or newer" src="https://img.shields.io/badge/Rust-1.88%2B-black?logo=rust">
    <img alt="Python 3.10 or newer" src="https://img.shields.io/badge/Python-3.10%2B-3776AB">
    <a href="LICENSE">
      <img alt="Apache-2.0 license" src="https://img.shields.io/badge/license-Apache--2.0-blue">
    </a>
  </p>
  <p>
    <a href="#quick-start">Quick start</a> ·
    <a href="#interfaces">Interfaces</a> ·
    <a href="#examples">Examples</a> ·
    <a href="#architecture">Architecture</a> ·
    <a href="#documentation">Documentation</a> ·
    <a href="ROADMAP.md">Roadmap</a>
  </p>
</div>

VisionQL lets you query images, recorded video, and live camera streams with SQL. It combines media decoding, model inference, and relational processing in one engine, returning Arrow results or writing continuously to Kafka.

<p align="center">
  <a href="docs/assets/workbench-rtsp-detection.mp4">
    <img alt="VisionQL Workbench detecting people in a live RTSP stream and inspecting a frame's bounding box" src="docs/assets/workbench-rtsp-detection.gif" width="900">
  </a>
  <br>
  <sub><a href="docs/assets/workbench-rtsp-detection.mp4">▶ Watch the full demo video (79 s)</a></sub>
</p>

## Why VisionQL

Use SQL to answer questions about visual data without assembling a separate decoding, inference, and aggregation pipeline for each task.

- **Replace one-off pipelines with queries.** Images become rows and sampled video frames become time-aware relations. Compose visual inference with familiar filters, joins, `UNNEST`, and aggregations instead of rebuilding orchestration for every question.
- **Optimize inference, not just SQL.** Model calls stay visible in the plan rather than hiding inside black-box UDFs. VisionQL can push down frame sampling and time predicates, batch inference, and avoid decoding columns the query never reads.
- **Develop on history, move to live data.** One query model covers bounded image/video data and unbounded RTSP camera streams, with attached execution or explicit persistent Table writes.
- **Keep execution close to the data.** Run the engine in-process or host it through the single-node `vqld` service without requiring media uploads, a scheduler, or a control plane.

## Quick start

These steps use the current source checkout. The release badge refers to v0.2.0; Workbench and the current `SUBMIT JOB` / `SHOW JOBS` command family are unreleased changes. See the [Roadmap](ROADMAP.md) and [Changelog](CHANGELOG.md) for release scope.

### 1. Build and open the shell

Install the [prerequisites](docs/user_guide/installation.md#prerequisites), including Rust, Python, FFmpeg 8 development libraries, and the native build toolchain. Then build the engine and fetch the sample images:

```bash
git clone https://github.com/zhenlohuang/visionql.git
cd visionql

export VQL_HOME="$PWD/data/.vql"
python3 -m venv .venv
source .venv/bin/activate
cargo build --workspace --locked
python scripts/fetch_datasets.py --dataset coco128
cargo run -q -p vql-cli -- shell
```

### 2. Query your images

In the shell, register the downloaded images as a Table and run a bounded query:

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

The Catalog persists the Table definition. Later CLI and Python Sessions using the same `VQL_HOME` can query `sample_images` without registering it again.

### 3. Detect people with SQL

From another terminal at the repository root, activate the same environment and export the YOLO26n detector:

```bash
source .venv/bin/activate
python -m pip install ultralytics huggingface_hub onnx
python scripts/export_yolo26.py --task detect --size n
```

Back in the SQL shell, register and resolve the Model, then call it in a query over `sample_images`:

```sql
CREATE MODEL yolo TYPE OBJECT_DETECTION
FROM './data/models/yolo26n.onnx'
OPTIONS (
  image_size = 640,
  format = 'yolo_e2e',
  labels = 'coco80',
  box_format = 'xyxy'
);

RESOLVE MODEL yolo;

SELECT f.uri,
       f.image AS thumbnail,
       det.label,
       det.confidence,
       det.box
FROM sample_images AS f,
     UNNEST(yolo(f.image,
       classes => ['person'],
       min_confidence => 0.5
     )) AS u(det)
LIMIT 5;
```

Each result contains the image, a detected person's label and confidence, and a normalized bounding box. `CREATE MODEL` declares the typed interface; `RESOLVE MODEL` validates and pins its execution contract.

See [Installation and configuration](docs/user_guide/installation.md) for Docker and runtime settings, or continue with another interface below.

## Interfaces

Choose the interface that fits your workflow. All use the same SQL engine; `vqld` owns the Catalog and persistent Jobs for remote clients.

| Interface | Use it for | Get started |
| --- | --- | --- |
| **CLI** | Explore SQL locally, run scripts, or connect to a daemon | [CLI guide](docs/user_guide/installation.md#cli) |
| **Python** | Query from applications and notebooks, collect PyArrow results, and register in-process Python UDFs | [Python setup](docs/user_guide/installation.md#python-api) |
| **`vqld`** | Host the engine over Arrow Flight SQL and keep persistent Jobs running after clients disconnect | [Daemon setup](docs/user_guide/installation.md#run-vqld) |
| **Workbench** | Author SQL in the browser, inspect images and boxes, and manage Catalog objects and Jobs | [Workbench setup](docs/user_guide/installation.md#workbench) |

### Python API

After [building the Python extension](docs/user_guide/installation.md#python-api), reuse the same Catalog and collect a PyArrow table:

```python
import visionql

session = visionql.connect()
table = session.sql("SELECT uri, width, height FROM sample_images LIMIT 5").collect()
print(table)
```

### Workbench

Start `vqld` and Workbench with the [startup instructions](docs/user_guide/installation.md#workbench), then open [http://127.0.0.1:6040](http://127.0.0.1:6040). Run the detection query above to inspect thumbnails, bounding boxes, labels, and confidence together.

The [Workbench user guide](docs/user_guide/workbench.md) covers connections, SQL drafts, visual inspection, Catalog, and Jobs. Closing the browser or cancelling an attached preview leaves persistent Jobs running in `vqld`.

## Examples

Continue with complete workflows in the [examples guide](examples/README.md):

| Workflow | Example |
| --- | --- |
| Count people in recorded video with sampled frames and windowed aggregation | [SQL script](examples/sql/video_people_count.sql) |
| Filter images with typed inference and batched Python UDFs | [Python script](examples/python/image_filtering.py) · [Notebook](examples/notebook/image_filtering.ipynb) |
| Inspect a live camera and submit a persistent windowed write to Kafka | [RTSP and Jobs tutorial](examples/README.md#rtsp-streaming-and-persistent-jobs) |

## Architecture

[![VisionQL architecture](docs/assets/visionql-architecture.svg)](docs/architecture.html)

The CLI and Python API can embed `vql-kernel`; `vqld` hosts the same engine over Flight SQL and owns service Sessions and persistent Job lifecycle. `vql-catalog` persists definitions and Jobs in one Catalog database. Workbench is an independent browser client of `vqld`.

Open the [interactive architecture diagram](docs/architecture.html) or the [High-Level Design](docs/high_level_design.md) for system boundaries and the component-design index.

## Documentation

| Document | Use it for |
| --- | --- |
| [Installation and configuration](docs/user_guide/installation.md) | Source builds, Docker, CLI, Python, `vqld`, Workbench, and runtime state |
| [SQL reference](docs/user_guide/sql-reference.md) | VQL types, provider Tables, Models, Functions, windows, and Jobs |
| [Built-in function reference](docs/user_guide/sql-functions.md) | Generated syntax, arguments, and examples for AI, spatial, and window functions |
| [Workbench user guide](docs/user_guide/workbench.md) | Connections, SQL drafts, visual inspection, Catalog, Jobs, and history |
| [Product requirements](docs/prd.md) | Target users, workflows, product value, and public semantics |
| [High-Level Design](docs/high_level_design.md) | System boundaries and component designs |

See the [Roadmap](ROADMAP.md) for release scope and future directions, and the [Changelog](CHANGELOG.md) for delivered and unreleased changes.

## Contributing

Issues and pull requests are welcome. The [contributor guide](CONTRIBUTING.md) covers development setup, tests, coverage, Git hooks, and review expectations. Follow the [Code of Conduct](CODE_OF_CONDUCT.md), and report vulnerabilities privately according to [SECURITY.md](SECURITY.md).

## License

VisionQL is licensed under the [Apache License 2.0](LICENSE).
