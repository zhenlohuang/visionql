# VisionQL examples

Examples cover local image and recorded-video queries, Python and notebook workflows, and live RTSP input with Kafka output. File-based examples have three entry-point directories, with each file named after its scenario:

```text
examples/
├── sql/       # one standalone .sql file per SQL scenario
├── notebook/  # one standalone .ipynb file per notebook scenario
└── python/    # one standalone .py file per Python scenario
```

Run commands from the repository root and keep development state out of your user-level VisionQL
home:

```bash
export VQL_HOME=./data/.vql
```

Fetch the sample datasets into the independent dataset tree:

```bash
python scripts/fetch_datasets.py
```

The tree is keyed by modality first and dataset second:

```text
data/datasets/
├── images/
│   └── coco128/    images/ + labels/   # 32 labelled photos with YOLO ground truth
└── videos/
    ├── sample-videos/                  # 3 person/bicycle/car clips, 20 s each
    └── vtest/                          # 1 pedestrian clip in an AVI container
```

See [`data/datasets/README.md`](../data/datasets/README.md) for the sources and how to check
detection results against the bundled ground truth.

Export the official Ultralytics YOLO26 nano checkpoint to the ONNX model used by all three
examples:

```bash
python -m pip install ultralytics huggingface_hub onnx
python scripts/export_yolo26.py --size n
```

The upstream checkpoint comes from
[`Ultralytics/YOLO26`](https://huggingface.co/Ultralytics/YOLO26); generated ONNX files stay under
the ignored `data/models/` development directory. See
[`data/models/README.md`](../data/models/README.md) for the export contract.

## SQL

[`sql/video_people_count.sql`](sql/video_people_count.sql) demonstrates sampled historical video,
model inference and a batch `TUMBLE` aggregation returned directly to the caller:

```bash
cargo run -p vql-cli -- run examples/sql/video_people_count.sql
```

## Notebook

[`notebook/image_filtering.ipynb`](notebook/image_filtering.ipynb) walks through an image table, a
vectorized Arrow Python UDF, typed ONNX inference, and an Arrow result in a notebook.

## Python

[`python/image_filtering.py`](python/image_filtering.py) provides the same representative image
filtering workflow as a directly executable Python program. After installing the Python package
with Maturin, run:

```bash
python examples/python/image_filtering.py
```

All examples use the same metadata-bearing `yolo26n.onnx` export. Minimal `CREATE MODEL`
declares the Runtime-scoped input and output contract, and `RESOLVE MODEL` validates the artifact
before use. Each direct Model call supplies query-specific classes and confidence. Remove
`data/.vql/` when you intentionally want a fresh development catalog.

## Built-in AI and Model examples

The [SQL reference](../docs/user_guide/sql-reference.md) includes typed object detection, Model versions, and SQL expression Functions. The generated [built-in function reference](../docs/user_guide/sql-functions.md) includes spatial queries and `VQL_CLASSIFY` / `VQL_DETECT` examples. Install built-in artifacts using the [sample setup](../docs/user_guide/installation.md#sample-data-and-models). Built-in functions do not require Catalog Model declarations; typed Model calls use `CREATE MODEL` followed by `RESOLVE MODEL`.

## RTSP streaming and persistent Jobs

This tutorial queries a live camera and submits a persistent windowed frame-count write to Kafka. Use the current source checkout, [start `vqld`](../docs/user_guide/installation.md#run-vqld), and connect through a remote shell or [Workbench](../docs/user_guide/workbench.md#connect-to-a-daemon). Export `data/models/yolo26n.onnx` using the preparation commands above. Run the daemon from the repository root so the relative artifact path resolves there. Replace the RTSP URL with your camera endpoint; cataloged URLs reject embedded credentials and query parameters.

Start the repository's local plaintext Kafka broker from another terminal:

```bash
docker compose up -d kafka
```

### Register the Model and camera

Run these statements through the daemon's SQL Session. Definitions persist, so use fresh names or reuse existing definitions when repeating the tutorial:

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

CREATE TABLE cam_entrance
USING RTSP
OPTIONS (
  url = 'rtsp://10.0.0.15:554/main',
  fps = 5,
  event_time = 'capture_time',
  watermark = '2 seconds',
  transport = 'tcp'
);
```

### Inspect an attached detection stream

Run this SELECT separately. In Workbench, choose **Run as attached stream**, then click a thumbnail to inspect its bounding box, label, and confidence. The shell prints rows incrementally:

```sql
SELECT f.ts,
       f.frame_id,
       f.frame AS thumbnail,
       det.label,
       det.confidence,
       det.box
FROM cam_entrance AS f,
     UNNEST(yolo(f.frame,
       classes => ['person'],
       min_confidence => 0.5
     )) AS u(det);
```

Cancel the preview before running the next statement. Workbench's ordinary Run accepts bounded results; its attached-stream action explicitly opts into continuous results. See [streaming semantics](../docs/user_guide/sql-reference.md#tumble-and-streaming-queries) for supported aggregates, watermark closure, and embedded or remote cancellation.

### Submit a persistent frame-count Job

The following Job counts source frames in ten-second windows. It uses the camera Table above and a writable Kafka output Table:

```sql
CREATE TABLE camera_counts (window_start TIMESTAMP, frames BIGINT)
USING KAFKA
OPTIONS (
  bootstrap_servers = '127.0.0.1:9092',
  topic = 'visionql-camera-counts'
);

SUBMIT JOB entrance_frames AS
INSERT INTO camera_counts
SELECT TUMBLE(ts, INTERVAL '10' SECOND) AS window_start,
       COUNT(*) AS frames
FROM cam_entrance
GROUP BY 1;

SHOW JOBS;
```

Copy the returned `job_id` into these statements:

```sql
DESCRIBE JOB '<job_id>';
STOP JOB '<job_id>';
```

Workbench's **Workspace → Jobs** provides the same inspection and Stop actions. The Job continues after the submitting client disconnects. After daemon restart, it resumes from the live position with fresh window state and a reported restart gap. Definitions are immutable: loading displayed SQL creates a separate draft, and submitting it creates a new Job. Replace redacted literals before resubmitting.

The local Compose broker needs no credential reference. Secured Kafka output uses `credential_ref` and a host-installed SecretProvider; see [provider options](../docs/user_guide/sql-reference.md#provider-options) and the [Kafka contract](../docs/design/kernel.md#kafka-table). Streaming delivery is best-effort; there is no exactly-once or replay guarantee.
