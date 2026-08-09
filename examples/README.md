# VisionQL examples

The v0.1 examples have three entry-point directories. Every file is named after the scenario it
demonstrates:

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
model inference, a batch `TUMBLE` aggregation, and the Console Sink:

```bash
cargo run -p vql-cli -- run examples/sql/video_people_count.sql
```

## Notebook

[`notebook/image_filtering.ipynb`](notebook/image_filtering.ipynb) walks through an image table, a
vectorized Arrow Python UDF, an ONNX model function, and an Arrow result in a notebook.

## Python

[`python/image_filtering.py`](python/image_filtering.py) provides the same representative image
filtering workflow as a directly executable Python program. After installing the Python package
with Maturin, run:

```bash
python examples/python/image_filtering.py
```

All examples use the same `yolo26n.onnx` export. `CREATE MODEL ... WITH (...)` selects the built-in
`yolo26-detect-v1` processor defaults, while `CREATE FUNCTION ... WITH (...)` narrows classes and
confidence for each query interface. Remove `data/.vql/` when you intentionally want a fresh
development catalog.
