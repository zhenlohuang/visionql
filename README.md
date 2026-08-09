# VisionQL

VisionQL is a local, service-free SQL engine for visual data. v0.1 includes persistent catalog
objects, image and sampled-video tables, ONNX/HTTP model inference, vectorized Python UDFs,
Console Sink, batch `TUMBLE`, a CLI, and a Python package backed by Arrow/DataFusion.

## Try v0.1

VisionQL keeps user runtime state under `VQL_HOME`. It defaults to `$HOME/.vql`; repository
development uses an ignored local home:

```bash
export VQL_HOME=./data/.vql
```

Fetch the public sample datasets — 32 labelled COCO128 images and 4 short detection clips, about
10 MB in total — then follow the representative workflows in
[`examples/README.md`](examples/README.md):

```bash
python scripts/fetch_datasets.py
python -m pip install ultralytics huggingface_hub onnx
python scripts/export_yolo26.py --size n
```

For example:

```bash
python examples/python/image_filtering.py
cargo run -p vql-cli -- run examples/sql/video_people_count.sql
```

The runtime layout is `$VQL_HOME/catalog/vql.db`, `$VQL_HOME/history`, and
`$VQL_HOME/cache/models/`. Only downloaded models are cached; local `file://` models remain at
their source path. Use `--catalog /path/to/vql.db` or `VQL_CATALOG` to override only the
catalog while retaining the same `VQL_HOME` for history and cache.

Open the interactive shell with:

```bash
cargo run -p vql-cli -- shell
```

Build and install the Python package with Maturin:

```bash
cd vql-python
maturin develop
python -c "import visionql; print(visionql.connect().sql('SELECT 1').collect())"
```

VisionQL uses end-to-end ONNX exports from the official
[`Ultralytics/YOLO26`](https://huggingface.co/Ultralytics/YOLO26) model family. The repository
contains PyTorch checkpoints, so `scripts/export_yolo26.py` performs the explicit ONNX export;
VisionQL itself does not embed PyTorch or execute repository code.

The built-in YOLO26 processor supplies the `images`/`output0` tensor contract, `640x640` input,
end-to-end XYXY output, and the 80 COCO labels. A model can override those defaults, and each
function can further narrow result semantics:

```sql
CREATE MODEL yolo26n TYPE OBJECT_DETECTION FROM './data/models/yolo26n.onnx'
WITH (processor = 'yolo26-detect-v1');
CREATE FUNCTION people USING MODEL yolo26n
WITH (classes = ['person'], min_confidence = 0.6);
```

No model sidecar file is required. `hf://owner/repository@revision/model.onnx` downloads only the
ONNX artifact into the content-addressed model cache (use `HF_TOKEN` for private repositories).
Run the ignored real-model gate with
`VQL_YOLO26_ONNX=./data/models/yolo26n.onnx cargo test real_yolo26_onnx_e2e -- --ignored`.

## Development

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Install the repository hooks with [pre-commit](https://pre-commit.com/):

```bash
python -m pip install pre-commit
pre-commit install
pre-commit run --all-files
```

The pre-commit hook runs repository hygiene checks, `cargo fmt`, and `cargo clippy`. The pre-push
hook runs `cargo test --workspace --locked`. Run the push gate explicitly with:

```bash
pre-commit run --hook-stage pre-push --all-files
```

See [the PRD](docs/prd.md), [design](docs/design.md), and [roadmap](ROADMAP.md) for the complete
v0.1 product contract and milestone boundaries.
