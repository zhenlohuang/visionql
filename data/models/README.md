# Development models

This directory holds locally exported development models and is independent from
`data/datasets/`. Generated model files are ignored by Git.

VisionQL uses ONNX exports of the official
[`Ultralytics/YOLO26`](https://huggingface.co/Ultralytics/YOLO26) `yolo26n.pt` detection and
`yolo26n-cls.pt` ImageNet classification checkpoints:

```bash
python -m pip install ultralytics huggingface_hub onnx
python scripts/export_yolo26.py --task detect --size n
python scripts/export_yolo26.py --task classify --size n
```

The default outputs are `data/models/yolo26n.onnx` and `data/models/yolo26n-cls.onnx`. The script
pins the official checkpoint revision used by v0.1 unless `--revision` is explicit, downloads the
selected checkpoint from the official Hugging Face repository, and exports it with a dynamic batch
dimension. Detection uses a `640x640` image shape and YOLO26's end-to-end `(N, 300, 6)` output;
classification uses a `224x224` ImageNet input and `(N, 1000)` scores. The dynamic batch dimension
is required because VisionQL combines concurrent inference rows into one ONNX Runtime call.

Install the fixed v0.1 artifacts under the active VisionQL home with:

```bash
python scripts/export_yolo26.py --task classify --install
python scripts/export_yolo26.py --task detect --install
```

These write `$VQL_HOME/models/yolo26n-cls.onnx` and `$VQL_HOME/models/yolo26n.onnx`, or the same
paths below `$HOME/.vql/models` when `VQL_HOME` is unset. `--install` is fixed to `n`; use
`--output` for development exports that should not replace a built-in artifact.

The upstream repository currently declares AGPL-3.0 and also documents a separate Ultralytics
Enterprise License; review the upstream terms before distributing weights or using them
commercially.
