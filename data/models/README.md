# Development models

This directory holds locally exported development models and is independent from
`data/datasets/`. Generated model files are ignored by Git.

VisionQL examples use an end-to-end ONNX export of the official
[`Ultralytics/YOLO26`](https://huggingface.co/Ultralytics/YOLO26) `yolo26n.pt` checkpoint:

```bash
python -m pip install ultralytics huggingface_hub onnx
python scripts/export_yolo26.py --size n
```

The default output is `data/models/yolo26n.onnx`. The script downloads the selected checkpoint
from the official Hugging Face repository and exports it with a dynamic batch dimension, a
`640x640` image shape, and YOLO26's end-to-end `(N, 300, 6)` detection output. The dynamic batch
dimension is required because VisionQL combines concurrent inference rows into one ONNX Runtime
call.

The upstream repository currently declares AGPL-3.0 and also documents a separate Ultralytics
Enterprise License; review the upstream terms before distributing weights or using them
commercially.
