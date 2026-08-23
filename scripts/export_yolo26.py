"""Download an official Ultralytics YOLO26 checkpoint and export it to ONNX."""

from __future__ import annotations

import argparse
import ast
import json
import os
import shutil
from pathlib import Path

DEFAULT_REVISION = "aed8ed1027f2b55ee78d8fcac93be889251d1e05"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export an Ultralytics/YOLO26 checkpoint for VisionQL"
    )
    parser.add_argument("--size", choices=("n", "s", "m", "l", "x"), default="n")
    parser.add_argument(
        "--task",
        choices=("detect", "classify"),
        default="detect",
        help="export the detection or ImageNet classification checkpoint",
    )
    parser.add_argument(
        "--revision",
        default=DEFAULT_REVISION,
        help="pinned Hugging Face revision or commit",
    )
    parser.add_argument("--output", type=Path, help="destination .onnx path")
    parser.add_argument(
        "--install",
        action="store_true",
        help="install the selected yolo26n artifact under $VQL_HOME/models",
    )
    args = parser.parse_args()
    if args.install and args.output:
        parser.error("--install and --output cannot be used together")
    if args.install and args.size != "n":
        parser.error("the v0.1 built-in functions require --size n")
    return args


def default_vql_home() -> Path:
    configured = os.environ.get("VQL_HOME")
    return Path(configured) if configured else Path.home() / ".vql"


def main() -> None:
    args = parse_args()
    try:
        from huggingface_hub import hf_hub_download
        import onnx
        from ultralytics import YOLO
    except ImportError as error:
        raise SystemExit(
            "Install export dependencies first: pip install ultralytics huggingface_hub onnx"
        ) from error

    suffix = "-cls" if args.task == "classify" else ""
    name = f"yolo26{args.size}{suffix}"
    checkpoint = hf_hub_download(
        repo_id="Ultralytics/YOLO26",
        filename=f"{name}.pt",
        revision=args.revision,
    )
    image_size = 224 if args.task == "classify" else 640
    export_options = {
        "format": "onnx",
        "imgsz": image_size,
        "dynamic": True,
        "simplify": False,
        "device": "cpu",
    }
    if args.task == "detect":
        export_options["end2end"] = True
    exported = Path(YOLO(checkpoint).export(**export_options))
    output = (
        default_vql_home() / "models" / f"yolo26n{suffix}.onnx"
        if args.install
        else args.output or Path("data/models") / f"{name}.onnx"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    if exported.resolve() != output.resolve():
        shutil.copy2(exported, output)
    graph = onnx.load(output)
    metadata = {entry.key: entry.value for entry in graph.metadata_props}
    labels = metadata.get("names")
    if labels:
        try:
            labels = json.dumps(ast.literal_eval(labels), ensure_ascii=False)
        except (SyntaxError, ValueError):
            pass
    metadata.update(
        {
            "visionql.output_format": (
                "classification" if args.task == "classify" else "yolo_e2e"
            ),
            "visionql.image_size": json.dumps([image_size, image_size]),
            "visionql.source_revision": args.revision,
            **({"names": labels} if labels else {}),
        }
    )
    del graph.metadata_props[:]
    for key, value in metadata.items():
        entry = graph.metadata_props.add()
        entry.key = key
        entry.value = value
    onnx.save(graph, output)
    print(output.resolve())


if __name__ == "__main__":
    main()
