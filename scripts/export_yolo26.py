"""Download an official Ultralytics YOLO26 checkpoint and export it to ONNX."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export an Ultralytics/YOLO26 detection checkpoint for VisionQL"
    )
    parser.add_argument("--size", choices=("n", "s", "m", "l", "x"), default="n")
    parser.add_argument("--revision", default="main", help="Hugging Face revision or commit")
    parser.add_argument("--output", type=Path, help="destination .onnx path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        from huggingface_hub import hf_hub_download
        from ultralytics import YOLO
    except ImportError as error:
        raise SystemExit(
            "Install export dependencies first: pip install ultralytics huggingface_hub onnx"
        ) from error

    name = f"yolo26{args.size}"
    checkpoint = hf_hub_download(
        repo_id="Ultralytics/YOLO26",
        filename=f"{name}.pt",
        revision=args.revision,
    )
    exported = Path(
        YOLO(checkpoint).export(
            format="onnx",
            imgsz=640,
            dynamic=True,
            simplify=False,
            end2end=True,
            device="cpu",
        )
    )
    output = args.output or Path("data/models") / f"{name}.onnx"
    output.parent.mkdir(parents=True, exist_ok=True)
    if exported.resolve() != output.resolve():
        shutil.copy2(exported, output)
    print(output.resolve())


if __name__ == "__main__":
    main()
