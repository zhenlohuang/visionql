"""Fetch the public datasets used to validate VisionQL locally.

The dataset root is keyed by modality first and dataset second, so a new dataset slots in as one
more subdirectory and `<root>/images` or `<root>/videos` can also be scanned recursively as a
single table spanning every dataset of that modality. A dataset holding only media puts its files
directly in its own directory; one that also ships ground truth splits it out inside:

    data/datasets/
    ├── images/
    │   └── coco128/
    │       ├── images/      COCO train2017 subset
    │       └── labels/      YOLO-format ground truth for the images above
    ├── videos/
    │   ├── sample-videos/   person / bicycle / car detection clips
    │   └── vtest/           pedestrian clip in an AVI container
    └── manifest.json        index of every dataset built so far

Only the Python standard library and `ffprobe`/`ffmpeg` are required; VisionQL already depends on
FFmpeg, so this adds no new tooling.

    python scripts/fetch_datasets.py                      # every dataset
    python scripts/fetch_datasets.py --list
    python scripts/fetch_datasets.py --dataset coco128 --images 128
    python scripts/fetch_datasets.py --clip-seconds 0 --force

Adding a dataset means adding one `@dataset(...)` builder below; nothing else needs to change.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

USER_AGENT = "visionql-fetch-datasets/1"


@dataclass(frozen=True)
class Options:
    """Per-run knobs a builder may consult."""

    images: int
    clip_seconds: int
    force: bool


@dataclass(frozen=True)
class Context:
    root: Path
    cache: Path
    options: Options

    def directory(self, modality: str, dataset: str, *parts: str) -> Path:
        """Return an emptied `<root>/<modality>/<dataset>[/<parts>]` directory."""
        path = self.root.joinpath(modality, dataset, *parts)
        shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True, exist_ok=True)
        return path


@dataclass(frozen=True)
class Dataset:
    name: str
    modality: str
    source: str
    license: str
    description: str
    build: Callable[[Context], dict]

    @property
    def root(self) -> str:
        return f"{self.modality}/{self.name}"


DATASETS: dict[str, Dataset] = {}


def dataset(
    name: str,
    *,
    modality: str,
    source: str,
    license: str,
    description: str,
) -> Callable[[Callable[[Context], dict]], Callable[[Context], dict]]:
    def register(builder: Callable[[Context], dict]) -> Callable[[Context], dict]:
        DATASETS[name] = Dataset(
            name=name,
            modality=modality,
            source=source,
            license=license,
            description=description,
            build=builder,
        )
        return builder

    return register


# --------------------------------------------------------------------------------------------
# Shared helpers
# --------------------------------------------------------------------------------------------


def log(message: str) -> None:
    print(f"[datasets] {message}", flush=True)


def require_tool(name: str) -> None:
    if shutil.which(name) is None:
        sys.exit(f"{name} not found on PATH; install FFmpeg first (brew install ffmpeg)")


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(urls: tuple[str, ...], destination: Path) -> None:
    """Download the first mirror that answers, writing atomically."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".part")
    errors: list[str] = []
    for url in urls:
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(request, timeout=120) as response:
                with temporary.open("wb") as handle:
                    shutil.copyfileobj(response, handle)
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            temporary.unlink(missing_ok=True)
            errors.append(f"{url}: {error}")
            continue
        temporary.replace(destination)
        return
    detail = "\n  ".join(errors)
    sys.exit(f"failed to download {destination.name} from every mirror:\n  {detail}")


def cached_download(context: Context, urls: tuple[str, ...], name: str) -> Path:
    path = context.cache / name
    if context.options.force or not path.exists():
        log(f"downloading {name}")
        download(urls, path)
    return path


def probe_stream(path: Path) -> dict[str, str]:
    """Return the first video stream's properties plus the container duration."""
    command = [
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries", "stream=codec_name,width,height,avg_frame_rate,nb_frames",
        "-show_entries", "format=duration",
        "-of", "json", str(path),
    ]
    payload = json.loads(subprocess.run(command, capture_output=True, check=True).stdout)
    stream = (payload.get("streams") or [{}])[0]
    stream["duration"] = (payload.get("format") or {}).get("duration")
    return stream


def parse_ratio(value: str | None) -> float | None:
    if not value or "/" not in value:
        return None
    numerator, denominator = value.split("/", 1)
    try:
        return round(float(numerator) / float(denominator), 6) if float(denominator) else None
    except ValueError:
        return None


# --------------------------------------------------------------------------------------------
# COCO128 — images with ground truth
# --------------------------------------------------------------------------------------------

COCO128_MIRRORS = (
    "https://github.com/ultralytics/assets/releases/download/v0.0.0/coco128.zip",
    "https://github.com/ultralytics/yolov5/releases/download/v1.0/coco128.zip",
    "https://ultralytics.com/assets/coco128.zip",
)
COCO128_SHA256 = "61e5e3028863d8ffc3b81d6a514603954889f0edd5e4b44c4ce60b2da99aeb8e"

# COCO 2017 detection classes, in the index order used by the YOLO-format label files.
COCO_CLASSES = (
    "person", "bicycle", "car", "motorcycle", "airplane", "bus", "train", "truck", "boat",
    "traffic light", "fire hydrant", "stop sign", "parking meter", "bench", "bird", "cat", "dog",
    "horse", "sheep", "cow", "elephant", "bear", "zebra", "giraffe", "backpack", "umbrella",
    "handbag", "tie", "suitcase", "frisbee", "skis", "snowboard", "sports ball", "kite",
    "baseball bat", "baseball glove", "skateboard", "surfboard", "tennis racket", "bottle",
    "wine glass", "cup", "fork", "knife", "spoon", "bowl", "banana", "apple", "sandwich", "orange",
    "broccoli", "carrot", "hot dog", "pizza", "donut", "cake", "chair", "couch", "potted plant",
    "bed", "dining table", "toilet", "tv", "laptop", "mouse", "remote", "keyboard", "cell phone",
    "microwave", "oven", "toaster", "sink", "refrigerator", "book", "clock", "vase", "scissors",
    "teddy bear", "hair drier", "toothbrush",
)


def read_labels(path: Path) -> dict[str, int]:
    """Count objects per class name in one YOLO-format label file."""
    counts: dict[str, int] = {}
    for line in path.read_text().splitlines():
        fields = line.split()
        if not fields:
            continue
        index = int(float(fields[0]))
        name = COCO_CLASSES[index] if index < len(COCO_CLASSES) else f"class_{index}"
        counts[name] = counts.get(name, 0) + 1
    return dict(sorted(counts.items()))


def select_images(labels: dict[str, dict[str, int]], wanted: int) -> list[str]:
    """Pick a spread of person counts so both the empty and the crowded cases are covered."""
    buckets: dict[str, list[str]] = {"crowd": [], "several": [], "few": [], "none": []}
    for stem in sorted(labels):
        people = labels[stem].get("person", 0)
        if people >= 7:
            buckets["crowd"].append(stem)
        elif people >= 3:
            buckets["several"].append(stem)
        elif people >= 1:
            buckets["few"].append(stem)
        else:
            buckets["none"].append(stem)
    order = ["crowd", "several", "few", "none"]
    selected: list[str] = []
    position = 0
    while len(selected) < wanted and any(len(buckets[key]) > position for key in order):
        for key in order:
            if len(selected) >= wanted:
                break
            if position < len(buckets[key]):
                selected.append(buckets[key][position])
        position += 1
    return sorted(selected)


@dataset(
    "coco128",
    modality="images",
    source="ultralytics/assets (COCO train2017 subset)",
    license="GPL-3.0 redistribution",
    description="Labelled photos: every image ships YOLO-format ground truth",
)
def build_coco128(context: Context) -> dict:
    archive = cached_download(context, COCO128_MIRRORS, "coco128.zip")
    digest = sha256_of(archive)
    if digest != COCO128_SHA256:
        sys.exit(
            f"coco128.zip checksum mismatch\n  expected {COCO128_SHA256}\n  actual   {digest}\n"
            "Delete the cached archive and retry, or update COCO128_SHA256 after verifying the "
            "source."
        )

    extracted = context.cache / "coco128"
    if context.options.force or not extracted.exists():
        staging = context.cache / "coco128-extract"
        shutil.rmtree(extracted, ignore_errors=True)
        shutil.rmtree(staging, ignore_errors=True)
        with zipfile.ZipFile(archive) as bundle:
            bundle.extractall(staging)
        (staging / "coco128").replace(extracted)
        shutil.rmtree(staging, ignore_errors=True)

    source_images = extracted / "images" / "train2017"
    source_labels = extracted / "labels" / "train2017"
    labels = {path.stem: read_labels(path) for path in sorted(source_labels.glob("*.txt"))}
    stems = select_images(labels, min(context.options.images, len(labels)))

    images_dir = context.directory("images", "coco128", "images")
    labels_dir = context.directory("images", "coco128", "labels")

    files: list[dict] = []
    for stem in stems:
        image = images_dir / f"{stem}.jpg"
        shutil.copyfile(source_images / f"{stem}.jpg", image)
        shutil.copyfile(source_labels / f"{stem}.txt", labels_dir / f"{stem}.txt")
        stream = probe_stream(image)
        counts = labels[stem]
        files.append(
            {
                "file": f"images/coco128/images/{stem}.jpg",
                "labels": f"images/coco128/labels/{stem}.txt",
                "width": stream.get("width"),
                "height": stream.get("height"),
                "bytes": image.stat().st_size,
                "sha256": sha256_of(image),
                "ground_truth": counts,
                "person_count": counts.get("person", 0),
            }
        )

    totals: dict[str, int] = {}
    for entry in files:
        for name, count in entry["ground_truth"].items():
            totals[name] = totals.get(name, 0) + count
    summary = {
        "image_count": len(files),
        "images_with_people": sum(1 for entry in files if entry["person_count"] > 0),
        "max_people_in_one_image": max((entry["person_count"] for entry in files), default=0),
        "objects_by_class": dict(sorted(totals.items(), key=lambda item: -item[1])),
    }
    log(
        f"coco128: {len(files)} images, {summary['images_with_people']} contain people, "
        f"up to {summary['max_people_in_one_image']} in one image"
    )
    return {
        "table_location": "images/coco128/images",
        "labels_location": "images/coco128/labels",
        "labels_format": "YOLO: class_index cx cy w h, normalized to the image",
        "upstream": COCO128_MIRRORS[0],
        "archive_sha256": COCO128_SHA256,
        "summary": summary,
        "files": files,
    }


# --------------------------------------------------------------------------------------------
# Video datasets
# --------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class VideoAsset:
    name: str
    url: str
    note: str


def build_video_dataset(context: Context, name: str, assets: tuple[VideoAsset, ...]) -> dict:
    videos_dir = context.directory("videos", name)
    clip_seconds = context.options.clip_seconds

    files: list[dict] = []
    for asset in assets:
        cached = cached_download(context, (asset.url,), asset.name)
        target = videos_dir / asset.name
        source_duration = float(probe_stream(cached).get("duration") or 0.0)
        trimmed = clip_seconds > 0 and source_duration > clip_seconds
        if trimmed:
            # Stream copy from the start of the file: no re-encode, and the cut is exact because
            # the range begins on the first key frame.
            subprocess.run(
                ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-i", str(cached),
                 "-t", str(clip_seconds), "-c", "copy", str(target)],
                check=True,
            )
        else:
            shutil.copyfile(cached, target)

        stream = probe_stream(target)
        frames = stream.get("nb_frames")
        files.append(
            {
                "file": f"videos/{name}/{asset.name}",
                "note": asset.note,
                "url": asset.url,
                "width": stream.get("width"),
                "height": stream.get("height"),
                "codec": stream.get("codec_name"),
                "source_fps": parse_ratio(stream.get("avg_frame_rate")),
                "duration_seconds": round(float(stream.get("duration") or 0.0), 3),
                "frame_count": int(frames) if frames and frames not in ("N/A", "0") else None,
                "trimmed_from_seconds": round(source_duration, 3) if trimmed else None,
                "bytes": target.stat().st_size,
                "sha256": sha256_of(target),
            }
        )
    log(f"{name}: {len(files)} clip{'' if len(files) == 1 else 's'} under {videos_dir}")
    return {
        "table_location": f"videos/{name}",
        "summary": {
            "clip_count": len(files),
            "total_seconds": round(sum(entry["duration_seconds"] for entry in files), 3),
            "source_frame_rates": sorted({entry["source_fps"] for entry in files if entry["source_fps"]}),
            "codecs": sorted({entry["codec"] for entry in files if entry["codec"]}),
        },
        "files": files,
    }


SAMPLE_VIDEOS = (
    VideoAsset(
        name="people-detection.mp4",
        url="https://github.com/intel-iot-devkit/sample-videos/raw/master/people-detection.mp4",
        note="indoor pedestrians, count varies over time",
    ),
    VideoAsset(
        name="person-bicycle-car-detection.mp4",
        url=(
            "https://github.com/intel-iot-devkit/sample-videos/raw/master/"
            "person-bicycle-car-detection.mp4"
        ),
        note="street scene mixing person, bicycle and car",
    ),
    VideoAsset(
        name="car-detection.mp4",
        url="https://github.com/intel-iot-devkit/sample-videos/raw/master/car-detection.mp4",
        note="road traffic, no people; negative case for person queries",
    ),
)


@dataset(
    "sample-videos",
    modality="videos",
    source="intel-iot-devkit/sample-videos",
    license="Apache-2.0",
    description="Detection clips covering person, bicycle and car at 12 and 12.5 fps",
)
def build_sample_videos(context: Context) -> dict:
    return build_video_dataset(context, "sample-videos", SAMPLE_VIDEOS)


VTEST_VIDEOS = (
    VideoAsset(
        name="vtest.avi",
        url="https://raw.githubusercontent.com/opencv/opencv/4.x/samples/data/vtest.avi",
        note="walking pedestrians; non-MP4 container and MPEG-4 Part 2 codec",
    ),
)


@dataset(
    "vtest",
    modality="videos",
    source="opencv/opencv",
    license="Apache-2.0",
    description="Pedestrian clip in an AVI container at 10 fps; container and codec coverage",
)
def build_vtest(context: Context) -> dict:
    return build_video_dataset(context, "vtest", VTEST_VIDEOS)


# --------------------------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("data/datasets"),
                        help="dataset root (default: data/datasets)")
    parser.add_argument("--dataset", action="append", metavar="NAME",
                        help="build one dataset; repeatable (default: all)")
    parser.add_argument("--list", action="store_true",
                        help="list the available datasets and exit")
    parser.add_argument("--images", type=int, default=32,
                        help="how many COCO128 images to keep, up to 128 (default: 32)")
    parser.add_argument("--clip-seconds", type=int, default=20,
                        help="trim each video to this length; 0 keeps the original (default: 20)")
    parser.add_argument("--force", action="store_true",
                        help="re-download instead of reusing the cache")
    arguments = parser.parse_args()

    if arguments.list:
        width = max(len(entry.root) for entry in DATASETS.values())
        for entry in DATASETS.values():
            print(f"{entry.root:<{width}}  {entry.license:<12}  {entry.description}")
        return

    selected = arguments.dataset or list(DATASETS)
    unknown = [name for name in selected if name not in DATASETS]
    if unknown:
        sys.exit(f"unknown dataset(s): {', '.join(unknown)}; run --list to see the available names")

    require_tool("ffprobe")
    if arguments.clip_seconds > 0:
        require_tool("ffmpeg")

    root = arguments.out.resolve()
    context = Context(
        root=root,
        cache=root / ".cache",
        options=Options(
            images=arguments.images,
            clip_seconds=arguments.clip_seconds,
            force=arguments.force,
        ),
    )
    root.mkdir(parents=True, exist_ok=True)
    context.cache.mkdir(parents=True, exist_ok=True)

    manifest_path = root / "manifest.json"
    previous = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    built: dict[str, dict] = dict(previous.get("datasets", {}))

    for name in selected:
        entry = DATASETS[name]
        built[name] = {
            "root": entry.root,
            "modality": entry.modality,
            "source": entry.source,
            "license": entry.license,
            "description": entry.description,
            **entry.build(context),
        }

    manifest = {
        "manifest_version": 2,
        "generated_by": "scripts/fetch_datasets.py",
        "datasets": {name: built[name] for name in sorted(built)},
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
    log(f"manifest written to {manifest_path}")


if __name__ == "__main__":
    main()
