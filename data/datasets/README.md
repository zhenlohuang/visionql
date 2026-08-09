# Example datasets

Small public datasets used to exercise VisionQL locally. Everything here except this file is
fetched on demand and ignored by Git:

```bash
python scripts/fetch_datasets.py          # about 10 MB
python scripts/fetch_datasets.py --list   # what is available
```

The tree is keyed by modality first and dataset second, so a new dataset slots in as one more
subdirectory. A dataset holding only media puts its files directly in its own directory; one that
also ships ground truth splits it out inside:

```text
data/datasets/
├── images/
│   └── coco128/
│       ├── images/     32 labelled photos (.jpg)
│       └── labels/     matching YOLO-format ground truth (.txt)
├── videos/
│   ├── sample-videos/  3 detection clips, 20 s each
│   └── vtest/          1 pedestrian clip, 20 s
├── manifest.json       index of every dataset, with per-file properties and ground truth
└── .cache/             downloaded archives, reused across runs
```

A table points at one dataset; `manifest.json` records the path as `table_location`:

```sql
CREATE TABLE photos USING IMAGES LOCATION './data/datasets/images/coco128/images/';
CREATE TABLE clips  USING VIDEOS LOCATION './data/datasets/videos/sample-videos/' WITH (fps = 2);
```

Because modality comes first, a whole modality is also a valid table root — handy for a quick
sweep across every dataset at once:

```sql
CREATE TABLE all_clips USING VIDEOS LOCATION './data/datasets/videos/'
WITH (recursive = true, fps = 2);
```

Keep runtime state out of the dataset tree by exporting `VQL_HOME=./data/.vql` from the repository
root.

## coco128 — labelled images

[COCO128](https://github.com/ultralytics/assets/releases/download/v0.0.0/coco128.zip) is the first
128 images of COCO `train2017` plus YOLO-format labels, redistributed by Ultralytics under
GPL-3.0. The script keeps a spread of crowd sizes so both the empty and the crowded cases appear:
of the default 32 images, 24 contain people and the busiest holds 13.

Because every image ships ground truth, detection results are checkable rather than merely
plausible. `manifest.json` carries the per-image counts by class name, so a query like

```sql
SELECT uri, COUNT_OBJECTS(detect(image), 'person', 0.6) AS people FROM photos ORDER BY uri
```

can be compared against `datasets.coco128.files[].ground_truth.person`. Label files sit in
`images/coco128/labels/` and use the YOLO convention — `class_index cx cy w h`, normalized to the
image — with class indexes in COCO 2017 order. They share the dataset directory with the images
rather than a tree of their own, and the `.txt` extension keeps them out of the image table.

Pass `--images 128` to take the whole set.

## sample-videos and vtest — detection clips

| Dataset | File | Resolution | Source fps | Content |
| --- | --- | --- | --- | --- |
| `sample-videos` | `people-detection.mp4` | 768×432 | 12 | Indoor pedestrians; the count changes over time |
| `sample-videos` | `person-bicycle-car-detection.mp4` | 768×432 | 12 | Street scene mixing person, bicycle and car |
| `sample-videos` | `car-detection.mp4` | 768×432 | 12.5 | Road traffic with no people — negative case |
| `vtest` | `vtest.avi` | 768×576 | 10 | Walking pedestrians; AVI container, MPEG-4 Part 2 |

`sample-videos` comes from [intel-iot-devkit/sample-videos](https://github.com/intel-iot-devkit/sample-videos)
and `vtest` from [opencv/opencv](https://github.com/opencv/opencv/blob/4.x/samples/data/vtest.avi);
both are Apache-2.0. They are separate datasets because they are separate upstreams — and keeping
`vtest` apart lets a query opt into the AVI/MPEG-4 container path deliberately rather than by
accident, while `videos/` with `recursive = true` still covers both.

Three source frame rates and two containers exercise fps sampling and container probing; the
changing pedestrian count is what makes a `TUMBLE` aggregation produce different values per window
instead of a flat line.

Clips are trimmed with a stream copy, so no re-encode happens and the frame counts in
`manifest.json` are exact. Use `--clip-seconds 0` to keep the full-length originals (30–80 s).

## Refreshing

`--dataset NAME` (repeatable) rebuilds one dataset and leaves the others' manifest entries intact;
`--force` re-downloads instead of reusing `.cache/`. The COCO128 archive is checksum-verified on
every run, so a changed upstream artifact fails loudly instead of silently altering the ground
truth.

Adding a dataset means adding one `@dataset(...)` builder in `scripts/fetch_datasets.py`; the
layout, manifest and CLI pick it up automatically.
