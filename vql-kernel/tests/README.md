# vql-kernel integration tests

The integration suite runs SQL against the downloaded example datasets and the exported YOLO26
model. Parser, error, catalog, planner, and `mock://` behavior stays in unit tests.

## Run the suite

Fetch the untracked fixtures once:

```bash
python scripts/fetch_datasets.py
python scripts/export_yolo26.py --size n
```

Then run all cases or filter by path:

```bash
cargo test -p vql-kernel --test integration
VQL_TEST_CASE=functions/image_detection cargo test -p vql-kernel --test integration
```

Every main `.sql` file is registered as an individual test, so the output names each case and the
summary reports the number of SQL cases rather than one aggregate runner test. List discovered
cases with `cargo test -p vql-kernel --test integration -- --list`.

The suite skips when fixtures are absent. Set `VQL_INTEGRATION_TEST=1` to make missing fixtures a
failure.

## RTSP source scenarios

The default owner-module scenario generates a short video and uses the deterministic
`mock://person` Model to cover Source → epoch → `IMAGE_DETECTION` → Filter → global `LIMIT`:

```bash
cargo test -p vql-kernel local_video_stream_runs_people_detection_scenario --locked
```

The real-data integration test starts MediaMTX, publishes
`data/datasets/videos/sample-videos/people-detection.mp4` with FFmpeg, creates the Stream through
public SQL, and runs `data/models/yolo26n.onnx` over eight sampled RTSP frames:

```bash
cargo test -p vql-kernel --test rtsp_stream --locked -- --nocapture
```

It runs by default when MediaMTX, FFmpeg, the dataset, and the Model are present, and reports a skip
message otherwise because those dependencies are external to the repository. Streaming `TUMBLE`
remains an unfinished v0.1 stateful operator; these RTSP scenarios deliberately exercise the
currently supported stateless plan.

## Layout

Cases are grouped by SQL-facing behavior:

```text
tests/
├── ddl/
├── functions/
├── scenarios/
├── integration_tests.rs
├── rtsp_stream.rs
└── README.md
```

- `ddl/` covers one DDL statement per case.
- `functions/` covers one built-in function per case.
- `scenarios/` covers a complete user query that crosses several features.
- `rtsp_stream.rs` owns the prerequisite-gated MediaMTX/FFmpeg real-RTSP system scenario.

A case consists of one main statement and its expected result. Setup and teardown are optional:

```text
image_detection.setup.sql
image_detection.sql
image_detection.expected.json
image_detection.teardown.sql
```

The runner creates a fresh catalog and `VQL_HOME` for every case, then executes setup, the main
statement, and teardown in that order. Teardown is still attempted when the main statement fails.
Isolation prevents one failed case from affecting another; teardown keeps the case lifecycle
explicit and exercises cleanup SQL.

The main `.sql` file must contain exactly one statement. Put all prerequisite DDL in
`*.setup.sql` and cleanup in `*.teardown.sql`.

## Expected results

Expected JSON has only two fields:

```json
{
  "schema": ["found BOOLEAN"],
  "rows": [[true]]
}
```

Schema entries use `name TYPE`; names, types, values, and row order are exact. Logical VisionQL
types such as `IMAGE` are reported by their public type rather than their Arrow storage type. When
a real-model count can vary by execution provider, return the property the case needs to prove,
such as `COUNT(*) > 0`, instead of snapshotting an incidental count. Add `ORDER BY` whenever row
order is part of the result.

Main-query result columns must be `BOOLEAN`, integer, floating-point, or `VARCHAR`; cast other
types before returning them.

## Placeholders

Setup and main SQL can use:

| Placeholder | Value |
| --- | --- |
| `${IMAGES_LOCATION}` | `data/datasets/images/coco128/images` |
| `${VIDEOS_LOCATION}` | `data/datasets/videos/sample-videos` |
| `${MODEL}` | `data/models/yolo26n.onnx` |

## Adding a case

1. Choose `ddl`, `functions`, or `scenarios` by the behavior under test.
2. Give the main file one statement and one purpose.
3. Move object creation and cleanup into setup and teardown sidecars.
4. Return only the columns needed to prove that purpose.
5. Return stable values that can be compared exactly.
6. Run the filtered case before the whole integration target.
