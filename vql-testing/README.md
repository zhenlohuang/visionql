# vql-testing

`vql-testing` owns shared SQL conformance and cross-component system tests. It is the common test
layer for the embedded kernel today and for future `vql-cli` and `vql-server` adapters. Product
crates should keep their own integration tests small and limited to contracts they own.

## Test layers

```text
vql-kernel unit and owner tests
└── synthetic fixtures, mock:// models, exact Rust-level schemas

vql-testing SQL conformance
└── sqllogictest cases executed through an embedded Engine adapter

vql-testing system scenarios
└── public VQL + real data/model + external services from Docker Compose
```

The default workspace suite never requires Docker or downloaded fixtures. A missing fixture or
external dependency is reported as an ignored test. `VQL_INTEGRATION_TEST=1` turns it into a
failure for explicitly provisioned runs.

## SQL conformance

Cases use [`sqllogictest-rs`](https://github.com/risinglightdb/sqllogictest-rs) and live under:

```text
tests/cases/
├── ddl/
├── functions/
└── scenarios/
```

Run or list the cases with:

```bash
cargo test -p vql-testing --test sql --locked
cargo test -p vql-testing --test sql --locked -- --list
VQL_TEST_CASE=functions/image_detection cargo test -p vql-testing --test sql --locked
```

Every `.slt` file is a separate Cargo test and receives a fresh `Engine`, catalog, and temporary
`VQL_HOME`. An unmatched `VQL_TEST_CASE` fails instead of silently running nothing. The adapter
uses strict column-family validation and exact value comparison; it does not normalize whitespace.

Column codes are:

| Code | VisionQL result family |
| --- | --- |
| `B` | Boolean |
| `I` | Signed or unsigned integer |
| `R` | Floating point or decimal |
| `T` | Text and other displayable Arrow values |

Sqllogictest validates the ordered type vector but does not carry field names or nullability.
Protect those kernel-owned contracts with focused Rust tests under `vql-kernel/tests/`; do not add
a private test-file grammar.

VisionQL DDL returns a one-column result row, so write DDL records as `query T` with the expected
message instead of `statement ok`.

Cases may enable substitution and use:

| Placeholder | Value |
| --- | --- |
| `${IMAGES_LOCATION}` | `data/datasets/images/coco128/images` |
| `${VIDEOS_LOCATION}` | `data/datasets/videos/sample-videos` |
| `${MODEL}` | `data/models/yolo26n.onnx` |

Fetch real fixtures with:

```bash
python scripts/fetch_datasets.py
python scripts/export_yolo26.py --size n
```

Keep one behavior per `.slt` file. Put prerequisite statements before the assertion they support.
Explicit teardown is unnecessary because every file owns an isolated temporary catalog; lifecycle
behavior such as `DROP` belongs in its own case or a kernel owner test.

## Docker Compose, RTSP, and Kafka

The root `docker-compose.yaml` is shared by development and tests. Profiles keep optional services
off by default:

- `rtsp` starts the pinned MediaMTX service.
- `kafka` starts the pinned single-node Apache Kafka service.
- Add `server` when `vql-server` exists and needs a process boundary test.

Start the current development dependency with:

```bash
docker compose --profile rtsp up -d mediamtx
docker compose --profile rtsp down
```

Run the Kafka Sink system test against that profile with:

```bash
docker compose --profile kafka up -d kafka
VQL_INTEGRATION_TEST=1 \
VQL_TEST_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 \
  cargo test -p vql-testing --test kafka --locked -- --nocapture
docker compose --profile kafka down
```

The test creates an isolated topic, publishes a bounded SQL result, waits for producer
acknowledgements, then consumes and compares the exact JSON messages.

Compose owns long-running external services. The RTSP case owns its FFmpeg publisher so each test
controls its input lifecycle. To run that case against an already-running MediaMTX:

```bash
VQL_INTEGRATION_TEST=1 \
VQL_TEST_RTSP_URL=rtsp://127.0.0.1:8554/people \
  cargo test -p vql-testing --test rtsp --locked -- --nocapture
```

Or provision an isolated Compose project and run all strict integration tests:

```bash
scripts/run-integration-tests.sh
```

The wrapper chooses free host ports when `VQL_RTSP_PORT` or `VQL_KAFKA_PORT` is unset, uses a
unique Compose project name, waits for both dependencies, and always tears the project down.
Override `VQL_COMPOSE_PROJECT`, `VQL_RTSP_PORT`, or `VQL_KAFKA_PORT` when needed.
