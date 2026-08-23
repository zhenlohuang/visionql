# VisionQL Testing Design

> This document defines test ownership, shared SQL conformance, fixtures, and external-service system scenarios. Component boundaries come from the [High-Level Design](./high_level_design.md).

Tests live with the narrowest component that can prove the contract. Product crates own local invariants; `vql-testing` owns shared SQL behavior and cross-component system scenarios. A higher test layer must not replace a cheaper owner test for an exact schema, error code, parser rule, or host API.

## Test layers

```text
crate unit and owner tests
└── synthetic fixtures, mock:// models, exact schemas and public API contracts

vql-testing SQL conformance
└── sqllogictest cases executed through an embedded Engine adapter

vql-testing system scenarios
└── public VQL + real data/model + external services from Docker Compose

Python binding tests
└── PyO3/PyArrow API and Python UDF host
```

Ownership rules:

- Rust unit tests stay beside the module that owns the invariant.
- Crate integration tests cover public crate contracts using synthetic inputs or `mock://` models.
- Exact Arrow field names, types, nullability, and stable error codes remain in owner tests because sqllogictest cannot express them.
- CLI command and terminal contracts live in `vql-cli`; Python API contracts live in `vql-python/tests`.
- One-purpose `.slt` cases cover SQL behavior visible across components.
- Real data, real models, RTSP, Kafka, and process-boundary behavior belong to explicit system scenarios.

The default workspace suite never requires Docker or downloaded fixtures. A missing real fixture or external dependency is reported as an ignored test. `VQL_INTEGRATION_TEST=1` turns a missing prerequisite into a failure for explicitly provisioned runs.

## SQL conformance

Cases use [`sqllogictest-rs`](https://github.com/risinglightdb/sqllogictest-rs) and live under [`vql-testing/tests/cases`](../vql-testing/tests/cases):

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
VQL_TEST_CASE=functions/model_call cargo test -p vql-testing --test sql --locked
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
| `${BUILTIN_DETECTION_MODEL}` | Copies `data/models/yolo26n.onnx` to the isolated Engine's `$VQL_HOME/models/yolo26n.onnx` |
| `${BUILTIN_CLASSIFICATION_MODEL}` | Copies `data/models/yolo26n-cls.onnx` to the isolated Engine's `$VQL_HOME/models/yolo26n-cls.onnx` |

Fetch real fixtures with:

```bash
python scripts/fetch_datasets.py
python scripts/export_yolo26.py --task detect --size n
python scripts/export_yolo26.py --task classify --size n
```

Keep one behavior per `.slt` file. Put prerequisite statements before the assertion they support.
Explicit teardown is unnecessary because every file owns an isolated temporary catalog; lifecycle
behavior such as `DROP` belongs in its own case or a kernel owner test.

## Model and Function contract coverage

Model and Function redesign contracts are split by owning boundary:

- Catalog owner tests pin the cross-kind callable-name constraint under concurrent creates and renames, immutable resolved versions, initial/default publication rules, version tombstones and reuse, and snapshot isolation from concurrent aggregate mutations.
- Kernel parser and DDL tests pin both `CREATE MODEL` interface forms, flat option ownership, Runtime inference, every `ALTER MODEL` lifecycle constraint, sanitized multi-version `SHOW CREATE MODEL`, and the schemas of `SHOW MODELS`, `SHOW MODEL VERSIONS`, `SHOW FUNCTIONS`, and `DESCRIBE`.
- Planner tests prove direct Model-call extraction, constant `version =>` selection, per-version deduplication, volatile-call preservation, constant lifting, SQL-wrapper expansion, inferred constant-only parameters through nested Functions, built-in AI lowering, and stable non-IMAGE overload failures.
- ONNX Runtime owner tests use synthetic graphs for introspected names and static shapes, ambiguous output formats and labels, generic multi-input binding, scalar `[N]` inputs, input-processing union errors, `VECTOR`/`TENSOR` metadata, dynamic-shape rejection, and NULL-row compaction and scatter.
- Shared SQL conformance keeps one direct Model call and the two YOLO26-backed built-in AI functions visible through the public engine adapter. Real ONNX and Triton fixtures remain system evidence rather than substitutes for the owner tests above.

## Docker Compose, RTSP, and Kafka

The root `docker-compose.yaml` is shared by development and tests. Profiles keep optional services
off by default:

- `rtsp` starts the pinned MediaMTX service.
- `kafka` starts the pinned single-node Apache Kafka service.

Add a new profile only when a component genuinely needs a process boundary; nothing starts by default.

Start the current development dependency with:

```bash
docker compose --profile rtsp up -d mediamtx
docker compose --profile rtsp down
```

Run the Kafka table-write system test against that profile with:

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
