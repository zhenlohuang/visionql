# VisionQL Testing Design

> This document defines test ownership, suite boundaries, fixtures, execution policy, and coverage reporting. Component boundaries come from the [High-Level Design](./high_level_design.md).

VisionQL tests prove a contract at the narrowest boundary that owns it. The default workspace gate is deterministic, parallel, and independent of downloaded datasets, model artifacts, Docker, and network services. Real artifacts and process boundaries are covered by an explicit system suite.

## Test model

| Layer | Location | Proves | Dependencies | Default gate |
| --- | --- | --- | --- | --- |
| Unit | Owning module | Pure logic, branches, validation, state transitions | In-memory values and fakes | Yes |
| Owner contract | `<crate>/tests/` or owning module | Public crate/API schemas, errors, persistence, and process behavior | Temporary directories, generated fixtures, `mock://` | Yes |
| Kernel SQL contract | `vql-kernel/tests/slt/` | Stable embedded Engine SQL behavior | Generated images, empty directories, `mock://` | Yes |
| Python API | `vql-python/tests/` | PyO3/PyArrow conversion, Python UDF hosting, and Python errors | Built extension and generated fixtures | Separate CI job |
| System | `vql-testing/tests/` | Real ONNX/media execution and external-service boundaries | Downloaded artifacts, FFmpeg, Docker Compose | No |

A broader layer does not replace a cheaper owner test. Exact error identifiers, Arrow field names, types, nullability, parser rules, retry/cancellation semantics, and resource accounting stay with the component that defines them. System tests provide compatibility evidence; they are not the only proof of local behavior.

## Ownership by crate

### `vql-catalog`

Catalog tests own domain validation, callable namespace conflicts, revisions, immutable resolved versions, snapshot isolation, backend capability boundaries, SQLite reopen behavior, and Unity Catalog-compatible HTTP envelopes. Store tests exercise transactions and concurrency directly; HTTP tests exercise only routing and wire contracts that the catalog API owns.

### `vql-kernel`

Kernel tests own SQL parsing and rendering, planning and normalization, schemas, stable `VqlError` mappings, session isolation, memory accounting, cancellation, media timing, connector serialization, model resolution, batching, tensor conversion, preprocessing, and postprocessing. Use synthetic Arrow arrays, generated media, local fake servers, and `mock://` models before introducing a real artifact.

SQL behavior that executes only through the embedded `Engine` is a kernel contract. Sqllogictest cases pin ordered result types and values; `result_schema.rs` pins field names, Arrow types, and nullability. Cross-module behavior may use the public `Engine` API under `vql-kernel/tests/`; internal branches remain beside their implementations.

### `vql-cli`

CLI tests own the literal `shell` and `run` surface, argument rejection, standalone `\q`, script ordering, terminal rendering, exit status, stdout, and stderr. Parser and shell helpers use module tests; at least one process-level contract executes the built `vql` binary with an isolated `VQL_HOME`.

### `vql-python`

Python tests own connection arguments, query collection, PyArrow conversion, Python UDF registration and invocation, exception fields, HTML representation, and Python helper APIs. Rust workspace coverage excludes the extension crate; the Python CI job builds a wheel and publishes a separate Python coverage report.

### `vql-testing`

`vql-testing` contains only real-artifact and external-service integration tests. It has no library target and no unit tests. Each Cargo test target has a top-level `<target>.rs` entry point; target-private harness code and SQL resources live under the same-named `tests/<target>/` directory. Behavior that can be proved inside one product crate stays with that crate.

Its layout is:

```text
vql-testing/
├── Cargo.toml                 # test-only dependencies and explicit test targets
└── tests/
    ├── slt.rs                 # real-data/model Sqllogictest runner
    ├── slt/
    │   ├── harness.rs         # private Sqllogictest adapter
    │   ├── functions/
    │   └── models/
    ├── rtsp.rs                # RTSP system-test runner
    ├── rtsp/                  # RTSP setup and query scripts
    ├── kafka.rs               # Kafka system-test runner
    └── kafka/                 # Kafka setup and publish scripts
```

All dependencies are dev-dependencies, and the explicit `slt`, `rtsp`, and `kafka` integration-test targets require the `system-tests` feature. Cargo therefore selects no `vql-testing` target for the default workspace test and coverage graph.

## Default gate

The required local Rust gate is:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

The default suite creates all mutable state below per-test temporary directories. Owner tests generate small media fixtures and use `mock://` for inference. They do not inspect repository-local datasets or models, so a fresh clone and a developer checkout execute the same tests.

Tests may run concurrently unless they mutate process-global state or share a provisioned external service. Such tests must isolate the state first; serialization is the final fallback and is explicit in the system harness.

## Kernel SQL contracts

Deterministic SQL cases use [`sqllogictest-rs`](https://github.com/risinglightdb/sqllogictest-rs) and live under `vql-kernel/tests/slt/{ddl,connectors,functions,models}`. There is no generic `scenarios` category: a synthetic multi-step case belongs to the kernel domain that owns its behavior.

Run or filter them with:

```bash
cargo test -p vql-kernel --test slt --locked
cargo test -p vql-kernel --test slt --locked -- --list
VQL_TEST_CASE=models/object_detection cargo test -p vql-kernel --test slt --locked
```

Every `.slt` file appears as an individual Cargo test and receives a fresh Engine, catalog, and temporary `VQL_HOME`. Cases run in parallel and share only generated, read-only image and directory fixtures. An unmatched `VQL_TEST_CASE` fails instead of silently executing nothing.

The adapter uses `B`, `I`, `R`, and `T` for Boolean, integer, real, and text result families. It compares values exactly without whitespace normalization. Sqllogictest does not carry field names or nullability, so those assertions remain in `result_schema.rs`. VisionQL DDL returns a one-column result row and therefore uses `query T` with the exact returned message rather than `statement ok`.

## System suite

Real-data and real-model Sqllogictest cases live under `vql-testing/tests/slt/{functions,models}`. The runner's private adapter is `slt/harness.rs`; RTSP and Kafka SQL scripts live beside their runners under `tests/rtsp/` and `tests/kafka/`. Fetch the artifact prerequisites with:

```bash
python scripts/fetch_datasets.py
python scripts/export_yolo26.py --task detect --size n
python scripts/export_yolo26.py --task classify --size n
```

Run only the real SQL cases with:

```bash
VQL_INTEGRATION_TEST=1 \
  cargo test -p vql-testing \
  --features system-tests \
  --test slt \
  --locked
```

The system SQL harness uses [`sqllogictest-rs`](https://github.com/risinglightdb/sqllogictest-rs), exposes each `.slt` file as an individual Cargo test, and runs serially to bound CPU and model memory. `VQL_TEST_CASE` filters by the relative case name and fails when no case matches. The adapter compares values exactly without whitespace normalization and recognizes these result families:

| Code | VisionQL result family |
| --- | --- |
| `B` | Boolean |
| `I` | Signed or unsigned integer |
| `R` | Floating point or decimal |
| `T` | Text and other displayable Arrow values |

Sqllogictest validates ordered result types and values, but not field names or nullability; those remain kernel owner contracts. VisionQL DDL therefore uses `query T` with its exact returned message rather than `statement ok`.

The harness recognizes these substitutions:

| Placeholder | Repository artifact |
| --- | --- |
| `${IMAGES_LOCATION}` | `data/datasets/images/coco128/images` |
| `${VIDEOS_LOCATION}` | `data/datasets/videos/sample-videos` |
| `${MODEL}` | `data/models/yolo26n.onnx` |
| `${BUILTIN_DETECTION_MODEL}` | Detection model installed into the isolated `VQL_HOME` |
| `${BUILTIN_CLASSIFICATION_MODEL}` | Classification model installed into the isolated `VQL_HOME` |

The root `docker-compose.yaml` exposes pinned optional `rtsp` and `kafka` profiles. Compose owns long-running services; the RTSP test owns its FFmpeg publisher and the Kafka test owns its topic and consumer group. Run every strict system target in an isolated Compose project with:

```bash
scripts/run-integration-tests.sh
```

The wrapper chooses free host ports, waits for both services, enables `system-tests`, sets `VQL_INTEGRATION_TEST=1`, emits service logs on failure, and always tears the project down. When a system target is invoked manually without strict mode, a missing prerequisite is reported as ignored. Strict mode turns every missing prerequisite into a failure.

## Python API gate

Build and run the Python suite locally with:

```bash
python -m pip install pytest
cd vql-python
maturin develop --locked
cd ..
python -m pytest -q vql-python/tests
```

CI builds a wheel against Python 3.12, installs it, runs the API suite, prints missing Python lines, and uploads `python-coverage.xml`. Python tests use `tmp_path` and generated images; they do not consume the system fixtures.

## Coverage policy

Coverage is a diagnostic and prioritization signal, not a substitute for contract assertions. Every behavior change and regression fix adds a focused test at its owning boundary. Reviews examine uncovered error paths and branch behavior in the changed modules rather than accepting a repository percentage alone.

Rust CI executes the default suite once under `cargo-llvm-cov`, excluding `vql-python`, and uploads HTML and LCOV reports. Generate the same report locally with:

```bash
rustup component add llvm-tools-preview
cargo install cargo-llvm-cov --locked
cargo llvm-cov \
  --workspace \
  --exclude vql-python \
  --locked \
  --no-cfg-coverage \
  --html
cargo llvm-cov report \
  --ignore-filename-regex '/vql-python/' \
  --summary-only
```

The Rust report includes unit, owner-contract, kernel SQL, and CLI process tests. It excludes feature-gated system targets and Python execution. Python coverage is reported separately because combining Rust source instrumentation and Python source coverage into one percentage would misstate both surfaces.

Coverage work prioritizes public error branches, catalog transactions, planner rewrites, cancellation and resource cleanup, serialization boundaries, null handling, and shape/type validation. Repeating the same happy path at more expensive layers does not improve meaningful coverage.

## Efficiency and reliability rules

- Prefer values, Arrow arrays, temporary directories, generated PNGs, fake services, and `mock://` models in that order before real artifacts.
- Use virtual time or explicit synchronization for asynchronous state. Do not rely on arbitrary sleeps when a readiness condition can be observed.
- Give every blocking system operation a bounded timeout and include the failed boundary in its error.
- Keep default tests independent so Cargo can use its normal parallel runner.
- Keep real model execution serial unless a scenario specifically proves concurrency.
- Do not make behavior conditional on assets that happen to exist in the checkout.
- Do not weaken a failed test by broadening tolerances, increasing timeouts, or skipping it without changing the documented contract.

## CI matrix

| Job | Required behavior |
| --- | --- |
| Static analysis | Formatting, Compose configuration, workspace Clippy |
| Rust tests and coverage | One deterministic default workspace run under coverage instrumentation |
| Python API | Wheel build, Python API tests, Python source coverage |
| System suite | Explicit local or release validation through `scripts/run-integration-tests.sh` |

System scenarios remain outside the default pull-request matrix because they require downloaded artifacts and provisioned services. Changes to media decoding, ONNX execution, RTSP, Kafka, or their system contracts must report the relevant explicit system run in the pull request.
