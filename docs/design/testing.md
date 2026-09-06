# VisionQL Testing Design

> This document defines test ownership, suite boundaries, fixtures, execution policy, and coverage reporting. Component boundaries come from the [High-Level Design](../high_level_design.md).

VisionQL tests prove a contract at the narrowest boundary that owns it. The default workspace gate is deterministic, parallel, and independent of downloaded datasets, model artifacts, Docker, and network services. Real artifacts and process boundaries are covered by an explicit system suite.

## Test model

| Layer | Location | Proves | Dependencies | Default gate |
| --- | --- | --- | --- | --- |
| Unit | Owning module | Pure logic, branches, validation, state transitions | In-memory values and fakes | Yes |
| Crate integration / owner contract | `<crate>/tests/` or owning module | Public crate/API schemas, errors, persistence, and process behavior | Temporary directories, generated fixtures, `mock://`, local listeners | Yes |
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

### `vql-server`

Server tests own Flight SQL authentication and Session isolation, direct and prepared statement execution, query/update separation, execution tickets, attach and cancellation semantics, process-boundary IMAGE conversion, persistent Query lifecycle and recovery, daemon locking, health, and aggregate metrics. Protocol tests use the public Arrow Flight SQL Rust client instead of calling service internals.

### `vql-testing`

`vql-testing` contains only system tests over real artifacts, external services, or shipped process boundaries. It has no library target and no unit tests. Each Cargo test target has a top-level `<target>.rs` entry point; target-private harness code and SQL resources live under the same-named `tests/<target>/` directory. Behavior that can be proved inside one product crate stays with that crate. A system target may depend on a product crate when that crate's public embedded API is the boundary under test, but a process-boundary target must use the shipped executable and public protocol rather than importing host internals.

Its layout is:

```text
vql-testing/
├── Cargo.toml                 # test-only dependencies and explicit test targets
└── tests/
    ├── support/               # shared isolated-session and fixture helpers
    ├── image.rs               # real image and task-shaped inference runner
    ├── image/                 # image setup and assertion queries
    ├── video.rs               # real video and task-shaped inference runner
    ├── video/                 # video setup and assertion queries
    ├── model.rs               # real Catalog Model runner
    ├── model/                 # Catalog Model setup and assertion queries
    ├── rtsp.rs                # RTSP system-test runner
    ├── rtsp/                  # RTSP setup and query scripts
    ├── kafka.rs               # Kafka system-test runner
    ├── kafka/                 # Kafka setup and publish scripts
    └── vqld.rs                # containerized Flight SQL and persistent Query recovery runner
```

All dependencies are dev-dependencies, and the explicit `image`, `video`, `model`, `rtsp`, `kafka`, and `vqld` system-test targets require the `system-tests` feature. Cargo therefore selects no `vql-testing` target for the default workspace test and coverage graph.

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

Each system target exercises one public end-to-end boundary and uses Rust assertions over stable semantic outcomes. SQL resources live beside their runner under the matching `tests/<target>/` directory. Fetch the artifact prerequisites with:

```bash
python scripts/fetch_datasets.py
python scripts/export_yolo26.py --task detect --size n
python scripts/export_yolo26.py --task classify --size n
```

Run the real image, video, and Catalog Model targets without external services with:

```bash
VQL_SYSTEM_TEST=1 \
  cargo test -p vql-testing \
  --features system-tests \
  --test image \
  --test video \
  --test model \
  --locked
```

The targets prove these journeys:

| Target | Public journey | Stable assertions |
| --- | --- | --- |
| `image` | `IMAGES` → `VQL_CLASSIFY` / `VQL_DETECT` | Known classifications and detections, locator presence, unknown-class filtering, mixed-size batching |
| `video` | `VIDEOS` → decoded frames → `VQL_DETECT` | Eight distinct sequential frames with valid timing and dimensions, plus at least one person detection |
| `model` | `CREATE MODEL` → `RESOLVE MODEL` → direct call | A real Catalog Model resolves and produces a person detection |
| `rtsp` | RTSP frames → `VQL_DETECT` → `TUMBLE` | Eight frames, positive detections, and two internally consistent closed windows |
| `kafka` | `IMAGES` → `VQL_DETECT` → Kafka Sink | Broker acknowledgement and exact consumed JSON for the inferred semantic result |
| `vqld` | Packaged `vql shell --endpoint` and Flight SQL → attached RTSP / persistent RTSP-to-Kafka Query → container restart | Image contents, TLS and credential wiring, query/update routing, structured errors, exact execution cancellation, client-independent delivery, Catalog-backed stable Query identity, live-source reconnect gap, window-state reset, and terminal stop |

Embedded targets create an isolated temporary `VQL_HOME`. The `vqld` target uses an isolated Compose project and named volume so `$VQL_HOME/catalog/vql.db` survives the tested container restart and is removed during teardown. Task-shaped inference targets install only their required release-managed artifacts under their home. Real-model execution is serial within each target to bound CPU and model memory. Exact Arrow field names, types, and nullability remain kernel owner contracts rather than system assertions.

The root `docker-compose.yaml` defines pinned `mediamtx`, `kafka`, and `vqld` services. Compose owns the long-running dependencies and packaged daemon; the RTSP test owns its FFmpeg publisher and the Kafka test owns its topic and consumer group. The `vqld` service mounts an ephemeral TLS certificate and a project-scoped named volume, exposes only authenticated TLS Flight SQL, and keeps HTTP health checks on the container loopback interface. Run every strict system target in an isolated Compose project with:

```bash
scripts/run-system-tests.sh
```

Pass Cargo test filters through the wrapper to run one target, for example:

```bash
scripts/run-system-tests.sh --test vqld
```

The wrapper provisions only the services required by selected targets: none for `image`, `video`, or `model`; MediaMTX for `rtsp`; Kafka for `kafka`; and MediaMTX, Kafka, and the locally built `vqld` image for `vqld`. It chooses free host ports, generates an ephemeral TLS certificate, waits for readiness, enables `system-tests`, sets `VQL_SYSTEM_TEST=1`, emits selected-service logs on failure, and always removes containers, named volumes, and temporary credentials. The `vqld` target uses separate host and Compose-network RTSP/Kafka addresses, invokes only public clients, and asks Compose to restart the daemon. When a system target is invoked manually without strict mode, a missing prerequisite is reported as ignored. Strict mode turns every missing prerequisite into a failure.

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
| System suite | Explicit local or release validation through `scripts/run-system-tests.sh` |

System scenarios remain outside the default pull-request matrix because they require downloaded artifacts and provisioned services. Changes to media decoding, ONNX execution, RTSP, Kafka, or their system contracts must report the relevant explicit system run in the pull request.
