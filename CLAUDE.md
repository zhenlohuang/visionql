# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`AGENTS.md` holds the repository conventions (style, commit/PR expectations, file hygiene). This file adds the architecture context that is only visible after reading several files.

## Commands

```bash
export VQL_HOME="$PWD/data/.vql"          # keep local state out of $HOME for all repo work

cargo build --workspace --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked           # the three CI gates

cargo run -q -p vql-cli -- shell
cargo run -p vql-cli -- run examples/sql/video_people_count.sql
```

Tests:

```bash
cargo test -p vql-kernel --test slt --locked                      # kernel-owned SQL contracts
VQL_TEST_CASE=models/object_detection cargo test -p vql-kernel --test slt --locked # filter cases
VQL_INTEGRATION_TEST=1 cargo test -p vql-testing --features system-tests --test image --test video --test model --locked # real artifacts
cargo test -p vql-kernel session::tests::model_calls_are          # single Rust unit test by path
scripts/run-integration-tests.sh                                  # strict suite with profiled Compose services
```

Python (`vql-python` is a PyO3/Maturin extension, not a pure-Python package):

```bash
source .venv/bin/activate && python -m pip install "maturin>=1.9,<2" pytest
cd vql-python && maturin develop --locked && cd ..
python -m pytest -q vql-python/tests
```

Toolchain is pinned to Rust 1.91.1 by `rust-toolchain.toml` (workspace MSRV is 1.88). `pre-commit install` wires fmt+clippy to pre-commit and the workspace test suite to pre-push. `VQL_LOG_LEVEL=debug` enables debug tracing.

`VQL_HOME` is the whole local state root: the SQLite catalog, the model cache, shell history, and the optional strict schema-v1 `config.toml` all resolve under it (`config.rs`), with relative config paths resolved against it. Hosts call `EngineConfig::load()`; `vql-kernel` reads no other global state.

## Architecture

Five crates: `vql-catalog` (catalog domains, snapshots, backend ports, SQLite, Arrow storage schemas, and the UC-compatible API), `vql-kernel` (engine and owner tests), `vql-cli` (clap + reedline shell), `vql-python` (PyO3 bindings + Python UDF host), and `vql-testing` (integration-test targets only; no library or unit tests). The hosts only inject config and optional capabilities; `vql-kernel` never depends on clap or PyO3, never opens a port, and reads no global singletons.

`Engine` (`engine.rs`) owns four long-lived pieces shared by every session: the SQLite `CatalogStore`, a Tokio runtime, `MediaRuntime`, and `ModelRuntime`. `Session` is a cheap clone over the engine plus per-session state (`fail_on_error`, active-query cancellation token, optional Python UDF host).

### Statement path

`Session::sql` (`session.rs`) is the single entry point and splits on statement kind:

- **VQL DDL** (`CREATE TABLE/MODEL`, `RESOLVE MODEL`, `DROP`, `SHOW`, `DESCRIBE`) is parsed by the hand-written parser in `sql/ddl_parser.rs` into `sql/ast.rs` types and validated before commit. `CREATE FUNCTION` is classified there but parsed and validated by DataFusion's `CreateFunction` and VisionQL `FunctionFactory`; the resulting normalized definition is then committed to the Catalog. `Statement::Ddl` carries both a human message and a RecordBatch.
- **Queries** go through `planner::plan_statement`: `planner/normalize.rs` performs textual rewrites (SQL-macro expansion, typed-inference named arguments, `box.center` → `BOX_CENTER`, correlated `UNNEST`), DataFusion plans the result, and `planner/inference.rs` rewrites typed inference markers into `InferenceNode` extension nodes.
- `INSERT INTO <table> SELECT ...` resolves a writable provider Table and wraps bounded DataFrames with the internal `planner/sink.rs` nodes (`SinkWrite` / `SinkExec`).

### Three invariants worth preserving

**Model calls are plan nodes, not row UDFs.** Every persisted Model interface registers a typed volatile marker under the Model's own callable name. `planner/inference.rs` binds the version and semantic constants from the `DefinitionSnapshot`, then lifts the call into an `InferenceNode`. Identical invocations deduplicate only when the selected version, interface, and every input are deterministic. `InferenceExec` uses the `PreProcessor → RuntimeSession → PostProcessor` pipeline: VisionQL batches local ONNX Runtime work, while Triton owns dynamic batching for KServe V2 requests. Tests assert on `InferenceNode` / `InferenceExec` in the plan text — that is the contract.

**`IMAGE` carries references, not pixels.** `IMAGE` is an Arrow `Struct` (schema in `vql-catalog/src/objects.rs`, kernel helpers in `types/image.rs`) with `ARROW:extension:name = vql.image` metadata and ten nullable fields; the three payload shapes are *referenced* (`uri` + `locator`), *buffered* (`buffer_id`/`buffer_slot`, in-process only), and *encoded* (`encoded` + `encoding`, used at process boundaries such as Python, JSON, and Kafka). Scans never decode. Decoding is triggered only by an explicit consumer such as inference preprocessing or a Python UDF, and `MediaRuntime` counters exist so tests can assert that a metadata-only query decoded zero frames. Preserving that property is a load-bearing part of nearly every media change.

**Unbounded plans are allowlisted, not merely planned.** Streaming does not accept whatever DataFusion can plan (see below). Adding a streaming capability means extending the allowlist deliberately and rejecting everything else with a rewrite suggestion, not relaxing validation.

### Batch and streaming paths

Both paths share one planner and one type system; only execution differs. `QueryHandle::is_unbounded()` selects the path.

A bounded query executes as an ordinary DataFusion stream. An unbounded query (exactly one `USING RTSP` table) is driven by the epoch coordinator in `session.rs` (`stream_rtsp`), not by DataFusion alone:

- `connectors/rtsp.rs` decodes on dedicated worker threads and closes a `StreamEpoch` at 200 ms of event time or 64 sampled rows. Data, `SourceProgress`, watermark, and the frame-buffer lease travel as separate fields, so a `Filter` that drops every row still cannot stall progress or frame reclamation.
- The coordinator keeps the *logical* template and, per epoch, rebinds the stream scan to a single-partition `MemTable` (`bind_stream_epoch`) and lets DataFusion build a fresh physical tree. There is no compiled-plan reuse API.
- `TUMBLE` state (`stream/tumble.rs`) lives outside DataFusion as process-local state keyed by `(window_start, group_key)`; accumulators are temporary per epoch. Watermark advances only after an epoch's data completes; windows emit at `window_end <= watermark`; state may not contain `buffer_id`/`buffer_slot`, `IMAGE`, or `VIDEO`. State is not checkpointed and does not survive the process.
- Ordering is strict: apply data → advance watermark → write and await sink acknowledgement → release the frame lease. `Session::cancel()` aborts; `request_graceful_stop()` drains — keep both wired when touching this loop.
- `connectors/kafka.rs` is the only unbounded sink. `CREATE TABLE ... USING KAFKA` does no network I/O; `credential_ref` resolves at first write through the host-installed `SecretProvider` (`secrets.rs`) and resolved material never reaches the Catalog. The JSON wire format is fixed by exact tests (IMAGE projected to safe fields, never `encoded`/`buffer_id`/`buffer_slot`; raw binary rejected at planning).

The unbounded allowlist and its rejection table live in `docs/kernel.md`; a rejection must name the offending node and suggest a rewrite rather than surfacing a raw DataFusion error.

### Catalog

`vql-catalog` owns the append-only revision log and SQLite backend: `revisions` rows are immutable, `objects` rows point at a `head_revision`, and `DROP` writes a tombstone revision. Schemas are stored as Arrow IPC. Every DDL commits one object in a transaction. Planning takes a `DefinitionSnapshot` once and pins it for the whole query, so concurrent DDL cannot change a running query's meaning. SQL defaults to `vql.default`; all relation endpoints are Tables distinguished by provider capabilities. Models and Functions share one transactionally enforced callable namespace while retaining distinct kinds. Catalog object names are case-insensitive and normalize to lowercase, including quoted names.

### Media and models

`MediaRuntime` prefers the `ffmpeg-native` decoder (default cargo feature, needs FFmpeg 8 dev libraries) and silently falls back to the `ffmpeg`/`ffprobe` subprocess decoder; `video_available()` gates `USING VIDEOS`. Video tables expand to frame rows inside the scan operator using the table's `OPTIONS (fps = ...)`, sampled by PTS.

A Model aggregate stores one immutable expanded interface, named versions, and an explicit default pointer. Each version records source, selected Runtime, flat `OPTIONS`, fingerprints, and an optional resolved contract. `CREATE MODEL` is declaration-only and performs no I/O; `RESOLVE MODEL [VERSION]` introspects, downloads, verifies, and pins one immutable version. `ALTER MODEL` adds/drops versions, publishes a resolved default, changes the comment, or renames through the callable-namespace constraint. Invocation-only values such as `classes` and `min_confidence` belong to the Model call. Sources resolve through `models/resolver.rs`; `mock://` remains an internal test backend.

### Resources and host capabilities

Each Session owns one memory budget; cloned handles share it, separately built Sessions do not. `resources.rs` uses named memory consumers so new buffers participate in the same enforced limit rather than allocate outside it. Exceeding the budget fails the requesting query with `RESOURCE_EXHAUSTED`; `TUMBLE` state has no spill.

Optional host capabilities are injected, never discovered: `EngineConfig::with_secret_provider` for credentials and `SessionBuilder::with_python_udf_host` for Python UDFs. Kernel code must degrade with a clear error when a capability is absent.

### Error contract

`ErrorCode` (`error.rs`) is a stable, machine-readable enum rendered as `[VQL-CCDDD] SYMBOL: message`; `as_str()` returns the identifier and `symbol()` returns the readable name. Row-level failures (bad image, failed inference) produce NULL result columns; `SET vql.on_error='fail'` flips them to hard errors. Unimplemented-but-parseable syntax must return `FEATURE_NOT_AVAILABLE` with a target version and must not register a catalog object. Unit tests assert on codes directly (`ddl_parser.rs`, `registry.rs`, `session.rs`), so changing a code is a contract change. The registry and extension rules live in `docs/error_codes.md`.

## Testing model

Tests are split by the narrowest boundary that can prove a contract.

Rust unit tests live beside their modules and cover everything a synthetic fixture can reach: parser and DDL validation, error codes, catalog lifecycle, plan shape, scheduler batching, streaming allowlist rejections. `session.rs` holds the broadest ones, using `mock://` models and generated PNGs so they need no downloads. Note that `mock://` never decodes its input, so it cannot exercise decode failures — use a Triton endpoint model for those, as decoding happens before any request.

Kernel SQL contracts use `sqllogictest-rs` through `vql-kernel/tests/slt.rs`. Cases are grouped by owner under `tests/slt/{ddl,connectors,functions,models}`, and every case gets a fresh Engine, catalog, and temporary `VQL_HOME`. Exact field names and nullability stay in `result_schema.rs`; branch behavior such as invalid TUMBLE widths and connector projection stays beside its implementation. There is no synthetic `scenarios` layer.

Each `vql-testing` target pairs a top-level entry point with same-named private SQL resources. `image`, `video`, `model`, `rtsp`, and `kafka` cover real media/model execution and external-service journeys; shared isolated-session helpers live under `tests/support/`. Task-shaped scenarios call `VQL_CLASSIFY` or `VQL_DETECT`, while the `model` target preserves the real Catalog Model resolve/direct-call contract. Every target requires the `system-tests` feature and runs serially where needed. Fixtures are gitignored and fetched by `scripts/fetch_datasets.py` and `scripts/export_yolo26.py`. `VQL_INTEGRATION_TEST=1` turns a missing requirement into a failure. The root `docker-compose.yaml` provides optional external services through profiles (`rtsp`, `kafka`), and `scripts/run-integration-tests.sh` starts isolated dependencies and runs the strict suite. See `docs/testing.md`.

`cargo test --workspace` executes the same deterministic tests on a fresh clone and a fixture-rich checkout. CI runs Rust coverage and a separate built-wheel Python API job; it does not run real external-service E2E.

## Docs

`docs/high_level_design.md` is the authoritative system boundary. Detailed v0.1 contracts live in `docs/{kernel,catalog,cli,python_binding,testing}.md`; `ROADMAP.md` remains the source of truth for version scope, and `docs/proposals/` holds later-feature designs. When a public contract changes, update the README, PRD, Roadmap, HLD, and owning component design together.
