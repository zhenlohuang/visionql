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
cargo run -p vql-cli -- explain "SELECT 1"
```

Tests:

```bash
cargo test -p vql-kernel --test integration                          # integration suite over real data
VQL_TEST_CASE=functions/image_detection cargo test -p vql-kernel --test integration # filter cases
VQL_INTEGRATION_TEST=1 cargo test -p vql-kernel --test integration   # fail instead of skip when fixtures are missing
cargo test -p vql-kernel session::tests::model_calls_are          # single Rust unit test by path
VQL_TEST_CASE=scenarios/detect_objects_in_mixed_size_image_batch cargo test -p vql-kernel --test integration --locked
```

Python (`vql-python` is a PyO3/Maturin extension, not a pure-Python package):

```bash
source .venv/bin/activate && python -m pip install "maturin>=1.9,<2" pytest
cd vql-python && maturin develop --locked && cd ..
python -m pytest -q vql-python/tests
```

Toolchain is pinned to Rust 1.91.1 by `rust-toolchain.toml` (workspace MSRV is 1.88). `pre-commit install` wires fmt+clippy to pre-commit and the workspace test suite to pre-push. `VQL_LOG` (e.g. `vql_kernel=debug`) turns on tracing.

## Architecture

Three crates: `vql-kernel` (everything), `vql-cli` (clap + reedline shell), `vql-python` (PyO3 bindings + Python UDF host). The hosts only inject config and optional capabilities; `vql-kernel` never depends on clap or PyO3, never opens a port, and reads no global singletons.

`Engine` (`engine.rs`) owns four long-lived pieces shared by every session: the SQLite `CatalogStore`, a Tokio runtime, `MediaRuntime`, and `ModelRuntime`. `Session` is a cheap clone over the engine plus per-session state (`fail_on_error`, active-query cancellation token, optional Python UDF host).

### Statement path

`Session::sql` (`session.rs`) is the single entry point and splits on statement kind:

- **VQL DDL** (`CREATE TABLE/MODEL/SINK`, `DROP`, `SHOW`, `DESCRIBE`) is parsed by the hand-written parser in `sql/ddl_parser.rs` into `sql/ast.rs` types and validated before commit. `CREATE FUNCTION` is classified there but parsed and validated by DataFusion's `CreateFunction` and VisionQL `FunctionFactory`; the resulting normalized definition is then committed to the Catalog. `Statement::Ddl` carries both a human message and a RecordBatch.
- **Queries** go through `planner::plan_statement`: `planner/normalize.rs` performs textual rewrites (SQL-macro expansion, typed-inference named arguments, `box.center` → `BOX_CENTER`, correlated `UNNEST`), DataFusion plans the result, and `planner/inference.rs` rewrites typed inference markers into `InferenceNode` extension nodes.
- `INSERT INTO <sink> SELECT ...` reuses the query path and wraps the DataFrame with `planner/sink.rs` (`SinkWrite` / `SinkExec`).

### Two invariants worth preserving

**Model calls are plan nodes, not row UDFs.** A Model type owns a fixed marker such as `IMAGE_DETECTION`; its first argument is a literal Model name resolved from the query's `DefinitionSnapshot`. `planner/inference.rs` validates type and semantic constants, then lifts every call into an `InferenceNode` above the input. Identical calls are deduplicated by operation, Model fingerprint, invocation fingerprint, and input expression, but only for immutable Models. `InferenceExec` uses the `PreProcessor → RuntimeSession → PostProcessor` pipeline: VisionQL batches local ONNX Runtime work, while Triton owns dynamic batching for KServe V2 requests. Tests assert on `InferenceNode` / `InferenceExec` in the plan text — that is the contract.

**`IMAGE` carries references, not pixels.** `IMAGE` is an Arrow `Struct` (`types/image.rs`) with `ARROW:extension:name = visionql.image` metadata and ten nullable fields; the three payload shapes are *referenced* (`uri` + `locator`), *buffered* (`buffer_id`/`buffer_slot`, in-process only), and *encoded* (`encoded` + `encoding`, used at process boundaries such as Python and JSON). Scans never decode. Decoding is triggered only by an explicit consumer such as inference preprocessing or a Python UDF, and `MediaRuntime` counters exist so tests can assert that a metadata-only query decoded zero frames. Preserving that property is a load-bearing part of nearly every media change.

### Catalog

`catalog/store.rs` is an append-only revision log in SQLite: `revisions` rows are immutable, `objects` rows point at a `head_revision`, and `DROP` writes a tombstone revision. Schemas are stored as Arrow IPC. Every DDL commits one object in a transaction. Planning takes a `DefinitionSnapshot` once and pins it for the whole query, so concurrent DDL cannot change a running query's meaning. Relation names (tables) share one namespace; models, functions, and sinks each have their own. Unquoted identifiers lowercase.

### Media and models

`MediaRuntime` prefers the `ffmpeg-native` decoder (default cargo feature, needs FFmpeg 8 dev libraries) and silently falls back to the `ffmpeg`/`ffprobe` subprocess decoder; `video_available()` gates `USING VIDEOS`. Video tables expand to frame rows inside the scan operator using the table's `WITH (fps = ...)`, sampled by PTS.

A Model stores a typed `RuntimeSpec`, `PreProcessor` spec, and `PostProcessor` spec. `runtime.*` selects and binds execution; processor-specific values live only in complete `pre_processor.options` and `post_processor.options` objects. Unknown fields and unsupported combinations fail before Catalog commit. Invocation-only values such as `classes` and `min_confidence` belong to `IMAGE_DETECTION`, not the Model. Sources resolve through `models/resolver.rs`; the runtime opens a local/cached ONNX artifact or binds a Triton KServe V2 endpoint. `mock://` remains an internal test backend.

### Error contract

`ErrorCode` (`error.rs`) is a stable, machine-readable enum rendered as `[VQL:CODE] message`. Row-level failures (bad image, failed inference) produce NULL result columns and bump `QueryMetrics::error_rows`; `SET vql.on_error='fail'` flips them to hard errors. Unimplemented-but-parseable syntax must return `FEATURE_NOT_AVAILABLE` with a target version and must not register a catalog object. Unit tests assert on codes directly (`ddl_parser.rs`, `registry.rs`, `session.rs`), so changing a code is a contract change.

## Testing model

Two layers, split by what each can actually prove.

Rust unit tests live beside their modules and cover everything a synthetic fixture can reach: parser and DDL validation, error codes, catalog lifecycle, plan shape, scheduler batching. `session.rs` holds the broadest ones, using `mock://` models and generated PNGs so they need no downloads. Note that `mock://` never decodes its input, so it cannot exercise decode failures — use a Triton endpoint model for those, as decoding happens before any request.

`vql-kernel/tests/integration_tests.rs` is the only integration target. It uses a dynamic test harness, so every main SQL file appears as an individual Cargo test. Cases are grouped under `tests/{ddl,functions,scenarios}`. Each case has one main `.sql` statement and a `.expected.json` file containing exact `schema` (`"name TYPE"`, in order) and `rows`. When a real-model count can vary by execution provider, return the stable property the case needs to prove, such as `COUNT(*) > 0`.

Use `<case>.setup.sql` for prerequisite objects and `<case>.teardown.sql` for cleanup. Every case gets a fresh catalog and temporary `VQL_HOME`, and the runner executes setup, the main statement, and teardown in order. The runner substitutes `${IMAGES_LOCATION}`, `${VIDEOS_LOCATION}`, and `${MODEL}`. Do not pre-create shared objects or combine unrelated assertions in one query.

The fixtures are gitignored and fetched by `scripts/fetch_datasets.py` and `scripts/export_yolo26.py`; the suite skips when they are absent, and `VQL_INTEGRATION_TEST=1` turns that into a failure. See `vql-kernel/tests/README.md`.

`cargo test --workspace` therefore stays green on a fresh clone and in CI, which runs no downloads.

## Docs

`docs/design.md` is the authoritative HLD (in Chinese) and goes well beyond what v0.1 implements — treat unimplemented sections as design intent, and `ROADMAP.md` as the source of truth for what version a capability belongs to. `docs/proposals/` holds designs for later features. When a public contract (SQL syntax, error code, CLI flag, env var, catalog default, Python API) changes, update README, `docs/prd.md`, `docs/design.md`, and `ROADMAP.md` together.
