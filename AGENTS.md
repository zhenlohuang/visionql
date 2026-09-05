# Repository Guidelines

## Project Structure & Module Organization

VisionQL is a Rust workspace with five crates. `vql-catalog/` owns catalog objects and persistence; `vql-kernel/` contains SQL planning, media connectors, models, and execution; `vql-cli/` provides the `vql` shell and script runner; `vql-python/` contains the PyO3/Maturin package; and `vql-testing/` holds real-artifact and external-service tests. Keep examples in `examples/`, helper utilities in `scripts/`, fixture documentation in `data/`, and architecture or product decisions in `docs/`. `ROADMAP.md` defines release scope.

## Build, Test, and Development Commands

- `cargo build --workspace --locked` builds every crate with the pinned lockfile.
- `cargo run -p vql-cli -- shell` starts the development SQL shell.
- `cargo fmt --all -- --check` verifies formatting.
- `cargo clippy --workspace --all-targets --locked -- -D warnings` runs the required Rust lints.
- `cargo test --workspace --locked` runs the deterministic default Rust suite.
- `cargo test -p vql-kernel --test slt --locked` runs SQL logic tests.
- From `vql-python/`, run `maturin develop --locked`; from the root, run `python -m pytest -q vql-python/tests`.
- `scripts/run-integration-tests.sh` provisions Compose services and runs strict system tests.

Set `VQL_HOME="$PWD/data/.vql"` for reproducible local state. Install Git hooks with `pre-commit install`.

## Coding Style & Naming Conventions

Use rustfmt defaults and four-space indentation. Follow Rust naming conventions: `snake_case` for modules, functions, and tests; `PascalCase` for types and traits; and `SCREAMING_SNAKE_CASE` for constants. Keep Python code four-space indented and consistent with nearby typed APIs and pytest tests. Treat SQL syntax, error codes, configuration keys, paths, and environment variables as public contracts. Do not commit `target/`, virtual environments, downloaded datasets, models, local catalogs, or generated native extensions.

## Testing Guidelines

Place Rust unit tests beside their modules and crate-level contract tests under `<crate>/tests/`. Put deterministic SQL cases in focused `.slt` files under `vql-kernel/tests/slt/{ddl,connectors,functions,models}`; filter them with `VQL_TEST_CASE=models/object_detection`. Real media, ONNX, RTSP, and Kafka scenarios belong in `vql-testing/` and require `--features system-tests`. There is no numeric coverage threshold, but behavior changes and bug fixes should include focused regression tests. See `docs/design/testing.md` before adding system cases.

## Commit & Pull Request Guidelines

Use concise Conventional Commit subjects, matching history: `feat: add ...`, `fix: preserve ...`, or `docs: update ...`. Keep pull requests focused. Complete the template with the change, rationale, exact checks run or skipped, linked issues, and public-contract impact. Update the README and relevant design documents when behavior or version scope changes. Run `git diff --check` before submission. Report vulnerabilities through `SECURITY.md`, never a public issue.
