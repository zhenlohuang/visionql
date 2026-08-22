# Repository Guidelines

## Project Structure & Module Organization

VisionQL is a Rust workspace with five crates. `vql-catalog/` owns catalog domains, snapshots, backend ports, SQLite persistence, and the Unity Catalog-compatible API. Keep catalog relations in the Table namespace and distinguish read/write behavior through provider capabilities. `vql-kernel/` owns SQL planning, media decoding, model execution, and owner tests. `vql-cli/` provides the `vql` shell and script runner. `vql-python/` contains the PyO3/Maturin extension, Python package, and API tests. `vql-testing/` owns shared SQL conformance and external-service system tests. Keep executable examples under `examples/`, helper scripts under `scripts/`, local fixture documentation under `data/`, and product or architecture decisions under `docs/`. Treat `ROADMAP.md` as the source of truth for version scope.

## Build, Test, and Development Commands

- `cargo build --workspace --locked` builds all crates using `Cargo.lock`.
- `cargo run -p vql-cli -- shell` starts the development SQL shell.
- `cargo run -p vql-cli -- run examples/sql/video_people_count.sql` runs a SQL script.
- `cargo fmt --all -- --check` verifies Rust formatting.
- `cargo clippy --workspace --all-targets --locked -- -D warnings` enforces lint-free code.
- `cargo test --workspace --locked` runs the default Rust suite.
- `cargo test -p vql-testing --test sql --locked` runs shared sqllogictest cases.
- `scripts/run-integration-tests.sh` provisions profiled Compose dependencies and runs strict integration tests.
- From `vql-python/`, `maturin develop --locked`; then run `python -m pytest -q vql-python/tests` from the repository root.

Set `VQL_HOME="$PWD/data/.vql"` for reproducible local state. Default runtime settings come from the optional, strict schema-v1 file at `$VQL_HOME/config.toml`; relative paths resolve from `VQL_HOME`. Install hooks with `pre-commit install`; pre-commit checks formatting and Clippy, while pre-push runs workspace tests.

## Coding Style & Naming Conventions

Use rustfmt defaults (four-space indentation). Follow Rust conventions: `snake_case` for modules, functions, and test names; `PascalCase` for types and traits; `SCREAMING_SNAKE_CASE` for constants. Keep public errors, configuration fields, environment variables, paths, and SQL syntax stable unless the change intentionally revises the contract. The CLI surface is only `shell` and `run`: keep `EXPLAIN` as SQL, keep engine settings out of CLI flags, and keep standalone `\q` behavior aligned between the Reedline and stdin shell loops. Query metrics remain kernel/Python APIs, not a CLI output mode. Do not commit `target/`, `.venv/`, downloaded datasets, model artifacts, local catalogs, or generated native extensions.

## Testing Guidelines

Place Rust unit tests beside their modules and crate-owned integration tests under each crate's `tests/`. Anything provable with a synthetic fixture or a `mock://` model belongs in the owner crate; CLI and configuration contract changes need focused owner tests. Shared SQL behavior lives as one-purpose `.slt` files under `vql-testing/tests/cases/{ddl,functions,scenarios}`; every file is an individual Cargo test with an isolated Engine and catalog. Run them with `cargo test -p vql-testing --test sql --locked` and filter with `VQL_TEST_CASE=functions/image_detection`. Missing real fixtures are ignored by default, while `VQL_INTEGRATION_TEST=1` makes missing requirements fail. External services come from the root `docker-compose.yaml` profiles; use `scripts/run-integration-tests.sh` for the strict suite. See `vql-testing/README.md` before adding a case. There is no numeric coverage threshold; every behavior change or regression fix should add a focused test.

## Commit & Pull Request Guidelines

Use concise Conventional Commit subjects, as in `feat: add ...`, `fix: preserve ...`, or `docs: update ...`. Keep pull requests focused, explain user-visible intent, link relevant issues, and list checks run or skipped. Update README, PRD, design, and Roadmap together when public contracts or version boundaries change. Run `git diff --check` before submission. Report security issues through `SECURITY.md`, never a public issue.
