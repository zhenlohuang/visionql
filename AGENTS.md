# Repository Guidelines

## Project Structure & Module Organization

VisionQL is a Rust workspace with three crates. `vql-kernel/` owns SQL planning, the catalog, media decoding, model execution, and most tests. `vql-cli/` provides the `vql` shell and script runner. `vql-python/` contains the PyO3/Maturin extension, Python package, and API tests. Keep executable examples under `examples/`, helper scripts under `scripts/`, local fixture documentation under `data/`, and product or architecture decisions under `docs/`. Treat `ROADMAP.md` as the source of truth for version scope.

## Build, Test, and Development Commands

- `cargo build --workspace --locked` builds all crates using `Cargo.lock`.
- `cargo run -p vql-cli -- shell` starts the development SQL shell.
- `cargo run -p vql-cli -- run examples/sql/video_people_count.sql` runs a SQL script.
- `cargo fmt --all -- --check` verifies Rust formatting.
- `cargo clippy --workspace --all-targets --locked -- -D warnings` enforces lint-free code.
- `cargo test --workspace --locked` runs the default Rust suite.
- From `vql-python/`, `maturin develop --locked`; then run `python -m pytest -q vql-python/tests` from the repository root.

Set `VQL_HOME="$PWD/data/.vql"` for reproducible local state. Install hooks with `pre-commit install`; pre-commit checks formatting and Clippy, while pre-push runs workspace tests.

## Coding Style & Naming Conventions

Use rustfmt defaults (four-space indentation). Follow Rust conventions: `snake_case` for modules, functions, and test names; `PascalCase` for types and traits; `SCREAMING_SNAKE_CASE` for constants. Keep public errors, CLI flags, environment variables, paths, and SQL syntax stable unless the change intentionally revises the contract. Do not commit `target/`, `.venv/`, downloaded datasets, model artifacts, local catalogs, or generated native extensions.

## Testing Guidelines

Place Rust unit tests beside their modules and integration tests under each crate's `tests/`. SQL behavior cases use paired `vql-kernel/tests/sql/**/*.sql` and `.expected.json` files; run them with `cargo test -p vql-kernel --test sql_cases`. Use `VQL_SQL_CASE=models/function` to filter. Golden updates require `VQL_UPDATE_GOLDEN=1` and manual review. There is no numeric coverage threshold; every behavior change or regression fix should add a focused test.

## Commit & Pull Request Guidelines

Use concise Conventional Commit subjects, as in `feat: add ...`, `fix: preserve ...`, or `docs: update ...`. Keep pull requests focused, explain user-visible intent, link relevant issues, and list checks run or skipped. Update README, PRD, design, and Roadmap together when public contracts or version boundaries change. Run `git diff --check` before submission. Report security issues through `SECURITY.md`, never a public issue.
