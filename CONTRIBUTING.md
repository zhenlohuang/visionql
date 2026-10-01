# Contributing to VisionQL

Thank you for helping improve VisionQL. Contributions are welcome across the engine, Python and CLI interfaces, documentation, examples, and tests. All participation follows the project [Code of Conduct](CODE_OF_CONDUCT.md).

## Before you start

- Small bug fixes, tests, and documentation corrections can go directly to a pull request.
- Open an issue before changing public SQL, catalog, model, media, or Python contracts.
- Discuss substantial features before implementation so the intended version boundary is clear. The [Roadmap](ROADMAP.md) is the source of truth for delivered and planned capabilities.
- Report vulnerabilities through the private process in [SECURITY.md](SECURITY.md), not a public issue.

## Development setup

Follow [Installation and configuration](docs/user_guide/installation.md) for prerequisites, source builds, Docker, CLI, Python, `vqld`, Workbench, and runtime settings. For repository development, select an isolated instance and build from the root:

```bash
git clone https://github.com/zhenlohuang/visionql.git
cd visionql

export VQL_HOME="$PWD/data/.vql"
cargo build --workspace --locked
```

For Python development:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install "maturin>=1.9,<2"

cd vql-python
maturin develop --locked
cd ..
```

## Run the required checks

Run the same Rust gates used by CI:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

CI runs formatting, Clippy, and `docker compose config --quiet` in one Linux job. A second Linux job runs the deterministic Rust suite once under `cargo-llvm-cov`. A third job builds the Python wheel, runs the API tests, and publishes Python source coverage. CI does not provision real data, models, or external services. Run the Python suite locally after `maturin develop` whenever a change touches that interface:

```bash
python -m pip install pytest
python -m pytest -q vql-python/tests
```

The default suite does not require downloaded datasets or a real model. Unit tests create isolated temporary directories and use `mock://` models.

### Workbench checks

Workbench is a separate Rust workspace and frontend, excluded from the root workspace. Run its gates from the repository root when changing the browser client or bridge:

```bash
cargo fmt --manifest-path vql-workbench/Cargo.toml --all -- --check
cargo clippy --manifest-path vql-workbench/Cargo.toml --workspace --all-targets --locked -- -D warnings
cargo test --manifest-path vql-workbench/Cargo.toml --workspace --locked
pnpm --dir vql-workbench/frontend format:check
pnpm --dir vql-workbench/frontend lint
pnpm --dir vql-workbench/frontend test
pnpm --dir vql-workbench/frontend build
pnpm --dir vql-workbench/frontend exec playwright install chromium
pnpm --dir vql-workbench/frontend test:e2e
```

The E2E script builds and starts the shipped `vqld`, Workbench backend, and production frontend in an isolated temporary `VQL_HOME`. To use installed Chrome, run `VQL_WORKBENCH_E2E_BROWSER=chrome pnpm --dir vql-workbench/frontend test:e2e`. See the [Workbench checks](vql-workbench/README.md#checks) for the additional real-model Python/browser acceptance scenario.

### SQL reference generation

The [built-in function reference](docs/user_guide/sql-functions.md) is generated from DataFusion `#[user_doc(...)]` attributes beside the owning Rust implementation. Implement `ScalarUDFImpl::documentation()` by returning `self.doc()`; AI markers select the documentation for their public function. Maintain syntax, argument defaults, result types, availability, and examples in these attributes, then regenerate the page:

```bash
cargo run -p vql-kernel --example generate_sql_reference --locked
cargo run -p vql-kernel --example generate_sql_reference --locked -- --check
```

Session registration and documentation share `functions::builtin_udfs()`, so every registered VQL built-in must publish documentation. The exporter reads compiled `Documentation` values through `vql_kernel::documentation::builtin_functions()` and uses public AI names and syntax rather than internal planning markers or normalized signatures. It does not open a Catalog, load media, or resolve model artifacts. CI and pre-commit reject stale generated output or missing metadata.

The DataFusion metadata supplies descriptions, syntax, arguments, SQL examples, and related functions; it can also be consumed by other renderers. Keep examples complete or link their setup explicitly, and mark unavailable overloads as unavailable. Do not edit the generated function page directly. Maintain types, DDL, configuration statements, streaming behavior, and Jobs in the hand-written [SQL reference](docs/user_guide/sql-reference.md); function attributes are not a grammar or a semantic inference mechanism.

### Test coverage

CI uses `cargo-llvm-cov` to publish a line, function, and region coverage summary in the workflow run and attaches browsable HTML plus LCOV reports as the `rust-coverage` artifact. Generate the same report locally from the repository root:

```bash
rustup component add llvm-tools-preview
cargo install cargo-llvm-cov --locked
cargo llvm-cov --workspace --exclude vql-python --locked --no-cfg-coverage --html
cargo llvm-cov report --ignore-filename-regex '/vql-python/' --summary-only
```

Open `target/llvm-cov/html/index.html` for file-level results. Coverage is informational rather than a merge threshold. Rust coverage includes the default unit, owner-contract, kernel SQL, and CLI process tests. Feature-gated system targets are excluded. Python source coverage is published separately because it cannot be meaningfully combined with Rust instrumentation.

### Crate integration and system tests

Kernel-owned SQL behavior runs with the rest of the kernel tests:

```bash
cargo test -p vql-kernel --test slt --locked
VQL_TEST_CASE=models/object_detection cargo test -p vql-kernel --test slt --locked
```

Sqllogictest cases are grouped under `vql-kernel/tests/slt/{ddl,connectors,functions,models}`. Every case receives a fresh Engine and catalog while sharing read-only generated fixtures. Put pure branch behavior beside its implementation and exact Arrow field metadata in focused Rust owner tests.

Fetch the real system fixtures once:

```bash
python scripts/fetch_datasets.py          # about 10 MB
python scripts/export_yolo26.py --task detect --size n
python scripts/export_yolo26.py --task classify --size n
```

Then run the real-data/model targets that do not require external services:

```bash
VQL_SYSTEM_TEST=1 \
  cargo test -p vql-testing \
  --features system-tests \
  --test image \
  --test video \
  --test model \
  --locked
```

`vql-testing` has no library or unit-test target. Its dependencies are test-only, and every system-test target requires `system-tests`, so the package contributes no target to the default Cargo test graph. Without strict mode, a manually selected system target reports missing prerequisites as ignored; `VQL_SYSTEM_TEST=1` makes them fail. Deterministic integration and owner-contract tests remain under the crate that owns the behavior.

Real cases are grouped by their primary boundary under `vql-testing/tests/{image,video,model,rtsp,kafka,vqld}`. SQL-driven targets pair a top-level Rust runner with same-named resources and assert stable semantic outcomes instead of model-specific score snapshots. Task-shaped scenarios use `VQL_CLASSIFY` or `VQL_DETECT`; the `model` target preserves the public Catalog Model resolve/direct-call journey. The `vqld` target treats the locally built container as a black box and uses only the packaged CLI, public Flight SQL, and Compose lifecycle operations. Read the [Testing Design](docs/design/testing.md) before adding a case.

If a change affects media decoding, ONNX preprocessing, batching, or postprocessing, run this suite and state in the pull request that it passed.

### Real-model system scenario

The image target exercises classification, detection, unknown-class filtering, and mixed-size ONNX batching through public SQL:

```bash
VQL_SYSTEM_TEST=1 \
  cargo test -p vql-testing \
  --features system-tests \
  --test image \
  --locked
```

The real RTSP, Kafka, and `vqld` scenarios use services in the root `docker-compose.yaml`. Run every
strict system case in an isolated Compose project with:

```bash
scripts/run-system-tests.sh
```

## Git hooks

Install [pre-commit](https://pre-commit.com/) and the repository hooks:

```bash
python -m pip install pre-commit
pre-commit install
```

The pre-commit hooks run repository hygiene, generated SQL-reference checks, formatting, and Clippy checks. The pre-push hook runs the locked workspace test suite. Run all local hooks explicitly with:

```bash
pre-commit run --all-files
pre-commit run --hook-stage pre-push --all-files
```

## Pull requests

Keep pull requests focused and make the user-visible intent easy to review:

- Add focused tests for behavior changes and regression fixes.
- Update the README, PRD, Roadmap, HLD, and owning component design together when a public contract or version boundary changes.
- Preserve stable error codes, literal paths, CLI flags, environment variables, and catalog defaults unless the change explicitly revises that contract.
- Keep generated native extensions, local runtime state, downloaded datasets, model artifacts, and build output out of Git.
- Run `git diff --check` and the relevant checks above.
- Describe any skipped or ignored validation in the pull request.

By contributing, you agree that your contribution is licensed under the repository's [Apache License 2.0](LICENSE).
