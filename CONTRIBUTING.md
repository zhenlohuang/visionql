# Contributing to VisionQL

Thank you for helping improve VisionQL. Contributions are welcome across the engine, Python and CLI interfaces, documentation, examples, and tests.

## Before you start

- Small bug fixes, tests, and documentation corrections can go directly to a pull request.
- Open an issue before changing public SQL, catalog, model, media, or Python contracts.
- Discuss substantial features before implementation so the intended version boundary is clear. The [Roadmap](ROADMAP.md) is the source of truth for delivered and planned capabilities.
- Report vulnerabilities through the private process in [SECURITY.md](SECURITY.md), not a public issue.

## Development setup

Install the prerequisites listed in the [README](README.md#prerequisites), then clone and build the workspace:

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

### Test coverage

CI uses `cargo-llvm-cov` to publish a line, function, and region coverage summary in the workflow run and attaches browsable HTML plus LCOV reports as the `rust-coverage` artifact. Generate the same report locally from the repository root:

```bash
rustup component add llvm-tools-preview
cargo install cargo-llvm-cov --locked
cargo llvm-cov --workspace --exclude vql-python --locked --no-cfg-coverage --html
cargo llvm-cov report --ignore-filename-regex '/vql-python/' --summary-only
```

Open `target/llvm-cov/html/index.html` for file-level results. Coverage is informational rather than a merge threshold. Rust coverage includes the default unit, owner-contract, kernel SQL, and CLI process tests. Feature-gated system targets are excluded. Python source coverage is published separately because it cannot be meaningfully combined with Rust instrumentation.

### Integration tests

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

Then run the real-data/model SQL suite, optionally narrowing it while iterating:

```bash
VQL_INTEGRATION_TEST=1 \
  cargo test -p vql-testing \
  --features system-tests \
  --test slt \
  --locked
```

`vql-testing` has no library or unit-test target. Its dependencies are test-only, and every integration-test target requires `system-tests`, so the package contributes no target to the default Cargo test graph. Without strict mode, a manually selected system target reports missing prerequisites as ignored; `VQL_INTEGRATION_TEST=1` makes them fail.

A case contains its setup statements, behavior query, and expected rows in standard sqllogictest form:

```text
query B
SELECT COUNT(*) > 0 AS found FROM ...;
----
true
```

Real cases are grouped by their primary boundary under `vql-testing/tests/slt/{functions,models}`. The SLT adapter stays private in `tests/slt/harness.rs`; RTSP and Kafka SQL stay beside their runners under `tests/rtsp/` and `tests/kafka/`. Keep one behavior per file and return stable values that can be compared exactly. Every file receives an isolated temporary catalog, so do not add cleanup SQL unless cleanup is the behavior under test. Read the [Testing Design](docs/testing.md) before adding a case.

If a change affects media decoding, ONNX preprocessing, batching, or postprocessing, run this suite and state in the pull request that it passed.

### Real-model integration scenario

The mixed-size image batch scenario exercises the ONNX pipeline through public SQL:

```bash
VQL_TEST_CASE=models/mixed_size_images \
VQL_INTEGRATION_TEST=1 \
  cargo test -p vql-testing \
  --features system-tests \
  --test slt \
  --locked
```

The real RTSP scenario uses the `rtsp` profile in the root `docker-compose.yaml`. Run every strict
integration case in an isolated Compose project with:

```bash
scripts/run-integration-tests.sh
```

## Git hooks

Install [pre-commit](https://pre-commit.com/) and the repository hooks:

```bash
python -m pip install pre-commit
pre-commit install
```

The pre-commit hooks run repository hygiene, formatting, and Clippy checks. The pre-push hook runs the locked workspace test suite.

## Pull requests

Keep pull requests focused and make the user-visible intent easy to review:

- Add focused tests for behavior changes and regression fixes.
- Update the README, PRD, Roadmap, HLD, and owning component design together when a public contract or version boundary changes.
- Preserve stable error codes, literal paths, CLI flags, environment variables, and catalog defaults unless the change explicitly revises that contract.
- Keep generated native extensions, local runtime state, downloaded datasets, model artifacts, and build output out of Git.
- Run `git diff --check` and the relevant checks above.
- Describe any skipped or ignored validation in the pull request.

By contributing, you agree that your contribution is licensed under the repository's [Apache License 2.0](LICENSE).
