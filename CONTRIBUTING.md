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

CI runs only these three gates on Linux. The Python API suite is not part of CI, so run it locally from the repository root after `maturin develop` whenever a change touches the Python interface:

```bash
python -m pip install pytest
python -m pytest -q vql-python/tests
```

The default suite does not require downloaded datasets or a real model. Unit tests create isolated temporary directories and use `mock://` models.

### Integration tests

`vql-kernel/tests/integration_tests.rs` runs real SQL over the example datasets and the exported YOLO26 model. Fetch the fixtures once:

```bash
python scripts/fetch_datasets.py          # about 10 MB
python scripts/export_yolo26.py --size n  # data/models/yolo26n.onnx
```

Then run the suite, optionally narrowing it while iterating:

```bash
cargo test -p vql-kernel --test integration
VQL_TEST_CASE=functions/detect_objects cargo test -p vql-kernel --test integration
```

Without the fixtures every case reports a skip and passes, which keeps `cargo test --workspace` green on a fresh clone. Set `VQL_INTEGRATION_TEST=1` to turn a missing fixture into a failure instead.

Every main SQL file appears as an individual test in Cargo output; use `cargo test -p vql-kernel --test integration -- --list` to list them.

A case has one main SQL statement, an expected result, and optional setup and teardown sidecars:

```text
detect_objects.setup.sql
detect_objects.sql
detect_objects.expected.json
detect_objects.teardown.sql
```

Expected JSON contains the exact schema and rows:

```json
{
  "schema": ["found BOOLEAN"],
  "rows": [[true]]
}
```

Cases are grouped under `ddl/`, `functions/`, and `scenarios/`. Keep one purpose in each main statement, put prerequisite DDL in `*.setup.sql`, and put cleanup in `*.teardown.sql`. Return stable values that can be compared exactly. Read `vql-kernel/tests/README.md` before adding a case.

If a change affects media decoding, ONNX preprocessing, batching, or postprocessing, run this suite and state in the pull request that it passed.

### Real-model unit test

One ignored unit test exercises the ONNX pipeline directly:

```bash
VQL_YOLO26_ONNX=./data/models/yolo26n.onnx \
  cargo test real_yolo26_onnx_e2e -- --ignored
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
- Update the README, PRD, system design, and Roadmap together when a public contract or version boundary changes.
- Preserve stable error codes, literal paths, CLI flags, environment variables, and catalog defaults unless the change explicitly revises that contract.
- Keep generated native extensions, local runtime state, downloaded datasets, model artifacts, and build output out of Git.
- Run `git diff --check` and the relevant checks above.
- Describe any skipped or ignored validation in the pull request.

By contributing, you agree that your contribution is licensed under the repository's [Apache License 2.0](LICENSE).
