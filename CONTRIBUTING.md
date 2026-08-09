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

For Python interface changes, run the API suite from the repository root after `maturin develop`:

```bash
python -m pip install pytest
python -m pytest -q vql-python/tests
```

The default suite does not require downloaded datasets or a real model. Test fixtures create isolated temporary `${TEST_DATA}` and `${VQL_HOME}` directories.

### SQL behavior cases

Run every executable SQL case with:

```bash
cargo test -p vql-kernel --test sql_cases
```

Narrow the suite while iterating:

```bash
VQL_SQL_CASE=models/function cargo test -p vql-kernel --test sql_cases
```

Golden JSON updates must be explicit:

```bash
VQL_UPDATE_GOLDEN=1 VQL_SQL_CASE=models/function \
  cargo test -p vql-kernel --test sql_cases
```

Review every changed `.expected.json` file before submitting it. Do not update a golden merely to make an unexplained behavior change pass.

### Real-model test

The real YOLO26 test is optional and ignored by the default suite:

```bash
VQL_YOLO26_ONNX=./data/models/yolo26n.onnx \
  cargo test real_yolo26_onnx_e2e -- --ignored
```

If a change affects ONNX preprocessing, batching, or postprocessing, state whether this gate ran and which model artifact was used.

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
