# VisionQL Workbench

VisionQL Workbench is the independent browser client for one `vqld` endpoint. Its frontend and loopback backend use public Flight SQL; the project is excluded from the root Rust workspace.

For connection settings, SQL drafts, visual results, Catalog, Jobs, and history, see the [Workbench user guide](../docs/user_guide/workbench.md). Installation and startup instructions live in [Installation and configuration](../docs/user_guide/installation.md#workbench). Repository-wide checks and Git hooks are documented in [CONTRIBUTING.md](../CONTRIBUTING.md).

## Development

Start [`vqld`](../docs/user_guide/installation.md#run-vqld), then build the frontend and start the loopback bridge. Run these commands from `vql-workbench/`:

```bash
cd frontend
pnpm install --frozen-lockfile
pnpm build

cd ../backend
cargo run --locked -- --static-dir ../frontend/dist
```

Open `http://127.0.0.1:6040` and follow the [user guide](../docs/user_guide/workbench.md#connect-to-a-daemon) to establish a connection.

## Checks

```bash
cd frontend
pnpm format:check
pnpm lint
pnpm test
pnpm build
pnpm exec playwright install chromium
pnpm test:e2e

cd ../backend
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
```

`pnpm test:e2e` builds and starts the shipped `vqld`, the Workbench backend, and
the production frontend in an isolated temporary `VQL_HOME`. The suite exercises
only the browser HTTP bridge and public Flight SQL protocol.

To also validate the same real detection query through Python/notebook and
Workbench, build the current Python extension with `maturin develop --locked`
in `vql-python/`, and fetch the COCO images and YOLO26n detection model with
the [fixture setup commands](../docs/design/testing.md#system-suite). From the
repository root, run:

```bash
VQL_WORKBENCH_E2E_PYTHON="$PWD/vql-python/.venv/bin/python" \
VQL_WORKBENCH_E2E_BROWSER=chrome \
VQL_SYSTEM_TEST=1 \
  cargo test -p vql-testing --features workbench-tests --test workbench --locked
```

Set `VQL_WORKBENCH_E2E_PYTHON` to the Python interpreter with the current
`visionql` extension and PyArrow installed. Omit the browser variable to use
Playwright's Chromium. The strict visual check fails on missing prerequisites.
It runs shared SQL from `vql-testing/tests/workbench/` against separate Python
and service Catalogs, compares returned detection values, and checks typed
IMAGE/BOX2D columns, thumbnail aspect ratio, JSON and row inspection, and actual
overlay geometry in both table cells and the inspector. Python can retain an
original-image reference; Flight returns a bounded JPEG thumbnail with locators
and process-local buffer references cleared. The normalized boxes retain the
same meaning in both representations. Browser output includes a screenshot and
the Python result reference under `frontend/test-results/`.

The `workbench-tests` feature enables `system-tests` and selects this additional
Python/browser journey without adding its dependencies to the ordinary system
suite. To include the full browser regression suite in the same run, use
`pnpm --dir vql-workbench/frontend test:e2e --visual-parity` with the same Python
and browser environment variables.
