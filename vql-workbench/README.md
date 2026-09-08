# VisionQL Workbench

VisionQL Workbench is the independent browser client for one `vqld` endpoint. It provides local SQL drafts and history, bounded and attached-continuous execution, Arrow table and JSON views, and visual `IMAGE`/`BOX2D` inspection through public Flight SQL.

Catalog management and persistent Queries management are intentionally deferred until their product prototypes are available.

## Development

Build the frontend first, then start the loopback bridge:

```bash
cd frontend
pnpm install
pnpm build

cd ../backend
cargo run -- --static-dir ../frontend/dist
```

Open `http://127.0.0.1:6040`. The default `vqld` endpoint is `http://127.0.0.1:6031`; it can be changed in Workbench Settings. Credentials and optional TLS CA material are submitted only when a browser Session is established and remain in backend memory.

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
