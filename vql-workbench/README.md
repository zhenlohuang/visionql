# VisionQL Workbench

VisionQL Workbench is the independent browser client for one `vqld` endpoint. It provides local SQL drafts and history, bounded and attached-continuous execution, Arrow table and JSON views, and visual `IMAGE`/`BOX2D` inspection through public Flight SQL.

Runtime → Queries implements the [jobs management prototype](../docs/prototype/jobs_management/code.html) using `SHOW QUERIES`, `DESCRIBE QUERY`, and `STOP QUERY` through the same public SQL execution transport.

## Catalog

The Tables, Models, and Functions pages follow their [Catalog prototypes](../docs/prototype/catalog_tables/code.html). They provide searchable object directories, DDL and schema inspection, DDL copying and editor drafts, SQL creation dialogs, and confirmed deletion. Model details also expose version resolution, default-version selection, version removal, and editable `ALTER MODEL` SQL. Catalog operations share the Session's single execution slot with the editor.

Metadata comes from public `SHOW`, `DESCRIBE`, and `SHOW CREATE` statements. Model `RESOLVED` means its execution contract is resolved. Functions include registered Model callables and route their details and mutations to Model SQL. Python declarations can be inspected and managed, while Python Function execution in `vqld` remains unavailable.

## Persistent Queries

Submit a persistent continuous write with `SUBMIT QUERY <name> AS INSERT INTO ...` in the SQL editor, then open **Queries** to search by name, Query ID, state, or source health. The list refreshes every five seconds while visible, and provides a manual refresh action. Inspection, refresh, Stop, and editor execution share the browser Session's single execution slot.

**Show SQL** displays the server's redacted immutable definition, timestamps, restart gaps, window-state reset flag, and errors. Copying or loading that SQL into a new draft preserves the redaction; replace redacted literals before resubmitting. Loading creates a separate local draft and does not execute or modify the existing Query.

**Stop** sends `STOP QUERY '<query_id>'` for `STARTING` or `RUNNING` Queries and refreshes their state. Terminal and unknown states disable Stop. Closing the browser or its inspection requests does not stop persistent Queries; attached execution cancellation remains separate.

The list presents the public service's source health, last event time, restart-gap count, and errors. Prototype sequence numbers, watermark lag, committed-row counts, and checkpoints have no public Query fields and are not displayed as live data. Persistent Query state stays in `vqld`; Workbench keeps the management view only in page memory.

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
