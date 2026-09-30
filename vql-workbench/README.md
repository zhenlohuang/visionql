# VisionQL Workbench

VisionQL Workbench is the independent browser client for one `vqld` endpoint. It provides local SQL drafts and history, bounded and attached-continuous execution, Arrow table and JSON views, and visual `IMAGE`/`BOX2D` inspection through public Flight SQL.

New query files are named `Untitle.sql`, then `Untitle1.sql`, `Untitle2.sql`, and so on when a name is already in use. Double-click a file tab or click its pencil button to rename it. Names must be non-empty and unique among open drafts; `.sql` is added automatically when omitted. Draft names and SQL are saved in browser storage and survive reloads.

Workspace → Jobs implements the [jobs management prototype](../docs/prototype/jobs_management/code.html) using `SHOW JOBS`, `DESCRIBE QUERY`, and `STOP QUERY` through the same public SQL execution transport. Workspace groups SQL editor, History, and Jobs.

## Catalog

The Catalog sidebar groups objects in an expandable Catalog → Schema → Tables / Models / Functions tree across namespaces. Multi-version Models show a version list to the right of the DDL workspace, available through both their Models and Function callable entries. The list highlights the displayed version and marks the default; on narrow screens it sits above the DDL. Clicking a version executes `SHOW CREATE MODEL <name> VERSION '<version>'`; clicking the Model executes bare `SHOW CREATE MODEL <name>` for its latest live version. Both display the server's complete `CREATE MODEL` statement directly. The main workspace formats `SHOW CREATE` DDL using the SQL editor's read-only CodeMirror view, including line numbers, shared SQL/VisionQL highlighting, selection, search, and scrolling. Its header shows the actual Model version and provides only a copy action for the original server-returned SQL; selecting an object or version again reloads its definition. Selecting a category provides a namespace-specific creation draft in the SQL editor; creation, alteration, resolution, and deletion run through that editor. Successful editor update statements refresh the tree. Catalog reads share the Session's single execution slot with the editor.

Metadata comes from public `SHOW`, `DESCRIBE`, and `SHOW CREATE` statements. Model `RESOLVED` means its execution contract is resolved. Functions include registered Model callables and route their details and mutations to Model SQL. Python declarations can be inspected and managed, while Python Function execution in `vqld` remains unavailable.

## Jobs

Submit a persistent continuous write with `SUBMIT QUERY <name> AS INSERT INTO ...` in the SQL editor, then open **Jobs** in **Workspace** to search by name, ID, state, or source health. Jobs represent the service's persistent Queries. The list refreshes every five seconds while visible, and provides a manual refresh action. Inspection, refresh, Stop, and editor execution share the browser Session's single execution slot.

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
