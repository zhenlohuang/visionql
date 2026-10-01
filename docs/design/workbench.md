# VisionQL Workbench Design

Workbench is the visual SQL client for VisionQL. It combines SQL authoring, Catalog and persistent Job management, and multimodal result inspection without creating a second engine or control-plane API. Release scope and sequencing are maintained in the [Roadmap](../../ROADMAP.md).

## Purpose

Generic SQL clients can execute VisionQL SQL but do not naturally render `IMAGE`, `BOX2D`, or arrays of detections. Workbench centers on one connected workspace:

> A data or ML engineer discovers and manages SQL objects, writes batch or streaming queries, inspects image thumbnails with detection boxes, and manages persistent Jobs in a browser.

## Architecture Constraints

Workbench is an independent client. It uses public Arrow Flight SQL, SQL, and structured errors. It never reads SQLite files, imports engine crates, or requires a Workbench-only `vqld` RPC.

The reference implementation includes a small Workbench backend because the selected browser stack does not implement Flight SQL directly. The backend is a transport bridge: it owns only transport conversion, a short-lived browser Session, cancellation propagation, and bounded in-memory thumbnails. It stores no Catalog objects, Job definitions, permissions, or business data.

The backend must not:

- rewrite user SQL to add sampling, filters, or `LIMIT`;
- parse SQL to infer boundedness or side effects;
- treat Prometheus as a product-data API;
- cache credentials or original media in browser storage;
- keep an attached query alive after its browser result stream disappears.

## Workspace Layout

The Workbench screen provides:

1. a connection strip showing the configured `vqld` endpoint and current Session state;
2. a sidebar with Workspace navigation and a namespace-aware Catalog tree;
3. a main workspace with local SQL draft tabs, read-only object DDL, or the Jobs page;
4. an editor result region with typed table and JSON views, execution status, errors, and row inspection.

Draft tabs share one execution slot. The editor supports SQL highlighting and formatting, full-buffer or selected/current-statement execution, public `EXPLAIN`, and exact active-execution cancellation. Bounded results accumulate to completion; explicit attached-stream execution retains the latest 500 rows. History keeps the latest 500 local execution records with load, rerun, copy, edit, and clear actions. It stores SQL and execution metadata, while results, media, credentials, and persistent Job state remain outside browser history.

The result table exposes a transient Overlay Config that maps one `IMAGE` column to one `BOX2D` column and optional label and confidence columns. This configuration changes presentation only, remains in page memory, and is cleared with the Session.

SQL query files are local browser drafts. A new file uses the first available name in `Untitle.sql`, `Untitle1.sql`, `Untitle2.sql`, and so on. A tab's rename button or a double-click opens the file-name dialog. Renaming requires a non-empty name unique among open drafts and adds `.sql` when omitted; it preserves the draft identity, SQL, active selection, results, and execution history. Draft names and SQL persist in browser storage. Existing saved names are retained on load.

Workspace groups SQL editor, History, and Jobs. Workspace → Jobs provides a searchable list with status, Show SQL, and Stop actions. It uses the existing browser Session and public SQL execution transport.

## Jobs Management

The SQL editor submits persistent continuous Table writes with `SUBMIT JOB`. The Jobs page represents the service's persistent Jobs, reads `SHOW JOBS`, and selects returned Arrow fields by name. It filters names, Job IDs, state strings, and source health locally, preserves unknown state/health strings, and shows the actual `started_at`, `last_event_time`, `restart_gap_count`, `error_code`, and `error_message` values. Missing inspection values appear as unavailable. Prototype sequence numbers, watermark lag, committed-row counts, and checkpoint claims are not supported by the public Job schema.

The visible page refreshes every five seconds and offers manual refresh. It pauses automatic refresh while Settings, History, or a Job definition is open. Every management statement consumes its bounded Arrow result and checks terminal execution status before releasing the Session's shared execution slot. Navigation, reconnect, and editor execution are disabled during a management operation; page cleanup cancels only that transient inspection execution by its exact `execution_id`.

Show SQL sends `DESCRIBE JOB '<job_id>'` and opens a drawer with the server's immutable `sql_redacted` definition, lifecycle timestamps, restart-gap boundaries, window-state reset flag, and structured error fields. Copy SQL and Load SQL as new draft preserve the redaction, and the drawer tells the user to replace redacted literals before resubmission. Loading never executes SQL or edits the registered definition.

Stop sends `STOP JOB '<job_id>'` only for the currently supported non-terminal `STARTING` and `RUNNING` states, then reloads `SHOW JOBS`. Terminal and unknown states disable the action. Job IDs are SQL string literals with escaped quotes; display names are never mutation identities. A failed operation preserves its structured problem and the last successfully loaded list. Session expiry clears the management data and prompts reconnection; reconnecting establishes a fresh view.

Management reads are not local SQL-editor history entries. Persistent Job state is kept only in page memory and owned durably by `vqld` and the Catalog. Closing the browser or cancelling an inspection request does not issue `STOP JOB`. No private lifecycle API, SQLite read, `PAUSE`, `RESUME`, or manual terminal-history deletion is introduced.

## Reference Implementation

`vql-workbench/` is a separate project with two implementation parts:

```text
vql-workbench/
  frontend/  # browser SPA
  backend/   # loopback Workbench backend and Flight SQL client
```

The frontend and backend use one same-origin HTTP endpoint. The backend serves the production static assets, owns one short-lived in-memory browser Session, and connects to the configured `vqld` endpoint as an ordinary Flight SQL client. It imports neither `vql-server` nor engine crates. Its Rust Arrow and Flight dependencies stay on the same pinned Arrow release as `vqld` so schema metadata and IPC behavior are tested against one implementation baseline.

The backend binds to loopback. It accepts the `vqld` endpoint, TLS inputs, and service credential when establishing a Session, keeps credential material only in backend memory, and gives the browser an opaque `HttpOnly`, `SameSite=Strict` Session cookie. Closing or expiring the Session drops prepared statements, credentials, buffered thumbnails, and active execution state.

The internal browser transport is intentionally small:

- establish or close the one browser Session;
- prepare and execute one SQL statement using the public Flight SQL profile;
- stream bounded results as `application/vnd.apache.arrow.stream` without converting rows to JSON;
- expose the public server execution ID and propagate cancellation for exactly that execution;
- translate gRPC status and the versioned VQL error payload into JSON containing the original `code`, `symbol`, `message`, and optional `target_version` fields.

These HTTP routes are private implementation details of Workbench, not a second VisionQL API. Preparing and classifying SQL remains a `vqld` operation. Default execution rejects an `unbounded` result from `vql.statement_info.result_mode` before opening a stream; the editor's explicit attached-stream action opts into that result mode. A client-side policy rejection uses a Workbench-local problem response that is visually distinct from a forwarded VQL error and never invents a VQL identifier.

## Technology Selection

The frontend baseline is React + TypeScript + Vite + Tailwind CSS, with shadcn/ui as the component system.

| Layer | Selection | Contract in Workbench |
|---|---|---|
| Language and UI | React and strict TypeScript | Client-rendered single-page application; typed components for Session, editor, execution, result, and error states |
| Build and packages | Vite and pnpm | Development server and production asset build; exact JavaScript dependencies are committed in `pnpm-lock.yaml` |
| Component system | shadcn/ui with Radix UI primitives and Lucide React | Generated component source is committed under `frontend/src/components/ui`; Radix supplies accessible interaction behavior and Lucide supplies the shared icon set |
| SQL editor | CodeMirror 6 with `@codemirror/lang-sql` | Line numbers, SQL highlighting, bracket matching, and Run shortcut; VisionQL keywords and types extend highlighting only and never validate or rewrite SQL |
| Result model | Apache Arrow JavaScript | Incrementally reads Arrow IPC record batches from `fetch()` and preserves Arrow schema, nullability, nested values, and extension metadata |
| Result table | TanStack Table with semantic HTML | Headless column and row state with custom cells for `IMAGE`, `BOX2D`, nested values, timestamps, and numeric values; pagination is local and never rewrites the submitted SQL |
| Image overlay | native `<img>` plus SVG | JPEG bytes become revocable Blob URLs; an absolutely aligned SVG renders boxes and labels without rasterizing the thumbnail again |
| Styling | Tailwind CSS through `@tailwindcss/vite` | Utility classes implement the workspace shell, spacing, typography, status colors, and resizable editor/result split; CSS custom properties hold VisionQL-specific design tokens |
| Backend | Rust, Tokio, Axum, `arrow-flight`, and Tonic | Serves the frontend and performs same-origin HTTP streaming, Flight SQL authentication and preparation, Flight-to-IPC conversion, cancellation, and structured-error preservation |
| Client tests | Vitest and Testing Library | Covers reducers, schema-to-column mapping, scalar formatting, Blob URL cleanup, overlay geometry, and accessible interaction states |
| End-to-end tests | Playwright | Runs the built Workbench and shipped `vqld`, then verifies the complete Workbench acceptance path through the browser |

shadcn/ui owns reusable controls and accessibility wiring, not page composition or application state. Workbench keeps the editor, Arrow result model, media cells, and overlay renderer as dedicated components, and uses Tailwind design tokens to give generated controls the VisionQL visual language.

Workbench has one explicit execution state machine: `disconnected`, `idle`, `preparing`, `running`, `cancelling`, `completed`, or `failed`. React owns this state through a typed reducer. The reference frontend uses no client router, global state framework, service worker, offline cache, or browser database. Starting a second execution is disabled until the first reaches a terminal state.

The browser consumes the response body as a stream and appends complete record batches to the bounded in-memory result. Browser abort, navigation, Session expiry, and the Cancel action all trigger backend-side Flight cancellation before local state is discarded. Blob URLs are revoked when their batch, result, or Session is released.

## Catalog Management

The Catalog sidebar presents an expandable Catalog → Schema → Tables / Models / Functions → Object tree. It lists all namespaces represented by the public `SHOW TABLES`, `SHOW MODELS`, and `SHOW FUNCTIONS` results, and retains `vql.default` as the default namespace even when empty. Callable namespaces come from the returned `catalog` and `schema` fields. The current Table SQL surface is limited to `vql.default`, so the tree exposes the Tables category only there. The tree does not enumerate empty remote namespaces through a private API.

Selecting a tree object displays its defining SQL directly in the main workspace, with no object directory or detail modal. Selecting a category displays a selection prompt and a namespace-specific creation draft action. The DDL view formats `SHOW CREATE` output with VisionQL clause breaks and indentation, and reuses the SQL editor's CodeMirror component in read-only mode, with the same line numbers, gutter, typography, and keyword, type, string, number, and comment highlighting. Selection, copying, search, bracket matching, and scrolling remain available; editing and Run shortcuts are disabled. Quoted identifiers and string contents are preserved, and unsupported formatting falls back to the original text. The DDL header provides only a copy action, which uses the original server-returned SQL. Selecting an object or version again reloads its definition. Navigation preserves existing editor drafts and results.

The Catalog tree stops at object leaves. Multi-version Models show their version list in a narrow inspector to the right of the DDL, available through both the Models and Functions entries. The inspector highlights the actual displayed version and marks the default from `is_default`; on narrow screens it sits above the DDL as a horizontal list. Selecting a version executes `SHOW CREATE MODEL <name> VERSION '<version>'`; selecting the Model itself executes bare `SHOW CREATE MODEL <name>` for the latest live version, which can differ from the default. The Model's tree entry remains selected when its displayed version changes. The frontend displays the complete `CREATE MODEL` statement returned by the server without splitting or synthesizing version definitions. The header shows the actual `version` result field, and the copy action uses the original returned statement. A removed version produces the server's structured `NOT_FOUND`; refreshing the tree reloads version metadata. Version-list failures retain the objects and show their structured problem in the inspector.

The frontend uses the existing execution transport for every Catalog read. It consumes bounded Arrow results and checks terminal execution status before releasing the Session's execution slot. Navigation loads the three object listings and uses the version count in `SHOW MODELS` to fetch `SHOW MODEL VERSIONS` for multi-version Models. The version metadata is shared by each Model's entries under Models and Functions. Selecting an object fetches only that object's `SHOW CREATE`, without bulk definition enrichment. `SHOW FUNCTIONS` includes Model callables, which use `SHOW CREATE MODEL`. Catalog loading disables competing editor and management operations; leaving the DDL workspace cancels pending reads. Tree refresh waits for the current execution to finish, and successful editor update statements refresh its listings. Changing or closing the Session cancels pending reads and clears Catalog state. Catalog objects and DDL remain in page memory rather than browser storage. Background reads do not create local history entries.

| Tree section | Listing | Selected DDL |
|---|---|---|
| Tables | `SHOW TABLES` | `SHOW CREATE TABLE` |
| Models | `SHOW MODELS`, `SHOW MODEL VERSIONS` for multi-version objects | `SHOW CREATE MODEL <name> [VERSION '<version>']` |
| Functions | `SHOW FUNCTIONS` | `SHOW CREATE FUNCTION` or `SHOW CREATE MODEL <name> [VERSION '<version>']` for Model callables |

Creation, alteration, resolution, deletion, schema inspection, and Model version management use ordinary public SQL in the editor. Creating a draft does not execute it. Statements and structured failures use the editor's existing result and history behavior. The frontend does not add `CASCADE` or bypass dependency errors.

Model and Function display addresses are resolved to the stored SQL name through `SHOW CREATE`. Fallback address spellings are tried only on structured `NOT_FOUND`. If multiple rows have the same display address, the DDL workspace reports ambiguity; users can inspect an explicitly named object in the SQL editor.

Prototype-only facts without public metadata, including Table health, runtime environment versions, and release-managed task DDL, are omitted. The frontend does not fabricate built-in task records or `CREATE TASK` statements.

## Media Contract

Workbench consumes the bounded thumbnail representation defined by the [`vqld` Service Design](./vqld.md#image-flight-boundary). It does not request inline originals, dereference media locators, or persist query results.

Thumbnail dimensions and byte bounds are service configuration, not Workbench protocol extensions. A `BOX2D` value uses the Kernel contract's normalized top-left `x`, `y`, `w`, and `h` coordinates. The overlay maps those coordinates to the displayed image content rectangle, including any letterboxing, and clips invalid drawing geometry without changing the value shown in the ordinary `BOX2D` cell.

An `IMAGE` cell renders only when the field carries `ARROW:extension:name=vql.image`, the encoded payload is present, and the encoding is a browser-supported image type. Missing or invalid thumbnails render a typed empty or error cell rather than a broken image. The client never renders a URI or locator as an image source.

## Query and Error Contract

Workbench uses prepared schema metadata from `vqld` to classify statements without SQL parsing. Ordinary Run and management actions require bounded results; the attached-stream action explicitly permits an unbounded rolling preview. `SUBMIT JOB` returns a bounded registration result while the persistent Job runs independently in `vqld`. Cancelling a browser execution targets its public `execution_id`; stopping a persistent Job uses `STOP JOB '<job_id>'`.

Errors use the versioned VQL representation and standard gRPC status. The UI may add local editor context, but it does not parse messages to infer codes or retryability.

Browser, backend, and connectivity failures use a separate Workbench problem type with an explicit local source. They never reuse or synthesize a `VQL-*` identifier, and a forwarded VQL error is rendered from its structured fields without changing its machine-readable identity.

## Validation

The acceptance path demonstrates all of the following with a real visual query:

- connection to an existing `vqld` instance without a private engine API;
- rejection of an unbounded result from prepared statement metadata without client-side SQL parsing;
- incremental Arrow IPC decoding without JSON row conversion or loss of extension metadata;
- correct thumbnail and box rendering for bounded results;
- cancellation of exactly the active query;
- useful structured error presentation;
- the same query and result semantics as the Python/notebook path.

Unit fixtures include scalar, null, nested, `vql.image`, and `BOX2D` Arrow columns. Browser tests cover image decode failure, multiple aspect ratios, overlay clipping, keyboard execution, cancellation during streaming, Session expiry, and Blob URL cleanup. The end-to-end acceptance test uses the built frontend, Workbench backend, and shipped `vqld`; mocks alone cannot satisfy the acceptance path.

Persistent Job tests cover named-field decoding, timestamp and Int64 handling, unknown states, escaped Job IDs, searching, disabled terminal actions, serialized Stop/refresh, redacted SQL loading, structured failures, polling, and request cleanup. The browser acceptance path submits a real persistent Job through shipped `vqld`, inspects it, loads a separate draft, stops it, and rediscovers its terminal state after reload, including a narrow-screen layout check.

Catalog browser tests cover namespaces, object creation, formatted read-only DDL, original-SQL copying, Model versions through both object and callable entries, selection persistence, and narrow-screen version inspection. Local history and draft persistence, selected-statement execution, result JSON, and row inspection have frontend owner tests.

The strict `--visual-parity` browser run uses shared real-model SQL fixtures from `vql-testing/tests/workbench/`. A current Python extension generates an oracle through `sess.sql().collect()` and exercises the notebook HTML representation; the browser runs exactly the same SQL through the built Workbench and shipped `vqld`. It compares ordered labels, confidences, and normalized boxes, then validates Arrow IMAGE/BOX2D interpretation, bounded thumbnail dimensions and aspect ratio, JSON values, and rendered overlay geometry in the table and row inspector. The original-image reference in Python and the encoded thumbnail over Flight are transport representations of the same image; encoded bytes and local buffer identities are not compared. Missing artifacts or Python dependencies fail the strict run. Commands and output locations are documented in the [Workbench README](../../vql-workbench/README.md#checks).

## References

- [React with TypeScript](https://react.dev/learn/typescript)
- [Vite Guide](https://vite.dev/guide/)
- [Tailwind CSS with Vite](https://tailwindcss.com/docs/installation/using-vite)
- [shadcn/ui with Vite](https://ui.shadcn.com/docs/installation/vite)
- [Radix Primitives](https://www.radix-ui.com/primitives/docs/overview/introduction)
- [Lucide React](https://lucide.dev/guide/react)
- [CodeMirror Reference Manual](https://codemirror.net/docs/ref/)
- [CodeMirror SQL language package](https://github.com/codemirror/lang-sql)
- [Apache Arrow JavaScript](https://arrow.apache.org/js/current/)
- [Apache Arrow Flight SQL Rust client](https://docs.rs/arrow-flight/latest/arrow_flight/sql/client/)
- [TanStack Table](https://tanstack.com/table/latest/docs/overview)
- [Axum](https://docs.rs/axum/latest/axum/)
- [Vitest](https://vitest.dev/guide/)
- [Playwright](https://playwright.dev/docs/intro)
