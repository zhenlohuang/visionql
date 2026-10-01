# Workbench user guide

Workbench is the browser client for one `vqld` endpoint. Use it to author SQL, inspect visual results, browse Catalog objects, and manage persistent Jobs. Workbench is available in the current source checkout for the upcoming v0.3 release; see [installation and availability](installation.md#availability).

- [Connect to a daemon](#connect-to-a-daemon)
- [Manage SQL drafts](#manage-sql-drafts)
- [Run SQL](#run-sql)
- [Inspect results](#inspect-results)
- [Browse the Catalog](#browse-the-catalog)
- [Manage persistent Jobs](#manage-persistent-jobs)
- [Use execution history](#use-execution-history)
- [Troubleshooting](#troubleshooting)

## Connect to a daemon

Follow [Workbench installation](installation.md#workbench) to start `vqld` and Workbench, then open [http://127.0.0.1:6040](http://127.0.0.1:6040).

1. Open **Settings**.
2. Set **vqld endpoint**, normally `http://127.0.0.1:6031` for local development.
3. For a secured endpoint, use HTTPS, enter its **Service credential**, and choose a PEM **TLS CA certificate** when additional certificate trust is needed.
4. Choose **Connect**, or **Reconnect** to replace an existing connection.

The connected daemon owns the Catalog, Models, source paths, and persistent Jobs. Local files used in SQL must be accessible to that daemon; relative paths resolve from its working directory. Workbench does not select a separate local Catalog.

Connection credentials and TLS material remain in backend memory and are not saved in browser storage. **Disconnect** ends the browser Session. Idle Sessions expire according to the backend timeout; active requests and attached previews keep the Session usable.

## Manage SQL drafts

Use the new-draft button to open another query tab. Default names are `Untitle.sql`, `Untitle1.sql`, `Untitle2.sql`, and so on, choosing the first available name.

Double-click a tab or click its pencil button to rename it. Names must be non-empty, contain no slashes, and be unique among open drafts; `.sql` is added when omitted. Open draft names and SQL are saved in browser storage and survive reloads. A draft is local editor content; renaming or editing it does not change a registered Catalog object or Job.

## Run SQL

Replace the initial draft with this query to check the connection:

```sql
SELECT 1 AS value;
```

Choose **Run buffer** to execute it. For media queries, follow the [Table and Model examples](sql-reference.md#create-table) through the Workbench editor, using data and artifacts available to the daemon.

| Action | Use it for |
|---|---|
| **Run buffer** | Execute the draft with bounded results |
| **Run selection or current statement** | Execute selected SQL, or the statement at the cursor |
| **Run as attached stream** | Preview a continuous query using the selected SQL or current statement |
| **Explain selection or current statement** | Inspect its plan without starting execution |
| **Cancel active execution** | Stop the active attached execution |

Workbench allows one active execution per browser Session, shared by editor queries, Catalog reads, and Job management. Cancel an attached preview before starting another statement. A stream preview retains the latest 500 rows in a rolling buffer.

Ordinary Run expects bounded results. Use the attached-stream action for an unbounded RTSP SELECT, or `SUBMIT JOB` for a persistent continuous write. Closing the browser or cancelling an attached preview leaves persistent Jobs running. See the [RTSP and Kafka tutorial](../../examples/README.md#rtsp-streaming-and-persistent-jobs) for a complete workflow.

## Inspect results

Results support table and JSON views. Click an image thumbnail to inspect its bounding-box overlay, label, and confidence, or open a row in the inspector to examine its returned values.

For typed visual inference, return the image together with the Model's `box`, `label`, and `confidence` columns, as in the [README example](../../README.md#typed-visual-inference). Use **Overlay Config** to select the IMAGE column and the corresponding box, label, and confidence columns. These choices apply only to the selected IMAGE column.

Same-named result columns remain separate. Table headers preserve their returned names; JSON, row inspection, and overlay choices distinguish duplicates by column position.

Flight results carry bounded image thumbnails. An OBJECT_DETECTION Model returns normalized BOX2D coordinates suitable for the overlay; `VQL_DETECT` instead returns pixel-coordinate boxes inside `locator`. See the [type reference](sql-reference.md#visual-and-tensor-types) for their meanings.

## Browse the Catalog

Expand **Catalog → Schema → Tables / Models / Functions** to browse objects across namespaces. Select an object to inspect its formatted, read-only `SHOW CREATE` declaration. The view supports selection, search, and copying the original server-returned SQL.

For a multi-version Model, the version list highlights the displayed version and marks the default. Selecting the Model loads its latest live declaration, which may differ from the default; selecting a version loads that exact version. The header shows the returned version. Select an object or version again to reload its definition.

Select a category to open a creation draft for its namespace. Create, resolve, alter, and drop objects through SQL in the editor. Successful update statements refresh the tree. See the [SQL reference](sql-reference.md) for declarations and lifecycle commands.

Model `RESOLVED` means its execution contract has been resolved. The Functions category also includes Model callables and routes them to Model details. Python Function declarations can be inspected and managed, but execution through `vqld` is unavailable.

## Manage persistent Jobs

Submit a continuous Table write with `SUBMIT JOB <name> AS INSERT INTO ...` in the editor, then open **Workspace → Jobs**. The [streaming tutorial](../../examples/README.md#rtsp-streaming-and-persistent-jobs) includes camera and Kafka Table setup.

Search Jobs by name, ID, state, or source health. The visible list refreshes every five seconds and has a manual refresh action. It shows service-reported status, source health, last event time, restart gaps, and errors.

**Show SQL** displays the redacted immutable definition, timestamps, restart gaps, window-state reset flag, and errors. Copying or loading the SQL preserves its redaction. Loading opens a separate draft and does not execute or modify the existing Job; replace redacted literals before resubmitting it.

**Stop** sends `STOP JOB '<job_id>'` for a STARTING or RUNNING Job and refreshes its state. Terminal and unknown states disable Stop. Closing Workbench, disconnecting, or cancelling an inspection does not stop a persistent Job.

After daemon restart, active RTSP Jobs resume from the current live position with fresh window state and a reported restart gap. Definitions are immutable; submitting a loaded draft creates a new Job. See [persistent Job semantics](sql-reference.md#persistent-jobs) for the lifecycle limits.

## Use execution history

Open **Workspace → History** to search prior runs by draft name or SQL, copy their SQL, load them into the editor, rerun them, or clear the local history. Workbench retains up to 500 execution records in browser storage, including SQL, status, and diagnostic information. Results and thumbnails are not saved with history.

Loading a record creates a draft. Rerun executes its SQL against the current connection and Catalog state, so its results may differ from the original run. History records are separate from the persistent Jobs listed by `vqld`.

## Troubleshooting

- **Connection failed or expired:** check the endpoint, credential, and certificate trust in Settings, then reconnect. See [secure Flight SQL setup](installation.md#secure-flight-sql-access).
- **An operation is busy:** cancel the active attached execution and retry. Editor and management actions share one Session execution slot.
- **A Table, Model, or file is missing:** check the connected daemon and its Catalog and working directory. Run the required declaration and resolution SQL through that connection.
- **A visual overlay uses the wrong image or columns:** select the intended IMAGE column and corresponding overlay inputs in Overlay Config. Duplicate names are distinguished by their positions.
- **Stopping a preview leaves a Job running:** open Jobs and explicitly Stop that Job; preview cancellation affects the attached execution.

Workbench development and acceptance checks remain in the [component README](../../vql-workbench/README.md). The [Workbench design](../design/workbench.md) defines its system and protocol boundaries.
