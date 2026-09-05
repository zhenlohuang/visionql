# VisionQL Workbench Design

Workbench is the v0.3 visual SQL client for VisionQL. It shortens the loop between writing a bounded query and understanding image-based results without creating a second engine or control-plane API.

## Purpose

Generic SQL clients can execute VisionQL SQL but do not naturally render `IMAGE`, `BOX2D`, or arrays of detections. The v0.3 workflow is deliberately narrow:

> A data or ML engineer writes one bounded visual query and inspects image thumbnails with detection boxes in a browser.

Catalog administration, continuous-job operations, dashboards, cost reporting, team sharing, and a general BI experience are separate capabilities and are not part of v0.3.

## v0.3 Scope

The v0.3 release contains only:

- one endpoint and one authenticated Session;
- a single-statement SQL editor with run and cancel;
- a bounded result table;
- rendering of the v0.2 `IMAGE` thumbnail representation;
- `BOX2D` overlays for detection results;
- structured error display using the public VQL error fields.

The release excludes:

- multi-statement scripts, tabs, saved queries, and collaboration;
- live-stream preview;
- Catalog browsing or mutation;
- persistent-job submission or operation;
- original-media lookup or locator handling;
- metrics, cost views, dashboards, and Prometheus access;
- a Workbench-owned identity or permission model;
- mobile-specific behavior and production deployment topology.

Each excluded capability requires its own observed user task before it is added.

## Architecture Constraints

Workbench is an independent client. It uses public Arrow Flight SQL, SQL, and structured errors. It never reads SQLite files, imports engine crates, or requires a Workbench-only `vqld` RPC.

A small browser-facing adapter may be used if direct browser Flight SQL support is insufficient. That adapter owns only transport conversion, a short-lived browser Session, cancellation propagation, and bounded in-memory thumbnails. It stores no Catalog objects, query definitions, jobs, permissions, or business data.

The adapter must not:

- rewrite user SQL to add sampling, filters, or `LIMIT`;
- parse SQL to infer boundedness or side effects;
- treat Prometheus as a product-data API;
- cache credentials or original media in browser storage;
- keep an attached query alive after its browser result stream disappears.

## Media Contract

Workbench consumes the one bounded thumbnail representation defined by the [`vqld` Service Design](./vqld.md#image-flight-boundary). It does not request inline originals, dereference media locators, or persist query results.

Thumbnail dimensions and byte bounds are service configuration, not Workbench protocol extensions. The UI scales the received thumbnail and maps `BOX2D` pixel coordinates to its displayed dimensions.

## Query and Error Contract

v0.3 executes only bounded statements. It uses prepared schema metadata from `vqld` to reject an unbounded result before opening a browser stream. Cancellation targets the server query ID returned by the public Flight boundary.

Errors use the versioned VQL representation and standard gRPC status. The UI may add local editor context, but it does not parse messages to infer codes or retryability.

## Validation

The v0.3 acceptance path demonstrates all of the following with a real visual query:

- connection to an existing `vqld` instance without a private engine API;
- correct thumbnail and box rendering for bounded results;
- cancellation of exactly the active query;
- useful structured error presentation;
- the same query and result semantics as the Python/notebook path.

## Deferred Capabilities

Later evidence may justify one capability at a time:

- attached live-result preview;
- persistent-job inspection using `SHOW QUERIES` and `DESCRIBE QUERY`;
- job submission and stop controls through public SQL;
- Catalog browsing through the supported Flight SQL metadata profile;
- original-media access after a separate authorization and transport design;
- per-query operational views from service query-status data rather than Prometheus.

None of these capabilities is part of v0.3.
