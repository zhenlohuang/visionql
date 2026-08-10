# VisionQL Proposals

This directory contains focused designs for VisionQL features that can be developed and delivered independently but are not yet part of the current scope in [design.md](../design.md). The complete design for active work—including image and video tables, the model runtime, RTSP and windows, and Console/Kafka Sinks—remains in `design.md`. Product requirements are defined in [prd.md](../prd.md).

## Index

| Created | Title | Status | Target version | Summary |
|---|---|---|---|---|
| [2026-08-05](./2026-08-05-workbench.md) | Workbench | Draft | v0.3 | Multimodal SQL client for editing, result and live preview, catalog browsing, and job operations |
| [2026-08-06](./2026-08-06-vqld-service.md) | vqld Service | Draft | v0.3 | Public Flight SQL contract, media protocol, durable jobs, checkpoints, and recovery |
| [2026-08-06](./2026-08-06-parquet-sink.md) | Parquet Sink | Draft | v0.4 | Persist results through bounded appends and streaming rolling files |
| [2026-08-07](./2026-08-07-cross-modal-retrieval.md) | Cross-modal Retrieval with Lance | Draft | v0.4 | Typed image/text embedding, vector Top-K, HNSW, and Lance storage |

## Planned but Not Yet Proposed

- Out-of-process Python UDF worker for v0.3. The in-process Arrow batch ABI is defined in [design.md](../design.md) §7.4; create a proposal when the worker work begins.
- Items under “Future Directions” in the [Roadmap](../../ROADMAP.md). Create a proposal only after an item is scheduled.

## Conventions

- **Filename:** `YYYY-MM-DD-kebab-case.md`, using the proposal's creation date. Multiple proposals created on the same day are distinguished by their descriptive slug; dates are not identifiers and files are never renumbered.
- **Template:** Start new proposals from [template.md](./template.md).
- **Language:** Proposal documents, titles, metadata keys, and index entries are written in English.
- **Front matter:** Include only `created_at`, `status`, `target_version`, and `updated_at`.
- **Status:** `draft` → `accepted` → `implemented`; use `superseded` when another proposal replaces it and link the replacement in the document body. When a feature enters the current scope of `design.md`, merge its content there and remove the proposal without renaming any remaining files.
- **System-design boundary:** If a change affects a global invariant—such as payload states, the epoch contract, Query Manifests, or a public protocol version—revise and review [design.md](../design.md) first. Otherwise, create or revise a proposal.
- **Index maintenance:** This README is the only proposal index. Update the table whenever a proposal is added, renamed, or removed.
