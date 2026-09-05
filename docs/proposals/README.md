# VisionQL Proposals

This directory contains focused designs for capabilities outside the current scope in the [High-Level Design](../high_level_design.md). Active v0.1 contracts live in the HLD and the `kernel`, `catalog`, `cli`, `python_binding`, and `testing` component designs. The planned v0.2 service contract lives in the [`vqld` Service Design](../vqld.md). Product requirements are defined in [prd.md](../prd.md).

## Index

| Created | Title | Status | Target version | Summary |
|---|---|---|---|---|
| [2026-08-05](./2026-08-05-workbench.md) | Workbench | Draft | v0.2 | Multimodal SQL client for editing, result and live preview, catalog browsing, and job operations |
| [2026-08-06](./2026-08-06-parquet-sink.md) | Parquet Table Writes | Draft | v0.3 | Persist results through bounded appends and streaming rolling files |
| [2026-08-07](./2026-08-07-cross-modal-retrieval.md) | Cross-modal Retrieval with Lance | Draft | v0.3 | Typed image/text embedding, vector Top-K, HNSW, and Lance storage |

## Planned but Not Yet Proposed

- Out-of-process Python UDF worker for v0.2. The in-process Arrow batch ABI is defined in the [Python Binding Design](../python_binding.md#python-udf-host), and its service ownership is defined in the [`vqld` Service Design](../vqld.md).
- Items under “Future Directions” in the [Roadmap](../../ROADMAP.md). Create a proposal only after an item is scheduled.

## Conventions

- **Filename:** `YYYY-MM-DD-kebab-case.md`, using the proposal's creation date. Multiple proposals created on the same day are distinguished by their descriptive slug; dates are not identifiers and files are never renumbered.
- **Template:** Start new proposals from [template.md](./template.md).
- **Language:** Proposal documents, titles, metadata keys, and index entries are written in English.
- **Front matter:** Include only `created_at`, `status`, `target_version`, and `updated_at`.
- **Status:** `draft` → `accepted` → `implemented`; use `superseded` when another proposal replaces it and link the replacement in the document body. When a feature enters current scope, merge its contract into the HLD and owning component design, then remove the proposal without renaming other files.
- **System-design boundary:** If a change affects a global v0.1 invariant—such as payload states or the epoch contract—revise and review the [High-Level Design](../high_level_design.md) first. Durable Query Manifests and public protocol versions belong to the [`vqld` Service Design](../vqld.md).
- **Index maintenance:** This README is the only proposal index. Update the table whenever a proposal is added, renamed, or removed.
