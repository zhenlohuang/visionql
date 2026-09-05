# VisionQL Proposals

This directory contains focused designs for capabilities outside the current scope in the [High-Level Design](../high_level_design.md). Active and planned component contracts live under `docs/design/`, including the [`vqld` Service Design](../design/vqld.md) and [Workbench Design](../design/workbench.md). Product requirements are defined in [prd.md](../prd.md).

## Index

There are currently no active proposals.

## Unscheduled Directions

- Out-of-process Python UDF workers remain unscheduled. The in-process Arrow batch ABI is defined in the [Python Binding Design](../design/python_binding.md#python-udf-host); v0.2 service execution rejects Python Functions.
- Items under “Future Directions” in the [Roadmap](../../ROADMAP.md). Create a proposal only after an item is scheduled.

## Conventions

- **Filename:** `YYYY-MM-DD-kebab-case.md`, using the proposal's creation date. Multiple proposals created on the same day are distinguished by their descriptive slug; dates are not identifiers and files are never renumbered.
- **Template:** Start new proposals from [template.md](./template.md).
- **Language:** Proposal documents, titles, metadata keys, and index entries are written in English.
- **Front matter:** Include only `created_at`, `status`, `target_version`, and `updated_at`.
- **Status:** `draft` → `accepted` → `implemented`; use `superseded` when another proposal replaces it and link the replacement in the document body. When a feature enters current scope, merge its contract into the HLD and owning component design, then remove the proposal without renaming other files.
- **System-design boundary:** If a change affects a global v0.1 invariant—such as payload states or the epoch contract—revise and review the [High-Level Design](../high_level_design.md) first. Persistent service-job and public protocol contracts belong to the [`vqld` Service Design](../design/vqld.md).
- **Index maintenance:** This README is the only proposal index. Update it whenever a proposal is added, renamed, or removed.
