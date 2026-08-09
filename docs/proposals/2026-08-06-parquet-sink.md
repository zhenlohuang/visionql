---
created_at: 2026-08-06
status: draft
target_version: v0.4
updated_at: 2026-08-09
---

# Parquet Sink

## Summary

The v0.4 persisted-results feature writes query output to Parquet files, including through `CREATE TABLE ... AS SELECT`, and makes the resulting tables queryable. Parquet is the default local format for event-frame retention and materialized results.

## Motivation and Scope

This proposal covers Parquet write behavior and restoration of logical types when data is read back. The public `CREATE SINK` contract is defined in [design.md](../design.md) §8.4. Lance persistence ships with v0.4 cross-modal retrieval, as described in the [Cross-modal Retrieval proposal](./2026-08-07-cross-modal-retrieval.md).

## Detailed Design

- Bounded queries append directly. Streaming output uses rolling files, closes files by time or size, and atomically renames temporary files so readers never observe a partial file.
- Starting in v0.4, `CREATE TABLE ... AS SELECT` supports Parquet. Earlier versions return the version-specific unsupported error defined in [design.md](../design.md) §7.1.
- Parquet files written by VisionQL preserve logical-type metadata. The Parquet table provider restores `IMAGE`, `BOX2D`, and other logical types and supports projection pushdown, predicate pushdown, and statistics.
- `IMAGE` values are persisted in encoded form. Media in persisted tables can later be reauthorized and dereferenced through its locator. Ephemeral live frames must first be written by an event-retention query; see [prd.md](../prd.md) §3.3.6 and [design.md](../design.md) §6.2.

## Relationship to the System Design

- Follow the public Sink contract in [design.md](../design.md) §8.4, including registration, validation, cancellation, timeout, bounded buffering, and coordinator-owned retries.
- Follow the `IMAGE` payload invariants in [design.md](../design.md) §6.2: `arena_id` and `arena_slot` must never be persisted.
- Restore logical types according to the Arrow extension-type contract in [design.md](../design.md) §6.1.

## Testing and Acceptance

Cover rolling-file atomicity, logical-type write/read round trips, schema validation, cancellation, and backpressure.

## Open Questions

None.

## Changelog

| Date | Change |
|---|---|
| 2026-08-06 | Extracted from engine design v0.5.0 §8.4 |
| 2026-08-09 | Converted metadata to front matter, adopted date-based naming, and translated to English |
