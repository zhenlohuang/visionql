---
created_at: 2026-08-07
status: draft
target_version: v0.4
updated_at: 2026-08-10
---

# Cross-modal Retrieval with Lance Storage

## Summary

The v0.4 text-to-image retrieval feature registers separate `IMAGE_EMBEDDING(n)` and `TEXT_EMBEDDING(n)` Models, calls them through `EMBED_IMAGE` and `EMBED_TEXT`, persists `VECTOR(n)` results in Lance, and performs Top-K search with the `<->` distance operator and an optional HNSW index. This proposal currently fixes the new types, syntax, and execution model; the remaining implementation detail will be completed when v0.4 begins.

## Motivation and Scope

This proposal covers the complete PRD §3.3.5 workflow: embed data, write it to Lance, and run `ORDER BY <-> LIMIT` Top-K queries. Its scope includes the `VECTOR` type, retrieval syntax, vector Top-K execution, HNSW indexes, the Lance table provider, and Lance Sink. Physical execution for image and text embedding reuses the typed model pipeline in [design.md](../design.md) §10.

## Detailed Design

### Vector Top-K

v0.4 normalizes `<->` to `L2_DISTANCE` and then executes a bounded Top-K. Without an index, `EXPLAIN` must report `BruteForceTopK`. With a compatible HNSW index, the engine rewrites `ORDER BY <-> LIMIT` to ANN. Earlier versions return a version-specific unsupported error for `<->`, `L2_DISTANCE`, and `CREATE INDEX ... USING HNSW`; they must not register an index that execution cannot use.

### Lance Storage

- Lance Sink appends bounded results directly. Streaming queries combine multiple epochs by time or size before committing a new version, avoiding one small commit per epoch. `IMAGE` is stored as an encoded blob with logical-type metadata for embedding and evidence frames.
- The Lance table provider supports projection pushdown, predicate pushdown, and statistics. Files written by VisionQL preserve logical-type metadata and restore `IMAGE`, `BOX2D`, and `VECTOR` on read, covering the complete write-then-search workflow.
- The public Sink contract for registration, validation, cancellation, timeout, and bounded buffering is defined in [design.md](../design.md) §8.4.

### New Types, Syntax, and Functions

| Addition | Definition | Existing contract it must preserve |
|---|---|---|
| `VECTOR(n)` | Arrow storage is `FixedSizeList<Float32, n>`; dimension is part of the type and is checked during planning | Logical types and extension metadata in [design.md](../design.md) §6.1 |
| `IMAGE_EMBEDDING(n)` / `TEXT_EMBEDDING(n)` Model types | Fixed functions are `EMBED_IMAGE('<model>', image)` and `EMBED_TEXT('<model>', text)`; earlier versions reject registration | Typed Model contract and semantic fingerprints in [design.md](../design.md) §7.3 |
| Vector-dimension resolution | Dimension `n` is structural Model-type data. Registration validates it against the resolved Runtime and PostProcessor contracts; planning derives the exact return type from the Model. | Query Manifest and pipeline validation in [design.md](../design.md) §4.3 and §10.1 |
| `L2_DISTANCE(VECTOR(n), VECTOR(n)) -> FLOAT` and `<->` | Require equal dimensions during planning; normalize `<->` to `L2_DISTANCE` in the AST or logical plan | Normalization and built-in functions in [design.md](../design.md) §7.5–§7.6 |
| `CREATE INDEX ... USING HNSW` | Earlier versions parse and return unsupported without registering an empty object | DDL and rejection behavior in [design.md](../design.md) §7.1 |
| Vector Top-K and ANN rewrite | Defined above | Rule ordering and truthful `EXPLAIN` in [design.md](../design.md) §9 |
| Constant-argument embedding, such as `EMBED_TEXT('clip_text', '...')` | Execute once as a query-init expression when determinism requirements are met | [design.md](../design.md) §9.2 item 4 |
| Reused `Inference` node | Extract `EMBED_IMAGE` and `EMBED_TEXT` into the same type-generic `Inference` node | [design.md](../design.md) §4.1, §9.2, and §10 |

## Relationship to the System Design

- New types, syntax, and optimizer behavior use the existing registries. Implementation adds the `VECTOR` logical type, the two embedding Model types and fixed functions, compatible processor registrations, the HNSW implementation, and a Lance connector; it does not change the streaming coordinator boundary in [design.md](../design.md) §2.
- Embedding scheduling reuses the Runtime registry and batching path in [design.md](../design.md) §10. The first supported local bundle path is the isolated `transformers` Runtime.

## Testing and Acceptance

Test Top-K correctness using an explicit comparison between brute-force and ANN result criteria, truthful `EXPLAIN` output for `BruteForceTopK` and ANN rewrites, logical-type round trips through Lance, and the PRD §3.3.5 end-to-end scenario. Complete the detailed acceptance thresholds with the v0.4 design.

## Open Questions

| Question | Evidence required | Deadline |
|---|---|---|
| Lance streaming append and compaction for small batches | A continuous seven-day write test covering version count, point reads, and compaction | Before the v0.4 Lance Sink release |

## Changelog

| Date | Change |
|---|---|
| 2026-08-06 | Extracted from engine design v0.5.0 §8.4 and §10.2 |
| 2026-08-09 | Converted metadata to front matter, adopted date-based naming, and translated to English |
| 2026-08-10 | Aligned embedding types, fixed inference calls, vector dimensions, and Runtime reuse with the active typed-model design |
