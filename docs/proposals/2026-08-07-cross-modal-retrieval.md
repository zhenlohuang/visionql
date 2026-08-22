---
created_at: 2026-08-07
status: draft
target_version: v0.3
updated_at: 2026-08-15
---

# Cross-modal Retrieval with Lance Storage

## Summary

The v0.3 text-to-image retrieval feature registers separate `IMAGE_EMBEDDING(n)` and `TEXT_EMBEDDING(n)` Models, calls them through `IMAGE_EMBEDDING` and `TEXT_EMBEDDING`, persists `VECTOR(n)` results in Lance, and performs Top-K search with the `<->` distance operator and an optional HNSW index. This proposal currently fixes the new types, syntax, and execution model; the remaining implementation detail will be completed when v0.3 begins.

## Motivation and Scope

This proposal covers the complete PRD §3.3.5 workflow: embed data, write it to Lance, and run `ORDER BY <-> LIMIT` Top-K queries. Its scope includes the `VECTOR` type, retrieval syntax, vector Top-K execution, HNSW indexes, and a readable/writable Lance Table provider. Physical execution for image and text embedding reuses the [Kernel model pipeline](../kernel.md#model-runtime-and-inference).

## Detailed Design

### Vector Top-K

v0.3 normalizes `<->` to `L2_DISTANCE` and then executes a bounded Top-K. Without an index, `EXPLAIN` must report `BruteForceTopK`. With a compatible HNSW index, the engine rewrites `ORDER BY <-> LIMIT` to ANN. Earlier versions return a version-specific unsupported error for `<->`, `L2_DISTANCE`, and `CREATE INDEX ... USING HNSW`; they must not register an index that execution cannot use.

### Lance Storage

- The Lance Table provider appends bounded results directly. Streaming queries combine multiple epochs by time or size before committing a new version, avoiding one small commit per epoch. `IMAGE` is stored as an encoded blob with logical-type metadata for embedding and evidence frames.
- The Lance table provider supports projection pushdown, predicate pushdown, and statistics. Files written by VisionQL preserve logical-type metadata and restore `IMAGE`, `BOX2D`, and `VECTOR` on read, covering the complete write-then-search workflow.
- The public writable-Table contract for registration, validation, cancellation, timeout, and bounded buffering is defined in [Kernel Table Providers](../kernel.md#table-providers).

### New Types, Syntax, and Functions

| Addition | Definition | Existing contract it must preserve |
|---|---|---|
| `VECTOR(n)` | Arrow storage is `FixedSizeList<Float32, n>`; dimension is part of the type and is checked during planning | [Arrow representation](../kernel.md#arrow-representation) |
| `IMAGE_EMBEDDING(n)` / `TEXT_EMBEDDING(n)` Model types | Fixed functions are `IMAGE_EMBEDDING('<model>', image)` and `TEXT_EMBEDDING('<model>', text)`; earlier versions reject registration | [Typed Model contract](../kernel.md#typed-model-contract) |
| Vector-dimension resolution | Dimension `n` is structural Model-type data. Registration validates it against the resolved Runtime and PostProcessor contracts; planning derives the exact return type from the Model. | [Immutable query definition snapshot](../kernel.md#immutable-query-definition-snapshot) and [compiled pipeline](../kernel.md#compiled-pipeline-and-interfaces) |
| `L2_DISTANCE(VECTOR(n), VECTOR(n)) -> FLOAT` and `<->` | Require equal dimensions during planning; normalize `<->` to `L2_DISTANCE` in the AST or logical plan | [Syntax normalization](../kernel.md#syntax-normalization) and [built-in functions](../kernel.md#built-in-functions) |
| `CREATE INDEX ... USING HNSW` | Earlier versions parse and return unsupported without registering an empty object | [Parser boundary](../kernel.md#parser-boundary) |
| Vector Top-K and ANN rewrite | Defined above | [Optimizer and `EXPLAIN`](../kernel.md#optimizer-and-explain) |
| Constant-argument embedding, such as `TEXT_EMBEDDING('clip_text', '...')` | Execute once as a query-init expression when determinism requirements are met | [Extracting inference](../kernel.md#extracting-inference) |
| Reused `Inference` node | Extract `IMAGE_EMBEDDING` and `TEXT_EMBEDDING` into the same type-generic `Inference` node | [Logical planning](../kernel.md#datafusion-logical-plan-and-vql-metadata), [inference extraction](../kernel.md#extracting-inference), and [model runtime](../kernel.md#model-runtime-and-inference) |

## Relationship to the System Design

- New types, syntax, and optimizer behavior use the existing registries. Implementation adds the `VECTOR` logical type, the two embedding Model types and fixed functions, compatible processor registrations, the HNSW implementation, and a Lance connector; it does not change the streaming coordinator boundary in the [High-Level Design](../high_level_design.md#core-invariants).
- Embedding scheduling reuses the [Runtime registry and batching path](../kernel.md#runtime-registry-and-batching-ownership). The first supported local bundle path is the isolated `transformers` Runtime.

## Testing and Acceptance

Test Top-K correctness using an explicit comparison between brute-force and ANN result criteria, truthful `EXPLAIN` output for `BruteForceTopK` and ANN rewrites, logical-type round trips through Lance, and the PRD §3.3.5 end-to-end scenario. Complete the detailed acceptance thresholds with the v0.3 design.

## Open Questions

| Question | Evidence required | Deadline |
|---|---|---|
| Lance streaming append and compaction for small batches | A continuous seven-day write test covering version count, point reads, and compaction | Before the v0.3 Lance Table release |

## Changelog

| Date | Change |
|---|---|
| 2026-08-06 | Extracted from engine design v0.5.0 §8.4 and §10.2 |
| 2026-08-09 | Converted metadata to front matter, adopted date-based naming, and translated to English |
| 2026-08-10 | Aligned embedding types, fixed inference calls, vector dimensions, and Runtime reuse with the active typed-model design |
| 2026-08-15 | Retargeted cross-modal retrieval from v0.4 to v0.3 after merging the embedded releases |
