# 0008: 跨模态检索（含 Lance 存储）

- **编号**：0008
- **状态**：Draft
- **目标版本**：v0.4
- **对应 PRD**：[prd.md](../prd.md) §3.3.5
- **依赖设计**：[design.md](../design.md) §6.1（VECTOR 类型）、§7.3（EMBEDDING 模型）、§9（优化器）
- **关联 proposal**：0003（EMBEDDING 推理复用模型运行时）
- **最后更新**：2026-08-06

## 摘要

v0.4「文搜图检索」feature：注册 EMBEDDING 模型，把图片/文本嵌入为 `VECTOR(n)`，结果落盘 Lance，用 `<->` 距离做 TopK 检索，可选 HNSW 索引加速。本文当前只固定已在顶层设计中预留的契约与执行方式，详细设计随 v0.4 立项补全。

## 动机与范围

覆盖 PRD 3.3.5 的完整检索流程：「嵌入 → 写入 Lance → `ORDER BY <-> LIMIT` TopK」。范围包括向量 TopK 执行、HNSW 索引、Lance 表 provider 与 Lance Sink；EMBEDDING 推理的物理执行复用 proposal 0003 的模型运行时。

## 详细设计

### 向量 TopK

v0.4 将 `<->` 规范化为 `L2_DISTANCE`，再使用有界 TopK 执行。没有索引时必须如实显示 `BruteForceTopK`；存在 HNSW 索引时 `ORDER BY <-> LIMIT` 改写为 ANN。此前版本对 `<->`、`L2_DISTANCE` 和 `CREATE INDEX ... USING HNSW` 返回版本明确的未支持错误，不能登记一个不会被使用的索引。

### Lance 存储

- Lance Sink：有界查询直接追加；流查询按时间/大小合并多个 epoch 后提交新版本，避免逐 epoch 产生小 commit；`IMAGE` 落为编码态 blob 并保存逻辑类型元数据，用于嵌入和证据帧；
- Lance 表 provider 支持列裁剪、谓词下推和统计信息；VisionQL 自己写出的文件保存逻辑类型 metadata，读回时恢复 `IMAGE/BOX2D/VECTOR`，从而覆盖「写入 Lance 后再做 TopK」的检索流程；
- Sink 公共契约（登记与校验、取消/超时/有界缓冲）见 design.md §8.3。

### 顶层设计中已固定的契约

以下契约已在 [design.md](../design.md) 各章预留，v0.4 实现时不得破坏：

| 契约 | 位置 |
|---|---|
| `VECTOR(n)` = `FixedSizeList<Float32, n>`，维度属于类型、规划期检查 | design.md §6.1 |
| `EMBEDDING` 模型标准签名 `(IMAGE) -> VECTOR(n)` / `(STRING) -> VECTOR(n)`；v0.4 前拒绝注册 | design.md §7.3 |
| `VECTOR(n)` 维度在 Function 创建时确定（显式 `RETURNS` 优先，否则从 manifest 推导，冲突即 DDL 失败） | design.md §7.3 |
| `L2_DISTANCE` 内置函数与 `<->` 语法糖 | design.md §7.5～§7.6 |
| `CREATE INDEX ... USING HNSW` 语法（v0.4 前解析后拒绝，不登记空对象） | design.md §7.1 |
| `embed_text('...')` 等常量参数调用按 determinism 条件作为 query init expression 执行一次 | design.md §9 |
| Catalog index 对象接口、`Inference` 节点复用 | design.md §15（演进接口） |

## 与顶层设计的关系

- 类型、语法与优化器扩展点均已由 design.md 预留（上表）；本 proposal 实现时只新增 EMBEDDING processor、HNSW 索引实现和 Lance connector 注册项，不改 parser 或流协调器（design.md §15 的边界检验标准）；
- EMBEDDING 推理调度复用 proposal 0003 的运行时与批路径。

## 测试与验收

TopK 正确性（暴力与 ANN 结果对齐口径）、`EXPLAIN` 如实显示 `BruteForceTopK` / ANN 改写、Lance 写入/读回逻辑类型 round-trip、PRD 3.3.5 端到端场景。详细验收标准随 v0.4 设计补全。

## 开放问题

| 问题 | 决策前需要的证据 | 最迟时间 |
|---|---|---|
| Lance 小批流式追加与压实 | 连续 7 天写入、版本数、点查和压实测试 | v0.4 Lance Sink 发布前 |

## 变更记录

| 日期 | 变更 |
|---|---|
| 2026-08-06 | 从引擎设计 v0.5.0 §8.4、§10.2 迁出成文 |
