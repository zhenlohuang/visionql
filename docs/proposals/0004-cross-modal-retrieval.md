# 0004: 跨模态检索（含 Lance 存储）

- **编号**：0004
- **状态**：Draft
- **目标版本**：v0.4
- **对应 PRD**：[prd.md](../prd.md) §3.3.5
- **依赖设计**：[design.md](../design.md) §6.1（Arrow 物理表示约定）、§7.3（MODEL 与 FUNCTION）、§9（优化器）、§10（模型运行时）
- **关联 proposal**：无
- **最后更新**：2026-08-06

## 摘要

v0.4「文搜图检索」feature：注册 EMBEDDING 模型，把图片/文本嵌入为 `VECTOR(n)`，结果落盘 Lance，用 `<->` 距离做 TopK 检索，可选 HNSW 索引加速。本文当前只固定新增的类型、语法与执行方式，详细设计随 v0.4 立项补全。

## 动机与范围

覆盖 PRD 3.3.5 的完整检索流程：「嵌入 → 写入 Lance → `ORDER BY <-> LIMIT` TopK」。范围包括 `VECTOR` 类型与检索语法、向量 TopK 执行、HNSW 索引、Lance 表 provider 与 Lance Sink；EMBEDDING 推理的物理执行复用 design.md §10 的模型运行时。

## 详细设计

### 向量 TopK

v0.4 将 `<->` 规范化为 `L2_DISTANCE`，再使用有界 TopK 执行。没有索引时必须如实显示 `BruteForceTopK`；存在 HNSW 索引时 `ORDER BY <-> LIMIT` 改写为 ANN。此前版本对 `<->`、`L2_DISTANCE` 和 `CREATE INDEX ... USING HNSW` 返回版本明确的未支持错误，不能登记一个不会被使用的索引。

### Lance 存储

- Lance Sink：有界查询直接追加；流查询按时间/大小合并多个 epoch 后提交新版本，避免逐 epoch 产生小 commit；`IMAGE` 落为编码态 blob 并保存逻辑类型元数据，用于嵌入和证据帧；
- Lance 表 provider 支持列裁剪、谓词下推和统计信息；VisionQL 自己写出的文件保存逻辑类型 metadata，读回时恢复 `IMAGE/BOX2D/VECTOR`，从而覆盖「写入 Lance 后再做 TopK」的检索流程；
- Sink 公共契约（登记与校验、取消/超时/有界缓冲）见 design.md §8.4。

### 本 feature 新增的类型、语法与函数

以下内容不在 design.md 的当前范围内，由本 proposal 引入；实现时必须与 design.md 已固定的约定兼容：

| 新增内容 | 定义 | 必须兼容的既有约定 |
|---|---|---|
| `VECTOR(n)` 类型 | Arrow storage 为 `FixedSizeList<Float32, n>`；维度属于类型，规划期检查 | design.md §6.1 的逻辑类型/扩展元数据约定 |
| `EMBEDDING` 模型类型 | 标准签名 `(IMAGE) -> VECTOR(n)` 或 `(STRING) -> VECTOR(n)`，入口决定输入模态；此前拒绝注册 | design.md §7.3 的语义指纹、参数三层归属与 `ALTER MODEL` 兼容性规则 |
| 向量维度确定规则 | Function 创建时确定：显式 `RETURNS VECTOR(n)` 优先，否则从模型 manifest 推导，冲突或都无法确定时 DDL 失败 | design.md §7.3、§10.2 的 manifest 解析 |
| `L2_DISTANCE(VECTOR(n), VECTOR(n)) -> FLOAT` 与 `<->` 语法糖 | 规划期要求维度相同；`<->` 在 AST/逻辑计划层规范化为 `L2_DISTANCE` | design.md §7.5～§7.6 的规范化与内置函数约定 |
| `CREATE INDEX ... USING HNSW` | 本版之前解析后返回未支持错误，不登记空对象 | design.md §7.1 的 DDL 路径与拒绝规则 |
| 向量 TopK 与 ANN 改写规则 | 见上文「向量 TopK」 | design.md §9 的规则顺序与 `EXPLAIN` 如实展示要求 |
| 常量参数嵌入调用（如 `embed_text('...')`） | 满足 determinism 条件时作为 query init expression 执行一次 | design.md §9.2 第 4 条 |
| `Inference` 节点复用 | EMBEDDING 调用同样提取为 `Inference` | design.md §4.1、§9.2、§10 |

## 与顶层设计的关系

- 类型、语法与优化器扩展点都通过既有注册点接入（上表）；本 proposal 实现时只新增 `VECTOR` 逻辑类型、EMBEDDING processor、HNSW 索引实现和 Lance connector 注册项，不改流协调器（design.md §2 的边界检验标准）；
- EMBEDDING 推理调度复用 design.md §10 的模型运行时与批路径。

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
