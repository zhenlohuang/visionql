# VisionQL Proposals

本目录存放 VisionQL 的子功能设计文档（proposal）。每个 proposal 对应一个**可独立开发、独立交付的 feature**，且尚未进入 [design.md](../design.md) 的当前范围；当前在建能力的完整设计（含图片/视频表、模型运行时、RTSP 与窗口、Console/Kafka）都在 design.md，产品需求见 [prd.md](../prd.md)。

## 索引

| 编号 | 标题 | 状态 | 目标版本 | 摘要 |
|---|---|---|---|---|
| [0001](./0001-vqld-service.md) | vqld 服务态 | Draft | v0.3 | Flight SQL 公开契约、媒体协议、持久作业、检查点与恢复 |
| [0002](./0002-workbench.md) | Workbench（Web 工作台） | Draft | v0.3 | 多模态 SQL 客户端：编辑执行、结果预览、实时预览、目录浏览与作业运维 |
| [0003](./0003-parquet-sink.md) | Parquet Sink | Draft | v0.4 | 结果落盘 Parquet：批追加与流式滚动文件 |
| [0004](./0004-cross-modal-retrieval.md) | 跨模态检索（含 Lance 存储） | Draft | v0.4 | EMBEDDING 模型、VECTOR 距离 TopK、HNSW 索引与 Lance 存储 |

## 规划中（尚未立项，暂不建文件）

- 进程外 Python UDF worker（v0.3）：进程内批量 Arrow ABI 见 design.md §7.4；进程外执行的详细设计待立项。
- ROADMAP「后续方向」中的各项：排期后各开新 proposal。

## 约定

- **文件名**：`NNNN-kebab-case.md`，四位编号从 0001 起连贯分配；行文中用编号引用（如 "proposal 0001"）。
- **模板**：新 proposal 从 [0000-template.md](./0000-template.md) 复制。
- **状态流转**：Draft →（评审通过）Accepted →（对应版本发布）Implemented；被替代时标 Superseded 并指向新编号。feature 进入 design.md 的当前范围后，内容整体并入 design.md、删除该文件，其余 proposal 依次前移保持编号连贯，并同步更新所有引用。
- **与顶层设计的分界**：改动若触及全局不变量（类型三态、epoch 契约、定义快照语义、公开协议版本等），先修订 [design.md](../design.md) 并走评审；否则新开或修订 proposal 即可。
- **索引维护**：本 README 是唯一索引，新增 proposal 时同步更新上表。
