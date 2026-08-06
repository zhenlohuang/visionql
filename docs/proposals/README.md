# VisionQL Proposals

本目录存放 VisionQL 的子功能设计文档（proposal）。每个 proposal 对应一个**可独立开发、独立交付的 feature**；贯穿全系统的核心设计（架构、逻辑计划、流执行模型、类型系统、公共契约等）见 [design.md](../design.md)，产品需求见 [prd.md](../prd.md)。

## 索引

| 编号 | 标题 | 状态 | 目标版本 | 摘要 |
|---|---|---|---|---|
| [0001](./0001-media-table-providers.md) | 图片与视频目录表 | Draft | v0.1 | 把本地/对象存储中的图片目录与视频目录注册为可查询的表，视频按建表 fps 展开为帧行 |
| [0002](./0002-video-stream-processing.md) | 视频流处理（RTSP 接入与窗口聚合） | Draft | v0.2 | RTSP 摄入、事件时间与断流重连、TUMBLE 窗口聚合状态 |
| [0003](./0003-model-runtime-and-inference.md) | 模型运行时与 InferenceExec | Draft | v0.1 | 模型加载与完整性、processor、批量推理执行与调度 |
| [0004](./0004-kafka-sink.md) | Kafka Sink | Draft | v0.2 | 把查询结果以 JSON 写入 Kafka |
| [0005](./0005-vqld-service.md) | vqld 服务态 | Draft | v0.3 | Flight SQL 公开契约、媒体协议、持久作业、检查点与恢复 |
| [0006](./0006-workbench.md) | Workbench（Web 工作台） | Draft | v0.3 | 多模态 SQL 客户端：编辑执行、结果预览、实时预览、目录浏览与作业运维 |
| [0007](./0007-parquet-sink.md) | Parquet Sink | Draft | v0.3 | 结果落盘 Parquet：批追加与流式滚动文件 |
| [0008](./0008-cross-modal-retrieval.md) | 跨模态检索（含 Lance 存储） | Draft | v0.4 | EMBEDDING 模型、VECTOR 距离 TopK、HNSW 索引与 Lance 存储 |

## 规划中（尚未立项，暂不建文件）

- 进程外 Python UDF worker（v0.3）：v0.1 的进程内批量 Arrow ABI 见 design.md §7.4；v0.3 进程外执行的详细设计待立项。
- ROADMAP「后续方向」中的各项（优化器降本、`TRACK`/`HOP`/`SESSION` 窗口、Kafka 帧源、MCP 服务器、集群部署等）：扩展点已在 design.md §15 固定，排期后各开新 proposal。

## 约定

- **文件名**：`NNNN-kebab-case.md`，四位编号递增分配，永不复用；行文中用编号引用（如 "proposal 0005"）。
- **模板**：新 proposal 从 [0000-template.md](./0000-template.md) 复制。
- **状态流转**：Draft →（评审通过）Accepted →（对应版本发布）Implemented；被替代时标 Superseded 并指向新编号。历史 proposal 不删除。
- **与顶层设计的分界**：改动若触及全局不变量（类型三态、epoch 契约、定义快照语义、公开协议版本等），先修订 [design.md](../design.md) 并走评审；否则新开或修订 proposal 即可。
- **索引维护**：本 README 是唯一索引，新增 proposal 时同步更新上表。
