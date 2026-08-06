# VisionQL Roadmap

VisionQL 是面向多模态数据的批流一体查询与处理引擎。本文档描述近期版本的交付计划，按能力划分、不承诺具体日期；详细的产品定义见 [PRD](./docs/prd.md)，技术设计见[系统设计](./docs/design.md)与 [proposals](./docs/proposals/README.md)。

状态图例：✅ 已完成 · 🚧 进行中 · 📋 计划中

## v0.1 — MVP：单机查询图片与视频文件 📋

**目标**：`pip install` 后无需任何服务，5 分钟内用 SQL 得到第一个视觉查询结果。纯库态、纯批处理，不包含服务进程和流处理。

- [ ] 多模态类型系统：`IMAGE`、`VIDEO`、`BOX2D`
- [ ] 图片目录表（`USING IMAGES`）与 `UNNEST` 检测结果展开
- [ ] 视频目录表（`USING VIDEOS`，建表时按 fps 展开为帧表）
- [ ] 模型与函数注册：`CREATE MODEL` / `CREATE FUNCTION ... USING MODEL`（OBJECT_DETECTION）
- [ ] 模型运行时与批量推理（[proposal 0003](./docs/proposals/0003-model-runtime-and-inference.md)）：manifest 校验与模型完整性、processor 前后处理、批量调度、显存管理、取消
- [ ] 库态 Python UDF
- [ ] Sink：Console
- [ ] SQL shell、`vql run job.sql`（脚本顺序执行）、Python 库接口（`sess.sql()`、Arrow 结果交换、notebook 富显示）
- [ ] 基础优化：推理提取、列裁剪、帧采样下推、时间谓词下推

**验收**：

- 场景 A（首次使用，PRD 第 4 节）：本地图片目录 → Python UDF 过滤 → 检测筛选目标图片，结果显示在 Python 会话中（进程内 UDF 要求引擎与用户代码同进程），从安装到第一个结果不超过 5 分钟，全程无外部服务
- 性能（PRD 3.7）：批扫描吞吐以解码为瓶颈打满硬件；已落盘结果的交互查询 P95 < 1s
- 成本（PRD 第 7 节）：在可采样负载上，帧采样下推使 GPU 时长相对逐帧全量推理按采样比例线性降低

## v0.2 — 流：RTSP 摄入与窗口聚合 📋

**目标**：把 v0.1 验证过的查询逻辑原样切换到实时流，得到批流一体。仍为纯库态。

流建立在批之上，不与批同版本交付：场景 B 的验收断言是「批量回算与流式运行结果一致」，批必须先成为可信基准，两侧对不上时才有判断依据。

- [ ] RTSP 视频流摄入（[proposal 0002](./docs/proposals/0002-video-stream-processing.md)）：尽力而为投递、事件时间与水位线、断流自动重连
- [ ] `TUMBLE` 窗口聚合（`COUNT/SUM/AVG/MIN/MAX`）
- [ ] Sink：Kafka
- [ ] 持续查询前台运行：shell 中的无界 SELECT 持续打印，`vql run` 附着执行，Ctrl-C 先优雅停止、再次立即取消

**验收**：

- 场景 B（批流一体，PRD 第 4 节）：同一条“每分钟人数”查询先在本地视频表上批量回算，再切换到 RTSP 流前台运行并写入 Kafka，结果一致
- 性能（PRD 3.7）：单机 1×消费级 GPU 上 ≥ 8 路 1080p@5fps 并发流运行轻量检测 + 窗口聚合
- 正确性（PRD 第 7 节）：使用相同模型和采样率时，窗口聚合结果与手写基线管道一致

## v0.3 — vqld 服务态与 Workbench 📋

**目标**：持续查询脱离客户端进程常驻运行；分析师通过浏览器直接使用，无需本地安装。

`vqld`（单机守护进程，单二进制内置目录、模型运行时和流运行时）：

- [ ] Arrow Flight SQL 前端：元数据、流式结果、取消、结构化错误；兼容 ADBC/JDBC 客户端
- [ ] TLS、认证与表/流级权限
- [ ] 持久作业：`SUBMIT QUERY` 提交（CLI 封装为 `vql submit job.sql`）、`SHOW/DESCRIBE QUERY`、`PAUSE`/`RESUME`/`STOP`、检查点与崩溃自愈
- [ ] 媒体协议：缩略图会话选项；原图经 locator（Flight ticket/DoGet）解引用获取，仅对持久数据有效
- [ ] 进程外 Python UDF worker
- [ ] Prometheus 指标端点

Python 客户端：

- [ ] DataFrame API：链式构造逻辑计划，与 SQL 等价；库态和服务态通用

Workbench（独立子项目，标准 Flight SQL 客户端）：

- [ ] SQL 编辑与执行、查询历史
- [ ] 多模态结果预览：缩略图、检测框叠加、实时流结果滚动
- [ ] 目录浏览、持久作业提交与运维
- [ ] 成本面板（读取引擎 Prometheus 指标端点的实测数据）

**验收**：

- 稳定性：在 PRD 3.7 基线负载（≥ 8 路 1080p@5fps 并发流 + 持久作业）下连续运行 7×24，进程 RSS 无持续增长、无非预期重启、持久作业无掉线
- 恢复：强制终止演练通过——重启后持久作业自动恢复，规范化窗口状态不丢，已确认输出只可能重复（[系统设计](./docs/design.md) §14）
- 升级：用 v0.2 库态建好表、流、模型和函数的 Catalog 目录，直接由 `vqld` 打开后查询与 DDL 行为不变；目录格式版本变化时自动迁移，迁移失败可回滚，不要求用户重建目录
- 可用性：分析师可在 Workbench 中完成查询、原图点查和作业运维全流程

## v0.4 — 跨模态检索与结果落盘 📋

**目标**：在检测之上加入嵌入与向量检索，支持“以文搜图 / 以图搜图”；同期交付列式落盘，让检测与嵌入结果可以留存复用。

Parquet 与 Lance 一并在本版交付：两者共用同一套写出、`CREATE TABLE ... AS SELECT` 与逻辑类型恢复契约，分版本做会把 `IMAGE` 列存设计两遍。

- [ ] EMBEDDING 模型类型：同一模型派生多个函数（如 CLIP 的 `embed_image` / `embed_text`）
- [ ] `VECTOR(n)` 类型、`<->`（`L2_DISTANCE`）与 `ORDER BY ... LIMIT` 暴力 TopK
- [ ] Parquet Sink 与表 provider（[proposal 0007](./docs/proposals/0007-parquet-sink.md)）：批追加与流式滚动文件、`CREATE TABLE ... AS SELECT`、逻辑类型写出/读回
- [ ] Lance 存储与 Sink：`IMAGE` 原生列存、向量列，嵌入结果落盘复用
- [ ] HNSW 向量索引：复用 Lance 原生索引，存在索引时 TopK 自动改写为 ANN；自动启用的规模阈值由实测确定并写入文档，在此之前只支持显式建索引

**验收**：本地图片目录 → 嵌入 → 写入 Lance → 以文搜图返回 Top-20（PRD 3.3.5）；暴力 TopK 与 HNSW 各覆盖一档数据规模，规模口径随 [proposal 0008](./docs/proposals/0008-cross-modal-retrieval.md) 立项确定。

## 后续方向（暂无版本计划）

以下方向已在讨论中，为避免过早设计暂不排期，将根据实际使用反馈定义和排序：

- **优化器降本**：模型级联、推理结果物化与跨查询复用、`EXPLAIN` 成本预估、自动采样与 ROI 裁剪
- **更多算子与数据源**：`TRACK` 跨帧跟踪、`HOP`/`SESSION` 窗口、VLM 谓词、Kafka 帧源（至少一次投递）、跨流 JOIN（ReID 轨迹）
- **生态入口**：MCP 服务器（Agent 接入）、场景包
- **规模化**：集群部署、精确一次投递、多租户治理与审计、WASM UDF
- **边缘协同**：查询的边缘/中心自动切分、边缘节点集群管理

欢迎通过 Issues 参与讨论：如果你希望某个方向提前排期，请说明你的使用场景。
