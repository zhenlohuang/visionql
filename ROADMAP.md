# VisionQL Roadmap

VisionQL 是面向多模态数据的批流一体查询与处理引擎。本文档描述近期版本的交付计划，按能力划分、不承诺具体日期；详细的产品定义见 [PRD](./docs/prd.md)。

状态图例：✅ 已完成 · 🚧 进行中 · 📋 计划中

## v0.1 — MVP：单机处理图片、视频文件与视频流 🚧

**目标**：`pip install` 后无需任何服务，5 分钟内用 SQL 得到第一个视觉查询结果。纯库态，不包含服务进程。

- [ ] 多模态类型系统：`IMAGE`、`VIDEO`、`BOX2D`
- [ ] 图片目录表（`USING IMAGES`）与 `UNNEST` 检测结果展开
- [ ] 视频目录表（`USING VIDEOS`，建表时按 fps 展开为帧表）
- [ ] RTSP 视频流摄入 + `TUMBLE` 窗口聚合（`COUNT/SUM/AVG/MIN/MAX`；尽力而为投递，断流自动重连）
- [ ] 模型与函数注册：`CREATE MODEL` / `CREATE FUNCTION ... USING MODEL`（OBJECT_DETECTION）
- [ ] 库态 Python UDF
- [ ] Sink：Kafka、Console
- [ ] SQL shell、Python DataFrame API、`visionql run`（持续查询前台运行）；CLI 同时提供 `vql` 别名
- [ ] 基础优化：推理提取、列裁剪、帧采样下推、时间谓词下推

**验收**（详见 PRD 第 4 节）：

- 场景 A（批流一体）：同一条“每分钟人数”查询先在本地视频表上批量回算，再切换到 RTSP 流前台运行并写入 Kafka，结果一致
- 场景 B（首次使用）：本地图片目录 → Python UDF 过滤 → 检测筛选目标图片，结果直接显示在 shell 中，从安装到第一个结果不超过 5 分钟，全程无外部服务

## v0.2 — vqld 服务态与 Workbench 📋

**目标**：持续查询脱离客户端进程常驻运行；分析师通过浏览器直接使用，无需本地安装。

`vqld`（单机守护进程，单二进制内置目录、模型运行时和流运行时）：

- [ ] Arrow Flight SQL 前端：元数据、流式结果、取消、结构化错误；兼容 ADBC/JDBC 客户端
- [ ] TLS、认证与表/流级权限
- [ ] 持久作业：`SUBMIT QUERY` 提交（CLI 封装为 `visionql submit job.sql`）、`SHOW/DESCRIBE QUERY`、`PAUSE`/`RESUME`/`STOP`、检查点与崩溃自愈
- [ ] 媒体协议：缩略图会话选项；原图经 locator（Flight ticket/DoGet）解引用获取，仅对持久数据有效
- [ ] 进程外 Python UDF worker
- [ ] Parquet Sink（查询结果落盘）
- [ ] Prometheus 指标端点

Workbench（独立子项目，标准 Flight SQL 客户端）：

- [ ] SQL 编辑与执行、查询历史
- [ ] 多模态结果预览：缩略图、检测框叠加、实时流结果滚动
- [ ] 目录浏览、持久作业提交与运维
- [ ] 成本面板（读取引擎 Prometheus 指标端点的实测数据）

**验收**：7×24 稳定性测试与强制终止恢复演练通过；分析师可在 Workbench 中完成查询、原图点查和作业运维全流程。

## v0.3 — 跨模态检索（文搜图）📋

**目标**：在检测之上加入嵌入与向量检索，支持“以文搜图 / 以图搜图”。

- [ ] EMBEDDING 模型类型：同一模型派生多个函数（如 CLIP 的 `embed_image` / `embed_text`）
- [ ] `VECTOR(n)` 类型、`<->`（`L2_DISTANCE`）与 `ORDER BY ... LIMIT` 暴力 TopK
- [ ] Lance 存储与 Sink：`IMAGE` 原生列存、向量列，嵌入结果落盘复用
- [ ] HNSW 向量索引：复用 Lance 原生索引，存在索引时 TopK 自动改写为 ANN，按数据规模启用

**验收**：本地图片目录 → 嵌入 → 写入 Lance → 以文搜图返回 Top-20（PRD 3.3.5）。

## 后续方向（暂无版本计划）

以下方向已在讨论中，为避免过早设计暂不排期，将根据实际使用反馈定义和排序：

- **优化器降本**：模型级联、推理结果物化与跨查询复用、`EXPLAIN` 成本预估、自动采样与 ROI 裁剪
- **更多算子与数据源**：`TRACK` 跨帧跟踪、`HOP`/`SESSION` 窗口、VLM 谓词、Kafka 帧源（至少一次投递）、跨流 JOIN（ReID 轨迹）
- **生态入口**：MCP 服务器（Agent 接入）、场景包
- **规模化**：集群部署、精确一次投递、多租户治理与审计、WASM UDF
- **边缘协同**：查询的边缘/中心自动切分、边缘节点集群管理

欢迎通过 Issues 参与讨论：如果你希望某个方向提前排期，请说明你的使用场景。
