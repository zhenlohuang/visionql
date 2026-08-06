# 0005: vqld 服务态（Flight SQL 契约、持久作业与恢复）

- **编号**：0005
- **状态**：Draft
- **目标版本**：v0.3
- **对应 PRD**：[prd.md](../prd.md) §3.5（服务态）、§3.6、§3.7
- **依赖设计**：[design.md](../design.md) §5（epoch 模型）、§6.2（IMAGE 三态与 locator）、§10（无进程假设内核）、§12.2（服务态安全）
- **关联 proposal**：0002（检查点建立在其规范化窗口状态之上）、0006（Workbench 是本契约的首个客户端）
- **最后更新**：2026-08-06

## 摘要

v0.3「服务态部署与作业管理」feature：`vql-server` 构建的 `vqld` 守护进程，对外只暴露 Arrow Flight SQL、公开 SQL 系统语句、健康检查和 Prometheus 指标端点。本文固定 Flight SQL 公开契约（能力协商、statement 分类、错误 envelope）、Workbench 所需媒体协议、系统查询最小 schema、`SUBMIT QUERY` 持久作业及其检查点与恢复、持续查询状态机。

## 动机与范围

库态内核（v0.1）不监听端口；v0.3 由 `vqld` 承担网络、TLS/认证、持久作业管理与恢复。所有客户端（CLI `--server`、Workbench、ADBC/JDBC）只通过本文契约接入，引擎不提供私有管理 API（design.md §16 ADR-010 / ADR-014）。服务态安全边界见 design.md §12.2。

## 详细设计

### Flight SQL 契约

`vql-server` 在 v0.3 实现以下公开能力：

- Flight SQL statement query/update、prepared statement、`GetSchema`、`GetCatalogs`、`GetDbSchemas`、`GetTables`、`GetTableTypes`、`GetSqlInfo`，以及长查询使用的 `PollFlightInfo`；
- statement-query 的有界和无界结果都通过 `DoGet` 返回 Arrow batch。无界查询在 schema 和 endpoint 就绪后立即返回可消费的 FlightInfo，不等待查询结束；取消使用 `CancelFlightInfo`，连接断开也触发服务端 cancellation token；
- Handshake 返回逻辑 session token；`SET` 和临时执行状态绑定该 session，而不是底层 gRPC channel。channel 可以复用，但客户端必须在每个 RPC 上携带对应 token，服务端也必须逐请求验证并据此选择 Session；不能把“这个 channel 已完成过 Handshake”当作授权；
- `SHOW STREAMS/MODELS/FUNCTIONS/SINKS` 和 `DESCRIBE` 使用公开 SQL，不增加私有目录 RPC；
- ADBC/JDBC 通过 Flight SQL 驱动接入，不再维护另一套查询或目录协议；驱动不认识 `visionql.image` 时仍可按标准 Struct storage type 读取；
- vendor `GetSqlInfo` 字段公布 VisionQL 客户端协议版本、SQL 方言版本、`visionql.image` 扩展版本和能力位，供独立发布的客户端协商。

Vendor SqlInfo ID 从规范保留的 10000 起固定：

| ID | 类型 | 含义 |
|---|---|---|
| 10000 | string | `visionql_protocol_version` |
| 10001 | string | `sql_dialect_version` |
| 10002 | string | `visionql_image_version` |
| 10003 | list<string> | capability 名称集合 |

`visionql_protocol_version` 和 `sql_dialect_version` 使用十进制 `MAJOR.MINOR` 字符串；客户端按两个整数解析，不能做字符串大小比较。`visionql_image_version` 当前固定为整数版本字符串 `1`。

名称集合至少按版本公布 `unbounded_do_get`、`poll_flight_info`、`cancel_flight_info`、`statement_info_v1`、`image_thumbnail_mode`、`frame_at_v1` 和 `query_control_v1`。未知 capability 必须可忽略。

每条 prepared statement 在 prepare 成功后通过 result schema metadata 返回以下稳定字段；update/DDL 使用零列 schema 携带同样的 metadata：

```text
visionql.statement_info.version = 1
visionql.statement.kind = query | update | ddl | persistent_submission
visionql.query.mode = bounded | unbounded | not_applicable
visionql.statement.side_effect = read_only | write
```

`kind` 描述客户端必须使用的 transport，不等同于 SQL 的首个关键字：

| 语句 | kind | mode | side_effect | 执行结果 |
|---|---|---|---|---|
| 有界 `SELECT`、`SHOW`、`DESCRIBE` | `query` | `bounded` | `read_only` | statement-query / `DoGet` 数据结果 |
| 普通无界 `SELECT` | `query` | `unbounded` | `read_only` | 附着式 `DoGet` 数据流 |
| 有界 `INSERT` / DML | `update` | `bounded` | `write` | statement-update，完成后返回 affected rows |
| 普通无界 `INSERT INTO ... SELECT ...` | `query` | `unbounded` | `write` | 附着式 `DoGet` 状态流 |
| `SUBMIT QUERY ...` | `persistent_submission` | `unbounded` | `write` | statement-query，一行持久作业结果 |
| 目录 DDL / `SET` / 作业控制 | `ddl` | `not_applicable` | `write` | statement-update；零列 result schema 仍带 metadata |

附着式无界 `INSERT` 的固定状态流 schema 为 `query_id Utf8, lifecycle Utf8, state Utf8, definition_revision Utf8, updated_at Timestamp(Nanosecond, UTC)`；`lifecycle` 恒为 `attached`，启动和状态改变时各发送一行，DoGet 保持打开直到查询结束或取消。所有 statement-query 返回的 `FlightInfo.app_metadata` 使用 UTF-8 JSON `VisionqlFlightInfoV1`，至少包含 `{"version":1,"query_id":"...","statement_kind":"...","query_mode":"..."}`；kind/mode 必须与 prepared schema metadata 一致，后者是分类真相。客户端忽略未知字段，不从 ticket 内容提取 query ID。

prepare 只解析、授权和规划，不执行 DDL、DML 或外部 I/O。上述信息让 CLI、Workbench 和其他客户端在执行前选择 statement-query/update、识别无界语句并决定取消策略，不需要复制完整 VQL parser。脚本中后续语句可能依赖前面 DDL，因此客户端按顺序 prepare/execute；不能声称在执行整份脚本前完成了全局语义预检。

查询错误使用标准 gRPC status 表示大类，并在 `visionql-error-bin` trailing metadata 中携带 Protobuf 二进制编码的 `visionql.protocol.v1.VisionqlErrorV1`：

```proto
message VisionqlErrorV1 {
  uint32 version = 1;          // 固定为 1
  string code = 2;
  string message = 3;
  optional string hint = 4;
  optional uint64 source_start = 5;
  optional uint64 source_end = 6;
  optional string query_id = 7;
  bool retryable = 8;
}
```

`code`、span 和 `retryable` 是协议字段，`message/hint` 是人类可读文本。source span 是相对于当前 statement UTF-8 文本的半开字节区间 `[source_start, source_end)`；没有可靠位置时不设置。不了解扩展的 Flight 客户端仍能读取标准 status；Workbench 必须读取 envelope，不能解析错误字符串。BFF 自己负责附加多语句脚本中的 `statement_index`。

服务态从 v0.3 首个版本起就包含 TLS、认证和表/流级权限，以及 `SUBMIT QUERY`、`SHOW/DESCRIBE QUERY`、`SHOW QUERY DEPENDENCIES`、`PAUSE/RESUME/STOP` 等公开作业 SQL；不提供只给 Workbench 使用的管理 RPC。

无界语句生命周期：

- 普通无界 `SELECT`、`INSERT INTO <sink> SELECT ...` 附着当前 Flight session；结果或状态持续通过 Flight 返回，客户端取消、session 过期或连接丢失后终止，不写入 QueryJob。升级服务版本不会悄悄改变同一条 SQL 的生命周期；
- 只有显式 `SUBMIT QUERY <name> AS INSERT INTO <sink> SELECT ...` 才创建持久作业。它通过 statement-query 路径返回一行 Arrow 结果 `query_id Utf8, name Utf8, state Utf8, definition_revision Utf8`；引擎完成规划和 Catalog 事务后立即返回，作业由 `vqld` 后台管理，Flight 请求结束不影响它；
- 有界 SELECT/INSERT 由当前 Flight 请求等待完成；Console Sink 不允许成为服务态持久作业目标。

### Workbench 所需媒体协议

Flight 会话提供三种 IMAGE 结果模式和按引用取帧：

```sql
SET vql.result.image_mode = 'reference'; -- 默认
SET vql.result.image_mode = 'thumbnail'; -- Workbench 默认，最长边和质量另有会话选项
SET vql.result.image_mode = 'inline';    -- 原编码内容，受结果字节上限保护
```

三种模式使用同一个 `visionql.image` Arrow storage schema，仅改变 `encoded` 是否存在以及它的语义标记。

原图按需读取使用 PRD 已定义的函数：

```sql
-- 打开 locator 指向的当前帧
SELECT TO_JPEG(FRAME_AT($1), 90);

-- 在同一视频对象内显式选择另一个时间点
SELECT TO_JPEG(FRAME_AT($1, $2), 90);
-- $1 = IMAGE.locator, $2 = 目标 pts_ms
```

`FRAME_AT` 不接受展示 URI，只解析版本化 locator，并按其中的 source revision 对当前 principal 重新授权和执行范围校验。文件和对象存储引用可以重新读取；live 流只在服务端短期环形缓存仍有该帧时成功（缓存行为见 design.md §6.2）。详细客户端行为见 [proposal 0006](./0006-workbench.md)。

### 系统查询的最小 schema

为了让 Workbench 不解析日志，v0.3 固定以下最小列；后续版本可以追加列：

```text
SHOW QUERIES:
  query_id, name, lifecycle, state, mode, source_kind, source_health,
  delivery_semantics, last_event_time, started_at, updated_at,
  definition_revision, error_code, error_message

DESCRIBE QUERY <id>:
  query_id, name, lifecycle, state, owner, sql_redacted,
  created_at, started_at, updated_at, definition_revision,
  checkpoint_id, checkpoint_at, error_code, error_message

SHOW QUERY DEPENDENCIES <id>:
  query_id, object_kind, object_id, object_name,
  revision_id, semantic_fingerprint
```

作业控制语法固定为：

```sql
SUBMIT QUERY people_per_minute AS
INSERT INTO people_sink SELECT ...;
DESCRIBE QUERY '<query_id>';
SHOW QUERY DEPENDENCIES '<query_id>';
PAUSE QUERY '<query_id>';
RESUME QUERY '<query_id>';
STOP QUERY '<query_id>';
```

`query_id` 是服务端生成的 UUID 字符串；持久作业名称由 `SUBMIT QUERY` 显式提供，在 owner 的非终态作业中唯一。附着查询的 `name` 为 NULL、`lifecycle=attached`；持久作业为 `lifecycle=persistent`。名称只用于展示和筛选，不能代替 ID 执行状态变更。

查询级指标不提供系统 SQL 通道：v0.3 经 Prometheus 指标端点按 `query_id` 等标签暴露（design.md §11.3），与 PRD 3.8 的“成本面板读取 Prometheus 指标端点”保持一致。指标名在 v1.0 前仍可演进，但必须遵循 Prometheus 命名与单位后缀约定，客户端不通过字符串猜单位。

### 持久作业检查点与恢复

v0.3 为 `SUBMIT QUERY` 提交的持久作业提供检查点与崩溃自愈。RTSP 是不可重放 live 源，因此检查点的目标不是重放数据，而是：**已积累的窗口状态和水位线在崩溃后不丢失，恢复后从 live 位置继续，缺口如实反映**。

协调器按时间或状态增量选择一个已完成 epoch 作为检查点边界，每个边界使用以下协议：

1. 从当前检查点恢复的状态开始，应用 epoch 数据并生成应输出的关闭窗口；
2. 将输出写入 Sink，等待所有写入确认；
3. 原子持久化新的检查点，其中包含逻辑计划哈希、目录定义快照、水位线、规范化窗口状态、各 `WindowStateCodec` 版本和 Sink delivery sequence。

检查点直接写入 [proposal 0002](./0002-video-stream-processing.md) 定义的规范化 Arrow state，并记录 operator ID、state schema fingerprint、codec version 和 engine state-format version；checkpoint 不调用活动 accumulator 的 `state()`。恢复时这些字段必须与定义快照匹配；不兼容时不能勉强反序列化，作业进入 `state=FAILED, error_code=RECOVERY_INCOMPATIBLE`。

恢复行为：

- 从最近一次成功检查点恢复窗口状态与水位线；RTSP 以新的 `source_generation` 从 live 位置重新接入，并经过 proposal 0002 的时间连续性门；
- 检查点之后、崩溃之前已写出的窗口结果可能重复输出——对已确认输出而言语义是至少一次；
- 崩溃期间与断流期间的源数据不可恢复，形成的缺口通过指标和 gap 记录如实反映，不补造行。

可重放源（如 Kafka 帧源）的基于 offset 的重放式恢复未排期；届时源进度已通过 `SourceProgress` 进入同一检查点边界，协议只需扩展 offset 提交顺序，不改变本节结构。精确一次同样未排期，需要把检查点与事务 Sink 的 commit 放进同一 barrier，不由本协议冒充。

### 持续查询状态机

```text
SUBMITTED → STARTING → RUNNING ⇄ PAUSED
                 │         │
                 ├────→ RECOVERING ───→ RUNNING
                 └────→ FAILED
RUNNING / PAUSED / FAILED → STOPPED
```

- `PAUSE` 在当前 epoch 的一致性边界完成后停源；RTSP 暂停期间会产生不可恢复缺口；
- `RESUME` 只能使用原定义快照继续，不能顺便 replan。需要采用新函数或模型修订时，必须 `STOP` 旧作业并用原 SQL 显式 `SUBMIT` 新作业；新作业取得新的 `query_id` 和定义快照；
- `STOP` 是终态，释放模型、源和状态资源，但保留作业历史；
- 服务进程启动时恢复 `RUNNING` 或 `RECOVERING` 作业：窗口状态从检查点恢复，RTSP 从 live 位置继续。

## 与顶层设计的关系

- `vqld` 基于 design.md §10.1 的无进程假设内核构建；`vql-server` 的 crate 边界见 design.md §13；
- IMAGE 传输遵守 design.md §6.2 的三态载荷与 locator 不变量；错误码固定集也定义在该节；
- 检查点边界建立在 design.md §5 的 epoch 一致性边界之上；窗口状态恢复 ABI 由 proposal 0002 的 `WindowStateCodec` 承担；
- 定义快照与 revision lease 语义遵守 design.md §4.3、§7.2；
- TLS、认证与权限边界见 design.md §12.2；至少一次投递语义对应 design.md §16 的 ADR-008。

## 测试与验收

对应 design.md §14.1 的「协议 v0.3」与「恢复 v0.3」测试行：全部约定 metadata RPC、statement/prepared transport 映射、`statement_info_v1`、FlightInfo query ID、附着式无界状态流、断连取消、逐 RPC session 隔离、Protobuf 错误 envelope、IMAGE storage schema/version、TLS/认证/权限、`SUBMIT QUERY`、查询详情/依赖/控制、IMAGE 三种模式、locator 篡改/撤权/过期、`FRAME_AT`、能力协商；在 Sink ack 与检查点持久化前后逐点 kill，验证规范化窗口状态不丢、已确认输出只可能重复，并覆盖 codec/version 不兼容。

## 开放问题

| 问题 | 决策前需要的证据 | 最迟时间 |
|---|---|---|
| `IMAGE` thumbnail/inline 上限与 locator TTL | Workbench、Python、BI 的带宽、可用性和撤权/过期测试；默认引用以及 uri/locator 分工已固定 | v0.3 Flight schema 冻结前 |

## 参考资料

- [Apache Arrow Flight SQL 规范](https://arrow.apache.org/docs/format/FlightSql.html)

## 变更记录

| 日期 | 变更 |
|---|---|
| 2026-08-06 | 从引擎设计 v0.5.0 §5.6～§5.7、§11.4～§11.6 迁出成文 |
