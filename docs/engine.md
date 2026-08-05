# VisionQL 引擎设计

> 本文根据 [VisionQL PRD](./prd.md) v0.1.4 重新设计引擎。详细实现范围是 v0.1 单机 MVP，包括库态、CLI、Python 和基础服务态；为了让 v0.2 的服务生产化、Kafka 恢复与 Workbench 能够建立在稳定边界上，本文同时定义它们依赖的公开契约，但不把后续能力提前算入 MVP。

- **设计版本**：v0.4.1（Draft）
- **日期**：2026-08-05
- **对应 PRD**：v0.1.4
- **状态**：评审中

---

## 1. 设计范围

### 1.1 本文要解决的问题

本文需要把 PRD 中的产品承诺落实为可以编码和验收的系统边界：

1. 同一套 SQL 和 DataFrame 逻辑如何同时用于有界数据与无界数据；
2. `IMAGE` 如何在不复制大量像素的前提下穿过列式计划；
3. 模型调用如何成为优化器可见、运行时可调度的计划节点；
4. 事件时间、水位线、窗口状态和源进度如何在过滤、异步推理与失败恢复后仍然正确；
5. 库态、服务态和未来集群态如何复用同一内核；
6. Workbench 如何只通过公开 SQL 与 Arrow Flight SQL 使用引擎。

### 1.2 版本边界

| 范围 | 本文详细设计 | 本文只固定接口 | 本文不设计 |
|---|---|---|---|
| v0.1 | 类型系统、图片和视频表、`FRAMES`、单路 RTSP、`TUMBLE`、检测与嵌入模型、Python UDF、Kafka/Lance/Parquet/Console Sink、shell、DataFrame API、采样下推、`vql-server`/`visionqld` 和基础 Flight SQL | 持久查询描述、检查点接口和 v0.2 服务增强所需的协议扩展点 | 集群调度、多租户、精确一次 |
| v0.2 | — | Kafka 帧源与至少一次、持续查询管理与恢复、TLS/认证/权限、Workbench 所需的媒体和系统查询契约 | 分布式执行 |
| v0.3+ | — | 级联、结果物化、向量索引、VLM、成本估算的计划扩展点 | 具体优化策略与产品定价 |

下列语法即使可以被解析，也必须在未到对应版本时返回明确的 `FEATURE_NOT_AVAILABLE`，不能只写入目录后假装可用：

- v0.2：`TRACK`、`HOP`、`SESSION`、物化视图维护、Kafka 帧源；
- v0.3：`VQA`、向量索引、模型级联和成本预估；
- v1.0：`resource_group`、精确一次、WASM UDF 与多租户治理。

### 1.3 非目标

- 不修改或 fork DataFusion 内核；
- 不自研通用 SQL 执行引擎、视频存储格式或模型服务平台；
- 不把批处理和流处理强行塞进同一套物理算子。批流一体指语言、类型、目录和逻辑计划一致，物理运行时可以根据边界性采用不同实现；
- 不在 v0.1 实现跨查询共享解码、模型结果缓存、状态检查点、持久作业恢复或多用户安全边界；
- 不把训练、标注、VMS、流媒体转发或行业应用放进引擎。

### 1.4 关键术语

| 术语 | 含义 |
|---|---|
| 有界查询 | 输入最终结束，可以由普通 DataFusion 物理计划完整求值的查询 |
| 持续查询 | 至少包含一个无界源，需要长期运行的查询 |
| 执行周期（epoch） | 流源在一个短时间段内产生的一组 RecordBatch，以及与这组数据对应的水位线、源进度和资源租约 |
| 数据片段 | 在一个 epoch 内执行的有界 DataFusion 计划，不携带水位线等控制消息 |
| 作业协调器 | 顺序驱动 epoch、窗口状态、Sink 确认和检查点的流运行时组件 |
| 媒体引用 | 指向图片或视频帧的逻辑定位信息，不包含解码后的像素 |
| 帧仓 | 只在当前进程、当前 epoch 内有效的解码帧 arena |
| 定义快照 | 查询规划时解析并固定的表、模型、函数和 Sink 修订版本 |

---

## 2. 从 PRD 派生的设计约束

| 编号 | PRD 承诺 | 设计约束 |
|---|---|---|
| G1 | 批流共享 SQL / DataFrame 语义 | 只维护一套 `VqlLogicalPlan`；边界性在分析阶段推导，物理编译阶段再分为批计划和流作业图 |
| G2 | `pip install` 后无需服务即可使用 | 内核不得监听端口或依赖外部元数据服务；本地目录使用 SQLite；CLI 和 Python 都嵌入同一内核 |
| G3 | 模型调用可优化 | `USING MODEL` 函数必须在规划期提取为显式 `Inference` 节点，不能作为普通逐行 UDF 执行 |
| G4 | 大图像不能在算子间反复复制 | `IMAGE` 默认保存引用；像素只存在于有界帧仓、张量缓冲或明确的 IPC/落盘边界 |
| G5 | 流处理有事件时间与明确投递语义 | 水位线、源 offset 和帧租约属于 epoch 控制面，不编码成可能被 Filter 丢弃的普通行 |
| G6 | 错误行默认不终止整个查询 | 解码或推理失败时保留输入行，把对应结果列置为 NULL，并记录结构化错误指标；严格模式才失败 |
| G7 | 服务态和客户端使用公开协议 | `vql-server` 构建的 `visionqld` 只暴露 Flight SQL、SQL 系统语句和健康检查，v0.2 再增加 Prometheus；Workbench 不依赖根目录引擎 crate 或私有管理 API |
| G8 | 后续扩展不能破坏 MVP 主线 | 模型类型、processor、推理后端、源、Sink 和逻辑节点都通过窄 trait 或注册表扩展；未实现功能直接拒绝 |

所有实现还必须遵守五条原则：

1. **先保证语义，再追求复用。** 原生算子无法传递控制信息时，不把水位线伪装成数据列，也不假设过滤后的行仍能代表源进度。
2. **先减少工作量，再加速单次操作。** 优化顺序是列裁剪、时间裁剪、显式采样、推理去重、批量推理，最后才是硬件特化。
3. **所有队列都有上限。** 媒体、推理、窗口和 Sink 缓冲都必须纳入查询资源预算；无界队列视为实现错误。
4. **查询固定定义，DDL 创建新修订。** 正在运行的查询不因 `ALTER MODEL` 或 `ALTER FUNCTION` 在中途悄悄改变结果。
5. **版本范围按能力生效。** 暂未消费的参数不允许静默保存。

---

## 3. 总体架构

### 3.1 分层结构

```mermaid
flowchart TB
    subgraph HOSTS[宿主]
        PY[Python / DataFrame]
        CLI[visionql shell / run]
        DAEMON[visionqld v0.1]
    end

    subgraph CORE[引擎内核]
        ENTRY[Engine / Session API]
        SQL[VQL 解析与语义分析]
        CAT[Catalog 与定义快照]
        PLAN[VqlLogicalPlan 与优化器]
        BCOMP[批计划编译器]
        SCOMP[流作业编译器]
    end

    subgraph EXECUTION[执行]
        DF[DataFusion 有界执行]
        COORD[流作业协调器]
        STATE[TUMBLE 状态]
        CHECKPOINT[检查点 v0.2]
    end

    subgraph RUNTIME[运行时服务]
        MEDIA[媒体读取 / 解码 / 帧仓]
        MODELS[模型加载 / batching / 推理]
        CONNECTORS[表、流与 Sink 连接器]
        BUDGET[内存与资源预算]
        METRICS[指标与结构化日志]
    end

    PY --> ENTRY
    CLI --> ENTRY
    DAEMON --> ENTRY
    ENTRY --> SQL
    SQL <--> CAT
    SQL --> PLAN
    PLAN --> BCOMP --> DF
    PLAN --> SCOMP --> COORD
    COORD --> DF
    COORD --> STATE
    COORD -. v0.2 .-> CHECKPOINT
    DF --> MEDIA & MODELS & CONNECTORS
    COORD --> CONNECTORS
    MEDIA & MODELS & CONNECTORS & STATE --> BUDGET
    MEDIA & MODELS & CONNECTORS & COORD --> METRICS
```

### 3.2 组件职责

| 组件 | 职责 | 不负责 |
|---|---|---|
| `Engine` / `Session` | 组装目录、规划器、运行时和配置；提供 SQL/DataFrame 执行入口 | 进程信号、端口、用户认证 |
| VQL 前端 | 切分语句、解析 VQL DDL、规范化语法糖、生成统一逻辑计划 | 执行 DDL 之外的 I/O |
| Catalog | 对象修订、依赖、schema、模型哈希和作业定义的事务持久化 | 保存视频、权重字节或用户明文凭证 |
| 规划器 | 类型检查、边界性和可重放性推导、函数解析、推理提取、streamability 校验 | GPU 放置、模型加载 |
| 批计划编译器 | 将有界逻辑计划降为 DataFusion `ExecutionPlan` | 水位线与恢复 |
| 流作业编译器 | 将持续查询切成源、一个或多个有界数据片段、状态算子和 Sink | 自己实现表达式计算 |
| 作业协调器 | 驱动 epoch，按序推进控制面，管理状态、取消、Sink 确认和恢复 | 解释 SQL 表达式 |
| 媒体运行时 | 探测、读取、解码、采样、帧仓和编码 | 模型前后处理 |
| 模型运行时 | 权重解析、processor、设备会话、批量调度和推理 | SQL 语义与目录权限 |
| 连接器 | 读取图片、视频、RTSP、Kafka，以及写入 Kafka/Lance/Parquet | 改写查询计划 |

### 3.3 两条执行路径

| 阶段 | 有界查询 | 持续查询 |
|---|---|---|
| 解析与分析 | 同一套 VQL AST、Catalog 和 `VqlLogicalPlan` | 同左 |
| 优化 | 同一套列裁剪、谓词下推、推理提取和显式采样下推 | 同左，另外执行 streamability 校验 |
| 物理编译 | 完整降为一个 DataFusion 计划 | 切成 `Source → EpochTransform → StatefulOp → Sink` 作业图；每个 `EpochTransform` 是有界 DataFusion 片段 |
| 控制信息 | 不需要水位线；输入结束即完成 | 由协调器在 epoch 边界传递，不进入 RecordBatch |
| 结束条件 | 所有分区耗尽 | 用户停止、不可恢复错误或服务管理操作 |

这一区分是本设计最重要的选择。DataFusion 的 `ExecutionPlan::execute` 输出 `RecordBatch` 流，适合增量计算数据，但原生 Filter、Projection 等算子没有水位线或源进度通道。VisionQL 因此复用它的 SQL、表达式、优化器与有界执行能力，不要求原生算子承担它们没有声明过的控制语义。

---

## 4. 统一逻辑计划与物理编译

### 4.1 `VqlLogicalPlan`

标准关系节点尽量复用 DataFusion `LogicalPlan`。视觉或流语义无法由标准节点完整表达时，使用扩展节点：

| 扩展节点 | 输入与输出 | v0.1 物理实现 |
|---|---|---|
| `Frames` | 视频表 → 帧引用表，透传原表列 | `FramesExec` |
| `Inference` | 输入关系 → 追加模型结果列 | `InferenceExec` |
| `TumbleAggregate` | 带事件时间的关系 → 窗口聚合结果 | 有界：`date_bin + AggregateExec`；无界：`TumbleState` |
| `SinkWrite` | 输入关系 → 写入目录中的 Sink | `SinkExec` / 流 Sink 驱动器 |
| `Track` | 帧关系 → 带 `track_id` 的关系 | v0.2，v0.1 拒绝 |

每个计划节点都携带或推导以下属性：

```text
PlanProperties {
  boundedness: Bounded | Unbounded,
  replayability: Replayable | Live,
  event_time: None | { column, watermark_delay },
  ordering: ...,
  image_access: MetadataOnly | Encoded | Pixels,
  definition_snapshot: CatalogGeneration,
}
```

### 4.2 规划流水线

```text
SQL / DataFrame
  → 语法归一化
  → 名称、类型与目录修订解析
  → VqlLogicalPlan
  → 边界性、事件时间、可重放性分析
  → 推理调用提取与公共表达式消除
  → 列裁剪、谓词和显式采样下推
  → streamability 校验
  → 批计划或流作业图
  → 物理计划校验
  → 执行
```

规划阶段只读取目录和轻量元数据。对象存储扫描、模型下载、视频探测和网络连接都发生在执行阶段，避免 `EXPLAIN` 或补全触发昂贵 I/O。

### 4.3 定义快照

目录对象采用稳定 ID、不可变修订和可变 head：

- `CREATE` 生成第一版；`ALTER` 或 `CREATE OR REPLACE` 生成新修订；
- Function 的 `USING MODEL` 绑定稳定 `model_id`，不直接保存某个 Model revision。规划器在同一个 Catalog 读事务中先解析 Function revision，再解析该 `model_id` 当时的 head revision；
- 规划时把表、函数、解析后的模型和 Sink revision ID，以及模型语义指纹和绑定参数写进计划；模型语义指纹至少包含 artifact hash 或声明的 immutable endpoint revision/config hash、processor ID/版本、precision、backend kind/版本和模型声明的输出 schema；
- 批查询在执行期间固定该快照；持续查询在整个运行周期固定该快照；
- `ALTER MODEL` 生成新的 Model revision 并推进该 `model_id` 的 head，只影响之后新规划的查询；`ALTER FUNCTION ... SET MODEL` 生成新的 Function revision 并改绑另一个 `model_id`，同样只影响之后新规划的查询；
- 已 prepare 的 statement、正在运行的批查询和持续查询都不自动 replan。普通 prepared statement 需要关闭后重新 prepare；附着式持续查询需要取消后重新执行，持久作业需要停止旧作业并显式提交新作业；
- 删除对象只把名称 head 标记为 tombstone。活动查询、检查点或未过期媒体定位符持有 revision lease 时不得物理回收；权限撤销立即生效，不因 lease 继续授权；
- `EXPLAIN`、`SHOW QUERIES` 和错误日志都显示所使用的修订，保证结果可追溯。

### 4.4 无界查询白名单

v0.1 对无界计划采用白名单，而不是猜测任意 DataFusion 计划能否持续运行。

允许：

- 单个 RTSP 源；
- Projection、Filter、`UNNEST`、内置标量函数与 `Inference`；
- 一个 `TUMBLE` 聚合；
- 无状态 SELECT 预览或一个 Sink；
- 窗口后的 Projection、Filter 和 Sink。

拒绝：

| 计划形状 | 原因 | 提示 |
|---|---|---|
| 无窗口全局或分组聚合 | 输入永不结束 | 增加 `TUMBLE` |
| 无界 `ORDER BY` / TopK | 需要无限状态或等待结束 | 先限定窗口或改为批查询 |
| 无界 `DISTINCT` | 状态无法回收 | 改用 v0.1 白名单聚合；确需窗口去重时等待 v0.2 的受限 `COUNT(DISTINCT primitive)`，或改为批查询 |
| JOIN、UNION 多源 | v0.1 尚未定义多源水位线与一致性 | 等待对应版本或拆为独立查询 |
| `OVER` 分析窗口 | v0.1 没有有界状态规则 | 改为时间窗口聚合 |
| `TRACK`、`HOP`、`SESSION` | 属于 v0.2 | 返回明确版本信息 |
| 不在窗口聚合白名单中的 aggregate / UDAF | 无法保证内存、帧生命周期或检查点状态可恢复 | 改用受支持聚合或批查询 |

v0.1 的流式 `TUMBLE` 只允许 `COUNT`、`SUM`、`AVG`、`MIN` 和 `MAX`，参数与 group key 必须使用可持久化的标量 Arrow 类型。`COUNT(DISTINCT primitive)` 到 v0.2 在通过状态恢复与内存上限测试后单独启用。`ARRAY_AGG`、`STRING_AGG`、近似聚合、ordered aggregate、UDAF，以及对 `IMAGE`、`VIDEO`、Binary 或包含进程内媒体槽位的复杂类型做聚合，均不进入 v0.1 白名单。

校验错误必须指出第一个不支持的节点或聚合、所在 SQL 片段和可行改写，不能只返回 DataFusion 内部错误。

---

## 5. 流执行模型

### 5.1 为什么采用 epoch

流数据以短周期微批进入引擎，默认周期为 100～250ms，可由运行时根据输入率调整。每个 epoch 同时携带数据和独立控制信息：

```rust
struct StreamEpoch {
    epoch_id: u64,
    batches: Vec<RecordBatch>,
    source_progress: SourceProgress,
    watermark: Option<Timestamp>,
    frame_lease: Option<FrameArenaLease>,
}
```

`batches` 可以为空。`source_progress`、`watermark` 和 `frame_lease` 不会变成 RecordBatch 中的隐藏列。Filter 即使把整个 epoch 的行全部过滤掉，协调器仍然能够推进源进度、释放帧仓并处理水位线。

### 5.2 一个 epoch 的执行顺序

```mermaid
sequenceDiagram
    participant S as 流源
    participant C as 作业协调器
    participant D as DataFusion 数据片段
    participant W as TUMBLE 状态
    participant K as Sink

    S->>C: StreamEpoch(data, progress, watermark, frame lease)
    C->>D: 绑定 EpochInputExec 并执行有界片段
    D-->>C: 过滤/推理后的 RecordBatch
    C->>W: 应用本 epoch 的数据
    C->>W: 在数据完成后推进 watermark
    W-->>C: 已关闭窗口结果
    C->>K: 写出并等待确认
    K-->>C: ack
    C->>C: 标记 epoch 完成；v0.2 进入检查点协议
    C->>S: 释放 frame lease / 更新可提交进度
```

严格顺序如下：

1. 同一源的 epoch 按 `epoch_id` 串行应用；数据片段内部仍可并行解码、前处理和推理；
2. 只有当前 epoch 的所有数据输出完成后，协调器才把它的水位线交给状态算子；
3. 只有状态更新和所有新关闭窗口的 Sink 写入完成后，epoch 才算完成；
4. 取消查询会取消当前 DataFusion stream、模型请求和 Sink 请求，再释放帧租约；
5. v0.1 只有单源单分区，因此不需要合并水位线。v0.2 若启用多 Kafka 分区，作业水位线取所有非空闲分区水位线的最小值。

数据片段的**计划模板**在作业启动时只编译一次，但同一棵 `ExecutionPlan` 实例绝不能跨 epoch 直接重复 `execute()`。模板只保存 schema、表达式、operator ID、分区要求和扩展算子 factory，不保存 channel、动态过滤器、metrics recorder、输入槽或其他运行态：

```rust
trait EpochPlanTemplate {
    fn instantiate(
        &self,
        binding: EpochBinding,
        task_ctx: Arc<TaskContext>,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

struct EpochBinding {
    job_id: QueryId,
    epoch_id: u64,
    batches: Vec<RecordBatch>,
    frame_lease: Option<FrameArenaLease>,
    cancel: CancellationToken,
}
```

每个 epoch 都从模板实例化一棵新的执行树和新的 `EpochInputExec`，并创建新的子 `TaskContext`；它们共享作业级 RuntimeEnv、MemoryPool、模型运行时和指标汇聚器，但不共享算子运行态。DataFusion 适配层可以用当前锁定版本提供的递归 `reset_state` / `with_new_state` 实现 factory，外部不变量始终是“执行实例不复用”。当前实例的所有输出分区必须全部结束或完成取消、后台任务全部 join、资源 reservation 全部释放后，协调器才能实例化下一个 epoch。状态算子位于片段之外，不能被误放进每次都会重建的 DataFusion 执行状态。

### 5.3 `TUMBLE` 状态

流模式下，`TumbleState` 保存 VisionQL 自己拥有的规范化 Arrow 状态，而不是把任意 DataFusion `Accumulator` 对象长期放进作业状态：

```text
key = (window_start, group_key)
value = versioned_arrow_states + source_progress_span
```

- 窗口边界为半开区间 `[start, end)`；时间统一转为 UTC 纳秒，v0.1 以 Unix epoch 为固定窗口原点；
- v0.1 的 interval 必须是正的固定时长，不接受月、季度等日历间隔；无界查询的事件时间表达式必须是非空 TIMESTAMP，nullable 列需要先显式过滤 NULL；
- 收到数据时先按事件时间归入窗口。每个白名单聚合由 `WindowStateCodec` 描述输入类型、state schema、更新、求值、内存大小和恢复方式；
- DataFusion accumulator 只作为一次状态转换的临时对象：从当前规范化 Arrow state 通过 `merge_batch` 恢复，应用本 epoch 数据后调用一次 `state()` 取得新状态，随后丢弃。检查点复制规范化 state，不对仍在运行的 accumulator 调用破坏性快照；
- 应用完 epoch 数据后再处理 watermark；当 `window_end <= watermark` 时输出并删除窗口；
- `event_time < current_watermark` 的行是迟到数据，v0.1 默认丢弃并增加 `late_rows_total`；`allowed_lateness` 不在 v0.1；
- 停止查询时不输出尚未关闭的窗口，避免把部分窗口伪装成完整结果；
- state schema 由 `(aggregate_kind, input_types, state_codec_version)` 决定并纳入 fingerprint；每次更新后按 `size()` 调整查询 reservation，无法记账的 codec 不得注册；
- 任何 state 或 group key 都不能包含 `arena_id/arena_slot`。需要保留画面时必须先转换为持久媒体定位符或编码态值；v0.1 默认拒绝把 `IMAGE/VIDEO` 放入窗口状态；
- 批模式把同一个 `TUMBLE` 降为普通时间分桶和聚合。每个进入流白名单的聚合都必须用相同输入做批/流差分测试，确认 NULL、分组、溢出和最终值一致。

`WindowStateCodec` 是引擎拥有的恢复 ABI。DataFusion 升级不能在没有迁移或重放方案的情况下改变已发布 codec；新增聚合必须先提供确定性的 state schema、非破坏性 checkpoint、restore round-trip 和资源记账测试。

### 5.4 RTSP 事件时间与断流

- 作业启动时建立一个 `IngestClock`：记录一次 UTC 系统时间与 monotonic clock 锚点，之后用 monotonic elapsed 生成 UTC ingest time。重连不重置该时钟，因此进程内 NTP/系统时钟回拨不会让 ingest time 倒退；
- 每次初连、成功重连、codec/timebase 改变或 RTP/RTCP 映射失效都会开启新的 `source_generation`。Stream 显式选择 `ingest_time` 时跳过探测；选择 `capture_time` 时在有限的 `timestamp_probe_timeout` 内验证 RTP/RTCP 映射的单调性、漂移以及它与 `IngestClock` 的差值。验证成功后本 generation 固定使用 `capture_time`，否则固定使用 `ingest_time`，运行中不在两种时钟之间无 barrier 切换；
- capture 映射在 generation 中途失效、倒退或相对 `IngestClock` 漂移超过阈值时，结束该 generation，并以 `ingest_time` 开启下一 generation，同时记录 `event_time_fallback_total` 和不连续原因；
- 水位线为 `max_seen_event_time - watermark_delay`，单调不回退；`max_seen_event_time` 来自所有成功取得时间戳的源帧，而不是只来自采样后保留的行。当前 epoch 的采样数据处理完成后才能应用同一 epoch 的水位线；
- 断流后指数退避重连，默认 1s 起、最大 30s；断流期间水位线冻结，不用本地时钟伪造源进度；
- 新 generation 的候选事件时间必须先通过连续性门：如果 capture time 小于当前 watermark 或相对 `IngestClock` 异常偏移，拒绝该映射并让该 generation 使用 ingest time。进程恢复后需要新建 `IngestClock`；若它的第一个 ingest time 小于已恢复 watermark，作业进入 `state=FAILED, error_code=TIME_DISCONTINUITY`，不能把后续帧无限判为迟到或暗中把时钟钳到 watermark；
- 重连后的 ingest time 正常包含真实断流时长。它向前推进 watermark、关闭已越过的窗口并记录 gap，但不会为缺失时段补造行；只要 capture time 与同一时刻的 ingest time 在容差内，也可以继续使用 capture time，不能仅因它相对断流前最后一帧前跳就判错；
- 通过连续性门的新帧继续推进原作业 watermark；之前带数据但尚未关闭的窗口随后正常输出，断流时段不会补造行；
- 如果流始终没有恢复，最后一个未关闭窗口不会输出。查询运行状态仍为 `RUNNING`，另以 `source_health=DISCONNECTED` 和指标明确显示缺口时长与最后事件时间；
- RTSP 是不可重放源。进程崩溃、主动丢帧或暂停造成的数据永远不能恢复，因此 v0.1 只承诺尽力而为。

### 5.5 背压与丢帧

背压沿 `Sink → 状态 → 数据片段 → 源缓冲` 反向传递。所有缓冲都有容量：

- 批输入和 Kafka 输入在容量不足时等待；
- RTSP 无法让摄像头回放历史数据。缓冲达到上限时，只在最靠近源的位置丢弃尚未进入 epoch 的最旧采样帧；
- 已经进入 epoch 的行不会因为超载被静默丢弃；如果无法在预算内执行，查询失败；
- 每次丢弃记录源、原因、帧数和事件时间范围。`SHOW METRICS` 至少区分 `source_overrun`、`decode_slow`、`inference_backlog` 与 `sink_backlog`；
- `on_overload = 'fail'` 可将 live 丢帧改为失败，便于对完整性要求更高的测试环境使用。

### 5.6 v0.2 Kafka 至少一次协议

Kafka 源可重放，因此 v0.2 为它提供至少一次。检查点必须把源进度和窗口状态放进同一个恢复边界，不能在只确认 Sink 后立即提交高位 offset。

`FORMAT FRAME_JPEG` 把消息 payload 保存为编码态 `IMAGE`，事件时间、source 和 frame ID 按 Stream DDL 中声明的字段/header 映射读取；解码仍延迟到像素消费者。topic、partition、offset 和 leader epoch 只进入 `SourceProgress`，不作为可能被关系算子删除的业务列。

协调器按时间或状态增量选择一个已完成 epoch 作为检查点边界；边界之间的 Kafka offset 不提交，崩溃后允许重放。每个检查点边界使用以下协议：

1. 从当前检查点恢复的状态开始，应用 epoch 数据并生成应输出的关闭窗口；
2. 将输出写入 Sink，等待所有写入确认；
3. 原子持久化新的检查点，其中包含逻辑计划哈希、目录定义快照、各分区下一 offset、水位线、规范化窗口状态、各 `WindowStateCodec` 版本和 Sink delivery sequence；
4. 检查点落盘成功后，才异步提交 Kafka consumer offset；
5. 崩溃恢复时以本地检查点为准显式 seek，不依赖可能滞后的 broker commit。

检查点直接写入 §5.3 的规范化 Arrow state，并记录 operator ID、state schema fingerprint、codec version 和 engine state-format version；checkpoint 不调用活动 accumulator 的 `state()`。恢复时这些字段必须与定义快照匹配；不兼容时不能勉强反序列化，作业进入 `state=FAILED, error_code=RECOVERY_INCOMPATIBLE`，由升级工具或仍在保留期内的 Kafka 数据重新构建。

崩溃点的结果：

| 崩溃位置 | 恢复行为 |
|---|---|
| Sink 确认前 | 从旧检查点重放；已经被 Sink 部分接收的记录可能重复 |
| Sink 已确认、检查点未提交 | 从旧检查点重放；已确认记录会重复 |
| 检查点已提交、Kafka offset 未提交 | 从新检查点的 offset 继续，不丢失窗口状态 |
| Kafka offset 已提交 | 从同一或更新的检查点继续 |

因此不会丢失已进入检查点边界的数据，但 Sink 可能收到重复结果，语义正好是至少一次。v1.0 的精确一次需要把检查点与事务 Sink 的 commit 放进同一 barrier，不由本协议冒充。

### 5.7 v0.2 持续查询状态机

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
- 服务进程启动时恢复 `RUNNING` 或 `RECOVERING` 作业。RTSP 从 live 位置继续，Kafka 从检查点继续。

---

## 6. 多模态类型与媒体生命周期

### 6.1 Arrow 物理表示

VQL 类型名是逻辑类型。底层全部使用标准 Arrow storage type，并用字段元数据标注扩展语义。

| VQL 类型 | Arrow storage type | 约定 |
|---|---|---|
| `IMAGE` | `Struct`，见 §6.2 | `ARROW:extension:name=visionql.image` |
| `VIDEO` | `Struct<uri, locator, duration_ns, fps, width, height, codec>` | `uri` 只展示，`locator` 用于重新授权后的读取；永远不内联完整视频 |
| `BOX2D` | `Struct<x: Float32, y: Float32, w: Float32, h: Float32>` | 左上原点，归一化坐标 `[0,1]` |
| `VECTOR(n)` | `FixedSizeList<Float32, n>` | 维度属于类型，规划期检查 |
| `POINT2D` | `Struct<x: Float32, y: Float32>` | 空间函数的内部逻辑类型 |
| `POLYGON` | `List<POINT2D>` | v0.1 只支持归一化二维多边形 |
| 检测结果 | `List<Struct<label: Utf8, confidence: Float32, box: BOX2D>>` | 一帧对应一个数组；`UNNEST` 负责展开 |
| `AUDIO` / `MASK` | 保留逻辑类型 | v0.1 注册和执行都返回未支持错误 |

字段 metadata 固定包含 `ARROW:extension:name=visionql.image` 和 `ARROW:extension:metadata={"version":1}`；SqlInfo `visionql_image_version` 对应返回字符串 `1`。未知扩展类型的 Arrow 客户端仍能按其标准 storage type 读取，避免把协议绑定在 VisionQL 私有内存结构上。

### 6.2 `IMAGE` 的三种载荷

```text
IMAGE storage := Struct {
  uri: Utf8?,                 # 只用于展示的脱敏 URI，不参与读取或授权
  locator: Utf8?,             # 版本化 vql:// 媒体定位符
  pts_ms: Int64?,             # 视频帧时间；图片为 NULL
  frame_id: UInt64?,          # live 环形缓存中的帧标识
  encoded: Binary?,           # JPEG/PNG 或缩略图字节
  encoding: Utf8?,
  width: Int32?,
  height: Int32?,
  arena_id: UInt64?,          # 仅进程内
  arena_slot: UInt32?         # 仅进程内
}
```

同一值可以处于三种载荷形态：

| 形态 | 有效字段 | 使用位置 |
|---|---|---|
| 引用态 | `uri`、`locator`、`pts_ms`、元数据；`locator` 必须非 NULL | 表扫描、`FRAMES` 和绝大多数算子间传递 |
| 帧仓态 | `arena_id`、`arena_slot`、元数据 | 当前 epoch 内，解码点到像素消费者之间 |
| 编码态 | `encoded`、`encoding`、元数据 | Python 边界、Flight SQL、Kafka 显式输出、Lance/Parquet 落盘 |

不变量：

1. `arena_id` 和 `arena_slot` 绝不能跨进程、落盘或进入 Catalog；
2. `uri` 必须去掉账号、签名查询串和其他秘密，只用于 SQL 展示、日志和导出；运行时绝不能用它反查 Catalog 或直接发起 I/O；
3. `locator` 是 `vql://media/v1/...` 版本化不透明值，载荷至少绑定 `source_id`、`source_revision_id`、规范化 object key 或 stream generation、media version，以及适用时的 PTS/frame ID。它可以带完整性校验，但安全边界仍是服务端重新授权和路径范围校验；
4. 服务端只解析 `locator` 指向的已注册来源。解析时按当前 principal 重新检查对象权限，从对应 source revision 取得凭证引用，并验证 object key 仍在登记前缀内；协议错误码固定为 `INVALID_MEDIA_LOCATOR`、`MEDIA_LOCATOR_EXPIRED`、`PERMISSION_DENIED`、`SOURCE_REVISION_UNAVAILABLE` 和 `FRAME_NOT_AVAILABLE`，客户端不得解析错误文本；
5. live RTSP 帧没有可重放的长期引用。它的 `locator` 包含 stream generation 与 frame ID；服务态只在有界环形缓存中按该定位符提供短期点查，过期后返回 `FRAME_NOT_AVAILABLE`；
6. `encoded` 表示原图还是缩略图由字段元数据和会话 `image_mode` 明确标记，客户端不能靠尺寸猜测。

v0.2 的 live 点查不长期保留 6MB 级原始像素。RTSP connector 在启用媒体预览时保存一个受总字节数和 TTL 限制的压缩 packet/GOP ring；`FRAME_AT` 从目标帧之前最近的关键帧开始解码。缓存按最旧 GOP 淘汰，查询取消不会延长 TTL，缓存未命中时不尝试向 live 摄像头“回放”历史。

### 6.3 epoch 帧仓

RTSP 解码后把采样帧放入当前 epoch 的 `FrameArena`，RecordBatch 只保存槽位。协调器持有 `FrameArenaLease`，直到该 epoch 的数据片段、状态处理，以及 Sink/Flight 等出口所需的编码全部结束才整体释放 arena。

这种生命周期不依赖每一行都到达下游：

- Filter 丢弃单行或整批不会泄漏帧；
- 异步推理结束前 lease 不会释放；
- 查询取消会先取消使用者，再释放整个 epoch；
- v0.1 一个查询不跨 epoch 保存帧仓引用。窗口状态只能保存白名单标量；后续版本若允许保存媒体，只能保存可重新授权的 `locator` 或编码态值，不能保存 `arena_slot`。

批视频通常不需要帧仓。`InferenceExec` 可以把“读取 → 解码 → 前处理”融合在一个算子内；只有同一帧在一个计划中被多个像素消费者使用时，才为当前批建立短生命周期 arena。

### 6.4 NULL 与单行错误

- 解码失败：`IMAGE` 像素不可用，依赖像素的结果列为 NULL；引用和其他元数据仍保留；
- 推理失败：模型结果列为 NULL，输入列照常输出；
- `COUNT_OBJECTS(NULL, ...)` 返回 NULL，不把错误误算为 0；用户需要 0 时显式 `COALESCE`；
- 默认连续 5 分钟失败率超过配置阈值时告警，但不改变结果；
- `SET vql.on_error = 'fail'` 使第一次行级错误终止查询。

---

## 7. SQL、目录与函数

### 7.1 解析边界

VQL 使用 sqlparser-rs 的 tokenizer 和标准 SQL AST，但由自己的语句入口处理新增 DDL 与表值语法：

1. 先按字符串、注释和引用规则切分完整脚本；
2. `CREATE STREAM/MODEL/FUNCTION/SINK`、`ALTER`、`SHOW`、`SUBMIT QUERY`、`PAUSE/RESUME/STOP` 进入 VQL DDL parser；
3. SELECT、INSERT 和标准 DDL 进入标准 SQL parser；
4. `<->`、`.center`、`FRAMES(TABLE ...)`、`TUMBLE` 等在 AST/逻辑计划层规范化；
5. 规范化后的关系表达式交给 DataFusion 规划接口。

不能只通过 `Dialect` 钩子假设 sqlparser-rs 会自动支持所有新增 Statement；VQL parser 必须有自己的金样测试。

v0.1 未加引号的标识符按小写解析，双引号标识符保留原样；字符串只使用单引号。新增 DDL 的状态如下：

| 语句 | v0.1 行为 |
|---|---|
| `CREATE TABLE ... USING IMAGES/VIDEOS` | 创建外部图片或视频表 |
| `CREATE TABLE ... AS SELECT` | 写入 Lance/Parquet 表，目标格式必须明确或可从 location 推导 |
| `CREATE STREAM ... FROM 'rtsp://...'` | 创建单路 RTSP 流 |
| `CREATE STREAM ... FROM 'kafka://...'` | 解析后返回“v0.2 支持” |
| `CREATE MODEL ... [FUNCTION f]` | 创建模型；可在同一事务中派生一个函数 |
| `ALTER MODEL ...` | 创建新 Model 修订并推进稳定 `model_id` 的 head，不修改已规划查询 |
| `CREATE FUNCTION ...` | 支持 Model、Python 与 SQL 宏三种实现 |
| `ALTER FUNCTION ... SET MODEL` | 创建新 Function 修订，不修改运行中查询 |
| `CREATE SINK ...` | 创建 Kafka、Lance、Parquet 或 Console Sink |
| `CREATE MATERIALIZED VIEW ...` | 解析后返回“v0.2 支持”，不登记空对象 |
| `CREATE INDEX ... USING HNSW` | 解析后返回“v0.3 支持”，不登记空对象 |
| `SUBMIT QUERY name AS INSERT INTO ... SELECT ...` | v0.2 持久作业语法；v0.1 返回版本明确的未支持错误 |

对应对象的 `DROP`、`SHOW`、`DESCRIBE` 与 `SHOW CREATE` 走相同 VQL DDL 路径；`SHOW CREATE` 必须输出脱敏且可再次解析的定义。

### 7.2 目录对象

SQLite 是 v0.1 的默认目录，默认位置为平台用户数据目录下的 `visionql/catalog.db`。核心对象如下：

| 对象 | 关键内容 |
|---|---|
| Table | provider、location、options、Arrow schema、修订、凭证引用 |
| Stream | connector、endpoint（脱敏）、fps、事件时间、水位线、修订 |
| Model | type、不可变来源 revision、内容哈希、processor、precision、backend、输出 schema、声明式约束 |
| Function | 签名、实现种类、稳定 `model_id` 或代码入口、绑定参数、确定性 |
| Sink | connector、format、options、凭证引用 |
| QueryJob（v0.2） | 名称、SQL、定义快照、状态、检查点位置、owner |

约束：

- 每条 DDL 在一个 SQLite 事务中提交；`CREATE MODEL ... FUNCTION f` 同事务创建两个对象；
- 对象之间使用稳定 ID 和修订 ID，不使用可变名称作为内部外键；
- Function 通过稳定 `model_id` 引用 Model；QueryJob、计划和检查点引用不可变 revision ID。不能在同一字段中混用“跟随 head”和“固定 revision”两种语义；
- Table、Stream 和 View 共享 relation 名称空间；Model、Function 与 Sink 分别使用独立名称空间。未加引号的名称按 §7.1 的小写规则唯一；
- schema 使用 Arrow IPC schema 编码；Catalog 自身保存格式版本和迁移记录；
- 密码、token、S3 secret 和带签名 URL 不写入目录；只保存环境变量、文件或 secret provider 的引用；
- DROP 创建 tombstone 并阻止新规划；运行中查询的 revision lease 保证定义和凭证元数据在查询结束前不会被物理 GC。权限判定不进入 lease，撤权后新的媒体读取和恢复仍会失败；
- `SUBMIT QUERY` 的名称在 owner 范围内对非终态作业唯一且创建后不可变；状态变更只使用服务端 UUID，名称只用于展示和确认。

### 7.3 MODEL 与 FUNCTION

MODEL 是资源实现，FUNCTION 是查询接口。Function revision 保存稳定 `model_id`；规划时在同一 Catalog 快照中把它解析为当时的 Model head revision，并把该 revision 与 Function 语义参数一起固定到计划。

v0.1 模型类型：

| TYPE | 标准签名 | 状态 |
|---|---|---|
| `OBJECT_DETECTION` | `(IMAGE) -> ARRAY<STRUCT<label, confidence, box>>` | 支持 |
| `EMBEDDING` | `(IMAGE) -> VECTOR(n)` 或 `(STRING) -> VECTOR(n)` | 支持；入口决定输入模态 |
| `VQA` | `(IMAGE, STRING) -> STRING` | v0.3；v0.1 拒绝注册 |

`VECTOR(n)` 的维度必须在 Function 创建时确定。显式 `RETURNS VECTOR(n)` 优先；省略时从已解析的模型 manifest 推导；两处冲突或都无法确定时 DDL 失败，不能把未知维度拖到首批数据执行时才报错。

参数归属按“查询接口、模型实现、运行时部署”三层执行。权重、processor 和 precision 虽然属于 Model 管理，但可能改变结果，因此必须进入模型语义指纹和定义快照，不能被当作纯成本参数：

- Function 保存签名、稳定 `model_id`、`classes`、`min_confidence`、NMS 阈值、prompt 模板和 determinism 等查询接口语义；
- Model revision 保存固定 artifact revision/hash、processor ID/版本、label/schema、precision、backend 与 `latency_slo` 等实现定义；`ALTER MODEL` 改变这些内容时创建新 revision；
- device、replica、动态 batch 和队列权重属于运行时部署配置，不进入 Function，也不改变 Model semantic fingerprint；部署配置不能悄悄改变 precision、processor 或 backend kind；
- v0.1 不消费的 `resource_group` 返回“v1.0 才支持”，不静默保存；
- 参数白名单由 model type / processor schema 提供，未知参数直接报错。

`ALTER MODEL` 只有在新 revision 与所有引用该 `model_id` 的当前 Function head 在任务类型、输入模态、输出 schema/向量维度和绑定参数 schema 上兼容时，才能推进 head；校验与 head 更新在同一个 Catalog 事务中完成。不兼容升级必须创建新的 Model ID，再用 `ALTER FUNCTION ... SET MODEL` 或新 Function revision 显式迁移，不能让已有函数接口在下一次规划时突然失效。

固定 artifact 的本地/ONNX 模型默认可以声明为 `deterministic`。没有不可变 revision 的 endpoint 一律是 `volatile`；volatile 调用不能做公共表达式消除、常量提升或结果缓存。用户只能把经过能力声明和回归测试的固定 endpoint 标记为 `stable_within_query`，此时同一查询内可以去重，但仍不能跨查询缓存。

### 7.4 三类函数实现

| 语法 | 规划与执行 |
|---|---|
| `USING MODEL` | 注册签名与模型绑定；调用在规划期提取为 `Inference` 节点 |
| `LANGUAGE PYTHON AS 'module:function'` | 注册批量 Arrow ABI；只有 Python 宿主在 v0.1 能执行 |
| `AS (<表达式>)` | SQL 宏；规划前进行卫生替换和递归深度检查，不产生运行时函数 |

Python UDF v0.1 ABI：入口每次接收与参数一一对应的 `pyarrow.Array`，返回长度相同、类型匹配的 `pyarrow.Array`。`IMAGE` 参数在跨语言前转换为编码态；SDK 提供批量解码 helper。逐行 Python 回调不在支持范围内，模型推理必须使用 `USING MODEL`。

### 7.5 语法到计划的映射

| VQL 表达 | 规范化结果 |
|---|---|
| `a <-> b` | `L2_DISTANCE(a, b)` |
| `box.center` | `BOX_CENTER(box)` |
| `FRAMES(TABLE videos, fps => 1)` | `Frames` 扩展节点 |
| `TUMBLE(ts, interval)` | `TumbleAggregate`；物理编译时按边界性分流 |
| `FROM t, UNNEST(expr)` | DataFusion 原生展开节点；这是 v0.1 唯一行展开方式 |
| `CREATE ...` | Catalog 或运行时操作，不进入关系计划 |

检测函数的 `classes/min_confidence` 由 processor 对数组元素执行，不改写为行级 Filter。行级 Filter 会丢掉整帧，语义不等价。

### 7.6 v0.1 内置函数

| 函数 | 签名 | 实现约束 |
|---|---|---|
| `COUNT_OBJECTS` | `(detections, label STRING, min_confidence FLOAT) -> BIGINT` | 在数组内按标签和阈值计数，不展开或丢弃整帧 |
| `BOX_CENTER` | `(BOX2D) -> POINT2D` | `box.center` 的等价形式 |
| `POLYGON` / `ST_POLYGON` | `(STRING) -> POLYGON` | 常量参数在规划期解析并检查闭合、有限数值和 `[0,1]` 范围 |
| `ST_CONTAINS` | `(POLYGON, POINT2D) -> BOOLEAN` | 采用明确的边界规则：边界点视为包含 |
| `L2_DISTANCE` | `(VECTOR(n), VECTOR(n)) -> FLOAT` | 规划期要求维度相同；`<->` 的等价形式 |
| `TO_JPEG` | `(IMAGE [, quality]) -> BINARY` | 显式触发读取/解码/编码；quality 范围在规划期校验 |
| `FRAME_AT` | `(locator STRING [, pts_ms BIGINT]) -> IMAGE` | v0.2；含 I/O，规划为 `MediaFetchExec`，并按 locator 中的 source revision 执行权限与范围检查 |

上述函数遵循 SQL NULL 传播；`COUNT_OBJECTS` 的 NULL 输入返回 NULL。`FRAME_AT` 的 locator 已包含当前帧 PTS；显式第二参数只用于在同一已授权视频对象内选择其他时间点。它在 v0.1 返回版本明确的未支持错误，不登记一个无法执行的存根。

---

## 8. 批数据、媒体读取与 Sink

### 8.1 图片和视频表

内置 provider 的最小 schema：

| 来源 | 最小列 |
|---|---|
| IMAGES | `uri STRING, image IMAGE, width INT, height INT, captured_at TIMESTAMP` |
| VIDEOS | `uri STRING, video VIDEO, duration DOUBLE, fps DOUBLE, width INT, height INT, codec STRING, captured_at TIMESTAMP` |
| `FRAMES(TABLE videos, ...)` | 透传列 + `ts TIMESTAMP, pts_ms BIGINT, frame_id BIGINT, frame IMAGE` |
| RTSP Stream | `ts TIMESTAMP NOT NULL, frame IMAGE, frame_id BIGINT, source STRING` |
| Lance / Parquet Table | 从已保存 Arrow schema 恢复；未知逻辑类型仍按标准 storage type 读取 |

无法读取的可选元数据为 NULL；`uri`、媒体值和 RTSP 的 `ts/frame_id/source` 不为 NULL。用户通过目录选项添加的分区列可以追加，但不能改变上述列的含义。

- `USING IMAGES` 和 `USING VIDEOS` 实现为 `TableProvider`；规划期只返回 schema 和统计信息，实际列举与读取在 `execute()` 中发生；
- provider 支持 `file://` 和 object_store 支持的对象存储；路径、扩展名和 `recursive` 在执行前校验；
- Lance 与 Parquet provider 支持列裁剪、谓词下推和统计信息；VisionQL 自己写出的文件保存逻辑类型 metadata，读回时恢复 `IMAGE/BOX2D/VECTOR`，从而覆盖“写入 Lance 后再做 TopK”的 MVP 流程；
- `uri`、文件大小和修改时间来自对象列表；宽高、时长、codec 等昂贵元数据只在被投影时探测；
- 图片表输出引用态 `IMAGE`，视频表输出引用态 `VIDEO`；扫描阶段不解码像素。

### 8.2 `FramesExec`

`FRAMES` 是表进表出的逻辑算子：

- 输出 `uri`、`ts`、`frame_id`、引用态 `frame`，并透传输入视频行需要的列；
- `fps` 是显式采样目标，按 PTS 而不是帧序号采样，支持 VFR；
- `pts_ms` 始终表示媒体内相对时间；`ts` 是事件时间。存在可信 capture/start metadata 时使用 `start_time + pts`，否则使用表选项 `start_time`；两者都没有时使用 Unix epoch 作为可重复的合成原点，并在 schema/指标中标记 `synthetic_event_time`；
- 时间谓词下推为 `time_range`，容器支持时先 seek 到范围附近；
- 顺序解码与稀疏 seek 的选择由媒体运行时依据采样比、GOP 和存储能力决定。稀疏 seek 未经 PoC 前不作为吞吐承诺；
- 元数据查询若不读取 `frame` 像素，只生成帧定位信息。

### 8.3 RTSP 摄入

- FFmpeg demux/decode 运行在独立的受控工作线程，不能阻塞 async executor；
- RTSP 优先 TCP interleaved，可配置 UDP；
- 对普通帧间编码，摄像头 25/30fps 的码流通常仍需按源帧率解码后再采样。`fps=5` 主要减少帧仓、前处理和推理工作量，不虚报为 5fps 解码；
- 支持硬解时可以启用 NVDEC、VideoToolbox 等后端，失败回退软件解码并记录指标；
- 每个采样帧进入当前 epoch arena，达到行数或时间阈值后封装成 `StreamEpoch`。

### 8.4 Sink 契约

| Sink | v0.1 行为 |
|---|---|
| Console | 仅 shell 和 `visionql run` 前台可用；`IMAGE` 显示摘要，不输出像素；服务态拒绝常驻 Console Sink |
| Kafka | JSON 标量按稳定规则编码；`IMAGE` 默认只输出脱敏 URI、locator 和元数据，必须显式 `TO_JPEG` 才输出 base64 字节 |
| Lance | 有界查询直接追加；流查询按时间/大小合并多个 epoch 后提交新版本，避免逐 epoch 产生小 commit；`IMAGE` 落为编码态 blob 并保存逻辑类型元数据，用于嵌入和证据帧 |
| Parquet | 批追加；流输出使用滚动文件，按时间或大小封卷，临时文件原子 rename |

`CREATE SINK` 只登记连接信息。第一次 `INSERT INTO` 规划时完成输出 schema 与 format 校验。Sink 写入必须支持取消、超时和有界缓冲；持续查询中的重试策略由作业协调器统一管理。

---

## 9. 模型运行时与 `InferenceExec`

### 9.1 运行时接口

```rust
trait ModelBackend {
    fn load(&self, spec: &ResolvedModel) -> Result<ModelSession>;
}

trait ModelSession {
    async fn infer(&self, batch: TensorBatch, cancel: CancellationToken)
        -> Result<RawModelOutput>;
}

trait Processor {
    fn preprocess(&self, images: &DecodedBatch, buffers: &mut TensorBuffers)
        -> Result<TensorBatch>;
    fn postprocess(&self, raw: RawModelOutput, params: &BoundParams)
        -> Result<ArrayRef>;
}
```

v0.1 提供 ONNX Runtime 后端与 HTTP endpoint 后端。processor 负责 resize、归一化、tokenize、检测框还原、NMS 和绑定参数；后端只负责模型会话和张量 I/O。

### 9.2 模型来源与完整性

- `file://`、`hf://` 和 `endpoint://` 由独立 resolver 处理；
- 浮动的 Hugging Face revision 在首次解析时固定为不可变 commit，并记录内容哈希；
- 下载使用临时文件，哈希校验后原子放入内容寻址缓存；
- 离线环境可以只使用本地路径或预热缓存；
- endpoint URL 的鉴权通过 secret 引用注入，不写入模型 DDL 的可见输出。

模型可执行性由 manifest 决定，不能仅凭 `TYPE OBJECT_DETECTION` 猜张量布局。解析后的 manifest 至少包含 backend artifact、输入/输出张量、processor ID 与版本、图像尺寸/归一化、标签表、入口名称，以及嵌入维度（如适用）。来源可以是仓库内的 `visionql-manifest.json`、内置已测试模型清单，或用户显式指定的受支持 processor 配置。

v0.1 不在引擎内嵌 PyTorch，也不隐式执行任意仓库代码。`hf://` 来源没有可用 ONNX artifact 或受支持 manifest 时，`CREATE MODEL` 直接说明需要的 artifact/endpoint。远程 endpoint 应提供或由用户声明模型 revision；无法固定 revision 时对象标记为 `mutable_endpoint`，`EXPLAIN` 和作业详情显示可复现性警告，且后续版本不得对它启用跨查询结果缓存。

### 9.3 推理提取

规划器扫描 Projection、Filter 和聚合输入中的模型函数调用：

1. 把调用替换为内部列引用；
2. 在最早同时具备所需输入列、且不会改变语义的位置插入 `Inference`；
3. 只有 determinism 为 `deterministic` 或 `stable_within_query` 时，完全相同的 Function revision、Model semantic fingerprint、输入表达式和绑定参数才执行一次；volatile 调用保持原次数和顺序；
4. 常量文本调用，例如 `embed_text('...')`，只有满足同一 determinism 条件时才作为 query init expression 执行一次；
5. v0.1 不跨不同 Function 修订共享原始模型输出。跨绑定参数共享与缓存留到 v0.3，避免后处理语义被错误合并。

`InferenceExec` 的批路径如下：

```text
引用/编码态 IMAGE
  → 异步读取
  → 解码
  → 批量前处理
  → 模型调度队列
  → 异步推理
  → 后处理
  → 追加 nullable Arrow 结果列
```

输出顺序与输入行一致。取消计划 stream 时，尚未提交的请求立即移除，已提交的请求结果被丢弃且资源最终释放。

### 9.4 调度与 batching

- 每个 `ModelInstanceKey`（Model semantic fingerprint、设备和运行时配置代次）有一个队列；
- 请求按 `interactive`、`stream`、`batch` 三类进入加权公平队列，流请求可带 deadline，批任务不能无限挤占流 SLO；
- 达到 `max_batch` 或最早 deadline / `max_wait` 时发车；具体 batch 大小、等待时间和 GPU 选择是运行时配置，不写进 FUNCTION；
- 张量缓冲按最大在途批次预分配并复用；队列满时提交端等待，背压回传；
- v0.1 默认单设备。显存不足时在加载阶段失败并给出模型、估算需求和可选 endpoint，不在运行中用未经验证的 LRU 换出；
- 运行时记录实际 batch 分布、排队时间、推理时间和设备利用率，为 v0.2 Workbench 实测成本面板提供数据。

---

## 10. 优化器与 `EXPLAIN`

### 10.1 v0.1 规则顺序

| 顺序 | 规则 | 正确性或收益 |
|---|---|---|
| R1 | SQL 宏展开与类型检查 | 保证语义 |
| R2 | 模型调用提取；仅对 deterministic / stable-within-query 调用去重和常量提升 | 保证模型调用可调度，同时保留 volatile 调用次数与顺序 |
| R3 | 列裁剪与 `image_access` 分析 | 不消费像素时完全跳过读取和解码 |
| R4 | 时间谓词下推 | 只读取目标视频范围 |
| R5 | 显式采样下推 | 把 `FRAMES(... fps)` 和 Stream `fps` 推到媒体层 |
| R6 | 原生 DataFusion 规则 | 谓词、投影、常量折叠和普通关系优化 |

根据窗口粒度自动猜测 fps 不属于 v0.1。用户显式指定的 fps 是结果语义的一部分；v0.3 的自动调整必须在 `EXPLAIN` 中可见，并允许关闭。

### 10.2 向量 TopK

v0.1 将 `<->` 规范化为 `L2_DISTANCE`，再使用有界 TopK 执行。没有索引时必须如实显示 `BruteForceTopK`。`CREATE INDEX ... USING HNSW` 在 v0.3 之前返回未支持，不能登记一个不会被使用的索引。

### 10.3 `EXPLAIN` 输出

v0.1 的 `EXPLAIN` 至少展示：

- 定义快照和查询模式；
- 逻辑计划与批计划 / 流作业图；
- 视频时间范围、源 fps、采样后预计帧率；
- 每个推理节点的模型修订、输入规模和去重情况；
- 是否需要解码和使用哪种 IMAGE 形态；
- 流查询的状态算子、watermark delay、投递语义和不支持项。

GPU 时长或费用预估属于 v0.3。v0.1 只展示工作量，不输出貌似精确但没有校准的数据。

---

## 11. 产品形态与公开接口

### 11.1 无进程假设的内核

`vql-core` 不处理信号、不监听端口、不读取全局单例。宿主构造 `EngineConfig`、注入 secret provider 和可选 Python UDF host，再负责生命周期。

| 形态 | 宿主职责 | 阶段 |
|---|---|---|
| Python 库 | PyO3 绑定、DataFrame、进程内 Python UDF、notebook 富显示 | v0.1 |
| CLI | shell、脚本执行、信号处理、前台持续查询 | v0.1 |
| `visionqld`（`vql-server`） | v0.1 承担 Flight SQL、服务配置和进程生命周期；v0.2 增加认证、持久作业管理、恢复和 Prometheus | v0.1 基础；v0.2 生产化 |
| 集群节点 | 远程片段执行和资源隔离 | v1.0 |

### 11.2 DataFrame API

DataFrame 方法直接构造 `VqlLogicalPlan`，不先生成 SQL 字符串。`sess.sql()` 和链式 API 返回同一个 DataFrame 类型；`collect/show/write/start` 才触发执行。

Arrow C Data Interface 用于 Python 结果交换。`IMAGE` 在普通 `show()` 中只显示摘要；notebook 需要缩略图时显式请求编码，避免 collect 隐式搬运原图。

### 11.3 CLI

| 命令 | 契约 |
|---|---|
| `visionql shell` | 多行 SQL、历史、目录查看；无界 SELECT 持续打印；Ctrl-C 取消当前查询 |
| `visionql run job.sql [--server endpoint]` | 未指定 server 时顺序执行脚本，持续查询以前台作业运行；指定 server 时通过 Flight SQL 执行，普通无界语句仍保持客户端附着。v0.2 使用 `--detach --name <job>` 显式把脚本中唯一一条无界 Sink 语句包装为 `SUBMIT QUERY`；源文件不改写。附着执行时 Ctrl-C 先优雅停止，第二次立即取消 |
| `visionql explain query.sql` | 输出与 SQL `EXPLAIN` 相同的计划 |

v0.1 CLI 遇到 Python UDF 时明确提示改用 Python 宿主。它不能为了看似统一而把 Python 解释器嵌入引擎二进制。

### 11.4 Flight SQL 契约

`vql-server` 在 v0.1 实现以下基础公开能力：

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

名称集合至少按版本公布 `unbounded_do_get`、`poll_flight_info`、`cancel_flight_info`、`statement_info_v1`、`image_thumbnail_mode`、`frame_at_v1`、`query_control_v1`、`query_metrics_v1` 和后续 `explain_cost`。未知 capability 必须可忽略。

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

v0.1 按单可信主体运行，默认只监听回环地址，不把匿名 session 当作生产认证边界。v0.2 在同一协议上加入 TLS、认证和表/流级权限，并增加 `SUBMIT QUERY`、`SHOW/DESCRIBE QUERY`、`SHOW METRICS`、`PAUSE/RESUME/STOP` 等公开作业 SQL；仍不提供只给 Workbench 使用的管理 RPC。

服务态按版本区分无界语句生命周期：

- v0.1 和 v0.2 的普通无界 `SELECT`、`INSERT INTO <sink> SELECT ...` 都附着当前 Flight session；结果或状态持续通过 Flight 返回，客户端取消、session 过期或连接丢失后终止，不写入 QueryJob。升级服务版本不会悄悄改变同一条 SQL 的生命周期；
- v0.2 只有显式 `SUBMIT QUERY <name> AS INSERT INTO <sink> SELECT ...` 才创建持久作业。它通过 statement-query 路径返回一行 Arrow 结果 `query_id Utf8, name Utf8, state Utf8, definition_revision Utf8`；引擎完成规划和 Catalog 事务后立即返回，作业由 `visionqld` 后台管理，Flight 请求结束不影响它；
- 有界 SELECT/INSERT 在两个版本中都由当前 Flight 请求等待完成；Console Sink 不允许成为服务态持久作业目标。

### 11.5 Workbench 所需媒体协议

v0.1 Flight 会话提供 `reference` 和 `inline` 两种基础 IMAGE 结果模式；v0.2 增加 Workbench 使用的 `thumbnail` 模式和按引用取帧：

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

`FRAME_AT` 不接受展示 URI，只解析版本化 locator，并按其中的 source revision 对当前 principal 重新授权和执行范围校验。文件和对象存储引用可以重新读取；live 流只在服务端短期环形缓存仍有该帧时成功。详细客户端行为见 [Workbench 设计](./workbench.md)。

### 11.6 系统查询的最小 schema

为了让 Workbench 不解析日志，v0.2 固定以下最小列；后续版本可以追加列：

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

SHOW METRICS [FOR QUERY <id>]:
  query_id, metric, value, unit, window_start, window_end
```

作业控制语法固定为：

```sql
SUBMIT QUERY people_per_minute AS
INSERT INTO people_sink SELECT ...;
DESCRIBE QUERY '<query_id>';
SHOW QUERY DEPENDENCIES '<query_id>';
SHOW METRICS FOR QUERY '<query_id>';
PAUSE QUERY '<query_id>';
RESUME QUERY '<query_id>';
STOP QUERY '<query_id>';
```

`query_id` 是服务端生成的 UUID 字符串；持久作业名称由 `SUBMIT QUERY` 显式提供，在 owner 的非终态作业中唯一。附着查询的 `name` 为 NULL、`lifecycle=attached`；持久作业为 `lifecycle=persistent`。名称只用于展示和筛选，不能代替 ID 执行状态变更。

不带 `FOR QUERY` 时返回当前 principal 可见作业的一个指标快照，供列表页一次读取；带过滤时返回单个作业。瞬时值的 `window_start/window_end` 可以为 NULL。

指标名在 v1.0 前仍可演进，但每个指标都必须携带 unit，Workbench 不通过字符串猜单位。

---

## 12. 资源、性能与可观测性

### 12.1 统一资源预算

查询配置一个总内存预算，以下资源都通过 DataFusion `MemoryPool` reservation 或 VisionQL 的等价外部 reservation 记账：

| 资源 | 超限策略 |
|---|---|
| Arrow batch 与算子状态 | 使用 DataFusion 内存管理；不支持 spill 的自定义状态明确失败 |
| 对象存储预取和压缩字节 | 收缩并发与 read-ahead |
| 解码帧仓 | 批处理背压；RTSP 只在入 epoch 前丢最旧采样帧 |
| live 媒体预览 ring（v0.2） | 按总字节数和 TTL 淘汰最旧 GOP；不影响查询数据语义 |
| 张量缓冲与推理队列 | 有界队列，提交方 await |
| TUMBLE 状态 | v0.1 不 spill；超限失败并提示降低 group key 基数或缩短窗口 |
| Sink 缓冲 | 背压；超时后按查询容错策略失败 |

显存单独计量。模型加载前依据权重、workspace 与 batch 上限做保守预估，加载后用实际值修正指标。

### 12.2 MVP 性能口径

PRD 的 8 路 1080p@5fps 基线要区分四个量：

| 量 | 典型值 | 说明 |
|---|---|---|
| 输入码率 | 8 × 约 4Mbps | 网络与 demux 压力 |
| 解码帧率 | 8 × 25～30fps | 普通 RTSP 帧间编码通常需要完整解码 |
| 采样输出率 | 8 × 5fps = 40fps | 进入帧仓、前处理与查询的数据 |
| 推理率 | 约 40fps，扣除查询过滤 | GPU 主要工作量 |

因此不能用 40fps 代替解码容量。性能验收必须分别报告网络、解码、采样、推理、窗口延迟和丢帧率，并注明模型、硬件、codec、GOP 和 watermark 配置。

### 12.3 指标

至少暴露：

- 查询：输入/输出行、epoch 延迟、端到端延迟、错误行、迟到行、状态内存、Sink 重试；
- 媒体：输入码率、解码 fps、采样 fps、丢帧数及原因、断流次数和缺口时长；
- 模型：队列深度、等待时间、batch 分布、推理次数、P50/P95、显存；
- 恢复（v0.2）：检查点耗时/大小、最后成功 epoch、Kafka lag、恢复次数；
- 资源：各 reservation 当前值和峰值。

库态和 v0.1 基础服务态通过执行结果、前台输出和 tracing 日志提供指标；v0.2 服务态增加 `SHOW METRICS` 与 Prometheus。日志必须包含 `query_id`、`epoch_id`、对象修订与稳定错误码。

### 12.4 错误分类

| 类别 | 示例 | 默认行为 |
|---|---|---|
| 行级数据错误 | 单帧损坏、单次推理失败 | 结果 NULL，计数并继续 |
| 查询语义错误 | 类型不匹配、无界排序、未支持功能 | 规划失败，不启动运行时 |
| 资源错误 | 内存/显存不足、状态超限 | 查询失败，释放全部租约 |
| 外部系统错误 | RTSP 断流、Kafka/Lance 不可达 | 按连接器策略重试；超过上限失败或保持 Disconnected |
| 引擎缺陷 | 不变量破坏、arena 越界 | 立即失败并记录诊断，不降级为 NULL |

稳定错误码与自然语言信息分离。Workbench 根据错误码决定界面状态，不匹配错误文本。

---

## 13. 安全与隐私

### 13.1 v0.1 库态与基础服务态

- `vql-core` 默认不监听网络；只有显式启动 `visionqld` 才创建服务端口，且 v0.1 默认只绑定回环地址；
- v0.1 服务态按单可信主体运行，不提供 TLS、认证或表/流级权限，也不应直接暴露在不可信网络；
- 除用户声明的 endpoint 模型、对象存储、Kafka、RTSP 和模型下载外，不产生出站连接；
- 模型权重固定 revision 和哈希，加载时复核；
- 目录、日志和 `SHOW CREATE` 都必须脱敏 URI 与 secret 引用；
- 所有网络读取都经过 URL scheme 与目标策略。`FRAME_AT` 只接受指向已登记来源的 `locator`；服务态默认阻止云 metadata、link-local 和配置未授权的目标，普通查询不能临时指定任意 URL。

### 13.2 服务态 v0.2

- Flight SQL 使用 TLS；认证身份映射到 Catalog principal；
- 查询规划和媒体解引用都检查表/流级权限，不能只在目录列表处隐藏对象；
- Workbench 取得的 `IMAGE.uri` 只是脱敏展示值；`IMAGE.locator` 才是可重新授权的媒体定位符，两者都不包含底层凭证；
- Python UDF 运行在进程外 worker，设置超时、内存限制和依赖环境；它不是多租户安全沙箱；
- 审计日志属于 v1.0，但 v0.2 已在执行上下文保留 principal、query_id 和对象修订字段，避免以后无法补齐来源。

---

## 14. 代码组织

```text
visionql/
├── Cargo.toml                    # 根 workspace，仅显式包含四个引擎 crate
├── vql-core/
│   └── src/
│       ├── types/                # Arrow 类型、错误码、公共配置
│       ├── catalog/              # 对象修订、SQLite、依赖
│       ├── sql/                  # VQL parser 与规范化
│       ├── planner/              # 逻辑计划、分析、优化与模式选择
│       ├── execution/
│       │   ├── batch/            # DataFusion 物理计划与扩展算子
│       │   └── stream/           # epoch、协调器、TUMBLE 与检查点接口
│       ├── media/                # FFmpeg、图片编解码、FrameArena
│       ├── models/               # backend、processor、scheduler
│       └── connectors/           # 表、流和 Sink
├── vql-cli/                      # shell / run
├── vql-python/                   # PyO3 与 Python UDF host
├── vql-server/                   # Cargo package
│   └── src/
│       ├── lib.rs                # vql_server crate
│       └── bin/visionqld.rs      # 对外守护进程
├── vql-workbench/                # 独立 workspace 和 Web 项目
└── docs/
```

根 `Cargo.toml` 显式列出 `vql-core`、`vql-cli`、`vql-python` 和 `vql-server`，并通过 `exclude = ["vql-workbench"]` 排除独立子项目，不能使用会把它纳入的宽泛 glob。`vql-workbench` 拥有自己的 workspace、前端工具链和 CI，不参加根 workspace 的默认构建。

crate 依赖只有三条：

```text
vql-cli ─────┐
vql-python ──┼──→ vql-core
vql-server ──┘
```

`types`、`catalog`、`sql`、`planner`、`execution`、`media`、`models` 和 `connectors` 在 v0.1 都是 `vql-core` 的内部 module，而不是独立 crate。默认使用 `pub(crate)`；只有宿主真正需要的 `Engine`、`Session`、配置、结果和注入 trait 进入公共 API。只有出现独立消费者或发布周期、无法用 feature 解决的原生依赖冲突，或有实测编译隔离收益时，才通过 ADR 将 module 提取为 crate。

边界规则：

- `vql-core` 不能依赖 PyO3、clap 或 Flight；
- `vql-cli` 负责 clap、终端和信号；`vql-python` 负责 PyO3 与 Python UDF host；`vql-server` 负责 Flight SQL、配置、进程生命周期以及 v0.2 的认证和作业恢复；
- `vql-server` 是 Cargo package 名称，library crate 标识符为 `vql_server`，对外 binary target 和守护进程命令保持 `visionqld`；
- planner module 不能调用 execution module；batch/stream 物理编译入口属于 execution，media、models 与 connectors 通过窄 trait 由内核装配，禁止形成反向调用；
- `vql-workbench/server` 不能直接依赖 `vql-core`、`vql-server` 或其他根 workspace crate，只能作为 Flight SQL 客户端；
- DataFusion 破坏性升级集中在 planner 与 execution module 的适配层，禁止其类型扩散到公开 Python、CLI 或 Flight API。
- 根 workspace 锁定一组经过验证的 DataFusion、Arrow 与 sqlparser 版本；文末 `latest` 文档链接只用于阅读，不是依赖声明。升级必须运行物理计划重新实例化、窗口 state codec、Arrow wire schema 和 Flight 协议回归套件后才能更新 lockfile。

---

## 15. 测试与验收

### 15.1 分层测试

| 层 | 必测内容 |
|---|---|
| Parser / Catalog | 全部新增语法、脚本切分、参数归属、对象修订、事务回滚、未支持功能错误；Function 稳定 model ID 解析、旧计划固定 revision、兼容/不兼容 `ALTER MODEL` |
| 逻辑计划 | SQL 与 DataFrame 生成等价计划；边界性与 definition snapshot 正确 |
| streamability | 每个允许节点正例；无界聚合、排序、DISTINCT、JOIN 等逐项负例 |
| epoch 控制 | 整个批次被 Filter 丢弃后仍推进水位线和释放 FrameArena；异步推理完成前不得推进 watermark；连续两个不同 epoch 在含 Repartition 的计划中不串数据/metrics；取消后下一实例无残留任务 |
| TUMBLE | 边界、乱序、迟到、NULL、空窗口、批流同语义；聚合白名单逐项差分；拒绝 IMAGE/UDAF；state codec 非破坏性快照、restore round-trip、版本不兼容与内存记账 |
| 媒体 | 固定图片/视频、VFR、不同 GOP、损坏帧、采样 PTS；RTSP source generation、capture/ingest 选择、进程内时钟回拨、恢复后时钟落后 watermark，以及断流后的真实前向 gap |
| 推理 | 固定小模型数值回归、processor、semantic fingerprint、deterministic/volatile 去重边界、batching 公平性、取消、显存不足 |
| Sink | schema 校验、取消、背压、滚动文件原子性、Kafka JSON IMAGE 规则 |
| 基础协议 v0.1 | 全部约定 metadata RPC、statement/prepared transport 映射、`statement_info_v1`、FlightInfo query ID、附着式无界状态流、断连取消、逐 RPC session 隔离、Protobuf 错误 envelope、回环地址默认绑定、IMAGE storage schema/version |
| 恢复 v0.2 | 在 Sink ack、checkpoint rename、Kafka commit 前后逐点 kill；验证规范化窗口状态不丢、只允许重复，并覆盖 codec/version 不兼容 |
| 协议增强 v0.2 | TLS/认证/权限、`SUBMIT QUERY`、查询详情/依赖/控制、IMAGE 三种模式、locator 篡改/撤权/过期、`FRAME_AT`、能力协商 |

### 15.2 PRD 验收场景

**场景 A：批流一体与基础服务态。**

1. 启动默认绑定回环地址的 `visionqld`，用固定视频通过本地 RTSP mock 和 Flight SQL 运行 PRD 3.2 的“每分钟人数写入 Kafka”；
2. 用相同视频文件、模型、时间轴和 5fps 采样，通过库态或 CLI 执行历史回算；
3. 对齐窗口边界后逐窗口比较结果；
4. 同时验证 Console Sink 的 `UNNEST` 明细、断流指标、Flight 取消、客户端断连清理和 Ctrl-C 行为；
5. 查询脚本不超过 PRD 的 30 行口径。

**场景 B：首次使用。**

1. 本地图片目录建表；
2. Python 批量 UDF 过滤模糊图片；
3. CLIP 图片/文本函数生成嵌入；
4. 写入 Lance；
5. 暴力 TopK 返回 20 张图；
6. 从安装到首个结果不超过 5 分钟，过程中不要求启动外部服务。

### 15.3 性能门槛

- 固定硬件、codec、模型和数据集运行 8 路基准至少 30 分钟；
- 报告源解码 fps、采样 fps、推理 fps、GPU 利用率、P95 窗口输出延迟、丢帧和内存峰值；
- 批基准分别覆盖元数据扫描、全帧推理、稀疏采样和图片嵌入；
- 元数据和已物化结果的交互查询在约定的本地基准数据集与缓存口径下达到 P95 < 1s，并同时报告冷缓存结果；
- 批扫描应让实际瓶颈资源达到稳定高利用率；如果瓶颈是 GPU 而不是解码器，报告必须如实区分，不能为了满足措辞而宣称“解码打满”；
- CI 跟踪 micro-benchmark；端到端 GPU 基准在固定 runner 上运行，回归阈值单独配置。

---

## 16. 演进接口

| 后续能力 | 已固定的扩展点 | 不提前实现的内容 |
|---|---|---|
| 服务态生产化 v0.2 | v0.1 已有的 `vql-server`、Flight session、取消 token 和定义快照 | v0.1 不实现持久 QueryJob、恢复、TLS、认证或权限 |
| Kafka 源 / 至少一次 | `SourceProgress`、epoch、checkpoint store | v0.1 不保存流状态 |
| Workbench | Flight SQL、IMAGE wire schema、`FRAME_AT`、系统查询 schema | 引擎不提供私有 Workbench API |
| MCP 服务器 v0.2 | 作为受权限约束的独立 Flight SQL 客户端适配器；复用相同身份和取消语义 | 不把 Agent 协议放入引擎内核 |
| `TRACK` / HOP / SESSION | 新 StatefulOp 与 streamability 能力位 | v0.1 parser 后直接拒绝 |
| Webhook Sink v0.2 | 实现现有 Sink trait、重试和 delivery ID | 不增加专用计划节点 |
| 物化与跨查询复用 | `Inference` 节点、模型内容哈希、媒体引用 | v0.1 不做共享缓存 |
| 模型级联 | model type、成本画像、显式推理节点 | v0.1 不自动替换用户模型 |
| 自动采样 / ROI v0.3 | `Frames` 的采样与 time range、`image_access` 与媒体 crop 参数 | v0.1 只执行用户显式采样，不改变结果语义 |
| 向量索引 | `L2_DISTANCE + LIMIT` 规范形式、Catalog index 对象接口 | v0.1 只做暴力 TopK |
| GPU 池化 v0.2 | ModelInstanceKey、统一调度队列和显存指标 | v0.1 只管理单设备，不做 LRU 换出 |
| 进程外 Python / WASM | Arrow 批 ABI 与可取消 UDF host | v0.1 只有 Python 宿主进程内执行；WASM 到 v1.0 |
| 精确一次 | checkpoint 与 sink delivery sequence | 没有事务 Sink 前不声明精确一次 |
| 多租户、资源组与审计 | definition snapshot、principal、query/resource 指标 | v1.0 前不接受 `resource_group`，不声称有审计能力 |
| 集群 / 边缘 | 可序列化逻辑计划和标准 Arrow 数据 | 不预埋分布式调度代码 |

新增实现必须能回答“只增加哪个 trait、注册项或逻辑节点”。如果为了一个新模型需要改 parser、stream coordinator 和多个无关 connector，说明边界设计失效。

---

## 17. 设计决策摘要

| ADR | 决策 | 主要理由 |
|---|---|---|
| ADR-001 | Rust + Arrow + DataFusion | 满足嵌入、列式执行、Python/Flight 互操作与公开扩展点要求 |
| ADR-002 | 统一逻辑计划，批与流分别物理编译 | “批流一体”保持用户语义，同时不把流控制面强塞给只处理 RecordBatch 的原生算子 |
| ADR-003 | 流运行时使用 epoch + 有界 DataFusion 片段 | Filter 不会吞掉水位线和源进度；异步推理与资源释放有明确 barrier |
| ADR-004 | `IMAGE` 使用标准 Arrow storage + 引用/帧仓/编码三态 | 减少像素复制，并保持 IPC 和未知客户端可读 |
| ADR-005 | FrameArena 按 epoch 整体租约释放 | 生命周期独立于存活行，避免 Filter 导致引用泄漏 |
| ADR-006 | 模型函数提取为显式 `Inference` 节点 | 支持异步 batching、去重、未来级联/缓存和成本观测 |
| ADR-007 | 查询固定目录定义修订 | 防止运行中 DDL 静默改变结果，支持审计和恢复 |
| ADR-008 | RTSP 尽力而为；Kafka 用状态+offset 检查点实现至少一次 | 投递语义服从源是否可重放，不作超出物理能力的承诺 |
| ADR-009 | SQLite 目录，运行时字节不入目录 | 零外部依赖，同时保留事务与迁移能力 |
| ADR-010 | Workbench 只使用 Flight SQL 和公开 SQL | 客户端解耦，并持续验证公开协议完整性 |
| ADR-011 | epoch 复用计划模板，但每次实例化新的物理执行树 | 新 TaskContext 不能替代算子 reset；避免 channel、动态状态和取消任务跨 epoch 泄漏 |
| ADR-012 | TUMBLE 使用白名单 `WindowStateCodec` 和规范化 Arrow 状态 | DataFusion accumulator 快照可能消耗内部状态，恢复 ABI 必须由 VisionQL 版本化 |
| ADR-013 | Function 绑定稳定 model ID；计划固定解析后的 Model revision；媒体使用带 source revision 的 locator | 同时满足新查询跟随升级、运行中可复现，以及媒体重新授权 |
| ADR-014 | Workbench 依赖版本化的 Flight/SQL 公共契约 | metadata、statement 分类、错误、作业详情和媒体点查都必须可由独立客户端实现 |

---

## 18. 开放技术问题与决策门槛

| 问题 | 决策前需要的证据 | 最迟时间 |
|---|---|---|
| FFmpeg wheel/二进制的 LGPL 分发方式 | 动态/静态构建 PoC、产物大小和法务意见 | v0.1 发布前 |
| 稀疏视频采样是否真的降低解码成本 | 不同 GOP、VFR、本地盘与 S3 range-read 基准 | v0.1 性能承诺前 |
| RTCP capture time 的可靠性 | 设计伙伴摄像头样本、漂移和回退比例 | v0.2 生产流验收前 |
| Lance 小批流式追加与压实 | 连续 7 天写入、版本数、点查和压实测试 | v0.2 物化视图前 |
| `IMAGE` thumbnail/inline 上限与 locator TTL | Workbench、Python、BI 的带宽、可用性和撤权/过期测试；默认引用以及 uri/locator 分工已固定 | v0.2 Flight schema 冻结前 |
| DataFusion 升级成本 | 第一次升级的适配 diff 和测试结果 | v0.1 beta 前 |
| epoch 周期默认值 | 8 路流下吞吐、P95 延迟、batch 分布与取消时延 | v0.1 性能调优阶段 |

---

## 附录 A：PRD 追踪矩阵

### A.1 v0.1 能力

| PRD 能力 | 设计章节 |
|---|---|
| IMAGE / VIDEO / BOX2D / VECTOR | §6 |
| 图片/视频目录表、`FRAMES`、`UNNEST` | §7.5、§8.1～§8.2 |
| RTSP、TUMBLE、尽力而为 | §5、§8.3 |
| MODEL / FUNCTION、检测与嵌入、Python UDF | §7.3～§7.4、§9 |
| Kafka、Lance、Parquet、Console Sink | §8.4 |
| 库态、shell、DataFrame、`visionql run` | §11.1～§11.3 |
| `vql-server`、`visionqld` 与基础 Flight SQL | §11.1、§11.4、§13.1、§14 |
| 采样下推 | §8.2～§8.3、§10.1 |
| 两个 MVP 验收场景 | §15.2 |

### A.2 NFR

| PRD NFR | 设计章节 |
|---|---|
| 8 路性能基线 | §12.2、§15.3 |
| RTSP 尽力而为、Kafka 至少一次、恢复 | §5.4～§5.7 |
| 行级 NULL 与严格模式 | §6.4、§12.4 |
| TLS、权限、模型防篡改、数据不出域 | §9.2、§13 |
| v1.0 起兼容性承诺 | §4.3、§7.2、§11.4～§11.6 |

### A.3 Workbench 引擎依赖

| Workbench 能力 | 引擎契约 |
|---|---|
| SQL 与脚本 | Flight SQL statement/prepared statement，§11.4 |
| 多模态结果 | `visionql.image`、`image_mode`，§6.2、§11.5 |
| 原图点查 | locator 重新授权后的 `FRAME_AT`，§6.2、§11.5 |
| 实时预览与取消 | 无界 `DoGet` 与 cancellation，§11.4 |
| 目录与补全 | Flight SQL metadata + `SHOW` / `DESCRIBE`，§11.4 |
| 运维与成本实测 | `SUBMIT QUERY`、`SHOW/DESCRIBE QUERY`、`SHOW QUERY DEPENDENCIES`、`SHOW METRICS` 与作业控制，§11.4、§11.6 |

---

## 参考资料

- [Apache DataFusion：自定义 TableProvider](https://datafusion.apache.org/library-user-guide/custom-table-providers.html)
- [Apache DataFusion：ExecutionPlan API](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html)
- [Apache DataFusion：无界数据源](https://datafusion.apache.org/user-guide/sql/ddl.html#example-unbounded-data-sources)
- [Apache Arrow：扩展类型与列式格式](https://arrow.apache.org/docs/format/Columnar.html#extension-types)
- [Apache Arrow Flight SQL 规范](https://arrow.apache.org/docs/format/FlightSql.html)
