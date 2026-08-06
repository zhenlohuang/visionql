# 0002: 视频流处理（RTSP 接入与窗口聚合）

- **编号**：0002
- **状态**：Draft
- **目标版本**：v0.2
- **对应 PRD**：[prd.md](../prd.md) §3.1（Stream、窗口）、§3.3.1、§3.6
- **依赖设计**：[design.md](../design.md) §4（无界查询白名单）、§5（epoch 流执行模型）、§6（帧仓）
- **关联 proposal**：0005（v0.3 检查点与恢复建立在本文的规范化窗口状态之上）
- **最后更新**：2026-08-06

## 摘要

v0.2「实时流分析」feature：单路 RTSP 流的摄入与解码、事件时间选择与断流重连，以及流式 `TUMBLE` 窗口聚合的状态管理。RTSP 是不可重放源，v0.2 承诺尽力而为语义；窗口状态使用引擎自有的规范化 Arrow 状态，为 v0.3 检查点恢复（proposal 0005）预留恢复 ABI。

## 动机与范围

范围包括 RTSP 摄入路径、`IngestClock` 与 capture/ingest 事件时间选择、断流语义、`TumbleState` 与聚合白名单细则。epoch 执行模型、背压丢帧总则与 `FrameArena` 属于贯穿性设计，见 [design.md](../design.md) §5～§6；Stream 目录对象与 RTSP 最小 schema 见 design.md §7.2、§8.1。

## 详细设计

### RTSP 摄入

- FFmpeg demux/decode 运行在独立的受控工作线程，不能阻塞 async executor；
- RTSP 优先 TCP interleaved，可配置 UDP；
- 对普通帧间编码，摄像头 25/30fps 的码流通常仍需按源帧率解码后再采样。`fps=5` 主要减少帧仓、前处理和推理工作量，不虚报为 5fps 解码；
- 支持硬解时可以启用 NVDEC、VideoToolbox 等后端，失败回退软件解码并记录指标；
- 每个采样帧进入当前 epoch arena，达到行数或时间阈值后封装成 `StreamEpoch`。

### RTSP 事件时间与断流

- 作业启动时建立一个 `IngestClock`：记录一次 UTC 系统时间与 monotonic clock 锚点，之后用 monotonic elapsed 生成 UTC ingest time。重连不重置该时钟，因此进程内 NTP/系统时钟回拨不会让 ingest time 倒退；
- 每次初连、成功重连、codec/timebase 改变或 RTP/RTCP 映射失效都会开启新的 `source_generation`。Stream 显式选择 `ingest_time` 时跳过探测；选择 `capture_time` 时在有限的 `timestamp_probe_timeout` 内验证 RTP/RTCP 映射的单调性、漂移以及它与 `IngestClock` 的差值。验证成功后本 generation 固定使用 `capture_time`，否则固定使用 `ingest_time`，运行中不在两种时钟之间无 barrier 切换；
- capture 映射在 generation 中途失效、倒退或相对 `IngestClock` 漂移超过阈值时，结束该 generation，并以 `ingest_time` 开启下一 generation，同时记录 `event_time_fallback_total` 和不连续原因；
- 水位线为 `max_seen_event_time - watermark_delay`，单调不回退；`max_seen_event_time` 来自所有成功取得时间戳的源帧，而不是只来自采样后保留的行。当前 epoch 的采样数据处理完成后才能应用同一 epoch 的水位线；
- 断流后指数退避重连，默认 1s 起、最大 30s；断流期间水位线冻结，不用本地时钟伪造源进度；
- 新 generation 的候选事件时间必须先通过连续性门：如果 capture time 小于当前 watermark 或相对 `IngestClock` 异常偏移，拒绝该映射并让该 generation 使用 ingest time。进程恢复后需要新建 `IngestClock`；若它的第一个 ingest time 小于已恢复 watermark，作业进入 `state=FAILED, error_code=TIME_DISCONTINUITY`，不能把后续帧无限判为迟到或暗中把时钟钳到 watermark；
- 重连后的 ingest time 正常包含真实断流时长。它向前推进 watermark、关闭已越过的窗口并记录 gap，但不会为缺失时段补造行；只要 capture time 与同一时刻的 ingest time 在容差内，也可以继续使用 capture time，不能仅因它相对断流前最后一帧前跳就判错；
- 通过连续性门的新帧继续推进原作业 watermark；之前带数据但尚未关闭的窗口随后正常输出，断流时段不会补造行；
- 如果流始终没有恢复，最后一个未关闭窗口不会输出。查询运行状态仍为 `RUNNING`，另以 `source_health=DISCONNECTED` 和指标明确显示缺口时长与最后事件时间；
- RTSP 是不可重放源。进程崩溃、主动丢帧或暂停造成的数据永远不能恢复，因此 v0.2 只承诺尽力而为。

### `TUMBLE` 状态

流模式下，`TumbleState` 保存 VisionQL 自己拥有的规范化 Arrow 状态，而不是把任意 DataFusion `Accumulator` 对象长期放进作业状态：

```text
key = (window_start, group_key)
value = versioned_arrow_states + source_progress_span
```

- 窗口边界为半开区间 `[start, end)`；时间统一转为 UTC 纳秒，v0.2 以 Unix epoch 为固定窗口原点；
- v0.2 的 interval 必须是正的固定时长，不接受月、季度等日历间隔；无界查询的事件时间表达式必须是非空 TIMESTAMP，nullable 列需要先显式过滤 NULL；
- 收到数据时先按事件时间归入窗口。每个白名单聚合由 `WindowStateCodec` 描述输入类型、state schema、更新、求值、内存大小和恢复方式；
- DataFusion accumulator 只作为一次状态转换的临时对象：从当前规范化 Arrow state 通过 `merge_batch` 恢复，应用本 epoch 数据后调用一次 `state()` 取得新状态，随后丢弃。检查点（v0.3，proposal 0005）复制规范化 state，不对仍在运行的 accumulator 调用破坏性快照；
- 应用完 epoch 数据后再处理 watermark；当 `window_end <= watermark` 时输出并删除窗口；
- `event_time < current_watermark` 的行是迟到数据，v0.2 默认丢弃并增加 `late_rows_total`；`allowed_lateness` 不在 v0.2；
- 停止查询时不输出尚未关闭的窗口，避免把部分窗口伪装成完整结果；
- state schema 由 `(aggregate_kind, input_types, state_codec_version)` 决定并纳入 fingerprint；每次更新后按 `size()` 调整查询 reservation，无法记账的 codec 不得注册；
- 任何 state 或 group key 都不能包含 `arena_id/arena_slot`。需要保留画面时必须先转换为持久媒体定位符或编码态值；v0.2 默认拒绝把 `IMAGE/VIDEO` 放入窗口状态；
- 批模式把同一个 `TUMBLE` 降为普通时间分桶和聚合。每个进入流白名单的聚合都必须用相同输入做批/流差分测试，确认 NULL、分组、溢出和最终值一致。

`WindowStateCodec` 是引擎拥有的恢复 ABI。DataFusion 升级不能在没有迁移或重放方案的情况下改变已发布 codec；新增聚合必须先提供确定性的 state schema、非破坏性 checkpoint、restore round-trip 和资源记账测试。

### 流式聚合白名单细则

v0.2 的流式 `TUMBLE` 只允许 `COUNT`、`SUM`、`AVG`、`MIN` 和 `MAX`，参数与 group key 必须使用可持久化的标量 Arrow 类型。`COUNT(DISTINCT primitive)` 留待后续版本在通过状态恢复与内存上限测试后单独启用。`ARRAY_AGG`、`STRING_AGG`、近似聚合、ordered aggregate、UDAF，以及对 `IMAGE`、`VIDEO`、Binary 或包含进程内媒体槽位的复杂类型做聚合，均不进入 v0.2 白名单。

无界计划形状层面的允许/拒绝清单见 [design.md](../design.md) §4.4。

## 与顶层设计的关系

- epoch 执行顺序、计划模板实例化与 `FrameArena` 租约遵守 design.md §5～§6；状态算子位于数据片段之外（design.md §5.2）；
- 背压与丢帧遵守 design.md §5.3 总则：RTSP 只在入 epoch 前丢最旧采样帧，已入 epoch 的行不静默丢弃；
- 规范化 Arrow 状态与 `WindowStateCodec` 是 v0.3 检查点（proposal 0005）的恢复 ABI 基础；
- 尽力而为投递语义对应 design.md §16 的 ADR-008。

## 测试与验收

对应 design.md §14.1 的「TUMBLE」与「媒体」（RTSP 部分）测试行：窗口边界、乱序、迟到、NULL、空窗口、批流同语义；聚合白名单逐项差分；source generation、capture/ingest 选择、进程内时钟回拨、断流后的真实前向 gap。PRD 验收场景 B（批流一体、逐窗口结果一致）依赖本 feature。

## 开放问题

| 问题 | 决策前需要的证据 | 最迟时间 |
|---|---|---|
| RTCP capture time 的可靠性 | 设计伙伴摄像头样本、漂移和回退比例 | v0.2 流验收前 |

## 变更记录

| 日期 | 变更 |
|---|---|
| 2026-08-06 | 从引擎设计 v0.5.0 §4.4、§5.3～§5.4、§8.3 迁出成文 |
