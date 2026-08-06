# 0001: 图片与视频目录表

- **编号**：0001
- **状态**：Draft
- **目标版本**：v0.1
- **对应 PRD**：[prd.md](../prd.md) §3.3.1、§3.3.7
- **依赖设计**：[design.md](../design.md) §6（多模态类型）、§8（数据源与 Sink 概览）
- **关联 proposal**：无
- **最后更新**：2026-08-06

## 摘要

把本地或对象存储中的图片目录、视频目录注册为可直接查询的外部表（`CREATE TABLE ... USING IMAGES/VIDEOS`）。图片表逐文件成行；视频表按建表声明的 `fps` 在扫描算子内部把每个视频文件展开为帧行。扫描阶段只产出引用态 `IMAGE`，不解码像素。

## 动机与范围

这是 v0.1「查询本地图片/视频文件」的数据入口。范围包括 provider 行为、帧展开与事件时间合成、时间谓词下推；不包括 RTSP 流源（proposal 0002）、Parquet/Lance 表 provider（proposal 0007 / 0008）。两类表的最小输出 schema 属于公共契约，固定在 [design.md](../design.md) §8.1。

## 详细设计

### provider 行为

- `USING IMAGES` 和 `USING VIDEOS` 实现为 `TableProvider`；规划期只返回 schema 和统计信息，实际列举与读取在 `execute()` 中发生；
- provider 支持 `file://` 和 object_store 支持的对象存储；路径、扩展名和 `recursive` 在执行前校验；
- `uri`、文件大小和修改时间来自对象列表；宽高、时长、codec 等昂贵元数据只在被投影时探测；
- 图片表输出引用态 `IMAGE`，视频帧表的 `frame` 列同为引用态；扫描阶段不解码像素，`frame` 仅在被查询引用时才进入解码。

### 视频表的帧展开

帧展开发生在 `USING VIDEOS` 的扫描算子内部，不引入表值函数或独立逻辑节点：

- 扫描按建表 `WITH (fps = ...)` 声明的采样率把每个视频文件展开为帧行，输出 `uri`、`ts`、`pts_ms`、`frame_id`、引用态 `frame`，并把 `duration` 等文件属性作为常量列透传；
- 需要不同采样率时对同一目录再建一张表；表只是逻辑定义，零拷贝；
- `fps` 是显式采样目标，按 PTS 而不是帧序号采样，支持 VFR；
- `pts_ms` 始终表示媒体内相对时间；`ts` 是事件时间。存在可信 capture/start metadata 时使用 `start_time + pts`，否则使用表选项 `start_time`；两者都没有时使用 Unix epoch 作为可重复的合成原点，并在 schema/指标中标记 `synthetic_event_time`；
- 时间谓词下推为 `time_range`，容器支持时先 seek 到范围附近；
- 顺序解码与稀疏 seek 的选择由媒体运行时依据采样比、GOP 和存储能力决定。稀疏 seek 未经 PoC 前不作为吞吐承诺；
- 元数据查询若不读取 `frame` 像素，只生成帧定位信息。

## 与顶层设计的关系

- 遵守 [design.md](../design.md) §8.1 固定的 IMAGES / VIDEOS 最小 schema，不改变约定列的含义；
- 帧展开位于扫描算子内部而非独立逻辑节点，与 design.md §4.1 一致；
- 输出的 `IMAGE` 遵守 design.md §6.2 的三态载荷不变量（扫描只产出引用态）；
- 时间谓词与显式采样下推对应 design.md §9 的优化规则 R4 / R5。

## 测试与验收

对应 design.md §14.1「媒体」测试行中的批部分：固定图片/视频、VFR、不同 GOP、损坏帧、采样 PTS 正确性；PRD 验收场景 A（图片目录建表后 5 分钟内出结果）依赖本 feature。

## 开放问题

| 问题 | 决策前需要的证据 | 最迟时间 |
|---|---|---|
| 稀疏视频采样是否真的降低解码成本 | 不同 GOP、VFR、本地盘与 S3 range-read 基准 | v0.1 性能承诺前 |

## 变更记录

| 日期 | 变更 |
|---|---|
| 2026-08-06 | 从引擎设计 v0.5.0 §8.1～§8.2 迁出成文 |
