# 0007: Parquet Sink

- **编号**：0007
- **状态**：Draft
- **目标版本**：v0.3
- **对应 PRD**：[prd.md](../prd.md) §3.1（Sink）、§3.3.6
- **依赖设计**：[design.md](../design.md) §8（Sink 公共契约与表 provider 概览）、§6.2（IMAGE 编码态）
- **关联 proposal**：无
- **最后更新**：2026-08-06

## 摘要

v0.3「结果落盘 Parquet」feature：把查询结果写入 Parquet 文件（含 `CREATE TABLE ... AS SELECT`），落盘表可被再次查询与回查。这是事件帧留存和结果物化的默认本地格式。

## 动机与范围

范围是 Parquet 写出行为与读回的逻辑类型恢复。`CREATE SINK` 公共契约见 [design.md](../design.md) §8.3；Lance 落盘随 v0.4 跨模态检索交付（proposal 0008）。

## 详细设计

- 批查询直接追加写出；流输出使用滚动文件，按时间或大小封卷，临时文件原子 rename，避免下游读到半个文件；
- `CREATE TABLE ... AS SELECT` 自 v0.3 起支持 Parquet（此前返回版本明确的未支持错误，见 design.md §7.1）；
- VisionQL 自己写出的 Parquet 文件保存逻辑类型 metadata，读回时恢复 `IMAGE/BOX2D` 等逻辑类型；Parquet 表 provider 支持列裁剪、谓词下推和统计信息；
- `IMAGE` 落盘为编码态值；落盘表中的媒体可通过 locator 随时重新授权解引用（live 流的瞬时帧需先经事件帧留存落盘才可回查，见 prd.md §3.3.6 与 design.md §6.2）。

## 与顶层设计的关系

- 遵守 design.md §8.3 的 Sink 公共契约（登记与校验时机、取消/超时/有界缓冲、协调器统一重试）；
- `IMAGE` 编码态落盘遵守 design.md §6.2 的载荷不变量（`arena_id/arena_slot` 绝不落盘）；
- 读回的逻辑类型恢复遵守 design.md §6.1 的 Arrow 扩展类型约定。

## 测试与验收

对应 design.md §14.1「Sink」测试行中随 v0.3 交付的部分：滚动文件原子性、逻辑类型写出/读回 round-trip、schema 校验、取消与背压。

## 开放问题

无。

## 变更记录

| 日期 | 变更 |
|---|---|
| 2026-08-06 | 从引擎设计 v0.5.0 §8.4 迁出成文 |
