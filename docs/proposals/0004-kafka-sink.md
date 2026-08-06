# 0004: Kafka Sink

- **编号**：0004
- **状态**：Draft
- **目标版本**：v0.2
- **对应 PRD**：[prd.md](../prd.md) §3.1（Sink）、§3.3.6
- **依赖设计**：[design.md](../design.md) §8.3（Sink 公共契约）、§6.2（IMAGE 编码态）
- **关联 proposal**：无
- **最后更新**：2026-08-06

## 摘要

v0.2「结果写出 Kafka」feature：把批查询或持续查询的结果以 JSON 写入 Kafka topic，是流分析结果进入下游告警/业务系统的默认通道。

## 动机与范围

范围是 Kafka connector 的编码规则与写入行为。`CREATE SINK` 语义、schema 校验时机、取消/超时/有界缓冲等公共契约见 [design.md](../design.md) §8.3，本文不重复。

## 详细设计

- JSON 标量按稳定规则编码，字段名与查询输出列名一致；编码规则进入回归测试，不随实现细节漂移；
- `IMAGE` 默认只输出脱敏 URI、locator 和元数据，必须显式 `TO_JPEG` 才输出 base64 字节，避免把原始像素隐式塞进消息流；
- 写入失败的重试策略由作业协调器统一管理（design.md §8.3）；缓冲有界，背压回传至上游。

## 与顶层设计的关系

- 遵守 design.md §8.3 的 Sink 公共契约：`CREATE SINK` 只登记连接信息，第一次 `INSERT INTO` 规划时完成输出 schema 与 format 校验；写入支持取消、超时和有界缓冲；
- `IMAGE` 输出遵守 design.md §6.2 的载荷不变量：`arena_id/arena_slot` 不落盘、不出进程，`uri` 只用于展示。

## 测试与验收

对应 design.md §14.1「Sink」测试行：schema 校验、取消、背压、Kafka JSON IMAGE 编码规则。PRD 验收场景 B 的流结果写入 Kafka 依赖本 feature。

## 开放问题

无。

## 变更记录

| 日期 | 变更 |
|---|---|
| 2026-08-06 | 从引擎设计 v0.5.0 §8.4 迁出成文 |
