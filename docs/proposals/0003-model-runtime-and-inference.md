# 0003: 模型运行时与 InferenceExec

- **编号**：0003
- **状态**：Draft
- **目标版本**：v0.1
- **对应 PRD**：[prd.md](../prd.md) §3.1（Model）、§3.3.2、§3.6
- **依赖设计**：[design.md](../design.md) §4（`Inference` 节点）、§7.3（MODEL 与 FUNCTION）、§9（推理提取规则）
- **关联 proposal**：无
- **最后更新**：2026-08-06

## 摘要

v0.1「SQL 内模型推理」feature：模型的加载与来源完整性、processor 前后处理、`InferenceExec` 的异步批推理路径，以及跨查询共享的调度与 batching。模型调用在规划期提取为显式 `Inference` 节点（提取规则见 [design.md](../design.md) §9），本文负责该节点的运行时实现。

## 动机与范围

范围包括模型后端接口、模型来源解析与完整性校验、`InferenceExec` 批路径与调度。MODEL / FUNCTION 的目录语义、语义指纹与 determinism 规则属于贯穿性设计，见 design.md §7.3；推理提取与去重规则见 design.md §9。

## 详细设计

### 运行时接口

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

### 模型来源与完整性

- `file://`、`hf://` 和 `endpoint://` 由独立 resolver 处理；
- 浮动的 Hugging Face revision 在首次解析时固定为不可变 commit，并记录内容哈希；
- 下载使用临时文件，哈希校验后原子放入内容寻址缓存；
- 离线环境可以只使用本地路径或预热缓存；
- endpoint URL 的鉴权通过 secret 引用注入，不写入模型 DDL 的可见输出。

模型可执行性由 manifest 决定，不能仅凭 `TYPE OBJECT_DETECTION` 猜张量布局。解析后的 manifest 至少包含 backend artifact、输入/输出张量、processor ID 与版本、图像尺寸/归一化、标签表、入口名称，以及嵌入维度（如适用）。来源可以是仓库内的 `visionql-manifest.json`、内置已测试模型清单，或用户显式指定的受支持 processor 配置。

v0.1 不在引擎内嵌 PyTorch，也不隐式执行任意仓库代码。`hf://` 来源没有可用 ONNX artifact 或受支持 manifest 时，`CREATE MODEL` 直接说明需要的 artifact/endpoint。远程 endpoint 应提供或由用户声明模型 revision；无法固定 revision 时对象标记为 `mutable_endpoint`，`EXPLAIN` 和作业详情显示可复现性警告，且后续版本不得对它启用跨查询结果缓存。

### `InferenceExec` 批路径

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

批视频通常不需要帧仓：`InferenceExec` 可以把「读取 → 解码 → 前处理」融合在一个算子内（见 design.md §6.3）。

### 调度与 batching

- 每个 `ModelInstanceKey`（Model semantic fingerprint、设备和运行时配置代次）有一个队列；
- 请求按 `interactive`、`stream`、`batch` 三类进入加权公平队列，流请求可带 deadline，批任务不能无限挤占流 SLO；
- 达到 `max_batch` 或最早 deadline / `max_wait` 时发车；具体 batch 大小、等待时间和 GPU 选择是运行时配置，不写进 FUNCTION；
- 张量缓冲按最大在途批次预分配并复用；队列满时提交端等待，背压回传；
- v0.1 默认单设备。显存不足时在加载阶段失败并给出模型、估算需求和可选 endpoint，不在运行中用未经验证的 LRU 换出；
- 运行时记录实际 batch 分布、排队时间、推理时间和设备利用率，为 v0.3 Workbench 实测成本面板提供数据。

## 与顶层设计的关系

- `Inference` 节点的规划期提取、去重与常量提升规则由 design.md §9 定义，本文只实现物理执行；
- 语义指纹、determinism（`deterministic` / `stable_within_query` / `volatile`）与参数三层归属遵守 design.md §7.3；
- 推理队列与张量缓冲纳入 design.md §11.1 的统一资源预算；
- 后端、processor 通过窄 trait 注册，遵守 design.md §2 的扩展约束 G8。

## 测试与验收

对应 design.md §14.1「推理」测试行：固定小模型数值回归、processor、semantic fingerprint、deterministic/volatile 去重边界、batching 公平性、取消、显存不足。

## 开放问题

无（模型级联、跨查询缓存等属于未排期方向，见 design.md §15）。

## 变更记录

| 日期 | 变更 |
|---|---|
| 2026-08-06 | 从引擎设计 v0.5.0 §9.1～§9.4 迁出成文 |
