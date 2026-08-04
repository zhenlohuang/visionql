# VisionQL 产品需求文档（PRD）

> VisionQL 是一个面向多模态数据的批流一体查询与处理引擎。用户可以通过 SQL 或 DataFrame API 查询和处理图片、视频文件及实时视频流。

- **版本**：v0.1.3（Draft）
- **日期**：2026-08-03
- **状态**：评审中

---

## 1. 一句话定义

**VisionQL：物理世界的查询引擎。**（*A query engine for the physical world.*）

传统数据库主要查询业务系统中的结构化记录。VisionQL 则把摄像头和视频记录的内容变成可以查询的数据。工程师和分析师可以用一条 SQL，或几行 DataFrame 代码，对图片、视频和实时视频流进行分析，减少手写 Python 管道和管理 GPU 推理任务的工作。

- 图片和视频文件使用**批处理**，属于有界数据集，类似 Spark batch。
- RTSP、WebRTC 或 Kafka 帧流使用**流处理**，属于无界数据集，类似 Flink 或 Spark Structured Streaming。
- 批处理和流处理共享同一套 SQL / DataFrame 语义。

---

## 2. 需求合理性分析

### 2.1 背景与趋势

1. **视觉数据的增长快于现有处理能力**。企业新增数据中有 80%～90% 是非结构化数据，其中包含大量图片、监控视频、行车记录和直播流。现有数据基础设施大多围绕结构化或半结构化数据构建。对视觉数据，主流引擎通常只能保存文件路径，无法直接理解和查询画面内容。

2. **视觉模型已经具备可用的查询能力**。随着检测、跟踪、OCR、多模态大模型（VLM）和向量检索逐渐成熟，“画面中有几个人”或“找出所有闯入禁区的片段”这类问题已经可以通过程序回答。当前缺少的是一层统一的数据系统，用来组织模型、数据和查询。

3. **声明式接口可以降低使用门槛**。早期大数据处理依赖手写 MapReduce 作业，随后 Hive 和 Spark SQL 通过声明式接口与优化器显著降低了开发成本。今天的视觉数据处理仍然大量依赖 OpenCV 和 PyTorch 脚本，适合用类似的方法进一步抽象和标准化。

### 2.2 现状痛点（为什么现有方案不够）

| 现有方案 | 不足 |
|---|---|
| **Spark / Flink** | 主要面向结构化数据。视觉处理通常只能放进黑盒 UDF，优化器无法下推采样或时间条件，也难以复用推理结果。视频解码、GPU 调度和模型 batching 仍需用户自行管理。 |
| **自建 Python 管道**（OpenCV + PyTorch + Celery/Airflow） | 容易形成一次性脚本，缺少统一的优化、增量计算和容错语义。批处理与流处理往往使用两套代码，分析师也很难直接参与。 |
| **研究系统**（EvaDB、BlazeIt、VIVA 等） | 已经证明 SQL over video 可行，并展示了模型级联和帧采样等优化空间。但这些系统大多是单机原型，偏重批处理，尚未提供完整的生产级流处理能力。 |
| **多模态数据框架**（Daft、Ray Data、LanceDB） | 擅长多模态数据的存取和并行计算，但定位仍是通用计算或存储层。视觉原生算子、流处理和 SQL 能力通常不完整。 |
| **云视觉 API**（Rekognition、阿里云视觉智能等） | 使用方便，但模型和执行过程不可控。复杂查询难以组合，通常不能使用自有模型；成本随处理帧数增长，数据也可能需要离开本地环境。 |

**结论**：现有方案很少同时提供视觉原生算子、声明式 SQL、批流一体和可解释优化。随着视觉模型逐渐成熟、GPU 资源更易获得，以及企业积累的视频数据持续增长，VisionQL 有明确的产品机会。

### 2.3 目标用户与使用场景

**目标用户**（按优先级）：

1. **数据和算法工程师**：目前负责开发视觉处理管道的用户，也是首个版本的核心用户，主要通过 pip 包使用库态能力。
2. **数据分析师**：熟悉 SQL，但不熟悉 PyTorch。v0.1 可以通过基础 Flight SQL 服务使用 VisionQL；v0.2 起可以通过 Workbench 或 BI 工具使用具备认证和权限控制的生产服务。
3. **平台团队**：希望将视觉分析建设成内部平台，需要多租户、治理和成本控制能力，也是 v1.0 企业版的主要购买方。
4. **AI 应用和 Agent 开发者**：把 VisionQL 作为 Agent 的视觉查询工具，通过 SQL 或 MCP 查询摄像头与视频库，用于回答问题或触发后续操作。现有 text-to-SQL 能力也可以直接复用。

**典型场景**：

| 场景 | 模式 | 典型问题 |
|---|---|---|
| 安防 / 智慧园区 | 流 | 每分钟画面人数、越界/闯入告警、逗留检测、跨摄像头轨迹 |
| 零售客流分析 | 流 | 进店人数、动线热力、货架前停留时长、排队长度 |
| 自动驾驶 / 机器人数据闭环 | 批 | 从大规模路采或遥操作视频中检索“雨天 + 行人横穿 + 遮挡”等 corner case，用于训练与评测 |
| 媒资 / 内容平台 | 批 | 视频打标、人物/场景检索、精彩片段抽取、“以文搜片” |
| 内容审核 | 批 + 流 | 实时检测直播流中的违规内容，并回扫已有内容；同一份规则用于两种执行模式 |
| 工业质检 | 流 | 产线相机流缺陷检测、良率分钟级聚合、异常帧留存 |
| 无人机 / 设施巡检 | 批 | 电力/管线/光伏巡检视频的缺陷检索、跨期变化对比、工单证据帧 |
| 体育 / 赛事分析 | 批 | 球员跟踪、阵型统计、射门或犯规等事件检索 |
| AI Agent / 智能助手 | 批 + 流 | Agent 通过工具提问，例如“仓库里是否有人逗留超过 10 分钟”，再将自然语言转换为 SQL 并返回画面结果；也可用于视觉 RAG |

内容审核可以直接体现**批流一体**的价值：同一条 SQL 查询文件表时用于历史回扫，查询实时流时用于实时拦截。虽然可覆盖的场景很多，首发版本只会重点做好一个场景。选择标准是当前痛点、数据规模和付费意愿，详见开放问题 1。其他场景后续通过开源场景包逐步扩展。

### 2.4 产品价值

1. **提高开发效率**。“统计每分钟画面人数并写入 Kafka”这类任务，可以从数百行 Python 和部署脚本缩减为十几行 SQL。数据分析师也能直接参与视频查询。
2. **通过优化降低 GPU 成本**。GPU 推理通常是视觉查询中最昂贵的部分，而声明式查询为系统提供了以下优化空间：
   - **帧采样下推**：分钟级聚合不需要按 30fps 对所有帧进行推理。
   - **模型级联**：先用小模型过滤无关画面，只把候选帧交给大模型。
   - **将谓词下推到解码层**：只解码查询需要的时间段或关键帧。
   - **物化并复用推理结果**：同一路视频流上的多个查询可以共享一次检测结果。

   黑盒 UDF 很难支持这些优化。将模型调用显式放入查询计划，是 VisionQL 相比手写管道的主要技术优势。
3. **批处理和流处理共享一份逻辑**。用户可以先在历史视频上验证查询，再将同一查询用于实时流，减少两套实现之间的维护成本和语义差异。
4. **让视频数据可以持续复用**。模型、数据源和查询结果都作为目录对象管理，可以追踪来源并控制权限。推理结果物化后，新查询可以优先读取已有结果，避免重复运行模型。
5. **默认支持数据留在本地环境**。从 v0.1 起，引擎就可以部署在数据附近，不要求上传视频。v1.0 的审计和数据来源追踪能力将进一步支持个保法、GDPR 等合规要求，这对安防和零售场景尤其重要。
6. **适合作为 Agent 的视觉查询接口**。Agent 可以把自然语言转换为 SQL，再从图片、视频和实时流中获得可审计、可限制权限的结果。现有的 text-to-SQL 生态也能降低接入成本。

### 2.5 风险与挑战

| 风险 | 说明 | 缓解 |
|---|---|---|
| **推理成本仍然较高** | 即使性能提升 10 倍，全量分析大规模视频仍会消耗大量 GPU | 在 `EXPLAIN` 中预估 GPU 时长，允许用户明确控制采样率，并在 v0.3 引入结果可解释的模型级联策略 |
| **查询结果具有概率性** | 检测模型可能漏检或误检，因此 `COUNT(*)` 不再表示绝对准确的事实 | 将置信度和阈值明确写入查询；聚合层是否需要专用的置信度语义，见开放问题 3 |
| **SQL 的表达能力有限** | 标定和复杂的多目标关联规则不适合全部放进 SQL | 不追求所有逻辑都用 SQL 表达。UDF、用户自定义模型和 DataFrame API 用于承载复杂逻辑 |
| **连接器和模型生态需要时间建设** | 引擎的实用性依赖数据源、模型和场景模板 | 首发聚焦检测、跟踪、嵌入和 VLM 问答四类高频能力，以及 RTSP、S3、Kafka 三类连接器。先做好安防或审核中的一个场景，再逐步扩展 |
| **大型平台可能补齐类似能力** | Databricks 或云厂商可能继续扩展多模态处理能力 | 重点做好批流一体和视觉原生优化，并通过开源建立用户和生态 |

---

## 3. 产品设计：用户如何使用 VisionQL

### 3.1 核心抽象

VisionQL 采用一个统一抽象：**视觉数据最终都可以表示为由帧组成的关系表**。

| 抽象 | 说明 |
|---|---|
| **多模态类型系统** | 在标准 SQL 类型之外增加 `IMAGE`、`VIDEO`、`AUDIO`、`BOX2D`（检测框）、`MASK`、`VECTOR(n)`（嵌入向量）以及 `STRUCT`/`ARRAY` 嵌套类型。v1 重点支持视觉模态；`AUDIO` 和 `MASK` 暂时只保留类型定义。`AUDIO` 的首个目标场景是直播审核中的音画联合判断 |
| **Table（表）** | 有界数据集。图片目录可以作为一张表，每行一张图片；视频目录也可以作为一张表，每行一个视频，并可进一步展开为帧 |
| **Stream（流）** | 无界数据集。RTSP、摄像头或 Kafka 帧流都表示为帧表，例如 `(ts TIMESTAMP, frame IMAGE, ...)`，并带有事件时间和水位线 |
| **Model（模型）** | 资源层对象，只保存权重来源、版本、任务类型、精度和 SLO 等影响成本或延迟的定义。GPU 放置、副本数和动态 batching 由运行时决定。模型参与资源调度，但不会直接出现在查询中 |
| **Function（函数）** | 查询中唯一可以调用的接口，保存签名、绑定参数和实现引用等影响结果的定义。实现可以是 `USING MODEL`、`LANGUAGE PYTHON` 或 SQL 宏。一个模型可以派生多个函数，优化器也可以在不修改查询的情况下调整函数绑定的模型 |
| **窗口** | 流数据的聚合单位。`TUMBLE` 是时间分桶标量函数，可以直接用于 `GROUP BY`；在批模式下，它就是普通的时间分桶聚合。`HOP` 和 `SESSION` 涉及行复制和跨行状态，因此使用表值函数表示 |
| **Sink / 物化视图** | 查询结果的输出位置，例如 Kafka、Parquet/Lance、告警 Webhook，或持续更新的物化视图 |

批流一体的关键是：**表和流使用同一套查询语言**。`FROM` 表时执行批任务，`FROM` 流时执行持续查询，窗口聚合等核心语义保持一致。

### 3.2 五分钟用户旅程

```bash
pip install visionql
visionql shell          # 交互式 SQL,或在 Python 中 import visionql
```

下面用一个完整任务说明基本流程：实时统计门口摄像头每分钟的平均人数，并写入 Kafka。

```sql
-- ① 注册视频流(声明源、采样率)
CREATE STREAM cam_entrance
FROM 'rtsp://10.0.0.15:554/main'
WITH (fps = 5, event_time = 'capture_time', watermark = INTERVAL '2' SECOND);

-- ② 注册模型,并同时派生查询函数 detect(1:1 语法糖,详见 3.3.2)
-- 函数按能力命名而非按模型命名——换绑模型时查询一行不改(见 3.3.3)
CREATE MODEL yolo
TYPE OBJECT_DETECTION
FROM 'hf://ultralytics/yolo26n'
FUNCTION detect;

-- ③ 声明输出
CREATE SINK people_per_minute
TO 'kafka://broker:9092/people-count'
FORMAT JSON;

-- ④ 一条持续查询,上线即运行
INSERT INTO people_per_minute
SELECT TUMBLE(ts, INTERVAL '1' MINUTE) AS window_start,
       AVG(person_cnt) AS avg_people,
       MAX(person_cnt) AS peak_people
FROM (
  SELECT ts,
         COUNT_OBJECTS(detect(frame), 'person', 0.6) AS person_cnt
  FROM cam_entrance
)
GROUP BY 1;
-- COUNT_OBJECTS(检测结果, 标签, 置信度阈值) 是内置数组函数,见 3.3.8 设计原则
```

整个任务分为四步，约 20 行 SQL，不需要编写 Python 或部署脚本。交互模式下，持续查询在前台运行，适合开发和调试；生产环境中的常驻运行方式见 3.5。

首次使用时不需要准备摄像头或 Kafka。将 `FROM` 改为本地视频或图片目录表，再把 Sink 改为 console，就能在本地完成同一流程。“不依赖外部服务即可得到第一个结果”也是 MVP 的验收场景之一（第 4 节场景 B）和第 7 节 TTFV 指标的口径。下面按主题说明 SQL 设计。

### 3.3 SQL 设计详解

#### 3.3.1 数据源注册（DDL）

```sql
-- 批:图片目录即表,每行一张图片
CREATE TABLE product_photos
USING IMAGES
LOCATION 's3://bucket/photos/'
WITH (recursive = true);
-- schema: (uri STRING, image IMAGE, width INT, height INT, captured_at TIMESTAMP, ...)

-- 批:视频文件目录即表,每行一个视频
CREATE TABLE traffic_videos
USING VIDEOS
LOCATION 's3://bucket/dashcam/2026/07/';
-- schema: (uri STRING, video VIDEO, duration DOUBLE, fps DOUBLE, ...)

-- 流:注册一路 RTSP 摄像头
CREATE STREAM cam_entrance
FROM 'rtsp://10.0.0.15:554/main'
WITH (
  fps        = 5,                        -- 引擎按需采样,而非全帧率摄入
  event_time = 'capture_time',
  watermark  = INTERVAL '2' SECOND
);
-- schema: (ts TIMESTAMP, frame IMAGE, frame_id BIGINT, source STRING)

-- 流也可以来自消息队列(帧已被上游发布)
CREATE STREAM cam_all
FROM 'kafka://broker:9092/camera-frames'
FORMAT FRAME_JPEG
WITH (event_time = 'ts', watermark = INTERVAL '5' SECOND);
```

#### 3.3.2 模型注册

**MODEL 是资源层对象**，用于声明权重来源、版本、任务类型和资源约束。它只保存影响成本、延迟或部署方式的信息，并参与资源调度，不直接出现在查询计划中。可以把它类比为 Postgres FDW 的 `SERVER`，或表背后的 Parquet 文件。

```sql
-- 只声明"是什么",放哪块 GPU、batch 多大等部署决策由引擎运行时负责,默认零配置
CREATE MODEL yolo
TYPE OBJECT_DETECTION
FROM 'hf://ultralytics/yolo26n';

-- 嵌入模型与远程端点同样是模型
CREATE MODEL clip TYPE EMBEDDING FROM 'hf://openai/clip-vit-base-patch32';
CREATE MODEL qwen_vl TYPE VQA FROM 'endpoint://http://vlm-serving:8000';

-- 简写语法：在 1:1 场景中，一条语句同时注册模型并创建对应函数。
-- 初次使用时只需调用 FUNCTION；需要一对多、切换模型或管理资源时再操作 MODEL
CREATE MODEL yolo_l
TYPE OBJECT_DETECTION
FROM 'hf://ultralytics/yolo26l'
FUNCTION detect_l;
```

**模型定义与部署相互独立**。`CREATE MODEL` 不接受设备、副本数和 batch 大小等物理部署参数，这些参数由运行时根据负载动态决定。`WITH` 子句只接受声明式约束和元信息，例如 `precision = 'fp16'`（精度）、`latency_slo = '50ms'`（延迟目标）和 `resource_group = 'gpu-pool-a'`（多租户资源池）。确实需要固定部署方式时，可以通过 `ALTER MODEL` 进行运维配置，但不会把这些细节写进建模语句。

模型下载、版本固定、GPU 放置、动态 batching 和失败重试均由引擎负责。

#### 3.3.3 函数注册

**FUNCTION 是查询中唯一可以调用的接口**，查询只调用函数，不直接引用模型。可以把它类比为 FDW 中的 `FOREIGN TABLE`。

MODEL 和 FUNCTION 的职责按以下规则划分：**影响查询结果的内容属于 FUNCTION，例如签名、绑定参数和实现引用；只影响成本、延迟或部署的内容属于 MODEL，例如权重、版本、精度和 SLO。** `WITH` 子句会按这一规则校验参数。资源参数写在函数上，或结果语义参数写在模型上，都会直接报错并提示正确位置。

```sql
-- TYPE 蕴含标准签名,签名与 RETURNS 可省略
-- (OBJECT_DETECTION 标准签名: (IMAGE) -> ARRAY<STRUCT<label STRING, confidence FLOAT, box BOX2D>>)
CREATE FUNCTION detect USING MODEL yolo;

-- 同一模型派生带绑定参数的函数(WITH 只收影响结果的语义参数)
CREATE FUNCTION person_det USING MODEL yolo
WITH (classes = ['person'], min_confidence = 0.5);

-- 一对多：一份 CLIP 权重提供图片和文本两个入口，用于跨模态检索
CREATE FUNCTION embed_image(img IMAGE) RETURNS VECTOR(512) USING MODEL clip;
CREATE FUNCTION embed_text(txt STRING) RETURNS VECTOR(512) USING MODEL clip;

-- VLM 问答函数
CREATE FUNCTION vlm USING MODEL qwen_vl;
```

FUNCTION 的定义由五个相互独立的部分组成。新增能力通常只需要扩展其中一项，而不需要引入新的语法结构：

```
CREATE [OR REPLACE] FUNCTION name [(param type, ...)] [RETURNS type]
  <实现子句>
  [WITH (绑定参数)]
```

| 槽位 | v0.1 | 预留扩展 |
|---|---|---|
| **形状** | 标量函数 | `CREATE AGGREGATE FUNCTION`、`CREATE TABLE FUNCTION`(如用户自定义跟踪器替换内置 `TRACK`) |
| **签名** | 显式声明,或由模型 `TYPE` 推导 | 重载(同名多签名) |
| **实现子句** | `USING MODEL m`(资源引用型)、`LANGUAGE PYTHON AS '<入口>'`(代码型)、`AS (<表达式>)`(SQL 宏) | 新资源类别扩 `USING` 后的枚举;新语言扩 `LANGUAGE` 后的枚举(如 `WASM`) |
| **WITH 绑定参数** | 影响结果的常量，例如类别、阈值和 prompt 模板 | 按前述职责边界校验，不接受资源参数 |
| **元属性** | 确定性、是否支持 batching 和成本信息，由实现类型自动推导 | 仅供优化器使用，不增加用户语法 |

```sql
-- 三种实现形状,同一个 FUNCTION 概念
CREATE FUNCTION detect USING MODEL yolo;                -- 资源引用型:引擎托管推理

CREATE FUNCTION blur_score(img IMAGE) RETURNS FLOAT
LANGUAGE PYTHON AS 'myops.quality:blur_score';          -- 代码型：用于自定义处理逻辑

CREATE FUNCTION is_large(b BOX2D) RETURNS BOOLEAN
AS (b.w * b.h > 0.25);                                  -- SQL 宏:纯表达式复用,解析期内联展开
```

**关键字约定**:`USING` 统一表示"由已注册资源/provider 支撑"(与 `USING IMAGES`、`USING HNSW` 一致);`AS` 保留给实现体本身(CTAS 的 `AS SELECT`、Python UDF 的 `AS '<入口>'`)。模型绑定是资源引用而非函数体,故用 `USING MODEL`。

这种分层带来三个直接收益：

1. **生命周期相互独立**：可以通过 `ALTER MODEL` 升级版本或更换设备，而不修改函数接口；也可以通过 `ALTER FUNCTION person_det SET MODEL yolo_l` 切换模型，现有查询不需要改动。这为灰度发布和回滚提供了基础。因此，函数应按能力命名，例如 `detect`，而不是按具体模型命名。
2. **一份模型可以复用到多个函数**：CLIP 可以同时提供图片和文本入口，一个 VLM 端点也可以派生多个使用不同 prompt 模板的函数，而权重只需加载一次。
3. **优化器可以明确识别模型成本**：模型级联可以表示为两个相同 `TYPE` 的模型，例如先用 `yolo`（26n）过滤，再用 `yolo_l`（26l）确认。优化器可以在函数接口不变的情况下选择执行策略；`EXPLAIN` 则从模型对象读取 GPU 成本信息。

#### 3.3.4 查询一：视频或流中人的位置

检测函数返回数组，使用 `UNNEST` 可以将数组展开为关系行。这是把视觉结果转换为关系数据的基本方式。语法采用类似 BigQuery 的隐式关联写法：`FROM t, UNNEST(expr) AS x`。

```sql
-- 流上:实时输出每个人的位置框
SELECT ts,
       det.box,           -- BOX2D: (x, y, w, h),可取 .center 中心点
       det.confidence
FROM cam_entrance,
     UNNEST(detect(frame)) AS det
WHERE det.label = 'person'
  AND det.confidence > 0.6;
```

```sql
-- 批上:同样的写法,只是先把视频表展开为帧表
-- FRAMES() 是"表进表出"的表值函数:输入视频表,输出帧表(原表各列透传),fps 参数即采样下推
SELECT f.uri, f.ts, det.box
FROM FRAMES(TABLE traffic_videos, fps => 1) AS f,
     UNNEST(detect(f.frame)) AS det
WHERE det.label = 'person';
```

空间谓词可以直接按位置过滤，例如检测目标是否进入禁区：

```sql
-- 出现在禁区多边形内的人 → 告警
SELECT ts, det.box
FROM cam_entrance, UNNEST(detect(frame)) AS det
WHERE det.label = 'person'
  AND ST_CONTAINS(POLYGON('(0.6,0.1),(0.95,0.1),(0.95,0.8),(0.6,0.8)'),
                  det.box.center);
```

#### 3.3.5 查询二：每分钟画面中有多少人

这个问题至少有两种常见含义。SQL 可以分别表达，并把两者的差异明确写在查询中。

**语义 A——每分钟画面内平均/峰值人数**(每帧数人,窗口聚合):

```sql
SELECT TUMBLE(ts, INTERVAL '1' MINUTE) AS window_start,
       AVG(person_cnt) AS avg_people,
       MAX(person_cnt) AS peak_people
FROM (
  SELECT ts,
         COUNT_OBJECTS(detect(frame), 'person', 0.6) AS person_cnt
  FROM cam_entrance
)
GROUP BY 1;
```

**语义 B——每分钟经过了多少个不同的人**(需要跨帧跟踪,`TRACK` 算子为每个目标分配稳定 `track_id`):

```sql
SELECT TUMBLE(ts, INTERVAL '1' MINUTE) AS window_start,
       COUNT(DISTINCT track_id) AS unique_people
FROM TRACK(TABLE cam_entrance,
           DETECTOR => detect,
           CLASS    => 'person')          -- 表进表出,输出: (ts, track_id, box, confidence)
GROUP BY 1;
```

将同一条语句的 `FROM` 改为历史视频表，就可以执行批量回算，其他查询逻辑保持不变。

#### 3.3.6 查询三：跨模态语义检索（以批处理为主）

```sql
-- 以文搜图:找出最像"戴红色安全帽的工人"的 20 张图
-- embed_image / embed_text 是同一 CLIP 模型派生的两个函数(见 3.3.3)
SELECT uri, image
FROM product_photos
ORDER BY embed_image(image) <-> embed_text('a worker wearing a red helmet')
LIMIT 20;

-- VLM 自然语言谓词:从行车视频中检索"行人横穿马路"的片段
SELECT f.uri, f.ts
FROM FRAMES(TABLE traffic_videos, fps => 0.5) AS f
WHERE vlm(f.frame, 'Is a pedestrian crossing the road?') = 'yes';
```

向量列可以通过 `CREATE INDEX ... USING HNSW` 建立索引。存在索引时，`ORDER BY <-> LIMIT` 会自动改写为 ANN 检索。`<->` 只是简写，也可以使用等价的 `L2_DISTANCE(a, b)` 函数。

#### 3.3.7 结果输出：Sink 与物化视图

```sql
-- 持续查询写入 Kafka(见 3.2 完整示例)
INSERT INTO people_per_minute SELECT ...;

-- 物化视图:持续维护、可被再次查询,推理结果自动复用
CREATE MATERIALIZED VIEW entrance_tracks AS
SELECT * FROM TRACK(TABLE cam_entrance, DETECTOR => detect, CLASS => 'person');

-- 下游多个查询共享同一份跟踪结果,不再重复推理
SELECT ... FROM entrance_tracks GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE);

-- 事件帧留存:告警同时把证据帧存下来
INSERT INTO evidence  -- Lance/Parquet 表,IMAGE 列原生存储
SELECT ts, frame, det.box
FROM cam_entrance, UNNEST(detect(frame)) AS det
WHERE det.label = 'person' AND det.confidence > 0.9;
```

#### 3.3.8 SQL 可落地性设计原则

上述语法都限制在成熟列式查询引擎现有的扩展能力之内。每一种扩展语法都必须映射到标准扩展机制，避免修改查询引擎内核：

1. **所有扩展都转换为三类标准机制**：
   - **标量函数**（包括异步远程调用）：用于模型推理（`detect`、`vlm`）、数组处理（`COUNT_OBJECTS`）以及空间和向量谓词（`ST_CONTAINS`、`L2_DISTANCE`）。函数按 RecordBatch 向量化执行，为推理 batching 提供基础。SQL 宏（`AS (<表达式>)`）在解析时内联，不产生运行时实体。标量、聚合和表值函数分别使用查询引擎对应的扩展点。
   - **表值算子**：`FRAMES`、`TRACK`、`HOP` 和 `SESSION` 都接收表并返回表。SQL 层将它们转换为自定义逻辑计划节点和物理算子，不依赖逐行关联的 correlated lateral 表函数。
   - **DDL 对应目录操作**：`CREATE STREAM/MODEL/FUNCTION/SINK/MATERIALIZED VIEW` 由 VisionQL 方言层解析，并写入 Catalog 或运行时，不进入查询计划。函数会注册到查询引擎的函数表；模型只保存在目录和模型运行时中。
2. **不引入 lambda 或高阶函数**。数组处理统一使用命名内置函数，例如 `COUNT_OBJECTS(dets, label, min_conf)`。保持一阶表达式可以简化谓词分析和下推优化。
3. **`UNNEST` 是唯一的行展开方式**。`FROM t, UNNEST(expr) AS x` 直接映射到查询引擎原生的展开节点，不要求通用 LATERAL 关联能力。
4. **`TUMBLE` 在批处理和流处理中保持一致**。批模式下，它转换为普通的时间分桶聚合；流模式下，运行时为同一计划附加窗口状态和水位线。语法和查询逻辑保持不变。

   `TUMBLE` 不占用 `WINDOW` 关键字。ANSI SQL 中的 `WINDOW`/`OVER` 表示逐行分析，结果行数不变；流式窗口表示分组聚合，结果行数会减少。两者共用一个名称容易造成混淆，因此 `WINDOW` 保留给标准分析函数，例如轨迹平滑中的 `AVG(speed) OVER (PARTITION BY track_id ...)`。时间窗口沿用 `TUMBLE`、`HOP` 和 `SESSION` 三个名称，因为它们的语义和执行成本本就不同。
5. **自定义算子皆有函数等价形式**。`<->` 等运算符经表达式规划扩展映射为函数调用,方言不兼容时用户总有退路。
6. **多模态类型建立在标准列式类型之上**:`IMAGE`/`VIDEO` 为带元数据的二进制/结构列,`BOX2D` 为结构体,`VECTOR(n)` 为定长浮点列表——类型名只存在于 DDL 与文档层,不要求引擎具备用户自定义类型内核。

### 3.4 DataFrame API（Python）

SQL 之下是同一套逻辑计划,DataFrame 面向工程师,适合复杂管道与编程式组装:

```python
import visionql as vq

sess = vq.connect()

# 与 3.3.5 语义 B 等价
counts = (
    sess.stream("cam_entrance")
        .track(detector="detect", cls="person")            # (ts, track_id, box, ...)
        .window(vq.tumble("1 minute"))
        .agg(unique_people=vq.count_distinct("track_id"))
)
counts.write.kafka("broker:9092", topic="people-count").start()

# 批:图片目录打标后存表
(
    sess.table("product_photos")
        .with_column("tags", vq.fn("detect")(vq.col("image")))
        .with_column("embedding", vq.fn("embed_image")(vq.col("image")))
        .write.lance("s3://bucket/photo_index/")
)
```

SQL 是 VisionQL 的主要用户接口，便于分析师使用，也能让优化器理解查询意图。DataFrame API 提供等价的编程接口，适合工程化组装复杂流程。两者可以混用，`sess.sql(...)` 会返回 DataFrame。

### 3.5 产品形态与部署

VisionQL 需要同时满足三类不同的使用条件：

- **批量探索应尽量减少运维成本**：分析师和工程师应当在 `pip install` 后直接开始查询，而不是先部署集群。
- **流查询需要长期运行**：持续查询包含状态和故障恢复，模型需要常驻显存，GPU 也需要在多个查询之间复用。这些能力更适合运行在长生命周期的服务中，而不是临时脚本进程。
- **视频数据不适合大规模搬运**：一路 1080p 视频流约为 4Mbps，数十路视频同时回传到中心会带来明显的网络和合规压力。因此，引擎需要能够部署到摄像头附近，只返回 KB 级的结构化结果。

单一部署方式无法同时满足这些条件。因此，VisionQL 使用**同一个引擎内核，提供三种宿主形态**；SQL 和目录（Catalog）在三种形态之间保持一致。

| 形态 | 载体 | 覆盖场景 | 阶段 |
|---|---|---|---|
| **库态** `visionql` | pip 包，像 DuckDB 一样嵌入进程 | notebook 探索、批任务、CI 回归；开发阶段也可在前台运行流查询 | MVP |
| **服务态** `visionqld` | 由 `vql-server` crate 构建的单机守护进程；目录、模型运行时和流运行时都包含在一个二进制中 | v0.1 提供单可信主体的基础 Flight SQL 查询；v0.2 增加持久作业、恢复、安全和 GPU 池化 | v0.1 基础；v0.2 生产化 |
| **集群态** | 多节点，计算与模型服务可以独立扩缩容 | 大规模历史回扫和多租户平台 | v1.0 |

边缘部署不是第四种产品形态，而是服务态的一种部署位置。同一个 `visionqld` 二进制可以运行在摄像头附近的边缘设备上，包括 ARM 设备，就地完成解码和检测；中心侧只负责聚合和检索。长期目标是由规划器自动拆分同一条查询在边缘和中心的执行部分，详见 Roadmap。

**形态间的关键约定**:

1. **在 notebook 中验证，再用同一条命令运行**。库态中调试完成的 SQL 可以直接通过 `visionql run job.sql` 执行。未指定服务端时，任务在本地前台运行；通过 `--server` 或配置指定 `visionqld` 后，v0.1 通过 Flight SQL 远程执行并保持客户端附着，v0.2 可以把无界 Sink 查询提交为脱离客户端运行的持久作业。批处理和流处理不仅共享语言，也共享从开发到生产的使用路径。
2. **持续查询逐步交给服务态管理**。v0.1 服务端支持附着客户端的持续查询和取消；v0.2 为持久作业增加名称、状态、`SHOW QUERIES`、`PAUSE`、`RESUME`、恢复以及查询级指标。
3. **客户端使用标准列式协议**。服务态从 v0.1 起使用 Arrow Flight SQL；Python SDK、BI 工具和第三方应用通过 Flight SQL、ADBC 或 JDBC 连接，不增加私有协议。
4. **首次运行不要求外部依赖**。服务态二进制内置目录和模型运行时；Kafka、对象存储和 Kubernetes 都是可选集成，不是启动前提。
5. **Workbench 在 v0.2 接入已有服务态**。Workbench 是独立的轻量子项目，通过 Arrow Flight SQL 连接 `visionqld`。它既使用公开协议，也用于持续验证协议是否覆盖完整的客户端需求。能力和边界见 3.8。

### 3.6 执行层关键设计（简述）

以下内容不在 PRD 中展开实现细节，但它们是上述用户体验成立的前提，也为后续技术设计提供约束：

1. **优化器**:帧采样下推(聚合粒度反推所需 fps)、解码裁剪(时间/空间 ROI)、模型级联(小模型过滤 + 大模型确认)、公共推理子表达式消除、推理结果缓存(以模型版本 + 帧指纹为键);
2. **帧数据通路**：解码后的帧占用大量内存，1080p RGB 约为 6MB/帧，5fps 单流约为 30MB/s。`IMAGE` 列在查询计划中尽量使用引用或压缩表示并减少复制，解码延迟到推理或落盘前。
3. **GPU 感知调度**:模型自动 batching、算子与模型的共置、背压;
4. **流语义**:事件时间 + 水位线、断流重连;投递语义按源分档——可重放源(Kafka 帧流)至少一次(随 v0.2 Kafka 源生效)→ 精确一次(GA);不可重放 live 源(RTSP)为尽力而为,断流/丢帧缺口如实反映在结果里,不伪造;
5. **存储**:列式多模态格式(Lance/Parquet + 视频引用),物化视图增量维护;
6. **可观测**:`EXPLAIN` 展示预估 GPU 成本;每查询的推理次数/延迟/费用面板;
7. **代码型函数执行**：计算量大的模型推理使用 `USING MODEL`，由引擎管理 GPU；代码型函数主要用于轻量的数据处理。Python UDF 根据产品形态采用不同的执行方式：库态在宿主 Python 进程中调用，通过 Arrow 批传递数据，并利用批处理和原生库降低 GIL 影响；v0.1 基础服务态不执行 Python UDF，v0.2 服务态再使用进程外 Python worker，通过 Arrow IPC 通信，按函数隔离依赖，避免 worker 崩溃影响引擎，也可以通过多个 worker 提高并发。引擎内核不嵌入 Python 解释器，只有注册 Python 函数时才需要 Python 运行时。v1.0 再引入 WASM UDF，用于多租户代码隔离以及边缘和场景包分发。

### 3.7 非功能需求（NFR）

| 类别 | 要求 |
|---|---|
| **性能(MVP 基线)** | 单机 1×消费级 GPU:≥ 8 路 1080p@5fps 并发流上运行轻量检测 + 窗口聚合;批扫描吞吐以解码为瓶颈打满硬件;元数据/已物化结果的交互查询 P95 < 1s |
| **容错** | 流查询投递语义按源分档:可重放源至少一次(v0.2 Kafka 源起)→ 精确一次(GA);不可重放 live 源(RTSP)尽力而为,缺口如实反映、不伪造;断流自动重连;v0.2 起服务态重启后持久查询自动恢复,不丢目录状态 |
| **错误语义** | 单帧解码/推理失败默认不中断查询:该行结果置 NULL 并计入每查询的错误指标,失败率超阈值告警;严格模式 `on_error = 'fail'` 可选。模型输出的概率性(漏检/误检)不属于错误,由置信度阈值显式管理(见 2.5) |
| **安全与隐私** | "数据不出域"是默认架构(引擎去数据旁,而非数据上云);模型来源哈希固定、防篡改;服务态:TLS + 认证(v0.2),表/流级权限(v0.2),审计日志(v1.0);用户代码隔离:Python UDF 于服务态进程外执行(v0.2),多租户场景以 WASM 沙箱承接(v1.0) |
| **兼容性承诺** | SQL 方言与目录格式自 v1.0 起遵循语义化版本;`EXPLAIN` 输出与内部指标名不作为稳定接口 |

### 3.8 Workbench（Web 工作台）

Workbench 是 v0.2 面向生产服务态提供的 Web 图形界面，也是位于 `vql-workbench/` 的独立轻量子项目。它包含单页应用和配套后端，后端作为标准 Arrow Flight SQL 客户端连接 `visionqld`。Workbench 与引擎之间只使用公开客户端协议，不依赖私有 API。

**为什么需要 Workbench？** DBeaver 等通用 SQL 客户端可以通过 JDBC/ADBC 连接 `visionqld`，但通常只会把 `IMAGE` 显示为二进制，把检测结果显示为结构体文本。视觉查询需要直接查看图片、检测框和实时画面，才能有效调试。Workbench 专注于多模态结果预览和视觉查询运维，不与通用 BI 工具竞争。

**目标用户**包括数据分析师、工程师和平台运维人员。数据分析师可以直接在浏览器中查询，无需本地安装；工程师用它调试 SQL 和模型效果；运维人员用它监控持续查询和成本。

**核心能力**:

| 能力 | 说明 | 阶段 |
|---|---|---|
| SQL 编辑与执行 | VQL 语法高亮、目录感知补全、多语句脚本执行和查询历史。交互查询会在传输端限制返回行数，不改写 SQL，也不改变查询语义 | v0.2 |
| 结果预览 | 表格分页;`IMAGE` 缩略图内联显示、点击取原图;检测结果(`BOX2D`)叠加绘制在对应帧上,置信度滑杆前端过滤(调阈值不重跑查询);`VECTOR` 折叠显示 | v0.2 |
| 流结果实时预览 | 实时滚动显示无界 SELECT 的最近 N 行结果；关闭页面时自动取消预览查询 | v0.2 |
| 目录浏览 | 浏览表、流、模型、函数和 Sink，并查看 schema 与 DDL | v0.2 |
| 持续查询运维 | 展示查询状态、推理量、延迟、丢帧和断流指标，并提供 `PAUSE`、`RESUME`、`STOP` 操作 | v0.2 |
| 成本面板 | v0.2 根据 `SHOW METRICS` 展示每个查询的实际 GPU 时长和推理次数；v0.3 再加入基于 `EXPLAIN` 的成本预估 | v0.2 实测；v0.3 增加预估 |

**产品原则**:

1. **所有功能都通过 SQL 或标准协议完成**：目录浏览使用 `SHOW`，运维使用 `PAUSE`/`RESUME`，指标使用 `SHOW METRICS`。Workbench 不要求引擎提供私有管理 API，只负责将这些能力呈现在界面中。
2. **保持无状态**：Workbench 不持久化业务数据。认证由引擎处理，保存的查询放在浏览器本地，因此 Workbench 进程可以随时重启或扩容。
3. **独立发布**：Workbench 有自己的版本号和发布节奏，引擎不依赖 Workbench。两者的兼容范围跟随 SQL 方言和 Flight SQL 协议的稳定性承诺（3.7）。

技术设计见 [Workbench 设计文档](./workbench.md)。

---

## 4. 产品边界与 MVP 范围

**非目标**：VisionQL 是查询与处理引擎，不是完整的行业应用。

- **不做模型训练和标注平台**：VisionQL 可以筛选和导出训练数据，例如检索 corner case，但不负责模型训练本身。
- **不做视频存储系统（VMS）或流媒体服务器**：VisionQL 连接 RTSP 和对象存储等现有系统，不替代它们。
- **不做面向最终用户的安防或审核应用**：VisionQL 为应用开发者提供引擎。场景包只包含模型、SQL 模板和面板。

**v0.1 包含的能力**聚焦于单机端到端运行：

- 类型系统 + IMAGE/VIDEO/BOX2D/VECTOR
- 批:图片/视频目录表、`FRAMES()`、`UNNEST`
- 流:RTSP 单流摄入、TUMBLE 窗口;投递语义:RTSP 为不可重放 live 源,尽力而为(断流/丢帧缺口如实反映)——至少一次随可重放源(Kafka 帧源,v0.2)生效
- `CREATE MODEL` + `CREATE FUNCTION ... USING MODEL`(OBJECT_DETECTION / EMBEDDING 两类,含 1:1 语法糖)+ 库态 Python UDF；v0.1 服务态遇到 Python UDF 返回明确的能力错误
- Sink:Kafka、Parquet/Lance、Console(前台调试用,`INSERT INTO` 形状不变只换 Sink;服务态拒绝)
- 产品形态:库态(pip 包)+ SQL shell + Python DataFrame API + 基础服务态;`vql-server` 构建 `visionqld`,以单可信主体、默认监听回环地址的方式提供 Flight SQL 查询、元数据、流式结果和取消
- 持续查询既可由 `visionql run job.sql` 在本地前台运行，也可通过 v0.1 Flight SQL 远程执行；两者都随客户端生命周期结束，不承诺持久恢复
- 优化：帧采样下推（实现相对简单，且效果容易验证）

**明确推迟到后续版本的能力**：

- Workbench(Web 工作台)、持久作业管理与恢复、TLS/认证和表流级权限、分布式执行、精确一次、`TRACK` 算子、VLM 谓词、向量索引、多租户治理——留待 v0.2+ 按场景反馈排序；v0.2 的首要任务是把已有服务态生产化。

**MVP 验收场景**：以下两个场景合并覆盖全部 MVP 组件，确保每项实现都被真实流程使用。

- **场景 A(批流一体与基础服务态)**:用一条 SQL 通过 `visionqld`/Flight SQL 完成 3.2 的"每分钟人数入 Kafka",并通过库态或 CLI 在同一逻辑下跑通历史视频回算,断言两者结果一致(相同模型与采样率);同时验证远程取消和断连清理,调试阶段以 console sink 查看 `UNNEST` 展开的检测明细;
- **场景 B（首次使用无需外部服务）**：全程在本地运行。用户从图片目录建表，通过 Python UDF 过滤模糊图片，使用 CLIP 生成嵌入并写入 Lance，最后以文搜图并返回 Top-20。从 `pip install` 到第一个检索结果不超过 5 分钟。场景 A 用于验证完整能力，场景 B 用于验证首次使用是否足够简单。

## 5. 路线图

| 阶段 | 主题 | 关键交付 |
|---|---|---|
| **v0.1(MVP)** | 单机端到端可用 | 库态 + SQL/DataFrame、基础服务态 `visionqld` + Flight SQL、批表 + RTSP 单流、检测/嵌入两类模型、帧采样下推;验收场景见第 4 节 |
| **v0.2** | 流查询生产化 | 将 `visionqld` 增强为持久查询管理与恢复服务、`TRACK` 算子、Workbench(Web 工作台,含实测口径成本面板,见 3.8)、TLS/认证与表流级权限、Kafka 帧源(可重放源,至少一次投递语义生效)、MCP 服务器(Agent 工具接入) |
| **v0.3** | 智能降本 | 模型级联优化器、推理结果物化与跨查询复用、向量索引(随物化配套,按规模触发;暴力 TopK 检索自 v0.1 可用)、VLM 谓词、`EXPLAIN` 成本预估 |
| **v1.0** | 规模化 | 集群态、精确一次、多租户治理与审计、WASM UDF(用户代码沙箱与边缘分发);SQL 方言与目录格式的稳定性承诺生效 |
| **v1.x+** | 边缘协同 | 同一条查询的边缘/中心执行段自动切分、边缘节点车队管理;手动的边缘盒部署随 v0.1 基础服务态即可用(同一二进制,含 ARM,见 3.5)，生产级远程访问与恢复从 v0.2 起提供 |

本表为产品级概要；根目录引擎 crates 与 `vql-workbench` 的完整交付清单、验收口径和依赖关系展开维护于 [Roadmap](./roadmap.md)。

## 6. 商业化路径

VisionQL 通过开源引擎建立用户和生态，商业化能力主要围绕生产环境中的规模化运行展开：

1. **开源版（Apache-2.0）**：包含引擎内核、库态、服务态基础能力和完整 SQL 语义，个人和小团队可以完整使用。长期目标是建立通用的视觉 SQL 使用方式。
2. **企业版**：面向平台团队提供多租户、配额、权限、审计、数据来源追踪、集群部署、边缘节点管理和 SLA 支持。
3. **托管云（优先 BYOC）**：控制面由 VisionQL 托管，数据面仍运行在客户 VPC 或边缘环境中，以满足视频数据留在本地的要求。
4. **定价方式**：按 GPU 计算时长或接入的视频流数量计费。优化器降低单次查询成本后，客户可以在相同预算下处理更多数据。
5. **首批用户策略**：与 2～3 家设计伙伴共同开发一个重点场景包，包括模型、SQL 模板和面板。重点场景在安防/园区和内容审核之间选择，详见开放问题 1。开源发布时提供可以直接运行的场景示例。

## 7. 成功指标

**北极星指标：每周通过 VisionQL 查询处理的视频小时数**，其中批处理和流处理统一折算。这个指标同时反映使用范围和实际负载规模。

| 维度 | 指标 |
|---|---|
| 激活 | 首次价值时间（TTFV）：从 `pip install` 到第一个查询结果少于 5 分钟，全程不依赖外部服务，口径见第 4 节场景 B |
| 效率 | 典型的“每分钟人数统计”任务少于 30 行代码；从零到上线少于 30 分钟 |
| 成本 | 相对逐帧全量推理基线，优化器默认配置使 GPU 时长降低至少 5 倍 |
| 正确性 | 使用相同模型和采样率时，窗口聚合结果与手写基线管道一致 |
| 采用 | 开源后 90 天内，至少有 3 个真实外部场景端到端上线，至少 1 家设计伙伴开始承载生产流量 |
| 留存 | 流查询平均持续在线超过 30 天；设计伙伴的周活跃查询数持续增长 |

## 8. 开放问题

以下问题将在设计评审和设计伙伴访谈后确定：

1. **首个重点场景**：选择安防/园区，还是内容审核？前者更依赖私有化部署和渠道，但付费意愿较强；后者更偏云原生，决策链较短，数据量更大。这个选择会影响首批连接器和场景包的投入方向。
2. **SQL 方言兼容范围**：类型名、函数命名和错误码需要在多大程度上遵循 PostgreSQL 习惯？这会直接影响现有生态工具的兼容成本。
3. **置信度在聚合中的语义**：是否需要提供区间估计等专用原语，还是长期保持由用户在查询中明确指定阈值？
4. **跨流 JOIN 的版本安排**：跨摄像头轨迹（ReID JOIN）放在 v0.3 还是 v1.x？实现难度较高，但安防场景的需求也较强。
5. **客户端协议中的 `IMAGE` 表示**：Arrow Flight 应返回引用还是内联字节？这会影响 BI 工具的使用体验和网络带宽。Workbench 已按“默认引用、`thumbnail` 会话选项返回缩略图、`FRAME_AT` 按需取帧”的方案设计，详见 [Workbench 设计](./workbench.md) §3.2。v0.2 Flight SQL 前端定稿前仍需确认。
6. **历史回填后接入实时流**：当前的批流一体表示同一条 SQL 可以分别用于表和流。更进一步的能力，是让同一查询先回扫录像或物化结果，再无缝接入实时流。这个能力需要定义历史与实时时间线的拼接和去重语义，初步安排在 v1.x，最终取决于设计伙伴的需求。

---

## 附录：SQL 保留字和新增语法

| 语法 | 类别 | 作用 |
|---|---|---|
| `CREATE STREAM ... FROM 'rtsp://...'` | DDL | 注册视频流 |
| `CREATE TABLE ... USING IMAGES/VIDEOS` | DDL | 目录即表 |
| `CREATE MODEL ... TYPE ... FROM ...` | DDL | 注册模型(资源层);可带 `FUNCTION` 子句顺带派生函数 |
| `CREATE FUNCTION ... USING MODEL / LANGUAGE <lang> AS '<入口>' / AS (<表达式>)` | DDL | 注册函数(接口层):资源引用型 / 代码型 / SQL 宏 |
| `ALTER FUNCTION ... SET MODEL` | DDL | 换绑模型,查询不变(灰度/回滚) |
| `CREATE SINK / MATERIALIZED VIEW` | DDL | 输出与物化 |
| `CREATE INDEX ... USING HNSW` | DDL | 向量索引,`ORDER BY <-> LIMIT` 自动改写为 ANN |
| `FRAMES(TABLE t, fps => n)` | 表值函数(表进表出) | 视频表展开为帧表,采样可下推 |
| `TRACK(TABLE t, DETECTOR, CLASS)` | 表值函数(表进表出) | 跨帧目标跟踪,输出 track_id |
| `TUMBLE(ts, interval)` | 时间分桶函数 | 滚动窗口,用于 GROUP BY,批流同形 |
| `HOP / SESSION` | 表值函数(表进表出) | 滑动/会话窗口 |
| `UNNEST(expr) AS x` | 关系化 | 检测结果数组 → 行(隐式关联) |
| `COUNT_OBJECTS(dets, label, conf)` | 内置数组函数 | 按标签/置信度计数,免 lambda |
| `ST_CONTAINS / POLYGON / .center` | 空间 | 区域事件 |
| `<->`(等价 `L2_DISTANCE`) | 向量 | 跨模态相似检索 |
| `SHOW QUERIES / PAUSE / RESUME` | 运维 | 持续查询管理(服务态) |
| `FRAME_AT(uri, pts_ms)` | 内置函数 | 按引用取帧,客户端原图点查(v0.2 随 Flight SQL 前端,见 3.8) |
| `EXPLAIN` | 运维 | 展示查询计划与预估 GPU 成本 |
