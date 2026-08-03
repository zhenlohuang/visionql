# VisionQL 产品需求文档 (PRD)

> 一个面向多模态数据的批流一体查询与处理引擎:用 DataFrame 与 SQL 表达对图片、视频文件与视频流的语义查询。

- **版本**: v0.1.1 (Draft)
- **日期**: 2026-07-31
- **状态**: 评审中

---

## 1. 一句话定义

**VisionQL:物理世界的查询引擎。**(*A query engine for the physical world.*)

传统数据库查询的是业务系统产生的记录;VisionQL 查询的是摄像头与镜头看见的真实世界。工程师和分析师用一条 SQL(或几行 DataFrame 代码)直接对图片、视频与实时视频流提问,完成过去需要数百行 Python 胶水代码 + 手工调度 GPU 推理才能完成的视觉分析任务。

- 图片、视频文件 → **批处理**(有界数据集,类比 Spark batch)
- 视频流(RTSP / WebRTC / Kafka 帧流)→ **流处理**(无界数据集,类比 Flink / Spark Structured Streaming)
- 同一套 SQL / DataFrame 语义覆盖批与流(批流一体)

---

## 2. 需求合理性分析

### 2.1 背景与趋势

1. **数据结构在倒挂**。企业新增数据中 80%~90% 是非结构化数据,其中视觉数据(图片、监控视频、行车记录、直播流)占比最大且增速最快。但过去二十年的数据基础设施(数仓、Spark、Flink)几乎全部围绕结构化/半结构化数据构建——对视觉数据,主流引擎只能存路径字符串,无法"看懂"内容。

2. **模型让"查询像素"成为可能**。检测、跟踪、OCR、多模态大模型(VLM)、向量检索的成熟,意味着"画面里有几个人""找出所有闯入禁区的片段"这类问题第一次有了可编程的答案。缺的不是模型,而是**把模型组织成数据系统的那一层**。

3. **历史在重演**。MapReduce 时代人人手写 Java 作业,直到 Hive/Spark SQL 用声明式接口 + 优化器把门槛降下来,大数据才真正普及。今天的视觉数据处理正处在"人人手写 OpenCV + PyTorch 脚本"的 MapReduce 阶段——声明式化是必然方向,问题只是谁来做、做成什么样。

### 2.2 现状痛点(为什么现有方案不够)

| 现有方案 | 不足 |
|---|---|
| **Spark / Flink** | 只理解结构化数据。视觉处理只能塞进黑盒 UDF:优化器无法下推、无法裁剪帧、无法复用推理结果;视频解码、GPU 调度、模型 batching 全部要用户自己管。 |
| **自建 Python 管道**(OpenCV + PyTorch + Celery/Airflow) | 一次性胶水代码:无优化、无增量计算、无容错语义、逻辑不可复用;批和流两套代码;分析师完全无法参与。 |
| **研究系统**(EvaDB、BlazeIt、VIVA 等) | 验证了 "SQL over video" 的可行性和优化空间(模型级联、帧采样可带来 10~100x 加速),但多为单机原型、偏批处理、无生产级流语义,未形成产品。 |
| **多模态数据框架**(Daft、Ray Data、LanceDB) | 解决了"多模态数据的存取与并行计算",但定位是通用计算/存储层:没有视觉原生算子(检测/跟踪/区域事件)、没有流处理、SQL 能力弱或缺失。 |
| **云视觉 API**(Rekognition、阿里云视觉智能等) | 按调用计费的黑盒:无法组合查询、无法用自己的模型、成本随帧数线性爆炸、数据必须出域。 |

**结论**:市场上存在明确空位——**"视觉原生 + SQL 声明式 + 批流一体 + 可优化"** 四者兼备的引擎尚不存在。VisionQL 的需求是合理的,且时间窗口(模型成熟、GPU 供给改善、企业视频数据积压)正在打开。

### 2.3 目标用户与使用场景

**目标用户**(按优先级):

1. **数据/算法工程师**——今天负责手写视觉管道的人,核心用户,从库态(pip 包)进入;
2. **数据分析师**——会 SQL 不会 PyTorch,声明式接口释放的新增用户,经服务态 + BI 直连进入(v0.2);
3. **平台团队**——把视觉分析做成内部平台,需要多租户、治理与成本控制,企业版的买单方(v1.0);
4. **AI 应用 / Agent 开发者**——把 VisionQL 当作智能体的"眼睛":Agent 以 SQL(或 MCP 工具)查询摄像头与视频库来回答问题、触发动作。SQL 是 LLM 生成成功率最高的目标语言,text-to-SQL 生态可直接复用。

**典型场景**:

| 场景 | 模式 | 典型问题 |
|---|---|---|
| 安防 / 智慧园区 | 流 | 每分钟画面人数、越界/闯入告警、逗留检测、跨摄像头轨迹 |
| 零售客流分析 | 流 | 进店人数、动线热力、货架前停留时长、排队长度 |
| 自动驾驶 / 机器人数据闭环 | 批 | 从百万小时路采/遥操作视频中检索"雨天 + 行人横穿 + 遮挡"类 corner case,用于训练与评测 |
| 媒资 / 内容平台 | 批 | 视频打标、人物/场景检索、精彩片段抽取、"以文搜片" |
| 内容审核 | 批 + 流 | 直播流实时违规检测 + 存量内容回扫,一份规则两种执行模式 |
| 工业质检 | 流 | 产线相机流缺陷检测、良率分钟级聚合、异常帧留存 |
| 无人机 / 设施巡检 | 批 | 电力/管线/光伏巡检视频的缺陷检索、跨期变化对比、工单证据帧 |
| 体育 / 赛事分析 | 批 | 球员跟踪、阵型统计、事件(射门/犯规)检索 |
| AI Agent / 智能助手 | 批 + 流 | Agent 以工具方式提问("仓库里是否有人逗留超过 10 分钟"),自然语言 → SQL → 画面答案;视觉 RAG |

两点说明:内容审核场景直接体现**批流一体**的价值——同一条 SQL,接文件表就是回扫,接流就是实时拦截;场景虽多,首发只做深一个楔子(选择标准:现状痛感 × 数据规模 × 付费意愿,见开放问题 1),其余靠开源社区的场景包机制横向生长。

### 2.4 产品价值

1. **开发效率一个数量级的提升**。"统计每分钟画面人数并写入 Kafka" 从 300 行 Python + 部署脚本变成 10 行 SQL。分析师第一次可以直接查询视频。
2. **优化器直接省钱**。视觉查询的成本大头是 GPU 推理,而声明式恰好给了系统优化空间:
   - **帧采样下推**:查询只要分钟级聚合,就没必要按 30fps 全量推理;
   - **模型级联**:先用小模型过滤空画面,只有可疑帧才进大模型(研究表明可获得 10~100x 加速);
   - **谓词下推到解码层**:只解码需要的时间段/关键帧;
   - **推理结果物化与复用**:同一路流上多个查询共享一次检测。
   这些优化在黑盒 UDF 架构里根本做不了——**这是声明式引擎相对于胶水脚本的结构性优势,也是本产品最深的护城河**。
3. **批流一体,一份逻辑**。同一查询在历史视频上回放验证,再原样上线到实时流,消除两套代码的维护与语义漂移。
4. **视频从成本中心变成数据资产**。模型、数据源、查询结果都是目录中的一等公民:血缘可追、权限可管;推理结果物化后"一次推理、永久可查",新问题优先查物化结果而非重跑模型——视频资产的查询价值随使用复利增长。
5. **合规内建**。"数据不出域"是架构默认而非部署选项;就地脱敏(打码/匿名化函数)、审计与血缘让敏感视频的每次使用可证明合规——在安防、零售等受个保法/GDPR 约束的场景,这是采购前提而非加分项,也是相对云视觉 API 的结构性差异。
6. **AI 时代的接口红利**。SQL 是 LLM 生成成功率最高的目标语言——把视觉世界暴露为可查询的表,等于给 Agent 一双稳定、可审计、可限权的眼睛(自然语言 → SQL → 画面答案)。"物理世界的查询引擎"因此也是 Agent 生态里的视觉感知层。

### 2.5 风险与挑战(诚实评估)

| 风险 | 说明 | 缓解 |
|---|---|---|
| **推理成本仍然昂贵** | 即使优化 10x,大规模视频全量分析依然烧 GPU | 把"省钱"做成产品能力:成本预估(EXPLAIN 出预估 GPU 时长)、采样率显式可控、级联默认开启 |
| **结果是概率性的** | 检测有漏检误检,COUNT(*) 不再是精确语义 | 置信度作为一等语义(阈值显式出现在查询中);提供 `WITH CONFIDENCE` 类原语;文档诚实说明 |
| **SQL 表达力边界** | 复杂 CV 逻辑(标定、多目标关联规则)塞不进 SQL | 不追求 100% SQL 化:UDF/UDM(用户自定义模型)机制作为逃生舱,DataFrame API 承接复杂逻辑 |
| **生态冷启动** | 引擎类产品依赖连接器与模型生态 | 首发聚焦"检测/跟踪/嵌入/VLM 问答"四类高频算子 + RTSP/S3/Kafka 三类连接器,做深一个场景(安防或审核)再横向扩 |
| **与大厂产品线撞车** | Databricks/云厂商可能补齐多模态能力 | 以批流一体 + 视觉原生优化器建立差异;开源引擎聚拢社区 |
| **模型许可合规** | 常用检测模型的许可证并不宽松(如 YOLO 系列为 AGPL-3.0),官方模型库若默认收录,商用分发有传染风险 | 官方模型库只收录宽松许可模型(Apache-2.0/MIT,如 RT-DETR 系);许可证写入目录元数据并在 `CREATE MODEL` 时展示;受限许可模型由用户显式引入、责任自担 |

---

## 3. 产品设计:用户如何使用 VisionQL

### 3.1 核心抽象

VisionQL 的世界观:**一切视觉数据最终都是"帧的关系表"**。

| 抽象 | 说明 |
|---|---|
| **多模态类型系统** | 在标准 SQL 类型之外新增:`IMAGE`、`VIDEO`、`AUDIO`、`BOX2D`(检测框)、`MASK`、`VECTOR(n)`(嵌入向量)、`STRUCT`/`ARRAY` 嵌套类型。v1 聚焦视觉模态;`AUDIO` 为预留类型,首个目标场景是直播审核的音画同判 |
| **Table(表)** | 有界数据集。一个图片目录是一张表(每行一张图);一个视频文件目录也是一张表(每行一个视频,可展开为帧) |
| **Stream(流)** | 无界数据集。一路 RTSP/摄像头/Kafka 帧流,schema 天然是帧表:`(ts TIMESTAMP, frame IMAGE, ...)`,自带事件时间与水位线 |
| **Model(模型)** | 资源层对象:只持有影响成本/延迟的定义——权重来源、版本、任务类型、资源约束(精度、SLO);GPU 放置、副本数、动态 batching 由引擎运行时决策。只出现在资源调度中,不出现在查询里 |
| **Function(函数)** | 接口层唯一可调用概念:只持有影响查询结果的定义——签名、绑定参数、实现引用(`USING MODEL` / `LANGUAGE PYTHON` / SQL 宏)。一个模型可派生多个函数;优化器可在函数背后换绑模型(级联、灰度) |
| **窗口** | 流上的聚合单位。滚动窗口 `TUMBLE` 是时间分桶标量函数,直接用于 `GROUP BY`(批模式下就是普通时间分桶聚合);滑动 `HOP`、会话 `SESSION` 涉及行复制与跨行状态,采用"表进表出"的表值函数形式 |
| **Sink / 物化视图** | 查询结果的去处:Kafka、Parquet/Lance、告警 Webhook,或持续更新的物化视图 |

批流一体的关键设计:**表和流共享同一套查询语言**,`FROM` 一张表就是批作业,`FROM` 一个流就是持续查询,窗口聚合等语义保持一致。

### 3.2 五分钟用户旅程

```bash
pip install visionql
visionql shell          # 交互式 SQL,或在 Python 中 import visionql
```

一个完整任务——"实时统计门口摄像头每分钟平均人数,写入 Kafka":

```sql
-- ① 注册视频流(声明源、采样率)
CREATE STREAM cam_entrance
FROM 'rtsp://10.0.0.15:554/main'
WITH (fps = 5, event_time = 'capture_time', watermark = INTERVAL '2' SECOND);

-- ② 注册模型,并同时派生查询函数 yolo_det(1:1 语法糖,详见 3.3.2)
CREATE MODEL yolo
TYPE OBJECT_DETECTION
FROM 'hf://ultralytics/yolov11n'
FUNCTION yolo_det;

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
         COUNT_OBJECTS(yolo_det(frame), 'person', 0.6) AS person_cnt
  FROM cam_entrance
)
GROUP BY 1;
-- COUNT_OBJECTS(检测结果, 标签, 置信度阈值) 是内置数组函数,见 3.3.8 设计原则
```

四步,约 20 行,零 Python、零部署脚本。交互模式下持续查询在前台运行,适合开发调试;生产化的常驻部署形态见 3.5。以下分主题展开 SQL 设计。

### 3.3 SQL 设计详解

#### 3.3.1 数据源注册 (DDL)

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

**MODEL 是资源层对象**——只声明"是什么":权重来源、版本、任务类型与资源约束。它只持有影响成本/延迟的定义,只出现在资源调度里,永不出现在查询计划中(类比 Postgres FDW 的 `SERVER`,或表背后的 Parquet 文件)。

```sql
-- 只声明"是什么",放哪块 GPU、batch 多大等部署决策由引擎运行时负责,默认零配置
CREATE MODEL yolo
TYPE OBJECT_DETECTION
FROM 'hf://ultralytics/yolov11n';

-- 嵌入模型与远程端点同样是模型
CREATE MODEL clip TYPE EMBEDDING FROM 'hf://openai/clip-vit-base-patch32';
CREATE MODEL qwen_vl TYPE VQA FROM 'endpoint://http://vlm-serving:8000';

-- 语法糖(渐进披露):1:1 场景一条语句同时注册模型与派生函数。
-- 入门用户只需理解 FUNCTION 一个概念,MODEL 在需要一对多/换绑/资源治理时才浮现
CREATE MODEL yolo_nano
TYPE OBJECT_DETECTION
FROM 'hf://ultralytics/yolo11n'
FUNCTION nano_det;
```

**模型定义与部署解耦**:`CREATE MODEL` 不接受物理部署参数(设备、副本数、batch 大小)——那是运行时调度器的职责,随负载动态调整。`WITH` 子句只接受声明式约束与元信息,引擎在约束内自行决策,例如 `precision = 'fp16'`(精度)、`latency_slo = '50ms'`(延迟目标)、`resource_group = 'gpu-pool-a'`(多租户资源池)。物理钉死仅作为运维逃生舱经 `ALTER MODEL` 使用,不出现在建模语句里。

引擎对模型负责:下载与版本固定、GPU 放置、动态 batching、失败重试。

#### 3.3.3 函数注册

**FUNCTION 是接口层唯一可调用概念**——查询里永远只出现函数调用,永不出现模型(类比 FDW 的 `FOREIGN TABLE`)。与模型的归属判定只有一条律令:**影响查询结果的定义属于 FUNCTION(签名、绑定参数、实现引用);只影响成本/延迟/部署的定义属于 MODEL(权重、版本、精度、SLO)**。两者内容集合不相交,`WITH` 子句据此校验——把资源参数写到函数上直接报错,反之亦然。

```sql
-- TYPE 蕴含标准签名,签名与 RETURNS 可省略
-- (OBJECT_DETECTION 标准签名: (IMAGE) -> ARRAY<STRUCT<label STRING, confidence FLOAT, box BOX2D>>)
CREATE FUNCTION yolo_det USING MODEL yolo;

-- 同一模型派生带绑定参数的函数(WITH 只收影响结果的语义参数)
CREATE FUNCTION person_det USING MODEL yolo
WITH (classes = ['person'], min_confidence = 0.5);

-- 一对多:CLIP 一份权重,图文两个入口——跨模态检索因此天然成立
CREATE FUNCTION embed_image(img IMAGE) RETURNS VECTOR(512) USING MODEL clip;
CREATE FUNCTION embed_text(txt STRING) RETURNS VECTOR(512) USING MODEL clip;

-- VLM 问答函数
CREATE FUNCTION vlm USING MODEL qwen_vl;
```

**FUNCTION 的可扩展定义**:函数 = 五个正交槽位,每个槽位独立演进,新增能力只扩枚举值,不引入新的句法形状:

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
| **WITH 绑定参数** | 影响语义的常量(类别、阈值、prompt 模板) | 按上述律令校验,资源参数拒收 |
| **元属性** | 确定性、可 batch、成本画像,由实现类型自动推导 | 优化器专用,不占用户语法 |

```sql
-- 三种实现形状,同一个 FUNCTION 概念
CREATE FUNCTION yolo_det USING MODEL yolo;              -- 资源引用型:引擎托管推理

CREATE FUNCTION blur_score(img IMAGE) RETURNS FLOAT
LANGUAGE PYTHON AS 'myops.quality:blur_score';          -- 代码型:逃生舱

CREATE FUNCTION is_large(b BOX2D) RETURNS BOOLEAN
AS (b.w * b.h > 0.25);                                  -- SQL 宏:纯表达式复用,解析期内联展开
```

**关键字约定**:`USING` 统一表示"由已注册资源/provider 支撑"(与 `USING IMAGES`、`USING HNSW` 一致);`AS` 保留给实现体本身(CTAS 的 `AS SELECT`、Python UDF 的 `AS '<入口>'`)。模型绑定是资源引用而非函数体,故用 `USING MODEL`。

这个分层买到三样东西:

1. **生命周期解耦**:`ALTER MODEL` 升级版本、换设备,不触碰接口层;`ALTER FUNCTION person_det SET MODEL yolo_v12` 换绑模型,查询一行不改(灰度/回滚的基础);
2. **一对多复用**:CLIP 图文双入口、一个 VLM 端点派生多个 prompt 模板函数,权重只加载一份;
3. **优化器抓手**:模型级联的自然表达就是两个同 `TYPE` 的模型(如 `yolo_nano` 过滤 + `yolo` 确认),优化器在函数背后自动换绑;`EXPLAIN` 的 GPU 成本预估挂在模型对象的代价画像上。

#### 3.3.4 查询一:视频/流中"人的位置"

检测结果是数组,用 `UNNEST` 展开成关系行——这是视觉数据关系化的核心手法(`FROM t, UNNEST(expr) AS x` 隐式关联写法,对齐 BigQuery 风格):

```sql
-- 流上:实时输出每个人的位置框
SELECT ts,
       det.box,           -- BOX2D: (x, y, w, h),可取 .center 中心点
       det.confidence
FROM cam_entrance,
     UNNEST(yolo_det(frame)) AS det
WHERE det.label = 'person'
  AND det.confidence > 0.6;
```

```sql
-- 批上:同样的写法,只是先把视频表展开为帧表
-- FRAMES() 是"表进表出"的表值函数:输入视频表,输出帧表(原表各列透传),fps 参数即采样下推
SELECT f.uri, f.ts, det.box
FROM FRAMES(TABLE traffic_videos, fps => 1) AS f,
     UNNEST(yolo_det(f.frame)) AS det
WHERE det.label = 'person';
```

空间谓词让"位置"可以参与过滤(如禁区检测):

```sql
-- 出现在禁区多边形内的人 → 告警
SELECT ts, det.box
FROM cam_entrance, UNNEST(yolo_det(frame)) AS det
WHERE det.label = 'person'
  AND ST_CONTAINS(POLYGON('(0.6,0.1),(0.95,0.1),(0.95,0.8),(0.6,0.8)'),
                  det.box.center);
```

#### 3.3.5 查询二:"每分钟画面中有多少人"

这个问题有两种语义,SQL 都能表达,且差异被显式写出(这正是声明式的好处):

**语义 A——每分钟画面内平均/峰值人数**(每帧数人,窗口聚合):

```sql
SELECT TUMBLE(ts, INTERVAL '1' MINUTE) AS window_start,
       AVG(person_cnt) AS avg_people,
       MAX(person_cnt) AS peak_people
FROM (
  SELECT ts,
         COUNT_OBJECTS(yolo_det(frame), 'person', 0.6) AS person_cnt
  FROM cam_entrance
)
GROUP BY 1;
```

**语义 B——每分钟经过了多少个不同的人**(需要跨帧跟踪,`TRACK` 算子为每个目标分配稳定 `track_id`):

```sql
SELECT TUMBLE(ts, INTERVAL '1' MINUTE) AS window_start,
       COUNT(DISTINCT track_id) AS unique_people
FROM TRACK(TABLE cam_entrance,
           DETECTOR => yolo_det,
           CLASS    => 'person')          -- 表进表出,输出: (ts, track_id, box, confidence)
GROUP BY 1;
```

同一条语句 `FROM` 换成历史视频表即为批量回算——批流一体在此处零成本兑现。

#### 3.3.6 查询三:跨模态语义检索(批场景为主)

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

向量列可建索引(`CREATE INDEX ... USING HNSW`),`ORDER BY <-> LIMIT` 自动改写为 ANN 检索。`<->` 是语法糖,始终存在等价函数形式 `L2_DISTANCE(a, b)`。

#### 3.3.7 结果落地:Sink 与物化视图

```sql
-- 持续查询写入 Kafka(见 3.2 完整示例)
INSERT INTO people_per_minute SELECT ...;

-- 物化视图:持续维护、可被再次查询,推理结果自动复用
CREATE MATERIALIZED VIEW entrance_tracks AS
SELECT * FROM TRACK(TABLE cam_entrance, DETECTOR => yolo_det, CLASS => 'person');

-- 下游多个查询共享同一份跟踪结果,不再重复推理
SELECT ... FROM entrance_tracks GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE);

-- 事件帧留存:告警同时把证据帧存下来
INSERT INTO evidence  -- Lance/Parquet 表,IMAGE 列原生存储
SELECT ts, frame, det.box
FROM cam_entrance, UNNEST(yolo_det(frame)) AS det
WHERE det.label = 'person' AND det.confidence > 0.9;
```

#### 3.3.8 SQL 可落地性设计原则

上述语法不是随意发明的,而是刻意约束在成熟列式查询引擎可扩展的范围内,保证每一条扩展语法都能降解到标准扩展机制,而不需要魔改引擎内核:

1. **一切扩展降解为三类机制**:
   - **标量函数**(含异步远程调用)——模型推理(`yolo_det`、`vlm`)、数组处理(`COUNT_OBJECTS`)、空间/向量谓词(`ST_CONTAINS`、`L2_DISTANCE`)。逐批(RecordBatch)向量化执行天然提供推理 batching。SQL 宏(`AS (<表达式>)`)在解析期内联展开,不产生运行时实体;函数的三种形状(标量/聚合/表值)各有对应的引擎扩展点;
   - **表值算子(表进表出)**——`FRAMES`、`TRACK`、`HOP`/`SESSION`,由 SQL 层解析后降解为自定义逻辑计划节点 + 自定义物理算子,不依赖按行关联(correlated lateral)的表函数;
   - **DDL → 目录操作**——`CREATE STREAM/MODEL/FUNCTION/SINK/MATERIALIZED VIEW` 由自有 SQL 方言层解析,落到目录(Catalog)与运行时,不进入查询计划。函数注册进查询引擎的函数注册表;模型只存在于目录与模型运行时,查询计划里看不到它。
2. **不引入 lambda / 高阶函数**。数组处理一律使用命名内置函数(如 `COUNT_OBJECTS(dets, label, min_conf)`),保持表达式系统一阶——这是谓词分析与下推优化可行的前提。
3. **`UNNEST` 是唯一的行展开原语**,采用 `FROM t, UNNEST(expr) AS x` 隐式关联写法,直接映射到引擎原生的展开计划节点,不需要通用 LATERAL 关联能力。
4. **`TUMBLE` 双模一致**:批模式降解为普通时间分桶聚合;流模式由运行时为同一计划附加窗口状态与水位线。语法与查询逻辑完全不变,这是批流一体的落点。**窗口不占用 `WINDOW` 关键字**:ANSI 的 `WINDOW`/`OVER` 是逐行分析语义(基数不变),流式窗口是分组语义(基数坍缩),撞名会造成双重语义;`WINDOW` 保留给标准分析函数——轨迹平滑等逐行分析(`AVG(speed) OVER (PARTITION BY track_id ...)`)正需要它。窗口命名沿用 Flink 词汇(`TUMBLE/HOP/SESSION`),三种窗口形状与代价本不相同,名字分化是语义分化的诚实呈现(Spark 试图用单一 `window()` 统一,最终仍被迫分出 `session_window()`)。
5. **自定义算子皆有函数等价形式**。`<->` 等运算符经表达式规划扩展映射为函数调用,方言不兼容时用户总有退路。
6. **多模态类型建立在标准列式类型之上**:`IMAGE`/`VIDEO` 为带元数据的二进制/结构列,`BOX2D` 为结构体,`VECTOR(n)` 为定长浮点列表——类型名只存在于 DDL 与文档层,不要求引擎具备用户自定义类型内核。

### 3.4 DataFrame API(Python)

SQL 之下是同一套逻辑计划,DataFrame 面向工程师,适合复杂管道与编程式组装:

```python
import visionql as vq

sess = vq.connect()

# 与 3.3.5 语义 B 等价
counts = (
    sess.stream("cam_entrance")
        .track(detector="yolo_det", cls="person")          # (ts, track_id, box, ...)
        .window(vq.tumble("1 minute"))
        .agg(unique_people=vq.count_distinct("track_id"))
)
counts.write.kafka("broker:9092", topic="people-count").start()

# 批:图片目录打标后存表
(
    sess.table("product_photos")
        .with_column("tags", vq.fn("yolo_det")(vq.col("image")))
        .with_column("embedding", vq.fn("embed_image")(vq.col("image")))
        .write.lance("s3://bucket/photo_index/")
)
```

约定:**SQL 是产品的第一公民**(降低门槛、可被优化、可被分析师使用),DataFrame 是等价的编程接口,二者可混用(`sess.sql(...)` 返回 DataFrame)。

### 3.5 产品形态与部署

产品形态由三个无法回避的张力决定:

- **批探索要零运维**:分析师/工程师的第一次接触必须是 `pip install` 就能查,任何"先部署个集群"都会杀死自下而上的采用(DuckDB 已经教育了市场);
- **流查询要常驻**:持续查询有状态、要故障恢复,模型要常驻显存、GPU 要池化复用——这些只能活在长生命周期的服务里,塞不进随起随灭的脚本进程;
- **视频数据搬不动**:一路 1080p 流约 4Mbps,几十路回传中心就不现实,加上隐私合规,计算必须能去到摄像头旁边——只回传 KB 级的结构化结果。

没有单一形态能同时满足三者。因此产品形态定为:**同一个引擎内核,三种宿主形态**,SQL 与目录(Catalog)在形态间完全一致。

| 形态 | 载体 | 覆盖场景 | 阶段 |
|---|---|---|---|
| **库态** `visionql` | pip 包,进程内嵌入(DuckDB 式) | notebook 探索、批作业、CI 回归;开发期以前台进程跑流查询调试 | MVP |
| **服务态** `visionqld` | 单机守护进程,**单二进制**内嵌目录、模型运行时、流运行时 | 生产流查询常驻、物化视图持续维护、多客户端共享、GPU 池化 | v0.2 |
| **集群态** | 多节点(计算与模型服务可独立扩缩) | 大规模回扫、多租户平台 | v1.0 |

边缘部署不是第四种形态,而是服务态的一种部署位置:同一个 `visionqld` 二进制跑在摄像头旁的边缘盒(含 ARM),就地解码 + 检测,中心侧只做聚合与检索。远期由规划器自动切分同一条查询的边缘/中心执行段(roadmap)。

**形态间的关键约定**:

1. **"notebook 验证,一条命令上线"**——库态里调通的 SQL,原样 `visionql run job.sql` 运行:未指定服务端时在本地前台执行(开发态);指向 `visionqld`(`--server` 参数或配置默认端点)时提交为常驻查询。动词只有一个 `run`,差别只在跑在哪。这是批流一体在产品形态上的兑现:批流不仅共享语言,还共享从探索到生产的路径;
2. **持续查询是一等对象**:归服务态管理,有名字、有状态、可观测(`SHOW QUERIES` / `PAUSE` / `RESUME`、每查询的推理量与延迟指标);
3. **客户端走标准列式协议**(Arrow Flight SQL / ADBC / JDBC):Python SDK、BI 工具、第三方应用直连,不发明私有协议;
4. **零外部依赖起步**:服务态单二进制自足(目录、模型运行时内嵌),Kafka / 对象存储 / K8s 都是可选外设而非前置条件;
5. **Web 控制台随服务态提供**(v0.2+):查询编辑、流与持续查询监控、GPU 成本面板——2.5 中"把省钱做成产品能力"的落点。

### 3.6 执行层关键设计(简述)

不属于本 PRD 的详细设计范围,但以下引擎能力是上述用户体验成立的前提,列出以指导后续技术设计:

1. **优化器**:帧采样下推(聚合粒度反推所需 fps)、解码裁剪(时间/空间 ROI)、模型级联(小模型过滤 + 大模型确认)、公共推理子表达式消除、推理结果缓存(以模型版本 + 帧指纹为键);
2. **帧数据通路**:解码后的帧是内存大户(1080p RGB ≈ 6MB/帧,5fps 单流即 30MB/s),`IMAGE` 列在计划内以引用/压缩形式流转、零拷贝传递,解码惰性化并尽量推迟到消费点(推理/落盘)前一刻;
3. **GPU 感知调度**:模型自动 batching、算子与模型的共置、背压;
4. **流语义**:事件时间 + 水位线、断流重连;投递语义按源分档——可重放源(Kafka 帧流)至少一次(随 v0.2 Kafka 源生效)→ 精确一次(GA);不可重放 live 源(RTSP)为尽力而为,断流/丢帧缺口如实反映在结果里,不伪造;
5. **存储**:列式多模态格式(Lance/Parquet + 视频引用),物化视图增量维护;
6. **可观测**:`EXPLAIN` 展示预估 GPU 成本;每查询的推理次数/延迟/费用面板;
7. **代码型函数执行**:重计算天然走 `USING MODEL` 路径(引擎托管 GPU 推理),代码型函数只承担轻量胶水逻辑,执行模型据此从简。Python UDF 按产品形态双模执行——库态进程内调用(宿主本就是 Python,Arrow 零拷贝传批,GIL 由批粒度与原生库释放缓解);服务态进程外 Python worker(Arrow IPC 通信、按函数隔离依赖环境、崩溃不伤引擎,多 worker 绕开 GIL)。引擎内核不内嵌解释器,Python 运行时仅在注册了 Python 函数时才成为可选外设,"单二进制零依赖"不破。WASM UDF(运行时内嵌、能力沙箱、单工件跨架构)承接多租户用户代码与边缘/场景包分发,列入 v1.0。

### 3.7 非功能需求(NFR)

| 类别 | 要求 |
|---|---|
| **性能(MVP 基线)** | 单机 1×消费级 GPU:≥ 8 路 1080p@5fps 并发流上运行轻量检测 + 窗口聚合;批扫描吞吐以解码为瓶颈打满硬件;元数据/已物化结果的交互查询 P95 < 1s |
| **容错** | 流查询投递语义按源分档:可重放源至少一次(v0.2 Kafka 源起)→ 精确一次(GA);不可重放 live 源(RTSP)尽力而为,缺口如实反映、不伪造;断流自动重连;服务态重启后持续查询自动恢复,不丢目录状态 |
| **错误语义** | 单帧解码/推理失败默认不中断查询:该行结果置 NULL 并计入每查询的错误指标,失败率超阈值告警;严格模式 `on_error = 'fail'` 可选。模型输出的概率性(漏检/误检)不属于错误,由置信度阈值显式管理(见 2.5) |
| **安全与隐私** | "数据不出域"是默认架构(引擎去数据旁,而非数据上云);模型来源哈希固定、防篡改;服务态:TLS + 认证(v0.2),表/流级权限(v0.2),审计日志(v1.0);用户代码隔离:Python UDF 于服务态进程外执行(v0.2),多租户场景以 WASM 沙箱承接(v1.0) |
| **兼容性承诺** | SQL 方言与目录格式自 v1.0 起遵循语义化版本;`EXPLAIN` 输出与内部指标名不作为稳定接口 |

---

## 4. 产品边界与 MVP 范围

**非目标(明确不做的事)**——VisionQL 是引擎,不是应用:

- **不做模型训练与标注平台**:训练数据的筛选导出是我们的事(corner case 检索),训练本身不是;
- **不做视频存储系统(VMS)/流媒体服务器**:接入它们(RTSP/对象存储),不替代它们;
- **不做面向最终用户的安防/审核成品应用**:把引擎交给做应用的人,场景包只到"模型 + SQL 模板 + 面板"为止。

**做**(v0.1,聚焦"单机可跑通端到端"):

- 类型系统 + IMAGE/VIDEO/BOX2D/VECTOR
- 批:图片/视频目录表、`FRAMES()`、`UNNEST`
- 流:RTSP 单流摄入、TUMBLE 窗口;投递语义:RTSP 为不可重放 live 源,尽力而为(断流/丢帧缺口如实反映)——至少一次随可重放源(Kafka 帧源,v0.2)生效
- `CREATE MODEL` + `CREATE FUNCTION ... USING MODEL`(OBJECT_DETECTION / EMBEDDING 两类,含 1:1 语法糖)+ Python UDF
- Sink:Kafka、Parquet/Lance
- 产品形态:库态(pip 包)+ SQL shell + Python DataFrame API;持续查询以前台进程运行(`visionql run job.sql`)
- 优化:帧采样下推(最容易兑现且收益直观)

**不做**(明确推迟):

- 服务态守护进程(`visionqld`)与 Web 控制台、边缘部署、分布式执行、精确一次、`TRACK` 算子、VLM 谓词、向量索引、多租户治理——留待 v0.2+ 按场景反馈排序(服务态是 v0.2 的头号项,流查询的生产化依赖它)。

**MVP 验收场景**:用一条 SQL 完成 3.2 的"每分钟人数入 Kafka",并在同一逻辑下跑通历史视频回算。

## 5. 路线图

| 阶段 | 主题 | 关键交付 |
|---|---|---|
| **v0.1(MVP)** | 单机端到端可用 | 库态 + SQL/DataFrame、批表 + RTSP 单流、检测/嵌入两类模型、帧采样下推;验收场景见第 4 节 |
| **v0.2** | 流查询生产化 | 服务态 `visionqld`(持续查询管理与恢复)、`TRACK` 算子、Web 控制台与成本面板、TLS/认证与表流级权限、Kafka 帧源(可重放源,至少一次投递语义生效)、向量索引、MCP 服务器(Agent 工具接入) |
| **v0.3** | 智能降本 | 模型级联优化器、推理结果物化与跨查询复用、VLM 谓词、`EXPLAIN` 成本预估 |
| **v1.0** | 规模化 | 集群态、精确一次、多租户治理与审计、WASM UDF(用户代码沙箱与边缘分发);SQL 方言与目录格式的稳定性承诺生效 |
| **v1.x+** | 边缘协同 | 边缘盒部署、同一条查询的边缘/中心执行段自动切分 |

## 6. 商业化路径

开源引擎聚拢标准与社区,商业化围绕"生产化运行"收费——与产品形态的三层结构天然对齐:

1. **开源(Apache-2.0)**:引擎内核、库态、服务态基础能力、全部 SQL 语义——个人与小团队完整可用,目标是建立"视觉 SQL"的事实标准;
2. **企业版**:向平台团队收费——多租户与配额、权限/审计/血缘、集群态、边缘节点车队管理、SLA 支持;
3. **托管云(BYOC 优先)**:控制面托管、数据面留在客户 VPC/边缘——顺应"视频不出域"的合规现实,也避开与云厂商拼存储引力;
4. **定价锚点**:按 GPU 计算时长/接入流路数计价,与客户利益同向——优化器帮客户省得越多,客户越敢扩规模;
5. **冷启动策略**:2~3 家设计伙伴(安防/园区或内容审核,二选一做深,见开放问题 1)共建场景包(模型 + SQL 模板 + 面板),开源发布时自带开箱即用的场景演示。

## 7. 成功指标

**北极星指标:每周经 VisionQL 查询处理的视频小时数**(批 + 流折算)——同时反映采用广度与负载深度。

| 维度 | 指标 |
|---|---|
| 效率 | 典型任务(每分钟人数统计)代码量 < 30 行;从零到上线 < 30 分钟 |
| 成本 | 相对逐帧全量推理基线,优化器默认配置下 GPU 时长下降 ≥ 5x |
| 正确性 | 窗口聚合结果与手写基线管道一致(给定相同模型与采样率) |
| 采用 | 开源后 90 天:≥ 3 个真实外部场景端到端上线;≥ 1 家设计伙伴进入生产流量 |
| 留存 | 流查询平均持续在线 > 30 天(生产黏性);设计伙伴周活跃查询数持续增长 |

## 8. 开放问题

待评审与设计伙伴反馈后决策:

1. **楔子场景二选一**:安防/园区(私有化部署、渠道重、付费意愿强)vs 内容审核(云原生、决策链短、量大)——决定首发连接器与场景包的投入方向;
2. **SQL 方言基准**:对齐 PostgreSQL 习惯到什么程度(类型名、函数命名、错误码),影响生态工具兼容成本;
3. **置信度传播语义**:聚合层是否需要一等原语(如输出区间估计),还是长期保持"阈值显式"的朴素方案;
4. **跨流 JOIN 范围**:跨摄像头轨迹(ReID JOIN)落 v0.3 还是 v1.x——技术难度高,但安防场景需求强度也高;
5. **`IMAGE` 列在客户端协议中的表示**:Arrow Flight 传引用还是内联字节——影响 BI 工具直连体验与带宽占用。

---

## 附录:SQL 保留字/新增语法一览

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
| `EXPLAIN` | 运维 | 展示查询计划与预估 GPU 成本 |

## 修订记录

| 版本 | 日期 | 变更 |
|---|---|---|
| v0.1 | 2026-07-30 | 初版 |