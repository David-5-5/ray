# Ray Data 核心分析

本文档按核心文件和调用链组织 Ray Data 核心流程分析，通过交叉引用建立完整的知识体系。

> **文档结构**：正文（§1-§14）聚焦 Ray Data **自身机制**，按「架构 → API → 建图 → 算子 → 优化 → 执行」主线组织；**附录**收录超出 Ray 本身的内容——附录 A 与 Hive/Spark 等系统的横向对比，附录 B 具体代码与示例的深度分析，附录 C 关键源码索引。

---

[toc]

---

## 1. Ray Data 总体架构

Ray Data 是 Ray 生态中的分布式数据处理库，专为机器学习工作流设计。它提供了统一的 API 来加载、转换、处理和保存大规模数据集，无缝集成 Ray Train、Ray Tune 和 Ray Serve。

### 1.1 核心设计理念

| 设计理念 | 说明 |
|---------|------|
| **Lazy Execution** | 算子调用时不立即执行，而是构建执行计划 |
| **Streaming Execution** | 数据分块流式处理，避免全量加载内存 |
| **Block-based Architecture** | 数据以 Block 为单位进行分布式处理 |

### 1.2 核心目录结构

```
python/ray/data/
├── dataset.py              # Dataset 对外 API（280KB，核心文件）
├── read_api.py             # 数据读取 API（183KB）
├── iterator.py             # 数据迭代器（183KB）
├── context.py              # 执行上下文配置
├── block.py                # Block 抽象定义
├── aggregate.py            # 聚合函数
├── grouped_data.py         # 分组数据操作
├── preprocessor.py         # 数据预处理器
├── expressions.py          # 表达式 API
├── _internal/
│   ├── plan.py             # 执行计划（25KB）
│   ├── execution/
│   │   ├── streaming_executor.py      # 流式执行器（36KB）
│   │   ├── streaming_executor_state.py# 执行状态管理
│   │   ├── resource_manager.py        # 资源管理器
│   │   └── operators/                 # 物理算子
│   ├── logical/                        # 逻辑算子层
│   ├── planner/                        # 计划优化器
│   ├── datasource/                     # 数据源实现
│   └── stats.py                        # 统计指标
├── datasource/             # 对外数据源接口
├── preprocessors/          # 各种预处理器实现
└── tests/                  # 测试用例
```

### 1.3 架构分层

Ray Data 采用 5 层架构设计：

```
┌─────────────────────────────────┐
│   Dataset API (用户交互层)       │
│   dataset.py, read_api.py       │
├─────────────────────────────────┤
│   Logical Plan (逻辑计划层)      │
│   _internal/logical/            │
├─────────────────────────────────┤
│   Execution Plan (执行计划层)    │
│   _internal/plan.py             │
├─────────────────────────────────┤
│   Streaming Executor (执行层)    │
│   _internal/execution/          │
├─────────────────────────────────┤
│   Physical Operators (算子层)    │
│   _internal/execution/operators/│
└─────────────────────────────────┘
```

---

## 2. Dataset API 层

**文件位置**: `python/ray/data/dataset.py`

Dataset 是用户直接交互的入口，提供了类似 Pandas 的声明式 API。

### 2.1 类设计与核心成员

```python
@PublicAPI
class Dataset:
    def __init__(
        self,
        plan: ExecutionPlan,
        logical_plan: LogicalPlan,
    ):
        self._plan = plan              # 执行计划（物理）
        self._logical_plan = logical_plan  # 逻辑计划
        self._current_executor: Optional["Executor"] = None
```

**设计要点**:
1. **构造函数不公开** - 用户通过 `ray.data.read_*()` 工厂方法创建
2. **双计划架构** - 同时持有逻辑计划和执行计划
3. **不可变设计** - 所有转换方法返回新的 Dataset 对象（通过 `copy()`）
4. **Lazy 执行** - 只构建 DAG，触发 Action 时才真正执行

### 2.2 API 分组架构

Dataset 使用常量定义 API 分组（第 155-163 行）：

| 分组常量 | 分组名称 | 代表方法 |
|---------|---------|---------|
| `BT_API_GROUP` | Basic Transformations | `map`, `map_batches`, `filter`, `add_column` |
| `SSR_API_GROUP` | Sorting, Shuffling and Repartitioning | `sort`, `random_shuffle`, `repartition` |
| `SMJ_API_GROUP` | Splitting, Merging, Joining | `split`, `union`, `join`, `zip` |
| `GGA_API_GROUP` | Grouped and Global aggregations | `groupby`, `sum`, `mean`, `min`, `max` |
| `CD_API_GROUP` | Consuming Data | `count`, `take`, `iter_rows`, `to_pandas` |
| `IOC_API_GROUP` | I/O and Conversion | `write_parquet`, `write_csv`, `to_arrow_refs` |
| `IM_API_GROUP` | Inspecting Metadata | `schema`, `columns`, `num_blocks` |
| `E_API_GROUP` | Execution | `materialize`, `explain`, `stats` |
| `EXPRESSION_API_GROUP` | Expressions | 列表达式计算 |

### 2.3 核心 API 分类

| 类别 | 代表方法 | 对应逻辑算子 | 是否触发执行 |
|------|---------|------------|------|
| 基础转换 | `map`, `map_batches`, `flat_map`, `filter` | MapRows, MapBatches, FlatMap, Filter | Lazy |
| 排序/混洗 | `sort`, `random_shuffle`, `repartition` | Sort, RandomShuffle, Repartition | Lazy |
| 拆分/合并 | `split`, `union`, `zip`, `join` | StreamingSplit, Union, Zip, Join | Lazy |
| 聚合 | `groupby`, `sum`, `mean`, `max`, `min` | Aggregate | Lazy |
| 消费（Action） | `count`, `take`, `iter_rows`, `iter_batches` | Count, Limit | ✅ 触发 |
| 写入 | `write_parquet`, `write_csv`, `write_json` | Write | ✅ 触发 |

**执行触发点**：所有 `iter_*`、`to_*`、`write_*` 方法，以及 `count()`、`take()`、`show()`、`materialize()`。

### 2.4 ComputeStrategy 多执行模式

`map_batches`/`map`/`filter` 支持三种执行模式：

| 模式 | 实现 | 适用场景 | 关键参数 |
|------|------|---------|---------|
| **Task 模式** | 普通 Ray 任务 | 无状态处理，启动快 | 默认（`TaskPoolStrategy`） |
| **Actor Pool 模式** | Actor 池复用 | 有状态初始化（模型加载） | `compute=ActorPoolStrategy(size=8)` |
| **Actor 自动扩缩容** | Actor 动态增减 | 负载波动场景 | `concurrency=(1, 10)` |

**`map_batches` 关键参数**（第 457-489 行签名）:
- `batch_size` - 批大小，性能调优关键
- `batch_format` - "pandas", "pyarrow", "numpy" 三种格式
- `zero_copy_batch` - 零拷贝批量读取优化
- `concurrency` - Actor 并发数（min, max）

### 2.5 数据消费模式

```python
# 逐批迭代（推荐，性能好）
for batch in ds.iter_batches(batch_size=256):
    process_batch(batch)

# 框架格式
for batch in ds.iter_torch_batches(batch_size=32):
    train_step(batch)

# 全量拉取（注意内存！）
df = ds.to_pandas()          # → Pandas DataFrame
table = ds.to_arrow()        # → Arrow Table
refs = ds.to_arrow_refs()    # → List[ObjectRef]（分布式）
```

支持直接转换到 Dask / Modin / Mars / Spark DataFrame、TensorFlow Dataset（`to_tf()`）、PyTorch DataLoader（`to_torch()`）。核心迭代技术是 `DataIteratorImpl`——流式拉取，内存友好。

### 2.6 Read / Write API

**Read API**（`read_api.py`，183KB）：提供 20+ 种数据源统一读取接口，所有数据源统一实现 `Datasource` 基类，最终生成 `Read` 逻辑算子。

**Write API**（`dataset.py` 第 3781-5100 行）：与 Read 对称，所有写出源实现 `Datasink` 基类，生成 `Write` 逻辑算子。

| 方向 | 核心抽象 | 逻辑算子 |
|------|---------|---------|
| **读** | `Datasource` + `ReadTask` | `Read` 算子 |
| **写** | `Datasink` + `WriteTask` | `Write` 算子 |

对称性带来的好处：新增格式只需同时实现 `Datasource` + `Datasink`；统一的错误处理和重试；ETL 表达简洁（`read → transform → write`）。

> Read/Write 的数据源分类清单、并行读取机制、SaveMode 冲突策略等细节见 [附录 B.1](#b1-readwrite-api-细节)。

### 2.7 MaterializedDataset 子类

当 Dataset 完全执行后可转换为 `MaterializedDataset`：Block 全部在 Plasma 对象存储中、可重复消费、支持 `random_access_dataset` 索引查询。

### 2.8 调试与诊断 API

| 方法 | 作用 |
|------|------|
| `explain()` | 打印逻辑计划 + 物理计划 |
| `stats()` | 执行统计：耗时、吞吐量、内存等 |
| `schema()` | 查看数据结构（不触发执行） |
| `num_blocks()` | Block 数量（并行度指标） |

```python
print(ds.explain())
# -------- Logical Plan --------
# Count
# +- Filter
#    +- MapBatches
#       +- ReadParquet
#
# -------- Physical Plan --------
# CountOperator[Count]
# +- FilterOperator[Filter]
#    +- MapOperator[MapBatches]
#       +- InputDataBuffer[ReadParquet]
```

---

## 3. 从用户代码到 LogicalPlan：DAG 的构建

Logical Plan 将用户脚本转换为有向无环图（DAG），支持拓扑排序、算子优化、并行调度。本章解析 DAG 从 API 调用到算子图的构建机制。

### 3.1 澄清：`@ray.remote` 不生成 LogicalPlan

`@ray.remote` 是 **Ray Core** 的 task/actor API，与 Ray Data 的 LogicalPlan 无关。生成 `LogicalPlan` 的是 **Dataset API**（`ray.data.read_*().map_batches().filter()...`）。

### 3.2 无"解析"步骤：方法调用链自身即建图

关键认知：Ray Data **不做词法/句法解析**。Python 方法调用本身就是"指令流"——`ds.filter(g)` 里的 `g` 已是 Python 函数对象，无需再解析。每个变换方法内部执行统一的**四步范式**（以 `filter` 为例，`dataset.py` 约 1566-1594 行）：

```python
input_op = self._logical_plan.dag          # ① 取当前链尾算子作为输入
filter_op = Filter(input_op=input_op, ...) # ② new 新算子，input_dependencies 指向 input_op（连边）
plan = self._plan.copy()
logical_plan = LogicalPlan(filter_op, self.context)  # ③ 新算子成为新 LogicalPlan 的 dag
return Dataset(plan, logical_plan)                   # ④ 返回全新 Dataset
```

调 N 次变换方法，就逐节 new 出 N 个 `LogicalOperator` 并用 `input_dependencies` 反向连边，DAG 自动"长"出来。

### 3.3 三种标准构建范式

| 范式 | 场景 | 代码位置 | 效果 |
|------|------|---------|------|
| **入口算子（0→1）** | 创建 DAG 根节点 | `read_api.py` Read 创建 | `input_dependencies=[]`，DAG 叶子 |
| **单输入链式追加（1→1）** | 90% 的转换 API | `dataset.py` `map_batches` 772-789 行 | 当前 DAG 根作为新算子输入 |
| **多输入合并（N→1）** | Join/Union/Zip | `dataset.py` `join` 2867-2881 行 | 两个独立 DAG 作为输入 |

```
单输入链式：ds = read_parquet().map_batches(fn).filter(pred)
  Read → MapBatches(Read) → Filter(MapBatches)  ← Filter 是新根节点

多输入合并：ds1.join(ds2)
          Join
         /    \
  ds1_root    ds2_root
```

### 3.4 DAG 方向与两个 plan

- `LogicalPlan.dag` 指向**最下游**算子；沿 `input_dependencies` 向上游追溯到 Source（Read）。**这解释了为何所有优化规则都"从 dag 出发沿 input_dependencies 往上"遍历**。
- `Dataset` 同时持有两个 plan（`dataset.py:254-256`）：`_logical_plan`（算子 DAG，蓝图）与 `_plan`（`ExecutionPlan`，惰性执行容器 + 结果快照 `_snapshot_bundle`）。

### 3.5 Operator 基类：DAG 节点核心机制

`Operator`（`interfaces/operator.py` 第 5-101 行）是整个 DAG 系统的基石，实现 4 个核心机制：

**① 双向引用自动建立**——创建新算子时无需手动修改输入算子，输入算子自动感知下游依赖：

```python
class Operator:
    def __init__(self, name: str, input_dependencies: List["Operator"]):
        self._input_dependencies = input_dependencies   # 入边
        self._output_dependencies = []                  # 出边
        self._wire_output_deps(input_dependencies)       # ★ 自动建立反向引用

    def _wire_output_deps(self, input_dependencies):
        for x in input_dependencies:
            x._output_dependencies.append(self)
```

**② 后序遍历 = 拓扑排序**——保证任何算子被访问时其所有上游依赖都已访问完毕：

```python
def post_order_iter(self) -> Iterator["Operator"]:
    for op in self.input_dependencies:
        yield from op.post_order_iter()
    yield self
```

**③ 不可变变换 `_apply_transform`**——递归产生新 DAG，原始 DAG 完全不被修改（优化器基础设施，详见 [附录 B.2](#b2-operator-不可变变换机制)）。

**④ DAG 字符串表示 `dag_str`**——递归生成整个 DAG 的字符串，用于调试输出和优化器的不动点检测（见 §5）。

**不可变设计的三大价值**：分支安全（`ds1=ds.filter(a); ds2=ds.filter(b)` 互不影响）、优化安全（可对比优化前后）、重放安全（故障恢复）。

### 3.6 LogicalPlan 容器

`LogicalPlan`（`interfaces/logical_plan.py`）持有 DAG 根节点，核心能力是 `sources()`——递归找出所有无输入依赖的源算子（并行执行起点）：

```python
class LogicalPlan(Plan):
    def __init__(self, dag: LogicalOperator, context: "DataContext"):
        self._dag = dag

    def sources(self) -> List[LogicalOperator]:
        """递归找出所有源算子（无输入依赖的叶子节点）"""
        if not any(self._dag.input_dependencies):
            return [self._dag]
        sources = []
        for op in self._dag.input_dependencies:
            sources.extend(LogicalPlan(op, self._context).sources())
        return sources
```

多输入场景（如 Join 两个数据源）天然并行执行，执行器基于源算子列表初始化并发任务数。

---

## 4. 逻辑算子类型体系

### 4.1 继承树

```
Operator（interfaces/operator.py — 图结构：双向依赖边、post_order_iter、_apply_transform）
└── LogicalOperator（interfaces/logical_operator.py — 计划语义：元数据推断接口）
    ├── 【Source 零输入】Read / AbstractFrom(FromItems…FromPandas) / InputData（非 lineage 可序列化）
    ├── 【AbstractOneToOne 单入单出】— 定义 can_modify_num_rows()
    │   ├── AbstractMap（+ compute 策略 / remote args / min_rows_per_bundle / per_block_limit）
    │   │   ├── AbstractUDFMap（+ fn 及构造参数）→ MapBatches / MapRows / Filter / FlatMap
    │   │   ├── Project / StreamingRepartition
    │   │   ├── Read（源头也算 Map！）/ Write
    │   ├── Limit / Download
    ├── 【AbstractAllToAll】RandomizeBlocks / RandomShuffle / Repartition / Sort / Aggregate
    ├── 【NAry 多输入】Zip / Union / Join
    └── 其他：StreamingSplit / Count
```

**五大类速查**：

| 类型 | 基类 | 输入数 | 代表算子 | 并行特性 |
|------|------|-------|---------|---------|
| **Source** | `SourceOperator` | 0 | Read, FromItems, InputData | ✅ 完全并行 |
| **OneToOne** | `AbstractOneToOne` | 1 | MapBatches, Filter, Project, Limit | ✅ 流水线并行 |
| **AllToAll** | `AbstractAllToAll` | 1 | Sort, Repartition, RandomShuffle | ⚠️ 必须等上游全部完成 |
| **NAry** | `NAry` | N | Join, Union, Zip | ⚠️ 必须等所有输入分支完成 |
| **Terminal** | 无专用基类 | 1 | Count, Write | 执行入口 |

### 4.2 两个正交维度

- **维度 A：图形状（arity）**——零/单/多输入，决定遍历改写逻辑；
- **维度 B：物理映射目标**——`AbstractMap` → 物理 `MapOperator`（流式逐 bundle）；`AbstractAllToAll` → 物理 `AllToAllOperator`（barrier 全量交换）。

**交叉的惊喜点**：`Read` 是 Source 却继承 `AbstractMap`——读数据物理上就是一批 map 任务，故 `Read→MapBatches` 能融合；`Write` 同理，`MapBatches→Write` 也能融合，写出不产生中间物化。

### 4.3 OneToOne vs AllToAll：系统的分水岭

Dataset = 分布在集群各节点上的 Block 集合。算子分类只看一个问题：

> **为了算出某个输出块，需不需要看到别的输入块里的行？**

- 不需要 → **OneToOne**：块不动，行在块内变；流式，来一个处理一个。
- 需要 → **AllToAll**：行跨块搬家（序列化→网络→object store→barrier 等待）。

**OneToOne 家族（便宜、可流式）**：
Project（只动列，唯一能推进 Read）→ MapRows（逐行 UDF）→ MapBatches（批 UDF + `batch_size` 保护）→ Filter/FlatMap（行数变，打标记）→ Limit（单入单出但需全局计数协调）。

**AllToAll 家族（贵、barrier）**：
Repartition（纯数据移动零语义；`shuffle=False` 的 split 模式便宜、`shuffle=True` 才允许与上游 Map 融合）→ RandomShuffle（随机定去向）→ Sort（采样定边界 + 范围分发 + 局部排序，即 total order partitioning；进度条名 `SORT_SAMPLE_SUB_PROGRESS_BAR_NAME` 可见两阶段）→ Aggregate（hash(key) 分发 + map 侧预聚合 + reduce 合并）。

**分水岭视图**——所有优化规则都画在这条线上：

```
Read → MapBatches → Filter → │ Sort │ → MapBatches → Write
  ──── 流式段（融合/并发的天下）──── 水闸 ──── 流式段 ────
```

- Map↔Map 融合 = 压缩流式段；Map→AllToAll 融合 = 省掉水闸前最后一次物化；
- Limit 下推到水闸为止（Sort/Aggregate 改行数，穿过即错）。

> **记忆锚点**：OneToOne——块不动，行在块内变。AllToAll——行动，跨块搬家。看任何流水线先找 AllToAll，它把计划切成流式段，段间是物化与等待的边界。

### 4.4 设计精髓：语义属性外化为算子谓词，规则是谓词组合器

逻辑算子只声明"做什么 + 语义属性"，各优化规则是这些谓词的组合器：

| 谓词/接口 | 消费方 |
|---|---|
| `can_modify_num_rows()` | LimitPushdown（能否穿过）、Fusion（行数保护） |
| `infer_schema()` / `infer_metadata()` | Limit 直接算 `min(input_rows, limit)`；SetReadParallelism 读 `size_bytes` |
| `LogicalOperatorSupportsProjectionPushdown` mixin | ProjectionPushdown 挂载点 |
| `_compute` / `_ray_remote_args` / `_min_rows_per_bundled_input` | Fusion 兼容性判据 |
| `estimated_num_outputs()` | plan 展示、并行度推断 |

新增算子的主要工作 = 正确实现这几个谓词，各规则自动获得正确行为。

**`can_modify_num_rows()` 实测表**（逐个核对源码）：

| 值 | 算子 | 理由 |
|---|---|---|
| `False` | Project / MapRows / MapBatches / Download / StreamingRepartition | 行保持（MapBatches 一批进一批出） |
| `True` | Filter / FlatMap / **Read** / Limit | 过滤展开；Read 注释明确：reader 把输入展开成多行 |

---

## 5. 优化器与优化规则

### 5.1 优化器框架三件套

经典 Rule-based 设计，全部代码不过百行：

- **`Rule`**（`interfaces/optimizer.py`）：`apply(plan) -> plan`；`dependencies()`/`dependents()` 声明规则间顺序。
- **`Optimizer.optimize()`**：**不动点循环**——反复应用全部规则直到 `plan.dag.dag_str` 不再变化。推论：算子名必须稳定（`dag_str` 是收敛判据）。
- **`Ruleset`**（`ruleset.py`）：拓扑排序 + 循环依赖检测。

```python
class Optimizer:
    def optimize(self, plan: Plan) -> Plan:
        previous_plan = plan
        while True:
            for rule in self.rules:
                plan = rule.apply(plan)
            if plan.dag.dag_str == previous_plan.dag.dag_str:  # 不动点判据
                break
            previous_plan = plan
        return plan
```

**不动点迭代的价值**：规则间无需显式声明顺序、可扩展、自动处理规则相互触发（如 Limit 下推到 Read 后激活新优化）。局限：`dag_str` 字符串比较较脆弱。

### 5.2 两阶段管线

完整流水线（`optimizers.py` `get_execution_plan()`，第 68-81 行）：

```
逻辑优化（3 条规则）→ Planner 翻译为物理算子 → 物理优化（4 条规则）→ 执行
```

```python
def get_execution_plan(logical_plan: LogicalPlan) -> PhysicalPlan:
    optimized_logical_plan = LogicalOptimizer().optimize(logical_plan)  # 阶段1
    logical_plan._dag = optimized_logical_plan.dag
    physical_plan = create_planner().plan(optimized_logical_plan)       # 阶段2
    return PhysicalOptimizer().optimize(physical_plan)                  # 阶段3
```

**顺序有讲究**：物理阶段的融合依赖前面规则已确定的块大小和读并行度。

### 5.3 七条规则详解

**逻辑优化阶段（3 条）**：

| 规则 | 作用 + 机制要点 |
|---|---|
| **InheritBatchFormatRule** | AllToAll 算子沿单输入链向上找最近 MapBatches 继承 `batch_format`——水闸不记得格式，要向上游借 |
| **LimitPushdownRule** | 三种动作：相邻 Limit 融合取 min；保守穿过**不改行数**的 OneToOne（额外排除 MapBatches——行数不变但可能 rebatch）；Union 时每分支插本地 Limit + 保留全局 Limit；给 Read/Map 挂 `per_block_limit` 减 I/O |
| **ProjectionPushdown** | 经 `LogicalOperatorSupportsProjectionPushdown` mixin 挂载（目前仅 Parquet Read 实现）；相邻 Project 用 `_ProjectSpec`（cols+rename+exprs）合并，做重命名双射/列子集校验；**表达式不下推进 Read** |

**物理优化阶段（4 条）**：

| 规则 | 作用 + 机制要点 |
|---|---|
| **InheritTargetMaxBlockSizeRule** | 递归 DFS 携带参数向上传播 `target_max_block_size`，遇另一个 override 换值 |
| **SetReadParallelismRule** | 遍历实为 FIFO 队列 **BFS**；`_autodetect_parallelism` 综合数据大小/目标块/集群 CPU；块数不足设 `additional_split_factor`；块数超 CPU 4 倍且 ≥5000 告警 |
| **FuseOperators** | 相邻 Map 算子融合（详见 §5.4） |
| **ConfigureMapTaskMemoryUsingOutputSize** | 遍历用 `post_order_iter()` 原语；包装 `ray_remote_args_fn` 动态注入 `memory=average_bytes_per_output`；放置组调度下跳过（否则任务无法调度） |

**遍历原语两处**：`operator.py` 的 `post_order_iter()`（递归生成器，物理规则用）与 `_apply_transform()`（函数式后序重写返回新节点，逻辑规则用）。物理规则多为命令式直接改 `_input_dependencies` 指针。

### 5.4 FuseOperators：算子融合

两遍 DFS：先融 `Map→Map`，再融 `Map→AllToAll`（仅 RandomShuffle / Repartition(shuffle=True)）。

**需求本质**：`Read → MapBatches(f) → MapBatches(g)` 若各自独立成 task，中间结果要物化进 object store（序列化 + IPC）。融合后一个 Ray task 内顺序执行 `g(f(read(x)))`。Map→AllToAll 融合则把 map transformer 经 `TaskContext.upstream_map_transformer` 传给 shuffle，map 输出直接灌进 shuffle 缓冲区。

**语义门槛全在 `_can_fuse`**（遍历骨架只是外壳）：

- 上游必须 TaskPool（Task→Task / Task→Actor；Actor 池有生命周期不能并）；
- Task→Task 池大小相等；Task→Actor 时 task size 须等于 actor `max_size`（融合后一个 task 只有一份资源配置）；
- `ray_remote_args` 规范化后一致（`num_cpus` 缺省 1、`num_gpus` 缺省 0；`scheduling_strategy` 可继承）；任一方有 `_ray_remote_args_fn` 不融；
- `target_max_block_size` 不冲突（双方 override 时必须相等）；
- **行数保护**：上游 `can_modify_num_rows()` 且下游有 `min_rows_per_bundled_input` → 拒绝。保护并行度（大 batch 压缩 Read 并发）与 batching 语义（Filter 后凑不齐 batch）；
- 上游 `additional_split_factor > 1` 不融（split 破坏 1:1 数据流）。

**不对称性实例**：`filter(g).map_batches(f, batch_size=N)` 可融合；`map_batches(f, batch_size=N).filter(g)` 不可融合。

> 融合遍历骨架（while + 递归）的三个易错读点、"物理 DAG 为树"的验证，见 [附录 B.3](#b3-融合遍历骨架的三个误读修正)。

### 5.5 Planner：逻辑到物理算子转换

Planner（`planner/planner.py`）用后序递归把 Logical Operator DAG 转换为 Physical Operator DAG，只转换不优化。采用**访问者模式变体**：`_plan_recursively()` 负责遍历，各 `plan_*_op()` 函数负责具体算子处理。

```python
_DEFAULT_PLAN_FNS = {
    Read: plan_read_op,          Write: plan_write_op,
    AbstractUDFMap: plan_udf_map_op,   AbstractAllToAll: plan_all_to_all_op,
    Filter: plan_filter_op,      Join: plan_join_op,  ...
}
```

**关键事实**：`_plan_recursively` **无 memoization**——共享逻辑子图被规划成多份物理拷贝，**物理 DAG 实为树**。这是 FuseOperators 遍历假设的前提。

> Planner 递归转换、算子映射的完整代码见 [附录 B.4](#b4-planner-递归转换代码)。

### 5.6 优化器的边界：哪些优化不做

Ray Data 优化器**刻意保守**，很多数据库经典优化它不实现，责任留给用户。典型例子——**无谓词下推**：

```python
ds.sort("age").filter(expr=col("age") > 18)
# DAG 原样保留 Read → Sort → Filter（先全量排序 shuffle，再过滤）
# 不会被优化成 Read → Filter → Sort
```

**源码佐证**：逻辑规则集只有 3 条，**没有 FilterPushdown/PredicatePushdown**。唯一的下推规则 `LimitPushdownRule` 文档明文规定"遇到 Sort/Shuffle/Aggregate/Read 就停止下推"（`limit_pushdown.py:23-24`）。原因：`Sort` 继承 `AbstractAllToAll`（`all_to_all_operator.py:147`），不在 `AbstractOneToOne.can_modify_num_rows()` 判定体系内。

> **实践准则**：在 Ray Data 里手动把 filter/select 放在 sort/groupby/shuffle 之前——优化器不会替你搬。与 Spark/Hive 的差异见 [附录 A.3](#a3-优化器能力差距)。

### 5.7 研究方向与优化机会

| 方向 | 问题描述 | 技术难度 | 收益潜力 |
|------|---------|---------|---------|
| **公共子图消除** | `ds1=ds.map(a); ds2=ds.map(b)` 目前 ds 计算两次 | ★★★☆☆ | ★★★★★ |
| **算子融合扩展** | 扩展到 Filter → Map 等更多组合 | ★★☆☆☆ | ★★★★☆ |
| **谓词下推** | 实现 Filter 穿过 Sort 等重排 | ★★★☆☆ | ★★★★☆ |
| **动态拓扑调整** | 执行中发现数据倾斜，动态插入 repartition | ★★★★★ | ★★★★☆ |
| **dag_str 替代** | 字符串比较改为结构哈希比较 | ★★☆☆☆ | ★★☆☆☆ |

---

## 6. Execution Plan（执行计划层）

**核心文件**: `python/ray/data/_internal/plan.py`

ExecutionPlan 是连接 Logical Plan 和 Streaming Executor 的枢纽，负责调用三阶段优化（见 §5.2）、执行调度和结果缓存。

### 6.1 核心职责与数据结构

```python
class ExecutionPlan:
    def __init__(self, stats: DatasetStats, data_context: DataContext):
        self._in_stats = stats
        # 计算快照：已执行的算子前缀
        self._snapshot_operator: Optional[LogicalOperator] = None
        self._snapshot_bundle = None       # 缓存的结果 blocks
        self._snapshot_metadata_schema = None  # 仅元数据缓存
        self._schema = None
        self._has_started_execution = False
        self._context = data_context
```

| 职责 | 关键代码 |
|------|---------|
| 逻辑优化 / 计划转换 / 物理优化 | `LogicalOptimizer` / `Planner.plan()` / `PhysicalOptimizer` |
| Snapshot 缓存 | `_snapshot_bundle` |
| Schema 推断 | `schema()` |
| 执行入口 | `execute()` / `execute_to_iterator()` |

### 6.2 Snapshot 机制：懒执行的核心

Snapshot 解决了"懒执行但不重复计算"的问题：多次执行同一 Dataset 时，已执行的前缀不重复计算（如 `ds.take(5)` 后再 `ds.count()` 复用已计算 blocks）。

```python
def has_computed_output(self) -> bool:
    """是否已完整执行整个 DAG"""
    return (self._snapshot_bundle is not None
            and self._snapshot_operator == self._logical_plan.dag)
```

**两种执行模式**：
- `execute()` — 全量执行并缓存 blocks 到 snapshot，再次执行命中缓存直接返回；
- `execute_to_iterator()` — 流式迭代，只缓存 Schema/元数据，不缓存 blocks（适合大数据量）。

### 6.3 Schema 推断与 Copy 机制

**Schema 推断**优先级：已缓存 → 已执行有 snapshot → `dag.infer_schema()` 逻辑推断 → 必要时执行少量数据。这是"看起来不执行但能获取元数据"的关键。

**Copy 机制**（与 Logical Plan 一致的不可变原则）：`copy()` 浅拷贝共享缓存、`deep_copy()` 完全独立。保证分支安全。

---

## 7. 分布式执行：序列化边界与算子-worker 差异

**核心视角**：实现者视角——理解什么被序列化、发送到哪、如何执行。

### 7.1 关键架构澄清：中心化调度 + 分布式执行

**常见误解**：Streaming Executor 会序列化分发到各节点执行。**实际**：Executor 只在 Driver 运行。

| 架构层 | 运行位置 | 是否序列化分发 | 序列化内容 |
|--------|---------|---------------|-----------|
| **Dataset API** | Driver 本地 | ❌ 否 | 用户脚本本身不分发 |
| **Logical Plan** | Driver 本地 | ❌ 否 | 算子 DAG 不分发 |
| **Execution Plan** | Driver 本地 | ❌ 否 | 执行计划不分发 |
| **Streaming Executor** | Driver 本地线程 | ❌ 否 | **只在 Driver 运行！** |
| **Physical Operators** | Worker 分布式 | ✅ 部分 | 仅 `MapTransformer`（含 UDF）、`ObjectRef` |

**核心结论**：只有用户 UDF、包装 UDF 的 `MapTransformer`、数据引用 `ObjectRef` 会被序列化分发到 Worker。

### 7.2 什么被序列化、什么留在 driver

- **plan 不出 driver**：LogicalPlan/PhysicalPlan 只在 driver 控制"提交哪些 task"。
- **序列化并分发的是**：计算逻辑（`MapTransformer`，含 UDF）+ 数据块引用（`ObjectRef`）。
- **UDF 序列化时机**：算子**构造时** `ray.put(map_transformer)`（`map_operator.py:311`），**非执行时**；执行时 task 只传引用。
- task 实际传参（`task_pool_map_operator.py:106-112`）：`transformer_ref` + `data_context` + `ctx` + `*block_refs`。

### 7.3 四类算子的 worker 执行差异

> 核心结论：物理算子**不是**统一的"发 UDF 给 worker"模型，而是**四种截然不同的执行形态**。

| 类型 | 代表算子 | 提交方式 | 发什么 | 流式? | worker 生命周期 |
|---|---|---|---|---|---|
| **TaskPoolMap** | filter/map/无状态 map_batches | `_map_task.options().remote()`（`task_pool_map_operator.py:106`） | transformer_ref + block_refs | ✅ 逐块提交 | 用完即弃（无状态 task） |
| **ActorPoolMap** | 有状态 UDF（callable class） | 先 `ray.remote(cls)` 建 actor，再 `actor.submit.remote()`（`actor_pool_map_operator.py:204,306`） | 同上，但目标是常驻 actor | ✅ | **常驻**，跨 task 保持状态 |
| **AllToAll** | sort/shuffle/aggregate/repartition | driver 上 `_bulk_fn(input_buffer)`（`base_physical_operator.py:122`），内部经 exchange 调度器提交多轮 map/reduce/sample task | **数据对象**（如 `SortKey`），非 Python UDF | ❌ **barrier** | 多阶段 task 图 |
| **InputDataBuffer** | 源头缓冲 | **不提交 task**（`input_data_buffer.py:12`） | 仅供给已物化 block ref | — | — |

**以 `ds.filter(expr=col("age")>18).sort("age", descending=True)` 为例**：
- **filter 走 TaskPoolMap**：plan 阶段一次序列化表达式，执行时流式逐块提交 task；
- **sort 走 AllToAll**：barrier——上游 filter 100% 完成后 `all_inputs_done()` 一次性 `_bulk_fn`，内部多阶段（采样→map分区→reduce排序）；分发的是 `SortKey`（`{"age":"descending"}`）纯数据对象。

### 7.4 调度心跳

`StreamingExecutor._scheduling_loop_step`（`streaming_executor.py:450-463`）是系统心跳：`select_operator_to_run()` 挑一个"有输入+有资源"的算子 → `dispatch_next_task()`。对 Map 算子逐块提交；对 AllToAll，输入未齐时不动，齐了才 `_bulk_fn` 放水。这就是"边执行边调度、流式喂下游"的精确含义——但 AllToAll 是水闸。

> UDF 序列化完整链路（5 步）、`cached_remote_fn` 缓存机制、`filter` 的两条 UDF 路径、性能优化机会点，见 [附录 B.5](#b5-udf-序列化链路与代码详解)。

---

## 8. Streaming Executor（流式执行层）

**文件位置**: `python/ray/data/_internal/execution/streaming_executor.py`

`StreamingExecutor` 采用**独立线程 + 事件循环**的调度模型。

```
  Main Thread                 StreamingExecutor 线程（Driver 本地）
───────────┐                  ┌───────────────────────┐
           │  execute()       │  Scheduling Loop      │
           └──────────────────>  ray.wait()           │
                              │  处理完成任务          │
OutputIterator <── yield ──── │  选择下一个算子运行     │
                              └───────────┬───────────┘
     ┌──────────┐  ┌──────────┐  ┌────────▼─┐
     │  Worker  │  │  Worker  │  │  Worker  │  ← Ray Task Pool
     └──────────┘  └──────────┘  └──────────┘
```

| 机制 | 说明 | 关键函数 |
|------|------|---------|
| **拓扑构建** | 构建算子执行拓扑 | `build_streaming_topology()` |
| **资源管理** | 管理 CPU/GPU/内存预算 | `ResourceManager` |
| **调度循环** | 事件驱动的任务调度 | `ray.wait()`, `process_completed_tasks()` |
| **算子选择** | 根据资源和背压选择算子 | `select_operator_to_run()` |
| **背压控制** | 限制算子队列长度防 OOM | `target_max_block_overflow`（默认 200 blocks） |

---

## 9. Physical Operators（物理算子层）

**文件位置**: `python/ray/data/_internal/execution/operators/`

| 算子类型 | 代表类 | 用途 |
|---------|--------|------|
| **输入源** | `InputDataBuffer` | 缓存输入数据 blocks（不提交 task） |
| **Map类** | `MapOperator` / `TaskPoolMapOperator` | 逐块转换，无状态 |
| **Actor Map类** | `ActorPoolMapOperator` | 使用 Actor 池执行有状态 UDF |
| **AllToAll类** | `AllToAllOperator` | shuffle、sort、repartition（barrier） |
| **N-Ary类** | `ZipOperator`, `UnionOperator` | 多输入合并 |

**ActorPoolMapOperator** 是最强大的特性之一，适合 GPU 推理（模型加载一次多次复用）、有状态处理、昂贵初始化：

```python
ds = ds.map_batches(
    inference_fn,
    compute=ray.data.ActorPoolStrategy(size=8),  # 8个 Actor 并行
    batch_size=256,
    num_gpus=1  # 每个 Actor 占用 1 个 GPU
)
```

---

## 10. Block 系统

**文件位置**: `python/ray/data/block.py`

Ray Data 的数据以 `Block` 为单位存储和传输，每个 Block 是一个 Arrow Table / Pandas DataFrame / NumPy 数组，大小通常 128MB~512MB（可配置），是分布式调度的最小单位。

**BlockAccessor** 提供统一操作接口：

```python
class BlockAccessor:
    def num_rows(self) -> int: ...
    def size_bytes(self) -> int: ...
    def schema(self) -> Schema: ...
    def slice(self, start: int, end: int) -> Block: ...
    def to_pandas(self) -> pandas.DataFrame: ...
    def to_arrow(self) -> pyarrow.Table: ...
```

| 实现类 | 说明 |
|--------|------|
| `ArrowBlockAccessor` | Arrow Table 格式（默认） |
| `PandasBlockAccessor` | Pandas DataFrame 格式 |
| `TensorBlockAccessor` | 张量数据支持 |

**RefBundle**（`interfaces/ref_bundle.py`）是 Block 在执行流中的载体：`blocks: List[Tuple[ObjectRef[Block], BlockMetadata]]` + 元数据。

---

## 11. DataContext（全局配置中心）

**文件位置**: `python/ray/data/context.py`

```python
from ray.data.context import DataContext
ctx = DataContext.get_current()

# Block 大小
ctx.target_max_block_size = 512 * 1024 * 1024  # 512MB
ctx.target_min_block_size = 16 * 1024 * 1024   # 16MB
# 错误处理
ctx.max_errored_blocks = 0
# 背压 & 融合
ctx.target_max_block_overflow = 200
ctx.enable_fusion = True
```

---

## 12. 典型执行流程

```python
import ray
# 步骤 1: 创建 Dataset（构建 Logical Plan，Lazy）
ds = ray.data.read_parquet("data.parquet")  # Read
ds = ds.map_batches(lambda df: df * 2)      # MapBatches
ds = ds.filter(lambda row: row["x"] > 0)    # Filter
# 步骤 2: 触发执行（Action）
result = ds.count()  # 这里开始真正执行
```

**执行时序**：

```
1. ds.count() → 创建 Count 逻辑算子
2. ExecutionPlan.execute() 调用
3. 逻辑优化（Limit/Projection 下推）
4. Planner 转换 → 物理优化（融合）
   生成: InputDataBuffer → MapOperator → FilterOperator → CountOperator
5. StreamingExecutor 在独立线程启动，构建拓扑
6. 调度循环：Read 任务先提交 → map 任务 → filter 任务，结果流经管道
7. OutputIterator 产出结果 → Executor 关闭
```

**验证现象**：若 UDF `f` 有 bug，`ds.map_batches(f)` 那行不会报错，崩溃发生在 `ds.count()` 那一刻——这是"建 plan"与"执行 plan"分属两个时刻的铁证。

---

## 13. 核心特性总结

| 特性 | 说明 | 关键文件 |
|------|------|---------|
| **延迟执行** | 算子调用构建 DAG，Action 触发执行 | `plan.py`, `dataset.py` |
| **流式执行** | 数据分块流式处理，内存友好 | `streaming_executor.py` |
| **算子融合** | 相邻 Map 算子合并，减少任务开销 | `rules/operator_fusion.py` |
| **背压控制** | 上游速率适配下游消费能力 | `backpressure_policy/` |
| **Actor 池** | 有状态任务复用 Actor，避免重复初始化 | `actor_pool_map_operator.py` |
| **自动扩缩容** | 根据负载自动调整 Actor 数量 | `actor_autoscaler/` |
| **进度追踪** | 详细执行进度条和统计指标 | `progress_bar.py`, `stats.py` |

---

## 14. 测试用例参考

### 14.1 功能测试

| 测试文件 | 测试内容 |
|---------|---------|
| `test_consumption.py`（72KB） | 数据消费：`iter_rows()`, `iter_batches()`, `to_tf()`, `to_torch()` |
| `test_actor_pool_map_operator.py`（28KB） | Actor 池创建、扩缩容、有状态 UDF 执行 |
| `test_backpressure_e2e.py`（11KB） | 背压机制、内存使用控制 |
| `test_autoscaler.py`（11KB） | Actor 自动扩缩容、资源利用率 |

### 14.2 优化器测试：验证优化后 DAG

Ray Data 用一套标准模式验证 `LogicalOptimizer` 优化结果，核心工具是 `LogicalOperator.dag_str`（与不动点收敛判据同一字符串表示）。

**双重断言模式**（`test_execution_optimizer_limit_pushdown.py:14-25`）：

```python
assert ds.take_all() == expected_result                      # ① 语义正确性
assert ds._plan._logical_plan.dag.dag_str == expected_plan   # ② DAG 结构正确性
```

**测试文件分工**：

| 测试文件 | 验证对象 | 断言方式 |
|---|---|---|
| `test_execution_optimizer_limit_pushdown.py` | LimitPushdown 后逻辑 DAG | `dag.dag_str == expected` |
| `test_operator_fusion.py` | 融合后物理 DAG | 算子 `.name`（如 `"ReadParquet->MapBatches(<lambda>)"`）+ `.input_dependencies` |
| `test_projection_fusion.py` | ProjectionPushdown | dag_str / name 断言 |
| `test_ruleset.py` | 规则**排序机制**（非 DAG 结果） | `list(ruleset) == [...]`、循环检测 |

**"下推遇 Sort 停止"的可执行证据**（`test_limit_pushdown_union_with_sort:296-311`）：

```python
ds = ds1.union(ds2).sort("id").limit(5)
expected_plan = "... Union[Union] -> Sort[Sort] -> Limit[limit=5]"
# docstring: "Limit after Union + Sort: limit must NOT push through the Sort."
```

Limit 仍在 Sort 下游——官方用断言明确"有意不让下推穿过 Sort"，佐证 §5.6 的保守边界结论。

---
---

# 附录

> 以下内容超出 Ray Data 自身机制：附录 A 是与其他系统的横向对比，附录 B 是具体代码/示例的深度分析，附录 C 是源码索引。

## 附录 A：与 Hive / Spark 等系统的横向对比

### A.1 建图路径对比：编译器 vs 程序化 Builder

| | SQL 编译器路径（Hive 主流） | 程序化 Builder 路径（Ray Data） |
|---|---|---|
| 输入 | HQL 字符串 | Python 方法调用 |
| 建图 | ANTLR 解析→AST→SemanticAnalyzer 一次性生成整棵树 | 每次方法调用追加一个节点，逐节生长 |
| 列名/类型解析 | 语义分析阶段全局查 catalog | 延迟到执行前 `infer_schema` |
| 灵活度 | 仅限 SQL 可表达 | 可嵌任意 Python UDF |

Ray Data 的链式调用 ≈ **Calcite 的 `RelBuilder`**（程序化构建），而非 Hive 面向用户的 SQL 编译器。Spark 两者皆有（`spark.sql(...)` vs `df.filter().groupBy()`），最终都汇聚到 Catalyst LogicalPlan。

### A.2 算子粒度对比：子句级，非函数级

Ray 算子对应 Hive 的**子句级关系代数节点**，不是 sum/max 这种函数：

| SQL 子句 | Hive 算子 | Ray 算子 |
|---|---|---|
| FROM / WHERE / SELECT 列 / JOIN / GROUP BY / ORDER BY / LIMIT / UNION ALL | TableScan / Filter / Select / Join / GroupBy / ReduceSink / Limit / Union | Read / Filter / Project / Join / Aggregate / Sort / Limit / Union |

**sum/max 在两边都不是算子**：Hive 中是 GroupByOperator 内的 UDAF descriptor；Ray 中是 `Aggregate._aggs: List[AggregateFn]`。执行模式同构：`AggregateFn`（init/accumulate/merge/finalize，`aggregate.py`）≈ Hive UDAF 两阶段聚合 ≈ MapReduce combiner。标量表达式同理：Hive 的 ExprNode 树在 Select 算子内，Ray 的 `Expr` 树在 `Project.exprs` 内——**函数永远比算子低一层**。

**Ray 独有（SQL 无对应物）**：
- `MapBatches`/`MapRows`：UDF 逃逸口，最接近 Hive 的 `TRANSFORM ... USING`；
- `FlatMap` ≈ `LATERAL VIEW explode`；
- `Repartition`/`RandomShuffle`/`StreamingRepartition`：物理数据重分布，SQL 里只是 hint（`DISTRIBUTE BY`、BROADCAST hint）——**Ray 把物理调优语义提升为一等逻辑算子，其"逻辑计划"并不纯逻辑**。

### A.3 优化器能力差距

Hive = Calcite CBO + 上百条规则（join 重排序、分区裁剪、代价模型）；Ray = 7 条启发式规则，无代价模型、无 join 重排序（Join 算子不参与任何下推规则）。但思想同构：projection/predicate pushdown ↔ ProjectionPushdown/LimitPushdown；map 侧操作链合并进同一 mapper ↔ FuseOperators。

**Ray 相当于实现了 Hive 优化器中最不依赖统计信息、收益最确定的那一小撮规则**。这也是 §5.6"优化器保守"的根本原因——`sort().filter()` 不会被重排为 `filter().sort()`，而 Spark Catalyst / Hive Calcite 会自动做。

### A.4 执行模型类比

- OneToOne ≈ MapReduce 的 **map 侧**操作链（Hive 的 TS/FIL/SEL 全在一个 mapper 里串行）；
- AllToAll ≈ **shuffle + reduce 侧**（Hive 的 ReduceSink/GroupBy/Join 落点）；
- Ray 的 Map→AllToAll 融合 ≈ Hive 把 map 侧操作链编译进同一个 mapper。

---

## 附录 B：具体代码与示例的深度分析

### B.1 Read/Write API 细节

**Read 数据源分类**：

| 类别 | 代表方法 | 对应 Datasource 类 |
|------|---------|-------------------|
| 列式存储 | `read_parquet`, `read_iceberg`, `read_lance` | `ParquetDatasource` |
| 行式存储 | `read_csv`, `read_json`, `read_avro` | `CSVDatasource` |
| 二进制/媒体 | `read_images`, `read_video`, `read_audio` | `ImageDatasource`, `VideoDatasource` |
| 数据库 | `read_sql`, `read_bigquery`, `read_mongo` | `SQLDatasource` |
| 内存数据 | `from_items`, `from_pandas`, `from_arrow` | `RangeDatasource` |
| 外部生态 | `from_huggingface`, `from_dask`, `from_spark` | 直接转 Blocks |

**两步读取架构**：Driver 端 `create_reader()` 列文件、按大小分片生成 N 个 ReadTask（Lazy）；Executor 端每个 ReadTask 在独立 Ray Task 中解析为 Block 写入 Plasma。自动并行度推断 `_autodetect_parallelism`（默认 `target_max_block_size=512MB`，受限集群 CPU）。

**内存数据**（`from_items`/`from_pandas`）是特殊情况：不需分布式读取，直接在 Driver 端分割为 Blocks，返回 `MaterializedDataset`（已物化）。

**SaveMode 写入冲突策略**：`ERROR`（默认，已存在报错）/ `OVERWRITE` / `APPEND` / `IGNORE`。

### B.2 Operator 不可变变换机制

```python
def _apply_transform(self, transform: Callable) -> "Operator":
    """递归应用变换，保证不可变性；原始 DAG 完全不被修改，变换产生新 DAG"""
    # Step 1: 递归变换所有输入（后序）
    transformed_input_ops = [op._apply_transform(transform) for op in self.input_dependencies]
    # Step 2: 输入有变化才创建副本
    if any(new is not old for new, old in zip(transformed_input_ops, self.input_dependencies)):
        target = copy.copy(self)
        target._input_dependencies = transformed_input_ops
        target._wire_output_deps(transformed_input_ops)
    else:
        target = self
    # Step 3: 对当前节点应用变换
    return transform(target)
```

逻辑优化规则（LimitPushdown/ProjectionPushdown）都基于此做函数式后序重写。

### B.3 融合遍历骨架的三个误读修正

`_fuse_map_operators_in_dag` 是 "while 循环 + 尾递归"：

```python
upstream_ops = dag.input_dependencies
while (len(upstream_ops) == 1
       and isinstance(dag, MapOperator)
       and isinstance(upstream_ops[0], MapOperator)
       and self._can_fuse(dag, upstream_ops[0])):
    dag = self._get_fused_map_operator(dag, upstream_ops[0])  # 造新节点，重绑定
    upstream_ops = dag.input_dependencies
dag._input_dependencies = [
    self._fuse_map_operators_in_dag(op) for op in upstream_ops  # 越过边界继续
]
```

经源码验证，三个容易读偏的点：

1. **`len(upstream_ops) == 1` 是防御性摆设，不是"线性链判据"**。`planner.py` `_plan_recursively` **无 memoization**——共享逻辑子图被规划成多份物理拷贝，**物理 DAG 实为树**；MapOperator 构造上恒单输入。该条件只对 Union/InputDataBuffer 起廉价短路作用，真正门槛是两个 `isinstance` + `_can_fuse`。
2. **非"就地融合"，是"新建 + 重绑定"**。`_get_fused_map_operator` 每轮 `MapOperator.create(...)` 造全新算子（transformer 合并、名 `"up->down"`、输入接到上游的上游）；对既有图的唯一写操作是循环退出后的 `dag._input_dependencies = [...]`。
3. **递归职责 = 越过任意边界继续往上游找**，分叉（Union）只是边界的一种。`Map → Shuffle → Map → Map` 中靠递归跳过 Shuffle 才融合上游链。

不对称性只在 `_can_fuse_map_ops`（约 492-503 行）可见，while 循环完全不体现。

### B.4 Planner 递归转换代码

```python
def _plan_recursively(self, logical_op, data_context):
    """后序递归转换：先转换输入依赖，再转换当前算子"""
    op_map = {}
    physical_children = []
    for child in logical_op.input_dependencies:          # Step1 递归转换输入（后序）
        physical_child, child_op_map = self._plan_recursively(child, data_context)
        physical_children.append(physical_child)
        op_map.update(child_op_map)
    plan_fn = self.get_plan_fn(logical_op)               # Step2 查找转换函数
    physical_op = plan_fn(logical_op, physical_children, data_context)  # Step3 生成物理算子
    # Step4 建立物理→逻辑反向映射（一个物理算子可能对应多个逻辑算子——融合后）
    queue = [physical_op]
    while queue:
        curr = queue.pop()
        if curr._logical_operators:
            break
        curr.set_logical_operators(logical_op)
        op_map[curr] = logical_op
        queue.extend(curr.input_dependencies)
    op_map[physical_op] = logical_op
    return physical_op, op_map
```

### B.5 UDF 序列化链路与代码详解

**5 步序列化流程**：

```
用户 UDF
 → 1. Dataset API 包装为 MapTransformFn
 → 2. 多个 MapTransformFn 封装为 MapTransformer
 → 3. MapTransformer 放入对象存储（ray.put）
 → 4. cached_remote_fn 提交到 Ray 调度器
 → 5. Worker 反序列化 MapTransformer 并执行 _map_task
```

**`cached_remote_fn` 缓存机制**（`remote_fn.py`）——避免重复 cloudpickle 序列化相同函数：

```python
CACHED_FUNCTIONS = {}
def cached_remote_fn(fn, **ray_remote_args):
    cache_key = (fn, hash(_make_hashable(ray_remote_args)))
    if cache_key not in CACHED_FUNCTIONS:
        CACHED_FUNCTIONS[cache_key] = ray.remote(**ray_remote_args)(fn)
    return CACHED_FUNCTIONS[cache_key]
```

**任务提交入口**（`task_pool_map_operator.py`）：

```python
gen = self._map_task.options(**dynamic_ray_remote_args).remote(
    self._map_transformer_ref,  # ✅ ray.put 预放对象存储，仅传引用
    data_context,               # ❌ 每次任务都序列化 ★ 优化点
    ctx,                        # ❌ 每次任务都序列化 ★ 优化点
    *bundle.block_refs,         # ✅ ObjectRef，轻量引用
)
```

**`filter` 的两条 UDF 路径**（`plan_udf_map_op.py:208-262`）：

| 写法 | 序列化发出的"UDF" | worker 执行 | 物理 transform |
|---|---|---|---|
| `filter(fn=lambda r: r["age"]>18)` | 你的**函数**（cloudpickle） | 逐行调 Python `fn(row)` | `RowMapTransformFn` |
| `filter(expr=col("age")>18)` | **表达式树**（结构化数据，无 Python 函数） | Arrow 列式向量化（`block_accessor.filter(expr)`） | `BlockMapTransformFn` |

官方推荐 `expr` 写法（native、无 Python 调用开销）——`dataset.py` 带 "Use 'expr' instead of 'fn' when possible" 提示。

**5 个性能优化机会点**：

| 优化方向 | 文件位置 | 改进思路 |
|------|---------|---------|
| DataContext 增量序列化 | `task_pool_map_operator.py` | 大部分字段不变，只序列化差异 |
| TaskContext 预序列化 | `task_pool_map_operator.py` | 任务间 ctx 结构相似，复用序列化结果 |
| MapTransformer 压缩 | `map_transformer.py` | 大 UDF 链压缩后再放对象存储 |
| 动态参数缓存失效优化 | `remote_fn.py` | 按参数子集分层缓存 |
| 闭包捕获分析 | 用户 UDF | 检测警告不必要的大对象闭包捕获 |

### B.6 多模态处理端到端示例

`test_e2e_prediction`（`tests/test_image.py:143-172`）演示图片推理管道范式：

```python
dataset = ray.data.read_images("example://image-datasets/simple")
def preprocess(batch):  # 张量化
    return {"out": np.stack([transform(image) for image in batch["image"]])}
dataset = dataset.map_batches(preprocess, batch_format="numpy")

class Predictor:        # 有状态模型（Actor 池）
    def __init__(self):
        self.model = resnet18(pretrained=True)
    def __call__(self, batch):
        with torch.inference_mode():
            return {"prediction": self.model(torch.as_tensor(batch["out"]))}

predictions = dataset.map_batches(
    Predictor, compute=ray.data.ActorPoolStrategy(min_size=1), batch_size=4096)
```

这是 `read → map_batches(预处理) → map_batches(ActorPool 模型推理)` 的标准范式，可套用到音频 embedding、视频行为识别等自定义 UDF。

---

## 附录 C：关键源码索引

| 内容 | 位置 |
|---|---|
| Dataset API | `python/ray/data/dataset.py` |
| Read/Write API | `python/ray/data/read_api.py` |
| Rule/Optimizer 基类、不动点循环 | `python/ray/data/_internal/logical/interfaces/optimizer.py` |
| Ruleset 拓扑排序 | `python/ray/data/_internal/logical/ruleset.py` |
| 两套规则注册、两阶段管线 | `python/ray/data/_internal/logical/optimizers.py` |
| 融合规则（核心） | `python/ray/data/_internal/logical/rules/operator_fusion.py` |
| Limit/Projection 下推 | `.../rules/limit_pushdown.py`、`.../rules/projection_pushdown.py` |
| 遍历原语 / Operator 基类 | `python/ray/data/_internal/logical/interfaces/operator.py` |
| 算子继承树 | `python/ray/data/_internal/logical/operators/` |
| Planner（无 memo，物理 DAG 为树） | `python/ray/data/_internal/planner/planner.py` |
| filter 物理规划（fn/expr 两路径） | `python/ray/data/_internal/planner/plan_udf_map_op.py` |
| ExecutionPlan / Snapshot | `python/ray/data/_internal/plan.py` |
| Streaming Executor 调度循环 | `python/ray/data/_internal/execution/streaming_executor.py` |
| Map/AllToAll 物理算子 | `python/ray/data/_internal/execution/operators/` |
| AggregateFn 四段式 | `python/ray/data/aggregate.py` |
| 优化器测试 | `python/ray/data/tests/test_execution_optimizer_*.py`、`test_operator_fusion.py`、`test_ruleset.py` |
