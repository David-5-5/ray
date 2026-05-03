# Ray Data 核心分析

本文档按核心文件和调用链组织 Ray Data 核心流程分析，通过交叉引用建立完整的知识体系。

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

### 2.1 典型使用流程

```python
import ray

# 1. 创建 Dataset（Lazy，不立即执行）
ds = ray.data.read_parquet("s3://bucket/data")  # -> Read 逻辑算子

# 2. 转换操作（继续构建 Logical Plan）
ds = ds.map_batches(lambda df: df[df["value"] > 0])  # -> MapBatches 逻辑算子
ds = ds.filter(lambda row: row["label"] != -1)       # -> Filter 逻辑算子
ds = ds.repartition(100)                             # -> Repartition 逻辑算子

# 3. 触发执行（Action 操作）
result = ds.count()           # 触发执行 -> Count 算子
materialized = ds.materialize()  # 全量执行并缓存
```

### 2.2 API 分类

| 类别 | 代表方法 | 对应逻辑算子 |
|------|---------|------------|
| 基础转换 | `map_batches()`, `map_rows()`, `flat_map()`, `filter()` | MapBatches, MapRows, FlatMap, Filter |
| 排序/混洗 | `sort()`, `random_shuffle()`, `repartition()` | Sort, RandomShuffle, Repartition |
| 拆分/合并 | `split()`, `union()`, `zip()`, `train_test_split()` | StreamingSplit, Union, Zip |
| 聚合 | `groupby()`, `sum()`, `mean()`, `max()`, `min()` | Aggregate |
| 消费 | `count()`, `take()`, `iter_rows()`, `iter_batches()` | Count, Limit |
| 写入 | `write_parquet()`, `write_csv()`, `write_json()` | Write |

### 2.3 explain() 调试工具

查看完整执行计划是理解 Ray Data 执行的关键：

```python
print(ds.explain())
```

示例输出：
```
-------- Logical Plan --------
Count
+- Filter
   +- MapBatches
      +- ReadParquet

-------- Physical Plan --------
CountOperator[Count]
+- FilterOperator[Filter]
   +- MapOperator[MapBatches]
      +- InputDataBuffer[ReadParquet]
```

---

## 3. Logical Plan（逻辑计划层）

**文件位置**: `python/ray/data/_internal/logical/`

当用户调用 Dataset 的转换方法时，Ray Data 并不会立即执行，而是构建一个逻辑算子 DAG。

### 3.1 逻辑算子 DAG 构建

```
用户调用链:
  read_parquet() -> map_batches() -> filter() -> count()

生成的 Logical Plan DAG:
  Read (DataSource) -> MapBatches (UDF) -> Filter (Predicate) -> Count
```

### 3.2 逻辑算子类型

| 算子类型 | 说明 | 包含算子 |
|---------|------|---------|
| **InputDataOperator** | 输入数据算子 | InputData |
| **Read** | 读取数据源算子 | Read |
| **MapOperator** | 逐元素转换 | MapBatches, MapRows, Filter, FlatMap, Project |
| **AllToAllOperator** | 全局混洗操作 | Sort, RandomShuffle, Repartition, Aggregate |
| **NAryOperator** | 多输入合并 | Union, Zip |
| **WriteOperator** | 写入数据 | Write |
| **终端算子** | 触发执行 | Count, Limit |

---

## 4. Execution Plan（执行计划层）

**文件位置**: `python/ray/data/_internal/plan.py`

`ExecutionPlan` 是连接逻辑计划和物理执行的桥梁。

### 4.1 核心职责

1. **逻辑优化**：算子融合（Fusion）、谓词下推、投影下推
2. **物理计划生成**：将 Logical Operator 转换为 Physical Operator
3. **执行调度**：创建 StreamingExecutor 并调度执行

### 4.2 ExecutionPlan 核心方法

```python
class ExecutionPlan:
    def create_executor(self) -> StreamingExecutor:
        """创建流式执行器"""
        return StreamingExecutor(self._context, self.get_dataset_id())
    
    def explain(self) -> str:
        """打印逻辑计划和物理计划"""
        logical_plan = self._logical_plan
        physical_plan = get_execution_plan(self._logical_plan)
        return logical_plan_str + physical_plan_str
```

---

## 5. Streaming Executor（流式执行层）

**文件位置**: `python/ray/data/_internal/execution/streaming_executor.py`

`StreamingExecutor` 是 Ray Data 的执行核心，采用**独立线程 + 事件循环**的调度模型。

### 5.1 调度模型

```
                                     ┌─────────────────────────────┐
                                     │   StreamingExecutor 线程     │
                                     │                             │
  Main Thread                        │  ┌───────────────────────┐  │
───────────┐                         │  │  Scheduling Loop      │  │
           │  execute()              │  │  ┌─────────────────┐  │  │
           └─────────────────────────────> │ ray.wait()       │  │  │
                                         │  │ 处理完成任务     │  │  │
OutputIterator <─── yield blocks ──────── │ 选择下一个算子运行 │  │  │
                                         │  └─────────────────┘  │  │
                                         └───────────────────────┘  │
                                                                  │
     ┌──────────┐    ┌──────────┐    ┌──────────┐                   │
     │  Worker  │    │  Worker  │    │  Worker  │ <──────────────────┘
     └────┬─────┘    └────┬─────┘    └────┬─────┘
          │                │                │
          └────────────────┴────────────────┘
                   Ray Task Pool
```

### 5.2 核心执行机制

| 机制 | 说明 | 关键函数 |
|------|------|---------|
| **拓扑构建** | 构建算子执行拓扑 | `build_streaming_topology()` |
| **资源管理** | 管理 CPU/GPU/内存预算 | `ResourceManager` |
| **调度循环** | 事件驱动的任务调度 | `ray.wait()`, `process_completed_tasks()` |
| **算子选择** | 根据资源和背压选择算子 | `select_operator_to_run()` |
| **背压控制** | 限制算子队列长度，防止 OOM | `target_max_block_overflow` |

### 5.3 背压机制

- 限制每个算子的队列长度（默认 200 个 blocks）
- 防止上游生产速度远超下游消费导致 OOM
- 可配置：`DataContext.target_max_block_overflow`

---

## 6. Physical Operators（物理算子层）

**文件位置**: `python/ray/data/_internal/execution/operators/`

### 6.1 算子分类

| 算子类型 | 代表类 | 用途 |
|---------|--------|------|
| **输入源** | `InputDataBuffer` | 缓存输入数据 blocks |
| **Map类** | `MapOperator` | 逐块转换，无状态 |
| **Actor Map类** | `ActorPoolMapOperator` | 使用 Actor 池执行有状态 UDF |
| **AllToAll类** | `AllToAllOperator` | shuffle、sort、repartition |
| **N-Ary类** | `ZipOperator`, `UnionOperator` | 多输入合并 |
| **Output** | `OutputOperator` | 输出结果 |

### 6.2 ActorPoolMapOperator

这是 Ray Data 最强大的特性之一，特别适合：
- GPU 推理（模型加载一次，多次复用）
- 有状态处理
- 昂贵的初始化操作

```python
ds = ds.map_batches(
    inference_fn,
    compute=ray.data.ActorPoolStrategy(size=8),  # 8个 Actor 并行
    batch_size=256,
    num_gpus=1  # 每个 Actor 占用 1 个 GPU
)
```

---

## 7. Block 系统

### 7.1 Block 抽象

**文件位置**: `python/ray/data/block.py`

Ray Data 的数据以 `Block` 为单位存储和传输，每个 Block 是：
- 一个 Arrow Table、Pandas DataFrame 或 NumPy 数组
- 大小通常在 128MB ~ 512MB 之间（可配置）
- 是分布式调度的最小单位

### 7.2 BlockAccessor 接口

`BlockAccessor` 提供统一的 Block 操作接口：

```python
class BlockAccessor:
    def num_rows(self) -> int: ...
    def size_bytes(self) -> int: ...
    def schema(self) -> Schema: ...
    def slice(self, start: int, end: int) -> Block: ...
    def to_pandas(self) -> pandas.DataFrame: ...
    def to_arrow(self) -> pyarrow.Table: ...
    def to_numpy(self, column: str) -> np.ndarray: ...
```

### 7.3 Block 实现类

| 实现类 | 说明 |
|--------|------|
| `ArrowBlockAccessor` | Arrow Table 格式（默认） |
| `PandasBlockAccessor` | Pandas DataFrame 格式 |
| `TensorBlockAccessor` | 张量数据支持 |

### 7.4 RefBundle

**文件位置**: `python/ray/data/_internal/execution/interfaces/ref_bundle.py`

`RefBundle` 是 Block 在执行流中的载体，包含：
- `blocks: List[Tuple[ObjectRef[Block], BlockMetadata]]`
- 元数据：行数、大小、schema 等

---

## 8. DataContext（全局配置中心）

**文件位置**: `python/ray/data/context.py`

`DataContext` 是 Ray Data 的全局配置管理器，控制着执行的方方面面。

### 8.1 常用配置

```python
from ray.data.context import DataContext

ctx = DataContext.get_current()

# Block 大小配置
ctx.target_max_block_size = 512 * 1024 * 1024  # 512MB
ctx.target_min_block_size = 16 * 1024 * 1024   # 16MB

# 错误处理
ctx.max_errored_blocks = 0  # 允许的错误 block 数（0=不允许）

# 执行配置
ctx.execution_options.verbose_progress = True

# 自动扩缩容配置
ctx.autoscaling_config = {
    "max_autoscale_workers": 100,
    "min_autoscale_workers": 1
}

# 背压配置
ctx.target_max_block_overflow = 200  # 算子队列最大长度
ctx.enable_fusion = True  # 启用算子融合优化
```

---

## 9. 典型执行流程

让我们追踪一个完整的执行路径：

```python
import ray

# 步骤 1: 创建 Dataset（构建 Logical Plan）
ds = ray.data.read_parquet("data.parquet")  # ReadOperator
ds = ds.map_batches(lambda df: df * 2)      # MapBatchesOperator
ds = ds.filter(lambda row: row["x"] > 0)    # FilterOperator

# 步骤 2: 触发执行（Action）
result = ds.count()  # 这里开始真正执行
```

### 9.1 执行时序

```
1. User calls ds.count()
   ↓
2. Dataset.count() creates Count logical operator
   ↓
3. ExecutionPlan.execute() is called
   ↓
4. Logical optimizer runs (operator fusion, etc.)
   ↓
5. Physical plan generated: InputDataBuffer -> MapOperator -> FilterOperator -> CountOperator
   ↓
6. StreamingExecutor created and started on a separate thread
   ↓
7. Topology built, resource manager initialized
   ↓
8. Scheduling loop runs:
   - Read tasks submitted first
   - As read tasks complete, map tasks submitted
   - As map tasks complete, filter tasks submitted
   - Results flow through the pipeline
   ↓
9. OutputIterator yields results to user
   ↓
10. Executor shuts down when done
```

---

## 10. 核心特性总结

| 特性 | 说明 | 关键文件 |
|------|------|---------|
| **延迟执行** | 算子调用构建 DAG，Action 触发执行 | `plan.py`, `dataset.py` |
| **流式执行** | 数据分块流式处理，内存友好 | `streaming_executor.py` |
| **算子融合** | 相邻 Map 算子合并，减少任务开销 | `planner/fusion.py` |
| **背压控制** | 上游速率适配下游消费能力 | `backpressure_policy/` |
| **Actor 池** | 有状态任务复用 Actor，避免重复初始化 | `actor_pool_map_operator.py` |
| **自动扩缩容** | 根据负载自动调整 Actor 数量 | `actor_autoscaler/` |
| **进度追踪** | 详细的执行进度条和统计指标 | `progress_bar.py`, `stats.py` |

---

## 11. 测试用例参考

### 11.1 基础功能测试

| 测试文件 | 大小 | 测试内容 |
|---------|------|---------|
| `test_consumption.py` | 72KB | 数据消费方式测试：`iter_rows()`, `iter_batches()`, `to_tf()`, `to_torch()` |

### 11.2 算子测试

| 测试文件 | 大小 | 测试内容 |
|---------|------|---------|
| `test_actor_pool_map_operator.py` | 28KB | Actor 池的创建、扩缩容、错误处理，有状态 UDF 的执行 |

### 11.3 执行器测试

| 测试文件 | 大小 | 测试内容 |
|---------|------|---------|
| `test_backpressure_e2e.py` | 11KB | 背压机制正确工作、内存使用控制 |

### 11.4 资源管理测试

| 测试文件 | 大小 | 测试内容 |
|---------|------|---------|
| `test_autoscaler.py` | 11KB | Actor 自动扩缩容逻辑、资源利用率优化 |
