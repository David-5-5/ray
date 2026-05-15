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

### 2.3 核心 API 分类详解

#### 2.3.1 基础转换（Lazy，不执行）

**代表方法**: `map()`, `map_batches()`, `flat_map()`, `filter()`

```python
def map(
    self,
    fn: Callable[[Dict[str, Any]], Dict[str, Any]],
    *,
    compute: Optional[ComputeStrategy] = None,  # Task / Actor 选择
    num_cpus: Optional[float] = None,            # 资源配置
    num_gpus: Optional[float] = None,
    concurrency: Optional[Union[int, Tuple]] = None,  # Actor 并发数
    **ray_remote_args,
) -> "Dataset":
    """Lazy: 只构建 MapRows 逻辑算子，不立即执行"""
```

**关键技术点**:
- **返回新 Dataset** - 通过 `copy()` 创建新实例，原对象不变
- **ComputeStrategy** - 支持 Task 模式（默认）或 Actor 模式（有状态）
- **资源隔离** - 每个 UDF 可独立配置 CPU/GPU

#### 2.3.2 全局混洗（All-to-All）

**代表方法**: `sort()`, `random_shuffle()`, `repartition()`

**关键技术点**:
- **Map + Reduce 两阶段** - 先分片采样，再全局排序
- **Exchange 算子** - 跨节点数据传输，网络开销大
- **SortKey 抽象** - 支持多列排序、升降序配置

#### 2.3.3 Action 算子（触发执行）

**代表方法**: `count()`, `take()`, `materialize()`, `iter_rows()`

```python
def count(self) -> int:
    """触发执行：构建 Count 算子并执行"""
    return self._execute_with_count(Count(self._logical_plan.dag))

def materialize(self) -> "MaterializedDataset":
    """全量执行并缓存所有 Block 到对象存储"""
```

**执行触发点**:
- 所有 `iter_*` 系列方法
- 所有 `to_*` 转换方法
- 所有 `write_*` 输出方法
- `count()`, `take()`, `show()` 等立即返回结果的方法

#### 2.3.4 I/O 方法

**代表方法**: `write_parquet()`, `write_csv()`, `write_json()` 等 10+ 种

**设计模式**:
- 每种格式对应一个 `Datasink` 类（如 `ParquetDatasink`）
- `Write` 逻辑算子封装写入操作
- 支持 `SaveMode`: `append`, `overwrite`, `error`, `ignore`

### 2.4 Dataset 调用链构建机制

**核心模式**: 链式调用 + 不可变对象

```
用户调用:
  ds = ray.data.read_parquet("path")  # Read 算子
       .map_batches(fn)               # MapBatches 追加
       .filter(pred)                   # Filter 追加
       .count()                        # Count 触发执行
```

**内部实现**:
1. **Copy 新对象** - 每个转换方法先调用 `Dataset.copy()` 创建新实例
2. **追加逻辑算子** - 在新实例的 `_logical_plan` 上追加 Operator
3. **返回新实例** - 原对象完全不变，支持分支处理

```python
# copy 方法实现（第 265 行）
@staticmethod
def copy(ds: "Dataset", _deep_copy: bool = False, _as: Optional[type] = None) -> "Dataset":
    """创建新 Dataset 实例，共享计划对象"""
    if _deep_copy:
        return _as(ds._plan.deep_copy(), ds._logical_plan)
    else:
        return _as(ds._plan.copy(), ds._logical_plan)
```

### 2.5 ComputeStrategy 多执行模式

`map_batches` 支持三种执行模式：

| 模式 | 实现 | 适用场景 | 关键参数 |
|------|------|---------|---------|
| **Task 模式** | 普通 Ray 任务 | 无状态处理，启动快 | 默认 |
| **Actor Pool 模式** | Actor 池复用 | 有状态初始化（模型加载） | `compute=ActorPoolStrategy(size=8)` |
| **Actor 自动扩缩容** | Actor 动态增减 | 负载波动场景 | `concurrency=(1, 10)` |

**关键参数**（第 457-489 行 `map_batches` 签名）:
- `batch_size` - 批大小，性能调优关键
- `batch_format` - "pandas", "pyarrow", "numpy" 三种格式
- `zero_copy_batch` - 零拷贝批量读取优化
- `concurrency` - Actor 并发数（min, max）

### 2.6 数据消费模式

Dataset 支持多种消费方式，满足不同场景：

#### 2.6.1 迭代器模式（推荐）

```python
# 逐行迭代
for row in ds.iter_rows():
    process(row)

# 逐批迭代（推荐，性能好）
for batch in ds.iter_batches(batch_size=256):
    process_batch(batch)

# PyTorch 格式
for batch in ds.iter_torch_batches(batch_size=32):
    train_step(batch)
```

**关键技术**: `DataIteratorImpl` - 流式拉取，内存友好

#### 2.6.2 全量转换模式

```python
# 全部拉取到本地（注意内存！）
df = ds.to_pandas()          # → Pandas DataFrame
table = ds.to_arrow()        # → Arrow Table
refs = ds.to_arrow_refs()    # → List[ObjectRef]（分布式）
```

#### 2.6.3 外部系统集成

支持直接转换到：
- Dask / Modin / Mars 分布式 DataFrame
- Spark DataFrame
- TensorFlow Dataset (`to_tf()`)
- PyTorch DataLoader (`to_torch()`)

### 2.7 执行调试与诊断 API

| 方法 | 作用 |
|------|------|
| `explain()` | 打印逻辑计划 + 物理计划（见第 6102 行） |
| `stats()` | 执行统计：耗时、吞吐量、内存等 |
| `schema()` | 查看数据结构（不触发执行） |
| `num_blocks()` | Block 数量（并行度指标） |
| `size_bytes()` | 数据总大小 |

### 2.8 MaterializedDataset 子类

当 Dataset 完全执行后，可转换为 `MaterializedDataset`：
- Block 全部在 Plasma 对象存储中
- 可重复消费，不需要重新计算
- 支持 `random_access_dataset` 索引查询

### 2.9 Read API - 数据源读取接口

**文件位置**: `python/ray/data/read_api.py`（183KB，第二大文件）

Read API 是 Ray Data 的**入口层**，提供 20+ 种数据源的统一读取接口。

#### 2.9.1 数据源分类

| 类别 | 代表方法 | 对应 Datasource 类 |
|------|---------|-------------------|
| **列式存储** | `read_parquet`, `read_iceberg`, `read_lance` | `ParquetDatasource`, `IcebergDatasource` |
| **行式存储** | `read_csv`, `read_json`, `read_avro` | `CSVDatasource`, `ArrowJSONDatasource` |
| **半结构化** | `read_webdataset` | `WebDatasetDatasource` |
| **二进制/媒体** | `read_images`, `read_video`, `read_audio` | `ImageDatasource`, `VideoDatasource` |
| **数组格式** | `read_numpy`, `read_tfrecords` | `NumpyDatasource`, `TFRecordDatasource` |
| **数据库** | `read_sql`, `read_bigquery`, `read_mongo`, `read_clickhouse` | `SQLDatasource`, `BigQueryDatasource` |
| **数据湖** | `read_hudi`, `read_delta` | `HudiDatasource`, `DeltaSharingDatasource` |
| **内存数据** | `from_items`, `from_pandas`, `from_arrow`, `from_blocks` | `RangeDatasource` |
| **外部生态** | `from_huggingface`, `from_dask`, `from_spark`, `from_torch` | 直接转换为 Blocks |

#### 2.9.2 核心设计模式：Datasource 抽象

所有数据源统一实现 `Datasource` 基类，提供：

```python
# 核心方法协议
class Datasource:
    def create_reader(self, **read_args) -> Reader:
        """创建 Reader 实例，准备读取任务"""
        
    def prepare_read(self, parallelism: int, **read_args) -> List[ReadTask]:
        """生成并行读取任务列表 - 最核心的方法"""
        
    def on_read_complete(self, read_tasks: List[ReadTask]) -> None:
        """读取完成后的回调"""
```

**关键设计点**（第 67-68 行 `Read` 逻辑算子）：
```python
# 所有 read_*() 方法最终生成 Read 逻辑算子
Read(datasource, parallelism, read_args)
```

#### 2.9.3 并行读取机制

**两步读取架构**：

```
Step 1: Driver 端（本地执行）
  ↓
  read_parquet("s3://bucket/*")
  ↓
  1. 调用 datasource.create_reader()
  2. 列出所有文件 / 分区
  3. 按文件大小分片，生成 N 个 ReadTask
  4. 返回 Read 逻辑算子（Lazy，不执行）

Step 2: Executor 端（分布式执行）
  ↓
  每个 ReadTask 在独立 Ray Task 中执行
  ↓
  1. Worker 拉取文件分片
  2. 解析为 Arrow/Pandas Block
  3. 写入 Plasma 对象存储
  ↓
  输出 N 个 Block
```

**自动并行度推断**（第 72 行 `_autodetect_parallelism`）:
- 默认: `target_max_block_size = 512MB`
- 按文件大小估算所需 Block 数
- 受限于集群可用 CPU 数

#### 2.9.4 内存数据读取

`from_items`, `from_pandas`, `from_arrow` 等方法是**特殊情况**：
- 不需要分布式读取
- 直接在 Driver 端分割为 Blocks
- 调用 `FromItems`, `FromPandas`, `FromArrow` 等逻辑算子
- 直接返回 `MaterializedDataset`（已物化）

```python
# from_blocks 直接创建物化数据集（第 128-154 行）
def from_blocks(blocks: List[Block]):
    block_refs = [ray.put(block) for block in blocks]
    from_blocks_op = FromBlocks(block_refs, meta)
    return MaterializedDataset(execution_plan, logical_plan)
```

#### 2.9.5 高级特性

| 特性 | 说明 | 关键实现 |
|------|------|---------|
| **分区推断** | 自动识别 Hive 风格目录分区 | `Partitioning` 类 |
| **Schema 推断** | 采样前 N 个文件推断 Schema | 在 `prepare_read` 阶段执行 |
| **谓词下推** | Iceberg/Parquet 支持行过滤 | 底层引擎直接下推 |
| **列裁剪** | 只读取需要的列，减少 IO | `columns` 参数 |
| **文件元数据缓存** | 避免重复 list S3 等操作 | `FileMetadataProvider` 抽象 |

### 2.10 Write API - 数据写出接口

**文件位置**: `dataset.py` 第 3781-5100 行

与 Read API 对称，Ray Data 提供 10+ 种写出格式的统一接口。

#### 2.10.1 写出格式一览

| 类别 | 代表方法 | 对应 Datasink 类 |
|------|---------|-------------------|
| **列式存储** | `write_parquet`, `write_iceberg`, `write_lance` | `ParquetDatasink`, `IcebergDatasink` |
| **行式存储** | `write_csv`, `write_json` | `CSVDatasink`, `JSONDatasink` |
| **二进制/媒体** | `write_images`, `write_numpy` | `ImageDatasink`, `NumpyDatasink` |
| **机器学习** | `write_tfrecords`, `write_webdataset` | `TFRecordDatasink`, `WebDatasetDatasink` |
| **数据库** | `write_sql`, `write_bigquery`, `write_mongo`, `write_clickhouse`, `write_snowflake` | `SQLDatasink`, `MongoDatasink` |

#### 2.10.2 核心设计模式：Datasink 抽象

所有写出源实现 `Datasink` 基类（第 112 行 `dataset.py` import）：

```python
# 核心写出协议
class Datasink:
    def write(self, blocks: List[ObjectRef[Block]], **kwargs) -> WriteResult:
        """将 Blocks 写出到目标系统"""
        
    def on_write_complete(self, write_results: List[WriteResult]) -> None:
        """写出完成后的回调"""
```

**对应逻辑算子**（第 84 行 `Read` 的对称算子）：
```python
# Write 逻辑算子封装写出操作
Write(datasink, ds._logical_plan.dag)
```

#### 2.10.3 分布式写入机制

**并行写出流程**：
```
用户调用 ds.write_parquet("s3://bucket/output")
    ↓
1. 触发 Dataset 执行（Lazy → 执行）
   ↓
2. 每个 Block 对应一个写出 Task
   ↓
3. Worker 分布式并行写出
   ├── 每个 Task 写一个或多个文件
   ├── 文件名自动编号: part-00000.parquet
   └── 支持按列分区写入
    ↓
4. 返回 WriteResult 汇总
```

#### 2.10.4 SaveMode：写入冲突策略

**第 112 行 `SaveMode` 枚举**：
```python
from ray.data.datasource import SaveMode
```

| 模式 | 行为 | 适用场景 |
|------|------|---------|
| `ERROR` | 目录已存在则报错（默认） | 安全写入，防止覆盖 |
| `OVERWRITE` | 覆盖已有目录 | 重跑场景 |
| `APPEND` | 追加到已有目录 | 增量写入 |
| `IGNORE` | 目录已存在则跳过 | 幂等写入场景 |

#### 2.10.5 高级写出特性

| 特性 | 说明 | 关键参数 |
|------|------|---------|
| **按列分区写出** | Hive 风格分区目录 | `partition_cols` |
| **文件数控制** | 调整并行度控制文件数 | `repartition()` + write |
| **文件名模板** | 自定义输出文件名 | `filename_provider` |
| **行组大小** | Parquet 行组优化 | `row_group_size_bytes` |
| **分桶写出** | 按哈希分桶输出 | `bucket_col` |

#### 2.10.6 Read/Write 对称性设计

Ray Data 的 I/O 设计高度对称：

| 方向 | 核心抽象 | 逻辑算子 |
|------|---------|---------|
| **读** | `Datasource` + `ReadTask` | `Read` 算子 |
| **写** | `Datasink` + `WriteTask` | `Write` 算子 |

这种对称性带来的好处：
- 新增一种格式只需同时实现 `Datasource` + `Datasink`
- 统一的错误处理和重试机制
- ETL 流程表达简洁：`read -> transform -> write`

### 2.11 典型使用流程

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

### 2.12 API 分类

| 类别 | 代表方法 | 对应逻辑算子 |
|------|---------|------------|
| 基础转换 | `map_batches()`, `map_rows()`, `flat_map()`, `filter()` | MapBatches, MapRows, FlatMap, Filter |
| 排序/混洗 | `sort()`, `random_shuffle()`, `repartition()` | Sort, RandomShuffle, Repartition |
| 拆分/合并 | `split()`, `union()`, `zip()`, `train_test_split()` | StreamingSplit, Union, Zip |
| 聚合 | `groupby()`, `sum()`, `mean()`, `max()`, `min()` | Aggregate |
| 消费 | `count()`, `take()`, `iter_rows()`, `iter_batches()` | Count, Limit |
| 写入 | `write_parquet()`, `write_csv()`, `write_json()` | Write |

### 2.13 explain() 调试工具

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

## 3. 分布式执行模型：中心化调度 + 分布式执行

**核心视角**：实现者/优化者视角 - 深入理解 UDF 序列化、任务分发、执行机制

### 3.1 关键架构澄清

**常见误解**：用户容易认为 Streaming Executor 会序列化分发到各节点执行。

**实际架构**：中心化调度 + 分布式执行

```
┌─────────────────────────────────────────────────────────────────────┐
│                           Driver 节点                                │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  Dataset API Layer                  Logical Plan Layer        │  │
│  │  dataset.py                        _internal/logical/         │  │
│  └───────────────────────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  Execution Plan Layer            Streaming Executor Layer     │  │
│  │  _internal/plan.py               _internal/execution/        │  │
│  │  ★ 完全在 Driver 本地执行，不跨节点                              │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              │                                        │
│                              │ 序列化分发（仅 UDF、Transformer）      │
│                              ▼                                        │
└──────────────────────────────┼────────────────────────────────────────┘
                               │
┌──────────────────────────────┼────────────────────────────────────────┐
│                        Worker 节点 1..N                               │
│  ┌───────────────────────────────┐  ┌───────────────────────────────┐│
│  │  Physical Operators           │  │  Physical Operators           ││
│  │  _internal/execution/operators│  │  _internal/execution/operators││
│  │  ★ 接收反序列化后的 UDF 执行    │  │  ★ 接收反序列化后的 UDF 执行    ││
│  └───────────────────────────────┘  └───────────────────────────────┘│
└───────────────────────────────────────────────────────────────────────┘
```

### 3.2 5 层架构的序列化边界

| 架构层 | 运行位置 | 是否序列化分发 | 序列化内容 |
|--------|---------|---------------|-----------|
| **Dataset API** | Driver 本地 | ❌ 否 | 用户脚本本身不分发 |
| **Logical Plan** | Driver 本地 | ❌ 否 | 算子 DAG 不分发 |
| **Execution Plan** | Driver 本地 | ❌ 否 | 执行计划不分发 |
| **Streaming Executor** | Driver 本地线程 | ❌ 否 | **关键误解澄清**：Executor 只在 Driver 运行！ |
| **Physical Operators** | Worker 分布式 | ✅ 部分 | 仅 `MapTransformer`（含用户 UDF）、`ObjectRef` |

**核心结论**：只有用户定义的 UDF 函数、包装 UDF 的 `MapTransformer`、以及数据引用 `ObjectRef` 会被序列化分发到 Worker 节点。

---

### 3.3 UDF 序列化完整链路

**5 步序列化流程**：

```
用户定义 UDF
    ↓
1. Dataset API 层包装为 MapTransformFn
   [dataset.py map_batches() 方法]
    ↓
2. 多个 MapTransformFn 封装为 MapTransformer
   [map_transformer.py MapTransformer 类]
    ↓
3. MapTransformer 放入 Ray 对象存储（ray.put）
   [task_pool_map_operator.py _add_bundled_input]
    ↓
4. 通过 cached_remote_fn 提交到 Ray 调度器
   [remote_fn.py cached_remote_fn()]
    ↓
5. Worker 反序列化 MapTransformer 并执行 _map_task
   [task_pool_map_operator.py _map_task]
```

---

### 3.4 核心机制代码详解

#### 3.4.1 cached_remote_fn：序列化缓存机制

**文件位置**：`python/ray/data/_internal/remote_fn.py` 第 8-44 行

```python
# 全局缓存：避免重复 cloudpickle 序列化相同函数
CACHED_FUNCTIONS = {}

def cached_remote_fn(fn: Any, **ray_remote_args) -> Any:
    """
    优化点 #1：缓存已序列化的 remote 函数
    
    关键设计：
    - 使用 (fn, args_hash) 作为缓存键
    - 相同 UDF + 相同 ray_remote_args 只序列化一次
    - 显著减少 cloudpickle 序列化开销（UDF 越大收益越高）
    """
    hashable_args = _make_hashable(ray_remote_args)
    args_hash = hash(hashable_args)
    
    cache_key = (fn, args_hash)
    if cache_key not in CACHED_FUNCTIONS:
        # 缓存未命中：执行 ray.remote 包装 + cloudpickle 序列化
        CACHED_FUNCTIONS[cache_key] = ray.remote(**ray_remote_args)(fn)
    
    return CACHED_FUNCTIONS[cache_key]
```

**研究重点**：
- 缓存命中率统计（当前无监控）
- `fn` 作为键的哈希开销
- 动态 `ray_remote_args` 导致的缓存失效

---

#### 3.4.2 _add_bundled_input：任务提交入口

**文件位置**：`python/ray/data/_internal/execution/operators/task_pool_map_operator.py` 第 82-113 行

```python
def _add_bundled_input(self, bundle: RefBundle):
    """
    优化点 #2：每个任务的序列化开销
    
    关键发现：
    - DataContext 和 TaskContext 每次任务都序列化！★
    - 这两个对象体积不小，且大多数字段不变
    - 可以考虑增量序列化或引用传递
    """
    ctx = TaskContext(
        task_idx=self._next_data_task_idx,
        output_num_blocks=self._output_num_blocks,
    )
    
    # 动态计算资源参数（可能导致缓存失效）
    dynamic_ray_remote_args = self._get_runtime_ray_remote_args(input_bundle=bundle)
    
    # 提交 remote 任务
    gen = self._map_task.options(**dynamic_ray_remote_args).remote(
        self._map_transformer_ref,  # ✅ ray.put 预放对象存储，仅传引用
        data_context,                # ❌ 每次任务都序列化 ★ 优化点
        ctx,                         # ❌ 每次任务都序列化 ★ 优化点
        *bundle.block_refs,          # ✅ ObjectRef，轻量引用
        **self.get_map_task_kwargs(),
    )
    self._submit_data_task(gen, bundle)
```

**研究重点**：
- `dynamic_ray_remote_args` 变化频率对缓存的影响
- `DataContext` 序列化字节数（可达几十 KB）
- 增量序列化可行性

---

#### 3.4.3 MapTransformer + fuse：算子融合优化

**文件位置**：`python/ray/data/_internal/execution/operators/map_transformer.py` 第 132-272 行

```python
class MapTransformer:
    """
    优化点 #3：可序列化变换链 + 算子融合（fuse）
    
    核心设计：
    - 封装多个 MapTransformFn（含用户 UDF）
    - 支持相邻 map 算子融合，减少 N 次序列化 + 调度开销
    - 自身可序列化，通过 ObjectRef 传递给 Worker
    """
    def __init__(self, transform_fns: List[MapTransformFn], *, init_fn=None):
        self._transform_fns = transform_fns    # ★ 链式变换函数列表（含用户UDF）- 序列化内容
        self._init_fn = init_fn                 # ★ Actor 模式初始化函数 - 序列化内容
        self._output_block_size_option_override = ...
        self._udf_time = 0                      # 运行时统计，不序列化
    
    def fuse(self, other: "MapTransformer") -> "MapTransformer":
        """
        相邻 map 算子融合
        
        收益：N 个连续 map → 1 个任务
        - 减少 N-1 次调度开销
        - 减少 N-1 次数据写回 Plasma
        - 减少 N-1 次序列化/反序列化
        """
        combined_transform_fns = self._transform_fns + other._transform_fns
        return MapTransformer(
            combined_transform_fns,
            init_fn=self._init_fn or other._init_fn,
        )
```

**研究重点**：
- fuse 识别率（多少比例的连续 map 能成功融合）
- 变换链序列化体积优化（压缩？）
- 融合失败的边界条件

---

### 3.5 Worker 端执行流程

**文件位置**：`python/ray/data/_internal/execution/operators/task_pool_map_operator.py` 第 115-180 行

```python
def _map_task(
    map_transformer: MapTransformer,  # Worker 反序列化后直接使用
    ctx: TaskContext,
    *blocks: Block,
) -> Union[Iterator[Block], List[Block]]:
    """
    Worker 端实际执行函数
    
    关键特性：
    1. 参数已通过 Ray 反序列化
    2. map_transformer 包含完整用户 UDF 链
    3. 执行结果写回 Plasma 对象存储
    """
    # 应用变换链（用户 UDF 依次执行）
    result = map_transformer.apply(blocks, ctx)
    
    # 返回结果（自动通过 Plasma 存储）
    return list(result)
```

---

### 3.6 完整序列化/反序列化时序图

```
用户脚本 (Driver)                          Ray 调度器                    Worker
    │                                          │                           │
    ├─ ds.map_batches(udf)                     │                           │
    │   → MapTransformFn 包装                  │                           │
    │   → MapTransformer 封装                  │                           │
    │                                          │                           │
    ├─ ray.put(MapTransformer) ────────────────→                           │
    │   → ObjectRef 返回                        │                           │
    │                                          │                           │
    ├─ cached_remote_fn(_map_task)             │                           │
    │   → 检查缓存                              │                           │
    │   → 未命中：cloudpickle 序列化            │                           │
    │   → ray.remote 注册                       │                           │
    │                                          │                           │
    ├─ .remote(transformer_ref, ctx, ...) ────→                           │
    │                                          │   调度分配资源            │
    │                                          ├──────────────────────────→│
    │                                          │   Worker 接收任务         │
    │                                          │   反序列化参数            │
    │                                          │   → _map_task 函数        │
    │                                          │   → MapTransformer        │
    │                                          │   → TaskContext           │
    │                                          │                           │
    │                                          │   执行 UDF 变换           │
    │                                          │   结果写入 Plasma         │
    │                                          │   返回 ObjectRef          │
    │                                          ←───────────────────────────┤
    │   Streaming Executor 收集结果             │                           │
    ←───（仍在 Driver 本地线程）               │                           │
```

---

### 3.7 5 个具体性能优化机会点

| 序号 | 优化方向 | 文件位置 | 改进思路 |
|------|---------|---------|---------|
| **1** | DataContext 增量序列化 | `task_pool_map_operator.py:100` | 大部分字段不变，只需序列化差异部分 |
| **2** | TaskContext 预序列化 | `task_pool_map_operator.py:90` | 任务间 ctx 结构相似，复用序列化结果 |
| **3** | MapTransformer 压缩 | `map_transformer.py` | 大 UDF 链压缩后再放入对象存储 |
| **4** | 动态参数缓存失效优化 | `remote_fn.py:30` | 按参数子集分层缓存，减少重新序列化 |
| **5** | 闭包捕获分析 | 用户 UDF | 检测并警告不必要的大对象闭包捕获 |

---

### 3.8 3 阶段深入研究路径建议

**阶段一：理解序列化机制**（当前）
```
研究目标：量化各环节开销
1. 测量 DataContext 序列化字节数和耗时
2. 统计 cached_remote_fn 缓存命中率
3. 分析 MapTransformer 序列化体积分布

工具：
- cloudpickle 调试：测量各对象序列化大小
- py-spy：采样序列化热点
- pytest-benchmark：对比不同 UDF 大小的序列化开销
```

**阶段二：理解调度分发**（下一步）
```
研究目标：理解任务如何从 Driver 到 Worker
关键文件：
- streaming_executor.py：调度循环
- resource_manager.py：资源分配
- task_pool_map_operator.py：任务状态机

研究点：
- 任务提交与执行的流水线延迟
- 背压（backpressure）机制对吞吐量的影响
- 数据本地性（locality）的实际命中率
```

**阶段三：定位验证优化点**（最终目标）
```
研究目标：实现并验证上述优化
1. DataContext 增量序列化原型
2. 缓存命中率提升方案
3. 算子融合边界条件扩展

输出：
- 性能对比数据（Before/After）
- Ray 社区 PR 提案
- 自定义优化版 Ray Data
```

---

## 4. Logical Plan（逻辑计划层）

**核心目录**: `python/ray/data/_internal/logical/interfaces/`

Logical Plan 是 Ray Data 的核心抽象，将用户的执行脚本转换为有向无环图（DAG）结构，支持拓扑排序、算子优化、并行调度。本章从代码架构出发，自底向上系统解析完整实现。

---

### 4.1 类继承体系与总体架构

Logical Plan 采用清晰的分层抽象设计，从基类到具体实现形成完整的继承链：

```
Plan (抽象基类)
  └── LogicalPlan (逻辑计划容器)

Operator (DAG 节点基类)
  └── LogicalOperator (逻辑算子扩展)
       ├── AbstractOneToOne (单输入算子基类)
       ├── AbstractAllToAll (全局混洗算子基类)
       ├── NAry (多输入算子基类)
       └── SourceOperator (数据源基类)
```

**核心类文件分布**：

| 类名 | 文件 | 核心职责 |
|------|------|---------|
| `Plan` | `interfaces/plan.py` | 抽象基类，定义 context 接口 |
| `LogicalPlan` | `interfaces/logical_plan.py` | DAG 容器，提供源算子分析 |
| `Operator` | `interfaces/operator.py` | DAG 节点基类，双向引用、遍历机制 |
| `LogicalOperator` | `interfaces/logical_operator.py` | 逻辑算子扩展，元数据推断接口 |
| `Rule / Optimizer` | `interfaces/optimizer.py` | 优化器抽象，规则依赖机制 |
| `Ruleset` | `ruleset.py` | 规则依赖图管理与拓扑排序应用 |

---

### 4.2 Operator 基类：DAG 节点核心机制

`Operator` 是整个 DAG 系统的基石，实现了 4 个核心机制：双向引用自动建立、后序遍历、不可变变换、字符串表示。

**代码位置**：`interfaces/operator.py` 第 5-101 行

#### 4.2.1 双向引用自动建立

```python
class Operator:
    def __init__(self, name: str, input_dependencies: List["Operator"]):
        self._name = name
        self._input_dependencies = input_dependencies   # 入边：我依赖哪些算子
        self._output_dependencies = []                  # 出边：哪些算子依赖我
        
        # ★ 魔法：自动为每个输入算子建立反向引用
        self._wire_output_deps(input_dependencies)

    def _wire_output_deps(self, input_dependencies: List["Operator"]):
        for x in input_dependencies:
            # 把自己注册到输入算子的输出依赖列表中
            x._output_dependencies.append(self)
```

**设计精妙之处**：
- 创建新算子时，**无需手动修改输入算子**
- 输入算子自动感知到有新的下游依赖
- 支持一个算子被多个下游算子依赖（分支场景）

**示例**：创建 `Join(left_dag, right_dag)` 时，left_dag 和 right_dag 的根节点会自动把 Join 加入自己的 `_output_dependencies` 列表。

---

#### 4.2.2 后序遍历：拓扑排序的实现

```python
def post_order_iter(self) -> Iterator["Operator"]:
    """
    ★ 后序遍历 = 拓扑排序
    
    遍历顺序：先递归访问所有输入依赖，再返回当前节点
    保证：任何算子在被访问时，其所有上游依赖都已访问完毕
    
    用途：
    - 执行调度：上游先执行，下游后执行
    - 优化器应用：从叶子开始优化，逐步向上
    """
    for op in self.input_dependencies:
        yield from op.post_order_iter()
    yield self
```

**遍历顺序示例**：
```
DAG: Join ← [Map1 ← Read1, Map2 ← Read2]

输出顺序: Read1 → Map1 → Read2 → Map2 → Join
```

**为什么选择后序？**
- ✅ 自然满足依赖顺序：上游先执行，下游后执行
- ✅ 优化器可以基于已优化的子节点做决策
- ✅ 天然支持多分支、多输入场景

---

#### 4.2.3 不可变变换：优化器的基础设施

这是整个优化器体系的核心支撑机制：

```python
def _apply_transform(self, transform: Callable) -> "Operator":
    """
    ★ 递归应用变换，保证不可变性
    
    执行流程：
    1. 递归变换所有输入依赖（后序执行）
    2. 如果任何输入发生了变化，创建当前节点的副本
    3. 对当前节点应用变换函数
    4. 返回新节点（或原节点，如果无变化）
    
    关键：原始 DAG 完全不被修改，变换产生新 DAG
    """
    # Step 1: 递归变换所有输入
    transformed_input_ops = [
        op._apply_transform(transform) 
        for op in self.input_dependencies
    ]
    
    # Step 2: 检查输入是否变化，决定是否创建副本
    if any(new is not old for new, old in zip(transformed_input_ops, self.input_dependencies)):
        target = copy.copy(self)                     # 创建浅拷贝
        target._input_dependencies = transformed_input_ops  # 更新输入
        target._wire_output_deps(transformed_input_ops)     # 重建反向引用
    else:
        target = self  # 输入无变化，复用原节点
    
    # Step 3: 对当前节点应用变换
    return transform(target)
```

**不可变设计的 3 大价值**：

| 价值 | 说明 | 场景 |
|------|------|------|
| **分支安全** | 一个 Dataset 派生出多个分支时，各分支的 DAG 修改互不影响 | `ds1 = ds.filter(a); ds2 = ds.filter(b)` |
| **优化安全** | 优化器产生新 DAG 不破坏原始计划，可对比优化前后 | 算子融合、谓词下推等优化 |
| **重放安全** | 出错时可完整重放执行历史，不受中间修改影响 | Debug、故障恢复 |

---

#### 4.2.4 DAG 字符串表示

```python
@property
def dag_str(self) -> str:
    """递归生成整个 DAG 的字符串表示"""
    if self.input_dependencies:
        out_str = ", ".join([x.dag_str for x in self.input_dependencies])
        out_str += " -> "
    else:
        out_str = ""
    out_str += f"{self.__class__.__name__}[{self._name}]"
    return out_str
```

用于调试输出和优化器的不动点检测（见 4.6 节）。

---

### 4.3 LogicalOperator 与算子分类体系

`LogicalOperator` 在 `Operator` 基础上扩展了逻辑算子特有的属性和方法。

**代码位置**：`interfaces/logical_operator.py` 第 10-102 行

```python
class LogicalOperator(Operator):
    def __init__(self, name: str, input_dependencies: List["LogicalOperator"], ...):
        super().__init__(name, input_dependencies)
        self._num_outputs: Optional[int] = num_outputs  # 预估输出 Block 数

    def estimated_num_outputs(self) -> Optional[int]:
        """预估输出 Block 数，用于并行度估计"""
        if self._num_outputs is not None:
            return self._num_outputs
        elif len(self._input_dependencies) == 1:
            # 单输入算子默认继承上游的输出数
            return self._input_dependencies[0].estimated_num_outputs()
        return None

    def infer_schema(self) -> Optional["Schema"]:
        """不执行计算，推断输出 schema"""
        return None

    def infer_metadata(self) -> "BlockMetadata":
        """推断输出元数据（行数、大小等）"""
        return BlockMetadata(None, None, None, None)

    def is_lineage_serializable(self) -> bool:
        """检查整个算子链是否可序列化"""
        return True
```

---

#### 4.3.1 五大算子分类

所有逻辑算子都继承自 `LogicalOperator`，按输入输出特征分为 5 大类：

| 类型 | 基类 | 输入数 | 代表算子 | 并行特性 |
|------|------|-------|---------|---------|
| **Source** | `SourceOperator` | 0 | Read, FromItems, InputData | ✅ 完全并行 |
| **OneToOne** | `AbstractOneToOne` | 1 | MapBatches, Filter, Project, Limit | ✅ 流水线并行 |
| **AllToAll** | `AbstractAllToAll` | 1 | Sort, Repartition, RandomShuffle | ⚠️ 必须等上游全部完成 |
| **NAry** | `NAry` | N | Join, Union, Zip | ⚠️ 必须等所有输入分支完成 |
| **Terminal** | 无专用基类 | 1 | Count, Write | 执行入口 |

---

#### 4.3.2 关键基类实现

**OneToOne 单输入算子基类**：`operators/one_to_one_operator.py`
```python
class AbstractOneToOne(LogicalOperator):
    def __init__(self, name: str, input_op: LogicalOperator, ...):
        # ★ 自动包装单输入为列表，简化子类实现
        super().__init__(name, [input_op] if input_op else [], num_outputs)

    @property
    def input_dependency(self) -> LogicalOperator:
        """便捷访问唯一的输入依赖"""
        return self._input_dependencies[0]
```

**NAry 多输入算子基类**：`operators/n_ary_operator.py`
```python
class NAry(LogicalOperator):
    def __init__(self, *input_ops: LogicalOperator, ...):
        # ★ 接收可变数量的输入算子
        super().__init__(self.__class__.__name__, list(input_ops), num_outputs)
```

---

### 4.4 DAG 构建机制：从 API 调用到算子图

用户的每一次 Dataset API 调用，本质上都是在向 DAG 中追加新节点。Ray Data 实现了 3 种标准构建范式。

---

#### 4.4.1 范式 1：入口算子构建（0 → 1）

**场景**：创建第一个算子，建立 DAG 的根节点

**代码位置**：`read_api.py` Read 算子创建流程
```python
# Step 1: 创建数据源算子（无输入依赖）
read_op = Read(
    datasource,
    datasource_or_legacy_reader,
    parallelism=parallelism,
    num_outputs=len(read_tasks),
)

# Step 2: 包装为 LogicalPlan + Dataset
logical_plan = LogicalPlan(read_op, context)
return Dataset(plan=execution_plan, logical_plan=logical_plan)
```

**特点**：
- 没有输入依赖，`input_dependencies = []`
- 是整个 DAG 的叶子节点（执行起点）
- 对应 `ray.data.read_*()` 和 `Dataset.from_*()` 系列 API

---

#### 4.4.2 范式 2：单输入链式追加（1 → 1）

**场景**：90% 的 Dataset 转换 API 都属于这一类

**代码位置**：`dataset.py` `map_batches` 第 772-789 行
```python
def map_batches(self, fn, ...) -> "Dataset":
    # Step 1: 复制执行计划（不可变）
    plan = self._plan.copy()
    
    # Step 2: 创建新算子，把当前 DAG 作为输入
    map_batches_op = MapBatches(
        self._logical_plan.dag,  # ★ 关键：当前 DAG 的根节点作为输入
        fn,
        batch_size=batch_size,
        ...
    )
    
    # Step 3: 新算子作为新 DAG 的根，返回新 Dataset
    logical_plan = LogicalPlan(map_batches_op, self.context)
    return Dataset(plan, logical_plan)
```

**链式构建效果**：
```
用户调用: ds = read_parquet().map_batches(fn).filter(pred)

DAG 构建过程:
  Read → MapBatches(Read) → Filter(MapBatches)
                              ↑ 新根节点
```

---

#### 4.4.3 范式 3：多输入算子合并（N → 1）

**场景**：Join、Union、Zip 等需要合并多个 DAG 的操作

**代码位置**：`dataset.py` `join` 第 2867-2881 行
```python
def join(self, ds: "Dataset", ...) -> "Dataset":
    plan = self._plan.copy()
    
    # ★ 同时接收两个独立 DAG 作为输入
    op = Join(
        left_input_op=self._logical_plan.dag,   # 左分支 DAG 根
        right_input_op=ds._logical_plan.dag,    # 右分支 DAG 根
        on=("id",),
        ...
    )
    
    # Join 算子成为合并后 DAG 的新根
    return Dataset(plan, LogicalPlan(op, self.context))
```

**多分支构建效果**：
```
用户调用: ds1.join(ds2)

DAG 结构:
          Join
         /    \
  ds1_root    ds2_root
     │            │
    ...          ...
```

---

### 4.5 LogicalPlan 容器：DAG 分析与管理

`LogicalPlan` 是 DAG 的容器类，提供对整个算子图的分析能力。

**代码位置**：`interfaces/logical_plan.py` 第 10-31 行

```python
class LogicalPlan(Plan):
    def __init__(self, dag: LogicalOperator, context: "DataContext"):
        super().__init__(context)
        self._dag = dag  # ★ 持有整个 DAG 的根节点

    @property
    def dag(self) -> LogicalOperator:
        """获取 DAG 根节点"""
        return self._dag

    def sources(self) -> List[LogicalOperator]:
        """
        ★ 递归找出所有源算子（无输入依赖的叶子节点）
        
        返回的每个源算子都是独立的并行执行起点
        """
        if not any(self._dag.input_dependencies):
            return [self._dag]  # 单源场景

        sources = []
        for op in self._dag.input_dependencies:
            # 对每个输入分支递归查找源算子
            sources.extend(LogicalPlan(op, self._context).sources())
        return sources
```

**`sources()` 的战略价值**：
- 识别所有可以并行启动的执行起点
- 多输入场景下（如 Join 两个数据源），天然并行执行
- 执行器基于源算子列表初始化并发任务数

---

### 4.6 优化器体系：Ruleset + Rule + Optimizer

Logical Plan 不仅仅是数据结构，更是整个优化流水线的基础。

**完整优化流水线**：
```
用户 DAG → 逻辑优化（3 条规则） → 物理转换 → 物理优化（4 条规则） → 执行
```

---

#### 4.6.1 Rule 抽象基类

```python
class Rule:
    def apply(self, plan: Plan) -> Plan:
        """应用规则到计划，返回优化后的新计划"""
        raise NotImplementedError

    @classmethod
    def dependencies(cls) -> List[Type["Rule"]]:
        """本规则依赖的其他规则，必须先执行"""
        return []

    @classmethod
    def dependents(cls) -> List[Type["Rule"]]:
        """依赖本规则的其他规则，必须后执行"""
        return []
```

---

#### 4.6.2 Ruleset：依赖图管理与拓扑排序

```python
class Ruleset:
    def __init__(self, rules: Optional[List[Type[Rule]]] = None):
        self._rules = list(rules) if rules else []

    def __iter__(self) -> Iterator[Type[Rule]]:
        """
        ★ 按拓扑顺序产出规则
        
        1. 构建规则依赖图（边：被依赖 → 依赖者）
        2. 拓扑排序产出执行顺序
        3. 保证依赖先执行，被依赖后执行
        """
        roots = self._build_graph()
        queue = collections.deque(roots)
        while queue:
            node = queue.popleft()
            yield node.rule
            queue.extend(node.dependents)
```

---

#### 4.6.3 Optimizer：迭代优化到不动点

```python
class LogicalOptimizer(Optimizer):
    @property
    def rules(self) -> List[Rule]:
        return [rule_cls() for rule_cls in get_logical_ruleset()]

    def optimize(self, plan: Plan) -> Plan:
        """
        ★ 迭代应用规则直到 DAG 不再变化（不动点）
        
        关键：用 dag_str 比较 DAG 是否变化
        可能需要多轮：规则 A 产生的变化激活规则 B
        """
        previous_plan = plan
        while True:
            for rule in self.rules:
                plan = rule.apply(plan)
            # 检测不动点：DAG 字符串不再变化
            if plan.dag.dag_str == previous_plan.dag.dag_str:
                break
            previous_plan = plan
        return plan
```

---

#### 4.6.4 当前实现的 7 条优化规则

**逻辑优化阶段**（LogicalOptimizer）：
1. `InheritBatchFormatRule` - 批量格式继承
2. `LimitPushdownRule` - Limit 算子下推（减少数据处理）
3. `ProjectionPushdown` - 投影下推（列裁剪）

**物理优化阶段**（PhysicalOptimizer）：
4. `InheritTargetMaxBlockSizeRule` - Block 大小继承
5. `SetReadParallelismRule` - 读取并行度设置
6. `FuseOperators` - 相邻 Map 算子融合
7. `ConfigureMapTaskMemoryUsingOutputSize` - Map 任务内存配置

---

### 4.7 核心设计模式深度解析

Logical Plan 体系体现了多个精心设计的架构决策：

---

#### 4.7.1 不可变设计：为什么每个 API 都返回新对象？

```python
# 每个转换 API 的第一步都是 copy
ds1 = ds.map(fn1)  # ds._plan.copy()
ds2 = ds.map(fn2)  # ds._plan.copy() 再次复制，互不影响
```

**架构权衡**：

| 优点 | 缺点 |
|------|------|
| ✅ 分支天然安全 | ❌ 额外的对象创建开销 |
| ✅ 优化器安全（无副作用） | ❌ 大型 DAG 复制开销明显 |
| ✅ 可重放、可对比 | ❌ 内存占用略高 |

**结论**：对于 ML 数据处理场景，计算开销远大于对象创建开销，不可变设计的收益远大于成本。

---

#### 4.7.2 双向引用：为什么需要 `_output_dependencies`？

```
单向引用只有：算子 → 输入依赖
双向引用增加：算子 → 输出依赖
```

**使用场景**：
1. **分支检测**：一个算子有多个输出依赖 → 检测分支场景
2. **公共子图识别**：多个下游共享同一个上游 → 可考虑计算复用
3. **DAG 可视化**：绘制完整的图结构需要双向引用

**当前局限性**：双向引用机制已实现，但 Ray Data 当前并未充分利用其潜力（如公共子图消除），这是优化方向。

---

#### 4.7.3 后序遍历的无处不在

| 组件 | 后序应用 |
|------|---------|
| 执行调度 | 上游先执行，下游后执行 |
| 优化器 | 叶子先优化，优化结果向上传递 |
| Schema 推断 | 基于上游输出推断当前输出 |
| 并行度估计 | 基于上游输出 Block 数估计 |

---

### 4.8 研究方向与优化机会

基于代码分析，以下是高价值的优化方向：

| 方向 | 问题描述 | 技术难度 | 收益潜力 |
|------|---------|---------|---------|
| **公共子图消除** | `ds1 = ds.map(a); ds2 = ds.map(b)` 目前 ds 计算两次，可复用 | ★★★☆☆ | ★★★★★ |
| **DAG 增量序列化** | 大型 DAG 每次完整 pickle 开销大，可增量序列化 | ★★★★☆ | ★★★★☆ |
| **算子融合扩展** | 目前只有连续 Map 能融合，扩展到 Filter → Map 等组合 | ★★☆☆☆ | ★★★★☆ |
| **动态拓扑调整** | 执行中发现数据倾斜，动态插入 repartition 算子 | ★★★★★ | ★★★★☆ |
| **双向引用利用** | 利用 `_output_dependencies` 做更智能的优化决策 | ★★☆☆☆ | ★★★☆☆ |

---

## 5. Execution Plan（执行计划层）

**文件位置**: `python/ray/data/_internal/plan.py`

`ExecutionPlan` 是连接逻辑计划和物理执行的桥梁。

### 5.1 核心职责

1. **逻辑优化**：算子融合（Fusion）、谓词下推、投影下推
2. **物理计划生成**：将 Logical Operator 转换为 Physical Operator
3. **执行调度**：创建 StreamingExecutor 并调度执行

### 5.2 ExecutionPlan 核心方法

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

## 6. Streaming Executor（流式执行层）

**文件位置**: `python/ray/data/_internal/execution/streaming_executor.py`

`StreamingExecutor` 是 Ray Data 的执行核心，采用**独立线程 + 事件循环**的调度模型。

### 6.1 调度模型

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

### 6.2 核心执行机制

| 机制 | 说明 | 关键函数 |
|------|------|---------|
| **拓扑构建** | 构建算子执行拓扑 | `build_streaming_topology()` |
| **资源管理** | 管理 CPU/GPU/内存预算 | `ResourceManager` |
| **调度循环** | 事件驱动的任务调度 | `ray.wait()`, `process_completed_tasks()` |
| **算子选择** | 根据资源和背压选择算子 | `select_operator_to_run()` |
| **背压控制** | 限制算子队列长度，防止 OOM | `target_max_block_overflow` |

### 6.3 背压机制

- 限制每个算子的队列长度（默认 200 个 blocks）
- 防止上游生产速度远超下游消费导致 OOM
- 可配置：`DataContext.target_max_block_overflow`

---

## 7. Physical Operators（物理算子层）

**文件位置**: `python/ray/data/_internal/execution/operators/`

### 7.1 算子分类

| 算子类型 | 代表类 | 用途 |
|---------|--------|------|
| **输入源** | `InputDataBuffer` | 缓存输入数据 blocks |
| **Map类** | `MapOperator` | 逐块转换，无状态 |
| **Actor Map类** | `ActorPoolMapOperator` | 使用 Actor 池执行有状态 UDF |
| **AllToAll类** | `AllToAllOperator` | shuffle、sort、repartition |
| **N-Ary类** | `ZipOperator`, `UnionOperator` | 多输入合并 |
| **Output** | `OutputOperator` | 输出结果 |

### 7.2 ActorPoolMapOperator

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

## 8. Block 系统

### 8.1 Block 抽象

**文件位置**: `python/ray/data/block.py`

Ray Data 的数据以 `Block` 为单位存储和传输，每个 Block 是：
- 一个 Arrow Table、Pandas DataFrame 或 NumPy 数组
- 大小通常在 128MB ~ 512MB 之间（可配置）
- 是分布式调度的最小单位

### 8.2 BlockAccessor 接口

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

### 8.3 Block 实现类

| 实现类 | 说明 |
|--------|------|
| `ArrowBlockAccessor` | Arrow Table 格式（默认） |
| `PandasBlockAccessor` | Pandas DataFrame 格式 |
| `TensorBlockAccessor` | 张量数据支持 |

### 8.4 RefBundle

**文件位置**: `python/ray/data/_internal/execution/interfaces/ref_bundle.py`

`RefBundle` 是 Block 在执行流中的载体，包含：
- `blocks: List[Tuple[ObjectRef[Block], BlockMetadata]]`
- 元数据：行数、大小、schema 等

---

## 9. DataContext（全局配置中心）

**文件位置**: `python/ray/data/context.py`

`DataContext` 是 Ray Data 的全局配置管理器，控制着执行的方方面面。

### 9.1 常用配置

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

## 10. 典型执行流程

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

### 10.1 执行时序

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

## 11. 核心特性总结

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

## 12. 测试用例参考

### 12.1 基础功能测试

| 测试文件 | 大小 | 测试内容 |
|---------|------|---------|
| `test_consumption.py` | 72KB | 数据消费方式测试：`iter_rows()`, `iter_batches()`, `to_tf()`, `to_torch()` |

### 12.2 算子测试

| 测试文件 | 大小 | 测试内容 |
|---------|------|---------|
| `test_actor_pool_map_operator.py` | 28KB | Actor 池的创建、扩缩容、错误处理，有状态 UDF 的执行 |

### 12.3 执行器测试

| 测试文件 | 大小 | 测试内容 |
|---------|------|---------|
| `test_backpressure_e2e.py` | 11KB | 背压机制正确工作、内存使用控制 |

### 12.4 资源管理测试

| 测试文件 | 大小 | 测试内容 |
|---------|------|---------|
| `test_autoscaler.py` | 11KB | Actor 自动扩缩容逻辑、资源利用率优化 |
