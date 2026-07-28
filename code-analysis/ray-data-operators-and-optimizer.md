# Ray Data 算子体系与优化规则分析

本文档聚焦 Ray Data 的**逻辑算子类型体系**和**逻辑/物理优化规则**的深入机制，是 [ray-data.md](ray-data.md)（总体架构，含规则清单见 4.6.4/5.3 节）的专题深挖补充。核心源码位置：

- 规则实现：`python/ray/data/_internal/logical/rules/`
- 算子定义：`python/ray/data/_internal/logical/operators/`
- 框架接口：`python/ray/data/_internal/logical/interfaces/`

---

## 1. 优化器框架要点

三件套（全部代码不过百行，经典 Rule-based 设计）：

- **`Rule`**（[interfaces/optimizer.py](../python/ray/data/_internal/logical/interfaces/optimizer.py)）：`apply(plan) -> plan`；`dependencies()`/`dependents()` 声明规则间顺序。
- **`Optimizer.optimize()`**：**不动点循环**——反复应用全部规则直到 `plan.dag.dag_str` 不再变化。推论：算子名必须稳定（`dag_str` 是收敛判据）。
- **`Ruleset`**（[ruleset.py](../python/ray/data/_internal/logical/ruleset.py)）：拓扑排序 + 循环依赖检测。

两阶段管线（[optimizers.py](../python/ray/data/_internal/logical/optimizers.py) `get_execution_plan()`）：

```
逻辑优化（InheritBatchFormat → LimitPushdown → ProjectionPushdown）
  → Planner 翻译为物理算子
  → 物理优化（InheritTargetMaxBlockSize → SetReadParallelism → FuseOperators → ConfigureMapTaskMemory）
```

顺序有讲究：融合依赖前面规则已确定的块大小和读并行度。

---

## 2. 规则机制精要（清单见 ray-data.md，此处只记深挖所得）

| 规则 | 清单之外的要点 |
|---|---|
| **LimitPushdownRule** | 三种动作：相邻 Limit 融合取 min；保守穿过**不改行数**的 OneToOne（额外排除 MapBatches——行数不变但可能 rebatch）；Union 时每分支插本地 Limit + 保留全局 Limit；给 Read/Map 挂 `per_block_limit` 减 I/O |
| **ProjectionPushdown** | 经 `LogicalOperatorSupportsProjectionPushdown` mixin 挂载（目前仅 Parquet Read 实现）；相邻 Project 用 `_ProjectSpec`（cols+rename+exprs）合并，做重命名双射/列子集校验；**表达式不下推进 Read** |
| **SetReadParallelismRule** | 遍历实为 FIFO 队列 **BFS**（非 DFS）；`_autodetect_parallelism` 综合数据大小/目标块/集群 CPU；块数不足设 `additional_split_factor`；块数超 CPU 4 倍且 ≥5000 告警 |
| **InheritTargetMaxBlockSizeRule** | 递归 DFS 携带参数向上传播，遇另一个 override 换值 |
| **ConfigureMapTaskMemoryUsingOutputSize** | 遍历直接用 `post_order_iter()` 原语；包装 `ray_remote_args_fn` 动态注入 `memory=average_bytes_per_output`；放置组调度下跳过（否则任务无法调度） |
| **InheritBatchFormatRule** | AllToAll 算子沿单输入链向上找最近 MapBatches 继承 `batch_format`——水闸不记得格式，要向上游借 |

遍历原语两处：[operator.py](../python/ray/data/_internal/logical/interfaces/operator.py) `post_order_iter()`（递归生成器，物理规则用）与 `_apply_transform()`（函数式后序重写返回新节点，逻辑规则用）。物理规则多为命令式直接改 `_input_dependencies` 指针。

---

## 3. 算子融合（FuseOperators）的精确理解

[operator_fusion.py](../python/ray/data/_internal/logical/rules/operator_fusion.py) 两遍 DFS：先融 `Map→Map`，再融 `Map→AllToAll`（仅 RandomShuffle / Repartition(shuffle=True)）。

### 3.1 需求本质

`Read → MapBatches(f) → MapBatches(g)` 若各自独立成 task，中间结果要物化进 object store（序列化 + IPC）。融合后一个 Ray task 内顺序执行 `g(f(read(x)))`。Map→AllToAll 融合则把 map transformer 经 `TaskContext.upstream_map_transformer` 传给 shuffle，map 输出直接灌进 shuffle 缓冲区。

### 3.2 遍历骨架的三个误读修正

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

1. **`len(upstream_ops) == 1` 是防御性摆设，不是"线性链判据"**。[planner.py](../python/ray/data/_internal/planner/planner.py) `_plan_recursively` **无 memoization**——共享逻辑子图被规划成多份物理拷贝，**物理 DAG 实为树**；MapOperator 构造上恒单输入。该条件只对 Union/InputDataBuffer 起廉价短路作用，真正门槛是两个 `isinstance` + `_can_fuse`。
2. **非"就地融合"，是"新建 + 重绑定"**。`_get_fused_map_operator` 每轮 `MapOperator.create(...)` 造全新算子（transformer 合并、名 `"up->down"`、输入接到上游的上游）；对既有图的唯一写操作是循环退出后的 `dag._input_dependencies = [...]`。
3. **递归职责 = 越过任意边界继续往上游找**，分叉（Union）只是边界的一种。`Map → Shuffle → Map → Map` 中靠递归跳过 Shuffle 才融合上游链。

### 3.3 语义门槛全在 `_can_fuse`（while 只是骨架）

- 上游必须 TaskPool（Task→Task / Task→Actor；Actor 池有生命周期不能并）；
- Task→Task 池大小相等；Task→Actor 时 task size 须等于 actor `max_size`（融合后一个 task 只有一份资源配置）；
- `ray_remote_args` 规范化后一致（`num_cpus` 缺省 1、`num_gpus` 缺省 0；`scheduling_strategy` 可继承）；任一方有 `_ray_remote_args_fn` 不融；
- `target_max_block_size` 不冲突（双方 override 时必须相等）；
- **行数保护**：上游 `can_modify_num_rows()` 且下游有 `min_rows_per_bundled_input` → 拒绝。保护并行度（大 batch 压缩 Read 并发）与 batching 语义（Filter 后凑不齐 batch）；
- 上游 `additional_split_factor > 1` 不融（split 破坏 1:1 数据流）。

**不对称性实例**：`filter(g).map_batches(f, batch_size=N)` 可融合；`map_batches(f, batch_size=N).filter(g)` 不可融合。该不对称性只在 `_can_fuse_map_ops`（约 492-503 行）可见，while 循环完全不体现。

---

## 4. 逻辑算子类型体系

### 4.1 继承树

```
Operator（interfaces/operator.py — 图结构：双向依赖边、post_order_iter、_apply_transform）
└── LogicalOperator（interfaces/logical_operator.py — 计划语义）
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

### 4.2 两个正交维度

- **维度 A：图形状（arity）**——零/单/多输入，决定遍历改写逻辑；
- **维度 B：物理映射目标**——`AbstractMap` → 物理 `MapOperator`（流式逐 bundle）；`AbstractAllToAll` → 物理 `AllToAllOperator`（barrier 全量交换）。

**交叉的惊喜点**：`Read` 是 Source 却继承 `AbstractMap`——读数据物理上就是一批 map 任务，故 `Read→MapBatches` 能融合；`Write` 同理，`MapBatches→Write` 也能融合，写出不产生中间物化。

### 4.3 设计精髓：语义属性外化为算子谓词，规则是谓词组合器

| 谓词/接口 | 消费方 |
|---|---|
| `can_modify_num_rows()` | LimitPushdown（能否穿过）、Fusion（行数保护） |
| `infer_schema()` / `infer_metadata()` | Limit 直接算 `min(input_rows, limit)`；SetReadParallelism 读 `size_bytes` |
| `LogicalOperatorSupportsProjectionPushdown` mixin | ProjectionPushdown 挂载点 |
| `_compute` / `_ray_remote_args` / `_min_rows_per_bundled_input` | Fusion 兼容性判据 |

新增算子的主要工作 = 正确实现这几个谓词，各规则自动获得正确行为。

### 4.4 `can_modify_num_rows()` 实测表（逐个核对源码）

| 值 | 算子 | 理由 |
|---|---|---|
| `False` | Project / MapRows / MapBatches / Download / StreamingRepartition | 行保持（MapBatches 一批进一批出） |
| `True` | Filter / FlatMap / **Read** / Limit | 过滤展开；Read 注释明确：reader 把输入展开成多行 |

---

## 5. OneToOne vs AllToAll：系统的分水岭

### 5.1 判据

Dataset = 分布在集群各节点上的 Block 集合。分类只看一个问题：

> **为了算出某个输出块，需不需要看到别的输入块里的行？**

- 不需要 → OneToOne：块不动，行在块内变；流式，来一个处理一个。
- 需要 → AllToAll：行跨块搬家（序列化→网络→object store→barrier 等待）。

### 5.2 两个家族由浅入深

**OneToOne**（便宜、可流式）：
Project（只动列，唯一能推进 Read）→ MapRows（逐行 UDF）→ MapBatches（批 UDF + `batch_size` 保护）→ Filter/FlatMap（行数变，打标记）→ Limit（单入单出但需全局计数协调）。

**AllToAll**（贵、barrier）：
Repartition（纯数据移动零语义；shuffle=False 的 split 模式便宜、shuffle=True 才允许与上游 Map 融合——fusion 检查 `down_logical_op._shuffle` 的原因）→ RandomShuffle（随机定去向）→ Sort（采样定边界 + 范围分发 + 局部排序，即 total order partitioning；进度条名 `SORT_SAMPLE_SUB_PROGRESS_BAR_NAME` 可见两阶段）→ Aggregate（hash(key) 分发 + map 侧预聚合 + reduce 合并）。

### 5.3 所有规则都画在这条分界线上

```
Read → MapBatches → Filter → │ Sort │ → MapBatches → Write
  ──── 流式段（融合/并发的天下）──── 水闸 ──── 流式段 ────
```

- Map↔Map 融合 = 压缩流式段；Map→AllToAll 融合 = 省掉水闸前最后一次物化；
- Limit 下推到水闸为止（Sort/Aggregate 改行数，穿过即错）；
- 对应 MapReduce/Hive：OneToOne ≈ map 侧操作链，AllToAll ≈ shuffle+reduce；Map→AllToAll 融合 ≈ Hive 把 map 侧操作编译进同一 mapper。

### 5.4 记忆锚点

> **OneToOne：块不动，行在块内变。AllToAll：行动，跨块搬家。**
> 看任何流水线先找 AllToAll——它把计划切成流式段，段间是物化与等待的边界。

---

## 6. 与 Hive 的对比

### 6.1 算子粒度：子句级，非函数级

Ray 算子对应 Hive 的**子句级关系代数节点**，不是 sum/max 这种函数：

| SQL 子句 | Hive 算子 | Ray 算子 |
|---|---|---|
| FROM / WHERE / SELECT 列 / JOIN / GROUP BY / ORDER BY / LIMIT / UNION ALL | TableScan / Filter / Select / Join / GroupBy / ReduceSink / Limit / Union | Read / Filter / Project / Join / Aggregate / Sort / Limit / Union |

**sum/max 在两边都不是算子**：Hive 中是 GroupByOperator 内的 UDAF descriptor；Ray 中是 `Aggregate._aggs: List[AggregateFn]`。执行模式同构：`AggregateFn`（init/accumulate/merge/finalize，[aggregate.py](../python/ray/data/aggregate.py)）≈ Hive UDAF 两阶段聚合 ≈ MapReduce combiner。标量表达式同理：Hive 的 ExprNode 树在 Select 算子内，Ray 的 `Expr` 树在 `Project.exprs` 内——**函数永远比算子低一层**。

### 6.2 Ray 独有（SQL 无对应物）

- `MapBatches`/`MapRows`：UDF 逃逸口，最接近 Hive 的 `TRANSFORM ... USING`；
- `FlatMap` ≈ `LATERAL VIEW explode`；
- `Repartition`/`RandomShuffle`/`StreamingRepartition`：物理数据重分布，SQL 里只是 hint（`DISTRIBUTE BY`、BROADCAST hint）——**Ray 把物理调优语义提升为一等逻辑算子，其"逻辑计划"并不纯逻辑**。

### 6.3 优化器差距

Hive = Calcite CBO + 上百条规则（join 重排序、分区裁剪、代价模型）；Ray = 7 条启发式规则，无代价模型、无 join 重排序（Join 算子不参与任何下推规则）。但思想同构：projection/predicate pushdown ↔ ProjectionPushdown/LimitPushdown；map 侧操作链合并进同一 mapper ↔ FuseOperators。**Ray 相当于实现了 Hive 优化器中最不依赖统计信息、收益最确定的那一小撮规则**。

---

## 8. 优化器的边界：哪些优化不做

Ray Data 优化器**刻意保守**，很多数据库经典优化它不实现，责任留给用户。

### 8.1 无谓词下推：`sort().filter()` 不会重排为 `filter().sort()`

逻辑规则集只有 3 条（[optimizers.py:23-30](../python/ray/data/_internal/logical/optimizers.py#L23)），**没有 FilterPushdown / PredicatePushdown**。所以：

```python
ds.sort("age").filter(expr=col("age") > 18)
# DAG 原样保留 Read → Sort → Filter（先全量排序 shuffle，再过滤）
# 不会被优化成 Read → Filter → Sort
```

这个重排在关系代数上完全合法（filter 与 sort 可交换，先 filter 减少 sort 数据量），Spark Catalyst / Hive Calcite 会自动做，但 **Ray Data 不做**。

**佐证**：唯一的下推规则 `LimitPushdownRule` 文档明文规定"遇到 Sort/Shuffle/Aggregate/Read 就停止下推"（[limit_pushdown.py:23-24](../python/ray/data/_internal/logical/rules/limit_pushdown.py#L23)）。连更简单的 Limit 下推都不敢穿过 AllToAll，更没实现 Filter 穿 Sort。原因：`Sort` 继承 `AbstractAllToAll`（[all_to_all_operator.py:147](../python/ray/data/_internal/logical/operators/all_to_all_operator.py#L147)），根本不在 `AbstractOneToOne.can_modify_num_rows()` 判定体系内。

### 8.2 实践准则

在 Ray Data 里**手动把 filter/select 放在 sort/groupby/shuffle 之前**——优化器不会替你搬。这是与 Spark/Hive 使用习惯的重要差异。

---

## 9. 优化后 DAG 的测试验证

Ray Data 用一套标准模式验证 `LogicalOptimizer` 优化结果，核心工具是 `LogicalOperator.dag_str`（与 `Optimizer.optimize()` 判不动点收敛用的是同一字符串表示）。

### 9.1 双重断言模式

`_check_valid_plan_and_result`（[test_execution_optimizer_limit_pushdown.py:14-25](../python/ray/data/tests/test_execution_optimizer_limit_pushdown.py#L14)）：

```python
assert ds.take_all() == expected_result                      # ① 语义正确性
assert ds._plan._logical_plan.dag.dag_str == expected_plan   # ② DAG 结构正确性
```

既验证优化没改结果，又验证优化确实改了 DAG 结构。例：

```python
ray.data.range(100).limit(50).limit(80).limit(5).limit(20)
  → dag_str == "Read[ReadRange] -> Limit[limit=5]"   # 四个 Limit 融合成 min=5
```

### 9.2 测试文件分工

| 测试文件 | 验证对象 | 断言方式 |
|---|---|---|
| `test_execution_optimizer_limit_pushdown.py` | LimitPushdown 后逻辑 DAG | `dag.dag_str == expected` |
| `test_operator_fusion.py` | 融合后物理 DAG | 断言算子 `.name`（如 `"ReadParquet->MapBatches(<lambda>)"`）+ `.input_dependencies` |
| `test_projection_fusion.py` | ProjectionPushdown | dag_str / name 断言 |
| `test_ruleset.py` | 规则**排序机制**（非 DAG 结果） | `list(ruleset) == [...]`、循环检测 |

### 9.3 "下推遇 Sort 停止"的可执行证据

[test_limit_pushdown_union_with_sort:296-311](../python/ray/data/tests/test_execution_optimizer_limit_pushdown.py#L296)：

```python
ds = ds1.union(ds2).sort("id").limit(5)
expected_plan = "... Union[Union] -> Sort[Sort] -> Limit[limit=5]"
# docstring: "Limit after Union + Sort: limit must NOT push through the Sort."
```

Limit 仍在 Sort 下游——官方用断言明确"有意不让下推穿过 Sort"，佐证第 8 节的保守边界结论。

---

## 10. 关键源码索引

| 内容 | 位置 |
|---|---|
| Rule/Optimizer 基类、不动点循环 | `python/ray/data/_internal/logical/interfaces/optimizer.py` |
| Ruleset 拓扑排序 | `python/ray/data/_internal/logical/ruleset.py` |
| 两套规则注册、两阶段管线 | `python/ray/data/_internal/logical/optimizers.py` |
| 融合规则（核心） | `python/ray/data/_internal/logical/rules/operator_fusion.py` |
| Limit/Projection 下推 | `.../rules/limit_pushdown.py`、`.../rules/projection_pushdown.py` |
| 遍历原语 | `python/ray/data/_internal/logical/interfaces/operator.py` |
| 算子继承树 | `python/ray/data/_internal/logical/operators/` |
| Planner（无 memo，物理 DAG 为树） | `python/ray/data/_internal/planner/planner.py` |
| AggregateFn 四段式 | `python/ray/data/aggregate.py` |
