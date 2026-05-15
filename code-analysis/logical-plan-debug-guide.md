# Ray Data Logical Plan 调试与分析指南

本指南提供了一个完整的工作流程，用于调试和分析 Ray Data 的 Logical Plan 构建过程。

---

## 步骤 1：创建调试脚本

创建文件 `code-analysis/debug_logical_plan.py`：

```python
"""
Ray Data Logical Plan 调试脚本
用途：分析 API 调用如何转化为 Logical Operator DAG
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))

import ray
from ray.data import Dataset
from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalPlan,
    Operator
)


def print_operator_tree(op: Operator, indent: int = 0, visited: set = None):
    """
    递归打印 Operator DAG 树结构

    Args:
        op: 当前算子
        indent: 缩进级别
        visited: 已访问算子集合（避免循环）
    """
    if visited is None:
        visited = set()

    if id(op) in visited:
        print(" " * indent + f"<-- {op.name} (已访问)")
        return

    visited.add(id(op))

    prefix = " " * indent
    print(f"{prefix}{op.__class__.__name__}[{op.name}]")
    print(f"{prefix}  ├── 输入依赖数: {len(op.input_dependencies)}")
    print(f"{prefix}  ├── 输出依赖数: {len(op.output_dependencies)}")

    if hasattr(op, "infer_schema"):
        try:
            schema = op.infer_schema()
            print(f"{prefix}  ├── Schema: {schema}")
        except Exception:
            print(f"{prefix}  ├── Schema: (无法推断)")

    if hasattr(op, "estimated_num_outputs"):
        try:
            num_outputs = op.estimated_num_outputs()
            print(f"{prefix}  ├── 预估输出 Blocks: {num_outputs}")
        except Exception:
            pass

    if op.input_dependencies:
        print(f"{prefix}  └── 输入依赖列表:")
        for input_op in op.input_dependencies:
            print_operator_tree(input_op, indent + 4, visited)


def analyze_logical_plan(dataset: Dataset, description: str = ""):
    """
    分析 Dataset 的 Logical Plan 结构

    Args:
        dataset: Ray Dataset 对象
        description: 场景描述
    """
    print("\n" + "=" * 80)
    if description:
        print(f"{description}")
        print("=" * 80)

    logical_plan = dataset._logical_plan
    root_op = logical_plan.dag

    print(f"\n根算子类型: {root_op.__class__.__name__}")
    print(f"\nDAG 树结构:")
    print_operator_tree(root_op)

    print(f"\n源算子列表（并行执行起点）:")
    sources = logical_plan.sources()
    for i, source in enumerate(sources):
        print(f"  {i + 1}. {source.__class__.__name__}[{source.name}]")

    print(f"\n后序遍历顺序（拓扑排序）:")
    for i, op in enumerate(root_op.post_order_iter()):
        print(f"  {i + 1:2d}. {op.__class__.__name__}[{op.name}]")

    return logical_plan


def main():
    """主函数：演示各种场景的 Logical Plan 构建"""

    ray.init(num_cpus=2, include_dashboard=False)
    print("Ray 初始化完成（本地模式，不实际执行任务）")

    # =========================================================================
    # 场景 1：简单链式调用
    # =========================================================================
    print("\n" + "=" * 80)
    print("场景 1：简单链式调用 - range -> map -> filter")
    print("=" * 80)

    ds1 = ray.data.range(1000)
    ds1 = ds1.map(lambda x: {"value": x["id"] * 2})
    ds1 = ds1.filter(lambda x: x["value"] > 100)

    analyze_logical_plan(ds1, "简单链式 DAG")

    # =========================================================================
    # 场景 2：多输入 Join 场景
    # =========================================================================
    print("\n" + "=" * 80)
    print("场景 2：多输入 Join 场景 - 两个独立分支合并")
    print("=" * 80)

    left_ds = ray.data.range(100).map(lambda x: {"id": x["id"], "name": f"user_{x['id']}"})
    right_ds = ray.data.range(100).map(lambda x: {"id": x["id"], "score": x["id"] * 10})

    joined_ds = left_ds.join(
        right_ds,
        on=("id",),       # tuple/list 类型
        join_type="inner",
        num_partitions=4  # 必须参数
    )

    analyze_logical_plan(joined_ds, "多输入 Join DAG")

    # =========================================================================
    # 场景 3：多阶段转换 + shuffle
    # =========================================================================
    print("\n" + "=" * 80)
    print("场景 3：多阶段转换 + shuffle 算子")
    print("=" * 80)

    ds3 = ray.data.from_items([{"id": i, "value": i * 1.5} for i in range(100)])
    ds3 = ds3.map_batches(lambda batch: batch)
    ds3 = ds3.random_shuffle()
    ds3 = ds3.repartition(5)

    analyze_logical_plan(ds3, "包含 Shuffle 算子的复杂 DAG")

    # =========================================================================
    # 场景 4：分支复用
    # =========================================================================
    print("\n" + "=" * 80)
    print("场景 4：分支复用场景 - 一个父节点多个子节点")
    print("=" * 80)

    base_ds = ray.data.range(100)
    branch1 = base_ds.map(lambda x: {"id": x["id"], "type": "A"})
    branch2 = base_ds.map(lambda x: {"id": x["id"], "type": "B"})
    union_ds = branch1.union(branch2)

    analyze_logical_plan(union_ds, "分支复用 + Union DAG")

    # =========================================================================
    # 对比官方 explain() 输出
    # =========================================================================
    print("\n" + "=" * 80)
    print("官方 explain() 输出对比：")
    print("=" * 80)
    print(ds1.explain())

    print("\n分析完成！")


if __name__ == "__main__":
    try:
        main()
    finally:
        ray.shutdown()
```

---

## 步骤 2：运行调试脚本

```bash
cd /home/luming/workspace/repos/ray-project/ray

# 确保使用正确的 Python 环境
# 激活你的 conda 环境或 venv

# 运行调试脚本
python code-analysis/debug_logical_plan.py
```

**预期输出**：
1. 🌳 完整的 DAG 树结构（算子类型、名称、输入输出数量）
2. 📋 所有源算子列表（无输入依赖的叶子节点）
3. 🔗 拓扑排序顺序（后序遍历）
4. 🔍 与官方 `explain()` 输出对比

---

## 🔧 步骤 3：断点调试高级技巧

### 3.1 使用 PDB 断点调试算子创建

在你想要断点的位置插入：

```python
import pdb; pdb.set_trace()
```

**推荐断点位置**：

| 文件 | 行号 | 断点位置 | 用途 |
|------|------|---------|------|
| `python/ray/data/dataset.py` | ~780 | `map_batches_op = MapBatches(...)` | 观察 Map 算子创建过程 |
| `python/ray/data/dataset.py` | ~2868 | `op = Join(...)` | 观察 Join 算子如何合并两个 DAG |
| `python/ray/data/_internal/logical/interfaces/operator.py` | 20 | `_wire_output_deps` | 观察双向引用自动建立过程 |

---

### 3.2 使用 VSCode 调试

创建 `.vscode/launch.json`：

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: Debug Logical Plan",
            "type": "python",
            "request": "launch",
            "program": "${workspaceFolder}/code-analysis/debug_logical_plan.py",
            "console": "integratedTerminal",
            "justMyCode": false,
            "env": {
                "PYTHONPATH": "${workspaceFolder}/python"
            }
        }
    ]
}
```

**关键断点设置**：
- ✅ `Operator.__init__` - 观察每个算子的创建时刻
- ✅ `Operator._wire_output_deps` - 观察依赖关系自动建立
- ✅ `Dataset.map_batches` - 观察 API 调用如何转化为算子

---

## 📊 步骤 4：深入分析的 5 个关键角度

### 角度 1：算子输入依赖变化

在调试器中观察：
- 创建算子前：`input_op._output_dependencies` 有哪些元素？
- 创建算子后：`input_op._output_dependencies` 增加了什么？

### 角度 2：不可变性验证

验证不可变设计是否真的生效：
```python
base_ds = ray.data.range(100)
base_op_id = id(base_ds._logical_plan.dag)

branch1 = base_ds.map(fn1)
branch2 = base_ds.map(fn2)

# 验证 base_ds 的根算子没有变化
assert id(base_ds._logical_plan.dag) == base_op_id

# 验证 branch1 和 branch2 有独立的根算子
assert id(branch1._logical_plan.dag) != base_op_id
assert id(branch2._logical_plan.dag) != base_op_id
```

### 角度 3：多输入的源算子识别

观察 `LogicalPlan.sources()` 的递归遍历：
```python
# 在 logical_plan.py 的 sources() 方法中加断点
def sources(self) -> List[LogicalOperator]:
    import pdb; pdb.set_trace()  # 断点
    if not any(self._dag.input_dependencies):
        return [self._dag]
    # ...
```

### 角度 4：拓扑排序正确性验证

后序遍历应该保证：**所有输入算子都出现在当前算子之前**

```python
visited = set()
for op in root_op.post_order_iter():
    # 验证所有输入都已访问
    for input_op in op.input_dependencies:
        assert input_op in visited, f"{op.name} 的输入 {input_op.name} 未访问"
    visited.add(op)
print("✅ 拓扑排序正确！")
```

### 角度 5：`_apply_transform` 递归变换机制

这是优化器的核心机制，观察算子如何被整体替换：

```python
# 在 operator.py 的 _apply_transform 方法中加断点
def _apply_transform(self, transform):
    import pdb; pdb.set_trace()  # 断点
    # ...
```

---

## 🧪 步骤 5：进阶实验

### 实验 1：验证分支复用的计算重复问题

```python
# 当前 Ray Data 的行为：每个分支独立计算，不共享
base = ray.data.range(100).map(expensive_fn)  # 上游计算
b1 = base.map(fn1)  # 需要重新执行 base 的 map
b2 = base.map(fn2)  # 也需要重新执行 base 的 map

# 问题：有没有办法让 base 的计算只执行一次？
```

**调试目标**：找到 base 的计算是否真的在两个 DAG 中被视为独立节点。

### 实验 2：观察算子融合过程

```python
ds = ray.data.range(1000)
ds = ds.map(fn1).map(fn2).map(fn3)

# 逻辑上有 4 个算子（1 个 Read + 3 个 Map）
# 优化后应该融合成 1 个 Map 算子
print(ds.explain())  # 观察融合前后的差异
```

---

## 📝 步骤 6：问题排查清单

| 问题现象 | 排查方法 | 常见原因 |
|---------|---------|---------|
| `sources()` 返回空 | 检查 `input_dependencies` 是否正确设置 | 算子基类初始化错误 |
| `post_order_iter()` 无限循环 | 检查是否有循环引用 | `_wire_output_deps` 逻辑错误 |
| 算子参数丢失 | 检查 `_get_args()` 方法 | 新增算子未正确实现序列化 |
| schema 推断错误 | 检查 `infer_schema()` 实现 | 元数据传递中断 |

---

## 📚 关键代码文件速查表

| 功能 | 文件路径 | 关键方法 |
|------|---------|---------|
| Logical Plan 容器 | `_internal/logical/interfaces/logical_plan.py` | `sources()`, `dag` |
| Operator 基类 | `_internal/logical/interfaces/operator.py` | `__init__`, `post_order_iter()`, `_apply_transform()`, `_wire_output_deps()` |
| 单输入算子基类 | `_internal/logical/operators/one_to_one_operator.py` | `AbstractOneToOne.__init__` |
| 多输入算子基类 | `_internal/logical/operators/n_ary_operator.py` | `NAry.__init__` |
| API → 算子转换 | `dataset.py` | `map_batches()`, `join()`, `filter()` 等 |

---

## 🎯 学习路径建议

1. **Day 1**: 运行调试脚本，理解输出结构
2. **Day 2**: 单步调试 `Operator._wire_output_deps`，理解依赖自动建立
3. **Day 3**: 对比简单链式调用和 Join 调用的 DAG 结构差异
4. **Day 4**: 研究 `_apply_transform` 如何实现整棵 DAG 不可变变换
5. **Day 5**: 尝试自己新增一个简单的 Logical Operator（如 `Sample`）

祝你分析顺利！有任何问题随时讨论。
