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
        on=("id",),       # 必须是 tuple/list
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