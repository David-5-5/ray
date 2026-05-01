"""
Mini-Ray 使用示例

演示如何使用 mini-ray 进行分布式计算
"""

import sys
sys.path.insert(0, ".")

from remote import remote, init, get, shutdown


@remote
def add(a, b):
    """简单加法"""
    return a + b


@remote
def square(x):
    """平方计算"""
    return x * x


@remote
def fibonacci(n):
    """计算斐波那契数列 (CPU密集型)"""
    if n <= 1:
        return n
    return fibonacci.remote(n - 1) + fibonacci.remote(n - 2)


def main():
    print("=" * 50)
    print("Mini-Ray 分布式计算示例")
    print("=" * 50)

    # 初始化连接到 Scheduler
    print("\n[步骤 1] 初始化 Mini-Ray 客户端...")
    init("localhost:50051")

    # 示例 1: 简单任务提交
    print("\n[步骤 2] 提交简单加法任务...")
    result_ref = add.remote(10, 20)
    print(f"  任务已提交，返回 ObjectRef: {result_ref}")

    result = get(result_ref)
    print(f"  任务结果: 10 + 20 = {result}")

    # 示例 2: 批量任务提交
    print("\n[步骤 3] 批量提交任务 (10个平方计算)...")
    refs = [square.remote(i) for i in range(10)]
    results = [get(ref) for ref in refs]
    print(f"  批量任务结果: {results}")

    # 示例 3: 并行计算
    print("\n[步骤 4] 并行执行多个加法任务...")
    refs = [add.remote(i, i * 10) for i in range(5)]
    results = [get(ref) for ref in refs]
    for i, r in enumerate(results):
        print(f"  {i} + {i * 10} = {r}")

    print("\n" + "=" * 50)
    print("所有示例执行完成!")
    print("=" * 50)

    # 关闭客户端
    shutdown()


if __name__ == "__main__":
    main()
