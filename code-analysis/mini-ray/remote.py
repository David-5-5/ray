"""
Mini-Ray @ray.remote 装饰器实现

对应 Ray 中 remote_function.py 的核心逻辑
"""

import sys
import pickle
import time
import grpc

sys.path.insert(0, "proto")
import scheduler_pb2 as pb2
import scheduler_pb2_grpc as pb2_grpc


# 全局客户端状态（对应 Ray 的全局 worker）
_global_client = None


class RemoteFunction:
    """被 @ray.remote 装饰的函数包装类

    对应 Ray 的 RemoteFunction 类，核心功能:
    1. 捕获被装饰的函数
    2. 提供 .remote() 方法提交任务
    """

    def __init__(self, func):
        self.func = func
        self.function_data = pickle.dumps(func)

    def remote(self, *args, **kwargs):
        """提交远程任务

        对应 Ray 中 .remote() 方法的核心逻辑
        """
        if _global_client is None:
            raise RuntimeError("请先调用 ray.init() 初始化")

        # 序列化参数
        args_data = pickle.dumps(args)
        kwargs_data = pickle.dumps(kwargs)

        # 通过 gRPC 提交任务到 Scheduler
        response = _global_client.stub.SubmitTask(pb2.SubmitTaskRequest(
            function_data=self.function_data,
            args_data=args_data,
            kwargs_data=kwargs_data
        ))

        if not response.success:
            raise RuntimeError(f"任务提交失败: {response.message}")

        # 返回 ObjectRef（简化版，就是 task_id 字符串）
        return ObjectRef(response.task_id)


class ObjectRef:
    """对象引用，对应 Ray 的 ObjectRef

    用于标识一个远程对象，可以通过 ray.get() 获取其值
    """

    def __init__(self, task_id: str):
        self.task_id = task_id

    def __repr__(self):
        return f"ObjectRef({self.task_id})"


class RayClient:
    """Mini-Ray 客户端

    对应 Ray 中 Driver 进程的 CoreWorker
    """

    def __init__(self, scheduler_address: str):
        self.scheduler_address = scheduler_address
        self.channel = grpc.insecure_channel(scheduler_address)
        self.stub = pb2_grpc.SchedulerServiceStub(self.channel)

    def get(self, obj_ref: ObjectRef, timeout: int = None):
        """获取任务结果

        对应 Ray 的 ray.get()
        """
        start_time = time.time()

        while True:
            # 检查超时
            if timeout and time.time() - start_time > timeout:
                raise TimeoutError(f"等待任务结果超时: {obj_ref.task_id}")

            # 查询任务结果
            response = self.stub.GetTaskResult(pb2.GetTaskResultRequest(
                task_id=obj_ref.task_id
            ))

            if response.status == pb2.FINISHED:
                # 任务完成，反序列化结果
                return pickle.loads(response.result)
            elif response.status == pb2.FAILED:
                # 任务失败
                raise RuntimeError(f"任务执行失败: {response.error}")
            else:
                # 任务还在执行，等一会儿再查
                time.sleep(0.1)

    def shutdown(self):
        """关闭客户端"""
        self.channel.close()


def remote(func):
    """@ray.remote 装饰器

    用法:
        @ray.remote
        def my_func(x, y):
            return x + y

        # 提交任务
        future = my_func.remote(1, 2)

        # 获取结果
        result = ray.get(future)
    """
    return RemoteFunction(func)


def init(scheduler_address: str = "localhost:50051"):
    """初始化 Mini-Ray 客户端

    对应 Ray 的 ray.init()
    """
    global _global_client
    _global_client = RayClient(scheduler_address)
    print(f"[Ray] 已连接到 Scheduler: {scheduler_address}")
    return _global_client


def get(obj_ref: ObjectRef, timeout: int = None):
    """获取任务结果

    对应 Ray 的 ray.get()
    """
    if _global_client is None:
        raise RuntimeError("请先调用 ray.init() 初始化")
    return _global_client.get(obj_ref, timeout)


def shutdown():
    """关闭客户端

    对应 Ray 的 ray.shutdown()
    """
    global _global_client
    if _global_client:
        _global_client.shutdown()
        _global_client = None
    print("[Ray] 已关闭")
