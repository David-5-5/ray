"""
Mini-Ray Scheduler - 调度器核心实现

调度器职责:
1. 接收 Worker 注册和心跳
2. 维护 Worker 池（活跃/死亡节点）
3. 接收客户端任务提交
4. 任务分发（Round Robin 策略）
5. 维护任务状态表
6. 返回任务结果给客户端

对应 Ray 中的 GCS + Raylet 调度模块的简化版
"""

import sys
import uuid
import time
import grpc
import pickle
import threading
from concurrent import futures
from typing import Dict, List, Optional, Deque
from collections import deque

# 添加 proto 目录到路径
sys.path.insert(0, "proto")
import scheduler_pb2 as pb2
import scheduler_pb2_grpc as pb2_grpc


class WorkerInfo:
    """Worker 信息"""
    def __init__(self, worker_id: str, address: str, cpu_cores: int):
        self.worker_id = worker_id
        self.address = address
        self.cpu_cores = cpu_cores
        self.running_tasks = 0
        self.last_heartbeat = time.time()


class Task:
    """任务对象"""
    def __init__(self, task_id: str, function_data: bytes, args_data: bytes, kwargs_data: bytes):
        self.task_id = task_id
        self.function_data = function_data
        self.args_data = args_data
        self.kwargs_data = kwargs_data
        self.status = pb2.PENDING
        self.result = None
        self.error = None
        self.worker_id = None
        self.create_time = time.time()


class SchedulerService(pb2_grpc.SchedulerServiceServicer):
    """调度器 gRPC 服务"""

    def __init__(self):
        # Worker 池: worker_id -> WorkerInfo
        self.workers: Dict[str, WorkerInfo] = {}
        self.workers_lock = threading.Lock()

        # 待执行任务队列
        self.pending_tasks: Deque[Task] = deque()

        # 任务状态表: task_id -> Task
        self.task_table: Dict[str, Task] = {}
        self.tasks_lock = threading.Lock()

        # 心跳超时阈值 (秒)
        self.HEARTBEAT_TIMEOUT = 30

        # 启动 Worker 健康检查线程
        self.health_check_thread = threading.Thread(target=self._health_check_loop, daemon=True)
        self.health_check_thread.start()

        print("[Scheduler] 调度器服务已启动")

    def RegisterWorker(self, request, context):
        """Worker 注册"""
        with self.workers_lock:
            worker_id = request.worker_id
            worker = WorkerInfo(
                worker_id=worker_id,
                address=request.worker_address,
                cpu_cores=request.cpu_cores
            )
            self.workers[worker_id] = worker
            print(f"[Scheduler] Worker 已注册: {worker_id} @ {request.worker_address}")
            print(f"[Scheduler] 当前活跃 Worker 数量: {len(self.workers)}")
            return pb2.RegisterWorkerResponse(success=True, message="注册成功")

    def Heartbeat(self, request, context):
        """Worker 心跳"""
        with self.workers_lock:
            if request.worker_id in self.workers:
                self.workers[request.worker_id].last_heartbeat = time.time()
                self.workers[request.worker_id].running_tasks = request.running_tasks
        return pb2.HeartbeatResponse(ok=True)

    def SubmitTask(self, request, context):
        """客户端提交任务"""
        task_id = str(uuid.uuid4())

        with self.tasks_lock:
            task = Task(
                task_id=task_id,
                function_data=request.function_data,
                args_data=request.args_data,
                kwargs_data=request.kwargs_data
            )
            self.task_table[task_id] = task
            self.pending_tasks.append(task)

        print(f"[Scheduler] 任务已提交: {task_id}, 待执行队列长度: {len(self.pending_tasks)}")
        return pb2.SubmitTaskResponse(
            task_id=task_id,
            success=True,
            message="任务提交成功"
        )

    def PullTask(self, request, context):
        """Worker 拉取任务 (轮询)"""
        worker_id = request.worker_id

        with self.tasks_lock:
            if not self.pending_tasks:
                return pb2.PullTaskResponse(has_task=False)

            # Round Robin: 取队首任务
            task = self.pending_tasks.popleft()
            task.status = pb2.RUNNING
            task.worker_id = worker_id

            print(f"[Scheduler] 任务 {task.task_id} 分配给 Worker {worker_id}")

            return pb2.PullTaskResponse(
                has_task=True,
                task=pb2.Task(
                    task_id=task.task_id,
                    function_data=task.function_data,
                    args_data=task.args_data,
                    kwargs_data=task.kwargs_data,
                    status=task.status,
                    worker_id=task.worker_id
                )
            )

    def ReportTaskResult(self, request, context):
        """Worker 上报任务结果"""
        task_id = request.task_id

        with self.tasks_lock:
            if task_id not in self.task_table:
                return pb2.ReportTaskResultResponse(success=False)

            task = self.task_table[task_id]
            if request.success:
                task.status = pb2.FINISHED
                task.result = request.result
                print(f"[Scheduler] 任务 {task_id} 执行成功")
            else:
                task.status = pb2.FAILED
                task.error = request.error
                print(f"[Scheduler] 任务 {task_id} 执行失败: {request.error}")

        return pb2.ReportTaskResultResponse(success=True)

    def GetTaskResult(self, request, context):
        """客户端获取任务结果"""
        task_id = request.task_id

        with self.tasks_lock:
            if task_id not in self.task_table:
                return pb2.GetTaskResultResponse(
                    status=pb2.FAILED,
                    error="任务不存在"
                )

            task = self.task_table[task_id]
            return pb2.GetTaskResultResponse(
                status=task.status,
                result=task.result if task.result else b'',
                error=task.error if task.error else ''
            )

    def _health_check_loop(self):
        """Worker 健康检查循环"""
        while True:
            time.sleep(10)  # 每 10 秒检查一次
            now = time.time()

            with self.workers_lock:
                dead_workers = []
                for worker_id, worker in self.workers.items():
                    if now - worker.last_heartbeat > self.HEARTBEAT_TIMEOUT:
                        dead_workers.append(worker_id)

                for worker_id in dead_workers:
                    print(f"[Scheduler] Worker 超时掉线: {worker_id}")
                    del self.workers[worker_id]


def serve(port: int = 50051):
    """启动调度器服务"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=100))
    pb2_grpc.add_SchedulerServiceServicer_to_server(SchedulerService(), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"[Scheduler] 调度器启动成功，监听端口 {port}")
    print(f"[Scheduler] 等待 Worker 连接...")
    server.wait_for_termination()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Mini-Ray Scheduler')
    parser.add_argument('--port', type=int, default=50051, help='监听端口')
    args = parser.parse_args()
    serve(args.port)
