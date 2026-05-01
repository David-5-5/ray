"""
Mini-Ray Worker - 任务执行进程

Worker 职责:
1. 启动时注册到 Scheduler
2. 定期发送心跳（证明自己还活着）
3. 轮询拉取任务执行
4. 反序列化函数和参数，执行用户代码
5. 序列化结果上报给 Scheduler

对应 Ray 中的 Worker 进程 + CoreWorker 执行模块
"""

import sys
import os
import uuid
import time
import grpc
import pickle
import threading
import multiprocessing

# 添加 proto 目录到路径
sys.path.insert(0, "proto")
import scheduler_pb2 as pb2
import scheduler_pb2_grpc as pb2_grpc


class Worker:
    """Worker 执行器"""

    def __init__(self, scheduler_address: str, worker_port: int = None):
        self.worker_id = str(uuid.uuid4())
        self.scheduler_address = scheduler_address
        self.worker_port = worker_port or (50052 + len(os.listdir('.')) % 1000)  # 随机端口
        self.worker_address = f"localhost:{self.worker_port}"

        # gRPC 通道连接 Scheduler
        self.channel = grpc.insecure_channel(scheduler_address)
        self.stub = pb2_grpc.SchedulerServiceStub(self.channel)

        # 运行中任务计数器
        self.running_tasks = 0
        self.running_tasks_lock = threading.Lock()

        print(f"[Worker] Worker ID: {self.worker_id}")
        print(f"[Worker] Scheduler 地址: {scheduler_address}")

    def register(self):
        """注册到 Scheduler"""
        cpu_cores = multiprocessing.cpu_count()
        response = self.stub.RegisterWorker(pb2.RegisterWorkerRequest(
            worker_id=self.worker_id,
            worker_address=self.worker_address,
            cpu_cores=cpu_cores
        ))

        if response.success:
            print(f"[Worker] 注册成功!")
            return True
        else:
            print(f"[Worker] 注册失败: {response.message}")
            return False

    def start_heartbeat(self):
        """启动心跳线程"""
        def heartbeat_loop():
            while True:
                try:
                    with self.running_tasks_lock:
                        running = self.running_tasks
                    self.stub.Heartbeat(pb2.HeartbeatRequest(
                        worker_id=self.worker_id,
                        running_tasks=running
                    ))
                except Exception as e:
                    print(f"[Worker] 心跳失败: {e}")
                time.sleep(5)  # 每 5 秒一次心跳

        threading.Thread(target=heartbeat_loop, daemon=True).start()
        print("[Worker] 心跳线程已启动")

    def execute_task(self, task: pb2.Task):
        """执行单个任务"""
        try:
            print(f"[Worker] 开始执行任务: {task.task_id}")

            # 反序列化函数和参数
            func = pickle.loads(task.function_data)
            args = pickle.loads(task.args_data)
            kwargs = pickle.loads(task.kwargs_data)

            # 执行用户代码
            result = func(*args, **kwargs)

            # 序列化结果
            result_data = pickle.dumps(result)

            # 上报成功
            self.stub.ReportTaskResult(pb2.ReportTaskResultRequest(
                task_id=task.task_id,
                worker_id=self.worker_id,
                success=True,
                result=result_data
            ))

            print(f"[Worker] 任务 {task.task_id} 执行完成")

        except Exception as e:
            import traceback
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            print(f"[Worker] 任务 {task.task_id} 执行失败: {error_msg}")

            # 上报失败
            try:
                self.stub.ReportTaskResult(pb2.ReportTaskResultRequest(
                    task_id=task.task_id,
                    worker_id=self.worker_id,
                    success=False,
                    error=error_msg
                ))
            except:
                pass

    def pull_and_execute(self):
        """拉取并执行任务循环"""
        while True:
            try:
                # 拉取任务
                response = self.stub.PullTask(pb2.PullTaskRequest(
                    worker_id=self.worker_id
                ))

                if response.has_task:
                    # 有任务，在新线程中执行
                    with self.running_tasks_lock:
                        self.running_tasks += 1

                    def run_task(task):
                        try:
                            self.execute_task(task)
                        finally:
                            with self.running_tasks_lock:
                                self.running_tasks -= 1

                    threading.Thread(
                        target=run_task,
                        args=(response.task,),
                        daemon=True
                    ).start()
                else:
                    # 没有任务，睡一会儿再拉
                    time.sleep(0.1)

            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    print("[Worker] Scheduler 不可用，等待重试...")
                    time.sleep(1)
                else:
                    print(f"[Worker] gRPC 错误: {e}")
                    time.sleep(0.5)
            except Exception as e:
                print(f"[Worker] 拉取任务出错: {e}")
                time.sleep(0.5)

    def start(self):
        """启动 Worker"""
        if not self.register():
            return

        self.start_heartbeat()
        print("[Worker] 开始拉取任务...")
        self.pull_and_execute()


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Mini-Ray Worker')
    parser.add_argument('--scheduler', type=str, default='localhost:50051', help='Scheduler 地址')
    parser.add_argument('--port', type=int, default=None, help='Worker 监听端口')
    args = parser.parse_args()

    worker = Worker(args.scheduler, args.port)
    worker.start()


if __name__ == '__main__':
    main()
