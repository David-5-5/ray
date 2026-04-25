# Ray Core 核心流程分析

本文档聚合 Ray 核心流程，按主题分类记录。

---

## 目录

- [ray start --head 执行流程](#ray-start---head-执行流程)
- [@ray.remote 任务提交流程](#rayremote-任务提交流程)

---

## ray start --head 执行流程

### 概述

详细记录 `ray start --head` 命令从 Python 入口到 C++ 服务的完整执行路径。

### 一、Python 入口层

#### 1. CLI 入口

**关键文件**: `python/ray/scripts/scripts.py` (第 679 行)

```python
@click.command()
@click.option("--head", is_flag=True, default=False, help="provide this argument for the head node")
def start(head, ...):
    """Start Ray processes manually on the local machine."""
```

**执行流程**:
1. 解析命令行参数 (`--head`, `--port`, `--num-cpus` 等)
2. 解析资源和标签配置
3. 构造 RayParams 对象
4. 创建 Node 实例，head=True
5. 调用 node.start_head_processes() 和 node.start_ray_processes()

#### 2. Node 类

**关键文件**: `python/ray/_private/node.py`

**关键方法**:
- `__init__()` - 初始化节点配置
- `start_head_processes()` (第 1344 行) - 启动 Head 专属服务
- `start_ray_processes()` (第 1373 行) - 启动通用节点服务

### 二、Head 节点启动流程

#### 第一阶段：Head 专属服务 (`start_head_processes`)

```
1. start_gcs_server()
   ↓
2. start_monitor() (autoscaler)
   ↓
3. start_ray_client_server() (可选，默认端口 10001)
   ↓
4. start_api_server() (Dashboard + REST API)
```

#### 第二阶段：通用节点服务 (`start_ray_processes`)

```
1. start_raylet()  ← 核心！包含 Object Store + Node Manager
   ↓
2. 启动 Dashboard Agent
   ↓
3. 启动 Runtime Env Agent
```

### 三、Python → C++ 进程启动层

**关键文件**: `python/ray/_private/services.py`

| 启动函数 | 可执行文件 | C++ 入口文件 |
|---------|-----------|-------------|
| `start_gcs_server()` | `gcs_server` | `src/ray/gcs/gcs_server_main.cc` |
| `start_raylet()` | `raylet` | `src/ray/raylet/main.cc` |

**可执行文件路径**:
```python
RAYLET_EXECUTABLE = "python/ray/core/src/ray/raylet/raylet"
GCS_SERVER_EXECUTABLE = "python/ray/core/src/ray/gcs/gcs_server"
```

---

## @ray.remote 任务提交流程

### 概述

详细追踪 `@ray.remote` 装饰器从 Python 函数定义到任务提交、调度、执行、返回结果的完整调用链。

### 一、Python 语法层：装饰器阶段

#### 1. `@ray.remote` 装饰器入口

**关键文件**: `python/ray/_private/worker.py` (第 3552 行)

```python
@PublicAPI
def remote(
    *args, **kwargs
) -> Union[ray.remote_function.RemoteFunction, ray.actor.ActorClass]:
```

**执行流程**:
1. 当用 `@ray.remote` 装饰一个函数或类时
2. 如果是函数 → 创建 `RemoteFunction` 对象
3. 如果是类 → 创建 `ActorClass` 对象

### 二、Python 层：RemoteFunction 初始化

**关键文件**: `python/ray/remote_function.py` (第 41 行)

```python
class RemoteFunction:
    def __init__(self, language, function, function_descriptor, task_options):
```

**关键点**:
- `self.remote` 方法是实际调用 `.remote()` 时的入口
- 此时函数并未真正执行，只是被包装

### 三、调用 `.remote()` 提交任务

**关键文件**: `python/ray/remote_function.py` (第 314 行)

```python
def _remote(self, args=None, kwargs=None, ...):
    """Submit the remote function for execution."""
```

**提交流程**:
1. 检查 worker 连接状态
2. 函数序列化与导出（第一次调用时）
3. 创建函数描述符
4. 序列化函数（pickle）
5. 导出函数到 GCS
6. 调用 core_worker.submit_task (C++ 边界)

### 四、C++ 层：任务提交

**关键文件**: `src/ray/core_worker/task_submission/normal_task_submitter.h`

```cpp
class NormalTaskSubmitter {
public:
    void SubmitTask(TaskSpecification task_spec);
private:
    DependencyResolver resolver_;
    TaskManagerInterface &task_manager_;
    std::shared_ptr<RayletClientInterface> local_raylet_client_;
};
```

### 五、调度层：Raylet 任务调度

**关键文件**: `src/ray/raylet/node_manager.h`

**调度流程**:
```
RequestLease RPC → Raylet
  ↓
1. ClusterResourceScheduler 检查资源可用性
  ↓
2. 找到可用节点后，检查是否有匹配的 worker 进程
  ↓
3. 若没有现成 worker: 启动新的 Python 进程
  ↓
4. 返回租约（包含目标 worker 的地址）
```

### 六、完整流程架构图

```
用户代码 Python 进程 (Driver)
┌─────────────────────────────────────────────────────────┐
│  @ray.remote                                            │
│      ↓ (装饰阶段)                                        │
│  RemoteFunction 实例                                     │
│      ↓ (调用 .remote())                                  │
│  _remote() 方法                                           │
│      ↓ (函数序列化 + export)                              │
│  worker.core_worker.submit_task()                        │
└────────────────────────┬────────────────────────────────┘
                         │ C++ 边界
┌────────────────────────▼────────────────────────────────┐
│  C++ CoreWorker (Driver)                                │
│      ↓ NormalTaskSubmitter::SubmitTask                  │
│  1. 依赖解析                                             │
│  2. 向 Raylet 请求租约 (RequestLease)                   │
└────────────────────────┬────────────────────────────────┘
                         │ gRPC
┌────────────────────────▼────────────────────────────────┐
│  Raylet (本地节点调度器)                                 │
│      ↓ ClusterResourceScheduler                         │
│  1. 资源匹配与节点选择                                    │
│  2. 启动/复用 Worker 进程                                 │
│  3. 返回租约（目标 Worker 地址）                          │
└────────────────────────┬────────────────────────────────┘
                         │ gRPC
┌────────────────────────▼────────────────────────────────┐
│  Worker 进程 (Python + C++)                             │
│  ┌─────────────────────────────────────────────────┐   │
│  │  C++ CoreWorker                                 │   │
│  │    ↓ TaskReceiver::HandleTasks                  │   │
│  │  1. 反序列化任务参数                              │   │
│  │  2. 调用 Python 执行器                            │   │
│  └──────────────────┬──────────────────────────────┘   │
│                     │ Python 边界                        │
│  ┌──────────────────▼──────────────────────────────┐   │
│  │  Python Worker                                   │   │
│  │    ↓ 反序列化函数和参数                            │   │
│  │    ↓ 执行用户函数                                  │   │
│  │    ↓ 序列化结果                                   │   │
│  └──────────────────┬──────────────────────────────┘   │
│                     │ 写入对象存储                        │
└────────────────────────▼────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│  Object Store (Plasma) - 共享内存对象存储               │
└─────────────────────────────────────────────────────────┘
```

---

## 关键文件索引

### Python 层

| 文件 | 作用 |
|------|------|
| `python/ray/scripts/scripts.py` | CLI 入口 |
| `python/ray/_private/node.py` | Node 类，进程编排 |
| `python/ray/_private/services.py` | 服务启动与进程管理 |
| `python/ray/remote_function.py` | RemoteFunction 类 |

### C++ 层

| 文件 | 作用 |
|------|------|
| `src/ray/gcs/gcs_server.h` | GCS Server 核心类 |
| `src/ray/raylet/raylet.h` | Raylet 核心类 |
| `src/ray/raylet/node_manager.h` | 节点管理器 |
| `src/ray/core_worker/core_worker.h` | CoreWorker 核心类 |
| `src/ray/core_worker/task_submission/normal_task_submitter.h` | 任务提交器 |

---

*文档生成时间: 2024-04-25*
