# Ray Core 核心流程分析

本文档按核心文件和调用链组织 Ray 核心流程分析，通过交叉引用建立完整的知识体系。

---

[toc]

---

## 1. ray start 集群启动流程

### 1.1 入口层：scripts.py

**位置**: 第 679-1180 行

`ray start` 命令支持 **Head 节点** 和 **Worker 节点** 两种模式，共用同一个 `start()` 函数（约 500 行代码），通过 `--head` 参数分支执行不同逻辑。

#### 1.1.1 CLI 参数设计

40+ 个参数按功能分类：

| 参数类别 | 关键参数 | 作用 |
|---------|---------|------|
| **节点标识** | `--head`, `--address`, `--port` | Head/Worker 模式选择 |
| **资源配置** | `--num-cpus`, `--num-gpus`, `--memory`, `--resources` | 节点资源声明 |
| **网络端口** | `--object-manager-port`, `--node-manager-port`, `--min-worker-port` | 各组件端口绑定 |
| **Dashboard** | `--include-dashboard`, `--dashboard-port` | Dashboard 配置 |
| **存储配置** | `--plasma-directory`, `--object-spilling-directory` | 对象存储配置 |
| **高级配置** | `--system-config`, `--temp-dir`, `--block` | 系统级配置 |
| **标签** | `--labels`, `--labels-file` | 节点标签（调度用） |

#### 1.1.2 执行流程总览

```
ray start [--head]
    ↓
[ 通用阶段 ] - Head 和 Worker 都执行
    ├── 1. 参数解析与验证
    ├── 2. 资源和标签解析
    └── 3. 构建 RayParams 对象
    ↓
[ 分支阶段 ] - 根据 --head 分流
    ├─→ Head 节点流程 ──┐
    │                   │
    └─→ Worker 节点流程 ┘
    ↓
[ 收尾阶段 ]
    ├── 写入 gcs_address 到临时目录
    └── --block 模式：进入无限循环监控
```

#### 1.1.3 通用阶段详解

**参数解析与验证**

```python
# 双重标签解析：字符串 + YAML 文件
labels_from_file = parse_node_labels_from_yaml_file(labels_file)
labels_from_string = parse_node_labels_string(labels)  # key1=val1,key2=val2
labels_dict = {**labels_from_file, **labels_from_string}  # 字符串优先级更高

# 资源隔离配置
resource_isolation_config = ResourceIsolationConfig(...)
```

**RayParams 对象构建**

**关联文件**: `python/ray/_private/parameter.py`

RayParams 是**所有配置的唯一容器**，包含 30+ 个基础配置字段、资源隔离配置、Dashboard 配置、系统级配置。

**扩展点：RAY_START_HOOK**

```python
if ray_constants.RAY_START_HOOK in os.environ:
    load_class(os.environ[ray_constants.RAY_START_HOOK])(ray_params, head)
```

允许在真正启动前注入自定义逻辑（测试、监控、审计等）。

#### 1.1.4 Head 节点流程 (`--head`)

```
ray start --head --port=6379
    ↓
1. 设置 GCS 端口（默认 6379）
    ↓
2. ✅ 冲突检测：检查默认地址是否已在运行
    ↓
3. 创建 Node 实例 (head=True)
    ↓
4. 打印 Dashboard 地址和使用说明
    ↓
5. 写入 gcs_address 到临时文件
    ↓
6. --block 模式：进入监控循环
```

**关键设计：冲突检测**：通过临时目录中的 bootstrap 地址与默认地址比较，防止重复启动。

#### 1.1.5 Worker 节点流程

```
ray start --address=head-node:6379
    ↓
1. ✅ 必须指定 --address，否则直接 abort
    ↓
2. ✅ Head-only 参数检查（禁止 Worker 指定 --port 等）
    ↓
3. 规范化 GCS 地址
    ↓
4. 获取本机 IP 地址（连接到 GCS 后自动探测）
    ↓
5. 创建 Node 实例 (head=False) → 见 [1.2 进程编排层](#12-进程编排层nodepy)
    ↓
6. 版本检查：确保 Worker 与 Head 版本一致
    ↓
7. 写入 gcs_address 到临时文件
    ↓
8. --block 模式：进入监控循环
```

**关键设计：Worker 自动 IP 探测**：Worker 需要连接到 GCS 后才能确定自己应该使用哪个网卡。

#### 1.1.6 --block 模式详解

指定 `--block` 时，进程进入无限循环监控子进程状态：

```python
while True:
    time.sleep(1)
    deceased = node.dead_processes()  # 见 [1.2.4 进程统一管理机制](#124-进程统一管理机制)
    
    # 检查异常退出的进程
    expected_return_codes = [0, SIGTERM, -SIGTERM, 128+SIGTERM]
    unexpected_deceased = [p for p in deceased if p.returncode not in expected_return_codes]
    
    if unexpected_deceased:
        cli_logger.error("Some Ray processes died unexpectedly...")
```

**作用**:
- 生产环境推荐使用，防止进程退出导致集群节点离线
- 监控所有子进程（raylet、gcs_server 等）
- 异常退出时及时报警

---

### 1.2 进程编排层：node.py

**调用入口**: `scripts.py` 第 112/146 行创建 Node 实例

Node 类是**进程编排器**，负责所有子进程的生命周期管理，所有 C++ 进程均通过此类间接调用 `services.py` 启动。

#### 1.2.1 Node 类核心职责

| 职责 | 说明 |
|------|------|
| **进程启动** | Head 专属服务、Raylet、Agent 等 |
| **进程监控** | 检测子进程异常退出 |
| **配置同步** | Worker 从 GCS 拉取系统配置 |
| **生命周期** | 集群启动、关闭、清理 |

**关键方法矩阵**:

| 方法 | Head | Worker | 作用 |
|------|------|--------|------|
| `__init__()` | ✅ | ✅ | 初始化，调用下面两个方法 |
| `start_head_processes()` | ✅ | ❌ | GCS、Monitor、API Server |
| `start_ray_processes()` | ✅ | ✅ | Raylet、Agent、Log Monitor |
| `dead_processes()` | ✅ | ✅ | 获取已死亡的子进程 |
| `kill_all_processes()` | ✅ | ✅ | 杀死所有子进程 |
| `check_version_info()` | ❌ | ✅ | Worker 版本校验 |

---

#### 1.2.2 start_head_processes() 详解

**位置**: 第 1344 行

Head 节点专属，启动 GCS 及相关服务。

```python
def start_head_processes(self):
    self.start_gcs_server()              # 见 [1.2.2.1 Node.start_gcs_server()]
    self._write_cluster_info_to_kv()     # 写入元数据到 KV
    
    if not self._ray_params.no_monitor:
        self.start_monitor()             # 自动扩缩容监控
    
    if self._ray_params.ray_client_server_port:
        self.start_ray_client_server()   # 客户端连接服务
    
    self.start_api_server()              # Dashboard + REST API
```

**启动顺序原则**:
1. **GCS Server 必须第一个启动**，其他所有服务都依赖 GCS 进行通信和状态存储
2. 启动顺序严格按照依赖关系排列

##### 1.2.2.1 Node.start_gcs_server()

**位置**: 第 1113 行

Head 节点启动 GCS Server 的包装方法：

```python
def start_gcs_server(self):
    # 前置断言：确保只启动一次
    gcs_server_port = self._ray_params.gcs_server_port
    assert gcs_server_port > 0
    assert self._gcs_address is None, "GCS server is already running."
    
    # 1. 日志文件命名（唯一化）
    stdout_log_fname, stderr_log_fname = self.get_log_file_names(
        "gcs_server", unique=True, create_out=True, create_err=True
    )
    
    # 2. 调用 services.start_gcs_server() 启动 C++ 进程 → 见 [1.3.2]
    process_info = ray._private.services.start_gcs_server(
        self.redis_address,
        log_dir=self._logs_dir,
        stdout_filepath=stdout_log_fname,
        stderr_filepath=stderr_log_fname,
        session_name=self.session_name,
        ...,  # 12 个参数
    )
    
    # 3. 注册进程到 all_processes 字典 → 见 [1.2.4]
    assert ray_constants.PROCESS_TYPE_GCS_SERVER not in self.all_processes
    self.all_processes[ray_constants.PROCESS_TYPE_GCS_SERVER] = [process_info]
    
    # 4. 保存 GCS 地址供其他组件使用
    self._gcs_address = build_address(self._node_ip_address, gcs_server_port)
```

**关键设计要点**:

| 设计点 | 说明 |
|--------|------|
| **前置断言** | 防止重复启动 GCS，确保单例 |
| **日志唯一化** | `unique=True` 确保每次启动生成不同的日志文件 |
| **Fate Share 机制** | `kernel_fate_share` 确保父进程退出时 GCS 自动清理 → 见 [1.3.4] |
| **进程注册** | 所有进程注册到 `all_processes` 字典统一管理 → 见 [1.2.4] |

---

#### 1.2.3 start_ray_processes() 详解

**位置**: 第 1373 行

**调用方**: Head 和 Worker 节点 `__init__` 时都调用此方法

```python
def start_ray_processes(self):
    # [Worker 节点专属] 先从 GCS 拉取系统配置
    if not self.head:
        gcs_options = ray._raylet.GcsClientOptions.create(...)
        global_state = ray._private.state.GlobalState()
        global_state._initialize_global_state(gcs_options)
        new_config = global_state.get_system_config()
        
        # 配置一致性校验：本地配置必须是 GCS 配置的子集
        assert self._config.items() <= new_config.items(), (...)
        self._config = new_config
    
    # 1. 资源规格与硬件使用记录
    resource_and_label_spec = self.get_resource_and_label_spec()
    
    # 2. 确定 Plasma 对象存储配置
    plasma_directory, fallback_directory, object_store_memory = \
        services.determine_plasma_store_config(...)
    
    # 3. 资源隔离：将对象存储内存计入系统保留内存
    if self.resource_isolation_config.is_enabled():
        self.resource_isolation_config.add_object_store_memory(object_store_memory)
    
    # 4. 启动 Log Monitor（可选，用于日志聚合）
    if self._ray_params.include_log_monitor:
        self.start_log_monitor()
    
    # 5. 启动 Raylet（核心！本地调度器和对象存储）→ 见 [1.2.3.1]
    self.start_raylet(plasma_directory, fallback_directory, object_store_memory)
```

**关键设计：Worker 配置同步机制**

Worker 节点必须从 GCS 拉取系统配置，确保整个集群配置一致：
- 使用 `GlobalState` 客户端连接 GCS
- 拉取后进行子集校验，确保本地配置没有冲突
- 冲突时直接抛出异常，防止启动不一致的节点

##### 1.2.3.1 Node.start_raylet()

**位置**: 第 1147 行

Node 类中参数最多的方法之一，负责启动 Raylet 进程，分为 4 个阶段：

```python
def start_raylet(
    self,
    plasma_directory: str,
    fallback_directory: str,
    object_store_memory: int,
    use_valgrind: bool = False,
    use_profiler: bool = False,
):
    # 阶段 1：日志文件准备 - 为 Raylet 及内部 Agent 分别准备日志
    raylet_stdout_filepath, raylet_stderr_filepath = \
        self.get_log_file_names(PROCESS_TYPE_RAYLET, unique=True, ...)
    dashboard_agent_stdout_filepath, dashboard_agent_stderr_filepath = \
        self.get_log_file_names(PROCESS_TYPE_DASHBOARD_AGENT, ...)
    runtime_env_agent_stdout_filepath, runtime_env_agent_stderr_filepath = \
        self.get_log_file_names(PROCESS_TYPE_RUNTIME_ENV_AGENT, ...)
    
    # 阶段 2：资源隔离配置 - 收集系统进程 PID 用于 Cgroup
    self.resource_isolation_config.add_system_pids(
        self._get_system_processes_for_resource_isolation()
    )
    
    # 阶段 3：调用 services.start_raylet() 启动 C++ 进程 → 见 [1.3.3]
    process_info = ray._private.services.start_raylet(
        self.redis_address, self.gcs_address, self._node_id,  # 地址与标识
        self._node_ip_address, self._ray_params.node_manager_port,
        self._raylet_socket_name, self._plasma_store_socket_name,  # Socket 通信
        ...,  # 共 30+ 个参数
    )
    
    # 阶段 4：进程注册 → 见 [1.2.4]
    assert ray_constants.PROCESS_TYPE_RAYLET not in self.all_processes
    self.all_processes[ray_constants.PROCESS_TYPE_RAYLET] = [process_info]
```

---

#### 1.2.4 进程统一管理机制

Node 类通过 `all_processes` 字典统一管理所有子进程：

```python
# 字典结构
self.all_processes = {
    PROCESS_TYPE_GCS_SERVER: [process_info],      # Head 专属
    PROCESS_TYPE_RAYLET: [process_info],           # 所有节点
    PROCESS_TYPE_DASHBOARD: [process_info],        # Head 专属
    PROCESS_TYPE_MONITOR: [process_info],          # Head 专属
    PROCESS_TYPE_LOG_MONITOR: [process_info],      # 可选
    # ... 其他进程类型
}
```

**统一接口**:
- `dead_processes()`：从此字典查询异常退出的进程 → 被 `scripts.py` 的 --block 模式调用
- `kill_all_processes()`：从此字典获取所有进程并杀死 → 被 `ray stop` 命令调用

---

### 1.3 进程启动层：services.py

**调用入口**: `node.py` 中所有 `start_xxx()` 方法最终都调用此文件

底层进程启动函数的集合，提供所有 C++ 进程的 Python 包装。

#### 1.3.1 可执行文件路径映射

**位置**: 第 52-56 行

这些路径对应 Bazel 编译输出，打包在 Python wheel 中：

```python
RAYLET_EXECUTABLE = os.path.join(
    RAY_PATH, "core", "src", "ray", "raylet", "raylet" + EXE_SUFFIX
)
GCS_SERVER_EXECUTABLE = os.path.join(
    RAY_PATH, "core", "src", "ray", "gcs", "gcs_server" + EXE_SUFFIX
)
```

---

#### 1.3.2 start_gcs_server() 详解

**位置**: 第 1452 行

**调用方**: `Node.start_gcs_server()` → 见 [1.2.2.1]

```python
def start_gcs_server(
    redis_address: str, log_dir: str, ..., gcs_server_port: Optional[int] = None,
):
    # 构建命令行参数
    command = [
        GCS_SERVER_EXECUTABLE,
        f"--log_dir={log_dir}",
        f"--config_list={serialize_config(config)}",  # 序列化系统配置
        f"--gcs_server_port={gcs_server_port}",
        f"--node-ip-address={node_ip_address}",
        f"--session-name={session_name}",
        f"--ray-commit={ray.__commit__}",  # Git commit hash，版本校验
    ]
    
    # Redis 相关参数（可选外部 Redis）
    if redis_address:
        command += [f"--redis_address={redis_ip_address}", f"--redis_port={redis_port}"]
    
    # 调用通用进程启动器 → 见 [1.3.4]
    process_info = start_ray_process(
        command, ray_constants.PROCESS_TYPE_GCS_SERVER, ...
    )
    return process_info
```

---

#### 1.3.3 start_raylet() 详解

**位置**: 第 1536 行

**调用方**: `Node.start_raylet()` → 见 [1.2.3.1]

**函数签名**（30+ 个参数）：

```python
def start_raylet(
    redis_address: str, gcs_address: str, node_id: str, node_ip_address: str,
    node_manager_port: int, raylet_name: str, plasma_store_name: str,
    cluster_id: str, worker_path: str, setup_worker_path: str,
    temp_dir: str, session_dir: str, resource_dir: str, log_dir: str,
    resource_and_label_spec, plasma_directory: str, fallback_directory: str,
    object_store_memory: int, session_name: str, is_head_node: bool,
    resource_isolation_config: ResourceIsolationConfig,
    min_worker_port: Optional[int] = None, max_worker_port: Optional[int] = None,
    # ... 共 30+ 个参数
):
    # 内部调用 start_ray_process() → 见 [1.3.4]
```

**设计特点**:
- 参数数量极多（30+），反映了 Raylet 的复杂性
- 包含资源隔离、端口管理、路径配置等所有底层细节
- 所有配置最终通过命令行参数传递给 C++ 进程

---

#### 1.3.4 通用进程启动器：start_ray_process()

所有 C++ 进程都通过此函数启动，统一处理：

```python
def start_ray_process(
    command: List[str], process_type: str,
    stdout_file=None, stderr_file=None, fate_share=None,
):
    # 1. 环境变量设置
    # 2. 进程创建 subprocess.Popen
    # 3. Fate Share 机制（父进程死亡时子进程自动死亡）
    # 4. 返回 ProcessInfo 对象
```

**核心机制：Fate Share**

Ray 的核心设计，确保父进程退出时所有子进程自动清理，防止孤儿进程。此参数由 `node.py` 传入的 `kernel_fate_share` 控制 → 见 [1.2.2.1]。

---

### 1.4 C++ 核心组件层

启动入口：`services.py` 中通过可执行文件路径调用 C++ 二进制

#### 1.4.1 GCS Server 架构

**关键文件**: `src/ray/gcs/gcs_server.h`

GCS（Global Control Store）是 Ray 的全局状态管理中心，所有元数据都存储在这里。

**核心组件**

```cpp
class GcsServer {
private:
    // 元数据管理
    std::unique_ptr<GcsNodeManager> gcs_node_manager_;
    std::unique_ptr<GcsActorManager> gcs_actor_manager_;   // 核心！
    std::unique_ptr<GcsJobManager> gcs_job_manager_;
    std::unique_ptr<GcsWorkerManager> gcs_worker_manager_;
    std::unique_ptr<GcsPlacementGroupManager> gcs_placement_group_manager_;
    std::unique_ptr<GcsTaskManager> gcs_task_manager_;
    
    // 资源与调度
    std::unique_ptr<GcsResourceManager> gcs_resource_manager_;
    std::unique_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;
    std::unique_ptr<ClusterLeaseManager> cluster_lease_manager_;
    
    // KV 存储与通信
    std::unique_ptr<GcsKVManager> gcs_kv_manager_;
    std::unique_ptr<rpc::GrpcServer> rpc_server_;
};
```

**启动完整流程**

```
GcsServer::Start()
  ↓
1. 初始化存储后端（Redis / 内存）
  ↓
2. 初始化各 Manager（按依赖顺序）
  ├── GcsNodeManager
  ├── GcsResourceManager
  ├── GcsActorManager    ← Actor 生命周期管理 → 见 [2.2]
  ├── GcsJobManager
  ├── GcsTaskManager
  └── GcsKVManager
  ↓
3. 注册所有 gRPC 服务 handler
  ↓
4. 启动 gRPC Server 监听（默认端口 6379）
  ↓
5. 启动后台定时任务（节点健康检查等）
```

---

#### 1.4.2 Raylet 内部架构

**关键文件**: `src/ray/raylet/raylet.h`

Raylet 是每个节点上的本地调度器，负责：
- 本地资源管理
- Worker 进程池管理
- 任务调度
- 对象管理

**核心组件**

| 组件 | 作用 | 关键文件 |
|------|------|---------|
| `NodeManager` | 节点资源管理、任务调度、Worker 生命周期 | `node_manager.h` |
| `WorkerPool` | Worker 进程池，管理空闲/忙碌的 Worker | `worker_pool.h` |
| `ClusterResourceScheduler` | 全局资源视图、节点选择 | `cluster_resource_scheduler.h` |
| `LocalResourceManager` | 本地资源核算 | `local_resource_manager.h` |
| `ObjectManager` | 对象传输、拉取/推送 | `object_manager.h` |
| `AgentManager` | Dashboard Agent 管理 | `agent_manager.h` |

**Worker 进程类型**

| 类型 | 作用 | 数量 |
|------|------|------|
| Python Worker | 执行普通任务/Actor | 动态增减 |
| Java Worker | 执行 Java 任务 | 动态增减 |
| CoreWorker | C++ 核心层（每个进程 1 个） | 每个进程 1 个 |
| Driver | 用户主进程（Python/Java） | 每个 Job 1 个 |

---

### 1.5 手动启动方式（调试进阶）

**适用场景**：底层调试、gdb 附加、单步追踪 C++ 代码。

---

#### 1.5.1 组件启动顺序与依赖

```
1. GCS Server (gcs_server)    ← 全局控制存储
    ↓
2. Raylet (raylet)            ← 包含 Plasma Store + Object Manager + Scheduler
    ↓
3. Python Client (ray.init()) ← 连接到 GCS 地址
```

**核心依赖关系**：

| 组件 | 必需参数 |
|------|---------|
| **GCS Server** | `--gcs_server_port`, `--session-name`, `--config_list` |
| **Raylet** | `--gcs-address`, `--raylet_socket_name`, `--plasma_store_name`, `--node-id` |
| **Python Client** | `gcs_address` |

---

#### 1.5.2 方案一：命令行手动启动（不推荐，仅供学习）

##### 步骤 1：启动 GCS Server

```bash
# 获取 gcs_server 可执行文件路径
GCS_SERVER=$(python -c "import ray; print(ray.__path__[0])")/core/src/ray/gcs/gcs_server

# 必需参数（最简配置）
$GCS_SERVER \
  --gcs_server_port=6379 \
  --node-ip-address=127.0.0.1 \
  --session-name=manual-session-$(date +%s) \
  --log_dir=/tmp/ray/logs \
  --config_list='[{"ClientServerPort": 0}]'
```

##### 步骤 2：启动 Raylet

```bash
# 获取 raylet 可执行文件路径
RAYLET=$(python -c "import ray; print(ray.__path__[0])")/core/src/ray/raylet/raylet

# 创建 session 目录（存放 socket 文件）
mkdir -p /tmp/ray/session_manual

# 启动 raylet（核心参数缺一不可）
$RAYLET \
  --gcs-address=127.0.0.1:6379 \
  --node-manager-port=6380 \
  --object-manager-port=6381 \
  --node-id=$(python -c "import uuid; print(uuid.uuid4().hex)") \
  --raylet_socket_name=/tmp/ray/session_manual/raylet_socket \
  --plasma_store_name=/tmp/ray/session_manual/plasma_socket \
  --temp-dir=/tmp/ray/session_manual \
  --log_dir=/tmp/ray/logs \
  --object-store-memory=1000000000  # 1GB 对象存储内存
```

##### 步骤 3：Python 客户端连接

```python
import ray

# 关键：指定 gcs_address 连接到手动启动的集群
ray.init(
    gcs_address="127.0.0.1:6379",
    # 多网卡环境需指定
    # node_ip_address="127.0.0.1"
)

# 验证连接
@ray.remote
def f():
    return "hello from manual cluster"

print(ray.get(f.remote()))
```

---

#### 1.5.3 方案二：Python API 启动（推荐用于调试）

使用 Ray 内部的 `services.py` 模块启动，自动处理参数：

```python
import ray._private.services as services
import tempfile
import os

# 1. 创建临时 session 目录
session_dir = tempfile.mkdtemp(prefix="ray_session_")
os.makedirs(os.path.join(session_dir, "logs"), exist_ok=True)

# 2. 启动 GCS Server
gcs_process = services.start_gcs_server(
    redis_address=None,              # 使用内置内存存储，无需外部 Redis
    log_dir=os.path.join(session_dir, "logs"),
    stdout_filepath=None,
    stderr_filepath=None,
    session_name="manual-session",
    gcs_server_port=6379,
    node_ip_address="127.0.0.1",
    config={}
)

print(f"GCS Server started, PID: {gcs_process.process.pid}")
print(f"GCS address: 127.0.0.1:6379")

# 3. 现在可以用 Python 连接进行调试
# import ray
# ray.init(gcs_address="127.0.0.1:6379")
```

---

#### 1.5.4 为什么手动启动很困难？

| 难点 | 说明 |
|------|------|
| **参数爆炸** | `start_raylet()` 需要 **30+ 个参数**，缺一个都无法启动 |
| **Socket 文件** | Raylet 需要 `raylet_socket` 和 `plasma_socket`，Python 客户端路径必须完全一致 |
| **Node ID** | 需要唯一的 hex ID，且各组件之间必须匹配 |
| **资源配置格式** | `resource_argument` 格式特殊（`CPU,4.0,GPU,0`） |
| **Worker Path** | 需要正确指定 worker 进程的 Python 入口文件路径 |
| **Fate Share 失效** | 手动启动失去父子进程绑定，退出时容易产生孤儿进程 |

---

#### 1.5.5 ✅ 最佳实践：别手动启动，用 `ray start`！

```bash
# 最简单：一行命令启动 Head 节点（推荐）
ray start --head --port=6379

# Python 直接连接
import ray
ray.init(address="auto")  # 或 ray.init(address="127.0.0.1:6379")
```

**自动化优势**：
- ✅ 自动生成所有参数和 socket 文件路径
- ✅ 正确配置日志、资源、端口分配
- ✅ 内置健康检查
- ✅ `ray stop` 一键清理无孤儿进程

> 💡 **调试技巧**：先用 `ray start --head` 正常启动，然后 `ps aux | grep gcs_server` 查看它实际用了哪些参数，这是学习参数配置最快的方式。

---

#### 1.5.6 各方式对比总结

| 启动方式 | 复杂度 | 推荐度 | 适用场景 |
|---------|--------|--------|---------|
| **`ray start --head`** | ⭐ | ✅ 强烈推荐 | 日常开发、生产环境 |
| **Python API (`services.py`)** | ⭐⭐⭐ | ⚠️ 仅限特殊场景 | 调试、定制化启动、测试 |
| **纯命令行手动** | ⭐⭐⭐⭐⭐ | ❌ 不推荐 | 仅用于学习底层机制、gdb 调试 |

---

## 2. ray remote 任务提交与执行

### 2.1 @ray.remote 完整流程

**关键文件**: `python/ray/_private/worker.py` (第 3552 行)

详细追踪 `@ray.remote` 装饰器从 Python 函数定义到任务提交、调度、执行、返回结果的完整调用链。

#### 2.1.1 阶段一：Python 语法层 - 装饰器

```python
@PublicAPI
def remote(*args, **kwargs) -> Union[RemoteFunction, ActorClass]:
    # 当用 @ray.remote 装饰一个函数或类时
    # 如果是函数 → 创建 RemoteFunction 对象
    # 如果是类 → 创建 ActorClass 对象
```

#### 2.1.2 阶段二：任务提交 - 调用 `.remote()`

```
函数.remote(args)
    ↓
1. 检查 worker 连接状态
    ↓
2. 函数序列化与导出（第一次调用时）
    ├── 创建函数描述符
    ├── 序列化函数（cloudpickle）
    └── 导出函数到 GCS
    ↓
3. 调用 core_worker.submit_task（C++ 边界）
```

#### 2.1.3 阶段三：C++ 层任务提交

**关键文件**: `src/ray/core_worker/task_submission/normal_task_submitter.h`

```cpp
class NormalTaskSubmitter {
public:
    void SubmitTask(TaskSpecification task_spec);
private:
    DependencyResolver resolver_;                  // 依赖解析器
    TaskManagerInterface &task_manager_;           // 任务管理器
    std::shared_ptr<RayletClientInterface> local_raylet_client_;
};
```

**提交流程**:
```
SubmitTask(task_spec)
  ↓
1. 任务加入待提交队列
  ↓
2. DependencyResolver 解析对象依赖
  ↓
3. 等待依赖就绪
  ↓
4. 向 Raylet 请求 worker 租约（RequestLease RPC）
  ↓
5. 获得租约后，远程调用目标 worker 执行任务
```

#### 2.1.4 阶段四：调度层 - Raylet 调度

```
RequestLease RPC → Raylet
  ↓
1. ClusterResourceScheduler 检查资源可用性
  ↓
2. 找到可用节点后，检查是否有匹配的 worker 进程
  ↓
3. 若没有现成 worker：启动新的 Python 进程
  ↓
4. 返回租约（包含目标 worker 的地址）
```

#### 2.1.5 完整流程架构图

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

### 2.2 Actor 创建完整流程

**概述**: Actor 是 Ray 的有状态计算抽象，创建流程涉及多个组件协同。

**调用链追踪**:

```
用户代码: @ray.remote class MyActor: pass
  ↓
1. Python 层: ActorClass 实例化 (python/ray/actor.py)
  ↓
2. 调用 .remote() 创建 Actor
  ↓
3. CoreWorker::CreateActor（C++ 边界）
  ↓
4. 向 GCS 发送 RegisterActorRequest → 见 [1.4.1 GCS Server]
  ↓
5. GcsActorManager 调度 Actor 到合适节点
  ├── 检查资源需求
  ├── 选择合适的 Raylet
  └── 记录 Actor 状态（PENDING → SCHEDULED）
  ↓
6. 目标 Raylet 创建 Worker 进程 → 见 [1.4.2 Raylet]
  ↓
7. Worker 进程中初始化 Actor
  ├── 反序列化 Actor 类
  ├── 执行 __init__ 方法
  └── 向 GCS 报告 Actor READY
  ↓
8. 返回 ActorHandle 给用户
```

**关键状态转换**:

```
PENDING (等待调度)
    ↓
SCHEDULED (已分配节点)
    ↓
CREATING (Worker 正在创建中)
    ↓
READY (可接收任务)
    ↓
DEAD (已死亡/退出)
```

---

## 3. 对象存储（Plasma）与外部存储集成

### 3.1 Plasma 对象存储基础架构

**关键文件**: `src/ray/object_manager/plasma/store.h`

Plasma 是 Ray 的**共享内存对象存储**，实现了高性能的跨进程数据共享：

```cpp
class PlasmaStore {
public:
    // 核心 API
    Status CreateObject(const ObjectID &object_id, int64_t data_size, int64_t metadata_size);
    Status SealObject(const ObjectID &object_id);    // 标记对象为只读
    Status DeleteObject(const ObjectID &object_id);
    
private:
    // 内存管理
    std::unique_ptr<Allocator> allocator_;           // 内存分配器
    std::unordered_map<ObjectID, PlasmaObject> objects_;
    
    // 客户端管理
    std::vector<std::unique_ptr<Client>> clients_;
    
    // 事件循环
    std::unique_ptr<epoll_event> events_;
};
```

#### 核心设计特点

| 特性 | 说明 |
|------|------|
| **共享内存** | 使用 `/dev/shm` 或 `mmap` 文件，跨进程零拷贝访问 |
| **不可变对象** | 对象一旦 Seal 就不可修改，确保安全并发 |
| **引用计数** | 每个对象被多少 Worker 引用，用于自动 Eviction |
| **内存映射** | 大对象通过 mmap 映射，避免物理内存复制 |

---

### 3.2 对象传输与本地/远程复制机制

**关键文件**: `src/ray/object_manager/object_manager.h`

ObjectManager 负责节点间对象的高效传输：

```cpp
class ObjectManager {
public:
    // Pull 流程 - 从远程节点获取对象
    uint64_t Pull(const std::vector<rpc::ObjectReference> &object_refs, ...);
    
    // Push 流程 - 将本地对象推送到远程节点
    void Push(const ObjectID &object_id, const NodeID &node_id);
    
private:
    PullManager pull_manager_;       // 拉取请求管理
    PushManager push_manager_;       // 推送请求管理
    ObjectBufferPool buffer_pool_;   // 对象缓冲池
    std::unique_ptr<ObjectStoreRunner> object_store_internal_;
};
```

#### 小对象 vs 大对象策略

```
┌─────────────────────────────────────────────────────────┐
│              对象传输策略选择                            │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  小对象 (< chunk_size)                                 │
│    ├─ 直接序列化传输                                    │
│    └─ 完整复制到目标节点 Plasma                         │
│                                                         │
│  大对象 (> chunk_size)                                 │
│    ├─ 分 chunk 传输 (默认 1MB)                         │
│    ├─ 边接收边写入 Plasma                               │
│    └─ 接收完所有 chunk 后 Seal                         │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

**chunk 传输实现** (`object_manager.h:315-339`):
- 使用 `ChunkObjectReader` 分块读取对象
- 每个 chunk 独立发送 gRPC 请求
- 接收端使用 `ObjectBufferPool` 管理写入缓冲

---

### 3.3 对象溢出（Spilling）机制详解

**关键文件**: `python/ray/_private/external_storage.py`

当 Plasma 内存不足时，Ray 会将冷对象**溢出**到外部存储。

#### 溢出触发条件

| 触发条件 | 说明 |
|---------|------|
| **内存不足** | Plasma 使用率达到阈值（默认 90%） |
| **对象不可用** | 所有本地引用已释放 |
| **LRU 选择** | 选择最近最少使用的对象 |

#### 完整溢出/恢复流程

```
溢出流程 (Plasma → 外部存储):
──────────────────────────────────
1. Raylet 检测到 Plasma 内存压力
   ↓
2. ObjectManager 选择可溢出对象（仅 pinned by raylet）
   ↓
3. 调用 spill_objects() 回调 → Python 层
   ├── 获取对象从 Plasma（零拷贝）
   ├── 序列化到外部存储文件
   └── 记录 URL 到 GCS
   ↓
4. 删除 Plasma 中的对象，释放内存

恢复流程 (外部存储 → Plasma):
──────────────────────────────────
1. Worker 访问被溢出的对象
   ↓
2. CoreWorker 检测对象不在 Plasma
   ↓
3. 向 GCS 查询 spilled URL
   ↓
4. 调用 restore_spilled_objects()
   ├── 从外部存储读取
   ├── 写回 Plasma 共享内存
   └── 继续任务执行
```

---

### 3.4 ExternalStorage 抽象与实现

**关键文件**: `python/ray/_private/external_storage.py:72`

```python
class ExternalStorage(metaclass=abc.ABCMeta):
    """外部存储抽象基类"""
    
    @abc.abstractmethod
    def spill_objects(self, object_refs, owner_addresses) -> List[str]:
        """将对象溢出到外部存储，返回 URL 列表"""
        
    @abc.abstractmethod
    def restore_spilled_objects(self, object_refs, url_list) -> int:
        """从外部存储恢复对象"""
        
    @abc.abstractmethod
    def delete_spilled_objects(self, urls: List[str]):
        """删除已溢出的对象"""
```

#### 内置实现

| 实现类 | 后端 | 特点 |
|--------|------|------|
| `FileSystemStorage` | 本地磁盘 | 默认实现，多目录轮询，最高性能 |
| `ExternalStorageSmartOpenImpl` | S3/GCS/... | 使用 `smart_open` 库，支持云端存储 |
| `NullStorage` | 无 | 禁用溢出（开发/测试） |

#### FileSystemStorage 实现细节

```python
# 多目录轮询 - 支持挂载多个磁盘
self._directory_paths = ["/disk1/ray_spill", "/disk2/ray_spill"]
self._current_directory_index = 0  # Round-Robin 选择

# 对象融合优化 - 多个小对象写入同一文件
def _write_multiple_objects(self, f, object_refs, ...):
    """将多个对象融合到一个文件，减少小文件开销"""
    # Header 格式: [data_size][metadata_size][owner_len][data...]
```

---

### 3.5 外部数据源集成：Ray Data Datasource

**关键文件**: `python/ray/data/_internal/datasource/sql_datasource.py`

对于**计算对象就是分布式存储数据**的场景（Hadoop、数据库、数据湖），Ray Data 提供了 Datasource 抽象，实现零拷贝读取。

#### SQLDatasource 并行读取架构

```python
class SQLDatasource(Datasource):
    """支持分片并行读取的 DB API2 数据源"""
    
    def supports_sharding(self, parallelism: int) -> bool:
        """检测数据库是否支持 MOD/ABS/CONCAT 分片操作"""
        
    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        """将 SQL 查询拆分为多个并行 ReadTask"""
```

#### 分片并行读取流程

```
┌─────────────────────────────────────────────────────────────┐
│                    SQL 数据库                                 │
│  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐                    │
│  │Shard0│  │Shard1│  │Shard2│  │Shard3│  ← MOD(哈希(shard_key))│
│  └───┬──┘  └───┬──┘  └───┬──┘  └───┬──┘                    │
└──────┼──────────┼──────────┼──────────┼──────────────────────┘
       │          │          │          │
       ▼          ▼          ▼          ▼
    ReadTask   ReadTask   ReadTask   ReadTask
    (Worker 0)  (Worker 1)  (Worker 2)  (Worker 3)
       │          │          │          │
       └──────────┴─────┬────┴──────────┘
                         │
                         ▼
                    Ray Dataset
                    (Blocks in Plasma)
```

**分片 SQL 生成**:
```sql
SELECT * FROM (
    {用户原始 SQL}
) as T 
WHERE MOD(ABS(MD5(shard_key)), {parallelism}) = {task_id}
```

---

### 3.6 绕过对象复制的三种策略

用户问题：**能否避免在 Ray 中进行高代价的对象复制？**

答案：**可以，Ray 提供了三层策略**：

| 策略层级 | 方案 | 适用场景 | 性能 |
|---------|------|---------|------|
| **存储层** | **零拷贝共享内存** | 单机多进程 | 🟢 最高 |
| **溢出层** | **远端直接读取** | 对象在其他节点 | 🟡 中 |
| **数据源层** | **Datasource 并行读取** | 外部系统（Hadoop/DB） | 🔵 原生 |

#### 策略一：Plasma 零拷贝（单机）

**工作原理**:
- 对象存储在 `/dev/shm` 共享内存
- Worker 进程通过 mmap 直接映射，**无物理内存复制**
- 小对象通过 Value Semantics 复制，大对象始终共享内存

**关键优化点**:
```cpp
// CoreWorker::Get() - 检测是否为本地 Plasma 对象
if (object.IsPlasmaBuffer()) {
    // 直接返回共享内存指针，零拷贝
    return buffer;
}
```

#### 策略二：分布式对象直接读取

**避免全量 Pull**:
```
传统方式 (Pull 全量):
    Worker A → 对象传输 → Worker B
    (高网络开销)

优化方式 (本地引用):
    Worker B → 检测对象在 Worker A
    Worker B → 向 Worker A 远程执行计算
    (仅传输任务，不传输数据)
```

**Ray 调度亲和性**:
- `ray.get()` 时，调度器会优先将任务调度到**对象所在节点**
- 减少网络传输，计算移动而不是数据移动

#### 策略三：外部数据源原生集成

**推荐架构**（针对 Hadoop/数据库场景）：

```
┌───────────────────────────────────────────────────────────────┐
│                    外部存储系统 (HDFS/S3/DB)                   │
│  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐              │
│  │ Block 0│  │ Block 1│  │ Block 2│  │ Block N│  数据分片     │
│  └───┬────┘  └───┬────┘  └───┬────┘  └───┬────┘              │
└──────┼───────────┼───────────┼───────────┼───────────────────┘
       │           │           │           │
       └───────────┼───────────┼───────────┘
                   │
       ┌───────────▼───────────┐
       │   Ray Data Datasource │  ← 自定义 Datasource
       └───────────┬───────────┘
                   │
       ┌───────────▼───────────┐
       │  并行 ReadTasks       │  ← 每个 Task 只读取所属分片
       └───────────┬───────────┘
                   │
       ┌───────────▼───────────┐
       │  计算 Task (map_batches) │ ← 计算在数据所在节点执行
       └───────────────────────┘
```

**关键实现**:
1. **自定义 Datasource**：继承 `Datasource` 类，实现分片逻辑
2. **计算亲和性**：通过 `NodeAffinityScheduleStrategy` 调度
3. **零序列化**：直接在 Reader 中进行 Arrow 格式转换

---

### 3.7 自定义外部存储开发指南

如需接入自定义外部存储（如 HDFS、Redis、自定义缓存等），可实现 `ExternalStorage` 接口：

```python
class MyCustomStorage(ExternalStorage):
    def spill_objects(self, object_refs, owner_addresses) -> List[str]:
        """实现自定义溢出逻辑"""
        pass
        
    def restore_spilled_objects(self, object_refs, url_list) -> int:
        """实现自定义恢复逻辑"""
        pass
```

典型场景：
- **内存数据库溢出**：Redis、Memcached
- **分布式文件系统**：HDFS、MinIO
- **对象存储服务**：阿里云 OSS、腾讯云 COS、AWS S3

---

## 附录：关键文件索引

### Python 层

| 文件 | 作用 | 章节引用 |
|------|------|---------|
| `python/ray/scripts/scripts.py` | CLI 入口，ray start 命令 | [1.1 入口层](#11-入口层scriptspy) |
| `python/ray/_private/node.py` | Node 类，进程编排器 | [1.2 进程编排层](#12-进程编排层nodepy) |
| `python/ray/_private/services.py` | C++ 进程启动包装 | [1.3 进程启动层](#13-进程启动层servicespy) |
| `python/ray/remote_function.py` | RemoteFunction 类 | [2.1 @ray.remote 完整流程](#21-rayremote-完整流程) |
| `python/ray/_private/external_storage.py` | 对象溢出外部存储 | [3.4 ExternalStorage 抽象与实现](#34-externalstorage-抽象与实现) |
| `python/ray/data/_internal/datasource/sql_datasource.py` | SQL 数据源并行读取 | [3.5 外部数据源集成](#35-外部数据源集成ray-data-datasource) |

### C++ 层

| 文件 | 作用 | 章节引用 |
|------|------|---------|
| `src/ray/gcs/gcs_server.h` | GCS Server 核心类 | [1.4.1 GCS Server 架构](#141-gcs-server-架构) |
| `src/ray/raylet/raylet.h` | Raylet 核心类 | [1.4.2 Raylet 内部架构](#142-raylet-内部架构) |
| `src/ray/raylet/node_manager.h` | 节点管理器 | [1.4.2 Raylet 内部架构](#142-raylet-内部架构) |
| `src/ray/core_worker/core_worker.h` | CoreWorker 核心类 | [2.1 @ray.remote 完整流程](#21-rayremote-完整流程) |
| `src/ray/object_manager/object_manager.h` | 对象传输管理器 | [3.2 对象传输与本地远程复制机制](#32-对象传输与本地远程复制机制) |
| `src/ray/object_manager/plasma/store.h` | Plasma 共享内存存储 | [3.1 Plasma 对象存储基础架构](#31-plasma-对象存储基础架构) |

---

*文档生成时间: 2024-04-25*
*重构时间: 2024-04-25 - 按调用链组织并建立交叉引用*
*新增章节时间: 2024-04-27 - 新增对象存储与外部存储集成*
*结构整理时间: 2024-04-27 - 统一数字编号，使用 [toc] 自动目录*
