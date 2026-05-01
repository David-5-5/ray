# Mini-Ray: 极简分布式计算框架

用于学习 Ray 核心架构思想的简化实现，剥离了复杂的对象管理、资源调度、GCS 等，只保留最核心的分布式任务执行机制。

---

## 核心架构

```
┌─────────────────────────────────────────────────────┐
│              User Python Process (Driver)           │
│  ┌──────────┐      ┌─────────────────────────────┐ │
│  │@ray.remote│─────▶│  Task Table (in-memory)    │ │
│  └──────────┘      └──────────────┬──────────────┘ │
└────────────────────────────────────┼────────────────┘
                                     │ gRPC
┌────────────────────────────────────┼────────────────┐
│              Scheduler Server                       │
│  ┌────────────────┐     ┌───────────────────────┐  │
│  │  Task Queue    │────▶│  Worker Pool Manager  │  │
│  └────────────────┘     └──────────┬────────────┘  │
└─────────────────────────────────────┼───────────────┘
                                      │ gRPC
              ┌───────────────────────┼───────────────────────┐
              │                       │                       │
    ┌─────────▼─────────┐   ┌────────▼─────────┐    ┌─────────▼─────────┐
    │   Worker Process  │   │  Worker Process  │    │  Worker Process   │
    │  ┌─────────────┐ │   │  ┌─────────────┐  │    │  ┌─────────────┐  │
    │  │ Task Exec   │ │   │  │ Task Exec   │  │    │  │ Task Exec   │  │
    │  │ (Python)    │ │   │  │ (Python)    │  │    │  │ (Python)    │  │
    │  └─────────────┘ │   │  └─────────────┘  │    │  └─────────────┘  │
    └───────────────────┘   └───────────────────┘    └───────────────────┘
```

---

## 核心功能对比

| 功能 | Ray | Mini-Ray |
|------|-----|----------|
| @ray.remote 装饰器 | ✅ | ✅ |
| .remote() 任务提交 | ✅ | ✅ |
| ray.get() 获取结果 | ✅ | ✅ |
| 分布式 Worker 进程 | ✅ | ✅ |
| 简单调度 (Round Robin) | ✅ | ✅ |
| gRPC 通信 | ✅ | ✅ |
| Plasma 对象存储 | ✅ | ❌ |
| 复杂资源调度 (CPU/GPU) | ✅ | ❌ |
| GCS 全局状态管理 | ✅ | ❌ |
| Actor 有状态计算 | ✅ | ❌ |
| 对象序列化优化 | ✅ | ❌ |
| 自动扩缩容 | ✅ | ❌ |

---

## 快速开始

### 启动 Scheduler
```bash
python scheduler.py --port 50051
```

### 启动 Worker
```bash
python worker.py --scheduler localhost:50051
```

### 运行任务
```bash
python main.py
```

---

## 代码结构

```
mini-ray/
├── README.md              # 本文档
├── scheduler.py           # 调度器服务 (gRPC server)
├── worker.py              # Worker 执行进程
├── remote.py              # @ray.remote 装饰器
├── api.py                 # ray.init / ray.get API
├── main.py                # 使用示例
└── proto/                 # gRPC 协议定义
    └── scheduler.proto
```

---

## 调用流程详解

```python
# 1. 初始化
import ray
ray.init("localhost:50051")

# 2. 装饰远程函数
@ray.remote
def add(a, b):
    return a + b

# 3. 提交任务
future = add.remote(1, 2)
# └─→ Python 序列化函数+参数 → gRPC 发送到 Scheduler
#    └─→ Scheduler 选择可用 Worker → 发送任务
#       └─→ Worker 反序列化执行 → 返回结果

# 4. 获取结果
result = ray.get(future)  # = 3
```

---

## 设计思想

1. **KISS 原则**：能用简单实现的绝不复杂化
2. **关注点分离**：调度器只管分发，Worker 只管执行
3. **接口对齐**：API 完全对齐 Ray，便于理解后无缝切换到真实 Ray
4. **单文件模块**：每个核心概念一个文件，便于阅读和调试
