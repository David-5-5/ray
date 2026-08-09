# Ray Train 综述

本文档综合分析 Ray Train 的整体架构、用户 API、执行模型与后端生态，是入门与体系化理解的入口。深度专题（如 `TorchTrainer.fit()` 全链路）建议在通读本文后展开。

> **文档状态说明**：Ray Train 处于 v1→v2 迁移期。2.50 版本 v1 默认启用、v2 需显式开启（`RAY_TRAIN_V2_ENABLED=1`）。**本文以 v1 为主**（生产稳定），v2 在第 7 章做架构对比。

---

## 1. 定位与设计目标

Ray Train 是 Ray 生态的**分布式训练编排层**，构建在 Ray Core（task/actor）之上，与 Ray Data / Ray Serve / Ray Tune 平级。核心目标：

- **统一 API**——一个 `TorchTrainer` 包装 PyTorch DDP/单卡/Accelerate/DeepSpeed 等差异；
- **SPMD 数据并行**——把单机训练循环扩展到多 worker；
- **Ray Data 集成**——`get_dataset_shard()` 让训练 worker 拉取分片数据集；
- **容错与弹性**——检查点 + worker 失败重启 + 自动重调度；
- **后端无关**——同一 `BaseTrainer` 骨架适配 PyTorch / TensorFlow / JAX / Horovod / XGBoost / LightGBM。

```
Ray 生态层（应用库）
  ├─ Ray Data        分布式数据处理       ← Train 的数据来源
  ├─ Ray Train       分布式训练编排       ← 本文档
  ├─ Ray Tune        超参调优            ← Train 内部依赖 Tune
  ├─ Ray Serve       在线服务            ← 训练后模型部署
  ├─ Ray RLlib       强化学习
  └─ Ray LLM         LLM 训练/推理

底层支撑
  └─ Ray Core        task/actor/object  ← Train 的执行载体
```

---

## 2. 目录结构

`python/ray/train/` 子目录分工（2.50）：

| 目录 | 作用 |
|------|------|
| `base_trainer.py` | **BaseTrainer 抽象基类**（911 行）+ `_train_coordinator_fn`（连接 Tune 桥） |
| `data_parallel_trainer.py` | SPMD 数据并行基类 |
| `trainer.py` | `TrainingIterator` 等辅助类 |
| `torch/` `tensorflow/` `horovod/` `xgboost/` `lightgbm/` `jax/` | **6 个后端**各自的 Trainer 与工具 |
| `huggingface/` `lightning/` | **辅助工具**（无独立 Trainer） |
| `collective/` | Ray Collectives 通信层（backend） |
| `_internal/` | session、storage、data_config 等内部实现 |
| `examples/` | 30 个可运行示例（同时被测试复用） |
| `v2/` | **新架构实验性实现**——独立 controller/worker_group |
| `tests/` | 51 个 v1 测试 |

---

## 3. 用户三件套

Ray Train 入门只需掌握三组 API：

### 3.1 Trainer 入口

```python
from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig, RunConfig

trainer = TorchTrainer(
    train_loop_per_worker=train_func,    # 用户训练函数
    train_loop_config={...},            # 超参
    scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
    run_config=RunConfig(storage_path="s3://...", name="exp1"),
)
result = trainer.fit()
```

**继承链**：`TorchTrainer` → `DataParallelTrainer` → `BaseTrainer`（abc）。6 个后端 Trainer 都继承自 `DataParallelTrainer`，自身只做框架特定的 `prepare_data_loader` / `prepare_model` 等适配。

### 3.2 训练循环 API（worker 端用）

在 `train_func` 内部通过 `import ray.train as train` 访问的 session API：

| API | 作用 |
|---|---|
| `train.report(metrics, checkpoint=...)` | 报告本步指标与检查点给 Tune/driver |
| `train.get_context()` | 拿 `rank`/`world_rank`/`local_rank`/`world_size` 等分布式上下文 |
| `train.get_dataset_shard()` | 拿当前 worker 的 Ray Dataset 分片 |
| `train.get_checkpoint()` | 拿上一个 checkpoint（用于恢复训练） |
| `train.torch.prepare_data_loader(loader)` | 多 worker 时自动包 `DistributedSampler` |
| `train.torch.prepare_model(model)` | 多 GPU 时自动包 DDP / FSDP |

### 3.3 配置对象

| 对象 | 职责 | 关键字段 |
|---|---|---|
| `ScalingConfig` | 训练规模 | `num_workers`、`use_gpu`、`resources_per_worker`、`placement_strategy` |
| `RunConfig` | 运行配置 | `storage_path`、`name`、`stop`、`failure_config`、`checkpoints_config` |
| `CheckpointConfig` | 检查点策略 | `num_to_keep`、`checkpoint_frequency`（部分后端支持） |
| `FailureConfig` | 容错策略 | `max_failures` |
| `DataConfig` | 数据集配置 | datasets 注入工厂 |

---

## 4. 6 个后端 Trainer 一览

每个后端都是一个**继承 `DataParallelTrainer` 的具体 Trainer 类**，自身只重写少量框架相关方法：

| 后端 | Trainer 类 | 适配机制 | 文件行数 |
|---|---|---|---|
| **PyTorch** | `TorchTrainer` | `prepare_data_loader` + `prepare_model`（DDP/Accelerate） | 204 |
| **TensorFlow** | `TensorflowTrainer` | `MultiWorkerMirroredStrategy` | 194 |
| **JAX** | `JaxTrainer` | pjit 分布式 | — |
| **Horovod** | `HorovodTrainer` | Horovod all-reduce 风格 | 202 |
| **XGBoost** | `XGBoostTrainer` | Rabit/分布式梯度提升 | 318 |
| **LightGBM** | `LightGBMTrainer` | LightGBM 分布式 | 319 |

> **认知陷阱**：`huggingface/transformers/` 和 `lightning/_lightning_utils.py` 不是独立 Trainer——它们是**辅助工具**，实际训练仍走 `TorchTrainer` + 适配。同理 `examples/accelerate/` 和 `examples/deepspeed/` 是 `TorchTrainer` 的高级集成示例。

---

## 5. 执行模型（v1）

### 5.1 完整调用链

`TorchTrainer.fit()` 不直接跑训练，而是把 trainer 转成 Tune Trainable 委派给 Tune 框架：

```
用户脚本 (driver)                                Worker 节点
─────────────────────────────────────────         ─────────────────
TorchTrainer(train_func, scaling_config, ...)
   │
   ▼ BaseTrainer.fit()  [base_trainer.py:651]
   │
   ├─ as_trainable()  ──→ 生成 Tune Trainable 类
   │     │
   │     └─ wrap_function(_train_coordinator_fn)
   │           │
   │           └─ _train_coordinator_fn  [base_trainer.py:92]
   │                 │
   │                 ├─ trainer = trainer_cls(**config)    # worker 端重建 trainer
   │                 ├─ trainer.setup()                     # 用户 setup 钩子
   │                 └─ trainer.training_loop()             # SPMD 入口
   │                       │
   │                       └─ DataParallelTrainer.training_loop
   │                             │
   │                             └─ 起 worker group（Ray actor pool）
   │                                   │
   │                                   └─ 每个 worker 跑 train_func(config)
   │                                         │
   │                                         ├─ prepare_data_loader / prepare_model
   │                                         ├─ 训练循环
   │                                         └─ train.report(metrics, checkpoint=...)
   ▼
Tuner.fit()  ← 由 Tune 驱动执行
   │
   ▼
ResultGrid  ← Tune 汇总 worker report
   │
   ▼
fit() 返回 Result（metrics + checkpoint）
```

**关键设计**：`BaseTrainer.fit()` 不直接调训练——它把 trainer **包成 Tune Trainable** 交给 Tune。这是为什么 Ray Train 必须依赖 `ray.tune`。

### 5.2 关键类的角色

| 类/方法 | 位置 | 角色 |
|---|---|---|
| `BaseTrainer.__init__()` | `base_trainer.py:240-256` | 存配置（不动任务） |
| `BaseTrainer.fit()` | `base_trainer.py:651-720` | 调 `as_trainable` + `Tuner.fit()` |
| `BaseTrainer.as_trainable()` | `base_trainer.py:903-907` | Trainer → Tune Trainable |
| `_generate_trainable_cls()` | `base_trainer.py:777-825` | 调 `wrap_function(_train_coordinator_fn)` |
| **`_train_coordinator_fn`** | **`base_trainer.py:92-117`** | **真正在 remote actor 跑**：重建 trainer → `setup` → `training_loop` |
| `DataParallelTrainer.training_loop` | `data_parallel_trainer.py` | 起 worker group + 分发 `train_func` |

### 5.3 Session 机制

`train.report` / `get_context` / `get_checkpoint` / `get_dataset_shard` 全部走 session 单例（`_internal/session.py`，1199 行）：

- worker 启动时 `_train_coordinator_fn` 调用 `get_session()` 初始化；
- worker 内所有 `train.*` 调用实际是访问 session 的全局上下文；
- session 内部把 `report` 数据通过 Ray actor 通信回 driver/Tune。

### 5.4 数据流：Train × Data 集成

```
Ray Dataset                          train_func 内
─────────────                        ─────────────
get_dataset_shard("train")   →       shard = get_dataset_shard()
                                      ├─ shard.iter_torch_batches(...)  ← Ray Data 端分片
                                      └─ batch 直接喂给 model
```

Ray Dataset 在 `ScalingConfig(num_workers=N)` 下**自动按 N 分片**，每 worker 拿到独立子集（无需用户写 `DistributedSampler`）。

### 5.5 序列化要求

`train_func` 必须可 cloudpickle 序列化（[基础测试验证](python/ray/train/tests/test_torch_trainer.py:327)）。常见陷阱：
- 闭包捕获 `threading.Lock` / `socket` / DB connection → 报错并打印 `inspect_serializability` 诊断；
- 不可 pickle 的 `lambda` 默认能过（cloudpickle 支持）但 `lambda` 本身若是闭包会失败。

---

## 6. 后端封装机制：三层抽象 + 钩子模式

> 重点：本章先讲 6 后端如何被统一封装，再聚焦 PyTorch 详谈引入 Ray 前后的异同。

### 6.1 统一抽象：Backend / BackendConfig / BackendExecutor

5 个 v1 后端（**torch / tensorflow / horovod / xgboost / lightgbm**）都遵循**同一套三层抽象**——这是 Ray Train 的核心架构设计：

```
DataParallelTrainer          ← SPMD 入口（起 worker group、调度）
   └─ BackendExecutor        ← 统一执行引擎（资源分配、actor pool、报告回流）
        └─ Backend (抽象基类)  ← 4 个生命周期钩子：on_start / on_training_start / on_training_end / on_shutdown
             └─ BackendConfig   ← 描述符：返回 backend_cls() + train_func_context()
```

**关键源码**：

- `Backend`（[backend.py:32-45](python/ray/train/backend.py#L32)）：只定义 4 个 `on_*` 钩子方法签名，不感知任何框架；
- `BackendConfig`（[backend.py:16-30](python/ray/train/backend.py#L16)）：`backend_cls` 属性 + `train_func_context` 方法；
- `BackendExecutor`（`_internal/backend_executor.py:73-230`）：负责 worker group 启动、资源共享（Cuda visible devices 等）、报告聚合。

各后端只需实现 `_TorchBackend`/`_TensorflowBackend` 等具体类继承 `Backend`，**所有"非框架特化"逻辑**（worker 管理、容错、checkpoint 同步、报告回流）由 `BackendExecutor` 统一处理。

### 6.2 5 个后端 Backend 与 Config 实现一览

| 后端 | Config 类（继承 `BackendConfig`） | Backend 类（继承 `Backend`） | `on_start` 实际做的事 |
|---|---|---|---|
| **PyTorch** | `TorchConfig` | `_TorchBackend` | 设 `MASTER_ADDR`/`MASTER_PORT` → 调 `torch.distributed.init_process_group` |
| **TensorFlow** | `TensorflowConfig` | `_TensorflowBackend` | 设置 `TF_CONFIG` JSON → 触发 `MultiWorkerMirroredStrategy` |
| **Horovod** | `HorovodConfig` | `_HorovodBackend` | 调 `hvd.init()` |
| **XGBoost** | `XGBoostConfig` | `_XGBoostRabitBackend` | 启动 Rabit tracker（分布式梯度提升） |
| **LightGBM** | `LightGBMConfig` | `_LightGBMBackend` | 设置网络参数（`get_network_params`） |
| **JAX** | *（仅 v2 有 `JaxTrainer`，v1 无）* | — | — |

**PyTorch 后端 `on_start` 关键代码**（[torch/config.py:160-208](python/ray/train/torch/config.py#L160)）：

```python
class _TorchBackend(Backend):
    def on_start(self, worker_group, backend_config):
        if dist.is_available():
            backend = backend_config.backend or ("nccl" if num_gpus > 0 else "gloo")
            master_addr, master_port = worker_group.execute_single(0, get_address_and_port)
            # ... 设 env / init_method ...
            # 所有 worker 并行调 init_process_group
            for i in range(len(worker_group)):
                worker_group.execute_single_async(i, _setup_torch_process_group,
                    backend=backend, world_rank=i, world_size=len(worker_group), init_method=url)
            ray.get(setup_futures)
```

**本质**——PyTorch 用户原本要写的 `init_process_group` 调用，被 `_TorchBackend.on_start` 在**所有 worker actor 上并行**完成。DataParallelTrainer 完全不感知 PyTorch 存在。

### 6.3 PyTorch 异同：引入 Ray 前后的精确对比

#### 6.3.1 关键差异表

| 维度 | 原生 PyTorch DDP | Ray Train PyTorch | 谁负责 |
|---|---|---|---|
| 进程启动 | `torchrun --nproc_per_node=N` | `TorchTrainer(scaling_config=ScalingConfig(num_workers=N)).fit()` | Ray |
| 进程组初始化 | 手动 `init_process_group(backend, init_method)` | `_TorchBackend.on_start` 自动 | Ray |
| 数据分片 | 手动 `DistributedSampler` 包装 | `prepare_data_loader` 自动加 | Ray |
| 模型包装 | 手动 `DDP(model, device_ids=[rank])` | `prepare_model` 自动 DDP/FSDP | Ray |
| 设备迁移 | 手动 `model.to(rank)` + `data.to(device)` | `prepare_model`/`prepare_data_loader` 自动 | Ray |
| 分布式上下文 | `dist.get_rank()` 等 | `train.get_context().get_*()` session API | 都提供 |
| rank 0 检测 | `dist.get_rank() == 0` | `train.get_context().get_world_rank() == 0` | 习惯切换 |
| 报告/检查点 | 自定义保存 | `train.report(metrics, checkpoint=...)` | Ray 提供 |
| 数据源 | 任意自管 | `get_dataset_shard()` 自动分片 Ray Dataset | Ray |
| 容错 | SLURM/K8s 外部接管 | `FailureConfig(max_failures=N)` 自动重试 | Ray |

#### 6.3.2 代码对比：原生 DDP vs Ray Train

**原生 PyTorch DDP**（用户需写）：

```python
def main():
    dist.init_process_group(backend="nccl")
    rank = dist.get_rank()
    world_size = dist.get_world_size()
    sampler = DistributedSampler(dataset) if world_size > 1 else None
    loader = DataLoader(dataset, sampler=sampler)
    model = MyModel().to(rank)
    model = DDP(model, device_ids=[rank])
    for epoch in range(epochs):
        if sampler is not None:
            sampler.set_epoch(epoch)              # 多 worker 必须
        for X, y in loader:
            X, y = X.to(rank), y.to(rank)        # 手动迁设备
            optimizer.zero_grad()
            loss = criterion(model(X), y)
            loss.backward()
            optimizer.step()
        if rank == 0 and epoch % 10 == 0:
            torch.save(model.state_dict(), f"ckpt_{epoch}.pt")
```

**Ray Train 上的同样逻辑**（[torch_linear_example.py](python/ray/train/examples/pytorch/torch_linear_example.py)）：

```python
def train_func(config):
    loader = train.torch.prepare_data_loader(loader)      # 自动 DistributedSampler
    model = train.torch.prepare_model(model)              # 自动 DDP + device
    for epoch in range(epochs):
        if train.get_context().get_world_size() > 1:      # 仍要 set_epoch
            loader.sampler.set_epoch(epoch)
        for X, y in loader:                              # 无需 X.to(device)
            optimizer.zero_grad()
            loss = criterion(model(X), y)
            loss.backward()
            optimizer.step()
        train.report({"loss": loss},
                     checkpoint=Checkpoint.from_directory(tmpdir))
```

#### 6.3.3 "几乎没变"的部分

- **模型定义**（`class MyModel(nn.Module)`）完全不变；
- **训练循环结构**（epoch + for batch）完全不变；
- **`DistributedSampler.set_epoch()` 仍要手动调**——这是 PyTorch 自身硬性要求（[官方文档](https://pytorch.org/docs/stable/data.html#torch.utils.data.distributed.DistributedSampler) 明确要求），Ray 不应也无法隐藏它；
- **单 GPU/单 worker 训练代码完全相同**——Ray 透明切换单/多 worker，`prepare_model`/`prepare_data_loader` 在 `world_size==1` 时**不包装 DDP、不加 DistributedSampler**。

#### 6.3.4 新增但要学的 Ray 概念

- **`get_dataset_shard()`**——从 Ray Dataset 拿本 worker 的数据分片（Train × Data 集成入口）；
- **`train.report(metrics, checkpoint=...)`**——把本步指标+检查点回传 driver/Tune（替代 `torch.save` + 外部记录）；
- **`train.get_checkpoint()`**——拿上一个 checkpoint 用于恢复（容错的关键）；
- **`ScalingConfig` / `RunConfig` / `FailureConfig`**——把"训练规模、存储、容错"外置为 config，与训练逻辑解耦；
- **资源即代码**——`num_gpus=0.5`（每 worker 半张 GPU）、`resources_per_worker={"XPU": 2}` 等成为声明式配置。

### 6.4 实践要点

- **优先用 `train.torch.prepare_model`/`prepare_data_loader`**——几乎所有样板都消除了；
- **`set_epoch` 不要忘**——多 worker 时它是 PyTorch 正确 shuffle 的硬性要求；
- **report/checkpoint 与 PyTorch 原生 save 的区别**——`report` 走 actor 通信回 driver 并触发 Tune 汇总；`torch.save` 是本地落盘，两条路径并存时注意状态一致；
- **JaxTrainer 仅 v2 存在**——如需 JAX 训练建议开 `RAY_TRAIN_V2_ENABLED=1` 或直接用 `TorchTrainer` + jax2torch 桥。

---

## 7. 测试与示例

### 7.1 测试规模

| 类别 | 数量 | 位置 |
|---|---|---|
| v1 测试 | 51 个 | `python/ray/train/tests/` |
| v2 测试 | 37 个 | `python/ray/train/v2/tests/` |
| examples 脚本 | 30 个 | `python/ray/train/examples/` |

### 7.2 关键入门测试

`test_torch_trainer.py`（17 个测试项，11 函数 parametrize 展开）覆盖五大能力面：

- **A. 核心训练闭环**：`test_torch_linear[1/2]`——最小可运行范式
- **B. Checkpoint 三粒度**：`test_torch_e2e[_state_dict][_dir]`——`from_model`/`from_state_dict`/`from_directory`
- **C. 分布式上下文**：`_env_vars` `_get_device_gpu`——DDP 所需 rank/world_size 注入
- **D. 混合精度 AMP**：`test_torch_amp[_with_custom_get_state]`
- **E. 容错与校验**：`test_single_worker_failure` `test_*_errors` `test_nonserializable`

### 7.3 examples 目录三重身份

`python/ray/train/examples` 不是单纯的"示例"：

1. **用户样例**——按框架（pytorch/tf/horovod/...）组织的可运行教学代码；
2. **测试夹具**——其中 `train_func` 被 10+ 正式测试 import 复用（如 `test_torch_linear` 直接用 `torch_linear_example.train_func`）；
3. **CI 回归**——通过 `BUILD.bazel` 注册为 Bazel test target（`tests from the examples directory`）。

这种"示例即测试"设计保证**文档示例永远可运行**。

---

## 8. v1 vs v2 架构对比

### 7.1 状态

| 维度 | v1 | v2 |
|---|---|---|
| 默认启用 | ✅ `RAY_TRAIN_V2_ENABLED` 默认 `False` 即走 v1 | ❌ 需显式开 `RAY_TRAIN_V2_ENABLED=1` |
| 成熟度 | 生产稳定 | 实验性、有迁移警告（[issue 49454](https://github.com/ray-project/ray/issues/49454)） |
| 测试规模 | 51 个 | 37 个 |
| 文档 | 全部基于 v1 | 文档未迁移 |
| 6 个后端支持 | ✅ | ✅（v2 独立实现） |

### 7.2 架构差异

```
v1 执行路径                              v2 执行路径
─────────────                            ─────────────
BaseTrainer.fit()                        controller/controller.py
   ↓                                        ↓
as_trainable()                           起 worker group
   ↓                                        ↓
wrap_function(_train_coordinator_fn)    每个 worker 调 train_func
   ↓                                        ↓
Tuner 驱动 (ray.tune)                   自有 controller 驱动（非 Tune）
   ↓                                        ↓
→ worker group 起 actor                 → worker group 起 actor
   → 跑 train_func                         → 跑 train_func
```

**本质差异**：v1 把"运行调度"委派给 Tune；v2 引入**独立 controller**（`v2/_internal/execution/controller/controller.py`）直接调度 worker group。

### 7.3 v2 新增核心组件

`python/ray/train/v2/_internal/execution/`：

| 模块 | 作用 |
|---|---|
| `controller/` | 独立调度器（不再依赖 Tune） |
| `worker_group/` | 抽象的 worker 组管理（弹性扩缩） |
| `scaling_policy/` | 弹性伸缩策略 |
| `failure_handling/` | 容错与重启 |
| `training_report.py` | 报告系统重构 |
| `checkpoint/` | 检查点抽象 |
| `callbacks/` | 用户回调 |

### 7.4 实践建议

> **2.50 阶段**——以 **v1 为生产**，深入学 v1；**v2 仅作架构了解**，新项目不建议冒进。**待 v2 公告 GA 后**（issue 49454）再考虑迁移。

---

## 9. 关键源码索引

| 内容 | 位置 |
|---|---|
| BaseTrainer 抽象基类 | `python/ray/train/base_trainer.py`（911 行） |
| **`_train_coordinator_fn`**（连接 Tune 桥） | `python/ray/train/base_trainer.py:92-117` |
| `BaseTrainer.fit()` | `python/ray/train/base_trainer.py:651` |
| `as_trainable()` | `python/ray/train/base_trainer.py:903` |
| DataParallelTrainer 基类 | `python/ray/train/data_parallel_trainer.py` |
| 6 个后端 Trainer | `python/ray/train/{torch,tensorflow,horovod,xgboost,lightgbm,jax}/` |
| Session / report / get_context | `python/ray/train/_internal/session.py`（1199 行） |
| Checkpoint | `python/ray/train/_checkpoint.py` |
| 训练示例 | `python/ray/train/examples/`（30 个） |
| v2 控制器 | `python/ray/train/v2/_internal/execution/controller/` |
| v2 worker group | `python/ray/train/v2/_internal/execution/worker_group/` |
| 配置对象（ScalingConfig 等） | `python/ray/air/config.py`（Train 复用 Air） |
| 文档 | `doc/source/train/`（getting-started-pytorch 等 9 个 .rst） |
| 关键入门测试 | `python/ray/train/tests/test_torch_trainer.py` |
| v2 默认开关 | `python/ray/train/v2/_internal/constants.py:120` |
| 迁移警告 | `python/ray/train/constants.py:57-71` |

---

## 附录 A：与 Ray 其他库的关系

- **Train × Data**——`get_dataset_shard()` 是 Train 与 Data 的桥梁；Data 端按 worker 数自动分片；
- **Train × Tune**——v1 内部依赖 Tune（`wrap_function` + `Tuner.fit()`）；Tune 负责 trial 管理、调度、报告汇总；
- **Train × Serve**——训练完成后用 `TorchPredictor` 部署到 Serve（参见 `test_e2e_dir`）；
- **Train × LLM**——`ray.llm` 中 `ray.data.llm` 的 vLLM EMBED 任务用 Train 的分布式框架跑批量 embedding。

## 附录 B：常见操作清单

```python
# 最小可运行范式（v1）
from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig

trainer = TorchTrainer(
    train_loop_per_worker=lambda cfg: your_loop(cfg),
    train_loop_config={"lr": 1e-3, "epochs": 10},
    scaling_config=ScalingConfig(num_workers=4, use_gpu=True),
)
result = trainer.fit()
print(result.metrics)            # 最新一次 report 的 metrics
print(result.checkpoint)          # 最终 checkpoint

# worker 端
import ray.train as train
def train_func(config):
    model = train.torch.prepare_model(YourModel())
    loader = train.torch.prepare_data_loader(your_loader)
    for epoch in range(config["epochs"]):
        # ... training ...
        train.report({"loss": loss}, checkpoint=Checkpoint.from_directory(...))
```

## 附录 C：版本与配置速查

- **当前版本**：2.50.0
- **v1 默认开**（无需环境变量）
- **v2 开启**：`export RAY_TRAIN_V2_ENABLED=1`
- **抑制迁移警告**：`export RAY_TRAIN_ENABLE_V2_MIGRATION_WARNINGS=0`
- **Train Metrics 开启**：`export RAY_TRAIN_METRICS_ENABLED=1`
- **后端依赖**（extras）：`ray[train]` 默认装（依赖 `ray[data]` + `ray[tune]`）；XGBoost/LightGBM/Horovod 各自 extra。
