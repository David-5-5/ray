# Ray 大规模部署架构分析

本文档系统梳理 Ray 在生产环境的多节点集群管理方案，对比 VM 和 K8s 两种主流部署范式。

---

[toc]

---

## 1. 部署架构总览

Ray 集群的拓扑结构非常简单：**一主多从**

```
┌─────────────┐
│  Head Node  │  ← GCS + Raylet + Dashboard
└──────┬──────┘
       │ gRPC
┌──────┴──────┐     ┌──────────────┐
│ Worker Node │ ... │ Worker Node  │  ← 只跑 Raylet + Worker
└─────────────┘     └──────────────┘
```

**所有 Worker 只需要知道 Head 节点的 GCS 地址即可加入集群**，这是 Ray 部署简单性的核心。

---

## 2. VM / 物理机部署方案

适用场景：传统 IDC、已有虚拟化基础设施、追求极致性能

### 2.1 方案一：Ray Autoscaler 原生方案（推荐）

**核心工具**：`ray up` 命令 + YAML 配置

#### 工作原理

```
ray up cluster.yaml
    ↓
1. Ansible 通过 SSH 连接到所有机器
    ↓
2. 在每台机器上执行启动脚本
    ├─ 安装/检查 Ray 版本
    ├─ 配置环境变量
    └─ 启动 ray start --head / --address
    ↓
3. Head 节点启动 Autoscaler 组件
    ↓
4. Autoscaler 根据任务负载自动增删 Worker
```

#### 最简配置示例

```yaml
# cluster.yaml
cluster_name: my-ray-cluster

max_workers: 100

# 节点启动命令
head_start_ray_commands:
    - ray stop
    - ulimit -n 65536; ray start --head --port=6379 --object-store-memory=10000000000

worker_start_ray_commands:
    - ray stop
    - ulimit -n 65536; ray start --address=$RAY_HEAD_IP:6379

# 节点列表（静态）
available_node_types:
    head_node:
        node_config:
            Hostname: 192.168.1.100
        resources: {"CPU": 32, "GPU": 0}
        max_workers: 0
    
    worker_nodes:
        node_config:
            Hostname: 192.168.1.101
        resources: {"CPU": 32, "GPU": 4}
        max_workers: 5
```

#### 支持的节点提供者

| Provider | 说明 |
|----------|------|
| **Local** | 单机伪分布式 |
| **Static** | 固定 IP 列表（传统 IDC） |
| **AWS** | EC2 自动伸缩 |
| **GCP** | GCE 自动伸缩 |
| **Azure** | VM 自动伸缩 |
| **Aliyun** | 阿里云 ECS |

#### 优点与局限

✅ **优点**：
- 零依赖，只需 SSH 访问
- 性能无损耗（无容器开销）
- 直接控制物理资源
- 支持自动扩缩容

❌ **局限**：
- 没有服务发现（依赖固定 IP）
- 没有健康检查自动恢复
- 日志/监控需要自己搭
- 滚动更新需要手动实现
- 多租户隔离困难

---

### 2.2 方案二：配置管理工具（Ansible / Puppet / Chef）

适合已有成熟配置管理体系的企业：

```
Ansible Playbook
    ↓
1. 批量下发 Ray 二进制
    ↓
2. 配置 systemd 服务
    ↓
3. 统一管理配置文件
    ↓
4. systemctl start ray-head / ray-worker
```

**systemd 单元示例**：
```ini
# /etc/systemd/system/ray-head.service
[Unit]
Description=Ray Head Node
After=network.target

[Service]
Type=simple
User=ray
ExecStart=/usr/local/bin/ray start --head --port=6379 --block
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

---

## 3. Kubernetes 部署方案（企业级推荐）

适用场景：云原生架构、需要弹性伸缩、多租户、服务治理

### 3.1 KubeRay Operator 架构

**官方标准组件**：

```
┌─────────────────────────────────────────────────────┐
│                   Kubernetes API                     │
└────────────┬────────────────────────────────────────┘
             │
┌────────────▼────────────────────────────────────────┐
│                KubeRay Operator                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │RayCluster│  │ RayJob   │  │RayService│         │
│  │   CRD    │  │   CRD    │  │   CRD   │         │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘         │
└───────┼──────────────┼──────────────┼──────────────┘
        │              │              │
        ▼              ▼              ▼
   ┌─────────┐    ┌─────────┐   ┌─────────┐
   │ Head    │    │  Job    │   │ Serve   │
   │ Pod     │    │  Pod    │   │  Pod    │
   └────┬────┘    └─────────┘   └─────────┘
        │
   ┌────▼────┐
   │ Worker  │
   │ Pods    │
   └─────────┘
```

### 3.2 三大核心 CRD

#### ① RayCluster - 长驻集群

```yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: ray-cluster
spec:
  headGroupSpec:
    rayStartParams:
      port: "6379"
      dashboard-host: 0.0.0.0
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:2.9.0
          resources:
            requests: {cpu: "8", memory: "32G"}
            
  workerGroupSpecs:
  - groupName: gpu-workers
    replicas: 3
    minReplicas: 0
    maxReplicas: 20
    rayStartParams:
      num-gpus: "4"
    template:
      spec:
        containers:
        - name: ray-worker
          image: rayproject/ray-ml:2.9.0-gpu
          resources:
            requests: {cpu: "32", memory: "128G", nvidia.com/gpu: 4}
```

#### ② RayJob - 一次性任务

```yaml
apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: training-job
spec:
  entrypoint: python train.py  # 任务入口
  shutdownAfterJobFinishes: true  # 跑完自动删集群
  rayClusterSpec:
    # 内嵌 RayCluster 配置...
```

#### ③ RayService - 高可用在线服务

```yaml
apiVersion: ray.io/v1
kind: RayService
metadata:
  name: my-model-serving
spec:
  serveConfigV2: |
    applications:
      - name: model
        import_path: serve.run:model
        route_prefix: /
  rayClusterSpec:
    # 内嵌 RayCluster 配置...
```

**关键能力**：
- ✅ 零停机滚动更新
- ✅ 自动故障转移
- ✅ 流量分配

---

### 3.3 K8s 部署的核心优势

| 能力 | 说明 |
|------|------|
| **自动恢复** | Head/Worker Pod 挂了自动重建 |
| **服务发现** | 用 DNS 名称连接，无需固定 IP |
| **弹性伸缩** | 基于资源使用率/自定义指标 HPA |
| **滚动更新** | 不中断业务更新 Ray 版本 |
| **多租户隔离** | Namespace + ResourceQuota |
| **日志/监控** | 标准 Prometheus + Grafana 体系 |
| **服务暴露** | Ingress / LoadBalancer 统一入口 |

---

### 3.4 K8s 部署的坑点与解决方案

| 问题 | 现象 | 解决方案 |
|------|------|---------|
| **GCS 失联** | Head Pod 重启后 IP 变了，Worker 连不上 | 用 **Headless Service** 固定 DNS 名称 |
| **对象丢失** | Worker Pod 重建后 Plasma 对象丢失 | 开启对象溢出到 PV / 避免节点级资源 |
| **端口冲突** | Ray 用大量随机端口 | 指定 `min-worker-port` / `max-worker-port` 范围 |
| **PV 挂载慢** | 大模型 checkpoint 加载慢 | 用本地 SSD + HostPath / LocalPV |
| **GPU 亲和性** | Pod 调度不到有 GPU 的节点 | 配置 nodeSelector + tolerations |

---

## 4. VM vs K8s 方案对比

| 维度 | VM / 物理机 | Kubernetes |
|------|------------|------------|
| **部署复杂度** | ⭐ 低（SSH 就行） | ⭐⭐⭐⭐ 高（需要 K8s 知识） |
| **运维成本** | ⭐⭐⭐⭐ 高（人肉运维） | ⭐⭐ 中（标准化） |
| **性能损耗** | ✅ 无额外开销 | ⚠️ 约 3-5% 容器开销 |
| **故障恢复** | ❌ 手动恢复 | ✅ 自动重建 |
| **弹性伸缩** | ⚠️ 有限支持 | ✅ 原生 HPA |
| **多租户** | ❌ 困难 | ✅ Namespace 隔离 |
| **服务治理** | ❌ 自建 | ✅ 完整生态 |
| **适合团队规模** | 小团队 / 10 节点以下 | 中大型团队 / 生产级 |

---

## 5. 企业级落地关键组件

### 5.1 必选组件

```
┌─────────────────────────────────────────────┐
│                Ray 集群                     │
│  ┌────────┐  ┌────────┐  ┌────────┐       │
│  │  Head  │  │ Worker │  │ Worker │  ...   │
│  └────┬───┘  └────┬───┘  └────┬───┘       │
└───────┼───────────┼───────────┼────────────┘
        │           │           │
        └───────────┼───────────┘
                    │
        ┌───────────┼───────────┐
        │           │           │
   ┌────▼────┐ ┌───▼────┐ ┌───▼──────┐
   │ 监控    │ │  日志  │ │   追踪   │
   │Prometheus│ │ELK/Loki│ │Jaeger/OTel│
   └─────────┘ └────────┘ └──────────┘
```

### 5.2 可选增强组件

| 组件 | 作用 |
|------|------|
| **Ray Dashboard** | 内置 Web UI，看任务、看资源 |
| **Ray Client** | 本地提交任务到远程集群 |
| **KubeRay CLI** | K8s 下的 ray 命令行替代 |
| **Volcano / Yunikorn** | 批调度器，优化 Gang Scheduling |
| **MLflow** | 实验跟踪 + 模型管理 |

---

## 6. 生产环境最佳实践

### 6.1 Head 节点高可用

```
⚠️ Ray Head 仍是单点（2.x 版本正在改进）

建议做法：
1. Head 节点部署在稳定节点，设置高优先级
2. 开启 GCS 持久化（Redis / MySQL 后端）
3. 定期备份 GCS 元数据
4. 业务层做重试逻辑
```

### 6.2 网络配置

```bash
# Ray 需要大量端口通信，防火墙至少开放：
- 6379  # GCS Server
- 6380  # Node Manager
- 6381  # Object Manager  
- 10000-20000  # Worker 端口范围
```

### 6.3 版本一致性

**绝对保证全集群 Ray 版本相同**，否则会出现诡异的序列化/反序列化问题：
- Docker Image 统一打包 Ray 版本
- VM 用 Ansible 批量更新
- 滚动更新期间禁止提交新任务

---

## 7. 选择建议决策树

```
开始
  │
  ├─ 你们有 K8s 运维能力吗？
  │   ├─ 有 → 直接上 KubeRay ✅
  │   └─ 没有 → 继续
  │
  ├─ 集群规模多大？
  │   ├─ <10 节点 → ray up + SSH 足够
  │   ├─ 10-50 节点 → Ansible + systemd 可撑
  │   └─ >50 节点 → 还是上 K8s 吧，否则运维累死
  │
  └─ 业务类型？
      ├─ 离线训练（7x24 长稳）→ K8s 自动恢复很重要
      ├─ 在线服务（Ray Serve）→ 必须 K8s，要高可用
      └─ 实验性/临时任务 → 怎么简单怎么来
```

---

## 8. 总结

| 方案 | 一句话总结 | 典型用户 |
|------|-----------|---------|
| **ray up + SSH** | 最快启动，零依赖 | 算法工程师、小团队、POC |
| **Ansible + systemd** | 传统 IDC 标准做法 | 企业运维团队 |
| **KubeRay** | 企业级生产部署唯一推荐 | 平台团队、中大型公司 |

> 💡 **历史趋势**：Ray 社区重心已全面转向 KubeRay，新功能（如 Ray 2.x GCS FT、RayService）优先在 K8s 落地。VM 方案逐渐退化为辅助/兼容模式。
