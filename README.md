# Crawler Autoscaler

一个基于 Kafka Consumer Lag 的轻量级 Docker Worker 自动伸缩器。

## 📖 项目简介

Crawler Autoscaler 是一个用 Go 编写的自动伸缩系统，专门用于根据 Kafka Consumer Group 的消息积压（Lag）动态调整 Worker 容器数量。

### 核心特性

- 🔍 **实时监控**：监控 Kafka Consumer Group 的消息积压情况
- 🎯 **智能决策**：基于阈值和冷却时间的伸缩算法
- 🐳 **Docker 集成**：动态创建和销毁 Worker 容器（开发中）
- 🛡️ **防抖设计**：避免频繁扩缩容导致的系统抖动
- 📊 **可配置化**：所有参数通过 YAML 配置文件管理

---

## 🏗️ 系统架构

```
                 ┌──────────────┐
                 │   Scheduler  │
                 └──────┬───────┘
                        │
                        ▼
                 ┌──────────────┐
                 │    Kafka     │
                 └──────┬───────┘
                        │
                 Consumer Group
                        │
                        ▼
              ┌───────────────────┐
              │    Autoscaler     │
              │                   │
              │ 1. Get Kafka Lag  │
              │ 2. Calculate      │
              │ 3. Decide         │
              │ 4. Scale          │
              └─────────┬─────────┘
                        │
                  Docker API
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
      worker-1      worker-2      worker-N
```

---

## 🚀 快速开始

### 前置要求

- Go 1.21+
- Kafka 2.8+
- Docker Engine（用于实际扩缩容）

### 安装依赖

```bash
go mod download
```

### 配置文件

编辑 `config.yaml`：

```yaml
app:
  name: crawler-autoscaler
  environment: dev

kafka:
  brokers:
    - localhost:9092
  topic: football_tasks
  consumer_group: football-worker

autoscaler:
  min_workers: 2
  max_workers: 20
  tasks_per_worker_per_minute: 500
  scale_up_threshold: 1000
  scale_down_threshold: 200
  scale_up_cooldown: 120s
  scale_down_cooldown: 300s
  check_interval: 10s
```

### 运行

```bash
go run ./cmd/autoscaler
```

---

## 📁 项目结构

```
crawler-autoscaler/
├── cmd/
│   └── autoscaler/
│       └── main.go              # 程序入口，控制循环
│
├── internal/
│   ├── config/
│   │   └── config.go            # 配置文件加载
│   │
│   ├── kafka/
│   │   └── monitor.go           # Kafka Lag 监控
│   │
│   ├── scaler/
│   │   └── scaler.go            # 伸缩决策引擎
│   │
│   └── docker/
│       └── manager.go           # Docker 容器管理（待实现）
│
├── config.yaml                   # 配置文件
├── go.mod
└── README.md
```

---

## 🔧 核心模块

### 1. Kafka Monitor

负责连接 Kafka 并获取 Consumer Group 的消息积压（Lag）。

**核心指标：**
- `Latest Offset`：Topic 中最新的消息偏移量
- `Consumer Offset`：Consumer Group 当前消费到的偏移量
- `Lag = Latest Offset - Consumer Offset`

**代码示例：**

```go
monitor := kafkamonitor.NewMonitor(brokers, topic, consumerGroup)
lagInfo, err := monitor.GetLag()

fmt.Printf("Total Lag: %d\n", lagInfo.TotalLag)
```

### 2. Scaler

根据 Kafka Lag 计算期望的 Worker 数量，并做出扩缩容决策。

**伸缩算法：**

```
期望 Worker 数量 = ceil(Total Lag / 单 Worker 处理能力)
```

**防抖机制：**

| 机制 | 说明 |
|------|------|
| **扩容阈值** | 只有当 Lag > 1000 时才考虑扩容 |
| **缩容阈值** | 只有当 Lag < 200 时才考虑缩容 |
| **扩容冷却** | 扩容后 120 秒内不再次扩容 |
| **缩容冷却** | 缩容后 300 秒内不再次缩容 |
| **最小 Worker** | 至少保持 2 个 Worker |
| **最大 Worker** | 最多扩容到 20 个 Worker |

**决策流程：**

```go
decision := scaler.Evaluate(lag)

switch decision.Action {
case ActionScaleUp:
    // 扩容
case ActionScaleDown:
    // 缩容
case ActionNone:
    // 保持不变
}
```

### 3. Docker Manager（待实现）

负责与 Docker Engine 交互，动态创建和销毁 Worker 容器。

**核心功能：**
- `ListWorkers()` - 列出当前运行的 Worker
- `ScaleUp(n)` - 启动 n 个新 Worker
- `ScaleDown(n)` - 优雅停止 n 个 Worker

---

## 📊 运行示例

### 正常运行（Lag 低）

```
2026/09/07 04:03:18 [Autoscaler] Starting crawler-autoscaler (dev)
2026/09/07 04:03:18 [Config] Worker Range: 2 - 20
2026/09/07 04:03:18 [Config] Scale Up Threshold: 1000
2026/09/07 04:03:18 [Config] Scale Down Threshold: 200
2026/09/07 04:03:18 [Autoscaler] Control loop started
2026/09/07 04:03:19 [Metrics] Lag=1 | Current=2 | Desired=2 | Action=none | Reason=current workers optimal
```

### 扩容场景（Lag 高）

```
2026/09/07 04:05:29 [Metrics] Lag=3500 | Current=2 | Desired=7 | Action=scale_up | Reason=lag exceeds threshold, scaling up
2026/09/07 04:05:29 [Decision] scale_up: 2 -> 7 workers
2026/09/07 04:05:29 [Applied] Workers updated to 7
```

### 缩容场景（Lag 低）

```
2026/09/07 04:10:49 [Metrics] Lag=150 | Current=7 | Desired=2 | Action=scale_down | Reason=lag below threshold, scaling down
2026/09/07 04:10:49 [Decision] scale_down: 7 -> 2 workers
2026/09/07 04:10:49 [Applied] Workers updated to 2
```

### 冷却期间（防抖）

```
2026/09/07 04:06:00 [Metrics] Lag=4000 | Current=7 | Desired=8 | Action=none | Reason=scale up cooldown period
```

---

## ⚙️ 配置说明

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `min_workers` | 最小 Worker 数量 | 2 |
| `max_workers` | 最大 Worker 数量 | 20 |
| `tasks_per_worker_per_minute` | 单 Worker 每分钟处理任务数 | 500 |
| `scale_up_threshold` | 扩容 Lag 阈值 | 1000 |
| `scale_down_threshold` | 缩容 Lag 阈值 | 200 |
| `scale_up_cooldown` | 扩容冷却时间 | 120s |
| `scale_down_cooldown` | 缩容冷却时间 | 300s |
| `check_interval` | 检查间隔 | 10s |

---

## 🎯 伸缩算法详解

### 基本公式

```
期望 Worker 数量 = ceil(Total Lag / tasks_per_worker_per_minute)
```

### 示例计算

假设配置：
- `tasks_per_worker_per_minute = 500`
- `min_workers = 2`
- `max_workers = 20`

| Kafka Lag | 计算 | 期望 Worker |
|-----------|------|-------------|
| 100 | ceil(100/500) = 1 | 2（最小值限制） |
| 1000 | ceil(1000/500) = 2 | 2 |
| 2500 | ceil(2500/500) = 5 | 5 |
| 5000 | ceil(5000/500) = 10 | 10 |
| 15000 | ceil(15000/500) = 30 | 20（最大值限制） |

### 扩容决策条件

扩容需要**同时满足**以下条件：

1. ✅ `期望 Worker > 当前 Worker`
2. ✅ `Lag > scale_up_threshold`
3. ✅ `距离上次扩容 > scale_up_cooldown`

### 缩容决策条件

缩容需要**同时满足**以下条件：

1. ✅ `期望 Worker < 当前 Worker`
2. ✅ `Lag < scale_down_threshold`
3. ✅ `距离上次缩容 > scale_down_cooldown`

---

## 🔐 线程安全

Scaler 使用 `sync.RWMutex` 保证并发安全：

```go
type Scaler struct {
    mu sync.RWMutex
    // ...
}

func (s *Scaler) Evaluate(lag int64) *Decision {
    s.mu.Lock()
    defer s.mu.Unlock()
    // ...
}
```

---

## 🚧 开发路线图

### ✅ 已完成

- [x] 项目基础结构
- [x] Config 模块（YAML 配置）
- [x] Kafka Monitor（Lag 监控）
- [x] Scaler 决策引擎
- [x] 控制循环和优雅退出

### 🔨 进行中

- [ ] Docker Manager 实现
  - [ ] 连接 Docker Engine
  - [ ] 通过 Label 识别 Worker
  - [ ] 扩容：动态创建 Container
  - [ ] 缩容：优雅停止 Container

### 📝 待开发

- [ ] Worker 优雅退出机制
  - [ ] Worker 响应 SIGTERM
  - [ ] 停止消费新任务
  - [ ] 等待当前任务完成
  - [ ] 提交 Kafka Offset

- [ ] 监控和指标
  - [ ] Prometheus Metrics
  - [ ] Grafana Dashboard
  - [ ] 日志结构化（JSON 格式）

- [ ] 高级特性
  - [ ] 根据实际吞吐动态调整 `tasks_per_worker_per_minute`
  - [ ] 支持多 Consumer Group 管理
  - [ ] 支持多种伸缩策略（不仅基于 Lag）
  - [ ] Web UI 查看实时状态

---

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

---

## 📄 License

MIT License

---

## 🙏 致谢

- [IBM/sarama](https://github.com/IBM/sarama) - Kafka Go 客户端
- [Docker SDK for Go](https://github.com/docker/docker) - Docker API 客户端
