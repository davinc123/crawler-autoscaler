# Crawler Autoscaler 设计文档

## 1. 设计背景

### 1.1 问题场景

在爬虫系统中，任务调度器会将爬虫任务发送到 Kafka，Worker 从 Kafka 消费任务并执行。但任务量是动态变化的：

- **高峰期**：任务大量产生，Kafka 消息积压（Lag）快速增长
- **低谷期**：任务很少，Worker 大部分时间空闲

**问题：**
- 固定数量的 Worker 无法应对流量波动
- 手动扩缩容效率低，响应慢
- 过多 Worker 浪费资源，过少 Worker 处理不及时

### 1.2 解决方案

构建一个 **Autoscaler**，根据 Kafka Consumer Group 的 Lag 自动调整 Worker 数量：

```
Kafka Lag ↑  →  扩容 Worker
Kafka Lag ↓  →  缩容 Worker
```

---

## 2. 系统设计

### 2.1 核心理念

Autoscaler 不是"自动创建机器"，而是：

> **监控 Kafka → 计算期望 Worker 数量 → 调用 Docker API → 扩容/缩容 → 持续循环**

这是一个经典的 **控制循环（Control Loop）** 模式。

### 2.2 系统架构图

```
┌─────────────────────────────────────────────────────────────┐
│                        Autoscaler                           │
│                                                             │
│  ┌───────────────┐      ┌───────────────┐                 │
│  │ Kafka Monitor │──────▶│    Scaler     │                 │
│  │   (获取Lag)    │      │  (计算决策)    │                 │
│  └───────────────┘      └───────┬───────┘                 │
│                                  │                          │
│                                  ▼                          │
│                         ┌────────────────┐                 │
│                         │ Docker Manager │                 │
│                         │   (执行扩缩容)   │                 │
│                         └────────┬───────┘                 │
└──────────────────────────────────┼──────────────────────────┘
                                   │
                            Docker Engine
                                   │
                    ┌──────────────┼──────────────┐
                    ▼              ▼              ▼
                Worker-1       Worker-2       Worker-N
```

### 2.3 核心组件

| 组件 | 职责 | 技术选型 |
|------|------|----------|
| **Kafka Monitor** | 获取 Consumer Group 的 Lag | IBM/sarama |
| **Scaler** | 根据 Lag 计算期望 Worker 数量 | 自研算法 |
| **Docker Manager** | 创建、停止、删除 Worker 容器 | Docker SDK |
| **Config** | 配置管理（阈值、冷却时间等） | YAML |

---

## 3. Kafka Monitor 设计

### 3.1 核心指标：Lag

```
Lag = Latest Offset - Consumer Offset
```

**示例：**

```
Partition 0:
    Latest Offset: 10000
    Consumer Offset: 9500
    Lag: 500

Partition 1:
    Latest Offset: 12000
    Consumer Offset: 11000
    Lag: 1000

Total Lag: 1500
```

### 3.2 数据结构

```go
type PartitionLag struct {
    Partition      int32
    LatestOffset   int64
    ConsumerOffset int64
    Lag            int64
}

type LagInfo struct {
    Topic      string
    Partitions []PartitionLag
    TotalLag   int64  // 最终用于决策
}
```

### 3.3 Sarama API 使用

```go
// 1. 获取 Topic 的所有 Partition
partitions := client.Partitions(topic)

// 2. 获取每个 Partition 的 Latest Offset
latestOffset := client.GetOffset(topic, partition, sarama.OffsetNewest)

// 3. 获取 Consumer Group 的当前 Offset
result := admin.ListConsumerGroupOffsets(consumerGroup, partitionMap)
```

### 3.4 边界情况处理

| 情况 | 处理方式 |
|------|----------|
| Consumer Offset = -1 | 视为 0（未开始消费） |
| Lag < 0 | 视为 0（异常情况） |
| Partition 无 Offset 记录 | 视为 0 |

---

## 4. Scaler 设计

### 4.1 核心算法

```
期望 Worker 数量 = ceil(Total Lag / 单 Worker 处理能力)
```

**示例：**

```
Total Lag: 2500
单 Worker 处理能力: 500 tasks/min

期望 Worker = ceil(2500 / 500) = 5
```

### 4.2 状态管理

Scaler 需要维护以下状态：

```go
type Scaler struct {
    mu sync.RWMutex
    
    // 配置
    minWorkers              int
    maxWorkers              int
    tasksPerWorkerPerMinute int64
    scaleUpThreshold        int64
    scaleDownThreshold      int64
    scaleUpCooldown         time.Duration
    scaleDownCooldown       time.Duration
    
    // 状态
    currentWorkers int
    lastScaleUp    time.Time
    lastScaleDown  time.Time
}
```

### 4.3 防抖机制

为了避免频繁扩缩容导致的系统抖动，设计了四重防护：

#### 1️⃣ 扩容阈值

```go
if lag < scaleUpThreshold {
    return ActionNone  // 不扩容
}
```

**示例：**
- 配置 `scale_up_threshold = 1000`
- 当 Lag = 800 时，即使计算出需要更多 Worker，也不扩容

#### 2️⃣ 缩容阈值

```go
if lag > scaleDownThreshold {
    return ActionNone  // 不缩容
}
```

**示例：**
- 配置 `scale_down_threshold = 200`
- 当 Lag = 300 时，即使 Worker 过多，也不缩容

#### 3️⃣ 扩容冷却

```go
if time.Since(lastScaleUp) < scaleUpCooldown {
    return ActionNone  // 冷却期内
}
```

**示例：**
- 配置 `scale_up_cooldown = 120s`
- 扩容后 2 分钟内不再次扩容

#### 4️⃣ 缩容冷却

```go
if time.Since(lastScaleDown) < scaleDownCooldown {
    return ActionNone  // 冷却期内
}
```

**示例：**
- 配置 `scale_down_cooldown = 300s`
- 缩容后 5 分钟内不再次缩容

### 4.4 决策流程

```
┌─────────────┐
│ 获取 Lag    │
└─────┬───────┘
      │
      ▼
┌─────────────────────┐
│ 计算理想 Worker 数量 │
└─────┬───────────────┘
      │
      ▼
┌─────────────────┐
│ 需要扩容？      │
│ Yes             │
└─────┬───────────┘
      │
      ▼
┌────────────────────┐      ┌────────────┐
│ Lag > 扩容阈值？   │ No   │ 不扩容     │
└─────┬──────────────┘      └────────────┘
      │ Yes
      ▼
┌────────────────────┐      ┌────────────┐
│ 冷却期已过？       │ No   │ 不扩容     │
└─────┬──────────────┘      └────────────┘
      │ Yes
      ▼
┌────────────────────┐
│ 执行扩容           │
└────────────────────┘
```

缩容流程类似。

### 4.5 并发安全

使用读写锁保证线程安全：

```go
func (s *Scaler) Evaluate(lag int64) *Decision {
    s.mu.Lock()
    defer s.mu.Unlock()
    // 决策逻辑
}

func (s *Scaler) GetCurrentWorkers() int {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.currentWorkers
}
```

---

## 5. Docker Manager 设计（待实现）

### 5.1 核心功能

```go
type DockerManager struct {
    client *client.Client
}

// 列出当前 Worker
func (m *DockerManager) ListWorkers(ctx context.Context) ([]Worker, error)

// 扩容
func (m *DockerManager) ScaleUp(ctx context.Context, count int) error

// 缩容
func (m *DockerManager) ScaleDown(ctx context.Context, count int) error
```

### 5.2 Worker 识别

**不要通过 Container Name 判断 Worker！**

推荐使用 **Label** 识别：

```yaml
labels:
  com.autoscaler.service: crawler-worker
  com.autoscaler.biz: football
```

这样可以：
- 支持多业务 Worker 混合部署
- 避免 Name 冲突
- 便于管理和查询

### 5.3 扩容实现

扩容时，新 Worker 需要使用和原 Worker **相同的配置**：

```go
containerConfig := &container.Config{
    Image: "crawler-worker:latest",
    Env: []string{
        "KAFKA_BROKERS=localhost:9092",
        "KAFKA_TOPIC=football_tasks",
        "KAFKA_GROUP=football-worker",
        "REDIS_ADDR=localhost:6379",
        // ...
    },
    Labels: map[string]string{
        "com.autoscaler.service": "crawler-worker",
        "com.autoscaler.biz":     "football",
    },
}

hostConfig := &container.HostConfig{
    NetworkMode: "bridge",
    // Volumes, Resources, etc.
}

resp, err := client.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, "")
client.ContainerStart(ctx, resp.ID, container.StartOptions{})
```

### 5.4 缩容实现（重点）

**错误做法：**

```go
docker kill worker-1  // ❌ 强制杀死
```

**正确做法：优雅退出**

```
1. Autoscaler 发送 SIGTERM
2. Worker 收到信号
3. Worker 停止消费新任务
4. Worker 等待当前任务完成
5. Worker 提交 Kafka Offset
6. Worker 退出
7. Autoscaler 删除容器
```

**代码实现：**

```go
// 1. 发送 SIGTERM
timeout := 30 * time.Second
client.ContainerStop(ctx, containerID, container.StopOptions{
    Timeout: &timeout,
})

// 2. 等待容器退出
statusCh, errCh := client.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)

select {
case <-statusCh:
    // 容器正常退出
case <-errCh:
    // 超时或异常
}

// 3. 删除容器
client.ContainerRemove(ctx, containerID, container.RemoveOptions{})
```

---

## 6. Worker 优雅退出设计

Worker 本身需要支持优雅退出。

### 6.1 信号处理

```go
ctx, stop := signal.NotifyContext(
    context.Background(),
    syscall.SIGTERM,
    syscall.SIGINT,
)
defer stop()
```

### 6.2 消费循环

```go
for {
    select {
    case <-ctx.Done():
        log.Println("Received shutdown signal")
        
        // 1. 停止消费
        consumer.Close()
        
        // 2. 等待当前任务完成
        wg.Wait()
        
        // 3. 提交 Offset
        session.Commit()
        
        return
        
    default:
        // 正常消费
        msg := consumer.Consume()
        go processTask(msg)
    }
}
```

---

## 7. 控制循环设计

### 7.1 主循环

```go
func Run(ctx context.Context, monitor *Monitor, scaler *Scaler, interval time.Duration) {
    ticker := time.NewTicker(interval)
    defer ticker.Stop()
    
    for {
        select {
        case <-ctx.Done():
            return
            
        case <-ticker.C:
            Evaluate(monitor, scaler)
        }
    }
}
```

### 7.2 单次评估

```go
func Evaluate(monitor *Monitor, scaler *Scaler) {
    // 1. 获取 Lag
    lagInfo, err := monitor.GetLag()
    if err != nil {
        log.Error(err)
        return
    }
    
    // 2. 做出决策
    decision := scaler.Evaluate(lagInfo.TotalLag)
    
    // 3. 执行决策
    if decision.Action == ActionScaleUp {
        dockerManager.ScaleUp(decision.DesiredWorkers - decision.CurrentWorkers)
    } else if decision.Action == ActionScaleDown {
        dockerManager.ScaleDown(decision.CurrentWorkers - decision.DesiredWorkers)
    }
    
    // 4. 更新状态
    scaler.ApplyDecision(decision)
}
```

### 7.3 优雅退出

```go
ctx, stop := signal.NotifyContext(
    context.Background(),
    syscall.SIGTERM,
    syscall.SIGINT,
)
defer stop()

Run(ctx, monitor, scaler, checkInterval)
```

---

## 8. 配置设计

### 8.1 配置文件结构

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

### 8.2 参数说明

| 参数 | 类型 | 说明 | 推荐值 |
|------|------|------|--------|
| `min_workers` | int | 最小 Worker 数量 | 2 |
| `max_workers` | int | 最大 Worker 数量 | 20 |
| `tasks_per_worker_per_minute` | int64 | 单 Worker 每分钟处理能力 | 根据实际测试 |
| `scale_up_threshold` | int64 | 扩容 Lag 阈值 | 1000 |
| `scale_down_threshold` | int64 | 缩容 Lag 阈值 | 200 |
| `scale_up_cooldown` | duration | 扩容冷却时间 | 120s |
| `scale_down_cooldown` | duration | 缩容冷却时间 | 300s（应大于扩容） |
| `check_interval` | duration | 检查间隔 | 10s |

### 8.3 参数调优建议

#### tasks_per_worker_per_minute

通过压测得出：

```bash
# 启动 1 个 Worker，运行 5 分钟
# 观察 Kafka Consumer Metrics

处理任务数 / 运行时间 = 单 Worker 处理能力
```

#### scale_up_threshold 和 scale_down_threshold

保持合理的间隙，避免频繁抖动：

```
scale_down_threshold < scale_up_threshold

推荐比例：
scale_down_threshold = scale_up_threshold * 0.2
```

#### scale_up_cooldown 和 scale_down_cooldown

缩容冷却时间应该**大于**扩容冷却时间：

```
scale_down_cooldown > scale_up_cooldown

原因：
- 扩容影响小，可以快速响应
- 缩容影响大，需要更谨慎
```

---

## 9. 进阶优化

### 9.1 动态处理能力

不要写死 `tasks_per_worker_per_minute`，而是动态计算：

```go
// 过去 5 分钟的实际吞吐
actualThroughput := metrics.GetThroughput(5 * time.Minute)

// 动态调整
tasksPerWorker = actualThroughput / currentWorkers
```

### 9.2 多维度决策

不仅看 Lag，还可以综合考虑：

```go
type Metrics struct {
    KafkaLag        int64
    TaskProcessRate float64
    WorkerCPU       float64
    WorkerMemory    float64
}

desiredWorkers = f(metrics)
```

### 9.3 预测性扩容

基于历史数据预测未来 Lag：

```go
// 过去 10 分钟的 Lag 趋势
trend := calculateTrend(lagHistory)

if trend > 0 {
    // Lag 正在上升，提前扩容
    desiredWorkers += 1
}
```

---

## 10. 监控和可观测性

### 10.1 关键指标

| 指标 | 说明 |
|------|------|
| `kafka_lag_total` | 总 Lag |
| `kafka_lag_per_partition` | 每个 Partition 的 Lag |
| `workers_current` | 当前 Worker 数量 |
| `workers_desired` | 期望 Worker 数量 |
| `scale_up_total` | 扩容次数 |
| `scale_down_total` | 缩容次数 |
| `scale_decision_duration` | 决策耗时 |

### 10.2 Prometheus Metrics

```go
var (
    kafkaLagGauge = prometheus.NewGauge(prometheus.GaugeOpts{
        Name: "autoscaler_kafka_lag_total",
        Help: "Total Kafka consumer lag",
    })
    
    workersGauge = prometheus.NewGauge(prometheus.GaugeOpts{
        Name: "autoscaler_workers_current",
        Help: "Current number of workers",
    })
    
    scaleUpCounter = prometheus.NewCounter(prometheus.CounterOpts{
        Name: "autoscaler_scale_up_total",
        Help: "Total number of scale up events",
    })
)
```

### 10.3 日志设计

结构化日志：

```go
log.WithFields(log.Fields{
    "lag":     lagInfo.TotalLag,
    "current": decision.CurrentWorkers,
    "desired": decision.DesiredWorkers,
    "action":  decision.Action,
    "reason":  decision.Reason,
}).Info("Scale decision")
```

---

## 11. 部署架构

### 11.1 单机部署

```yaml
version: '3.8'

services:
  autoscaler:
    image: crawler-autoscaler:latest
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./config.yaml:/app/config.yaml
    environment:
      - KAFKA_BROKERS=kafka:9092
    restart: unless-stopped
```

### 11.2 高可用部署

使用 **Leader Election** 确保只有一个 Autoscaler 实例在工作：

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Autoscaler  │    │ Autoscaler  │    │ Autoscaler  │
│  (Leader)   │    │  (Standby)  │    │  (Standby)  │
└─────────────┘    └─────────────┘    └─────────────┘
       │
       ▼
  Etcd / Redis (分布式锁)
```

---

## 12. 测试策略

### 12.1 单元测试

```go
func TestScaler_Evaluate(t *testing.T) {
    scaler := NewScaler(Config{
        MinWorkers:       2,
        MaxWorkers:       10,
        ScaleUpThreshold: 1000,
    }, 2)
    
    // 测试低 Lag
    decision := scaler.Evaluate(100)
    assert.Equal(t, ActionNone, decision.Action)
    
    // 测试高 Lag
    decision = scaler.Evaluate(5000)
    assert.Equal(t, ActionScaleUp, decision.Action)
}
```

### 12.2 集成测试

模拟真实 Kafka 环境：

```go
func TestIntegration(t *testing.T) {
    // 1. 启动测试 Kafka
    kafka := startTestKafka()
    defer kafka.Stop()
    
    // 2. 生产大量消息
    produceMessages(kafka, 10000)
    
    // 3. 启动 Autoscaler
    autoscaler := startAutoscaler()
    
    // 4. 验证扩容
    time.Sleep(30 * time.Second)
    assert.Greater(t, autoscaler.GetWorkers(), 2)
}
```

---

## 13. 总结

### 13.1 核心设计原则

1. **简单优先**：先做基础功能，再优化
2. **防抖为王**：避免频繁扩缩容
3. **优雅退出**：Worker 必须支持优雅停机
4. **可观测性**：日志、指标、监控一个都不能少

### 13.2 关键技术点

- Kafka Lag 监控（Sarama）
- 伸缩算法（阈值 + 冷却）
- Docker API（容器管理）
- 控制循环（持续监控）
- 并发安全（Mutex）

### 13.3 生产就绪 Checklist

- [ ] Kafka 连接失败重试
- [ ] Docker API 调用失败处理
- [ ] 配置热更新
- [ ] Prometheus Metrics
- [ ] 结构化日志
- [ ] 健康检查接口
- [ ] 优雅退出
- [ ] 单元测试覆盖 > 80%
- [ ] 压力测试
- [ ] 监控告警
