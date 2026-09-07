# 开发指南

本文档面向希望参与 Crawler Autoscaler 开发的开发者。

---

## 目录

1. [开发环境设置](#1-开发环境设置)
2. [项目结构](#2-项目结构)
3. [开发工作流](#3-开发工作流)
4. [代码规范](#4-代码规范)
5. [测试指南](#5-测试指南)
6. [调试技巧](#6-调试技巧)
7. [常见问题](#7-常见问题)

---

## 1. 开发环境设置

### 1.1 前置要求

- **Go**: 1.21 或更高版本
- **Docker**: 用于本地测试 Worker 容器管理
- **Kafka**: 用于测试 Lag 监控（可使用 Docker Compose 快速启动）

### 1.2 克隆项目

```bash
git clone https://github.com/your-org/crawler-autoscaler.git
cd crawler-autoscaler
```

### 1.3 安装依赖

```bash
go mod download
```

### 1.4 启动本地 Kafka（可选）

如果需要本地测试，可以使用 Docker Compose 快速启动 Kafka：

```bash
# 创建 docker-compose.yml
cat > docker-compose.dev.yml << 'EOF'
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
EOF

# 启动
docker-compose -f docker-compose.dev.yml up -d

# 创建测试 Topic
docker exec -it crawler-autoscaler-kafka-1 \
  kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic football_tasks \
  --partitions 4 \
  --replication-factor 1
```

### 1.5 配置文件

复制示例配置：

```bash
cp config.example.yaml config.yaml
```

修改 `config.yaml` 中的 Kafka 地址。

---

## 2. 项目结构

```
crawler-autoscaler/
├── cmd/
│   └── autoscaler/
│       └── main.go              # 程序入口点
│
├── internal/                     # 内部包，不对外暴露
│   ├── config/
│   │   └── config.go            # 配置加载和管理
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
├── pkg/                          # 可对外暴露的包（未来）
│
├── test/                         # 集成测试
│
├── config.yaml                   # 配置文件
├── config.example.yaml           # 配置示例
├── go.mod
├── go.sum
├── README.md                     # 项目说明
├── DESIGN.md                     # 设计文档
└── DEVELOPMENT.md                # 本文档
```

### 2.1 包说明

#### `cmd/autoscaler`
- 程序入口
- 组装各个模块
- 启动控制循环

#### `internal/config`
- 加载 YAML 配置
- 配置验证
- 配置热更新（未来）

#### `internal/kafka`
- 连接 Kafka
- 获取 Consumer Group Offset
- 计算 Lag

#### `internal/scaler`
- 伸缩算法实现
- 防抖逻辑（阈值、冷却）
- 决策状态管理

#### `internal/docker`
- Docker Engine 连接
- 容器生命周期管理
- Worker 识别和过滤

---

## 3. 开发工作流

### 3.1 创建新功能

```bash
# 1. 创建功能分支
git checkout -b feature/add-prometheus-metrics

# 2. 开发
# 修改代码...

# 3. 运行测试
go test ./...

# 4. 格式化代码
go fmt ./...

# 5. 提交
git add .
git commit -m "feat: add prometheus metrics support"

# 6. 推送并创建 PR
git push origin feature/add-prometheus-metrics
```

### 3.2 本地运行

```bash
# 直接运行
go run ./cmd/autoscaler

# 编译后运行
go build -o autoscaler ./cmd/autoscaler
./autoscaler
```

### 3.3 调试模式

添加环境变量启用调试日志：

```bash
LOG_LEVEL=debug go run ./cmd/autoscaler
```

---

## 4. 代码规范

### 4.1 命名规范

**包名**：
- 小写，单数形式
- 简短且描述性
- 示例：`config`, `kafka`, `scaler`

**文件名**：
- 小写，下划线分隔
- 示例：`kafka_monitor.go`, `docker_manager.go`

**函数和方法**：
- 驼峰命名
- 导出函数首字母大写
- 示例：`NewMonitor()`, `GetLag()`, `calculateIdealWorkers()`

**常量**：
- 驼峰命名或全大写（枚举类型）
- 示例：`ActionScaleUp`, `DefaultTimeout`

**接口**：
- 名词或动词 + er
- 示例：`Monitor`, `Manager`, `Scaler`

### 4.2 注释规范

**包注释**（每个包的第一个文件）：

```go
// Package kafka provides Kafka consumer lag monitoring functionality.
package kafka
```

**导出类型和函数**必须有注释：

```go
// Monitor monitors Kafka consumer group lag.
type Monitor struct {
    // ...
}

// NewMonitor creates a new Kafka monitor instance.
func NewMonitor(brokers []string, topic string, consumerGroup string) (*Monitor, error) {
    // ...
}
```

**复杂逻辑**添加解释性注释：

```go
// 计算理想 Worker 数量
// 算法：ceil(Lag / 单 Worker 处理能力)
func (s *Scaler) calculateIdealWorkers(lag int64) int {
    // ...
}
```

### 4.3 错误处理

**总是检查错误**：

```go
// ❌ 错误
data, _ := os.ReadFile(path)

// ✅ 正确
data, err := os.ReadFile(path)
if err != nil {
    return fmt.Errorf("failed to read file: %w", err)
}
```

**使用 fmt.Errorf 包装错误**：

```go
if err != nil {
    return fmt.Errorf("get kafka lag failed: %w", err)
}
```

### 4.4 并发安全

需要状态的结构体使用 Mutex：

```go
type Scaler struct {
    mu sync.RWMutex
    currentWorkers int
}

func (s *Scaler) GetCurrentWorkers() int {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.currentWorkers
}
```

### 4.5 Context 使用

所有可能阻塞的操作传递 context：

```go
func (m *Monitor) GetLag(ctx context.Context) (*LagInfo, error) {
    // ...
}
```

---

## 5. 测试指南

### 5.1 单元测试

**文件命名**：`xxx_test.go`

**示例**：

```go
// internal/scaler/scaler_test.go
package scaler

import (
    "testing"
    "time"
)

func TestScaler_CalculateIdealWorkers(t *testing.T) {
    tests := []struct {
        name     string
        lag      int64
        capacity int64
        min      int
        max      int
        want     int
    }{
        {
            name:     "low lag",
            lag:      100,
            capacity: 500,
            min:      2,
            max:      20,
            want:     2, // ceil(100/500) = 1, but min is 2
        },
        {
            name:     "normal lag",
            lag:      2500,
            capacity: 500,
            min:      2,
            max:      20,
            want:     5, // ceil(2500/500) = 5
        },
        {
            name:     "high lag",
            lag:      15000,
            capacity: 500,
            min:      2,
            max:      20,
            want:     20, // ceil(15000/500) = 30, but max is 20
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            s := NewScaler(Config{
                MinWorkers:              tt.min,
                MaxWorkers:              tt.max,
                TasksPerWorkerPerMinute: tt.capacity,
            }, tt.min)

            got := s.calculateIdealWorkers(tt.lag)
            if got != tt.want {
                t.Errorf("calculateIdealWorkers() = %d, want %d", got, tt.want)
            }
        })
    }
}

func TestScaler_Evaluate_Cooldown(t *testing.T) {
    s := NewScaler(Config{
        MinWorkers:              2,
        MaxWorkers:              20,
        TasksPerWorkerPerMinute: 500,
        ScaleUpThreshold:        1000,
        ScaleUpCooldown:         1 * time.Second,
    }, 2)

    // 第一次扩容
    decision := s.Evaluate(5000)
    if decision.Action != ActionScaleUp {
        t.Errorf("first scale up should be allowed")
    }
    s.ApplyDecision(decision)

    // 立即再次评估（冷却期内）
    decision = s.Evaluate(5000)
    if decision.Action != ActionNone {
        t.Errorf("should be in cooldown period")
    }

    // 等待冷却期过后
    time.Sleep(1100 * time.Millisecond)
    decision = s.Evaluate(5000)
    if decision.Action != ActionScaleUp {
        t.Errorf("should allow scale up after cooldown")
    }
}
```

**运行测试**：

```bash
# 运行所有测试
go test ./...

# 运行指定包的测试
go test ./internal/scaler

# 运行指定测试
go test -run TestScaler_Evaluate ./internal/scaler

# 查看覆盖率
go test -cover ./...

# 生成覆盖率报告
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### 5.2 表格驱动测试

Go 推荐的测试风格：

```go
func TestScaler_Evaluate(t *testing.T) {
    tests := []struct {
        name           string
        lag            int64
        threshold      int64
        expectedAction Action
    }{
        {"low lag", 100, 1000, ActionNone},
        {"high lag", 5000, 1000, ActionScaleUp},
        {"threshold", 1000, 1000, ActionScaleUp},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // 测试逻辑
        })
    }
}
```

### 5.3 Mock 和 Stub

对于依赖外部服务的代码，使用接口和 Mock：

```go
// 定义接口
type KafkaClient interface {
    GetLag(ctx context.Context) (*LagInfo, error)
}

// 实现 Mock
type MockKafkaClient struct {
    LagToReturn int64
    ErrToReturn error
}

func (m *MockKafkaClient) GetLag(ctx context.Context) (*LagInfo, error) {
    if m.ErrToReturn != nil {
        return nil, m.ErrToReturn
    }
    return &LagInfo{TotalLag: m.LagToReturn}, nil
}

// 测试
func TestAutoscaler_WithMock(t *testing.T) {
    mockKafka := &MockKafkaClient{LagToReturn: 5000}
    // 使用 mockKafka 进行测试
}
```

### 5.4 集成测试

在 `test/` 目录下：

```go
// test/integration_test.go
// +build integration

package test

import (
    "testing"
    "time"
)

func TestKafkaIntegration(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping integration test")
    }

    // 集成测试逻辑
}
```

运行集成测试：

```bash
go test -tags=integration ./test
```

---

## 6. 调试技巧

### 6.1 日志级别

添加不同级别的日志：

```go
log.Printf("[DEBUG] Kafka lag: %d", lag)
log.Printf("[INFO] Scale decision: %s", decision.Action)
log.Printf("[WARN] High lag detected: %d", lag)
log.Printf("[ERROR] Failed to get lag: %v", err)
```

### 6.2 使用 Delve 调试

安装 Delve：

```bash
go install github.com/go-delve/delve/cmd/dlv@latest
```

启动调试：

```bash
dlv debug ./cmd/autoscaler
```

常用命令：

```
(dlv) break main.main      # 设置断点
(dlv) continue             # 继续执行
(dlv) next                 # 单步执行
(dlv) print variable       # 打印变量
(dlv) stack                # 查看调用栈
```

### 6.3 性能分析

添加 pprof：

```go
import _ "net/http/pprof"

go func() {
    log.Println(http.ListenAndServe("localhost:6060", nil))
}()
```

查看性能：

```bash
# CPU profile
go tool pprof http://localhost:6060/debug/pprof/profile

# Heap profile
go tool pprof http://localhost:6060/debug/pprof/heap

# Goroutine
go tool pprof http://localhost:6060/debug/pprof/goroutine
```

### 6.4 Race Detector

检测数据竞争：

```bash
go run -race ./cmd/autoscaler
go test -race ./...
```

---

## 7. 常见问题

### 7.1 Kafka 连接失败

**问题**：
```
kafka: client has run out of available brokers
```

**解决**：
- 检查 Kafka Broker 地址是否正确
- 确认网络连通性：`telnet localhost 9092`
- 检查 Kafka 是否正常运行：`docker ps`

### 7.2 获取 Lag 失败

**问题**：
```
expected int32 array to be non null
```

**解决**：
- 不要传递 `nil` partition 数组
- 应该先获取 partition 列表，再传递给 `ListConsumerGroupOffsets`

### 7.3 Consumer Group 不存在

**问题**：
```
consumer group not found
```

**解决**：
- 确认 Consumer Group 名称正确
- 确认至少有一个 Consumer 在消费该 Group
- 使用 `kafka-consumer-groups.sh --list` 查看所有 Group

### 7.4 编译错误

**问题**：
```
imported and not used
```

**解决**：
```bash
# 自动移除未使用的 import
gofmt -w .
# 或使用 goimports
goimports -w .
```

### 7.5 Go Module 依赖问题

**问题**：
```
module not found
```

**解决**：
```bash
go mod tidy
go mod download
```

---

## 8. 提交规范

### 8.1 Commit Message 格式

遵循 [Conventional Commits](https://www.conventionalcommits.org/)：

```
<type>(<scope>): <subject>

<body>

<footer>
```

**type**：
- `feat`: 新功能
- `fix`: Bug 修复
- `docs`: 文档更新
- `style`: 代码格式（不影响功能）
- `refactor`: 重构
- `test`: 测试相关
- `chore`: 构建、工具等

**示例**：

```
feat(scaler): add dynamic throughput calculation

Calculate worker throughput based on last 5 minutes metrics
instead of using a fixed value.

Closes #123
```

### 8.2 PR 规范

**标题**：清晰描述改动

**描述**：
```markdown
## 改动内容
- 添加了 XXX 功能
- 修复了 YYY Bug

## 测试
- [ ] 单元测试通过
- [ ] 集成测试通过
- [ ] 手动测试通过

## 截图
（如果有 UI 改动）
```

---

## 9. 发布流程

### 9.1 版本号

遵循 [Semantic Versioning](https://semver.org/)：

```
MAJOR.MINOR.PATCH

1.0.0 -> 1.0.1 (Bug 修复)
1.0.1 -> 1.1.0 (新功能，向后兼容)
1.1.0 -> 2.0.0 (破坏性变更)
```

### 9.2 发布步骤

```bash
# 1. 更新版本号
git tag v1.0.0

# 2. 推送 tag
git push origin v1.0.0

# 3. 构建 Docker 镜像
docker build -t crawler-autoscaler:v1.0.0 .

# 4. 推送镜像
docker push crawler-autoscaler:v1.0.0
```

---

## 10. 资源链接

### 10.1 Go 相关
- [Go 官方文档](https://golang.org/doc/)
- [Effective Go](https://golang.org/doc/effective_go)
- [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)

### 10.2 依赖库
- [IBM Sarama](https://github.com/IBM/sarama)
- [Docker SDK](https://github.com/docker/docker)

### 10.3 项目文档
- [README.md](./README.md) - 项目介绍
- [DESIGN.md](./DESIGN.md) - 设计文档
