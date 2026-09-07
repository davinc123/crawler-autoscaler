package scaler

import (
	"math"
	"sync"
	"time"
)

// Scaler 负责根据 Kafka Lag 计算期望的 Worker 数量
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

// Config Scaler 配置
type Config struct {
	MinWorkers              int
	MaxWorkers              int
	TasksPerWorkerPerMinute int64
	ScaleUpThreshold        int64
	ScaleDownThreshold      int64
	ScaleUpCooldown         time.Duration
	ScaleDownCooldown       time.Duration
}

// Decision 扩缩容决策
type Decision struct {
	CurrentWorkers int
	DesiredWorkers int
	Action         Action
	Reason         string
}

// Action 扩缩容动作
type Action string

const (
	ActionNone      Action = "none"
	ActionScaleUp   Action = "scale_up"
	ActionScaleDown Action = "scale_down"
)

// NewScaler 创建 Scaler
func NewScaler(cfg Config, currentWorkers int) *Scaler {
	return &Scaler{
		minWorkers:              cfg.MinWorkers,
		maxWorkers:              cfg.MaxWorkers,
		tasksPerWorkerPerMinute: cfg.TasksPerWorkerPerMinute,
		scaleUpThreshold:        cfg.ScaleUpThreshold,
		scaleDownThreshold:      cfg.ScaleDownThreshold,
		scaleUpCooldown:         cfg.ScaleUpCooldown,
		scaleDownCooldown:       cfg.ScaleDownCooldown,
		currentWorkers:          currentWorkers,
	}
}

// Evaluate 根据 Kafka Lag 评估是否需要扩缩容
func (s *Scaler) Evaluate(lag int64) *Decision {
	s.mu.Lock()
	defer s.mu.Unlock()

	decision := &Decision{
		CurrentWorkers: s.currentWorkers,
		DesiredWorkers: s.currentWorkers,
		Action:         ActionNone,
	}

	// 计算理想 Worker 数量
	idealWorkers := s.calculateIdealWorkers(lag)

	// 检查是否需要扩容
	if idealWorkers > s.currentWorkers {
		if lag < s.scaleUpThreshold {
			decision.Reason = "lag below scale up threshold"
			return decision
		}

		if time.Since(s.lastScaleUp) < s.scaleUpCooldown {
			decision.Reason = "scale up cooldown period"
			return decision
		}

		decision.DesiredWorkers = idealWorkers
		decision.Action = ActionScaleUp
		decision.Reason = "lag exceeds threshold, scaling up"
		return decision
	}

	// 检查是否需要缩容
	if idealWorkers < s.currentWorkers {
		if lag > s.scaleDownThreshold {
			decision.Reason = "lag above scale down threshold"
			return decision
		}

		if time.Since(s.lastScaleDown) < s.scaleDownCooldown {
			decision.Reason = "scale down cooldown period"
			return decision
		}

		decision.DesiredWorkers = idealWorkers
		decision.Action = ActionScaleDown
		decision.Reason = "lag below threshold, scaling down"
		return decision
	}

	decision.Reason = "current workers optimal"
	return decision
}

// calculateIdealWorkers 计算理想的 Worker 数量
func (s *Scaler) calculateIdealWorkers(lag int64) int {
	if lag == 0 {
		return s.minWorkers
	}

	// Worker 数量 = ceil(Lag / 单 Worker 处理能力)
	workers := int(math.Ceil(float64(lag) / float64(s.tasksPerWorkerPerMinute)))

	// 限制在最小和最大值之间
	if workers < s.minWorkers {
		workers = s.minWorkers
	}

	if workers > s.maxWorkers {
		workers = s.maxWorkers
	}

	return workers
}

// ApplyDecision 应用决策，更新状态
func (s *Scaler) ApplyDecision(decision *Decision) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if decision.Action == ActionNone {
		return
	}

	s.currentWorkers = decision.DesiredWorkers

	if decision.Action == ActionScaleUp {
		s.lastScaleUp = time.Now()
	} else if decision.Action == ActionScaleDown {
		s.lastScaleDown = time.Now()
	}
}

// GetCurrentWorkers 获取当前 Worker 数量
func (s *Scaler) GetCurrentWorkers() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentWorkers
}

// SetCurrentWorkers 设置当前 Worker 数量（用于同步实际状态）
func (s *Scaler) SetCurrentWorkers(count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentWorkers = count
}
