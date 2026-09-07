package docker

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
)

// Manager 负责管理 Docker Worker 容器
type Manager struct {
	client *client.Client
	config WorkerConfig
}

// WorkerConfig Worker 容器配置
type WorkerConfig struct {
	// Worker 容器镜像
	Image string

	// 用于识别 Worker 的 Label
	ServiceLabel string // 例如: "com.autoscaler.service=crawler-worker"
	BizLabel     string // 例如: "com.autoscaler.biz=football"

	// 容器配置
	Env           []string          // 环境变量
	Labels        map[string]string // 标签
	NetworkMode   string            // 网络模式
	RestartPolicy string            // 重启策略

	// 资源限制
	CPULimit    int64 // CPU 限制（纳秒）
	MemoryLimit int64 // 内存限制（字节）
}

// WorkerInfo Worker 容器信息
type WorkerInfo struct {
	ID      string
	Name    string
	State   string
	Created time.Time
	Labels  map[string]string
}

// NewManager 创建 Docker Manager
func NewManager(config WorkerConfig) (*Manager, error) {
	os.Setenv("DOCKER_HOST", "tcp://10.55.49.169:2375")
	cli, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	if err != nil {
		return nil, fmt.Errorf("create docker client failed: %w", err)
	}

	return &Manager{
		client: cli,
		config: config,
	}, nil
}

// Close 关闭 Docker 客户端
func (m *Manager) Close() error {
	return m.client.Close()
}

// ListWorkers 列出所有 Worker 容器
func (m *Manager) ListWorkers(ctx context.Context) ([]WorkerInfo, error) {
	// 构建过滤器：根据 Label 识别 Worker
	filterArgs := filters.NewArgs()
	filterArgs.Add("label", m.config.ServiceLabel)
	if m.config.BizLabel != "" {
		filterArgs.Add("label", m.config.BizLabel)
	}

	containers, err := m.client.ContainerList(ctx, container.ListOptions{
		All:     true, // 包括停止的容器
		Filters: filterArgs,
	})
	if err != nil {
		return nil, fmt.Errorf("list containers failed: %w", err)
	}

	workers := make([]WorkerInfo, 0, len(containers))
	for _, c := range containers {
		name := ""
		if len(c.Names) > 0 {
			name = c.Names[0]
		}

		workers = append(workers, WorkerInfo{
			ID:      c.ID,
			Name:    name,
			State:   c.State,
			Created: time.Unix(c.Created, 0),
			Labels:  c.Labels,
		})
	}

	return workers, nil
}

// CountRunningWorkers 统计正在运行的 Worker 数量
func (m *Manager) CountRunningWorkers(ctx context.Context) (int, error) {
	workers, err := m.ListWorkers(ctx)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, w := range workers {
		if w.State == "running" {
			count++
		}
	}

	return count, nil
}

// ScaleUp 扩容：启动 n 个新 Worker
func (m *Manager) ScaleUp(ctx context.Context, count int) error {
	if count <= 0 {
		return nil
	}

	log.Printf("[Docker] Scaling up: creating %d new workers", count)

	for i := 0; i < count; i++ {
		if err := m.createWorker(ctx, i); err != nil {
			return fmt.Errorf("create worker %d failed: %w", i, err)
		}
	}

	log.Printf("[Docker] Successfully created %d workers", count)
	return nil
}

// createWorker 创建并启动一个 Worker 容器
func (m *Manager) createWorker(ctx context.Context, index int) error {
	// 生成容器名称
	containerName := fmt.Sprintf("crawler-worker-%d-%d", time.Now().Unix(), index)

	// 容器配置
	containerConfig := &container.Config{
		Image:  m.config.Image,
		Env:    m.config.Env,
		Labels: m.config.Labels,
	}

	// 主机配置
	hostConfig := &container.HostConfig{
		NetworkMode: container.NetworkMode(m.config.NetworkMode),
	}

	// 资源限制
	if m.config.CPULimit > 0 || m.config.MemoryLimit > 0 {
		hostConfig.Resources = container.Resources{
			NanoCPUs: m.config.CPULimit,
			Memory:   m.config.MemoryLimit,
		}
	}

	// 重启策略
	if m.config.RestartPolicy != "" {
		hostConfig.RestartPolicy = container.RestartPolicy{
			Name: container.RestartPolicyMode(m.config.RestartPolicy),
		}
	}

	// 创建容器
	resp, err := m.client.ContainerCreate(
		ctx,
		containerConfig,
		hostConfig,
		nil,
		nil,
		containerName,
	)
	if err != nil {
		return fmt.Errorf("container create failed: %w", err)
	}

	// 启动容器
	if err := m.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		// 创建成功但启动失败，尝试清理
		_ = m.client.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true})
		return fmt.Errorf("container start failed: %w", err)
	}

	log.Printf("[Docker] Created worker: %s (ID: %s)", containerName, resp.ID[:12])

	return nil
}

// ScaleDown 缩容：优雅停止 n 个 Worker
func (m *Manager) ScaleDown(ctx context.Context, count int) error {
	if count <= 0 {
		return nil
	}

	log.Printf("[Docker] Scaling down: stopping %d workers", count)

	// 获取正在运行的 Worker
	workers, err := m.ListWorkers(ctx)
	if err != nil {
		return fmt.Errorf("list workers failed: %w", err)
	}

	// 过滤出正在运行的 Worker
	runningWorkers := make([]WorkerInfo, 0)
	for _, w := range workers {
		if w.State == "running" {
			runningWorkers = append(runningWorkers, w)
		}
	}

	// 限制要停止的数量
	if count > len(runningWorkers) {
		count = len(runningWorkers)
	}

	// 停止 Worker（从最新创建的开始停止）
	// 这样可以保留运行时间较长的 Worker
	for i := 0; i < count; i++ {
		worker := runningWorkers[len(runningWorkers)-1-i]
		if err := m.stopWorker(ctx, worker); err != nil {
			log.Printf("[Error] Failed to stop worker %s: %v", worker.ID[:12], err)
			continue
		}
	}

	log.Printf("[Docker] Successfully stopped %d workers", count)
	return nil
}

// stopWorker 优雅停止一个 Worker
func (m *Manager) stopWorker(ctx context.Context, worker WorkerInfo) error {
	log.Printf("[Docker] Stopping worker: %s (%s)", worker.Name, worker.ID[:12])

	// 设置停止超时（30 秒）
	// 这会先发送 SIGTERM，等待容器优雅退出
	// 如果超时则发送 SIGKILL 强制停止
	timeout := 30
	stopOptions := container.StopOptions{
		Timeout: &timeout,
	}

	if err := m.client.ContainerStop(ctx, worker.ID, stopOptions); err != nil {
		return fmt.Errorf("stop container failed: %w", err)
	}

	// 等待容器完全停止
	statusCh, errCh := m.client.ContainerWait(
		ctx,
		worker.ID,
		container.WaitConditionNotRunning,
	)

	select {
	case <-statusCh:
		// 容器已停止
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("wait container stop failed: %w", err)
		}
	case <-time.After(60 * time.Second):
		// 超时保护
		return fmt.Errorf("wait container stop timeout")
	}

	// 删除容器
	removeOptions := container.RemoveOptions{
		RemoveVolumes: true,
		Force:         false,
	}

	if err := m.client.ContainerRemove(ctx, worker.ID, removeOptions); err != nil {
		return fmt.Errorf("remove container failed: %w", err)
	}

	log.Printf("[Docker] Worker stopped and removed: %s", worker.ID[:12])

	return nil
}

// PullImage 拉取 Worker 镜像
func (m *Manager) PullImage(ctx context.Context) error {
	log.Printf("[Docker] Pulling image: %s", m.config.Image)

	reader, err := m.client.ImagePull(ctx, m.config.Image, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("pull image failed: %w", err)
	}
	defer reader.Close()

	// 读取并丢弃输出（实际使用时可以解析 JSON 显示进度）
	_, err = io.Copy(io.Discard, reader)
	if err != nil {
		return fmt.Errorf("read pull output failed: %w", err)
	}

	log.Printf("[Docker] Image pulled successfully: %s", m.config.Image)
	return nil
}

// GetWorkerByID 根据 ID 获取 Worker 信息
func (m *Manager) GetWorkerByID(ctx context.Context, id string) (*WorkerInfo, error) {
	containerJSON, err := m.client.ContainerInspect(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("inspect container failed: %w", err)
	}

	name := containerJSON.Name
	if len(name) > 0 && name[0] == '/' {
		name = name[1:] // 去掉开头的斜杠
	}

	// 解析创建时间
	created, err := time.Parse(time.RFC3339Nano, containerJSON.Created)
	if err != nil {
		created = time.Now() // 解析失败使用当前时间
	}

	return &WorkerInfo{
		ID:      containerJSON.ID,
		Name:    name,
		State:   containerJSON.State.Status,
		Created: created,
		Labels:  containerJSON.Config.Labels,
	}, nil
}
