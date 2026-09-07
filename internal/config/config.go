package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	App        AppConfig        `yaml:"app"`
	Kafka      KafkaConfig      `yaml:"kafka"`
	Autoscaler AutoscalerConfig `yaml:"autoscaler"`
	Docker     DockerConfig     `yaml:"docker"`
}

type AppConfig struct {
	Name        string `yaml:"name"`
	Environment string `yaml:"environment"`
}

type KafkaConfig struct {
	Brokers       []string `yaml:"brokers"`
	Topic         string   `yaml:"topic"`
	ConsumerGroup string   `yaml:"consumer_group"`
}

type AutoscalerConfig struct {
	MinWorkers              int           `yaml:"min_workers"`
	MaxWorkers              int           `yaml:"max_workers"`
	TasksPerWorkerPerMinute int64         `yaml:"tasks_per_worker_per_minute"`
	ScaleUpThreshold        int64         `yaml:"scale_up_threshold"`
	ScaleDownThreshold      int64         `yaml:"scale_down_threshold"`
	ScaleUpCooldown         time.Duration `yaml:"scale_up_cooldown"`
	ScaleDownCooldown       time.Duration `yaml:"scale_down_cooldown"`
	CheckInterval           time.Duration `yaml:"check_interval"`
}

type DockerConfig struct {
	Worker WorkerConfig `yaml:"worker"`
}

type WorkerConfig struct {
	Image         string            `yaml:"image"`
	ServiceLabel  string            `yaml:"service_label"`
	BizLabel      string            `yaml:"biz_label"`
	Environment   []string          `yaml:"environment"`
	Labels        map[string]string `yaml:"labels"`
	NetworkMode   string            `yaml:"network_mode"`
	RestartPolicy string            `yaml:"restart_policy"`
	CPULimit      string            `yaml:"cpu_limit"`
	MemoryLimit   string            `yaml:"memory_limit"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
