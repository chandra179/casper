package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Task      TaskConfig      `yaml:"task"`
	Broker    BrokerConfig    `yaml:"broker"`
	Scheduler SchedulerConfig `yaml:"scheduler"`
	Worker    WorkerConfig    `yaml:"worker"`
	API       APIConfig       `yaml:"api"`
}

type TaskConfig struct {
	Postgres PostgresConfig `yaml:"postgres"`
}

type PostgresConfig struct {
	URI      string `yaml:"uri"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	SSLMode  string `yaml:"sslmode"`
}

type BrokerConfig struct {
	URI      string `yaml:"uri"`
	URL      string `yaml:"url"`
	Exchange string `yaml:"exchange"`
	Prefetch int    `yaml:"prefetch"`
}

type SchedulerConfig struct {
	PollIntervalMs          int `yaml:"poll_interval_ms"`
	BatchSize               int `yaml:"batch_size"`
	JitterMaxMs             int `yaml:"jitter_max_ms"`
	VisibilityTimeoutMs     int `yaml:"visibility_timeout_ms"`
	CleanupIntervalMs       int `yaml:"cleanup_interval_ms"`
	ShutdownDrainTimeoutMs  int `yaml:"shutdown_drain_timeout_ms"`
}

type WorkerConfig struct {
	Concurrency int    `yaml:"concurrency"`
	QueueName   string `yaml:"queue_name"`
}

type APIConfig struct {
	Port              string `yaml:"port"`
	ReadTimeoutInSec  int    `yaml:"read_timeout_in_second"`
	WriteTimeoutInSec int    `yaml:"write_timeout_in_second"`
	IdleTimeoutInSec  int    `yaml:"idle_timeout_in_second"`
}

func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var cfg Config
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
