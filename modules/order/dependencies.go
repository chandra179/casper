package order

import (
	"os"

	"gopkg.in/yaml.v3"

	"github.com/Chandra179/gosdk/logger"
	"brook/middleware"
)

type Dependencies struct {
	Config     Config
	Logger     logger.Logger
	Middleware *middleware.Dependencies
}

func loadConfig(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()
	var cfg Config
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func NewDependencies(cfg Config) *Dependencies {
	log := logger.NewLogger(cfg.Middleware.Logger.Level)
	return &Dependencies{
		Config:     cfg,
		Logger:     log,
		Middleware: middleware.NewDependencies(log),
	}
}
