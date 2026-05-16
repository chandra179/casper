package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"casper/config"
	"casper/middleware"
	"casper/modules/api"
	"casper/modules/metrics"
	"casper/modules/task"

	"casper/internal/logger"
)

func main() {
	cfgPath := "config/config.yaml"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx := context.Background()

	taskDeps, err := task.NewDependencies(ctx, task.PostgresConfig{
		Host:     cfg.Task.Postgres.Host,
		Port:     cfg.Task.Postgres.Port,
		User:     cfg.Task.Postgres.User,
		Password: cfg.Task.Postgres.Password,
		Database: cfg.Task.Postgres.Database,
		SSLMode:  cfg.Task.Postgres.SSLMode,
	})
	if err != nil {
		log.Fatalf("task deps: %v", err)
	}
	defer taskDeps.Close()

	apiDeps := api.NewDependencies(
		api.Config{
			Port:         cfg.API.Port,
			ReadTimeout:  time.Duration(cfg.API.ReadTimeoutInSec) * time.Second,
			WriteTimeout: time.Duration(cfg.API.WriteTimeoutInSec) * time.Second,
			IdleTimeout:  time.Duration(cfg.API.IdleTimeoutInSec) * time.Second,
		},
		taskDeps.Store,
	)

	logger := logger.NewLogger("dev")
	mwDeps := middleware.NewDependencies(logger)

	port, _ := strconv.Atoi(cfg.Metrics.Port)
	metrics.StartMetricsServer(":" + strconv.Itoa(port+2))

	log.Printf("API server starting on port %s", cfg.API.Port)
	if err := api.RunHTTPServer(apiDeps, mwDeps); err != nil {
		log.Fatalf("API server error: %v", err)
	}
}
