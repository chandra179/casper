package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"casper/config"
	"casper/modules/broker"
	"casper/modules/metrics"
	"casper/modules/scheduler"
	"casper/modules/task"
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

	brokerDeps, err := broker.NewDependencies(ctx, broker.Config{
		URL:      cfg.Broker.URL,
		Exchange: cfg.Broker.Exchange,
		Prefetch: cfg.Broker.Prefetch,
	})
	if err != nil {
		log.Fatalf("broker deps: %v", err)
	}
	defer brokerDeps.Close()

	schedDeps := scheduler.NewDependencies(
		scheduler.Config{
			PollInterval:         time.Duration(cfg.Scheduler.PollIntervalMs) * time.Millisecond,
			BatchSize:            cfg.Scheduler.BatchSize,
			JitterMax:            time.Duration(cfg.Scheduler.JitterMaxMs) * time.Millisecond,
			VisibilityTimeout:    time.Duration(cfg.Scheduler.VisibilityTimeoutMs) * time.Millisecond,
			CleanupInterval:      time.Duration(cfg.Scheduler.CleanupIntervalMs) * time.Millisecond,
			ShutdownDrainTimeout: time.Duration(cfg.Scheduler.ShutdownDrainTimeoutMs) * time.Millisecond,
			CleanupBatchSize:     100,
		},
		taskDeps.Pool,
		taskDeps.Store,
		brokerDeps.Broker,
	)

	sched := scheduler.New(schedDeps)

	metrics.StartMetricsServer(":" + cfg.Metrics.Port)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutting down scheduler...")
		cancel()
	}()

	log.Println("scheduler started")
	if err := sched.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("scheduler error: %v", err)
	}
	log.Println("scheduler stopped")
}
