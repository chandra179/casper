package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"casper/config"
	"casper/modules/broker"
	"casper/modules/metrics"
	"casper/modules/task"
	"casper/modules/worker"
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

	workerDeps := worker.NewDependencies(
		worker.Config{
			Concurrency: cfg.Worker.Concurrency,
			QueueName:   cfg.Worker.QueueName,
		},
		taskDeps.Store,
		brokerDeps.Broker,
	)

	w := worker.New(workerDeps)

	port, _ := strconv.Atoi(cfg.Metrics.Port)
	metrics.StartMetricsServer(":" + strconv.Itoa(port+1))

	// Register a default handler that logs and succeeds.
	w.RegisterHandler("*", func(ctx context.Context, taskType string, payload []byte) error {
		log.Printf("executed task type=%s payload=%s", taskType, string(payload))
		return nil
	})

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutting down worker...")
		cancel()
	}()

	log.Printf("worker started on queue=%s concurrency=%d", cfg.Worker.QueueName, cfg.Worker.Concurrency)
	if err := w.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("worker error: %v", err)
	}
	log.Println("worker stopped")
}
