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

	port, _ := strconv.Atoi(cfg.Metrics.Port)
	metrics.StartMetricsServer(":" + strconv.Itoa(port+1))

	workerCfg := worker.Config{
		Concurrency: cfg.Worker.Concurrency,
		QueueName:   cfg.Worker.QueueName,
	}

	if len(cfg.Worker.PriorityWeights) > 0 {
		workerCfg.HighConcurrency, workerCfg.MediumConcurrency, workerCfg.LowConcurrency =
			computeConcurrencies(cfg.Worker.Concurrency, cfg.Worker.PriorityWeights)

		workerDeps := worker.NewDependencies(workerCfg, taskDeps.Store, brokerDeps.Broker)
		pool := worker.NewPool(workerDeps)

		pool.RegisterHandler("*", func(ctx context.Context, taskType string, payload []byte) error {
			log.Printf("executed task type=%s payload=%s", taskType, string(payload))
			return nil
		})

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigCh
			log.Println("shutting down worker pool...")
			cancel()
		}()

		log.Printf("worker pool started high=%d medium=%d low=%d", workerCfg.HighConcurrency, workerCfg.MediumConcurrency, workerCfg.LowConcurrency)
		if err := pool.Run(ctx); err != nil && err != context.Canceled {
			log.Fatalf("worker pool error: %v", err)
		}
		log.Println("worker pool stopped")
		return
	}

	workerDeps := worker.NewDependencies(workerCfg, taskDeps.Store, brokerDeps.Broker)
	w := worker.New(workerDeps)

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

func computeConcurrencies(total int, weights map[string]int) (high, medium, low int) {
	h := weights["high"]
	m := weights["medium"]
	l := weights["low"]

	totalWeight := h + m + l
	if totalWeight == 0 {
		return total / 3, total / 3, total / 3
	}

	high = total * h / totalWeight
	medium = total * m / totalWeight
	low = total * l / totalWeight

	remaining := total - high - medium - low
	if remaining > 0 {
		if h >= m && h >= l {
			high += remaining
		} else if m >= h && m >= l {
			medium += remaining
		} else {
			low += remaining
		}
	}

	if high < 1 && total >= 3 {
		high = 1
	}
	if medium < 1 && total >= 3 {
		medium = 1
	}
	if low < 1 && total >= 3 {
		low = 1
	}

	return high, medium, low
}
