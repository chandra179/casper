package worker_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"casper/internal/testhelper"
	"casper/modules/broker"
	"casper/modules/task"
	"casper/modules/worker"
)

func workerMigrateTables(t *testing.T, uri string) {
	t.Helper()
	deps, err := task.NewDependencies(context.Background(), task.PostgresConfig{URI: uri})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	defer deps.Close()
	ctx := context.Background()
	deps.Store.Exec(ctx, `CREATE TABLE IF NOT EXISTS tasks (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(), task_type VARCHAR(255) NOT NULL,
		tenant_id VARCHAR(64) NOT NULL, payload JSONB NOT NULL DEFAULT '{}',
		status VARCHAR(20) NOT NULL DEFAULT 'PENDING', priority INT NOT NULL DEFAULT 0,
		scheduled_at TIMESTAMP NOT NULL DEFAULT NOW(), max_retries INT NOT NULL DEFAULT 3,
		retry_count INT NOT NULL DEFAULT 0, version BIGINT NOT NULL DEFAULT 0,
		claimed_by VARCHAR(255), claimed_at TIMESTAMP, completed_at TIMESTAMP,
		error_message TEXT, created_at TIMESTAMP NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMP NOT NULL DEFAULT NOW()
	)`)
	deps.Store.Exec(ctx, `CREATE TABLE IF NOT EXISTS processed_tasks (
		task_id UUID PRIMARY KEY, worker_id VARCHAR(255) NOT NULL,
		processed_at TIMESTAMP NOT NULL DEFAULT NOW()
	)`)
}

func TestWorkerIntegration(t *testing.T) {
	pg := testhelper.SetupPostgres(t)
	rmq := testhelper.SetupRabbitMQ(t)

	workerMigrateTables(t, pg.URI)

	taskDeps, err := task.NewDependencies(context.Background(), task.PostgresConfig{URI: pg.URI})
	if err != nil {
		t.Fatalf("task deps: %v", err)
	}
	defer taskDeps.Close()

	brokerDeps, err := broker.NewDependencies(context.Background(), broker.Config{URI: rmq.URI, Exchange: "tasks", Prefetch: 10})
	if err != nil {
		t.Fatalf("broker deps: %v", err)
	}
	defer brokerDeps.Close()

	taskID := uuid.New()
	tm := time.Now().UTC().Add(-time.Minute)
	if err := taskDeps.Store.Create(context.Background(), &task.Task{
		ID: taskID, TaskType: "worker_int_test", TenantID: "tenant-worker",
		Payload: []byte(`{"msg":"hello"}`), Status: task.StatusPending,
		Priority: 5, ScheduledAt: tm, MaxRetries: 3,
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	var handlerCalled bool
	var mu sync.Mutex

	workerDeps := worker.NewDependencies(
		worker.Config{Concurrency: 1, QueueName: broker.QueueHigh},
		taskDeps.Store, brokerDeps.Broker,
	)
	w := worker.New(workerDeps)
	w.RegisterHandler("worker_int_test", func(ctx context.Context, taskType string, payload []byte) error {
		mu.Lock()
		handlerCalled = true
		mu.Unlock()
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)

	err = brokerDeps.Broker.Publish(ctx, broker.QueueHigh, []byte(`{"msg":"hello"}`), 5,
		map[string]interface{}{"task_id": taskID.String(), "task_type": "worker_int_test", "tenant_id": "tenant-worker", "version": int64(0)},
	)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deadline := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for task completion")
		case <-ticker.C:
			got, _ := taskDeps.Store.GetByID(context.Background(), taskID)
			if got != nil && got.Status == task.StatusCompleted {
				goto done
			}
		}
	}
done:
	cancel()
	w.Stop()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	called := handlerCalled
	mu.Unlock()
	if !called {
		t.Error("handler was not called")
	}
}
