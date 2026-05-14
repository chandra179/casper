package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"casper/internal/testhelper"
	"casper/modules/broker"
	"casper/modules/scheduler"
	"casper/modules/task"
)

func migrateTables(t *testing.T, uri string) {
	t.Helper()
	deps, err := task.NewDependencies(context.Background(), task.PostgresConfig{URI: uri})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	defer deps.Close()
	ctx := context.Background()
	deps.Store.Exec(ctx, `CREATE TABLE IF NOT EXISTS tasks (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		task_type VARCHAR(255) NOT NULL, tenant_id VARCHAR(64) NOT NULL,
		payload JSONB NOT NULL DEFAULT '{}', status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
		priority INT NOT NULL DEFAULT 0, scheduled_at TIMESTAMP NOT NULL DEFAULT NOW(),
		max_retries INT NOT NULL DEFAULT 3, retry_count INT NOT NULL DEFAULT 0,
		version BIGINT NOT NULL DEFAULT 0, claimed_by VARCHAR(255), claimed_at TIMESTAMP,
		completed_at TIMESTAMP, error_message TEXT,
		created_at TIMESTAMP NOT NULL DEFAULT NOW(), updated_at TIMESTAMP NOT NULL DEFAULT NOW()
	)`)
	deps.Store.Exec(ctx, `CREATE TABLE IF NOT EXISTS processed_tasks (
		task_id UUID PRIMARY KEY, worker_id VARCHAR(255) NOT NULL,
		processed_at TIMESTAMP NOT NULL DEFAULT NOW()
	)`)
}

func TestSchedulerIntegration(t *testing.T) {
	pg := testhelper.SetupPostgres(t)
	rmq := testhelper.SetupRabbitMQ(t)

	migrateTables(t, pg.URI)

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

	ctx := context.Background()
	tm := time.Now().UTC().Add(-time.Minute)
	for i := 0; i < 3; i++ {
		if err := taskDeps.Store.Create(ctx, &task.Task{
			ID: uuid.New(), TaskType: "sched_int_test", TenantID: "tenant-sched",
			Payload: []byte(`{}`), Status: task.StatusPending,
			Priority: 5 + i, ScheduledAt: tm, MaxRetries: 3,
		}); err != nil {
			t.Fatalf("Create task %d: %v", i, err)
		}
	}

	schedDeps := scheduler.NewDependencies(
		scheduler.Config{PollInterval: 50 * time.Millisecond, BatchSize: 10, JitterMax: 0},
		taskDeps.Store, brokerDeps.Broker,
	)
	s := scheduler.New(schedDeps)

	sCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = s.Run(sCtx)

	deliveries, err := brokerDeps.Broker.Consume(broker.QueueHigh)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	msgCount := 0
	timeout := time.After(3 * time.Second)
loop:
	for i := 0; i < 5; i++ {
		select {
		case d := <-deliveries:
			msgCount++
			brokerDeps.Broker.Ack(d.DeliveryTag)
		case <-timeout:
			break loop
		}
	}

	if msgCount == 0 {
		t.Error("no messages dispatched to broker")
	}
}
