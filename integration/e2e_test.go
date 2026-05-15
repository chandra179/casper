package integration_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"casper/internal/testhelper"
	"casper/modules/broker"
	"casper/modules/scheduler"
	"casper/modules/task"
	"casper/modules/worker"
)

func e2eMigrate(t *testing.T, uri string) {
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

func TestE2EFullFlow(t *testing.T) {
	pg := testhelper.SetupPostgres(t)
	rmq := testhelper.SetupRabbitMQ(t)
	e2eMigrate(t, pg.URI)

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
	tsk := &task.Task{
		ID: uuid.New(), TaskType: "e2e_test", TenantID: "tenant-e2e",
		Payload: []byte(`{"action":"verify_flow"}`), Status: task.StatusPending,
		Priority: 5, ScheduledAt: tm, MaxRetries: 3,
	}
	if err := taskDeps.Store.Create(ctx, tsk); err != nil {
		t.Fatalf("Create task: %v", err)
	}

	var handlerCalled bool
	var mu sync.Mutex

	workerDeps := worker.NewDependencies(
		worker.Config{Concurrency: 1, QueueName: broker.QueueMedium},
		taskDeps.Store, brokerDeps.Broker,
	)
	w := worker.New(workerDeps)
	w.RegisterHandler("e2e_test", func(ctx context.Context, taskType string, payload []byte) error {
		mu.Lock()
		handlerCalled = true
		mu.Unlock()
		return nil
	})

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = w.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)

	schedDeps := scheduler.NewDependencies(
		scheduler.Config{PollInterval: 50 * time.Millisecond, BatchSize: 10, JitterMax: 0,
			VisibilityTimeout: 5 * time.Minute, CleanupInterval: 30 * time.Second,
			ShutdownDrainTimeout: 10 * time.Second, CleanupBatchSize: 100},
		taskDeps.Pool, taskDeps.Store, brokerDeps.Broker,
	)
	s := scheduler.New(schedDeps)
	go func() { _ = s.Run(ctx) }()

	deadline := time.After(15 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for task completion")
		case <-ticker.C:
			got, _ := taskDeps.Store.GetByID(ctx, tsk.ID)
			if got != nil && got.Status == task.StatusCompleted {
				goto done
			}
		}
	}
done:
	cancel()
	w.Stop()
	s.Stop()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	called := handlerCalled
	mu.Unlock()
	if !called {
		t.Error("handler was not called")
	}
}

func TestE2EConcurrentClaims(t *testing.T) {
	pg := testhelper.SetupPostgres(t)
	rmq := testhelper.SetupRabbitMQ(t)
	e2eMigrate(t, pg.URI)

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
	for i := 0; i < 10; i++ {
		tm := time.Now().UTC().Add(-time.Minute)
		if err := taskDeps.Store.Create(ctx, &task.Task{
			ID: uuid.New(), TaskType: "concurrent_test", TenantID: "tenant-concurrent",
			Payload: []byte(`{}`), Status: task.StatusPending,
			Priority: 1, ScheduledAt: tm, MaxRetries: 3,
		}); err != nil {
			t.Fatalf("Create task %d: %v", i, err)
		}
	}

	var claimedCount int
	var claimedMu sync.Mutex

	workerDeps := worker.NewDependencies(
		worker.Config{Concurrency: 1, QueueName: broker.QueueLow},
		taskDeps.Store, brokerDeps.Broker,
	)
	w := worker.New(workerDeps)
	w.RegisterHandler("concurrent_test", func(ctx context.Context, taskType string, payload []byte) error {
		claimedMu.Lock()
		claimedCount++
		claimedMu.Unlock()
		return nil
	})

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = w.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			schedDeps := scheduler.NewDependencies(
				scheduler.Config{PollInterval: 50 * time.Millisecond, BatchSize: 10, JitterMax: 10 * time.Millisecond,
					VisibilityTimeout: 5 * time.Minute, CleanupInterval: 30 * time.Second,
					ShutdownDrainTimeout: 10 * time.Second, CleanupBatchSize: 100},
				taskDeps.Pool, taskDeps.Store, brokerDeps.Broker,
			)
			s := scheduler.New(schedDeps)
			runCtx, runCancel := context.WithTimeout(ctx, 5*time.Second)
			defer runCancel()
			_ = s.Run(runCtx)
		}(i)
	}
	wg.Wait()

	deadline := time.After(15 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for all tasks to complete")
		case <-ticker.C:
			claimedMu.Lock()
			count := claimedCount
			claimedMu.Unlock()
			if count >= 10 {
				goto done2
			}
		}
	}
done2:
	cancel()
	w.Stop()
	time.Sleep(100 * time.Millisecond)

	if claimedCount != 10 {
		t.Errorf("claimed count: want 10, got %d", claimedCount)
	}
}

func TestE2ETenantIsolation(t *testing.T) {
	pg := testhelper.SetupPostgres(t)
	e2eMigrate(t, pg.URI)

	taskDeps, err := task.NewDependencies(context.Background(), task.PostgresConfig{URI: pg.URI})
	if err != nil {
		t.Fatalf("task deps: %v", err)
	}
	defer taskDeps.Close()

	ctx := context.Background()
	tm := time.Now().UTC().Add(-time.Minute)
	tenantA := "tenant-isolation-a"
	tenantB := "tenant-isolation-b"

	taskA := &task.Task{
		ID: uuid.New(), TaskType: "iso_test", TenantID: tenantA,
		Payload: []byte(`{"tenant":"a"}`), Status: task.StatusPending,
		Priority: 5, ScheduledAt: tm, MaxRetries: 3,
	}
	taskB := &task.Task{
		ID: uuid.New(), TaskType: "iso_test", TenantID: tenantB,
		Payload: []byte(`{"tenant":"b"}`), Status: task.StatusPending,
		Priority: 5, ScheduledAt: tm, MaxRetries: 3,
	}

	if err := taskDeps.Store.Create(ctx, taskA); err != nil {
		t.Fatalf("Create taskA: %v", err)
	}
	if err := taskDeps.Store.Create(ctx, taskB); err != nil {
		t.Fatalf("Create taskB: %v", err)
	}

	gotA, _ := taskDeps.Store.GetByID(ctx, taskA.ID)
	if gotA == nil || gotA.TenantID != tenantA {
		t.Errorf("tenantA mismatch: want %s", tenantA)
	}

	gotB, _ := taskDeps.Store.GetByID(ctx, taskB.ID)
	if gotB == nil || gotB.TenantID != tenantB {
		t.Errorf("tenantB mismatch: want %s", tenantB)
	}

	if gotA != nil && gotB != nil && gotA.TenantID == gotB.TenantID {
		t.Error("tenants should be different")
	}
}
