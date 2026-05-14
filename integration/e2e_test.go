package integration_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"casper/modules/broker"
	"casper/modules/scheduler"
	"casper/modules/task"
	"casper/modules/worker"
)

func testConfigs() (task.PostgresConfig, broker.Config) {
	host := os.Getenv("CASPER_DB_HOST")
	if host == "" {
		host = "localhost"
	}
	dbCfg := task.PostgresConfig{
		Host:     host,
		Port:     5432,
		User:     "casper",
		Password: "casper",
		Database: "casper",
		SSLMode:  "disable",
	}

	brokerURL := os.Getenv("CASPER_BROKER_URL")
	if brokerURL == "" {
		brokerURL = "amqp://casper:casper@localhost:5672/"
	}
	bkrCfg := broker.Config{
		URL:      brokerURL,
		Exchange: "tasks",
		Prefetch: 10,
	}
	return dbCfg, bkrCfg
}

func TestE2EFullFlow(t *testing.T) {
	dbCfg, bkrCfg := testConfigs()
	ctx := context.Background()

	taskDeps, err := task.NewDependencies(ctx, dbCfg)
	if err != nil {
		t.Skipf("postgres not available: %v", err)
	}
	defer func() {
		taskDeps.Store.Exec(ctx, "DELETE FROM processed_tasks")
		taskDeps.Store.Exec(ctx, "DELETE FROM tasks")
		taskDeps.Close()
	}()

	brokerDeps, err := broker.NewDependencies(ctx, bkrCfg)
	if err != nil {
		t.Skipf("rabbitmq not available: %v", err)
	}
	defer brokerDeps.Close()

	// Purge queues before test
	brokerDeps.Broker.PurgeQueue(broker.QueueHigh)
	brokerDeps.Broker.PurgeQueue(broker.QueueMedium)
	brokerDeps.Broker.PurgeQueue(broker.QueueLow)
	brokerDeps.Broker.PurgeQueue(broker.QueueDead)

	// Create a task
	tm := time.Now().UTC().Add(-time.Minute)
	tsk := &task.Task{
		ID:          uuid.New(),
		TaskType:    "e2e_test",
		TenantID:    "tenant-e2e",
		Payload:     []byte(`{"action":"verify_flow"}`),
		Status:      task.StatusPending,
		Priority:    5,
		ScheduledAt: tm,
		MaxRetries:  3,
	}
	if err := taskDeps.Store.Create(ctx, tsk); err != nil {
		t.Fatalf("Create task: %v", err)
	}

	var handlerCalled bool
	var mu sync.Mutex

	workerDeps := worker.NewDependencies(
		worker.Config{Concurrency: 1, QueueName: broker.QueueMedium},
		taskDeps.Store,
		brokerDeps.Broker,
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
		scheduler.Config{
			PollInterval: 50 * time.Millisecond,
			BatchSize:    10,
			JitterMax:    0,
		},
		taskDeps.Store,
		brokerDeps.Broker,
	)
	sched := scheduler.New(schedDeps)

	go func() { _ = sched.Run(ctx) }()

	// Wait for the task to be processed
	deadline := time.After(10 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for task completion")
		case <-ticker.C:
			got, err := taskDeps.Store.GetByID(ctx, tsk.ID)
			if err != nil {
				t.Fatalf("GetByID: %v", err)
			}
			if got.Status == task.StatusCompleted {
				goto done
			}
		}
	}
done:

	cancel()
	w.Stop()
	sched.Stop()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	called := handlerCalled
	mu.Unlock()
	if !called {
		t.Error("handler was not called")
	}

	got, _ := taskDeps.Store.GetByID(context.Background(), tsk.ID)
	if got == nil {
		t.Errorf("task disappeared")
	} else if got.Status != task.StatusCompleted {
		t.Errorf("final status: want COMPLETED, got %s", got.Status)
	}
}

func TestE2EConcurrentClaims(t *testing.T) {
	dbCfg, bkrCfg := testConfigs()
	ctx := context.Background()

	taskDeps, err := task.NewDependencies(ctx, dbCfg)
	if err != nil {
		t.Skipf("postgres not available: %v", err)
	}
	defer func() {
		taskDeps.Store.Exec(ctx, "DELETE FROM processed_tasks")
		taskDeps.Store.Exec(ctx, "DELETE FROM tasks")
		taskDeps.Close()
	}()

	brokerDeps, err := broker.NewDependencies(ctx, bkrCfg)
	if err != nil {
		t.Skipf("rabbitmq not available: %v", err)
	}
	defer brokerDeps.Close()

	brokerDeps.Broker.PurgeQueue(broker.QueueLow)

	// Create 10 tasks
	for i := 0; i < 10; i++ {
		tm := time.Now().UTC().Add(-time.Minute)
		if err := taskDeps.Store.Create(ctx, &task.Task{
			ID:          uuid.New(),
			TaskType:    "concurrent_test",
			TenantID:    "tenant-concurrent",
			Payload:     []byte(`{}`),
			Status:      task.StatusPending,
			Priority:    1,
			ScheduledAt: tm,
			MaxRetries:  3,
		}); err != nil {
			t.Fatalf("Create task %d: %v", i, err)
		}
	}

	// Run 3 concurrent scheduler goroutines
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var claimedCount int
	var claimedMu sync.Mutex

	workerDeps := worker.NewDependencies(
		worker.Config{Concurrency: 1, QueueName: broker.QueueLow},
		taskDeps.Store,
		brokerDeps.Broker,
	)
	w := worker.New(workerDeps)
	w.RegisterHandler("concurrent_test", func(ctx context.Context, taskType string, payload []byte) error {
		claimedMu.Lock()
		claimedCount++
		claimedMu.Unlock()
		return nil
	})

	go func() { _ = w.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			schedDeps := scheduler.NewDependencies(
				scheduler.Config{
					PollInterval: 50 * time.Millisecond,
					BatchSize:    10,
					JitterMax:    10 * time.Millisecond,
				},
				taskDeps.Store,
				brokerDeps.Broker,
			)
			s := scheduler.New(schedDeps)
			// Run briefly
			runCtx, runCancel := context.WithTimeout(ctx, 3*time.Second)
			defer runCancel()
			_ = s.Run(runCtx)
		}(i)
	}

	wg.Wait()

	deadline := time.After(10 * time.Second)
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
