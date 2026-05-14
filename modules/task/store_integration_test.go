package task_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"encoding/json"

	"casper/modules/task"
)

func jsonEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var ja, jb interface{}
	if err := json.Unmarshal(a, &ja); err != nil {
		t.Fatalf("unmarshal a: %v", err)
	}
	if err := json.Unmarshal(b, &jb); err != nil {
		t.Fatalf("unmarshal b: %v", err)
	}
	equal := fmt.Sprintf("%v", ja) == fmt.Sprintf("%v", jb)
	return equal
}

func testConfig() task.PostgresConfig {
	host := os.Getenv("CASPER_DB_HOST")
	if host == "" {
		host = "localhost"
	}
	return task.PostgresConfig{
		Host:     host,
		Port:     5432,
		User:     "casper",
		Password: "casper",
		Database: "casper",
		SSLMode:  "disable",
	}
}

func setupStore(t *testing.T) (*task.Store, func()) {
	t.Helper()

	ctx := context.Background()
	deps, err := task.NewDependencies(ctx, testConfig())
	if err != nil {
		t.Skipf("postgres not available, skipping integration test: %v", err)
	}

	cleanup := func() {
		deps.Store.Exec(ctx, "DELETE FROM processed_tasks")
		deps.Store.Exec(ctx, "DELETE FROM tasks")
		deps.Close()
	}

	deps.Store.Exec(ctx, "DELETE FROM processed_tasks")
	deps.Store.Exec(ctx, "DELETE FROM tasks")

	return deps.Store, cleanup
}

func TestCreateAndGet(t *testing.T) {
	store, cleanup := setupStore(t)
	defer cleanup()

	ctx := context.Background()
	id := uuid.New()
	tm := time.Now().UTC().Truncate(time.Second)

	newTask := &task.Task{
		ID:          id,
		TaskType:    "send_email",
		TenantID:    "tenant-a",
		Payload:     []byte(`{"to":"user@example.com"}`),
		Status:      task.StatusPending,
		Priority:    5,
		ScheduledAt: tm,
		MaxRetries:  3,
	}

	err := store.Create(ctx, newTask)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := store.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got == nil {
		t.Fatal("expected task, got nil")
	}

	if got.ID != id {
		t.Errorf("id: want %s, got %s", id, got.ID)
	}
	if got.TaskType != "send_email" {
		t.Errorf("task_type: want send_email, got %s", got.TaskType)
	}
	if got.TenantID != "tenant-a" {
		t.Errorf("tenant_id: want tenant-a, got %s", got.TenantID)
	}
	if !jsonEqual(t, got.Payload, []byte(`{"to":"user@example.com"}`)) {
		t.Errorf("payload: got %s", string(got.Payload))
	}
	if got.Status != task.StatusPending {
		t.Errorf("status: want PENDING, got %s", got.Status)
	}
	if got.Priority != 5 {
		t.Errorf("priority: want 5, got %d", got.Priority)
	}
	if !got.ScheduledAt.Equal(tm) {
		t.Errorf("scheduled_at: want %v, got %v", tm, got.ScheduledAt)
	}
}

func TestClaim(t *testing.T) {
	store, cleanup := setupStore(t)
	defer cleanup()

	ctx := context.Background()

	for i := 0; i < 5; i++ {
		tm := time.Now().UTC().Add(-time.Minute)
		err := store.Create(ctx, &task.Task{
			ID:          uuid.New(),
			TaskType:    "test",
			TenantID:    "tenant-a",
			Payload:     []byte(`{}`),
			Status:      task.StatusPending,
			Priority:    i,
			ScheduledAt: tm,
			MaxRetries:  3,
		})
		if err != nil {
			t.Fatalf("Create task %d: %v", i, err)
		}
	}

	claimed, err := store.Claim(ctx, "scheduler-1", 3)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	if len(claimed) != 3 {
		t.Fatalf("claimed count: want 3, got %d", len(claimed))
	}

	for _, c := range claimed {
		if c.Status != task.StatusInProgress {
			t.Errorf("task %s status: want IN_PROGRESS, got %s", c.ID, c.Status)
		}
		if c.ClaimedBy == nil || *c.ClaimedBy != "scheduler-1" {
			t.Errorf("task %s claimed_by: want scheduler-1, got %v", c.ID, c.ClaimedBy)
		}
		if c.ClaimedAt == nil {
			t.Errorf("task %s claimed_at is nil", c.ID)
		}
		if c.Version != 1 {
			t.Errorf("task %s version: want 1, got %d", c.ID, c.Version)
		}

		got, err := store.GetByID(ctx, c.ID)
		if err != nil {
			t.Fatalf("GetByID %s: %v", c.ID, err)
		}
		if got.Status != task.StatusInProgress {
			t.Errorf("DB task %s status: want IN_PROGRESS, got %s", c.ID, got.Status)
		}
	}

	remaining, err := store.Claim(ctx, "scheduler-1", 5)
	if err != nil {
		t.Fatalf("Claim remaining: %v", err)
	}
	if len(remaining) != 2 {
		t.Errorf("remaining count: want 2, got %d", len(remaining))
	}
}

func TestConcurrentClaim(t *testing.T) {
	store, cleanup := setupStore(t)
	defer cleanup()

	ctx := context.Background()

	numTasks := 20
	for i := 0; i < numTasks; i++ {
		tm := time.Now().UTC().Add(-time.Minute)
		err := store.Create(ctx, &task.Task{
			ID:          uuid.New(),
			TaskType:    "test",
			TenantID:    "tenant-a",
			Payload:     []byte(`{}`),
			Status:      task.StatusPending,
			Priority:    1,
			ScheduledAt: tm,
			MaxRetries:  3,
		})
		if err != nil {
			t.Fatalf("Create task %d: %v", i, err)
		}
	}

	var claimedIDs sync.Map
	var wg sync.WaitGroup
	errCh := make(chan error, 4)

	for s := 0; s < 4; s++ {
		wg.Add(1)
		go func(schedulerID string) {
			defer wg.Done()
			claimed, err := store.Claim(ctx, schedulerID, 10)
			if err != nil {
				errCh <- err
				return
			}
			for _, c := range claimed {
				if _, loaded := claimedIDs.LoadOrStore(c.ID, schedulerID); loaded {
					errCh <- fmt.Errorf("duplicate claim on task %s by %s", c.ID, schedulerID)
					return
				}
			}
		}(fmt.Sprintf("scheduler-%d", s))
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatal(err)
	}

	count := 0
	claimedIDs.Range(func(_, _ interface{}) bool {
		count++
		return true
	})

	if count != numTasks {
		t.Errorf("total claimed: want %d, got %d", numTasks, count)
	}
}

func TestCompleteAndFail(t *testing.T) {
	store, cleanup := setupStore(t)
	defer cleanup()

	ctx := context.Background()

	tm := time.Now().UTC().Add(-time.Minute)
	tsk := &task.Task{
		ID:          uuid.New(),
		TaskType:    "test",
		TenantID:    "tenant-a",
		Payload:     []byte(`{}`),
		Status:      task.StatusPending,
		Priority:    1,
		ScheduledAt: tm,
		MaxRetries:  3,
	}
	if err := store.Create(ctx, tsk); err != nil {
		t.Fatalf("Create: %v", err)
	}

	claimed, err := store.Claim(ctx, "scheduler-1", 1)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected 1 claimed, got %d", len(claimed))
	}
	ct := claimed[0]

	if err := store.Complete(ctx, ct.ID, ct.Version); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	got, _ := store.GetByID(ctx, ct.ID)
	if got.Status != task.StatusCompleted {
		t.Errorf("status after complete: want COMPLETED, got %s", got.Status)
	}
	if got.CompletedAt == nil {
		t.Error("completed_at is nil after complete")
	}

	tsk2 := &task.Task{
		ID:          uuid.New(),
		TaskType:    "test",
		TenantID:    "tenant-a",
		Payload:     []byte(`{}`),
		Status:      task.StatusPending,
		Priority:    1,
		ScheduledAt: tm,
		MaxRetries:  2,
	}
	if err := store.Create(ctx, tsk2); err != nil {
		t.Fatalf("Create tsk2: %v", err)
	}

	claimed2, _ := store.Claim(ctx, "scheduler-1", 1)
	if len(claimed2) != 1 {
		t.Fatal("expected 1 claimed for tsk2")
	}

	if err := store.Fail(ctx, claimed2[0].ID, claimed2[0].Version, "something went wrong"); err != nil {
		t.Fatalf("Fail (should retry): %v", err)
	}

	got2, _ := store.GetByID(ctx, tsk2.ID)
	if got2.Status != task.StatusPending {
		t.Errorf("status after first fail: want PENDING, got %s", got2.Status)
	}
	if got2.RetryCount != 1 {
		t.Errorf("retry_count: want 1, got %d", got2.RetryCount)
	}
	if got2.ErrorMessage == nil || *got2.ErrorMessage != "something went wrong" {
		t.Errorf("error_message: want 'something went wrong', got %v", got2.ErrorMessage)
	}

	claimed2b, _ := store.Claim(ctx, "scheduler-1", 1)
	if len(claimed2b) != 1 {
		t.Fatal("expected 1 claimed for retry")
	}

	if err := store.Fail(ctx, claimed2b[0].ID, claimed2b[0].Version, "failed again"); err != nil {
		t.Fatalf("Fail (should dead letter): %v", err)
	}

	got2b, _ := store.GetByID(ctx, tsk2.ID)
	if got2b.Status != task.StatusDeadLettered {
		t.Errorf("status after second fail: want DEAD_LETTERED, got %s", got2b.Status)
	}
	if got2b.RetryCount != 2 {
		t.Errorf("retry_count: want 2, got %d", got2b.RetryCount)
	}
}

func TestDedup(t *testing.T) {
	store, cleanup := setupStore(t)
	defer cleanup()

	ctx := context.Background()
	taskID := uuid.New()

	first, err := store.MarkProcessed(ctx, taskID, "worker-1")
	if err != nil {
		t.Fatalf("MarkProcessed first: %v", err)
	}
	if !first {
		t.Error("first MarkProcessed should return true (inserted)")
	}

	isProcessed, err := store.IsProcessed(ctx, taskID)
	if err != nil {
		t.Fatalf("IsProcessed: %v", err)
	}
	if !isProcessed {
		t.Error("IsProcessed should return true")
	}

	second, err := store.MarkProcessed(ctx, taskID, "worker-2")
	if err != nil {
		t.Fatalf("MarkProcessed second: %v", err)
	}
	if second {
		t.Error("second MarkProcessed should return false (duplicate)")
	}
}

func TestGetByIDNotFound(t *testing.T) {
	store, cleanup := setupStore(t)
	defer cleanup()

	got, err := store.GetByID(context.Background(), uuid.New())
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got != nil {
		t.Error("expected nil for non-existent task")
	}
}
