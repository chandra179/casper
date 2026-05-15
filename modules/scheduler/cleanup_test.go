package scheduler_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"casper/modules/scheduler"
	"casper/modules/task"
)

type mockCleaner struct {
	mu           sync.Mutex
	staleTasks   map[uuid.UUID]*task.Task
	reapedIDs    []uuid.UUID
	reapErr      error
	resetCount   int
	resetErr     error
	resetIDs     []uuid.UUID
}

func (m *mockCleaner) Claim(ctx context.Context, claimedBy string, batchSize int) ([]*task.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var result []*task.Task
	for _, t := range m.staleTasks {
		if len(result) >= batchSize {
			break
		}
		cp := *t
		cp.Status = task.StatusInProgress
		cp.Version++
		result = append(result, &cp)
	}
	return result, nil
}

func (m *mockCleaner) ReapStale(ctx context.Context, claimedBefore time.Time, batchSize int) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.reapErr != nil {
		return 0, m.reapErr
	}

	count := 0
	for id, t := range m.staleTasks {
		if t.ClaimedAt != nil && t.ClaimedAt.Before(claimedBefore) {
			m.reapedIDs = append(m.reapedIDs, id)
			count++
		}
	}
	m.staleTasks = make(map[uuid.UUID]*task.Task)
	return count, nil
}

func (m *mockCleaner) ResetTask(ctx context.Context, id uuid.UUID, version int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.resetErr != nil {
		return m.resetErr
	}
	m.resetCount++
	m.resetIDs = append(m.resetIDs, id)
	return nil
}

func newStaleTask() *task.Task {
	now := time.Now()
	claimedAt := now.Add(-10 * time.Minute)
	return &task.Task{
		ID:          uuid.New(),
		TaskType:    "test",
		TenantID:    "t1",
		Payload:     []byte(`{}`),
		Status:      task.StatusInProgress,
		Priority:    5,
		ClaimedBy:   stringPtr("scheduler-1"),
		ClaimedAt:   &claimedAt,
		Version:     1,
		ScheduledAt: now.Add(-15 * time.Minute),
		MaxRetries:  3,
	}
}

func newRecentTask() *task.Task {
	now := time.Now()
	claimedAt := now.Add(-1 * time.Minute)
	return &task.Task{
		ID:          uuid.New(),
		TaskType:    "test",
		TenantID:    "t1",
		Payload:     []byte(`{}`),
		Status:      task.StatusInProgress,
		Priority:    5,
		ClaimedBy:   stringPtr("scheduler-1"),
		ClaimedAt:   &claimedAt,
		Version:     1,
		ScheduledAt: now.Add(-2 * time.Minute),
		MaxRetries:  3,
	}
}

func stringPtr(s string) *string { return &s }

func TestReapStaleTasks(t *testing.T) {
	cleaner := &mockCleaner{
		staleTasks: make(map[uuid.UUID]*task.Task),
	}

	stale := newStaleTask()
	cleaner.staleTasks[stale.ID] = stale

	cutoff := time.Now().Add(-5 * time.Minute)
	n, err := cleaner.ReapStale(context.Background(), cutoff, 100)
	if err != nil {
		t.Fatalf("ReapStale: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 reaped, got %d", n)
	}
	if len(cleaner.reapedIDs) != 1 || cleaner.reapedIDs[0] != stale.ID {
		t.Error("expected stale task to be reaped")
	}
}

func TestReapStaleSkipsRecent(t *testing.T) {
	cleaner := &mockCleaner{
		staleTasks: make(map[uuid.UUID]*task.Task),
	}

	recent := newRecentTask()
	cleaner.staleTasks[recent.ID] = recent

	cutoff := time.Now().Add(-5 * time.Minute)
	n, err := cleaner.ReapStale(context.Background(), cutoff, 100)
	if err != nil {
		t.Fatalf("ReapStale: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 reaped (task recently claimed), got %d", n)
	}
}

func TestDrainDispatchesAllTasks(t *testing.T) {
	t1 := &task.Task{
		ID:       uuid.New(),
		TaskType: "drain_test",
		TenantID: "t1",
		Payload:  []byte(`{"test":1}`),
		Priority: 5,
		Version:  2,
		Status:   task.StatusInProgress,
	}
	t2 := &task.Task{
		ID:       uuid.New(),
		TaskType: "drain_test",
		TenantID: "t1",
		Payload:  []byte(`{"test":2}`),
		Priority: 5,
		Version:  3,
		Status:   task.StatusInProgress,
	}

	brk := &mockBroker{}
	resetter := &mockCleaner{}

	cfg := scheduler.Config{
		ShutdownDrainTimeout: 500 * time.Millisecond,
	}
	sched := scheduler.NewWithInterfaces(cfg, nil, brk)
	sched.DrainForTesting(resetter, []*task.Task{t1, t2})

	brk.mu.Lock()
	msgCount := len(brk.messages)
	brk.mu.Unlock()

	if msgCount != 2 {
		t.Errorf("expected 2 drain publishes, got %d", msgCount)
	}
	if resetter.resetCount != 0 {
		t.Errorf("expected 0 resets, got %d", resetter.resetCount)
	}
}

type blockingBroker struct {
	mu       sync.Mutex
	messages []mockMessage
	pubDelay time.Duration
}

func (m *blockingBroker) Publish(ctx context.Context, routingKey string, body []byte, priority uint8, headers map[string]interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(m.pubDelay):
	}
	m.mu.Lock()
	m.messages = append(m.messages, mockMessage{routingKey, body, priority})
	m.mu.Unlock()
	return nil
}

func TestDrainReleasesOnTimeout(t *testing.T) {
	t1 := &task.Task{
		ID:       uuid.New(),
		TaskType: "drain_timeout",
		TenantID: "t1",
		Payload:  []byte(`{"test":1}`),
		Priority: 5,
		Version:  2,
		Status:   task.StatusInProgress,
	}
	t2 := &task.Task{
		ID:       uuid.New(),
		TaskType: "drain_timeout",
		TenantID: "t1",
		Payload:  []byte(`{"test":2}`),
		Priority: 5,
		Version:  3,
		Status:   task.StatusInProgress,
	}

	brk := &blockingBroker{pubDelay: 200 * time.Millisecond}
	resetter := &mockCleaner{}

	cfg := scheduler.Config{
		ShutdownDrainTimeout: 50 * time.Millisecond,
	}
	sched := scheduler.NewWithInterfaces(cfg, nil, brk)
	sched.DrainForTesting(resetter, []*task.Task{t1, t2})

	if resetter.resetCount < 1 {
		t.Errorf("expected at least 1 task released on timeout, got %d resets", resetter.resetCount)
	}
}
