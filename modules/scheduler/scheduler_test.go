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

type mockStore struct {
	mu    sync.Mutex
	tasks []*task.Task
}

func (m *mockStore) Claim(ctx context.Context, claimedBy string, batchSize int) ([]*task.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var result []*task.Task
	remaining := make([]*task.Task, 0)
	for _, t := range m.tasks {
		if len(result) < batchSize {
			cp := *t
			cp.Status = task.StatusInProgress
			cp.Version++
			result = append(result, &cp)
		} else {
			remaining = append(remaining, t)
		}
	}
	m.tasks = remaining
	return result, nil
}

func (m *mockStore) addTask(tsk *task.Task) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks = append(m.tasks, tsk)
}

type mockBroker struct {
	mu       sync.Mutex
	messages []mockMessage
}

type mockMessage struct {
	routingKey string
	body       []byte
	priority   uint8
}

func (m *mockBroker) Publish(ctx context.Context, routingKey string, body []byte, priority uint8, headers map[string]interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, mockMessage{routingKey, body, priority})
	return nil
}

func TestSchedulerPollAndDispatch(t *testing.T) {
	store := &mockStore{}
	brk := &mockBroker{}

	t1 := &task.Task{
		ID:       uuid.New(),
		TaskType: "test",
		TenantID: "t1",
		Payload:  []byte(`{"x":1}`),
		Priority: 5,
	}
	t2 := &task.Task{
		ID:       uuid.New(),
		TaskType: "test",
		TenantID: "t1",
		Payload:  []byte(`{"x":2}`),
		Priority: 3,
	}
	store.addTask(t1)
	store.addTask(t2)

	sched := scheduler.NewWithInterfaces(
		scheduler.Config{
			PollInterval: 10 * time.Millisecond,
			BatchSize:    10,
			JitterMax:    0,
		},
		store, // store.Claim is a TaskClaimer
		brk,   // brk.Publish is a TaskPublisher
	)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	go func() {
		_ = sched.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)
	sched.Stop()

	brk.mu.Lock()
	msgs := make([]mockMessage, len(brk.messages))
	copy(msgs, brk.messages)
	brk.mu.Unlock()

	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}

	if msgs[0].routingKey != "tasks.medium" {
		t.Errorf("t1 routing: want tasks.medium, got %s", msgs[0].routingKey)
	}
	if msgs[1].routingKey != "tasks.low" {
		t.Errorf("t2 routing: want tasks.low, got %s", msgs[1].routingKey)
	}

	if string(msgs[0].body) != `{"x":1}` {
		t.Errorf("t1 body: want {\"x\":1}, got %s", string(msgs[0].body))
	}
	if string(msgs[1].body) != `{"x":2}` {
		t.Errorf("t2 body: want {\"x\":2}, got %s", string(msgs[1].body))
	}
}

func TestSchedulerEmptyPollBackoff(t *testing.T) {
	store := &mockStore{}
	brk := &mockBroker{}

	sched := scheduler.NewWithInterfaces(
		scheduler.Config{
			PollInterval: 10 * time.Millisecond,
			BatchSize:    10,
			JitterMax:    0,
		},
		store,
		brk,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_ = sched.Run(ctx)
	elapsed := time.Since(start)

	if elapsed < 10*time.Millisecond {
		t.Errorf("expected at least 10ms backoff, got %v", elapsed)
	}

	brk.mu.Lock()
	msgCount := len(brk.messages)
	brk.mu.Unlock()

	if msgCount != 0 {
		t.Errorf("expected 0 messages on empty poll, got %d", msgCount)
	}
}
