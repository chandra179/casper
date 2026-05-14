package worker_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	taskmod "casper/modules/task"
	"casper/modules/worker"
)

type mockStore struct {
	mu              sync.Mutex
	tasks           map[uuid.UUID]*taskmod.Task
	processedTasks  map[uuid.UUID]bool
	completions     []uuid.UUID
	failures        []mockFailure
}

type mockFailure struct {
	id    uuid.UUID
	errMsg string
}

func newMockStore() *mockStore {
	return &mockStore{
		tasks:          make(map[uuid.UUID]*taskmod.Task),
		processedTasks: make(map[uuid.UUID]bool),
	}
}

func (m *mockStore) addTask(t *taskmod.Task) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks[t.ID] = t
}

func (m *mockStore) GetByID(ctx context.Context, id uuid.UUID) (*taskmod.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	if !ok {
		return nil, nil
	}
	return t, nil
}

func (m *mockStore) Complete(ctx context.Context, id uuid.UUID, version int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.tasks[id]
	t.Status = taskmod.StatusCompleted
	t.Version = version + 1
	m.completions = append(m.completions, id)
	return nil
}

func (m *mockStore) Fail(ctx context.Context, id uuid.UUID, version int64, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failures = append(m.failures, mockFailure{id, errMsg})
	t := m.tasks[id]
	t.Status = taskmod.StatusPending
	t.RetryCount++
	if t.RetryCount >= t.MaxRetries {
		t.Status = taskmod.StatusDeadLettered
	}
	return nil
}

func (m *mockStore) MarkProcessed(ctx context.Context, taskID uuid.UUID, workerID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.processedTasks[taskID] {
		return false, nil
	}
	m.processedTasks[taskID] = true
	return true, nil
}

type mockBroker struct {
	mu          sync.Mutex
	deliveries  chan amqp.Delivery
	acked       []uint64
	nacked      []uint64
	nackRequeue []bool
}

func newMockBroker() *mockBroker {
	return &mockBroker{
		deliveries: make(chan amqp.Delivery, 100),
	}
}

func (m *mockBroker) Consume(queueName string) (<-chan amqp.Delivery, error) {
	return m.deliveries, nil
}

func (m *mockBroker) Ack(tag uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acked = append(m.acked, tag)
	return nil
}

func (m *mockBroker) Nack(tag uint64, requeue bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nacked = append(m.nacked, tag)
	m.nackRequeue = append(m.nackRequeue, requeue)
	return nil
}

func (m *mockBroker) sendDelivery(d amqp.Delivery) {
	m.deliveries <- d
}

func TestWorkerExecuteAndComplete(t *testing.T) {
	store := newMockStore()
	brk := newMockBroker()

	taskID := uuid.New()
	t1 := &taskmod.Task{
		ID:       taskID,
		TaskType: "send_email",
		TenantID: "t1",
		Payload:  []byte(`{"to":"a@b.com"}`),
		Status:   taskmod.StatusInProgress,
		Version:  1,
	}
	store.addTask(t1)

	var handlerCalled bool
	handler := func(ctx context.Context, taskType string, payload []byte) error {
		handlerCalled = true
		return nil
	}

	w := worker.NewWithInterfaces(
		worker.Config{Concurrency: 1, QueueName: "test"},
		store,
		brk,
	)
	w.RegisterHandler("send_email", handler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	brk.sendDelivery(amqp.Delivery{
		DeliveryTag: 1,
		Body:        []byte(`{"to":"a@b.com"}`),
		Headers: amqp.Table{
			"task_id":   taskID.String(),
			"task_type": "send_email",
			"tenant_id": "t1",
			"version":   int64(1),
		},
	})

	time.Sleep(200 * time.Millisecond)
	w.Stop()

	if !handlerCalled {
		t.Error("handler was not called")
	}
	if len(brk.acked) != 1 {
		t.Errorf("expected 1 ack, got %d", len(brk.acked))
	}
	if len(store.completions) != 1 {
		t.Errorf("expected 1 completion, got %d", len(store.completions))
	}
	if store.completions[0] != taskID {
		t.Errorf("expected taskID %s, got %s", taskID, store.completions[0])
	}
}

func TestWorkerDedup(t *testing.T) {
	store := newMockStore()
	brk := newMockBroker()

	taskID := uuid.New()
	t1 := &taskmod.Task{
		ID:       taskID,
		TaskType: "test",
		TenantID: "t1",
		Payload:  []byte(`{}`),
		Status:   taskmod.StatusInProgress,
		Version:  1,
	}
	store.addTask(t1)

	store.processedTasks[taskID] = true

	var handlerCalled bool
	handler := func(ctx context.Context, taskType string, payload []byte) error {
		handlerCalled = true
		return nil
	}

	w := worker.NewWithInterfaces(
		worker.Config{Concurrency: 1, QueueName: "test"},
		store,
		brk,
	)
	w.RegisterHandler("test", handler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	brk.sendDelivery(amqp.Delivery{
		DeliveryTag: 1,
		Body:        []byte(`{}`),
		Headers: amqp.Table{
			"task_id":   taskID.String(),
			"task_type": "test",
		},
	})

	time.Sleep(200 * time.Millisecond)
	w.Stop()

	if handlerCalled {
		t.Error("handler should NOT be called for duplicate task")
	}
	if len(brk.acked) != 1 {
		t.Errorf("expected 1 ack, got %d", len(brk.acked))
	}
	if len(store.completions) != 0 {
		t.Errorf("expected 0 completions, got %d", len(store.completions))
	}
}

func TestWorkerHandlerFailure(t *testing.T) {
	store := newMockStore()
	brk := newMockBroker()

	taskID := uuid.New()
	t1 := &taskmod.Task{
		ID:         taskID,
		TaskType:   "failing_task",
		TenantID:   "t1",
		Payload:    []byte(`{}`),
		Status:     taskmod.StatusInProgress,
		Version:    1,
		MaxRetries: 3,
	}
	store.addTask(t1)

	handler := func(ctx context.Context, taskType string, payload []byte) error {
		return &testError{"handler error"}
	}

	w := worker.NewWithInterfaces(
		worker.Config{Concurrency: 1, QueueName: "test"},
		store,
		brk,
	)
	w.RegisterHandler("failing_task", handler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	brk.sendDelivery(amqp.Delivery{
		DeliveryTag: 1,
		Body:        []byte(`{}`),
		Headers: amqp.Table{
			"task_id":   taskID.String(),
			"task_type": "failing_task",
		},
	})

	time.Sleep(200 * time.Millisecond)
	w.Stop()

	if len(store.failures) != 1 {
		t.Errorf("expected 1 failure, got %d", len(store.failures))
	}
	if store.failures[0].id != taskID {
		t.Errorf("expected taskID %s, got %s", taskID, store.failures[0].id)
	}
	if len(brk.nacked) != 1 {
		t.Errorf("expected 1 nack, got %d", len(brk.nacked))
	}
}

func TestWorkerMaxRetriesDeadLetter(t *testing.T) {
	store := newMockStore()
	brk := newMockBroker()

	taskID := uuid.New()
	t1 := &taskmod.Task{
		ID:         taskID,
		TaskType:   "doomed_task",
		TenantID:   "t1",
		Payload:    []byte(`{}`),
		Status:     taskmod.StatusInProgress,
		Version:    1,
		MaxRetries: 1,
		RetryCount: 1,
	}
	store.addTask(t1)

	handler := func(ctx context.Context, taskType string, payload []byte) error {
		return &testError{"fatal error"}
	}

	w := worker.NewWithInterfaces(
		worker.Config{Concurrency: 1, QueueName: "test"},
		store,
		brk,
	)
	w.RegisterHandler("doomed_task", handler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	brk.sendDelivery(amqp.Delivery{
		DeliveryTag: 1,
		Body:        []byte(`{}`),
		Headers: amqp.Table{
			"task_id":   taskID.String(),
			"task_type": "doomed_task",
		},
	})

	time.Sleep(200 * time.Millisecond)
	w.Stop()

	store.mu.Lock()
	tsk := store.tasks[taskID]
	store.mu.Unlock()

	if tsk == nil {
		t.Fatal("task disappeared")
	}
	if tsk.Status != taskmod.StatusDeadLettered {
		t.Errorf("status: want DEAD_LETTERED, got %s", tsk.Status)
	}
}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }
