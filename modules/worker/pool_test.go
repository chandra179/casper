package worker_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"casper/modules/broker"
	taskmod "casper/modules/task"
	"casper/modules/worker"
)

type multiQueueMockBroker struct {
	mu         sync.Mutex
	queues     map[string]chan amqp.Delivery
	acked      []uint64
	nacked     []uint64
	nackRequeue []bool
}

func newMultiQueueMockBroker() *multiQueueMockBroker {
	return &multiQueueMockBroker{
		queues: make(map[string]chan amqp.Delivery),
	}
}

func (m *multiQueueMockBroker) addQueue(name string, bufSize int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queues[name] = make(chan amqp.Delivery, bufSize)
}

func (m *multiQueueMockBroker) Consume(queueName string) (<-chan amqp.Delivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.queues[queueName], nil
}

func (m *multiQueueMockBroker) Ack(tag uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acked = append(m.acked, tag)
	return nil
}

func (m *multiQueueMockBroker) Nack(tag uint64, requeue bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nacked = append(m.nacked, tag)
	m.nackRequeue = append(m.nackRequeue, requeue)
	return nil
}

func (m *multiQueueMockBroker) sendDelivery(queueName string, d amqp.Delivery) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queues[queueName] <- d
}

func (m *multiQueueMockBroker) closeAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ch := range m.queues {
		close(ch)
	}
}

func TestPool_EachQueueGetsConfiguredShare(t *testing.T) {
	store := newMockStore()
	brk := newMultiQueueMockBroker()
	brk.addQueue(broker.QueueHigh, 20)
	brk.addQueue(broker.QueueMedium, 20)
	brk.addQueue(broker.QueueLow, 20)

	for _, q := range []struct {
		name       string
		taskType   string
		count      int
		priority   int
	}{
		{broker.QueueHigh, "high_task", 6, 8},
		{broker.QueueMedium, "med_task", 3, 5},
		{broker.QueueLow, "low_task", 3, 2},
	} {
		for i := 0; i < q.count; i++ {
			taskID := uuid.New()
			t1 := &taskmod.Task{
				ID:        taskID,
				TaskType:  q.taskType,
				TenantID:  "t1",
				Payload:   []byte(`{}`),
				Status:    taskmod.StatusInProgress,
				Priority:  q.priority,
				Version:   1,
				MaxRetries: 3,
			}
			store.addTask(t1)
			brk.sendDelivery(q.name, amqp.Delivery{
				DeliveryTag: uint64(i + 1),
				Body:        []byte(`{}`),
				Headers: amqp.Table{
					"task_id":   taskID.String(),
					"task_type": q.taskType,
					"tenant_id": "t1",
					"version":   int64(1),
				},
			})
		}
	}

	pool := worker.NewPoolWithInterfaces(
		worker.Config{
			Concurrency:       6,
			HighConcurrency:   3,
			MediumConcurrency: 2,
			LowConcurrency:    1,
		},
		store,
		brk,
	)

	var mu sync.Mutex
	type processed struct {
		highCount int
		medCount  int
		lowCount  int
	}
	var proc processed

	pool.RegisterHandler("high_task", func(ctx context.Context, taskType string, payload []byte) error {
		mu.Lock()
		proc.highCount++
		mu.Unlock()
		return nil
	})
	pool.RegisterHandler("med_task", func(ctx context.Context, taskType string, payload []byte) error {
		mu.Lock()
		proc.medCount++
		mu.Unlock()
		return nil
	})
	pool.RegisterHandler("low_task", func(ctx context.Context, taskType string, payload []byte) error {
		mu.Lock()
		proc.lowCount++
		mu.Unlock()
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = pool.Run(ctx) }()
	time.Sleep(300 * time.Millisecond)
	pool.Stop()

	mu.Lock()
	defer mu.Unlock()

	if proc.highCount != 6 {
		t.Errorf("high tasks: want 6, got %d", proc.highCount)
	}
	if proc.medCount != 3 {
		t.Errorf("medium tasks: want 3, got %d", proc.medCount)
	}
	if proc.lowCount != 3 {
		t.Errorf("low tasks: want 3, got %d (low-priority starved)", proc.lowCount)
	}
}

func TestPool_LowPriorityNeverStarves(t *testing.T) {
	store := newMockStore()
	brk := newMultiQueueMockBroker()
	brk.addQueue(broker.QueueHigh, 50)
	brk.addQueue(broker.QueueLow, 50)

	// Flood the high-priority queue
	for i := 0; i < 20; i++ {
		taskID := uuid.New()
		store.addTask(&taskmod.Task{
			ID:        taskID,
			TaskType:  "high",
			TenantID:  "t1",
			Payload:   []byte(`{}`),
			Status:    taskmod.StatusInProgress,
			Priority:  9,
			Version:   1,
			MaxRetries: 3,
		})
		brk.sendDelivery(broker.QueueHigh, amqp.Delivery{
			DeliveryTag: uint64(i + 1),
			Body:        []byte(`{}`),
			Headers:    amqp.Table{"task_id": taskID.String(), "task_type": "high", "tenant_id": "t1", "version": int64(1)},
		})
	}

	// Add to low-priority queue
	for i := 0; i < 5; i++ {
		taskID := uuid.New()
		store.addTask(&taskmod.Task{
			ID:        taskID,
			TaskType:  "low",
			TenantID:  "t1",
			Payload:   []byte(`{}`),
			Status:    taskmod.StatusInProgress,
			Priority:  1,
			Version:   1,
			MaxRetries: 3,
		})
		brk.sendDelivery(broker.QueueLow, amqp.Delivery{
			DeliveryTag: uint64(i + 100),
			Body:        []byte(`{}`),
			Headers:    amqp.Table{"task_id": taskID.String(), "task_type": "low", "tenant_id": "t1", "version": int64(1)},
		})
	}

	pool := worker.NewPoolWithInterfaces(
		worker.Config{
			Concurrency:       4,
			HighConcurrency:   3,
			LowConcurrency:    1,
		},
		store,
		brk,
	)

	var mu sync.Mutex
	highCount, lowCount := 0, 0

	pool.RegisterHandler("high", func(ctx context.Context, taskType string, payload []byte) error {
		mu.Lock()
		highCount++
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		return nil
	})
	pool.RegisterHandler("low", func(ctx context.Context, taskType string, payload []byte) error {
		mu.Lock()
		lowCount++
		mu.Unlock()
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = pool.Run(ctx) }()
	time.Sleep(500 * time.Millisecond)
	pool.Stop()

	mu.Lock()
	defer mu.Unlock()

	if lowCount == 0 {
		t.Error("low-priority tasks were starved: expected at least 1 processed")
	}
	if lowCount < 2 {
		t.Logf("low-priority tasks processed: %d (high: %d) — low share may be low due to single goroutine", lowCount, highCount)
	}
}
