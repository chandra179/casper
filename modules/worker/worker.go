package worker

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	taskmod "casper/modules/task"
	brmod "casper/modules/broker"
)

type TaskHandler func(ctx context.Context, taskType string, payload []byte) error

type Worker struct {
	cfg      Config
	store    TaskStore
	broker   MessageBroker
	handlers map[string]TaskHandler
	cancel   context.CancelFunc
	mu       sync.RWMutex
}

type TaskStore interface {
	GetByID(ctx context.Context, id uuid.UUID) (*taskmod.Task, error)
	Complete(ctx context.Context, id uuid.UUID, version int64) error
	Fail(ctx context.Context, id uuid.UUID, version int64, errMsg string) error
	MarkProcessed(ctx context.Context, taskID uuid.UUID, workerID string) (bool, error)
}

type MessageBroker interface {
	Consume(queueName string) (<-chan amqp.Delivery, error)
	Ack(tag uint64) error
	Nack(tag uint64, requeue bool) error
}

func New(deps *Dependencies) *Worker {
	return &Worker{
		cfg:      deps.Config,
		store:    deps.Store,
		broker:   deps.Broker,
		handlers: make(map[string]TaskHandler),
	}
}

func NewWithInterfaces(cfg Config, store TaskStore, broker MessageBroker) *Worker {
	return &Worker{
		cfg:      cfg,
		store:    store,
		broker:   broker,
		handlers: make(map[string]TaskHandler),
	}
}

func (w *Worker) RegisterHandler(taskType string, handler TaskHandler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handlers[taskType] = handler
}

func (w *Worker) Run(ctx context.Context) error {
	ctx, w.cancel = context.WithCancel(ctx)
	defer w.cancel()

	deliveries, err := w.broker.Consume(w.cfg.QueueName)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	hostname, _ := os.Hostname()
	workerID := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	sem := make(chan struct{}, w.cfg.Concurrency)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("delivery channel closed")
			}
			sem <- struct{}{}
			go func(delivery amqp.Delivery) {
				defer func() { <-sem }()
				w.processMessage(ctx, delivery, workerID)
			}(d)
		}
	}
}

func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
}

func (w *Worker) processMessage(ctx context.Context, d amqp.Delivery, workerID string) {
	taskIDStr, _ := d.Headers["task_id"].(string)
	taskType, _ := d.Headers["task_type"].(string)

	if taskIDStr == "" {
		_ = w.broker.Ack(d.DeliveryTag)
		return
	}

	taskID, err := uuid.Parse(taskIDStr)
	if err != nil {
		_ = w.broker.Ack(d.DeliveryTag)
		return
	}

	isNew, err := w.store.MarkProcessed(ctx, taskID, workerID)
	if err != nil {
		_ = w.broker.Nack(d.DeliveryTag, true)
		return
	}
	if !isNew {
		_ = w.broker.Ack(d.DeliveryTag)
		return
	}

	tsk, err := w.store.GetByID(ctx, taskID)
	if err != nil || tsk == nil {
		_ = w.broker.Ack(d.DeliveryTag)
		return
	}

	w.mu.RLock()
	handler, ok := w.handlers[taskType]
	w.mu.RUnlock()

	if !ok {
		_ = w.store.Complete(ctx, taskID, tsk.Version)
		_ = w.broker.Ack(d.DeliveryTag)
		return
	}

	if err := handler(ctx, taskType, d.Body); err != nil {
		if failErr := w.store.Fail(ctx, taskID, tsk.Version, err.Error()); failErr != nil {
			_ = w.broker.Nack(d.DeliveryTag, true)
			return
		}
		_ = w.broker.Nack(d.DeliveryTag, false)
		return
	}

	if err := w.store.Complete(ctx, taskID, tsk.Version); err != nil {
		_ = w.broker.Nack(d.DeliveryTag, true)
		return
	}

	_ = w.broker.Ack(d.DeliveryTag)
}

// Ensure *taskmod.Store implements TaskStore
var _ TaskStore = (*taskmod.Store)(nil)
// Ensure *brmod.RabbitMQ implements MessageBroker
var _ MessageBroker = (*brmod.RabbitMQ)(nil)
