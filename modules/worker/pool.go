package worker

import (
	"context"
	"fmt"
	"os"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	brmod "casper/modules/broker"
)

type Pool struct {
	cfg      Config
	store    TaskStore
	broker   MessageBroker
	handlers map[string]TaskHandler
	mu       sync.RWMutex
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func NewPool(deps *Dependencies) *Pool {
	return NewPoolWithInterfaces(deps.Config, deps.Store, deps.Broker)
}

func NewPoolWithInterfaces(cfg Config, store TaskStore, broker MessageBroker) *Pool {
	return &Pool{
		cfg:      cfg,
		store:    store,
		broker:   broker,
		handlers: make(map[string]TaskHandler),
	}
}

func (p *Pool) RegisterHandler(taskType string, handler TaskHandler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handlers[taskType] = handler
}

func (p *Pool) Run(ctx context.Context) error {
	ctx, p.cancel = context.WithCancel(ctx)
	defer p.cancel()

	hostname, _ := os.Hostname()
	workerID := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	queues := []struct {
		name        string
		concurrency int
	}{
		{brmod.QueueHigh, p.cfg.HighConcurrency},
		{brmod.QueueMedium, p.cfg.MediumConcurrency},
		{brmod.QueueLow, p.cfg.LowConcurrency},
	}

	errCh := make(chan error, len(queues))

	for _, q := range queues {
		if q.concurrency <= 0 {
			continue
		}
		p.wg.Add(1)
		go func(queueName string, concurrency int) {
			defer p.wg.Done()
			if err := p.runConsumer(ctx, queueName, concurrency, workerID); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}(q.name, q.concurrency)
	}

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		p.wg.Wait()
		return ctx.Err()
	}
}

func (p *Pool) runConsumer(ctx context.Context, queueName string, concurrency int, workerID string) error {
	deliveries, err := p.broker.Consume(queueName)
	if err != nil {
		return fmt.Errorf("consume %s: %w", queueName, err)
	}

	sem := make(chan struct{}, concurrency)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("delivery channel closed for %s", queueName)
			}
			sem <- struct{}{}
			go func(delivery amqp.Delivery) {
				defer func() { <-sem }()
				processMessage(ctx, delivery, workerID, p.store, p.broker, p.handlers, &p.mu)
			}(d)
		}
	}
}

func (p *Pool) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
}
