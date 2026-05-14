package broker_test

import (
	"context"
	"testing"
	"time"

	"casper/internal/testhelper"
	"casper/modules/broker"
)

func setupBroker(t *testing.T) (*broker.RabbitMQ, func()) {
	t.Helper()

	rmq := testhelper.SetupRabbitMQ(t)
	cfg := broker.Config{
		URI:      rmq.URI,
		Exchange: "tasks",
		Prefetch: 10,
	}

	deps, err := broker.NewDependencies(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewDependencies: %v", err)
	}

	return deps.Broker, deps.Close
}

func TestPublishAndConsume(t *testing.T) {
	rmq, cleanup := setupBroker(t)
	defer cleanup()

	ctx := context.Background()

	err := rmq.Publish(ctx, "tasks.high", []byte(`{"msg":"hello"}`), 10, nil)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deliveries, err := rmq.Consume(broker.QueueHigh)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	select {
	case d := <-deliveries:
		if string(d.Body) != `{"msg":"hello"}` {
			t.Errorf("body: want '{\"msg\":\"hello\"}', got %s", string(d.Body))
		}
		if d.Priority != 10 {
			t.Errorf("priority: want 10, got %d", d.Priority)
		}
		if err := rmq.Ack(d.DeliveryTag); err != nil {
			t.Errorf("Ack: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestNackRequeue(t *testing.T) {
	rmq, cleanup := setupBroker(t)
	defer cleanup()

	ctx := context.Background()

	if err := rmq.Publish(ctx, "tasks.high", []byte(`{"msg":"retry"}`), 5, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deliveries, err := rmq.Consume(broker.QueueHigh)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	select {
	case d := <-deliveries:
		if err := rmq.Nack(d.DeliveryTag, true); err != nil {
			t.Fatalf("Nack: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for first delivery")
	}

	select {
	case d2 := <-deliveries:
		if string(d2.Body) != `{"msg":"retry"}` {
			t.Errorf("body: got %s", string(d2.Body))
		}
		if err := rmq.Ack(d2.DeliveryTag); err != nil {
			t.Errorf("Ack: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for requeued message")
	}
}

func TestPriorityOrdering(t *testing.T) {
	rmq, cleanup := setupBroker(t)
	defer cleanup()

	ctx := context.Background()

	if err := rmq.Publish(ctx, "tasks.high", []byte(`low`), 0, nil); err != nil {
		t.Fatalf("Publish low: %v", err)
	}
	if err := rmq.Publish(ctx, "tasks.high", []byte(`high`), 10, nil); err != nil {
		t.Fatalf("Publish high: %v", err)
	}

	deliveries, err := rmq.Consume(broker.QueueHigh)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	var bodies []string
	for i := 0; i < 2; i++ {
		select {
		case d := <-deliveries:
			bodies = append(bodies, string(d.Body))
			rmq.Ack(d.DeliveryTag)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for message")
		}
	}

	if len(bodies) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(bodies))
	}
	if bodies[0] != "high" {
		t.Errorf("first msg should be high, got %s", bodies[0])
	}
	if bodies[1] != "low" {
		t.Errorf("second msg should be low, got %s", bodies[1])
	}
}
