package broker_test

import (
	"context"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"casper/modules/broker"
)

func testConfig() broker.Config {
	url := os.Getenv("CASPER_BROKER_URL")
	if url == "" {
		url = "amqp://casper:casper@localhost:5672/"
	}
	return broker.Config{
		URL:      url,
		Exchange: "tasks",
		Prefetch: 10,
	}
}

func setupBroker(t *testing.T) (*broker.RabbitMQ, func()) {
	t.Helper()

	cfg := testConfig()
	deps, err := broker.NewDependencies(context.Background(), cfg)
	if err != nil {
		t.Skipf("rabbitmq not available, skipping integration test: %v", err)
	}

	cleanup := func() {
		deps.Close()
	}

	return deps.Broker, cleanup
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

	var d amqp.Delivery
	select {
	case d = <-deliveries:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for first delivery")
	}

	if err := rmq.Nack(d.DeliveryTag, true); err != nil {
		t.Fatalf("Nack: %v", err)
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
