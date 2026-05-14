package broker

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ExchangeName = "tasks"
	QueueHigh    = "tasks.high"
	QueueMedium  = "tasks.medium"
	QueueLow     = "tasks.low"
	QueueDead    = "tasks.dead"
)

type RabbitMQ struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	url      string
	prefetch int
	notifyCh chan *amqp.Error
	mu       sync.Mutex
	closed   bool
}

func NewRabbitMQ(ctx context.Context, url string, exchange string, prefetch int) (*RabbitMQ, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("amqp.Dial: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("conn.Channel: %w", err)
	}

	if err := ch.Qos(prefetch, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("ch.Qos: %w", err)
	}

	rmq := &RabbitMQ{
		conn:     conn,
		channel:  ch,
		url:      url,
		prefetch: prefetch,
		notifyCh: conn.NotifyClose(make(chan *amqp.Error, 1)),
	}

	if err := rmq.declareTopology(); err != nil {
		rmq.Close()
		return nil, fmt.Errorf("declareTopology: %w", err)
	}

	return rmq, nil
}

func (r *RabbitMQ) declareTopology() error {
	if err := r.channel.ExchangeDeclare(
		ExchangeName,
		"topic",
		true,  // durable
		false, // autoDelete
		false, // internal
		false, // noWait
		nil,
	); err != nil {
		return fmt.Errorf("exchange declare: %w", err)
	}

	deadLetterArgs := amqp.Table{
		"x-dead-letter-exchange":    ExchangeName,
		"x-dead-letter-routing-key": QueueDead,
	}

	queues := []struct {
		name    string
		routing string
		priority int
	}{
		{QueueHigh, "tasks.high", 10},
		{QueueMedium, "tasks.medium", 5},
		{QueueLow, "tasks.low", 0},
	}

	for _, q := range queues {
		args := amqp.Table{
			"x-max-priority": 10,
		}
		for k, v := range deadLetterArgs {
			args[k] = v
		}

		if _, err := r.channel.QueueDeclare(
			q.name,
			true,  // durable
			false, // autoDelete
			false, // exclusive
			false, // noWait
			args,
		); err != nil {
			return fmt.Errorf("queue declare %s: %w", q.name, err)
		}

		if err := r.channel.QueueBind(q.name, q.routing, ExchangeName, false, nil); err != nil {
			return fmt.Errorf("queue bind %s: %w", q.name, err)
		}
	}

	if _, err := r.channel.QueueDeclare(
		QueueDead,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("dead queue declare: %w", err)
	}
	if err := r.channel.QueueBind(QueueDead, QueueDead, ExchangeName, false, nil); err != nil {
		return fmt.Errorf("dead queue bind: %w", err)
	}

	return nil
}

func (r *RabbitMQ) Publish(ctx context.Context, routingKey string, body []byte, priority uint8, headers map[string]interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return fmt.Errorf("rabbitmq: connection closed")
	}

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Body:         body,
		Priority:     priority,
		Headers:      amqp.Table{},
	}

	if headers != nil {
		for k, v := range headers {
			msg.Headers[k] = v
		}
	}
	msg.Headers["x-priority"] = priority

	return r.channel.PublishWithContext(ctx,
		ExchangeName,
		routingKey,
		false, // mandatory
		false, // immediate
		msg,
	)
}

func (r *RabbitMQ) Consume(queueName string) (<-chan amqp.Delivery, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, fmt.Errorf("rabbitmq: connection closed")
	}

	return r.channel.Consume(
		queueName,
		"",    // consumer tag (auto-generated)
		false, // autoAck (we ack manually)
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
}

func (r *RabbitMQ) Ack(tag uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.channel.Ack(tag, false)
}

func (r *RabbitMQ) Nack(tag uint64, requeue bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.channel.Nack(tag, false, requeue)
}

func (r *RabbitMQ) NotifyClose() <-chan *amqp.Error {
	return r.notifyCh
}

func (r *RabbitMQ) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	r.channel.Close()
	r.conn.Close()
}

func (r *RabbitMQ) PurgeQueue(queueName string) (int, error) {
	q, err := r.channel.QueueInspect(queueName)
	if err != nil {
		return 0, err
	}
	purged, err := r.channel.QueuePurge(queueName, false)
	if err != nil {
		return 0, err
	}
	return q.Messages - purged, nil
}
