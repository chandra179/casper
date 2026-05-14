package testhelper

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"github.com/testcontainers/testcontainers-go/wait"
)

type PostgresContainer struct {
	URI string
	ctr *postgres.PostgresContainer
}

func SetupPostgres(t *testing.T) *PostgresContainer {
	t.Helper()
	ctx := context.Background()

	ctr, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("casper"),
		postgres.WithUsername("casper"),
		postgres.WithPassword("casper"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("postgres testcontainer: %v", err)
	}

	uri, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}

	t.Cleanup(func() {
		if err := ctr.Terminate(ctx); err != nil {
			t.Logf("postgres terminate: %v", err)
		}
	})

	return &PostgresContainer{URI: uri, ctr: ctr}
}

func (p *PostgresContainer) Cfg() string { return p.URI }

type RabbitMQContainer struct {
	URI string
	ctr *rabbitmq.RabbitMQContainer
}

func SetupRabbitMQ(t *testing.T) *RabbitMQContainer {
	t.Helper()
	ctx := context.Background()

	ctr, err := rabbitmq.Run(ctx,
		"rabbitmq:3.13-management-alpine",
		rabbitmq.WithAdminUsername("casper"),
		rabbitmq.WithAdminPassword("casper"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Server startup complete").
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("rabbitmq testcontainer: %v", err)
	}

	uri, err := ctr.AmqpURL(ctx)
	if err != nil {
		t.Fatalf("rabbitmq amqp url: %v", err)
	}

	t.Cleanup(func() {
		if err := ctr.Terminate(ctx); err != nil {
			t.Logf("rabbitmq terminate: %v", err)
		}
	})

	return &RabbitMQContainer{URI: uri, ctr: ctr}
}
