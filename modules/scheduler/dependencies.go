package scheduler

import (
	"casper/modules/broker"
	"casper/modules/task"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Dependencies struct {
	Config Config
	Pool   *pgxpool.Pool
	Store  *task.Store
	Broker *broker.RabbitMQ
}

func NewDependencies(cfg Config, pool *pgxpool.Pool, store *task.Store, broker *broker.RabbitMQ) *Dependencies {
	return &Dependencies{
		Config: cfg,
		Pool:   pool,
		Store:  store,
		Broker: broker,
	}
}
