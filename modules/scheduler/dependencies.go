package scheduler

import (
	"casper/modules/broker"
	"casper/modules/task"
)

type Dependencies struct {
	Config Config
	Store  *task.Store
	Broker *broker.RabbitMQ
}

func NewDependencies(cfg Config, store *task.Store, broker *broker.RabbitMQ) *Dependencies {
	return &Dependencies{
		Config: cfg,
		Store:  store,
		Broker: broker,
	}
}
