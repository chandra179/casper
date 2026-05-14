package broker

import "context"

type Dependencies struct {
	Config Config
	Broker *RabbitMQ
}

func NewDependencies(ctx context.Context, cfg Config) (*Dependencies, error) {
	rmq, err := NewRabbitMQ(ctx, cfg.ConnectionURL(), cfg.Exchange, cfg.Prefetch)
	if err != nil {
		return nil, err
	}

	return &Dependencies{
		Config: cfg,
		Broker: rmq,
	}, nil
}

func (d *Dependencies) Close() {
	d.Broker.Close()
}
