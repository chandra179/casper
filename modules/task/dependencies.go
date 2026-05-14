package task

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Dependencies struct {
	Pool  *pgxpool.Pool
	Store *Store
}

func NewDependencies(ctx context.Context, cfg PostgresConfig) (*Dependencies, error) {
	pool, err := pgxpool.New(ctx, cfg.ConnectionString())
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pool.Ping: %w", err)
	}

	return &Dependencies{
		Pool:  pool,
		Store: NewStore(pool),
	}, nil
}

func (d *Dependencies) Close() {
	d.Pool.Close()
}
