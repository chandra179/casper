package scheduler

import (
	"context"
	"log"
	"time"

	"casper/modules/metrics"

	"github.com/jackc/pgx/v5/pgxpool"
)

type TaskCleaner interface {
	ReapStale(ctx context.Context, claimedBefore time.Time, batchSize int) (int, error)
}

func (s *Scheduler) runCleanup(ctx context.Context) {
	if s.pool == nil || s.cleaner == nil || s.cfg.CleanupInterval <= 0 || s.cfg.VisibilityTimeout <= 0 {
		return
	}

	for {
		s.tryBecomeCleanupLeader(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.cfg.CleanupInterval):
		}
	}
}

func (s *Scheduler) tryBecomeCleanupLeader(ctx context.Context) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		log.Printf("cleanup: acquire connection: %v", err)
		return
	}
	defer conn.Release()

	var locked bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock(hashtext('cleanup_leader'))").Scan(&locked); err != nil {
		log.Printf("cleanup: try_advisory_lock: %v", err)
		return
	}
	if !locked {
		metrics.SetCleanupLeaderElected(false)
		return
	}
	metrics.SetCleanupLeaderElected(true)
	defer func() {
		metrics.SetCleanupLeaderElected(false)
		if _, err := conn.Exec(context.Background(), "SELECT pg_advisory_unlock(hashtext('cleanup_leader'))"); err != nil {
			log.Printf("cleanup: advisory_unlock: %v", err)
		}
	}()

	log.Println("cleanup: acquired leader lock")
	s.reapLoop(ctx, conn)
}

func (s *Scheduler) reapLoop(ctx context.Context, conn *pgxpool.Conn) {
	ticker := time.NewTicker(s.cfg.CleanupInterval)
	defer ticker.Stop()

	for {
		cutoff := time.Now().Add(-s.cfg.VisibilityTimeout)
		n, err := s.cleaner.ReapStale(ctx, cutoff, s.cfg.CleanupBatchSize)
		if err != nil {
			log.Printf("cleanup: reap stale: %v", err)
			return
		}
		if n > 0 {
			log.Printf("cleanup: reaped %d stale tasks", n)
			metrics.RecordVisibilityTimeoutRecoveries(float64(n))
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
