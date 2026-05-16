package scheduler

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"casper/modules/broker"
	"casper/modules/metrics"
	"casper/modules/task"
)

type TaskClaimer interface {
	Claim(ctx context.Context, claimedBy string, batchSize int, ageBonusPerHour float64) ([]*task.Task, error)
}

type TaskPublisher interface {
	Publish(ctx context.Context, routingKey string, body []byte, priority uint8, headers map[string]interface{}) error
}

type Scheduler struct {
	cfg       Config
	claimer   TaskClaimer
	publisher TaskPublisher
	pool      *pgxpool.Pool
	resetter  TaskResetter
	cleaner   TaskCleaner
	cancel    context.CancelFunc
}

func New(deps *Dependencies) *Scheduler {
	return &Scheduler{
		cfg:       deps.Config,
		claimer:   deps.Store,
		publisher: deps.Broker,
		pool:      deps.Pool,
		resetter:  deps.Store,
		cleaner:   deps.Store,
	}
}

func NewWithInterfaces(cfg Config, claimer TaskClaimer, publisher TaskPublisher) *Scheduler {
	return &Scheduler{
		cfg:       cfg,
		claimer:   claimer,
		publisher: publisher,
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	ctx, s.cancel = context.WithCancel(ctx)
	defer s.cancel()

	go s.runCleanup(ctx)

	go s.runPendingDepthUpdater(ctx)

	backoff := time.Duration(0)
	maxBackoff := s.cfg.PollInterval * 10

	instanceID := fmt.Sprintf("scheduler-%d", rand.Intn(10000))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		claimed, err := s.claimer.Claim(ctx, instanceID, s.cfg.BatchSize, s.cfg.AgeBonusPerHour)
		if err != nil {
			return fmt.Errorf("claim: %w", err)
		}

		if len(claimed) == 0 {
			if backoff == 0 {
				backoff = s.cfg.PollInterval
			} else {
				backoff = time.Duration(math.Min(float64(backoff*2), float64(maxBackoff)))
			}

			jitter := time.Duration(0)
			if s.cfg.JitterMax > 0 {
				jitter = time.Duration(rand.Int63n(int64(s.cfg.JitterMax)))
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff + jitter):
			}
			continue
		}

		backoff = 0

		for _, t := range claimed {
			metrics.RecordTaskClaimed(t.TenantID, t.Priority)
		}

		for i, t := range claimed {
			select {
			case <-ctx.Done():
				s.drainRemaining(ctx, claimed[i:])
				return ctx.Err()
			default:
			}

			if s.cfg.JitterMax > 0 {
				jitter := time.Duration(rand.Int63n(int64(s.cfg.JitterMax)))
				time.Sleep(jitter)
			}

			if err := s.publishTask(ctx, t); err != nil {
				return fmt.Errorf("publish task %s: %w", t.ID, err)
			}
		}
	}
}

func (s *Scheduler) publishTask(ctx context.Context, t *task.Task) error {
	routingKey := priorityRoutingKey(t.Priority)

	headers := map[string]interface{}{
		"task_id":   t.ID.String(),
		"task_type": t.TaskType,
		"tenant_id": t.TenantID,
		"priority":  t.Priority,
		"version":   t.Version,
	}

	priority := intPriority(t.Priority)

	if err := s.publisher.Publish(ctx, routingKey, t.Payload, priority, headers); err != nil {
		return err
	}
	metrics.RecordTaskDispatched(t.TenantID, t.Priority)
	return nil
}

func (s *Scheduler) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
}

func priorityRoutingKey(priority int) string {
	if priority >= 7 {
		return broker.QueueHigh
	}
	if priority >= 4 {
		return broker.QueueMedium
	}
	return broker.QueueLow
}

func intPriority(priority int) uint8 {
	if priority > 255 {
		return 255
	}
	if priority < 0 {
		return 0
	}
	return uint8(priority)
}

func (s *Scheduler) runPendingDepthUpdater(ctx context.Context) {
	if s.pool == nil {
		return
	}

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.updatePendingDepth(ctx)
		}
	}
}

func (s *Scheduler) updatePendingDepth(ctx context.Context) {
	rows, err := s.pool.Query(ctx, `SELECT tenant_id, COUNT(*) FROM tasks WHERE status = 'PENDING' GROUP BY tenant_id`)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var tenantID string
		var count int
		if err := rows.Scan(&tenantID, &count); err != nil {
			continue
		}
		metrics.SetPendingQueueDepth(tenantID, float64(count))
	}
}
