package task

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) Create(ctx context.Context, t *Task) error {
	if t.ID == uuid.Nil {
		t.ID = uuid.New()
	}
	if t.Status == "" {
		t.Status = StatusPending
	}
	t.CreatedAt = time.Now()
	t.UpdatedAt = time.Now()

	_, err := s.pool.Exec(ctx, `
		INSERT INTO tasks (id, task_type, tenant_id, payload, status, priority, scheduled_at, max_retries, retry_count, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`, t.ID, t.TaskType, t.TenantID, t.Payload, t.Status, t.Priority, t.ScheduledAt, t.MaxRetries, t.RetryCount, t.Version, t.CreatedAt, t.UpdatedAt)
	if err != nil {
		return fmt.Errorf("insert task: %w", err)
	}
	return nil
}

func (s *Store) GetByID(ctx context.Context, id uuid.UUID) (*Task, error) {
	var t Task
	var payload []byte

	err := s.pool.QueryRow(ctx, `
		SELECT id, task_type, tenant_id, payload, status, priority, scheduled_at,
		       max_retries, retry_count, version, claimed_by, claimed_at,
		       completed_at, error_message, created_at, updated_at
		FROM tasks WHERE id = $1
	`, id).Scan(
		&t.ID, &t.TaskType, &t.TenantID, &payload, &t.Status, &t.Priority, &t.ScheduledAt,
		&t.MaxRetries, &t.RetryCount, &t.Version, &t.ClaimedBy, &t.ClaimedAt,
		&t.CompletedAt, &t.ErrorMessage, &t.CreatedAt, &t.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("select task: %w", err)
	}

	t.Payload = payload
	return &t, nil
}

func (s *Store) Claim(ctx context.Context, claimedBy string, batchSize int) ([]*Task, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, task_type, tenant_id, payload, status, priority, scheduled_at,
		       max_retries, retry_count, version, created_at, updated_at
		FROM tasks
		WHERE status = 'PENDING' AND scheduled_at <= NOW()
		ORDER BY priority DESC, scheduled_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, batchSize)
	if err != nil {
		return nil, fmt.Errorf("select pending: %w", err)
	}

	var candidates []struct {
		task    Task
		payload []byte
	}
	for rows.Next() {
		var t Task
		var payload []byte
		if err := rows.Scan(
			&t.ID, &t.TaskType, &t.TenantID, &payload, &t.Status, &t.Priority, &t.ScheduledAt,
			&t.MaxRetries, &t.RetryCount, &t.Version, &t.CreatedAt, &t.UpdatedAt,
		); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan task: %w", err)
		}
		t.Payload = payload
		candidates = append(candidates, struct {
			task    Task
			payload []byte
		}{t, payload})
	}
	rows.Close()

	now := time.Now()
	var claimed []*Task
	for i := range candidates {
		t := &candidates[i].task
		oldVersion := t.Version
		t.Version++
		t.ClaimedBy = &claimedBy
		t.ClaimedAt = &now
		t.Status = StatusInProgress
		t.UpdatedAt = now

		ct, err := tx.Exec(ctx, `
			UPDATE tasks
			SET status = 'IN_PROGRESS', version = $1, claimed_by = $2, claimed_at = $3, updated_at = $4
			WHERE id = $5 AND version = $6 AND status = 'PENDING'
		`, t.Version, claimedBy, now, now, t.ID, oldVersion)
		if err != nil {
			return nil, fmt.Errorf("claim update: %w", err)
		}
		if ct.RowsAffected() == 0 {
			continue
		}

		claimed = append(claimed, t)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	return claimed, nil
}

func (s *Store) Complete(ctx context.Context, id uuid.UUID, version int64) error {
	now := time.Now()
	ct, err := s.pool.Exec(ctx, `
		UPDATE tasks
		SET status = 'COMPLETED', completed_at = $1, updated_at = $2, version = version + 1
		WHERE id = $3 AND version = $4
	`, now, now, id, version)
	if err != nil {
		return fmt.Errorf("complete task: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf("task %s version mismatch or not found", id)
	}
	return nil
}

func (s *Store) Fail(ctx context.Context, id uuid.UUID, version int64, errMsg string) error {
	now := time.Now()
	ct, err := s.pool.Exec(ctx, `
		UPDATE tasks
		SET status = CASE WHEN retry_count + 1 >= max_retries THEN 'DEAD_LETTERED' ELSE 'PENDING' END,
		    retry_count = retry_count + 1,
		    error_message = $1,
		    claimed_by = NULL,
		    claimed_at = NULL,
		    updated_at = $2,
		    version = version + 1
		WHERE id = $3 AND version = $4
	`, errMsg, now, id, version)
	if err != nil {
		return fmt.Errorf("fail task: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf("task %s version mismatch or not found", id)
	}
	return nil
}

func (s *Store) MarkProcessed(ctx context.Context, taskID uuid.UUID, workerID string) (bool, error) {
	ct, err := s.pool.Exec(ctx, `
		INSERT INTO processed_tasks (task_id, worker_id)
		VALUES ($1, $2)
		ON CONFLICT (task_id) DO NOTHING
	`, taskID, workerID)
	if err != nil {
		return false, fmt.Errorf("mark processed: %w", err)
	}
	return ct.RowsAffected() > 0, nil
}

func (s *Store) IsProcessed(ctx context.Context, taskID uuid.UUID) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM processed_tasks WHERE task_id = $1)
	`, taskID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check processed: %w", err)
	}
	return exists, nil
}

func (s *Store) ReapStale(ctx context.Context, claimedBefore time.Time, batchSize int) (int, error) {
	ct, err := s.pool.Exec(ctx, `
		UPDATE tasks
		SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL, updated_at = NOW(), version = version + 1
		WHERE id IN (
			SELECT id FROM tasks
			WHERE status = 'IN_PROGRESS' AND claimed_at < $1
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
	`, claimedBefore, batchSize)
	if err != nil {
		return 0, fmt.Errorf("reap stale: %w", err)
	}
	return int(ct.RowsAffected()), nil
}

func (s *Store) ResetTask(ctx context.Context, id uuid.UUID, version int64) error {
	ct, err := s.pool.Exec(ctx, `
		UPDATE tasks
		SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL,
		    version = version + 1, updated_at = NOW()
		WHERE id = $1 AND version = $2 AND status = 'IN_PROGRESS'
	`, id, version)
	if err != nil {
		return fmt.Errorf("reset task: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf("task %s version mismatch or not IN_PROGRESS", id)
	}
	return nil
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *Store) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	ct, err := s.pool.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return ct.RowsAffected(), nil
}
