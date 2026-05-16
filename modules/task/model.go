package task

import (
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusPending      Status = "PENDING"
	StatusInProgress   Status = "IN_PROGRESS"
	StatusCompleted    Status = "COMPLETED"
	StatusFailed       Status = "FAILED"
	StatusDeadLettered Status = "DEAD_LETTERED"
)

type Task struct {
	ID           uuid.UUID  `json:"id"`
	TaskType     string     `json:"task_type"`
	TenantID     string     `json:"tenant_id"`
	Payload      []byte     `json:"payload"`
	Status       Status     `json:"status"`
	Priority     int        `json:"priority"`
	ScheduledAt  time.Time  `json:"scheduled_at"`
	MaxRetries   int        `json:"max_retries"`
	RetryCount   int        `json:"retry_count"`
	Version      int64      `json:"version"`
	ClaimedBy    *string    `json:"claimed_by,omitempty"`
	ClaimedAt    *time.Time `json:"claimed_at,omitempty"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	ErrorMessage *string    `json:"error_message,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

type TenantOutcome struct {
	TenantID     string
	Completed    int
	DeadLettered int
}
