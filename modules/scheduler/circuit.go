package scheduler

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"casper/modules/metrics"
	"casper/modules/task"
)

type CircuitState int

const (
	CircuitClosed CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "CLOSED"
	case CircuitOpen:
		return "OPEN"
	case CircuitHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}

type CircuitBreakerConfig struct {
	FailureThreshold     float64
	FailureWindow        time.Duration
	CircuitOpenDuration  time.Duration
	HalfOpenProbeCount   int
	MinimumNumberOfCalls int
	PollInterval         time.Duration
}

type tenantCircuit struct {
	state      CircuitState
	lastChange time.Time
	probeSent  int
}

type windowEntry struct {
	timestamp    time.Time
	failureCount int
	successCount int
}

type slidingWindow struct {
	entries       []windowEntry
	totalFailures int
	totalSuccess  int
}

func (w *slidingWindow) add(timestamp time.Time, successCount, failureCount int) {
	w.entries = append(w.entries, windowEntry{
		timestamp:    timestamp,
		failureCount: failureCount,
		successCount: successCount,
	})
	w.totalFailures += failureCount
	w.totalSuccess += successCount
}

func (w *slidingWindow) prune(cutoff time.Time) {
	keep := w.entries[:0]
	for _, e := range w.entries {
		if e.timestamp.After(cutoff) {
			keep = append(keep, e)
		} else {
			w.totalFailures -= e.failureCount
			w.totalSuccess -= e.successCount
		}
	}
	w.entries = keep
}

func (w *slidingWindow) totalCalls() int {
	return w.totalFailures + w.totalSuccess
}

func (w *slidingWindow) failureRate() float64 {
	total := w.totalCalls()
	if total == 0 {
		return 0
	}
	return float64(w.totalFailures) / float64(total)
}

type CircuitBreaker struct {
	mu       sync.RWMutex
	cfg      CircuitBreakerConfig
	circuits map[string]*tenantCircuit
	windows  map[string]*slidingWindow
	pool     *pgxpool.Pool
}

func NewCircuitBreaker(cfg CircuitBreakerConfig, pool *pgxpool.Pool) *CircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 0.9
	}
	if cfg.FailureWindow <= 0 {
		cfg.FailureWindow = 60 * time.Second
	}
	if cfg.CircuitOpenDuration <= 0 {
		cfg.CircuitOpenDuration = 5 * time.Minute
	}
	if cfg.HalfOpenProbeCount <= 0 {
		cfg.HalfOpenProbeCount = 1
	}
	if cfg.MinimumNumberOfCalls <= 0 {
		cfg.MinimumNumberOfCalls = 10
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	return &CircuitBreaker{
		cfg:      cfg,
		circuits: make(map[string]*tenantCircuit),
		windows:  make(map[string]*slidingWindow),
		pool:     pool,
	}
}

func (cb *CircuitBreaker) Allow(tenantID string) bool {
	cb.mu.RLock()
	tc, ok := cb.circuits[tenantID]
	cb.mu.RUnlock()

	if !ok {
		return true
	}

	if tc.state == CircuitClosed {
		return true
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	tc = cb.circuits[tenantID]
	if tc == nil {
		return true
	}

	switch tc.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		if time.Since(tc.lastChange) >= cb.cfg.CircuitOpenDuration {
			tc.state = CircuitHalfOpen
			tc.lastChange = time.Now()
			tc.probeSent = 1
			log.Printf("circuit breaker: tenant %s transitioned OPEN -> HALF_OPEN", tenantID)
			metrics.RecordCircuitBreakerTransition(tenantID, "OPEN", "HALF_OPEN")
			metrics.SetCircuitBreakerState(tenantID, 2)
			return true
		}
		return false
	case CircuitHalfOpen:
		if tc.probeSent < cb.cfg.HalfOpenProbeCount {
			tc.probeSent++
			return true
		}
		return false
	default:
		return true
	}
}

func (cb *CircuitBreaker) RecordOutcomes(tenantID string, successCount, failureCount int) {
	if successCount == 0 && failureCount == 0 {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()

	sw := cb.windows[tenantID]
	if sw == nil {
		sw = &slidingWindow{}
		cb.windows[tenantID] = sw
	}
	sw.add(now, successCount, failureCount)

	cutoff := now.Add(-cb.cfg.FailureWindow)
	sw.prune(cutoff)

	tc := cb.circuits[tenantID]

	totalCalls := sw.totalCalls()
	failureRate := sw.failureRate()

	switch {
	case tc == nil || tc.state == CircuitClosed:
		if totalCalls >= cb.cfg.MinimumNumberOfCalls && failureRate >= cb.cfg.FailureThreshold {
			cb.circuits[tenantID] = &tenantCircuit{
				state:      CircuitOpen,
				lastChange: now,
			}
			log.Printf("circuit breaker: tenant %s transitioned CLOSED -> OPEN (failure_rate=%.2f%%, total_calls=%d, threshold=%.2f%%)",
				tenantID, failureRate*100, totalCalls, cb.cfg.FailureThreshold*100)
			metrics.RecordCircuitBreakerTransition(tenantID, "CLOSED", "OPEN")
			metrics.SetCircuitBreakerState(tenantID, 1)
			return
		}

	case tc.state == CircuitHalfOpen:
		if failureCount > 0 {
			tc.state = CircuitOpen
			tc.lastChange = now
			tc.probeSent = 0
			log.Printf("circuit breaker: tenant %s transitioned HALF_OPEN -> OPEN (probe failed)", tenantID)
			metrics.RecordCircuitBreakerTransition(tenantID, "HALF_OPEN", "OPEN")
			metrics.SetCircuitBreakerState(tenantID, 1)
		} else if successCount > 0 {
			tc.state = CircuitClosed
			tc.lastChange = now
			cb.windows[tenantID] = &slidingWindow{}
			log.Printf("circuit breaker: tenant %s transitioned HALF_OPEN -> CLOSED (probe succeeded)", tenantID)
			metrics.RecordCircuitBreakerTransition(tenantID, "HALF_OPEN", "CLOSED")
			metrics.SetCircuitBreakerState(tenantID, 0)
		}
	}
}

func (cb *CircuitBreaker) Start(ctx context.Context) {
	if cb.pool == nil {
		return
	}
	go cb.runOutcomePoller(ctx)
}

func (cb *CircuitBreaker) runOutcomePoller(ctx context.Context) {
	ticker := time.NewTicker(cb.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cb.pollOutcomes(ctx)
		}
	}
}

func (cb *CircuitBreaker) pollOutcomes(ctx context.Context) {
	since := time.Now().Add(-cb.cfg.PollInterval * 2)
	outcomes, err := queryRecentOutcomes(ctx, cb.pool, since)
	if err != nil {
		return
	}

	for _, o := range outcomes {
		cb.RecordOutcomes(o.TenantID, o.Completed, o.DeadLettered)
	}
}

func queryRecentOutcomes(ctx context.Context, pool *pgxpool.Pool, since time.Time) ([]task.TenantOutcome, error) {
	rows, err := pool.Query(ctx, `
		SELECT tenant_id,
		       COUNT(*) FILTER (WHERE status = 'COMPLETED') as completed,
		       COUNT(*) FILTER (WHERE status = 'DEAD_LETTERED') as dead_lettered
		FROM tasks
		WHERE updated_at > $1
		  AND status IN ('COMPLETED', 'DEAD_LETTERED')
		GROUP BY tenant_id
	`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var outcomes []task.TenantOutcome
	for rows.Next() {
		var o task.TenantOutcome
		if err := rows.Scan(&o.TenantID, &o.Completed, &o.DeadLettered); err != nil {
			continue
		}
		outcomes = append(outcomes, o)
	}
	return outcomes, nil
}
