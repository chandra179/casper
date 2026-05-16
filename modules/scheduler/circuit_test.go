package scheduler_test

import (
	"testing"
	"time"

	"casper/modules/scheduler"
)

func TestCircuitBreakerClosedAllowsAll(t *testing.T) {
	cb := scheduler.NewCircuitBreaker(scheduler.CircuitBreakerConfig{
		FailureThreshold:     0.9,
		FailureWindow:        10 * time.Second,
		CircuitOpenDuration:  100 * time.Millisecond,
		HalfOpenProbeCount:   1,
		MinimumNumberOfCalls: 10,
	}, nil)

	for i := 0; i < 100; i++ {
		if !cb.Allow("t1") {
			t.Fatal("expected Allow to return true for closed circuit")
		}
	}
}

func TestCircuitBreakerOpensOnHighFailureRate(t *testing.T) {
	cb := scheduler.NewCircuitBreaker(scheduler.CircuitBreakerConfig{
		FailureThreshold:     0.5,
		FailureWindow:        10 * time.Second,
		CircuitOpenDuration:  5 * time.Minute,
		HalfOpenProbeCount:   1,
		MinimumNumberOfCalls: 5,
	}, nil)

	cb.RecordOutcomes("t1", 1, 9)

	if cb.Allow("t1") {
		t.Fatal("expected circuit to be OPEN after high failure rate")
	}
}

func TestCircuitBreakerStaysClosedBelowThreshold(t *testing.T) {
	cb := scheduler.NewCircuitBreaker(scheduler.CircuitBreakerConfig{
		FailureThreshold:     0.9,
		FailureWindow:        10 * time.Second,
		CircuitOpenDuration:  5 * time.Minute,
		HalfOpenProbeCount:   1,
		MinimumNumberOfCalls: 5,
	}, nil)

	cb.RecordOutcomes("t1", 8, 2)

	if !cb.Allow("t1") {
		t.Fatal("expected circuit to remain CLOSED when below threshold")
	}
}

func TestCircuitBreakerNotEnoughCalls(t *testing.T) {
	cb := scheduler.NewCircuitBreaker(scheduler.CircuitBreakerConfig{
		FailureThreshold:     0.5,
		FailureWindow:        10 * time.Second,
		CircuitOpenDuration:  5 * time.Minute,
		HalfOpenProbeCount:   1,
		MinimumNumberOfCalls: 10,
	}, nil)

	cb.RecordOutcomes("t1", 0, 9)

	if !cb.Allow("t1") {
		t.Fatal("expected circuit to remain CLOSED when minimum calls not met")
	}
}

func TestCircuitBreakerHalfOpensAfterTimeout(t *testing.T) {
	cb := scheduler.NewCircuitBreaker(scheduler.CircuitBreakerConfig{
		FailureThreshold:     0.5,
		FailureWindow:        10 * time.Second,
		CircuitOpenDuration:  10 * time.Millisecond,
		HalfOpenProbeCount:   1,
		MinimumNumberOfCalls: 5,
	}, nil)

	cb.RecordOutcomes("t1", 1, 9)

	if cb.Allow("t1") {
		t.Fatal("expected circuit to be OPEN")
	}

	time.Sleep(15 * time.Millisecond)

	if !cb.Allow("t1") {
		t.Fatal("expected circuit to be HALF_OPEN and allow probe after timeout")
	}
}

func TestCircuitBreakerHalfOpenAllowsOnlyProbeCount(t *testing.T) {
	cb := scheduler.NewCircuitBreaker(scheduler.CircuitBreakerConfig{
		FailureThreshold:     0.5,
		FailureWindow:        10 * time.Second,
		CircuitOpenDuration:  10 * time.Millisecond,
		HalfOpenProbeCount:   2,
		MinimumNumberOfCalls: 5,
	}, nil)

	cb.RecordOutcomes("t1", 1, 9)

	time.Sleep(15 * time.Millisecond)

	if !cb.Allow("t1") {
		t.Fatal("expected first probe to be allowed")
	}

	if !cb.Allow("t1") {
		t.Fatal("expected second probe to be allowed")
	}

	if cb.Allow("t1") {
		t.Fatal("expected third probe to be denied")
	}
}

func TestCircuitBreakerClosesOnSuccessfulProbe(t *testing.T) {
	cb := scheduler.NewCircuitBreaker(scheduler.CircuitBreakerConfig{
		FailureThreshold:     0.5,
		FailureWindow:        10 * time.Second,
		CircuitOpenDuration:  10 * time.Millisecond,
		HalfOpenProbeCount:   1,
		MinimumNumberOfCalls: 5,
	}, nil)

	cb.RecordOutcomes("t1", 1, 9)

	time.Sleep(15 * time.Millisecond)

	if !cb.Allow("t1") {
		t.Fatal("expected probe to be allowed in HALF_OPEN")
	}

	cb.RecordOutcomes("t1", 1, 0)

	if !cb.Allow("t1") {
		t.Fatal("expected circuit to be CLOSED after successful probe")
	}
}

func TestCircuitBreakerReopensOnFailedProbe(t *testing.T) {
	cb := scheduler.NewCircuitBreaker(scheduler.CircuitBreakerConfig{
		FailureThreshold:     0.5,
		FailureWindow:        10 * time.Second,
		CircuitOpenDuration:  10 * time.Millisecond,
		HalfOpenProbeCount:   1,
		MinimumNumberOfCalls: 5,
	}, nil)

	cb.RecordOutcomes("t1", 1, 9)

	time.Sleep(15 * time.Millisecond)

	if !cb.Allow("t1") {
		t.Fatal("expected probe to be allowed in HALF_OPEN")
	}

	cb.RecordOutcomes("t1", 0, 1)

	if cb.Allow("t1") {
		t.Fatal("expected circuit to re-open after failed probe")
	}
}

func TestCircuitBreakerIndependentTenants(t *testing.T) {
	cb := scheduler.NewCircuitBreaker(scheduler.CircuitBreakerConfig{
		FailureThreshold:     0.5,
		FailureWindow:        10 * time.Second,
		CircuitOpenDuration:  5 * time.Minute,
		HalfOpenProbeCount:   1,
		MinimumNumberOfCalls: 5,
	}, nil)

	cb.RecordOutcomes("t1", 0, 10)

	if cb.Allow("t1") {
		t.Fatal("expected t1 circuit to be OPEN")
	}

	if !cb.Allow("t2") {
		t.Fatal("expected t2 circuit to be CLOSED (no failures)")
	}

	cb.RecordOutcomes("t2", 10, 0)

	if !cb.Allow("t2") {
		t.Fatal("expected t2 circuit to remain CLOSED")
	}
}

func TestCircuitBreakerSlidingWindowPrunesOldEntries(t *testing.T) {
	cb := scheduler.NewCircuitBreaker(scheduler.CircuitBreakerConfig{
		FailureThreshold:     0.5,
		FailureWindow:        50 * time.Millisecond,
		CircuitOpenDuration:  5 * time.Minute,
		HalfOpenProbeCount:   1,
		MinimumNumberOfCalls: 5,
	}, nil)

	cb.RecordOutcomes("t1", 0, 10)

	if cb.Allow("t1") {
		t.Fatal("expected circuit to be OPEN after high failure rate")
	}

	time.Sleep(60 * time.Millisecond)

	cb.RecordOutcomes("t1", 10, 0)

	cb.RecordOutcomes("t1", 1, 0)

	cb.Allow("t1")

	if cb.Allow("t1") {
		t.Fatal("expected circuit to remain OPEN until timeout")
	}
}

func TestCircuitBreakerUnknownTenantAllowed(t *testing.T) {
	cb := scheduler.NewCircuitBreaker(scheduler.CircuitBreakerConfig{
		FailureThreshold:     0.5,
		FailureWindow:        10 * time.Second,
		CircuitOpenDuration:  5 * time.Minute,
		HalfOpenProbeCount:   1,
		MinimumNumberOfCalls: 5,
	}, nil)

	if !cb.Allow("unknown_tenant") {
		t.Fatal("expected unknown tenant to be allowed")
	}
}

func TestCircuitBreakerRecordOutcomesNoop(t *testing.T) {
	cb := scheduler.NewCircuitBreaker(scheduler.CircuitBreakerConfig{
		FailureThreshold:     0.5,
		FailureWindow:        10 * time.Second,
		CircuitOpenDuration:  5 * time.Minute,
		HalfOpenProbeCount:   1,
		MinimumNumberOfCalls: 5,
	}, nil)

	cb.RecordOutcomes("t1", 0, 0)

	if !cb.Allow("t1") {
		t.Fatal("expected no-op RecordOutcomes to keep circuit closed")
	}
}
