package scheduler

import (
	"testing"
	"time"
)

func TestComputeEffectivePriority_NoBonus(t *testing.T) {
	now := time.Now()
	p := ComputeEffectivePriority(5, now, 0.0)
	if p != 5 {
		t.Errorf("no bonus: want 5, got %d", p)
	}
}

func TestComputeEffectivePriority_WithBonus(t *testing.T) {
	// Task that has been waiting for 24 hours with a bonus of 1/hour.
	scheduledAt := time.Now().Add(-24 * time.Hour)
	effective := ComputeEffectivePriority(0, scheduledAt, 1.0)
	if effective < 10 {
		t.Errorf("24h wait with bonus=1.0: want effective >= 10, got %d", effective)
	}

	// A fresh high-priority task should have lower effective priority
	// than the aged low-priority task.
	freshBonus := ComputeEffectivePriority(10, time.Now(), 1.0)
	if effective <= freshBonus {
		t.Errorf("aged task (effective=%d) should beat fresh priority=10 (effective=%d)", effective, freshBonus)
	}
}

func TestComputeEffectivePriority_NegativeAgeBonus(t *testing.T) {
	now := time.Now()
	p := ComputeEffectivePriority(5, now, -1.0)
	if p != 5 {
		t.Errorf("negative bonus: want 5 (unchanged), got %d", p)
	}
}

func TestComputeEffectivePriority_FutureScheduled(t *testing.T) {
	future := time.Now().Add(time.Hour)
	p := ComputeEffectivePriority(5, future, 1.0)
	if p != 5 {
		t.Errorf("future scheduled: want 5 (no bonus), got %d", p)
	}
}
