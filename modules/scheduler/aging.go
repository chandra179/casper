package scheduler

import (
	"math"
	"time"
)

// ComputeEffectivePriority returns the effective priority of a task
// after applying age-based decay. The longer a task has waited, the
// higher its effective priority becomes.
//
//	effective = base_priority + FLOOR(hours_waiting * age_bonus_per_hour)
//
// Aging is a classic OS scheduling technique used to prevent starvation
// in fixed-priority scheduling (see Silberschatz, Galvin, Gagne,
// "Operating System Concepts").
func ComputeEffectivePriority(basePriority int, scheduledAt time.Time, ageBonusPerHour float64) int {
	if ageBonusPerHour <= 0 {
		return basePriority
	}
	hours := time.Since(scheduledAt).Hours()
	if hours < 0 {
		return basePriority
	}
	bonus := int(math.Floor(hours * ageBonusPerHour))
	return basePriority + bonus
}
