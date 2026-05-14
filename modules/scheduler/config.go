package scheduler

import "time"

type Config struct {
	PollInterval time.Duration
	BatchSize    int
	JitterMax    time.Duration
}
