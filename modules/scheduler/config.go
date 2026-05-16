package scheduler

import "time"

type Config struct {
	PollInterval         time.Duration
	BatchSize            int
	JitterMax            time.Duration
	VisibilityTimeout    time.Duration
	CleanupInterval      time.Duration
	ShutdownDrainTimeout time.Duration
	CleanupBatchSize     int
	AgeBonusPerHour      float64
	CircuitBreaker       CircuitBreakerConfig
}
