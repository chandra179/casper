package worker

type Config struct {
	Concurrency       int    `yaml:"concurrency"`
	QueueName         string `yaml:"queue_name"`
	HighConcurrency   int
	MediumConcurrency int
	LowConcurrency    int
}
