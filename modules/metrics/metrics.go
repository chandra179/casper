package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	LabelTenantID = "tenant_id"
	LabelPriority = "priority"
	LabelTaskType = "task_type"

	namespace = "casper"
)

type Metrics struct {
	TasksClaimedTotal            *prometheus.CounterVec
	TasksDispatchedTotal         *prometheus.CounterVec
	TasksCompletedTotal          *prometheus.CounterVec
	TasksFailedTotal             *prometheus.CounterVec
	DeadLetteredTasksTotal       *prometheus.CounterVec
	TaskExecutionDurationSeconds *prometheus.HistogramVec
	PendingQueueDepth            *prometheus.GaugeVec
	VisibilityTimeoutRecoveries  prometheus.Counter
	CleanupLeaderElected         prometheus.Gauge
	CircuitBreakerState          *prometheus.GaugeVec
	CircuitBreakerTransitions    *prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		TasksClaimedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "tasks_claimed_total",
				Help:      "Total number of tasks claimed by scheduler, partitioned by tenant and priority.",
			},
			[]string{LabelTenantID, LabelPriority},
		),
		TasksDispatchedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "tasks_dispatched_total",
				Help:      "Total number of tasks dispatched to broker, partitioned by tenant and priority.",
			},
			[]string{LabelTenantID, LabelPriority},
		),
		TasksCompletedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "tasks_completed_total",
				Help:      "Total number of tasks completed successfully, partitioned by tenant and task type.",
			},
			[]string{LabelTenantID, LabelTaskType},
		),
		TasksFailedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "tasks_failed_total",
				Help:      "Total number of tasks that failed execution, partitioned by tenant and task type.",
			},
			[]string{LabelTenantID, LabelTaskType},
		),
		DeadLetteredTasksTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "dead_lettered_tasks_total",
				Help:      "Total number of tasks routed to dead letter queue, partitioned by tenant and task type.",
			},
			[]string{LabelTenantID, LabelTaskType},
		),
		TaskExecutionDurationSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "task_execution_duration_seconds",
				Help:      "Execution duration of task handlers in seconds, partitioned by tenant and task type.",
				Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
			},
			[]string{LabelTenantID, LabelTaskType},
		),
		PendingQueueDepth: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "pending_queue_depth",
				Help:      "Number of tasks in PENDING status, partitioned by tenant.",
			},
			[]string{LabelTenantID},
		),
		VisibilityTimeoutRecoveries: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "visibility_timeout_recoveries_total",
				Help:      "Total number of tasks recovered by visibility timeout cleanup.",
			},
		),
		CleanupLeaderElected: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "cleanup_leader_elected",
				Help:      "Indicates whether this instance is the elected cleanup leader (1 = leader, 0 = not leader).",
			},
		),
		CircuitBreakerState: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "circuit_breaker_state",
				Help:      "State of the circuit breaker per tenant (0 = closed, 1 = open, 2 = half-open).",
			},
			[]string{LabelTenantID},
		),
		CircuitBreakerTransitions: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "circuit_breaker_transitions_total",
				Help:      "Total number of circuit breaker state transitions per tenant.",
			},
			[]string{LabelTenantID, "from_state", "to_state"},
		),
	}

	reg.MustRegister(
		m.TasksClaimedTotal,
		m.TasksDispatchedTotal,
		m.TasksCompletedTotal,
		m.TasksFailedTotal,
		m.DeadLetteredTasksTotal,
		m.TaskExecutionDurationSeconds,
		m.PendingQueueDepth,
		m.VisibilityTimeoutRecoveries,
		m.CleanupLeaderElected,
		m.CircuitBreakerState,
		m.CircuitBreakerTransitions,
	)

	return m
}

var Default = NewMetrics(prometheus.DefaultRegisterer)

func RecordTaskClaimed(tenantID string, priority int) {
	Default.TasksClaimedTotal.WithLabelValues(tenantID, strconv.Itoa(priority)).Inc()
}

func RecordTaskDispatched(tenantID string, priority int) {
	Default.TasksDispatchedTotal.WithLabelValues(tenantID, strconv.Itoa(priority)).Inc()
}

func RecordTaskCompleted(tenantID, taskType string) {
	Default.TasksCompletedTotal.WithLabelValues(tenantID, taskType).Inc()
}

func RecordTaskFailed(tenantID, taskType string) {
	Default.TasksFailedTotal.WithLabelValues(tenantID, taskType).Inc()
}

func RecordDeadLettered(tenantID, taskType string) {
	Default.DeadLetteredTasksTotal.WithLabelValues(tenantID, taskType).Inc()
}

func ObserveExecutionDuration(tenantID, taskType string, seconds float64) {
	Default.TaskExecutionDurationSeconds.WithLabelValues(tenantID, taskType).Observe(seconds)
}

func RecordVisibilityTimeoutRecoveries(count float64) {
	Default.VisibilityTimeoutRecoveries.Add(count)
}

func SetCleanupLeaderElected(elected bool) {
	if elected {
		Default.CleanupLeaderElected.Set(1)
	} else {
		Default.CleanupLeaderElected.Set(0)
	}
}

func SetPendingQueueDepth(tenantID string, depth float64) {
	Default.PendingQueueDepth.WithLabelValues(tenantID).Set(depth)
}

func SetCircuitBreakerState(tenantID string, state float64) {
	Default.CircuitBreakerState.WithLabelValues(tenantID).Set(state)
}

func RecordCircuitBreakerTransition(tenantID, fromState, toState string) {
	Default.CircuitBreakerTransitions.WithLabelValues(tenantID, fromState, toState).Inc()
}
