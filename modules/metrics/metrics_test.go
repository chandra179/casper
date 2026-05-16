package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

func TestNewMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	if m.TasksClaimedTotal == nil {
		t.Fatal("TasksClaimedTotal is nil")
	}
	if m.TasksDispatchedTotal == nil {
		t.Fatal("TasksDispatchedTotal is nil")
	}
	if m.TasksCompletedTotal == nil {
		t.Fatal("TasksCompletedTotal is nil")
	}
	if m.TasksFailedTotal == nil {
		t.Fatal("TasksFailedTotal is nil")
	}
	if m.DeadLetteredTasksTotal == nil {
		t.Fatal("DeadLetteredTasksTotal is nil")
	}
	if m.TaskExecutionDurationSeconds == nil {
		t.Fatal("TaskExecutionDurationSeconds is nil")
	}
	if m.PendingQueueDepth == nil {
		t.Fatal("PendingQueueDepth is nil")
	}
	if m.VisibilityTimeoutRecoveries == nil {
		t.Fatal("VisibilityTimeoutRecoveries is nil")
	}
	if m.CleanupLeaderElected == nil {
		t.Fatal("CleanupLeaderElected is nil")
	}
}

func TestTasksClaimedTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.TasksClaimedTotal.WithLabelValues("tenant-a", "5").Inc()
	m.TasksClaimedTotal.WithLabelValues("tenant-a", "5").Inc()
	m.TasksClaimedTotal.WithLabelValues("tenant-b", "8").Inc()

	val := getCounterValue(t, m.TasksClaimedTotal, map[string]string{LabelTenantID: "tenant-a", LabelPriority: "5"})
	if val != 2 {
		t.Errorf("expected 2, got %f", val)
	}

	val = getCounterValue(t, m.TasksClaimedTotal, map[string]string{LabelTenantID: "tenant-b", LabelPriority: "8"})
	if val != 1 {
		t.Errorf("expected 1, got %f", val)
	}
}

func TestTasksDispatchedTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.TasksDispatchedTotal.WithLabelValues("tenant-a", "3").Inc()

	val := getCounterValue(t, m.TasksDispatchedTotal, map[string]string{LabelTenantID: "tenant-a", LabelPriority: "3"})
	if val != 1 {
		t.Errorf("expected 1, got %f", val)
	}
}

func TestTasksCompletedTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.TasksCompletedTotal.WithLabelValues("tenant-a", "send_email").Inc()
	m.TasksCompletedTotal.WithLabelValues("tenant-a", "send_email").Inc()

	val := getCounterValue(t, m.TasksCompletedTotal, map[string]string{LabelTenantID: "tenant-a", LabelTaskType: "send_email"})
	if val != 2 {
		t.Errorf("expected 2, got %f", val)
	}
}

func TestTasksFailedTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.TasksFailedTotal.WithLabelValues("tenant-a", "send_email").Inc()

	val := getCounterValue(t, m.TasksFailedTotal, map[string]string{LabelTenantID: "tenant-a", LabelTaskType: "send_email"})
	if val != 1 {
		t.Errorf("expected 1, got %f", val)
	}
}

func TestDeadLetteredTasksTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.DeadLetteredTasksTotal.WithLabelValues("tenant-a", "process_payment").Inc()

	val := getCounterValue(t, m.DeadLetteredTasksTotal, map[string]string{LabelTenantID: "tenant-a", LabelTaskType: "process_payment"})
	if val != 1 {
		t.Errorf("expected 1, got %f", val)
	}
}

func TestTaskExecutionDurationSeconds(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.TaskExecutionDurationSeconds.WithLabelValues("tenant-a", "send_email").Observe(0.5)
	m.TaskExecutionDurationSeconds.WithLabelValues("tenant-a", "send_email").Observe(1.2)

	metric := &io_prometheus_client.Metric{}
	total := 0.0
	collectHistogram(m.TaskExecutionDurationSeconds, map[string]string{LabelTenantID: "tenant-a", LabelTaskType: "send_email"}, func(m *io_prometheus_client.Metric) {
		metric = m
		total += m.GetHistogram().GetSampleSum()
	})
	if metric.GetHistogram().GetSampleCount() != 2 {
		t.Errorf("expected 2 observations, got %d", metric.GetHistogram().GetSampleCount())
	}
	if total != 1.7 {
		t.Errorf("expected 1.7 sum, got %f", total)
	}
}

func TestPendingQueueDepth(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.PendingQueueDepth.WithLabelValues("tenant-a").Set(42)
	m.PendingQueueDepth.WithLabelValues("tenant-b").Set(7)

	val := getGaugeValue(t, m.PendingQueueDepth, map[string]string{LabelTenantID: "tenant-a"})
	if val != 42 {
		t.Errorf("expected 42, got %f", val)
	}

	val = getGaugeValue(t, m.PendingQueueDepth, map[string]string{LabelTenantID: "tenant-b"})
	if val != 7 {
		t.Errorf("expected 7, got %f", val)
	}
}

func TestVisibilityTimeoutRecoveries(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.VisibilityTimeoutRecoveries.Inc()
	m.VisibilityTimeoutRecoveries.Add(5)

	val := getCounterValue(t, m.VisibilityTimeoutRecoveries, nil)
	if val != 6 {
		t.Errorf("expected 6, got %f", val)
	}
}

func TestCleanupLeaderElected(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.CleanupLeaderElected.Set(1)
	val := getGaugeValue(t, m.CleanupLeaderElected, nil)
	if val != 1 {
		t.Errorf("expected 1, got %f", val)
	}

	m.CleanupLeaderElected.Set(0)
	val = getGaugeValue(t, m.CleanupLeaderElected, nil)
	if val != 0 {
		t.Errorf("expected 0, got %f", val)
	}
}

func TestDefaultMetrics(t *testing.T) {
	if Default == nil {
		t.Fatal("Default metrics not initialized")
	}
	if Default.TasksClaimedTotal == nil {
		t.Fatal("Default TasksClaimedTotal is nil")
	}
}

func TestRecordFunctions(t *testing.T) {
	reg := prometheus.NewRegistry()
	orig := Default
	Default = NewMetrics(reg)
	defer func() { Default = orig }()

	RecordTaskClaimed("t1", 5)
	RecordTaskClaimed("t1", 5)
	RecordTaskDispatched("t1", 5)

	RecordTaskCompleted("t1", "email")
	RecordTaskFailed("t1", "email")
	RecordDeadLettered("t1", "email")
	ObserveExecutionDuration("t1", "email", 0.5)

	RecordVisibilityTimeoutRecoveries(3)
	SetCleanupLeaderElected(true)
	SetPendingQueueDepth("t1", 100)

	claimed := getCounterValue(t, Default.TasksClaimedTotal, map[string]string{LabelTenantID: "t1", LabelPriority: "5"})
	if claimed != 2 {
		t.Errorf("claimed: expected 2, got %f", claimed)
	}

	dispatched := getCounterValue(t, Default.TasksDispatchedTotal, map[string]string{LabelTenantID: "t1", LabelPriority: "5"})
	if dispatched != 1 {
		t.Errorf("dispatched: expected 1, got %f", dispatched)
	}

	completed := getCounterValue(t, Default.TasksCompletedTotal, map[string]string{LabelTenantID: "t1", LabelTaskType: "email"})
	if completed != 1 {
		t.Errorf("completed: expected 1, got %f", completed)
	}

	failed := getCounterValue(t, Default.TasksFailedTotal, map[string]string{LabelTenantID: "t1", LabelTaskType: "email"})
	if failed != 1 {
		t.Errorf("failed: expected 1, got %f", failed)
	}

	dead := getCounterValue(t, Default.DeadLetteredTasksTotal, map[string]string{LabelTenantID: "t1", LabelTaskType: "email"})
	if dead != 1 {
		t.Errorf("dead_lettered: expected 1, got %f", dead)
	}

	depth := getGaugeValue(t, Default.PendingQueueDepth, map[string]string{LabelTenantID: "t1"})
	if depth != 100 {
		t.Errorf("pending_depth: expected 100, got %f", depth)
	}

	recoveries := getCounterValue(t, Default.VisibilityTimeoutRecoveries, nil)
	if recoveries != 3 {
		t.Errorf("recoveries: expected 3, got %f", recoveries)
	}

	leader := getGaugeValue(t, Default.CleanupLeaderElected, nil)
	if leader != 1 {
		t.Errorf("leader: expected 1, got %f", leader)
	}
}

func TestMetricNamesHaveCasperPrefix(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	ch := make(chan prometheus.Metric, 10)
	m.TasksClaimedTotal.Collect(ch)
	close(ch)
	for metric := range ch {
		desc := metric.Desc().String()
		if !strings.Contains(desc, "casper_tasks_claimed_total") {
			t.Errorf("metric name missing casper prefix: %s", desc)
		}
	}
}

func getCounterValue(t *testing.T, c prometheus.Collector, labels map[string]string) float64 {
	t.Helper()
	var result float64
	forEachMetric(c, labels, func(m *io_prometheus_client.Metric) {
		result = m.GetCounter().GetValue()
	})
	return result
}

func getGaugeValue(t *testing.T, c prometheus.Collector, labels map[string]string) float64 {
	t.Helper()
	var result float64
	forEachMetric(c, labels, func(m *io_prometheus_client.Metric) {
		result = m.GetGauge().GetValue()
	})
	return result
}

func forEachMetric(c prometheus.Collector, labels map[string]string, fn func(*io_prometheus_client.Metric)) {
	ch := make(chan prometheus.Metric, 10)
	go func() {
		c.Collect(ch)
		close(ch)
	}()
	for m := range ch {
		metric := &io_prometheus_client.Metric{}
		if err := m.Write(metric); err != nil {
			continue
		}
		if labels == nil {
			fn(metric)
			return
		}
		if matchLabels(metric.GetLabel(), labels) {
			fn(metric)
			return
		}
	}
}

func collectHistogram(c prometheus.Collector, labels map[string]string, fn func(*io_prometheus_client.Metric)) {
	ch := make(chan prometheus.Metric, 10)
	go func() {
		c.Collect(ch)
		close(ch)
	}()
	for m := range ch {
		metric := &io_prometheus_client.Metric{}
		if err := m.Write(metric); err != nil {
			continue
		}
		if matchLabels(metric.GetLabel(), labels) {
			fn(metric)
		}
	}
}

func matchLabels(got []*io_prometheus_client.LabelPair, want map[string]string) bool {
	if len(got) < len(want) {
		return false
	}
	found := 0
	for _, pair := range got {
		if v, ok := want[pair.GetName()]; ok && pair.GetValue() == v {
			found++
		}
	}
	return found == len(want)
}
