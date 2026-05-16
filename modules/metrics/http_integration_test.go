package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestMetricsEndpoint(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	RecordTaskClaimed("tenant-a", 5)
	RecordTaskClaimed("tenant-a", 5)
	RecordTaskClaimed("tenant-b", 8)
	RecordTaskDispatched("tenant-a", 5)
	RecordTaskCompleted("tenant-a", "send_email")
	RecordTaskFailed("tenant-a", "send_email")
	RecordDeadLettered("tenant-a", "send_email")
	ObserveExecutionDuration("tenant-a", "send_email", 0.75)
	RecordVisibilityTimeoutRecoveries(2)
	SetCleanupLeaderElected(true)
	SetPendingQueueDepth("tenant-a", 42)
	SetPendingQueueDepth("tenant-b", 7)

	time.Sleep(10 * time.Millisecond)

	t.Run("health", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/health")
		if err != nil {
			t.Fatalf("health: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("health: expected 200, got %d", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), `"status":"ok"`) {
			t.Errorf("health: unexpected body: %s", string(body))
		}
	})

	t.Run("metrics", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/metrics")
		if err != nil {
			t.Fatalf("metrics: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("metrics: expected 200, got %d", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		text := string(body)

		checks := []string{
			`casper_tasks_claimed_total{priority="5",tenant_id="tenant-a"} 2`,
			`casper_tasks_claimed_total{priority="8",tenant_id="tenant-b"} 1`,
			`casper_tasks_dispatched_total{priority="5",tenant_id="tenant-a"} 1`,
			`casper_tasks_completed_total{task_type="send_email",tenant_id="tenant-a"} 1`,
			`casper_tasks_failed_total{task_type="send_email",tenant_id="tenant-a"} 1`,
			`casper_dead_lettered_tasks_total{task_type="send_email",tenant_id="tenant-a"} 1`,
			`casper_task_execution_duration_seconds_bucket{task_type="send_email",tenant_id="tenant-a"`,
			`casper_pending_queue_depth{tenant_id="tenant-a"} 42`,
			`casper_pending_queue_depth{tenant_id="tenant-b"} 7`,
			`casper_visibility_timeout_recoveries_total 2`,
			`casper_cleanup_leader_elected 1`,
		}
		for _, check := range checks {
			if !strings.Contains(text, check) {
				t.Errorf("metrics output missing:\n  %q", check)
			}
		}
	})
}
