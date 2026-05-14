package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"casper/internal/testhelper"
	"casper/modules/api"
	taskmod "casper/modules/task"
)

func apiMigrate(t *testing.T, uri string) {
	t.Helper()
	deps, err := taskmod.NewDependencies(context.Background(), taskmod.PostgresConfig{URI: uri})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	defer deps.Close()
	ctx := context.Background()
	deps.Store.Exec(ctx, `CREATE TABLE IF NOT EXISTS tasks (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(), task_type VARCHAR(255) NOT NULL,
		tenant_id VARCHAR(64) NOT NULL, payload JSONB NOT NULL DEFAULT '{}',
		status VARCHAR(20) NOT NULL DEFAULT 'PENDING', priority INT NOT NULL DEFAULT 0,
		scheduled_at TIMESTAMP NOT NULL DEFAULT NOW(), max_retries INT NOT NULL DEFAULT 3,
		retry_count INT NOT NULL DEFAULT 0, version BIGINT NOT NULL DEFAULT 0,
		claimed_by VARCHAR(255), claimed_at TIMESTAMP, completed_at TIMESTAMP,
		error_message TEXT, created_at TIMESTAMP NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMP NOT NULL DEFAULT NOW()
	)`)
	deps.Store.Exec(ctx, `CREATE TABLE IF NOT EXISTS processed_tasks (
		task_id UUID PRIMARY KEY, worker_id VARCHAR(255) NOT NULL,
		processed_at TIMESTAMP NOT NULL DEFAULT NOW()
	)`)
}

func TestCreateTask(t *testing.T) {
	pg := testhelper.SetupPostgres(t)
	apiMigrate(t, pg.URI)

	deps, _ := taskmod.NewDependencies(context.Background(), taskmod.PostgresConfig{URI: pg.URI})
	defer deps.Close()

	srv := api.NewServer(api.NewDependencies(api.Config{Port: "8080"}, deps.Store))

	body := `{"task_type":"send_email","tenant_id":"t1","payload":{"to":"a@b.com"},"priority":5}`
	req := httptest.NewRequest("POST", "/api/tasks", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)

	if resp["status"] != "PENDING" {
		t.Errorf("status: want PENDING, got %v", resp["status"])
	}
	if resp["id"] == "" {
		t.Error("id is empty")
	}
}

func TestCreateTaskInvalidJSON(t *testing.T) {
	pg := testhelper.SetupPostgres(t)
	apiMigrate(t, pg.URI)

	deps, _ := taskmod.NewDependencies(context.Background(), taskmod.PostgresConfig{URI: pg.URI})
	defer deps.Close()

	srv := api.NewServer(api.NewDependencies(api.Config{Port: "8080"}, deps.Store))

	req := httptest.NewRequest("POST", "/api/tasks", strings.NewReader(`not json`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestCreateTaskMissingFields(t *testing.T) {
	pg := testhelper.SetupPostgres(t)
	apiMigrate(t, pg.URI)

	deps, _ := taskmod.NewDependencies(context.Background(), taskmod.PostgresConfig{URI: pg.URI})
	defer deps.Close()

	srv := api.NewServer(api.NewDependencies(api.Config{Port: "8080"}, deps.Store))

	tests := []struct {
		name string
		body string
	}{
		{"no task_type", `{"tenant_id":"t1"}`},
		{"no tenant_id", `{"task_type":"test"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/api/tasks", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			srv.Handler().ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Errorf("expected 400, got %d", w.Code)
			}
		})
	}
}

func TestGetTask(t *testing.T) {
	pg := testhelper.SetupPostgres(t)
	apiMigrate(t, pg.URI)

	deps, _ := taskmod.NewDependencies(context.Background(), taskmod.PostgresConfig{URI: pg.URI})
	defer deps.Close()

	ctx := context.Background()
	scheduledAt := time.Now().UTC().Truncate(time.Second)
	tsk := &taskmod.Task{
		TaskType: "send_email", TenantID: "t1",
		Payload: []byte(`{"to":"a@b.com"}`), Status: taskmod.StatusPending,
		Priority: 5, ScheduledAt: scheduledAt, MaxRetries: 3,
	}
	if err := deps.Store.Create(ctx, tsk); err != nil {
		t.Fatalf("Create: %v", err)
	}

	srv := api.NewServer(api.NewDependencies(api.Config{Port: "8080"}, deps.Store))

	req := httptest.NewRequest("GET", "/api/tasks/"+tsk.ID.String(), nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)

	if resp["id"] != tsk.ID.String() {
		t.Errorf("id: want %s, got %v", tsk.ID, resp["id"])
	}
	if resp["status"] != "PENDING" {
		t.Errorf("status: want PENDING, got %v", resp["status"])
	}
}

func TestGetTaskNotFound(t *testing.T) {
	pg := testhelper.SetupPostgres(t)
	apiMigrate(t, pg.URI)

	deps, _ := taskmod.NewDependencies(context.Background(), taskmod.PostgresConfig{URI: pg.URI})
	defer deps.Close()

	srv := api.NewServer(api.NewDependencies(api.Config{Port: "8080"}, deps.Store))

	req := httptest.NewRequest("GET", "/api/tasks/00000000-0000-0000-0000-000000000000", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}
