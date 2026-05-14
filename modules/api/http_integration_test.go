package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"casper/modules/api"
	taskmod "casper/modules/task"
)

func testStore(t *testing.T) *taskmod.Store {
	t.Helper()
	host := os.Getenv("CASPER_DB_HOST")
	if host == "" {
		host = "localhost"
	}
	cfg := taskmod.PostgresConfig{
		Host:     host,
		Port:     5432,
		User:     "casper",
		Password: "casper",
		Database: "casper",
		SSLMode:  "disable",
	}
	deps, err := taskmod.NewDependencies(context.Background(), cfg)
	if err != nil {
		t.Skipf("postgres not available: %v", err)
	}
	t.Cleanup(func() {
		deps.Store.Exec(context.Background(), "DELETE FROM processed_tasks")
		deps.Store.Exec(context.Background(), "DELETE FROM tasks")
		deps.Close()
	})
	return deps.Store
}

func TestCreateTask(t *testing.T) {
	store := testStore(t)

	srv := api.NewServer(api.NewDependencies(
		api.Config{Port: "8080"},
		store,
	))

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
	store := testStore(t)

	srv := api.NewServer(api.NewDependencies(
		api.Config{Port: "8080"},
		store,
	))

	req := httptest.NewRequest("POST", "/api/tasks", strings.NewReader(`not json`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestCreateTaskMissingFields(t *testing.T) {
	store := testStore(t)

	srv := api.NewServer(api.NewDependencies(
		api.Config{Port: "8080"},
		store,
	))

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
	store := testStore(t)

	ctx := context.Background()
	scheduledAt := time.Now().UTC().Truncate(time.Second)
	tsk := &taskmod.Task{
		TaskType:    "send_email",
		TenantID:    "t1",
		Payload:     []byte(`{"to":"a@b.com"}`),
		Status:      taskmod.StatusPending,
		Priority:    5,
		ScheduledAt: scheduledAt,
		MaxRetries:  3,
	}
	if err := store.Create(ctx, tsk); err != nil {
		t.Fatalf("Create: %v", err)
	}

	srv := api.NewServer(api.NewDependencies(
		api.Config{Port: "8080"},
		store,
	))

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
	if resp["task_type"] != "send_email" {
		t.Errorf("task_type: want send_email, got %v", resp["task_type"])
	}
}

func TestGetTaskNotFound(t *testing.T) {
	store := testStore(t)

	srv := api.NewServer(api.NewDependencies(
		api.Config{Port: "8080"},
		store,
	))

	req := httptest.NewRequest("GET", "/api/tasks/00000000-0000-0000-0000-000000000000", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		_ = bytes.NewReader(nil)
		t.Errorf("expected 404, got %d", w.Code)
	}
}
