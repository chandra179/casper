package api

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"

	taskmod "casper/modules/task"
	"casper/middleware"
)

type createTaskRequest struct {
	TaskType    string          `json:"task_type" validate:"required"`
	TenantID    string          `json:"tenant_id" validate:"required"`
	Payload     json.RawMessage `json:"payload"`
	ScheduledAt *time.Time      `json:"scheduled_at"`
	Priority    int             `json:"priority"`
	MaxRetries  int             `json:"max_retries"`
}

type createTaskResponse struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	CreatedAt   string `json:"created_at"`
	ScheduledAt string `json:"scheduled_at"`
}

type getTaskResponse struct {
	ID           string  `json:"id"`
	TaskType     string  `json:"task_type"`
	TenantID     string  `json:"tenant_id"`
	Payload      json.RawMessage `json:"payload"`
	Status       string  `json:"status"`
	Priority     int     `json:"priority"`
	ScheduledAt  string  `json:"scheduled_at"`
	MaxRetries   int     `json:"max_retries"`
	RetryCount   int     `json:"retry_count"`
	Version      int64   `json:"version"`
	ClaimedBy    *string `json:"claimed_by,omitempty"`
	ClaimedAt    *string `json:"claimed_at,omitempty"`
	CompletedAt  *string `json:"completed_at,omitempty"`
	ErrorMessage *string `json:"error_message,omitempty"`
	CreatedAt    string  `json:"created_at"`
	UpdatedAt    string  `json:"updated_at"`
}

type errorResponse struct {
	Error string `json:"error"`
}

type Server struct {
	deps *Dependencies
	mux  *http.ServeMux
}

func NewServer(deps *Dependencies) *Server {
	s := &Server{deps: deps, mux: http.NewServeMux()}
	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("POST /api/tasks", s.handleCreateTask)
	s.mux.HandleFunc("GET /api/tasks/{id}", s.handleGetTask)
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	var req createTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON: " + err.Error()})
		return
	}

	if req.TaskType == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "task_type is required"})
		return
	}
	if req.TenantID == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "tenant_id is required"})
		return
	}
	if req.MaxRetries <= 0 {
		req.MaxRetries = 3
	}
	if req.Priority < 0 {
		req.Priority = 0
	}

	scheduledAt := time.Now().UTC()
	if req.ScheduledAt != nil {
		scheduledAt = *req.ScheduledAt
	}

	payload := json.RawMessage(`{}`)
	if req.Payload != nil {
		payload = req.Payload
	}

	t := &taskmod.Task{
		ID:          uuid.New(),
		TaskType:    req.TaskType,
		TenantID:    req.TenantID,
		Payload:     payload,
		Status:      taskmod.StatusPending,
		Priority:    req.Priority,
		ScheduledAt: scheduledAt,
		MaxRetries:  req.MaxRetries,
	}

	if err := s.deps.Store.Create(r.Context(), t); err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "create task: " + err.Error()})
		return
	}

	resp := createTaskResponse{
		ID:          t.ID.String(),
		Status:      string(t.Status),
		CreatedAt:   t.CreatedAt.Format(time.RFC3339),
		ScheduledAt: t.ScheduledAt.Format(time.RFC3339),
	}

	writeJSON(w, http.StatusCreated, resp)
}

func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid task ID"})
		return
	}

	t, err := s.deps.Store.GetByID(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "get task: " + err.Error()})
		return
	}
	if t == nil {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "task not found"})
		return
	}

	var payload json.RawMessage
	if len(t.Payload) > 0 {
		payload = t.Payload
	}

	resp := getTaskResponse{
		ID:          t.ID.String(),
		TaskType:    t.TaskType,
		TenantID:    t.TenantID,
		Payload:     payload,
		Status:      string(t.Status),
		Priority:    t.Priority,
		ScheduledAt: t.ScheduledAt.Format(time.RFC3339),
		MaxRetries:  t.MaxRetries,
		RetryCount:  t.RetryCount,
		Version:     t.Version,
		CreatedAt:   t.CreatedAt.Format(time.RFC3339),
		UpdatedAt:   t.UpdatedAt.Format(time.RFC3339),
	}

	if t.ClaimedBy != nil {
		resp.ClaimedBy = t.ClaimedBy
	}
	if t.ClaimedAt != nil {
		s := t.ClaimedAt.Format(time.RFC3339)
		resp.ClaimedAt = &s
	}
	if t.CompletedAt != nil {
		s := t.CompletedAt.Format(time.RFC3339)
		resp.CompletedAt = &s
	}
	if t.ErrorMessage != nil {
		resp.ErrorMessage = t.ErrorMessage
	}

	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("writeJSON: %v", err)
	}
}

func RunHTTPServer(deps *Dependencies, middlewareDeps *middleware.Dependencies) error {
	s := NewServer(deps)

	chain := middleware.Chain(
		s.Handler(),
		middlewareDeps.Recovery(),
		middleware.RequestID,
		middleware.Timeout(middleware.TimeoutConfig{Duration: 30 * time.Second}),
	)

	srv := &http.Server{
		Addr:         ":" + deps.Config.Port,
		Handler:      chain,
		ReadTimeout:  deps.Config.ReadTimeout,
		WriteTimeout: deps.Config.WriteTimeout,
		IdleTimeout:  deps.Config.IdleTimeout,
	}

	return srv.ListenAndServe()
}
