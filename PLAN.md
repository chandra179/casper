# Casper MVP Implementation Plan

## Architecture

Each component is a Go package under `modules/` following the existing pattern (`config.go`, `dependencies.go`, behavior files). CLI entrypoints live under `cmd/`.

### Modules

| Module | Purpose |
|--------|---------|
| `modules/task/` | Task model + PostgreSQL store (CRUD, claim, dedup) |
| `modules/broker/` | RabbitMQ pub/sub (priority queues, prefetch, ack/nack) |
| `modules/scheduler/` | Poll + claim + dispatch loop (FOR UPDATE SKIP LOCKED + optimistic locking) |
| `modules/worker/` | Consume + dedup check + execute + acknowledge |
| `modules/api/` | HTTP endpoints for task ingestion and status |

### Flow

```
POST /api/tasks → API → DB (INSERT status=PENDING)
                           ↓
Scheduler loop: SELECT .. FOR UPDATE SKIP LOCKED → UPDATE version claim → Publish to Broker
                           ↓
Worker loop: Consume → INSERT processed_tasks (dedup) → Execute handler → UPDATE status → Ack
```

### External Dependencies

- **PostgreSQL 16** — task storage, coordination
- **RabbitMQ 3.13** — message broker with priority queues + DLQ

### What's Deferred

- Partitioning (pg_partman, hash-based partition polling)
- Leader election / visibility timeout cleanup singleton
- Circuit breakers per tenant
- Priority aging
- Weighted fair queuing
- mTLS
- Metrics/monitoring
- Graceful shutdown drain buffer

---

## Phase 1: Infrastructure & Task Model

**Goal:** Docker Compose services running, schema migrated, task store with tests.

### Checklist

- [x] `docker-compose.yml` — PostgreSQL 16 + RabbitMQ 3.13 services
- [x] `migrations/001_initial_schema.sql` — tasks + processed_tasks tables
- [x] `modules/task/model.go` — Task struct, status constants
- [x] `modules/task/config.go` — PostgresConfig
- [x] `modules/task/dependencies.go` — Dependencies, NewDependencies
- [x] `modules/task/store.go` — Create, Claim, Complete, Fail, MarkProcessed, IsProcessed, ReapStale
- [x] `config/config.go` — add TaskConfig (PostgresConfig)
- [x] `config/config.yaml` — add task DB config

### Tests

- [x] `modules/task/store_integration_test.go` — integration tests against real Postgres
  - Create task → read back
  - Claim task (optimistic lock)  
  - Concurrent claim (two goroutines, exactly one wins)
  - Complete / Fail task
  - Dedup (MarkProcessed + IsProcessed)

### Runnable Check

```
make test     # all 26 tests pass with testcontainers (no docker-compose required)
```

---

## Phase 2: Broker Layer

**Goal:** RabbitMQ pub/sub working, priority queues declared, DLQ.

### Checklist

- [x] `modules/broker/config.go` — BrokerConfig (URL, exchanges, queues, prefetch)
- [x] `modules/broker/dependencies.go` — Dependencies, NewDependencies
- [x] `modules/broker/rabbitmq.go` — Connect, DeclareTopology, Publish, Consume, Ack, Nack
- [x] `config/config.go` — add BrokerConfig
- [x] `config/config.yaml` — add broker config

### Tests

- [x] `modules/broker/rabbitmq_integration_test.go` — integration tests against real RabbitMQ
  - Declare topology (exchange + queues + DLQ)
  - Publish + consume + ack
  - Publish + nack → DLQ
  - Priority ordering (high before low)

### Runnable Check

```
make test     # all 26 tests pass with testcontainers
```

---

## Phase 3: Scheduler

**Goal:** Poll → claim → dispatch loop working end-to-end.

### Checklist

- [x] `modules/scheduler/config.go` — SchedulerConfig (poll interval, batch size, jitter)
- [x] `modules/scheduler/dependencies.go` — Dependencies (wraps task store + broker)
- [x] `modules/scheduler/scheduler.go` — Run loop
  - Poll PENDING tasks with FOR UPDATE SKIP LOCKED
  - Claim with optimistic locking (version column)
  - Dispatch claimed tasks to broker with priority header
  - Backoff on no tasks found
  - Jitter on dispatch
- [x] `cmd/scheduler/main.go` — entrypoint
- [x] `config/config.go` — add SchedulerConfig
- [x] `config/config.yaml` — add scheduler config

### Tests

- [x] `modules/scheduler/scheduler_test.go` — unit tests (mock store + broker)
  - Poll returns tasks → claims + dispatches
  - Empty poll → backoff
  - Version conflict → skip task (don't dispatch)
- [x] `modules/scheduler/scheduler_integration_test.go` — integration
  - Insert tasks → run one scheduler cycle → tasks dispatched to broker

### Runnable Check

```
make test     # all 26 tests pass with testcontainers
```

---

## Phase 4: Worker

**Goal:** Consume from broker, dedup, execute, acknowledge.

### Checklist

- [x] `modules/worker/config.go` — WorkerConfig (queue name, concurrency)
- [x] `modules/worker/dependencies.go` — Dependencies (wraps broker + task store)
- [x] `modules/worker/worker.go` — Run loop
  - Consume from broker with prefetch limit
  - Check dedup (INSERT INTO processed_tasks)
  - Execute handler (interface with Register)
  - Update task status to COMPLETED / FAILED
  - Ack on success, Nack on failure
  - Dead letter after max retries
- [x] `cmd/worker/main.go` — entrypoint
- [x] `config/config.go` — add WorkerConfig
- [x] `config/config.yaml` — add worker config

### Tests

- [x] `modules/worker/worker_test.go` — unit tests (mock broker + store)
  - Consume → dedup → execute → ack → status COMPLETED
  - Dedup hit → skip execute → ack
  - Execute fails → nack → status FAILED
  - Max retries exceeded → dead letter
- [x] `modules/worker/worker_integration_test.go` — integration
  - Publish task → worker consumes → status updated → acked

### Runnable Check

```
make test     # all 26 tests pass with testcontainers
```

---

## Phase 5: API Server

**Goal:** HTTP endpoints to create tasks and query status.

### Checklist

- [x] `modules/api/config.go` — APIConfig (HTTP port, timeouts)
- [x] `modules/api/dependencies.go` — Dependencies (wraps task store)
- [x] `modules/api/http.go` — HTTP server
  - `POST /api/tasks` — create task (validate payload, insert into DB)
  - `GET /api/tasks/{id}` — get task status
- [x] `cmd/api/main.go` — entrypoint
- [x] `config/config.go` — add APIConfig
- [x] `config/config.yaml` — add API config

### Tests

- [x] `modules/api/http_integration_test.go` — integration
  - POST creates task → 201 + task JSON
  - Invalid payload → 400
  - GET existing task → 200 + task JSON
  - GET non-existent task → 404

### Runnable Check

```
make test     # all 26 tests pass with testcontainers
```

---

## Phase 6: End-to-End Integration

**Goal:** Full flow working: API → DB → Scheduler → Broker → Worker → DB.

### Checklist

- [x] `integration/e2e_test.go` — full flow
  - POST task → GET pending → wait → GET completed
  - Concurrent tasks across priorities
  - Tenant isolation (separate tenant_id)
- [x] `Makefile` — full build targets
  - `make build` — builds all binaries
  - `make test` — runs all tests
  - `make run-scheduler`, `make run-worker`, `make run-api`
  - `make docker-up`, `make docker-down`

### Runnable Check

```
make test                # all 26 tests pass (testcontainers, no docker-compose needed)
```

---

## File Inventory

### New files

```
PLAN.md
docker-compose.yml
migrations/001_initial_schema.sql
modules/task/model.go
modules/task/config.go
modules/task/dependencies.go
modules/task/store.go
modules/task/store_integration_test.go
modules/broker/config.go
modules/broker/dependencies.go
modules/broker/rabbitmq.go
modules/broker/rabbitmq_integration_test.go
modules/scheduler/config.go
modules/scheduler/dependencies.go
modules/scheduler/scheduler.go
modules/scheduler/scheduler_test.go
modules/scheduler/scheduler_integration_test.go
modules/worker/config.go
modules/worker/dependencies.go
modules/worker/worker.go
modules/worker/worker_test.go
modules/worker/worker_integration_test.go
modules/api/config.go
modules/api/dependencies.go
modules/api/http.go
modules/api/http_integration_test.go
integration/e2e_test.go
cmd/scheduler/main.go
cmd/worker/main.go
cmd/api/main.go
Makefile
```

### Modified files

```
config/config.go
config/config.yaml
```
