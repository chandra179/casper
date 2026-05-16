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
- Circuit breakers per tenant
- Priority aging
- Weighted fair queuing
- mTLS
- Metrics/monitoring

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

---

## Post-MVP: Production Hardening Roadmap

The MVP proves the core flow (API → DB → Scheduler → Broker → Worker → DB). These phases harden the system for production deployments.

---

### Phase 7: Reliability — Visibility Timeout & Graceful Shutdown

**Goal:** Tasks never stuck `IN_PROGRESS` after crashes; no lost tasks on deploy.

#### 7a. Visibility Timeout Cleanup Singleton

- [x] `modules/scheduler/cleanup.go` — cleanup leader election + scan loop
  - Leader election via advisory locks (`pg_try_advisory_lock(hashtext('cleanup_leader'))`)
  - Periodically scan tasks where `status = 'IN_PROGRESS' AND claimed_at < now() - visibility_timeout`
  - Reset stuck tasks to `PENDING`, clear `claimed_by`/`claimed_at`
  - Config: `visibility_timeout` (default 5m), `cleanup_interval` (default 30s)
- [x] `modules/scheduler/cleanup_test.go` — unit tests (mock store)
  - Stale IN_PROGRESS task → reset to PENDING
  - Recently claimed task → NOT reset

#### 7b. Graceful Shutdown Drain Buffer

- [x] `modules/scheduler/shutdown.go` — SIGTERM handler
  - Stop polling immediately
  - Drain claimed-but-undispatched tasks to broker (deadline: 10s)
  - On deadline expiry, release undispatched tasks back to PENDING via atomic UPDATE
- [x] `modules/scheduler/shutdown_test.go` — unit tests
  - Drains buffer within deadline
  - Releases tasks on timeout

---

### Phase 8: Observability — Metrics & Monitoring

**Goal:** Prometheus metrics exported, alerting rules defined.

- [x] `modules/metrics/metrics.go` — Prometheus metrics struct
  - `casper_tasks_claimed_total` (counter, by tenant/priority)
  - `casper_tasks_dispatched_total` (counter, by tenant/priority)
  - `casper_tasks_completed_total` / `casper_tasks_failed_total` (counter)
  - `casper_task_execution_duration_seconds` (histogram)
  - `casper_pending_queue_depth` (gauge, by tenant)
  - `casper_dead_lettered_tasks_total` (counter)
  - `casper_visibility_timeout_recoveries_total` (counter)
  - `casper_cleanup_leader_elected` (gauge, 0/1)
- [x] `modules/metrics/http.go` — `/metrics` endpoint on a separate port
- [x] Wire metrics into scheduler/worker/api modules
- [x] `deploy/alerts.yml` — Prometheus alerting rules from README alert table
- [x] `modules/metrics/metrics_test.go` — unit + integration tests
- [x] `deploy/prometheus.yml` — Prometheus scrape config targeting all 3 binaries
- [x] `docker-compose.yml` — Added Prometheus service (port 9093)
- [x] End-to-end verification — task created → claimed → dispatched → completed, all metrics incremented, Prometheus scraping 3/3 targets UP, 8 alert rules loaded

---

### Phase 9: Fairness — Priority Aging & Weighted Fair Queuing

**Goal:** Low-priority tasks never starve; long-waiting tasks get promoted.

#### 9a. Priority Aging

- [ ] `modules/scheduler/aging.go` — age-bonus computation
  - Compute `effective_priority = base_priority + age_bonus(now - scheduled_at)`
  - Config: `age_bonus_per_hour` (how much priority increments per hour of waiting)
  - Poll query sorts by effective priority instead of raw priority
- [ ] `modules/scheduler/aging_test.go` — unit tests
  - Task waiting 24h has higher effective priority than fresh high-priority task
  - No age bonus when `age_bonus_per_hour = 0`

#### 9b. Weighted Fair Queuing

- [ ] `modules/worker/pool.go` — per-priority worker pools
  - Configurable worker allocation per priority (e.g. high: 60%, medium: 30%, low: 10%)
  - Separate goroutine pools per priority level
  - Config: `priority_weights` map
- [ ] `modules/worker/pool_test.go` — unit tests
  - Low-priority tasks always get at least the configured share

---

### Phase 10: Multi-Tenancy — Circuit Breakers

**Goal:** One tenant's failure storm doesn't affect others.

- [ ] `modules/scheduler/circuit.go` — per-tenant circuit breaker
  - Track failure rate per tenant (sliding window)
  - If failure rate > threshold (e.g. 90%) for duration (e.g. 5 min) → open circuit → skip dispatch for that tenant
  - Half-open: dispatch one task to probe recovery; if it succeeds → close; if it fails → back to open
  - Config: `failure_threshold`, `circuit_open_duration`, `half_open_probe_count`
- [ ] `modules/scheduler/circuit_test.go` — unit tests
  - Circuit opens on sustained failures
  - Circuit half-opens after timeout; closes on success; re-opens on failure

---

### Phase 11: Security — mTLS

**Goal:** All inter-service communication encrypted and authenticated.

- [ ] `modules/security/tls.go` — TLS config loader
  - Load CA cert, server cert, client cert from files
  - `tls.Config` for PostgreSQL, RabbitMQ, HTTP clients/servers
- [ ] Wire mTLS into all connections:
  - `modules/broker/rabbitmq.go` — TLS dial config
  - `modules/task/store.go` — PostgreSQL TLS connection
  - `modules/api/http.go` — HTTPS listener with client cert verification
- [ ] `modules/security/tls_test.go` — unit tests
- [ ] `deploy/certs/` — placeholder for cert generation scripts

---

### Phase 12: Scale — Partitioning

**Goal:** Handle 100K+ pending tasks across 50+ scheduler nodes without poll contention.

#### 12a. Hash-Based Partition Polling

- [ ] `modules/scheduler/partition.go` — partition assignment
  - `partition_key = hash(task_id) % N` assigned on task creation
  - Poll query: `WHERE partition_key IN (assigned_range) AND status = 'PENDING'`
  - Dynamic partition ownership via DB table (`partition_ownership`)
  - On startup: claim unowned partitions; on shutdown: release
- [ ] `migrations/003_partition_support.sql` — `partition_ownership` table, `partition_key` index

#### 12b. Database Partitioning (pg_partman)

- [ ] `migrations/004_pg_partman.sql` — `pg_partman` setup, auto-create partitions
- [ ] `docker-compose.yml` — add `pg_partman` extension setup
- [ ] Archiving script (`scripts/archive_partitions.sh`) — detach old partitions

#### Tests

- [ ] `modules/scheduler/partition_test.go` — unit tests
  - Partition assignment on startup
  - Reassignment on node failure
  - Poll restricted to assigned partitions

---

### Ordering Rationale

| Phase | Why this order |
|-------|---------------|
| 7 (Reliability) | Crashes lose tasks — must fix before any production use |
| 8 (Observability) | Can't debug what you can't see |
| 9 (Fairness) | Starvation becomes noticeable once real workloads hit |
| 10 (Circuit Breakers) | Blast-radius isolation before onboarding more tenants |
| 11 (Security) | mTLS is a deployment concern; can layer on after core is stable |
| 12 (Scale) | Partitioning only matters at 100K+ pending / 50+ nodes — defer until needed |
