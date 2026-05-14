# Brook — Agent Guide

Go skeleton for evolutionary architecture. Module `brook`, Go 1.26.1.

## Layout (current — docs are stale)

| What | Old path (in docs) | Actual path |
|------|-------------------|-------------|
| entrypoint | `cmd/http/main.go`, `cmd/grpc/main.go` | `cmd/order/main.go` |
| business logic | `internal/modules/<name>/` | `modules/<name>/` |
| middleware | `internal/middleware/` | `middleware/` |

README documents old structure. Trust filesystem.

## Commands

```bash
make vendor          # go mod tidy && go mod vendor
go run cmd/order/main.go
go build ./...
```

No test/lint/CI infrastructure. No `golangci-lint`, no pre-commit hooks.

## Module pattern (`modules/<name>/`)

Flat Go package. Required: `dependencies.go` (DI wireup), `types.go` (domain types).
Optional: `http.go`/`grpc.go`/`cron.go` entrypoint, `<action>.go` per handler.
See `modules/README.md` for details.

## Middleware (`middleware/`)

```go
type Middleware func(http.Handler) http.Handler
// Chain(handler, Recovery, RequestID, Timeout) — outermost first
```

Stateless middleware = plain constructors. Logger-dependent middleware = methods on `*Dependencies`.
`middleware.Dependencies` wraps shared state (logger from `github.com/Chandra179/gosdk`).
gRPC interceptor `RequestIDUnaryInterceptor` lives alongside HTTP middleware in same package.

## Config

YAML at `config/config.yaml`, loaded by `config.Load("config/config.yaml")`.
Fields: `http.addr`, `http.*_timeout`, `grpc.addr`, `middleware.timeout`, `middleware.logger.level`.

## Validation

`middleware.DecodeAndValidate[T](r)` — decodes JSON + validates `go-playground/validator` struct tags.
Call inside handlers, not as middleware.

## State

Repo is mid-restructure: old `internal/` files deleted on disk, new flat files untracked.
No tests exist. No vendor in git (`.gitignore` has `vendor`).
