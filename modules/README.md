# Modules

Each module is a Go package under `modules/<name>/`. Module owns its domain logic, transport, and DI.

## Required files

| File | Purpose |
|------|---------|
| `dependencies.go` | Wire dependencies, load config, construct services |
| `types.go` | Domain types, structs, constants |

## Optional files

| File | Purpose |
|------|---------|
| `http.go` | Module entrypoint — HTTP server, gRPC server, CLI runner, cron job, etc. Name reflects purpose (e.g. `http.go`, `grpc.go`, `cron.go`, `worker.go`) |
| `<action>.go` | One file per handler/operation (e.g. `create_order.go`, `get_order.go`) |

## Conventions

- No `internal/` sub-packages inside a module — keep flat.
- Handlers/receivers get deps via closure or struct field (no global state).
- Config loaded once in `dependencies.go` or entrypoint file, passed down.
- Entrypoint registers routes/tasks, impl lives in dedicated action files.
- Entrypoint type determined by module goal — not limited to HTTP/gRPC.
