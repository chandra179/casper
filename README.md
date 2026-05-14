# Brook

Go modular monolith skeleton. One binary, domain modules as Go packages. Split to microservices later — not before.

## Layout

```
cmd/order/main.go     # entrypoint — starts HTTP + gRPC
modules/              # domain modules
  order/              #   order domain
    config.go         #     module-specific config struct
    dependencies.go   #     wire deps, load own config
    http.go           #     HTTP handlers + route registration
middleware/           # shared: recovery, request ID, timeout, validation
config/               # YAML loader + config.yaml
```

## Module pattern

Each module under `modules/<name>/` is flat Go package. Owns domain logic, transport (HTTP/gRPC), DI, and config.

Module defines own `Config` struct in `config.go`. Config loaded via `loadConfig(path)` in `dependencies.go` — opens YAML, unmarshals module's section. Zero import of shared `config/`.

Shared `config/config.yaml` nests module sections under module key:

```yaml
order:
  http:
    addr: ":8080"
    ...
middleware:
  timeout: 30s
```

No inter-module coupling — modules call shared infra (`middleware/`), not each other.

## Config decoupling

Each module owns its config shape. Means 3 lines of YAML load boilerplate per module. Tradeoff: migration = copy module dir, zero dependency on shared config types.

## Commands

```bash
make vendor          # go mod tidy && go mod vendor
go run cmd/order/main.go
go build ./...
go test ./modules/...
```

## State

Mid-restructure. Single module (`order`). One entrypoint binary. Basic test coverage on config loading + DI wiring.

## Design choices

- Validation via `middleware.DecodeAndValidate[T](r)` inside handlers
- No `internal/` sub-packages inside modules
- Config struct per module, YAML section per module key
- No global state — deps injected via closure or struct field
