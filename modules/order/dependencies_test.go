package order

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTestConfig(t *testing.T, data string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadConfig(t *testing.T) {
	yml := `
order:
  http:
    addr: ":9999"
    read_timeout: 3s
    write_timeout: 10s
    idle_timeout: 60s
middleware:
  timeout: 15s
  logger:
    level: prod
`
	path := writeTestConfig(t, yml)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}

	if cfg.Order.HTTP.Addr != ":9999" {
		t.Errorf("Order.HTTP.Addr = %q, want %q", cfg.Order.HTTP.Addr, ":9999")
	}
	if cfg.Order.HTTP.ReadTimeout != 3*time.Second {
		t.Errorf("Order.HTTP.ReadTimeout = %v, want %v", cfg.Order.HTTP.ReadTimeout, 3*time.Second)
	}
	if cfg.Order.HTTP.WriteTimeout != 10*time.Second {
		t.Errorf("Order.HTTP.WriteTimeout = %v, want %v", cfg.Order.HTTP.WriteTimeout, 10*time.Second)
	}
	if cfg.Order.HTTP.IdleTimeout != 60*time.Second {
		t.Errorf("Order.HTTP.IdleTimeout = %v, want %v", cfg.Order.HTTP.IdleTimeout, 60*time.Second)
	}
	if cfg.Middleware.Timeout != 15*time.Second {
		t.Errorf("Middleware.Timeout = %v, want %v", cfg.Middleware.Timeout, 15*time.Second)
	}
	if cfg.Middleware.Logger.Level != "prod" {
		t.Errorf("Middleware.Logger.Level = %q, want %q", cfg.Middleware.Logger.Level, "prod")
	}
}

func TestLoadConfigFileNotFound(t *testing.T) {
	_, err := loadConfig("/nonexistent/path.yaml")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestLoadConfigInvalidYAML(t *testing.T) {
	path := writeTestConfig(t, `invalid: yaml: broken`)
	_, err := loadConfig(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML, got nil")
	}
}

func TestLoadConfigPartialYAML(t *testing.T) {
	yml := `
order:
  http:
    addr: ":7777"
`
	path := writeTestConfig(t, yml)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.Order.HTTP.Addr != ":7777" {
		t.Errorf("Order.HTTP.Addr = %q, want %q", cfg.Order.HTTP.Addr, ":7777")
	}
	if cfg.Middleware.Logger.Level != "" {
		t.Errorf("expected zero value for missing field, got %q", cfg.Middleware.Logger.Level)
	}
}

func TestNewDependencies(t *testing.T) {
	cfg := Config{
		Middleware: MiddlewareConfig{
			Logger: struct {
				Level string `yaml:"level"`
			}{
				Level: "dev",
			},
		},
	}
	deps := NewDependencies(cfg)
	if deps == nil {
		t.Fatal("NewDependencies returned nil")
	}
	if deps.Config.Middleware.Logger.Level != "dev" {
		t.Errorf("Logger level = %q, want %q", deps.Config.Middleware.Logger.Level, "dev")
	}
	if deps.Logger == nil {
		t.Error("Logger is nil")
	}
	if deps.Middleware == nil {
		t.Error("Middleware is nil")
	}
}
