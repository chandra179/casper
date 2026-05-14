package order

import "time"

type OrderHTTPConfig struct {
	Addr         string        `yaml:"addr"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	IdleTimeout  time.Duration `yaml:"idle_timeout"`
}

type OrderGRPCConfig struct {
	Addr string `yaml:"addr"`
}

type OrderConfig struct {
	HTTP OrderHTTPConfig `yaml:"http"`
	GRPC OrderGRPCConfig `yaml:"grpc"`
}

type MiddlewareConfig struct {
	Timeout time.Duration `yaml:"timeout"`
	Logger  struct {
		Level string `yaml:"level"`
	} `yaml:"logger"`
}

type Config struct {
	Order      OrderConfig      `yaml:"order"`
	Middleware MiddlewareConfig `yaml:"middleware"`
}
