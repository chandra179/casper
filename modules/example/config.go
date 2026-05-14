package example

import "time"

type httpConfig struct {
	Addr         string        `yaml:"addr"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	IdleTimeout  time.Duration `yaml:"idle_timeout"`
}

type appConfig struct {
	HTTP httpConfig `yaml:"http"`
}

type middlewareConfig struct {
	Timeout time.Duration `yaml:"timeout"`
	Logger  struct {
		Level string `yaml:"level"`
	} `yaml:"logger"`
}

type Config struct {
	AppCfg     appConfig        `yaml:"app_config"`
	Middleware middlewareConfig `yaml:"middleware"`
}
