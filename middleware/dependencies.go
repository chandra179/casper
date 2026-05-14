package middleware

import (
	"casper/internal/logger"
)

type Dependencies struct {
	logger logger.Logger
}

func NewDependencies(logger logger.Logger) *Dependencies {
	return &Dependencies{logger: logger}
}
