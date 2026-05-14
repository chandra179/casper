package api

import (
	taskmod "casper/modules/task"
)

type Dependencies struct {
	Config Config
	Store  *taskmod.Store
}

func NewDependencies(cfg Config, store *taskmod.Store) *Dependencies {
	return &Dependencies{
		Config: cfg,
		Store:  store,
	}
}
