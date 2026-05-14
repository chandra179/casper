package order

import (
	"context"
	"log"
	"net/http"

	"github.com/Chandra179/gosdk/logger"

	"brook/middleware"
)

func RunHttpServer() {
	cfg, err := loadConfig("config/config.yaml")
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	deps := NewDependencies(cfg)

	mux := http.NewServeMux()
	// mux.HandleFunc("POST /orders", HandleCreateOrder)

	chain := middleware.Chain(
		mux,
		deps.Middleware.Recovery(),
		middleware.RequestID,
		middleware.Timeout(middleware.TimeoutConfig{Duration: deps.Config.Middleware.Timeout}),
	)

	srv := &http.Server{
		Addr:         deps.Config.Order.HTTP.Addr,
		Handler:      chain,
		ReadTimeout:  deps.Config.Order.HTTP.ReadTimeout,
		WriteTimeout: deps.Config.Order.HTTP.WriteTimeout,
		IdleTimeout:  deps.Config.Order.HTTP.IdleTimeout,
	}

	deps.Logger.Info(context.Background(), "starting HTTP server", logger.Field{Key: "addr", Value: deps.Config.Order.HTTP.Addr})
	if err := srv.ListenAndServe(); err != nil {
		deps.Logger.Error(context.Background(), "server error", logger.Field{Key: "error", Value: err.Error()})
	}
}
