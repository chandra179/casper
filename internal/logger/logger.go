package logger

import (
	"context"
	"log/slog"
	"os"
	"time"
)

type Logger interface {
	Info(ctx context.Context, msg string, fields ...Field)
	Error(ctx context.Context, msg string, fields ...Field)
}

type Field struct {
	Key   string
	Value any
}

func NewLogger(level string) Logger {
	var lvl slog.Level
	switch level {
	case "dev", "debug":
		lvl = slog.LevelDebug
	case "prod", "warn":
		lvl = slog.LevelWarn
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return &slogger{h}
}

type slogger struct {
	h slog.Handler
}

func (l *slogger) log(ctx context.Context, level slog.Level, msg string, fields ...Field) {
	var attrs []slog.Attr
	for _, f := range fields {
		attrs = append(attrs, slog.Any(f.Key, f.Value))
	}
	r := slog.NewRecord(time.Now(), level, msg, 0)
	r.AddAttrs(attrs...)
	_ = l.h.Handle(ctx, r)
}

func (l *slogger) Info(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, slog.LevelInfo, msg, fields...)
}

func (l *slogger) Error(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, slog.LevelError, msg, fields...)
}
