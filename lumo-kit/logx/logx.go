// Package logx sets up structured JSON logging and carries request-scoped
// attributes (request_id, trace_id, user_id) through context.
package logx

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

type ctxKey struct{}

// New returns a JSON logger tagged with the service name and sets it as default.
func New(service, level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	l := slog.New(&ctxHandler{h}).With("service", service)
	slog.SetDefault(l)
	return l
}

// With returns a context whose logger lines carry the given attributes.
func With(ctx context.Context, args ...any) context.Context {
	attrs, _ := ctx.Value(ctxKey{}).([]any)
	merged := make([]any, 0, len(attrs)+len(args))
	merged = append(merged, attrs...)
	merged = append(merged, args...)
	return context.WithValue(ctx, ctxKey{}, merged)
}

// L returns the default logger; context attributes are added by the handler.
func L() *slog.Logger { return slog.Default() }

type ctxHandler struct{ slog.Handler }

func (h *ctxHandler) Handle(ctx context.Context, r slog.Record) error {
	if attrs, ok := ctx.Value(ctxKey{}).([]any); ok {
		r.Add(attrs...)
	}
	return h.Handler.Handle(ctx, r)
}

func (h *ctxHandler) WithAttrs(as []slog.Attr) slog.Handler {
	return &ctxHandler{h.Handler.WithAttrs(as)}
}

func (h *ctxHandler) WithGroup(name string) slog.Handler {
	return &ctxHandler{h.Handler.WithGroup(name)}
}
