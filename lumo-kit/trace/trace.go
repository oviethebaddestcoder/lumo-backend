// Package trace propagates W3C trace context (traceparent) across HTTP and
// Kafka so one trace_id follows a request through every service and log line.
// It is deliberately small; swap in the OpenTelemetry SDK exporter when you
// add a tracing backend, keeping the same header format.
package trace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"
)

const Header = "traceparent"

type ctxKey struct{}

type Span struct {
	TraceID string
	SpanID  string
}

func newHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Parse reads a traceparent header ("00-<32 hex>-<16 hex>-<2 hex>").
func Parse(h string) (Span, bool) {
	p := strings.Split(strings.TrimSpace(h), "-")
	if len(p) != 4 || len(p[1]) != 32 || len(p[2]) != 16 {
		return Span{}, false
	}
	if _, err := hex.DecodeString(p[1]); err != nil {
		return Span{}, false
	}
	return Span{TraceID: p[1], SpanID: p[2]}, true
}

// Start continues the trace in parent (or starts one) with a new span ID.
func Start(ctx context.Context, parent string) (context.Context, Span) {
	s, ok := Parse(parent)
	if !ok {
		s.TraceID = newHex(16)
	}
	s.SpanID = newHex(8)
	return context.WithValue(ctx, ctxKey{}, s), s
}

// FromContext returns the current span, if any.
func FromContext(ctx context.Context) (Span, bool) {
	s, ok := ctx.Value(ctxKey{}).(Span)
	return s, ok
}

// TraceID returns the current trace ID or "".
func TraceID(ctx context.Context) string {
	s, _ := FromContext(ctx)
	return s.TraceID
}

// Traceparent renders the header value for outgoing calls.
func Traceparent(ctx context.Context) string {
	s, ok := FromContext(ctx)
	if !ok {
		return ""
	}
	return "00-" + s.TraceID + "-" + s.SpanID + "-01"
}

// WithTraceID starts a span in an existing trace (used by Kafka consumers).
func WithTraceID(ctx context.Context, traceID string) context.Context {
	if len(traceID) != 32 {
		traceID = newHex(16)
	}
	return context.WithValue(ctx, ctxKey{}, Span{TraceID: traceID, SpanID: newHex(8)})
}
