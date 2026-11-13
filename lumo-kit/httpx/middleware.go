package httpx

import (
	"bufio"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/logx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/metrics"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/trace"
)

// Middleware wraps a handler.
type Middleware func(http.Handler) http.Handler

// Chain applies middleware so the first one listed runs first.
func Chain(h http.Handler, mw ...Middleware) http.Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}

const RequestIDHeader = "X-Request-ID"

// Observe sets the trace context and request ID, recovers panics, logs one
// line per request and records RED metrics.
func Observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx, span := trace.Start(r.Context(), r.Header.Get(trace.Header))
		rid := r.Header.Get(RequestIDHeader)
		if rid == "" || len(rid) > 64 {
			rid = span.TraceID[:16]
		}
		ctx = logx.With(ctx, "trace_id", span.TraceID, "request_id", rid)
		w.Header().Set(RequestIDHeader, rid)
		r = r.WithContext(ctx)

		sw := &statusWriter{ResponseWriter: w, status: 200}
		metrics.HTTPInFlight.Inc()
		defer func() {
			metrics.HTTPInFlight.Dec()
			if rec := recover(); rec != nil {
				if rec == http.ErrAbortHandler {
					panic(rec)
				}
				slog.ErrorContext(ctx, "panic", "panic", fmt.Sprint(rec), "stack", string(debug.Stack()))
				if !sw.wrote {
					JSON(sw, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "internal", "message": "Something went wrong.", "trace_id": span.TraceID}})
				}
			}
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			dur := time.Since(start)
			metrics.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(sw.status)).Inc()
			metrics.HTTPDuration.WithLabelValues(r.Method, route).Observe(dur.Seconds())
			lvl := slog.LevelInfo
			if sw.status >= 500 {
				lvl = slog.LevelError
			} else if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" || r.URL.Path == "/metrics" {
				return
			}
			slog.Log(ctx, lvl, "http", "method", r.Method, "route", route, "status", sw.status,
				"dur_ms", dur.Milliseconds(), "bytes", sw.bytes, "ip", ClientIP(r))
		}()
		next.ServeHTTP(sw, r)
	})
}

// Timeout bounds handler time (not used for WebSocket routes).
func Timeout(d time.Duration) Middleware {
	return func(next http.Handler) http.Handler {
		return http.TimeoutHandler(next, d, `{"error":{"code":"timeout","message":"The request took too long."}}`)
	}
}

// ClientIP returns the client IP. Behind the gateway/load balancer the first
// X-Forwarded-For hop is used; the gateway overwrites the header so clients
// cannot spoof it.
func ClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		ip, _, _ := strings.Cut(xff, ",")
		return strings.TrimSpace(ip)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
	wrote  bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wrote {
		w.status = code
		w.wrote = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	w.wrote = true
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack lets WebSocket upgrades pass through the middleware.
func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("hijack not supported")
	}
	w.status = http.StatusSwitchingProtocols
	w.wrote = true
	return h.Hijack()
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// InternalOnly guards service-to-service endpoints (/internal/...) with a
// shared token in X-Internal-Token. The gateway never routes /internal, and
// Kubernetes NetworkPolicies limit which pods can reach them; the token is a
// second lock on the door.
func InternalOnly(token string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got := r.Header.Get("X-Internal-Token")
			if token == "" || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
				JSON(w, http.StatusForbidden, map[string]any{"error": map[string]string{"code": "forbidden", "message": "internal endpoint"}})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
