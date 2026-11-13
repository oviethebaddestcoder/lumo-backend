package httpx

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/metrics"
)

// Check is a readiness dependency check (DB ping, Redis ping, ...).
type Check struct {
	Name string
	Fn   func(context.Context) error
}

// Server is an HTTP server with health, readiness and metrics endpoints and
// graceful shutdown: on SIGTERM readiness fails first so the load balancer
// drains the pod, then in-flight requests finish.
type Server struct {
	srv      *http.Server
	draining atomic.Bool
	checks   []Check
	drain    time.Duration
}

// NewServer wraps app routes with Observe and adds /healthz, /readyz, /metrics.
func NewServer(addr string, app http.Handler, checks ...Check) *Server {
	s := &Server{checks: checks, drain: 5 * time.Second}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", s.ready)
	mux.Handle("GET /metrics", metrics.Handler())
	if app == nil {
		app = http.NotFoundHandler()
	}
	mux.Handle("/", app)
	s.srv = &http.Server{
		Addr:              addr,
		Handler:           Observe(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      0, // per-route via Timeout middleware; WebSockets need none
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    32 << 10,
	}
	return s
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		http.Error(w, "draining", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	for _, c := range s.checks {
		if err := c.Fn(ctx); err != nil {
			slog.WarnContext(ctx, "readiness check failed", "check", c.Name, "err", err)
			http.Error(w, c.Name+" not ready", http.StatusServiceUnavailable)
			return
		}
	}
	_, _ = w.Write([]byte("ready"))
}

// Run serves until ctx is cancelled, then drains and shuts down.
func (s *Server) Run(ctx context.Context) error {
	errc := make(chan error, 1)
	go func() {
		slog.Info("http listening", "addr", s.srv.Addr)
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
		close(errc)
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	s.draining.Store(true)
	slog.Info("draining", "wait", s.drain)
	time.Sleep(s.drain)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return s.srv.Shutdown(shutdownCtx)
}

// HTTP returns the underlying server (tests, tuning).
func (s *Server) HTTP() *http.Server { return s.srv }

// SetDrain overrides the drain wait (tests use 0).
func (s *Server) SetDrain(d time.Duration) { s.drain = d }
