// Package metrics exposes Prometheus metrics shared by every service.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var Registry = prometheus.NewRegistry()

var factory = promauto.With(Registry)

func init() {
	Registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
}

var (
	HTTPRequests = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total", Help: "HTTP requests by route and status.",
	}, []string{"method", "route", "status"})

	HTTPDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name: "http_request_duration_seconds", Help: "HTTP latency by route.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"method", "route"})

	HTTPInFlight = factory.NewGauge(prometheus.GaugeOpts{
		Name: "http_in_flight_requests", Help: "Requests being served.",
	})

	EventsConsumed = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_events_consumed_total", Help: "Events handled by consumer group, topic and result.",
	}, []string{"group", "topic", "result"})

	EventHandleDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name: "kafka_event_handle_seconds", Help: "Event handler latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"group", "topic"})

	EventsDLQ = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_events_dlq_total", Help: "Events sent to a dead-letter topic.",
	}, []string{"group", "topic"})

	OutboxPublished = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "outbox_published_total", Help: "Outbox rows published to Kafka.",
	}, []string{"relay"})

	OutboxOldestAge = factory.NewGaugeVec(prometheus.GaugeOpts{
		Name: "outbox_oldest_age_seconds", Help: "Age of the oldest unpublished outbox row (alert > 30s).",
	}, []string{"relay"})

	OutboxIsLeader = factory.NewGaugeVec(prometheus.GaugeOpts{
		Name: "outbox_relay_leader", Help: "1 if this pod holds the relay lease.",
	}, []string{"relay"})
)

// NewCounterVec registers a service-specific counter.
func NewCounterVec(name, help string, labels ...string) *prometheus.CounterVec {
	return factory.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
}

// NewGaugeVec registers a service-specific gauge.
func NewGaugeVec(name, help string, labels ...string) *prometheus.GaugeVec {
	return factory.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
}

// NewHistogramVec registers a service-specific histogram.
func NewHistogramVec(name, help string, labels ...string) *prometheus.HistogramVec {
	return factory.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: prometheus.DefBuckets}, labels)
}

// Handler serves /metrics.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{Registry: Registry})
}
