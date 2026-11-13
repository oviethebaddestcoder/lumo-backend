// Package kafkax wraps franz-go with Lumo's conventions: JSON event
// envelopes, trace propagation in headers, an idempotent producer, and a
// consumer that runs one goroutine per assigned partition.
package kafkax

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/config"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/trace"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Config is read from KAFKA_* environment variables.
type Config struct {
	Brokers  []string
	ClientID string
	SASL     string // "", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"
	User     string
	Pass     string
	TLS      bool
}

func ConfigFromEnv(e *config.Env, clientID string) Config {
	return Config{
		Brokers:  e.List("KAFKA_BROKERS", []string{"localhost:9092"}),
		ClientID: clientID,
		SASL:     strings.ToUpper(e.Str("KAFKA_SASL_MECHANISM", "")),
		User:     e.Str("KAFKA_USERNAME", ""),
		Pass:     e.Str("KAFKA_PASSWORD", ""),
		TLS:      e.Bool("KAFKA_TLS", false),
	}
}

// BaseOpts returns connection options shared by producers and consumers.
func (c Config) BaseOpts() ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(c.ClientID),
		kgo.RequestRetries(10),
		kgo.RetryTimeout(30 * time.Second),
	}
	if c.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	switch c.SASL {
	case "":
	case "PLAIN":
		opts = append(opts, kgo.SASL(plain.Auth{User: c.User, Pass: c.Pass}.AsMechanism()))
	case "SCRAM-SHA-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: c.User, Pass: c.Pass}.AsSha256Mechanism()))
	case "SCRAM-SHA-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: c.User, Pass: c.Pass}.AsSha512Mechanism()))
	default:
		return nil, fmt.Errorf("kafkax: unsupported SASL mechanism %q", c.SASL)
	}
	return opts, nil
}

// Producer publishes envelopes. It is idempotent (no duplicates from retries)
// and waits for all in-sync replicas, so an acknowledged event is durable.
type Producer struct {
	cl       *kgo.Client
	producer string
}

func NewProducer(c Config, producerName string, extra ...kgo.Opt) (*Producer, error) {
	opts, err := c.BaseOpts()
	if err != nil {
		return nil, err
	}
	opts = append(opts,
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.Lz4Compression(), kgo.NoCompression()),
		kgo.ProducerBatchMaxBytes(1<<20),
		kgo.RecordDeliveryTimeout(30*time.Second),
	)
	opts = append(opts, extra...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafkax: producer: %w", err)
	}
	return &Producer{cl: cl, producer: producerName}, nil
}

// Client exposes the underlying client (outbox relay, admin).
func (p *Producer) Client() *kgo.Client { return p.cl }

func (p *Producer) Close() { p.cl.Close() }

// Ping checks broker connectivity (readiness).
func (p *Producer) Ping(ctx context.Context) error { return p.cl.Ping(ctx) }

// Record builds a Kafka record from an envelope.
func Record(topic string, env events.Envelope) (*kgo.Record, error) {
	b, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}
	r := &kgo.Record{Topic: topic, Key: []byte(env.Key), Value: b}
	r.Headers = append(r.Headers,
		kgo.RecordHeader{Key: "event_type", Value: []byte(env.Type)},
		kgo.RecordHeader{Key: "event_id", Value: []byte(env.ID)},
	)
	if env.TraceID != "" {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: "trace_id", Value: []byte(env.TraceID)})
	}
	return r, nil
}

// Publish sends one event synchronously. Prefer the outbox for anything that
// must stay consistent with a database write.
func (p *Producer) Publish(ctx context.Context, topic string, env events.Envelope) error {
	if env.TraceID == "" {
		env.TraceID = trace.TraceID(ctx)
	}
	if env.Producer == "" {
		env.Producer = p.producer
	}
	r, err := Record(topic, env)
	if err != nil {
		return err
	}
	return p.cl.ProduceSync(ctx, r).FirstErr()
}

// PublishAsync is fire-and-forget with error logging. Use only for
// best-effort data (chat history copies, impressions).
func (p *Producer) PublishAsync(ctx context.Context, topic string, env events.Envelope) {
	if env.TraceID == "" {
		env.TraceID = trace.TraceID(ctx)
	}
	if env.Producer == "" {
		env.Producer = p.producer
	}
	r, err := Record(topic, env)
	if err != nil {
		slog.ErrorContext(ctx, "kafka encode failed", "topic", topic, "err", err)
		return
	}
	p.cl.Produce(context.WithoutCancel(ctx), r, func(_ *kgo.Record, err error) {
		if err != nil {
			slog.Warn("kafka async publish failed", "topic", topic, "type", env.Type, "err", err)
		}
	})
}
