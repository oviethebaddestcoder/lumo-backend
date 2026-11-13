package kafkax

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/logx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/metrics"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/trace"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Handler processes one event. Return Permanent(err) to skip retries and send
// the event straight to the DLQ (e.g. a payload that can never be valid).
// Handlers MUST be idempotent: Kafka delivers at least once.
type Handler func(ctx context.Context, e events.Envelope) error

// BatchHandler processes all events fetched for one partition at once
// (in order). Used where aggregation matters, e.g. crediting a popular host.
type BatchHandler func(ctx context.Context, es []events.Envelope) error

type permanent struct{ error }

func (p permanent) Unwrap() error { return p.error }

// Permanent marks an error as not worth retrying.
func Permanent(err error) error { return permanent{err} }

// ConsumerOptions configures a consumer group.
type ConsumerOptions struct {
	Group  string
	Topics []string
	// Handle or HandleBatch (exactly one).
	Handle      Handler
	HandleBatch BatchHandler
	// MaxAttempts per event (or batch) before DLQ. Default 8 (~25s of backoff).
	MaxAttempts int
	// BatchSize caps a batch (HandleBatch). Default 500.
	BatchSize int
	// StartAtEnd makes a brand-new group start from the newest offset.
	StartAtEnd bool
	// HandlerTimeout bounds one handler call. Default 30s.
	HandlerTimeout time.Duration
}

type tp struct {
	t string
	p int32
}

type partitionWorker struct {
	quit chan struct{}
	done chan struct{}
	recs chan []*kgo.Record
}

// Consumer runs a consumer group: a goroutine per assigned partition keeps
// per-key ordering while partitions run in parallel. Offsets are marked only
// after a record is handled (or dead-lettered) and committed automatically;
// on rebalance, in-flight work finishes and marks are committed first.
type Consumer struct {
	o   ConsumerOptions
	cl  *kgo.Client
	dlq *kgo.Client

	mu      sync.Mutex
	workers map[tp]*partitionWorker
	ctx     context.Context
}

func NewConsumer(c Config, o ConsumerOptions, extra ...kgo.Opt) (*Consumer, error) {
	if (o.Handle == nil) == (o.HandleBatch == nil) {
		return nil, errors.New("kafkax: set exactly one of Handle or HandleBatch")
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 8
	}
	if o.BatchSize <= 0 {
		o.BatchSize = 500
	}
	if o.HandlerTimeout <= 0 {
		o.HandlerTimeout = 30 * time.Second
	}
	base, err := c.BaseOpts()
	if err != nil {
		return nil, err
	}
	k := &Consumer{o: o, workers: map[tp]*partitionWorker{}}
	reset := kgo.NewOffset().AtStart()
	if o.StartAtEnd {
		reset = kgo.NewOffset().AtEnd()
	}
	opts := append([]kgo.Opt{}, base...)
	opts = append(opts,
		kgo.ConsumerGroup(o.Group),
		kgo.ConsumeTopics(o.Topics...),
		kgo.ConsumeResetOffset(reset),
		kgo.AutoCommitMarks(),
		kgo.AutoCommitInterval(2*time.Second),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(k.assigned),
		kgo.OnPartitionsRevoked(k.revoked),
		kgo.OnPartitionsLost(k.lost),
		kgo.FetchMaxWait(500*time.Millisecond),
		kgo.SessionTimeout(45*time.Second),
		kgo.RebalanceTimeout(90*time.Second),
	)
	opts = append(opts, extra...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafkax: consumer: %w", err)
	}
	k.cl = cl
	dlqOpts := append([]kgo.Opt{}, base...)
	dlqOpts = append(dlqOpts, kgo.RequiredAcks(kgo.AllISRAcks()))
	if k.dlq, err = kgo.NewClient(dlqOpts...); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafkax: dlq producer: %w", err)
	}
	return k, nil
}

// Run polls until ctx is cancelled, then leaves the group cleanly.
func (k *Consumer) Run(ctx context.Context) error {
	k.mu.Lock()
	k.ctx = ctx
	k.mu.Unlock()
	defer func() {
		k.cl.CloseAllowingRebalance()
		k.dlq.Close()
	}()
	slog.Info("consumer started", "group", k.o.Group, "topics", k.o.Topics)
	for {
		fetches := k.cl.PollRecords(ctx, 10000)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		for _, fe := range fetches.Errors() {
			if errors.Is(fe.Err, context.Canceled) {
				return nil
			}
			slog.Error("fetch error", "group", k.o.Group, "topic", fe.Topic, "partition", fe.Partition, "err", fe.Err)
		}
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return
			}
			k.mu.Lock()
			w := k.workers[tp{p.Topic, p.Partition}]
			k.mu.Unlock()
			if w == nil {
				return // revoked between poll and dispatch; records will be redelivered
			}
			select {
			case w.recs <- p.Records:
			case <-w.quit:
			case <-ctx.Done():
			}
		})
		k.cl.AllowRebalance()
	}
}

func (k *Consumer) assigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	k.mu.Lock()
	defer k.mu.Unlock()
	for topic, parts := range assigned {
		for _, p := range parts {
			w := &partitionWorker{quit: make(chan struct{}), done: make(chan struct{}), recs: make(chan []*kgo.Record, 4)}
			k.workers[tp{topic, p}] = w
			go k.work(topic, p, w)
		}
	}
}

func (k *Consumer) revoked(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
	k.stop(revoked)
	if err := cl.CommitMarkedOffsets(ctx); err != nil {
		slog.Warn("commit on revoke failed", "group", k.o.Group, "err", err)
	}
}

func (k *Consumer) lost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	k.stop(lost)
}

func (k *Consumer) stop(parts map[string][]int32) {
	var wg sync.WaitGroup
	k.mu.Lock()
	for topic, ps := range parts {
		for _, p := range ps {
			key := tp{topic, p}
			w := k.workers[key]
			if w == nil {
				continue
			}
			delete(k.workers, key)
			close(w.quit)
			wg.Add(1)
			go func() { <-w.done; wg.Done() }()
		}
	}
	k.mu.Unlock()
	wg.Wait()
}

func (k *Consumer) work(topic string, partition int32, w *partitionWorker) {
	defer close(w.done)
	k.mu.Lock()
	parent := k.ctx
	k.mu.Unlock()
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	go func() {
		select {
		case <-w.quit:
			cancel()
		case <-ctx.Done():
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case recs := <-w.recs:
			if k.o.HandleBatch != nil {
				for i := 0; i < len(recs); i += k.o.BatchSize {
					end := min(i+k.o.BatchSize, len(recs))
					if !k.processBatch(ctx, topic, recs[i:end]) {
						return
					}
				}
				continue
			}
			for _, r := range recs {
				if !k.processOne(ctx, r) {
					return
				}
			}
		}
	}
}

// processOne returns false when the worker should stop (shutdown/revoke).
func (k *Consumer) processOne(ctx context.Context, r *kgo.Record) bool {
	env, err := decode(r)
	if err != nil {
		return k.deadLetter(ctx, r, fmt.Errorf("decode: %w", err))
	}
	ectx := eventContext(ctx, env)
	var lastErr error
	for attempt := 1; attempt <= k.o.MaxAttempts; attempt++ {
		start := time.Now()
		hctx, cancel := context.WithTimeout(ectx, k.o.HandlerTimeout)
		lastErr = k.o.Handle(hctx, env)
		cancel()
		metrics.EventHandleDuration.WithLabelValues(k.o.Group, r.Topic).Observe(time.Since(start).Seconds())
		if lastErr == nil {
			metrics.EventsConsumed.WithLabelValues(k.o.Group, r.Topic, "ok").Inc()
			k.cl.MarkCommitRecords(r)
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		var p permanent
		if errors.As(lastErr, &p) {
			break
		}
		metrics.EventsConsumed.WithLabelValues(k.o.Group, r.Topic, "retry").Inc()
		slog.WarnContext(ectx, "event handler failed", "group", k.o.Group, "type", env.Type, "attempt", attempt, "err", lastErr)
		if !sleep(ctx, backoff(attempt)) {
			return false
		}
	}
	return k.deadLetter(ectx, r, lastErr)
}

func (k *Consumer) processBatch(ctx context.Context, topic string, recs []*kgo.Record) bool {
	envs := make([]events.Envelope, 0, len(recs))
	good := make([]*kgo.Record, 0, len(recs))
	for _, r := range recs {
		env, err := decode(r)
		if err != nil {
			if !k.deadLetter(ctx, r, fmt.Errorf("decode: %w", err)) {
				return false
			}
			continue
		}
		envs = append(envs, env)
		good = append(good, r)
	}
	if len(envs) == 0 {
		return true
	}
	var lastErr error
	for attempt := 1; attempt <= k.o.MaxAttempts; attempt++ {
		start := time.Now()
		hctx, cancel := context.WithTimeout(ctx, k.o.HandlerTimeout)
		lastErr = k.o.HandleBatch(hctx, envs)
		cancel()
		metrics.EventHandleDuration.WithLabelValues(k.o.Group, topic).Observe(time.Since(start).Seconds())
		if lastErr == nil {
			metrics.EventsConsumed.WithLabelValues(k.o.Group, topic, "ok").Add(float64(len(envs)))
			k.cl.MarkCommitRecords(good...)
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		var p permanent
		if errors.As(lastErr, &p) {
			break
		}
		slog.WarnContext(ctx, "batch handler failed", "group", k.o.Group, "size", len(envs), "attempt", attempt, "err", lastErr)
		if !sleep(ctx, backoff(attempt)) {
			return false
		}
	}
	// Isolate the poison event: retry one by one, dead-lettering only failures.
	for i, r := range good {
		hctx, cancel := context.WithTimeout(ctx, k.o.HandlerTimeout)
		err := k.o.HandleBatch(hctx, envs[i:i+1])
		cancel()
		if err == nil {
			k.cl.MarkCommitRecords(r)
			continue
		}
		if !k.deadLetter(ctx, r, err) {
			return false
		}
	}
	return true
}

// deadLetter publishes r to <topic>.dlq and marks it. It retries the DLQ
// write until it succeeds or the worker stops: an event is never dropped.
func (k *Consumer) deadLetter(ctx context.Context, r *kgo.Record, cause error) bool {
	d := &kgo.Record{Topic: events.DLQ(r.Topic), Key: r.Key, Value: r.Value}
	d.Headers = append(append(d.Headers, r.Headers...),
		kgo.RecordHeader{Key: "dlq_error", Value: []byte(truncate(fmt.Sprint(cause), 2000))},
		kgo.RecordHeader{Key: "dlq_group", Value: []byte(k.o.Group)},
		kgo.RecordHeader{Key: "dlq_source", Value: []byte(r.Topic + "/" + strconv.Itoa(int(r.Partition)) + "/" + strconv.FormatInt(r.Offset, 10))},
		kgo.RecordHeader{Key: "dlq_at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)
	for attempt := 1; ; attempt++ {
		err := k.dlq.ProduceSync(ctx, d).FirstErr()
		if err == nil {
			metrics.EventsDLQ.WithLabelValues(k.o.Group, r.Topic).Inc()
			metrics.EventsConsumed.WithLabelValues(k.o.Group, r.Topic, "dlq").Inc()
			slog.ErrorContext(ctx, "event dead-lettered", "group", k.o.Group, "topic", r.Topic, "offset", r.Offset, "cause", cause)
			k.cl.MarkCommitRecords(r)
			return true
		}
		slog.ErrorContext(ctx, "dlq publish failed", "err", err, "attempt", attempt)
		if !sleep(ctx, backoff(attempt)) {
			return false
		}
	}
}

func decode(r *kgo.Record) (events.Envelope, error) {
	var e events.Envelope
	if err := json.Unmarshal(r.Value, &e); err != nil {
		return e, err
	}
	if e.ID == "" || e.Type == "" {
		return e, errors.New("missing event_id or type")
	}
	return e, nil
}

func eventContext(ctx context.Context, e events.Envelope) context.Context {
	ctx = trace.WithTraceID(ctx, e.TraceID)
	return logx.With(ctx, "trace_id", trace.TraceID(ctx), "event_id", e.ID, "event_type", e.Type)
}

func backoff(attempt int) time.Duration {
	d := time.Duration(100*(1<<min(attempt, 8))) * time.Millisecond
	return min(d, 10*time.Second)
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
