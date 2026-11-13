// Package outbox implements the transactional outbox pattern.
//
// A service writes its business rows and an outbox row in ONE transaction
// (Insert). A relay publishes outbox rows to Kafka in id order and deletes
// them. This guarantees an event exists if and only if the write committed.
//
// Ordering and leadership: exactly one relay per database publishes at a
// time, chosen by a lease row (works through Neon's PgBouncer pooler, unlike
// session advisory locks). Publishing is at-least-once; consumers dedupe on
// event_id.
//
// Required schema (each service includes it in its first migration):
//
//	CREATE TABLE outbox (
//	  id bigserial PRIMARY KEY,
//	  topic text NOT NULL,
//	  key text NOT NULL,
//	  payload jsonb NOT NULL,
//	  created_at timestamptz NOT NULL DEFAULT now());
//	CREATE TABLE outbox_lease (
//	  name text PRIMARY KEY, holder text NOT NULL, expires_at timestamptz NOT NULL);
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/kafkax"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/metrics"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/trace"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Schema is the DDL every service's first migration must contain.
const Schema = `
CREATE TABLE IF NOT EXISTS outbox (
  id bigserial PRIMARY KEY,
  topic text NOT NULL,
  key text NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS outbox_lease (
  name text PRIMARY KEY,
  holder text NOT NULL,
  expires_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS processed_events (
  consumer text NOT NULL,
  event_id text NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
);
CREATE INDEX IF NOT EXISTS processed_events_at ON processed_events (processed_at);
`

// Insert writes events to the outbox inside tx.
func Insert(ctx context.Context, tx db.Querier, topic string, envs ...events.Envelope) error {
	if len(envs) == 0 {
		return nil
	}
	b := &pgx.Batch{}
	tid := trace.TraceID(ctx)
	for _, e := range envs {
		if e.TraceID == "" {
			e.TraceID = tid
		}
		raw, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("outbox: marshal: %w", err)
		}
		b.Queue(`INSERT INTO outbox (topic, key, payload) VALUES ($1, $2, $3)`, topic, e.Key, raw)
	}
	return tx.SendBatch(ctx, b).Close()
}

// Relay publishes outbox rows.
type Relay struct {
	Name     string // lease name, e.g. "identity" or "wallet-shard-3"
	Pool     *pgxpool.Pool
	Producer *kafkax.Producer
	Batch    int
	LeaseTTL time.Duration
	holder   string
}

func NewRelay(name string, pool *pgxpool.Pool, p *kafkax.Producer) *Relay {
	host, _ := os.Hostname()
	return &Relay{Name: name, Pool: pool, Producer: p, Batch: 500, LeaseTTL: 15 * time.Second,
		holder: fmt.Sprintf("%s-%s", host, events.NewID()[24:])}
}

// Run loops until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	leaseEvery := r.LeaseTTL / 3
	nextLease := time.Time{}
	leader := false
	idle := 20 * time.Millisecond
	for ctx.Err() == nil {
		if time.Now().After(nextLease) {
			ok, err := r.acquire(ctx)
			if err != nil {
				slog.WarnContext(ctx, "outbox lease error", "relay", r.Name, "err", err)
			}
			if ok != leader {
				slog.Info("outbox leadership changed", "relay", r.Name, "leader", ok)
			}
			leader = ok
			metrics.OutboxIsLeader.WithLabelValues(r.Name).Set(b2f(leader))
			nextLease = time.Now().Add(leaseEvery)
		}
		if !leader {
			if !wait(ctx, leaseEvery) {
				break
			}
			continue
		}
		n, err := r.publishBatch(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "outbox publish failed", "relay", r.Name, "err", err)
			if !wait(ctx, time.Second) {
				break
			}
			continue
		}
		if n == 0 {
			if !wait(ctx, idle) {
				break
			}
			idle = min(idle*2, 250*time.Millisecond)
		} else {
			idle = 20 * time.Millisecond
		}
	}
	r.release(context.WithoutCancel(ctx))
	return nil
}

func (r *Relay) acquire(ctx context.Context) (bool, error) {
	var holder string
	err := r.Pool.QueryRow(ctx, `
		INSERT INTO outbox_lease (name, holder, expires_at) VALUES ($1, $2, now() + make_interval(secs => $3))
		ON CONFLICT (name) DO UPDATE SET holder = EXCLUDED.holder, expires_at = EXCLUDED.expires_at
		WHERE outbox_lease.holder = EXCLUDED.holder OR outbox_lease.expires_at < now()
		RETURNING holder`, r.Name, r.holder, r.LeaseTTL.Seconds()).Scan(&holder)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return holder == r.holder, nil
}

func (r *Relay) release(ctx context.Context) {
	_, _ = r.Pool.Exec(ctx, `DELETE FROM outbox_lease WHERE name = $1 AND holder = $2`, r.Name, r.holder)
}

func (r *Relay) publishBatch(ctx context.Context) (int, error) {
	rows, err := r.Pool.Query(ctx, `SELECT id, topic, payload, created_at FROM outbox ORDER BY id LIMIT $1`, r.Batch)
	if err != nil {
		return 0, err
	}
	var ids []int64
	var recs []*kgo.Record
	var oldest time.Time
	for rows.Next() {
		var id int64
		var topic string
		var payload []byte
		var at time.Time
		if err := rows.Scan(&id, &topic, &payload, &at); err != nil {
			rows.Close()
			return 0, err
		}
		if oldest.IsZero() {
			oldest = at
		}
		var env events.Envelope
		if err := json.Unmarshal(payload, &env); err != nil {
			rows.Close()
			return 0, fmt.Errorf("outbox row %d: %w", id, err)
		}
		rec, err := kafkax.Record(topic, env)
		if err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
		recs = append(recs, rec)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		metrics.OutboxOldestAge.WithLabelValues(r.Name).Set(0)
		return 0, nil
	}
	metrics.OutboxOldestAge.WithLabelValues(r.Name).Set(time.Since(oldest).Seconds())
	if err := r.Producer.Client().ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return 0, fmt.Errorf("produce: %w", err)
	}
	if _, err := r.Pool.Exec(ctx, `DELETE FROM outbox WHERE id = ANY($1)`, ids); err != nil {
		return 0, fmt.Errorf("delete published: %w", err)
	}
	metrics.OutboxPublished.WithLabelValues(r.Name).Add(float64(len(ids)))
	return len(ids), nil
}

// MarkProcessed records that consumer handled eventID, inside tx. It returns
// false if the event was already processed (skip the side effects).
func MarkProcessed(ctx context.Context, tx db.Querier, consumer, eventID string) (bool, error) {
	tag, err := tx.Exec(ctx, `INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, consumer, eventID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// PruneProcessed deletes dedupe rows older than keep, in small batches.
// Run it from a worker every few minutes; keep must exceed topic retention.
func PruneProcessed(ctx context.Context, pool *pgxpool.Pool, keep time.Duration) error {
	for {
		tag, err := pool.Exec(ctx, `DELETE FROM processed_events WHERE ctid IN (
			SELECT ctid FROM processed_events WHERE processed_at < now() - make_interval(secs => $1) LIMIT 5000)`, keep.Seconds())
		if err != nil {
			return err
		}
		if tag.RowsAffected() < 5000 {
			return nil
		}
	}
}

func wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
