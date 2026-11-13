package outbox_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/kafkax"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/outbox"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Two relays race for the lease; only one publishes, all rows arrive in
// order, and the table drains.
func TestRelayPublishesInOrderWithOneLeader(t *testing.T) {
	pg := os.Getenv("LUMO_TEST_PG")
	if pg == "" {
		t.Skip("set LUMO_TEST_PG")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, pg)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	name := fmt.Sprintf("outbox_test_%d", time.Now().UnixNano())
	admin.Exec(ctx, "CREATE DATABASE "+name)
	defer admin.Exec(context.Background(), "DROP DATABASE "+name+" WITH (FORCE)")
	cfgp, _ := pgx.ParseConfig(pg)
	pool, err := db.Open(ctx, fmt.Sprintf("postgres://%s@%s:%d/%s", cfgp.User, cfgp.Host, cfgp.Port, name), db.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, outbox.Schema); err != nil {
		t.Fatal(err)
	}

	kc, _ := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, "t.out"))
	defer kc.Close()
	kcfg := kafkax.Config{Brokers: kc.ListenAddrs(), ClientID: "relay-test"}

	const n = 300
	for i := 0; i < n; i++ {
		err := db.InTx(ctx, pool, func(tx pgx.Tx) error {
			return outbox.Insert(ctx, tx, "t.out", events.MustNew("test", "t.seq", "same-key", map[string]int{"n": i}))
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		p, err := kafkax.NewProducer(kcfg, "test")
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		r := outbox.NewRelay("test", pool, p)
		r.Batch = 50
		go r.Run(ctx)
	}

	cl, _ := kgo.NewClient(kgo.SeedBrokers(kcfg.Brokers...), kgo.ConsumeTopics("t.out"))
	defer cl.Close()
	next := 0
	for next < n {
		f := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("timed out at %d", next)
		}
		f.EachRecord(func(r *kgo.Record) {
			var e events.Envelope
			if err := jsonUnmarshal(r.Value, &e); err != nil {
				t.Fatal(err)
			}
			var d struct{ N int }
			e.Decode(&d)
			if d.N != next {
				t.Fatalf("got %d, want %d (order or duplicate)", d.N, next)
			}
			next++
		})
	}
	time.Sleep(300 * time.Millisecond)
	var left int
	pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&left)
	if left != 0 {
		t.Fatalf("%d rows left in outbox", left)
	}
}
