package kafkax_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/kafkax"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func cluster(t *testing.T, topics ...string) kafkax.Config {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(4, topics...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return kafkax.Config{Brokers: c.ListenAddrs(), ClientID: "test"}
}

// Per-key ordering holds while partitions are processed in parallel, a
// transient failure is retried, and a poison event goes to the DLQ without
// blocking the events behind it.
func TestConsumerOrderingRetryAndDLQ(t *testing.T) {
	topic := "test.events"
	cfg := cluster(t, topic, events.DLQ(topic))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prod, err := kafkax.NewProducer(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	const keys, perKey = 8, 50
	for i := 0; i < perKey; i++ {
		for k := 0; k < keys; k++ {
			e := events.MustNew("test", "test.seq", fmt.Sprintf("key-%d", k), map[string]int{"n": i})
			if err := prod.Publish(ctx, topic, e); err != nil {
				t.Fatal(err)
			}
		}
	}
	poison := events.MustNew("test", "test.poison", "key-0", map[string]int{"n": -1})
	if err := prod.Publish(ctx, topic, poison); err != nil {
		t.Fatal(err)
	}
	flaky := events.MustNew("test", "test.flaky", "key-1", map[string]int{"n": -2})
	if err := prod.Publish(ctx, topic, flaky); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	last := map[string]int{}
	seen := 0
	flakyTries := 0
	done := make(chan struct{})
	c, err := kafkax.NewConsumer(cfg, kafkax.ConsumerOptions{
		Group: "g1", Topics: []string{topic}, MaxAttempts: 3,
		Handle: func(_ context.Context, e events.Envelope) error {
			mu.Lock()
			defer mu.Unlock()
			switch e.Type {
			case "test.poison":
				return kafkax.Permanent(errors.New("cannot ever process"))
			case "test.flaky":
				flakyTries++
				if flakyTries < 2 {
					return errors.New("transient")
				}
				return nil
			}
			var d struct{ N int }
			_ = e.Decode(&d)
			if prev, ok := last[e.Key]; ok && d.N != prev+1 {
				t.Errorf("key %s out of order: %d after %d", e.Key, d.N, prev)
			}
			last[e.Key] = d.N
			seen++
			if seen == keys*perKey {
				close(done)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	go c.Run(ctx)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("timed out; handled %d", seen)
	}

	// Poison event landed in the DLQ with its error.
	cl, _ := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ConsumeTopics(events.DLQ(topic)))
	defer cl.Close()
	for {
		f := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatal("no DLQ record")
		}
		found := false
		f.EachRecord(func(r *kgo.Record) {
			for _, h := range r.Headers {
				if h.Key == "dlq_error" {
					found = true
				}
			}
		})
		if found {
			break
		}
	}
	// The flaky event succeeds on its second attempt (after backoff).
	for {
		mu.Lock()
		tries := flakyTries
		mu.Unlock()
		if tries == 2 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("flaky event tries = %d, want 2", tries)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
