package app_test

// Integration tests on two real Postgres databases acting as two shards.
// Run: LUMO_TEST_PG=postgres://postgres@localhost:5432/postgres go test ./...

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/shard"
	"github.com/oviethebaddestcoder/lumo-backend/svc-wallet/internal/app"
	"github.com/oviethebaddestcoder/lumo-backend/svc-wallet/internal/ledger"
	"github.com/oviethebaddestcoder/lumo-backend/svc-wallet/internal/stores"
	"github.com/oviethebaddestcoder/lumo-backend/svc-wallet/migrations"
)

func newDB(t *testing.T, admin *pgx.Conn, base string) *pgxpool.Pool {
	ctx := context.Background()
	name := fmt.Sprintf("wallet_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	cfg, _ := pgx.ParseConfig(base)
	dsn := fmt.Sprintf("postgres://%s@%s:%d/%s", cfg.User, cfg.Host, cfg.Port, name)
	t.Cleanup(func() { admin.Exec(context.Background(), "DROP DATABASE "+name+" WITH (FORCE)") })
	if err := db.Migrate(ctx, dsn, migrations.FS, migrations.LockID); err != nil {
		t.Fatal(err)
	}
	p, err := db.Open(ctx, dsn, db.Options{MaxConns: 40})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func setup(t *testing.T) (*app.Service, []*pgxpool.Pool) {
	pg := os.Getenv("LUMO_TEST_PG")
	if pg == "" {
		t.Skip("set LUMO_TEST_PG")
	}
	admin, err := pgx.Connect(context.Background(), pg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close(context.Background()) })
	a, b := newDB(t, admin, pg), newDB(t, admin, pg)
	router := shard.NewStatic(8, &shard.Range{From: 0, To: 3, Pool: a}, &shard.Range{From: 4, To: 7, Pool: b})
	return &app.Service{Shards: router, DiamondPercent: 50, MinPayout: 100, AccountSecret: []byte("secret-secret-secret-secret-1234")}, []*pgxpool.Pool{a, b}
}

// userOn returns a user ID that routes to the given shard index.
func userOn(s *app.Service, idx int) string {
	for {
		id := events.NewID()
		if s.Shards.For(id).Index == idx {
			return id
		}
	}
}

// fund credits coins through the real purchase path (claim + ledger).
func fund(t *testing.T, s *app.Service, user string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		ref := fmt.Sprintf("%s-%d", user, i)
		if err := s.Paystack_testCredit(context.Background(), user, ref); err != nil {
			t.Fatal(err)
		}
	}
}

func TestConcurrentGiftsNeverOverspend(t *testing.T) {
	s, _ := setup(t)
	ctx := context.Background()
	sender, host := userOn(s, 0), userOn(s, 1)
	fund(t, s, sender, 1) // coins_70 pack = 70 coins
	// 50 concurrent "Rose" (10 coins) sends, each a different request:
	// exactly 7 can succeed.
	var ok, broke atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.SendGift(ctx, app.SendGiftInput{RequestID: events.NewID(), SenderID: sender, ReceiverID: host, GiftID: "rose", Qty: 1, RoomID: "r1"})
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, app.ErrInsufficient):
				broke.Add(1)
			default:
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	b, _ := s.Balance(ctx, sender)
	if ok.Load() != 7 || broke.Load() != 43 || b.Coins != 0 {
		t.Fatalf("ok=%d insufficient=%d balance=%d; want 7/43/0", ok.Load(), broke.Load(), b.Coins)
	}
}

func TestGiftIdempotency(t *testing.T) {
	s, _ := setup(t)
	ctx := context.Background()
	sender, host := userOn(s, 0), userOn(s, 1)
	fund(t, s, sender, 1)
	in := app.SendGiftInput{RequestID: "req-1", SenderID: sender, ReceiverID: host, GiftID: "rose", Qty: 2, RoomID: "r1"}
	r1, err := s.SendGift(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := s.SendGift(ctx, in) // network retry
	if err != nil || !r2.Replayed || r2.GiftSendID != r1.GiftSendID {
		t.Fatalf("replay: %+v %v", r2, err)
	}
	b, _ := s.Balance(ctx, sender)
	if b.Coins != 50 {
		t.Fatalf("charged twice: balance %d", b.Coins)
	}
	in.Qty = 3 // same key, different body
	if _, err := s.SendGift(ctx, in); !errors.Is(err, app.ErrIdemConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
}

func TestCreditAcrossShardsDedupedAndBatched(t *testing.T) {
	s, pools := setup(t)
	ctx := context.Background()
	sender, host := userOn(s, 0), userOn(s, 1)
	fund(t, s, sender, 2) // 140 coins
	var envs []events.Envelope
	rows, _ := pools[0].Query(ctx, `SELECT payload FROM outbox WHERE topic = 'wallet.gifts' ORDER BY id`)
	rows.Close()
	for i := 0; i < 10; i++ {
		if _, err := s.SendGift(ctx, app.SendGiftInput{RequestID: fmt.Sprint(i), SenderID: sender, ReceiverID: host, GiftID: "rose", Qty: 1, RoomID: "r1"}); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := pools[0].Query(ctx, `SELECT payload FROM outbox WHERE topic = 'wallet.gifts' ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var e events.Envelope
		if err := rows.Scan(&e); err != nil {
			t.Fatal(err)
		}
		envs = append(envs, e)
	}
	rows.Close()
	if len(envs) != 10 {
		t.Fatalf("outbox has %d gift events", len(envs))
	}
	// Kafka is at-least-once: deliver the batch, then deliver half again.
	if err := s.CreditGifts(ctx, envs); err != nil {
		t.Fatal(err)
	}
	if err := s.CreditGifts(ctx, envs[:5]); err != nil {
		t.Fatal(err)
	}
	b, _ := s.Balance(ctx, host)
	if b.Diamonds != 50 { // 10 roses * 10 coins * 50%
		t.Fatalf("host diamonds = %d, want 50", b.Diamonds)
	}
	var credits int
	pools[1].QueryRow(ctx, `SELECT count(*) FROM transactions WHERE kind = 'gift_credit'`).Scan(&credits)
	if credits != 1 {
		t.Fatalf("want ONE batched credit transaction, got %d", credits)
	}
	for i, p := range pools {
		u, bad, err := ledger.Reconcile(ctx, p)
		if err != nil || u != 0 || bad != 0 {
			t.Fatalf("shard %d reconcile: unbalanced=%d mismatched=%d err=%v", i, u, bad, err)
		}
	}
}

func TestPurchaseClaimedOnceAcrossShards(t *testing.T) {
	s, _ := setup(t)
	ctx := context.Background()
	alice, bob := userOn(s, 0), userOn(s, 1)
	if err := s.Paystack_testCredit(ctx, alice, "receipt-1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Paystack_testCredit(ctx, alice, "receipt-1"); err != nil { // retry: no double credit
		t.Fatal(err)
	}
	if err := s.Paystack_testCredit(ctx, bob, "receipt-1"); !errors.Is(err, app.ErrPurchaseClaimd) {
		t.Fatalf("bob redeemed alice's receipt: %v", err)
	}
	b, _ := s.Balance(ctx, alice)
	if b.Coins != 70 {
		t.Fatalf("alice coins = %d", b.Coins)
	}
	if err := s.Reverse(ctx, "paystack", "receipt-1", "chargeback"); err != nil {
		t.Fatal(err)
	}
	if err := s.Reverse(ctx, "paystack", "receipt-1", "chargeback"); err != nil {
		t.Fatal(err)
	}
	b, _ = s.Balance(ctx, alice)
	if b.Coins != 0 {
		t.Fatalf("after reversal coins = %d", b.Coins)
	}
}

func TestPayout(t *testing.T) {
	s, pools := setup(t)
	ctx := context.Background()
	sender, host := userOn(s, 0), userOn(s, 1)
	fund(t, s, sender, 3)
	for i := 0; i < 20; i++ {
		s.SendGift(ctx, app.SendGiftInput{RequestID: fmt.Sprint(i), SenderID: sender, ReceiverID: host, GiftID: "rose", Qty: 1, VideoID: "v1"})
	}
	var envs []events.Envelope
	rows, _ := pools[0].Query(ctx, `SELECT payload FROM outbox WHERE topic = 'wallet.gifts'`)
	for rows.Next() {
		var e events.Envelope
		rows.Scan(&e)
		envs = append(envs, e)
	}
	rows.Close()
	s.CreditGifts(ctx, envs)
	if _, err := s.RequestPayout(ctx, host, "p1", 50); err == nil {
		t.Fatal("payout below minimum allowed")
	}
	res, err := s.RequestPayout(ctx, host, "p1", 100)
	if err != nil || res.Balance != 0 {
		t.Fatalf("payout: %+v %v", res, err)
	}
	if _, err := s.RequestPayout(ctx, host, "p1", 100); err == nil {
		t.Fatal("duplicate payout accepted")
	}
}

var _ = stores.ErrInvalid
