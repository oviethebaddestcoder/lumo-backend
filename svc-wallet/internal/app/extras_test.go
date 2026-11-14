package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/svc-wallet/internal/app"
	"github.com/oviethebaddestcoder/lumo-backend/svc-wallet/internal/ledger"
	"github.com/oviethebaddestcoder/lumo-backend/svc-wallet/internal/stores"
)

func giftEvents(t *testing.T, p *pgxpool.Pool) []events.Envelope {
	t.Helper()
	rows, err := p.Query(context.Background(), `SELECT payload FROM outbox WHERE topic = 'wallet.gifts' ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []events.Envelope
	for rows.Next() {
		var e events.Envelope
		if err := rows.Scan(&e); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}

func TestLevels(t *testing.T) {
	for _, c := range []struct {
		spent int64
		want  int
	}{{0, 1}, {49, 1}, {50, 2}, {199, 2}, {200, 3}, {4050, 10}, {1 << 40, 99}} {
		if got := app.Level(c.spent); got != c.want {
			t.Errorf("Level(%d) = %d, want %d", c.spent, got, c.want)
		}
	}
	if app.LevelStart(10) != 4050 {
		t.Fatal("LevelStart(10)")
	}
}

func TestStatsLevelsAndWeeklyDiamonds(t *testing.T) {
	s, pools := setup(t)
	ctx := context.Background()
	sender, host := userOn(s, 0), userOn(s, 1)
	fund(t, s, sender, 1) // 70 coins
	for i := 0; i < 6; i++ {
		if _, err := s.SendGift(ctx, app.SendGiftInput{RequestID: fmt.Sprint(i), SenderID: sender, ReceiverID: host, GiftID: "rose", Qty: 1, RoomID: "r"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.CreditGifts(ctx, giftEvents(t, pools[0])); err != nil {
		t.Fatal(err)
	}
	st, err := s.Stats(ctx, sender)
	if err != nil || st.CoinsSpent != 60 || st.Level != 2 || st.NextLevelAt != 200 {
		t.Fatalf("sender stats %+v %v", st, err)
	}
	days, err := s.Earnings(ctx, host, 7)
	if err != nil || len(days) != 7 || days[6].Diamonds != 30 || days[0].Diamonds != 0 {
		t.Fatalf("earnings %+v %v", days, err)
	}
	hs, _ := s.Stats(ctx, host)
	if hs.DiamondsWeek != 30 || hs.DiamondsEarned != 30 || hs.Level != 1 {
		t.Fatalf("host stats %+v", hs)
	}
	lv, err := s.Levels(ctx, []string{sender, host, "junk"})
	if err != nil || len(lv) != 2 {
		t.Fatalf("levels %+v %v", lv, err)
	}
	for _, l := range lv {
		if (l.UserID == sender && l.Level != 2) || (l.UserID == host && l.Level != 1) {
			t.Fatalf("levels %+v", lv)
		}
	}
}

func TestCheckinAndBackpackGifts(t *testing.T) {
	s, pools := setup(t)
	ctx := context.Background()
	fan, host := userOn(s, 0), userOn(s, 1)

	r1, err := s.Checkin(ctx, fan)
	if err != nil || r1.Already || r1.Streak != 1 || r1.Reward != app.CheckinRewards[0] {
		t.Fatalf("checkin %+v %v", r1, err)
	}
	if r2, _ := s.Checkin(ctx, fan); !r2.Already || r2.Streak != 1 {
		t.Fatalf("second checkin same day granted again: %+v", r2)
	}
	bp, err := s.Backpack(ctx, fan)
	if err != nil || !bp.CheckedInToday || len(bp.Items) != 1 || bp.Items[0].ID != "heart" || bp.Items[0].Qty != 5 {
		t.Fatalf("backpack %+v %v", bp, err)
	}

	// Concurrent sends of 2 hearts each from 5 hearts: exactly 2 succeed.
	var ok, empty atomic.Int64
	done := make(chan struct{})
	for i := 0; i < 6; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			_, err := s.SendGift(ctx, app.SendGiftInput{RequestID: events.NewID(), SenderID: fan, ReceiverID: host, GiftID: "heart", Qty: 2, RoomID: "r", FromBackpack: true})
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, app.ErrBackpackEmpty):
				empty.Add(1)
			default:
				t.Error(err)
			}
		}()
	}
	for i := 0; i < 6; i++ {
		<-done
	}
	if ok.Load() != 2 || empty.Load() != 4 {
		t.Fatalf("ok=%d empty=%d", ok.Load(), empty.Load())
	}
	// Idempotent retry.
	in := app.SendGiftInput{RequestID: "bp-1", SenderID: fan, ReceiverID: host, GiftID: "heart", Qty: 1, RoomID: "r", FromBackpack: true}
	a, err := s.SendGift(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.SendGift(ctx, in)
	if err != nil || !b.Replayed || b.GiftSendID != a.GiftSendID {
		t.Fatalf("replay %+v %v", b, err)
	}
	// Free gifts: events carry value for PK/goals but no diamonds; nothing is paid.
	evs := giftEvents(t, pools[0])
	for _, e := range evs {
		var g events.GiftSentData
		e.Decode(&g)
		if !g.FromBackpack || g.Diamonds != 0 || g.Coins == 0 {
			t.Fatalf("bad backpack event %+v", g)
		}
	}
	if err := s.CreditGifts(ctx, evs); err != nil {
		t.Fatal(err)
	}
	if hb, _ := s.Balance(ctx, host); hb.Diamonds != 0 {
		t.Fatalf("backpack gifts paid %d diamonds", hb.Diamonds)
	}
	if fs, _ := s.Stats(ctx, fan); fs.CoinsSpent != 0 {
		t.Fatalf("backpack gifts counted toward level: %+v", fs)
	}
}

// fakePaystack answers the transfer endpoints and records transfers.
func fakePaystack(t *testing.T, transfers *atomic.Int64) *stores.Paystack {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/bank/resolve"):
			if r.URL.Query().Get("account_number") == "0000000000" {
				w.WriteHeader(422)
				w.Write([]byte(`{"status":false,"message":"Could not resolve account name"}`))
				return
			}
			w.Write([]byte(`{"status":true,"data":{"account_name":"ADA OBI"}}`))
		case r.URL.Path == "/bank":
			w.Write([]byte(`{"status":true,"data":[{"code":"058","name":"GTBank","active":true},{"code":"999","name":"Old","active":false}]}`))
		case r.URL.Path == "/transferrecipient":
			w.Write([]byte(`{"status":true,"data":{"recipient_code":"RCP_test"}}`))
		case r.URL.Path == "/transfer":
			var in map[string]any
			json.NewDecoder(r.Body).Decode(&in)
			if in["recipient"] != "RCP_test" || in["reference"] == "" {
				w.WriteHeader(400)
				return
			}
			transfers.Add(1)
			w.Write([]byte(`{"status":true,"data":{"status":"pending"}}`))
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(srv.Close)
	return &stores.Paystack{SecretKey: "sk_test", BaseURL: srv.URL}
}

func TestBankCashoutFlow(t *testing.T) {
	s, pools := setup(t)
	ctx := context.Background()
	var transfers atomic.Int64
	s.Paystack = fakePaystack(t, &transfers)
	s.KoboPerDiamond = 50
	sender, host := userOn(s, 0), userOn(s, 1)
	fund(t, s, sender, 6) // 420 coins
	for i := 0; i < 40; i++ {
		s.SendGift(ctx, app.SendGiftInput{RequestID: fmt.Sprint(i), SenderID: sender, ReceiverID: host, GiftID: "rose", Qty: 1, RoomID: "r"})
	}
	if err := s.CreditGifts(ctx, giftEvents(t, pools[0])); err != nil {
		t.Fatal(err)
	}
	// 40 roses · 10 coins · 50% = 200 diamonds.

	if _, err := s.RequestPayout(ctx, host, "p0", 100); !errors.Is(err, app.ErrNoPayoutAccount) {
		t.Fatalf("payout without bank account: %v", err)
	}
	if _, err := s.SetPayoutAccount(ctx, host, "058", "0000000000"); !errors.Is(err, app.ErrBankUnverified) {
		t.Fatalf("unresolvable account accepted: %v", err)
	}
	if _, err := s.SetPayoutAccount(ctx, host, "999", "0123456789"); err == nil {
		t.Fatal("inactive bank accepted")
	}
	pa, err := s.SetPayoutAccount(ctx, host, "058", "0123456789")
	if err != nil || pa.AccountName != "ADA OBI" || pa.AccountLast4 != "6789" || pa.BankName != "GTBank" {
		t.Fatalf("account %+v %v", pa, err)
	}

	// Payout 1: approved → transfer → webhook success (twice) → paid once.
	p1, err := s.RequestPayout(ctx, host, "p1", 100)
	if err != nil || p1.Balance != 100 {
		t.Fatalf("payout 1 %+v %v", p1, err)
	}
	pending, _ := s.AdminPayouts(ctx, "pending_review", 0)
	if len(pending) != 1 || pending[0].AmountKobo != 5000 || pending[0].BankLabel != "GTBank ••6789" {
		t.Fatalf("pending %+v", pending)
	}
	reviewer := events.NewID()
	got, err := s.ApprovePayout(ctx, p1.PayoutID, reviewer)
	if err != nil || got.Status != "processing" || transfers.Load() != 1 {
		t.Fatalf("approve %+v %v transfers=%d", got, err, transfers.Load())
	}
	if _, err := s.ApprovePayout(ctx, p1.PayoutID, reviewer); err == nil {
		t.Fatal("approved twice")
	}
	for i := 0; i < 2; i++ {
		if err := s.PaystackTransfer(ctx, "transfer.success", p1.PayoutID); err != nil {
			t.Fatal(err)
		}
	}
	// A failure arriving after success must not refund.
	if err := s.PaystackTransfer(ctx, "transfer.failed", p1.PayoutID); err != nil {
		t.Fatal(err)
	}
	mine, _ := s.MyPayouts(ctx, host)
	if len(mine) != 1 || mine[0].Status != "paid" {
		t.Fatalf("my payouts %+v", mine)
	}
	if b, _ := s.Balance(ctx, host); b.Diamonds != 100 {
		t.Fatalf("diamonds after paid payout = %d, want 100", b.Diamonds)
	}

	// Payout 2: rejected → diamonds come back.
	p2, err := s.RequestPayout(ctx, host, "p2", 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RejectPayout(ctx, p2.PayoutID, reviewer, "Suspicious gifting pattern"); err != nil {
		t.Fatal(err)
	}
	if b, _ := s.Balance(ctx, host); b.Diamonds != 100 {
		t.Fatalf("diamonds after reject = %d, want 100", b.Diamonds)
	}

	// Payout 3: transfer fails at the bank → diamonds come back.
	p3, _ := s.RequestPayout(ctx, host, "p3", 100)
	s.ApprovePayout(ctx, p3.PayoutID, reviewer)
	if err := s.PaystackTransfer(ctx, "transfer.reversed", p3.PayoutID); err != nil {
		t.Fatal(err)
	}
	if b, _ := s.Balance(ctx, host); b.Diamonds != 100 {
		t.Fatalf("diamonds after failed transfer = %d, want 100", b.Diamonds)
	}
	// Unknown references are ignored (Paystack sends every transfer on the account).
	if err := s.PaystackTransfer(ctx, "transfer.success", "not-ours"); err != nil {
		t.Fatal(err)
	}
	for i, p := range pools {
		u, bad, err := ledger.Reconcile(ctx, p)
		if err != nil || u != 0 || bad != 0 {
			t.Fatalf("shard %d reconcile: unbalanced=%d mismatched=%d err=%v", i, u, bad, err)
		}
	}
}
