// Package app is svc-wallet's business logic: gifts, purchases, payouts.
package app

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/errs"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/metrics"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/outbox"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/shard"
	"github.com/oviethebaddestcoder/lumo-backend/svc-wallet/internal/ledger"
	"github.com/oviethebaddestcoder/lumo-backend/svc-wallet/internal/stores"
)

const producer = "svc-wallet"

var (
	ErrInsufficient   = errs.New(402, "insufficient_coins", "Not enough coins. Recharge to send this gift.")
	ErrGiftUnknown    = errs.BadRequest("gift_unknown", "That gift isn't available.")
	ErrSelfGift       = errs.BadRequest("self_gift", "You can't send gifts to yourself.")
	ErrIdemConflict   = errs.Conflict("idempotency_conflict", "This request ID was already used for a different gift.")
	ErrPurchaseClaimd = errs.Conflict("purchase_claimed", "This purchase was already used by another account.")
	ErrPurchaseBad    = errs.BadRequest("purchase_invalid", "We couldn't verify this purchase.")

	giftCoins = metrics.NewCounterVec("wallet_gift_coins_total", "Coins spent on gifts.", "gift")
)

type Gift struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Coins int64  `json:"coins"`
}

type Pack struct {
	ID              string  `json:"id"`
	Coins           int64   `json:"coins"`
	GoogleProductID *string `json:"google_product_id,omitempty"`
	AppleProductID  *string `json:"apple_product_id,omitempty"`
	WebAmountKobo   *int64  `json:"web_amount_kobo,omitempty"`
}

type Service struct {
	Shards *shard.Router
	// DiamondPercent: diamonds a host receives per 100 coins of gifts.
	DiamondPercent int64
	MinPayout      int64
	// KoboPerDiamond converts diamonds to naira for bank cash-outs.
	KoboPerDiamond int64
	AccountSecret  []byte // for Google obfuscatedAccountId tags

	Google   *stores.GooglePlay
	Apple    *stores.AppStore
	Paystack *stores.Paystack

	catMu     sync.RWMutex
	gifts     map[string]Gift
	giftList  []Gift
	packs     []Pack
	catLoaded time.Time
}

func (s *Service) userShard(userID string) *shard.Range { return s.Shards.For(userID) }

// ---- catalog ----

func (s *Service) Catalog(ctx context.Context) ([]Gift, []Pack, error) {
	if err := s.loadCatalog(ctx); err != nil {
		return nil, nil, err
	}
	s.catMu.RLock()
	defer s.catMu.RUnlock()
	return s.giftList, s.packs, nil
}

func (s *Service) gift(ctx context.Context, id string) (Gift, bool, error) {
	if err := s.loadCatalog(ctx); err != nil {
		return Gift{}, false, err
	}
	s.catMu.RLock()
	defer s.catMu.RUnlock()
	g, ok := s.gifts[id]
	return g, ok, nil
}

func (s *Service) loadCatalog(ctx context.Context) error {
	s.catMu.RLock()
	fresh := time.Since(s.catLoaded) < time.Minute
	s.catMu.RUnlock()
	if fresh {
		return nil
	}
	pool := s.Shards.All()[0].Pool
	rows, err := pool.Query(ctx, `SELECT id, name, coins FROM gifts WHERE active ORDER BY sort`)
	if err != nil {
		return err
	}
	gl, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Gift, error) {
		var g Gift
		return g, r.Scan(&g.ID, &g.Name, &g.Coins)
	})
	if err != nil {
		return err
	}
	rows, err = pool.Query(ctx, `SELECT id, coins, google_product_id, apple_product_id, web_amount_kobo FROM coin_packs WHERE active ORDER BY sort`)
	if err != nil {
		return err
	}
	pl, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Pack, error) {
		var p Pack
		return p, r.Scan(&p.ID, &p.Coins, &p.GoogleProductID, &p.AppleProductID, &p.WebAmountKobo)
	})
	if err != nil {
		return err
	}
	gm := make(map[string]Gift, len(gl))
	for _, g := range gl {
		gm[g.ID] = g
	}
	s.catMu.Lock()
	s.gifts, s.giftList, s.packs, s.catLoaded = gm, gl, pl, time.Now()
	s.catMu.Unlock()
	return nil
}

// ---- balances ----

type Balance struct {
	Coins    int64 `json:"coins"`
	Diamonds int64 `json:"diamonds"`
}

func (s *Service) Balance(ctx context.Context, userID string) (Balance, error) {
	c, d, err := ledger.Balances(ctx, s.userShard(userID).Pool, userID)
	return Balance{c, d}, err
}

// ---- gifts ----

type SendGiftInput struct {
	RequestID  string `json:"-"` // Idempotency-Key header
	SenderID   string `json:"-"`
	ReceiverID string `json:"receiver_id"`
	GiftID     string `json:"gift_id"`
	Qty        int    `json:"qty"`
	RoomID     string `json:"room_id,omitempty"`
	VideoID    string `json:"video_id,omitempty"`
	// FromBackpack spends a free backpack gift instead of coins.
	FromBackpack bool `json:"from_backpack,omitempty"`
}

type GiftReceipt struct {
	GiftSendID string `json:"gift_send_id"`
	Coins      int64  `json:"coins_spent"`
	Balance    int64  `json:"balance"`
	Replayed   bool   `json:"replayed,omitempty"`
}

// SendGift debits the sender in ONE transaction on the sender's shard and
// emits wallet.gift.sent. The host's diamonds are credited asynchronously by
// CreditGifts (on the host's shard), usually within a second.
func (s *Service) SendGift(ctx context.Context, in SendGiftInput) (GiftReceipt, error) {
	if in.RequestID == "" || len(in.RequestID) > 64 {
		return GiftReceipt{}, errs.BadRequest("idempotency_key_required", "Send an Idempotency-Key header (max 64 chars).")
	}
	if in.ReceiverID == in.SenderID {
		return GiftReceipt{}, ErrSelfGift
	}
	if in.Qty < 1 || in.Qty > 9999 {
		return GiftReceipt{}, errs.BadRequest("qty_invalid", "Quantity must be 1 to 9999.")
	}
	if (in.RoomID == "") == (in.VideoID == "") {
		return GiftReceipt{}, errs.BadRequest("target_required", "Send to exactly one of room_id or video_id.")
	}
	g, ok, err := s.gift(ctx, in.GiftID)
	if err != nil {
		return GiftReceipt{}, err
	}
	if !ok {
		return GiftReceipt{}, ErrGiftUnknown
	}
	if in.FromBackpack {
		return s.sendFromBackpack(ctx, in, g)
	}
	cost := g.Coins * int64(in.Qty)
	diamonds := cost * s.DiamondPercent / 100
	sendID := events.NewID()
	idem := "gift:" + in.SenderID + ":" + in.RequestID
	pool := s.userShard(in.SenderID).Pool

	var rec GiftReceipt
	err = db.InTx(ctx, pool, func(tx pgx.Tx) error {
		// A retry of a request that already succeeded returns the original
		// receipt, even if the balance has since dropped.
		var seen bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM transactions WHERE idempotency_key = $1)`, idem).Scan(&seen); err != nil {
			return err
		}
		if seen {
			return s.replayGift(ctx, tx, idem, in, &rec)
		}
		acct, err := ledger.Account(ctx, tx, in.SenderID, ledger.KindCoins)
		if err != nil {
			return err
		}
		bal, err := ledger.LockBalance(ctx, tx, acct)
		if err != nil {
			return err
		}
		if bal < cost {
			return ErrInsufficient
		}
		err = ledger.Post(ctx, tx, sendID, "gift_debit", idem,
			map[string]any{"gift_id": g.ID, "qty": in.Qty, "receiver_id": in.ReceiverID},
			ledger.Leg{AccountID: acct, Amount: -cost, User: true},
			ledger.Leg{AccountID: ledger.PlatformGiftClearing, Amount: cost},
		)
		if errors.Is(err, ledger.ErrDuplicate) {
			return s.replayGift(ctx, tx, idem, in, &rec)
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO gift_sends (id, sender_id, receiver_id, room_id, video_id, gift_id, qty, coins, diamonds)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`, sendID, in.SenderID, in.ReceiverID, in.RoomID, in.VideoID, g.ID, in.Qty, cost, diamonds); err != nil {
			return err
		}
		if err := addCoinsSpent(ctx, tx, in.SenderID, cost); err != nil {
			return err
		}
		ev := events.MustNew(producer, events.GiftSent, in.ReceiverID, events.GiftSentData{
			GiftSendID: sendID, SenderID: in.SenderID, ReceiverID: in.ReceiverID, RoomID: in.RoomID, VideoID: in.VideoID,
			GiftID: g.ID, Qty: in.Qty, Coins: cost, Diamonds: diamonds, SentAt: time.Now().UTC(),
		})
		if err := outbox.Insert(ctx, tx, events.TopicWalletGifts, ev); err != nil {
			return err
		}
		rec = GiftReceipt{GiftSendID: sendID, Coins: cost, Balance: bal - cost}
		return nil
	})
	if err == nil && !rec.Replayed {
		giftCoins.WithLabelValues(g.ID).Add(float64(cost))
	}
	return rec, err
}

// replayGift answers a retried request with the original receipt.
func (s *Service) replayGift(ctx context.Context, tx pgx.Tx, idem string, in SendGiftInput, rec *GiftReceipt) error {
	var id, giftID, receiver string
	var qty int
	var coins int64
	err := tx.QueryRow(ctx, `SELECT g.id::text, g.gift_id, g.receiver_id::text, g.qty, g.coins FROM transactions t
		JOIN gift_sends g ON g.id = t.id WHERE t.idempotency_key = $1`, idem).Scan(&id, &giftID, &receiver, &qty, &coins)
	if err != nil {
		return err
	}
	if giftID != in.GiftID || receiver != in.ReceiverID || qty != in.Qty {
		return ErrIdemConflict
	}
	c, _, err := ledger.Balances(ctx, tx, in.SenderID)
	*rec = GiftReceipt{GiftSendID: id, Coins: coins, Balance: c, Replayed: true}
	return err
}

// CreditGifts is the batch consumer of wallet.gifts. Events arrive ordered
// per receiver (topic key). It dedupes on gift_send_id and applies ONE
// balance update per receiver per batch, so a host receiving thousands of
// gifts a second costs one row update per batch, not thousands.
func (s *Service) CreditGifts(ctx context.Context, envs []events.Envelope) error {
	byShard := map[*shard.Range][]events.GiftSentData{}
	for _, e := range envs {
		if e.Type != events.GiftSent {
			continue
		}
		var g events.GiftSentData
		if err := e.Decode(&g); err != nil {
			slog.ErrorContext(ctx, "bad gift event skipped", "event_id", e.ID, "err", err)
			continue
		}
		if g.Diamonds <= 0 {
			continue
		}
		r := s.userShard(g.ReceiverID)
		byShard[r] = append(byShard[r], g)
	}
	for r, gs := range byShard {
		if err := s.creditShard(ctx, r, gs); err != nil {
			return fmt.Errorf("credit shard %d: %w", r.Index, err)
		}
	}
	return nil
}

func (s *Service) creditShard(ctx context.Context, r *shard.Range, gs []events.GiftSentData) error {
	ids := make([]string, len(gs))
	recv := make([]string, len(gs))
	dia := make([]int64, len(gs))
	for i, g := range gs {
		ids[i], recv[i], dia[i] = g.GiftSendID, g.ReceiverID, g.Diamonds
	}
	return db.InTx(ctx, r.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `INSERT INTO gift_credits (gift_send_id, receiver_id, diamonds)
			SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::bigint[])
			ON CONFLICT (gift_send_id) DO NOTHING
			RETURNING receiver_id::text, diamonds`, ids, recv, dia)
		if err != nil {
			return err
		}
		totals := map[string]int64{}
		counts := map[string]int{}
		for rows.Next() {
			var rid string
			var d int64
			if err := rows.Scan(&rid, &d); err != nil {
				rows.Close()
				return err
			}
			totals[rid] += d
			counts[rid]++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		receivers := make([]string, 0, len(totals))
		for rid := range totals {
			receivers = append(receivers, rid)
		}
		sort.Strings(receivers) // lock order: no deadlocks between batches
		for _, rid := range receivers {
			acct, err := ledger.Account(ctx, tx, rid, ledger.KindDiamonds)
			if err != nil {
				return err
			}
			txID := events.NewID()
			if err := ledger.Post(ctx, tx, txID, "gift_credit", "gcredit:"+txID, map[string]any{"gifts": counts[rid]},
				ledger.Leg{AccountID: acct, Amount: totals[rid], User: true},
				ledger.Leg{AccountID: ledger.PlatformDiamondIssuance, Amount: -totals[rid]},
			); err != nil {
				return err
			}
		}
		return nil
	})
}

// ---- purchases ----

// AccountTag is the value the Android app passes as obfuscatedAccountId.
func (s *Service) AccountTag(userID string) string {
	m := hmac.New(sha256.New, s.AccountSecret)
	m.Write([]byte(userID))
	return hex.EncodeToString(m.Sum(nil))[:64]
}

type PurchaseResult struct {
	PurchaseID string `json:"purchase_id"`
	Coins      int64  `json:"coins_added"`
	Balance    int64  `json:"balance"`
	Duplicate  bool   `json:"duplicate,omitempty"`
}

func (s *Service) VerifyGoogle(ctx context.Context, userID, productID, token string) (PurchaseResult, error) {
	if s.Google == nil {
		return PurchaseResult{}, errs.Unavailable("store_unavailable", "Google Play purchases aren't enabled.")
	}
	v, err := s.Google.Verify(ctx, productID, token, s.AccountTag(userID))
	if err != nil {
		return PurchaseResult{}, storeErr(err)
	}
	pack, err := s.packBy(ctx, "google", productID)
	if err != nil {
		return PurchaseResult{}, err
	}
	res, err := s.credit(ctx, userID, v, pack)
	if err != nil {
		return res, err
	}
	// Consume now; if it fails, the worker retries from the CoinsPurchased event.
	if err := s.Google.Consume(ctx, productID, token); err != nil {
		slog.WarnContext(ctx, "google consume failed; worker will retry", "err", err)
	}
	return res, nil
}

func (s *Service) VerifyApple(ctx context.Context, userID, txID string) (PurchaseResult, error) {
	if s.Apple == nil {
		return PurchaseResult{}, errs.Unavailable("store_unavailable", "App Store purchases aren't enabled.")
	}
	v, err := s.Apple.Verify(ctx, txID, userID)
	if err != nil {
		return PurchaseResult{}, storeErr(err)
	}
	pack, err := s.packBy(ctx, "apple", v.ProductID)
	if err != nil {
		return PurchaseResult{}, err
	}
	return s.credit(ctx, userID, v, pack)
}

func storeErr(err error) error {
	switch {
	case errors.Is(err, stores.ErrNotOwner):
		return ErrPurchaseClaimd.Wrap(err)
	case errors.Is(err, stores.ErrInvalid):
		return ErrPurchaseBad.Wrap(err)
	default:
		return errs.Unavailable("store_unavailable", "The store isn't responding. Your purchase is safe; try again.").Wrap(err)
	}
}

func (s *Service) packBy(ctx context.Context, channel, productID string) (Pack, error) {
	_, packs, err := s.Catalog(ctx)
	if err != nil {
		return Pack{}, err
	}
	for _, p := range packs {
		if (channel == "google" && p.GoogleProductID != nil && *p.GoogleProductID == productID) ||
			(channel == "apple" && p.AppleProductID != nil && *p.AppleProductID == productID) ||
			(channel == "web" && p.ID == productID) {
			return p, nil
		}
	}
	return Pack{}, ErrPurchaseBad.Wrap(fmt.Errorf("unknown product %s/%s", channel, productID))
}

// credit claims the receipt globally, then credits coins on the user's shard.
// Both steps are idempotent, so a crash between them is fixed by a retry.
func (s *Service) credit(ctx context.Context, userID string, v stores.Verified, pack Pack) (PurchaseResult, error) {
	claimKey := v.Channel + ":" + v.StoreRef
	cr := s.Shards.For(claimKey)
	var owner string
	err := cr.Pool.QueryRow(ctx, `INSERT INTO purchase_claims (claim_key, user_id) VALUES ($1, $2)
		ON CONFLICT (claim_key) DO UPDATE SET claim_key = EXCLUDED.claim_key RETURNING user_id::text`, claimKey, userID).Scan(&owner)
	if err != nil {
		return PurchaseResult{}, err
	}
	if owner != userID {
		return PurchaseResult{}, ErrPurchaseClaimd
	}

	pool := s.userShard(userID).Pool
	pid := events.NewID()
	var res PurchaseResult
	err = db.InTx(ctx, pool, func(tx pgx.Tx) error {
		acct, err := ledger.Account(ctx, tx, userID, ledger.KindCoins)
		if err != nil {
			return err
		}
		err = ledger.Post(ctx, tx, pid, "purchase", "purchase:"+claimKey, map[string]any{"pack": pack.ID, "channel": v.Channel},
			ledger.Leg{AccountID: acct, Amount: pack.Coins, User: true},
			ledger.Leg{AccountID: ledger.PlatformCoinIssuance, Amount: -pack.Coins},
		)
		if errors.Is(err, ledger.ErrDuplicate) {
			if err := tx.QueryRow(ctx, `SELECT id::text, coins FROM purchases WHERE channel = $1 AND store_ref = $2`,
				v.Channel, v.StoreRef).Scan(&res.PurchaseID, &res.Coins); err != nil {
				return err
			}
			res.Duplicate = true
			res.Balance, _, err = ledger.Balances(ctx, tx, userID)
			return err
		}
		if err != nil {
			return err
		}
		raw := v.Raw
		if len(raw) == 0 {
			raw = json.RawMessage(`{}`)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO purchases (id, user_id, channel, product_id, store_ref, coins, raw)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`, pid, userID, v.Channel, v.ProductID, v.StoreRef, pack.Coins, raw); err != nil {
			return err
		}
		if err := outbox.Insert(ctx, tx, events.TopicWalletEvents, events.MustNew(producer, events.CoinsPurchased, userID,
			events.CoinsPurchasedData{PurchaseID: pid, UserID: userID, Channel: v.Channel, ProductID: v.ProductID, StoreRef: v.StoreRef, Coins: pack.Coins})); err != nil {
			return err
		}
		res = PurchaseResult{PurchaseID: pid, Coins: pack.Coins}
		res.Balance, _, err = ledger.Balances(ctx, tx, userID)
		return err
	})
	return res, err
}

// Reverse takes back coins for a refunded or charged-back purchase. The
// balance may go negative; spending is blocked until it's positive again.
func (s *Service) Reverse(ctx context.Context, channel, storeRef, reason string) error {
	claimKey := channel + ":" + storeRef
	var userID string
	err := s.Shards.For(claimKey).Pool.QueryRow(ctx, `SELECT user_id::text FROM purchase_claims WHERE claim_key = $1`, claimKey).Scan(&userID)
	if db.IsNoRows(err) {
		return nil // never credited
	}
	if err != nil {
		return err
	}
	return db.InTx(ctx, s.userShard(userID).Pool, func(tx pgx.Tx) error {
		var pid string
		var coins int64
		err := tx.QueryRow(ctx, `UPDATE purchases SET status = 'reversed' WHERE channel = $1 AND store_ref = $2 AND status = 'credited'
			RETURNING id::text, coins`, channel, storeRef).Scan(&pid, &coins)
		if db.IsNoRows(err) {
			return nil // already reversed
		}
		if err != nil {
			return err
		}
		acct, err := ledger.Account(ctx, tx, userID, ledger.KindCoins)
		if err != nil {
			return err
		}
		if err := ledger.Post(ctx, tx, events.NewID(), "reversal", "reversal:"+claimKey, map[string]any{"reason": reason},
			ledger.Leg{AccountID: acct, Amount: -coins, User: true},
			ledger.Leg{AccountID: ledger.PlatformRefunds, Amount: coins},
		); err != nil && !errors.Is(err, ledger.ErrDuplicate) {
			return err
		}
		slog.WarnContext(ctx, "purchase reversed", "user_id", userID, "channel", channel, "coins", coins, "reason", reason)
		return outbox.Insert(ctx, tx, events.TopicWalletEvents, events.MustNew(producer, events.CoinsReversed, userID,
			events.CoinsReversedData{PurchaseID: pid, UserID: userID, Coins: coins, Reason: reason}))
	})
}

// ---- web wallet (Paystack) ----

func (s *Service) WebCheckout(ctx context.Context, userID, email, packID, callbackURL string) (string, string, error) {
	if s.Paystack == nil {
		return "", "", errs.Unavailable("web_wallet_disabled", "Web top-up isn't available right now.")
	}
	pack, err := s.packBy(ctx, "web", packID)
	if err != nil {
		return "", "", err
	}
	if pack.WebAmountKobo == nil || *pack.WebAmountKobo <= 0 {
		return "", "", ErrPurchaseBad.Wrap(fmt.Errorf("pack %s has no web price", packID))
	}
	ref := "lumo_" + events.NewID()
	u, err := s.Paystack.Initialize(ctx, email, ref, *pack.WebAmountKobo, map[string]string{"user_id": userID, "pack_id": pack.ID}, callbackURL)
	return u, ref, err
}

// PaystackCharge credits a successful Paystack charge, re-verified via API.
func (s *Service) PaystackCharge(ctx context.Context, reference string) error {
	t, raw, err := s.Paystack.VerifyTx(ctx, reference)
	if err != nil {
		return err
	}
	if t.Status != "success" || t.Currency != "NGN" {
		return nil
	}
	userID, _ := t.Metadata["user_id"].(string)
	packID, _ := t.Metadata["pack_id"].(string)
	pack, err := s.packBy(ctx, "web", packID)
	if err != nil || userID == "" {
		return fmt.Errorf("paystack %s: bad metadata", reference)
	}
	if pack.WebAmountKobo == nil || t.Amount < *pack.WebAmountKobo {
		return fmt.Errorf("paystack %s: amount %d below pack price", reference, t.Amount)
	}
	_, err = s.credit(ctx, userID, stores.Verified{Channel: "paystack", ProductID: pack.ID, StoreRef: reference, Raw: raw}, pack)
	return err
}

// ---- payouts ----

type PayoutResult struct {
	PayoutID string `json:"payout_id"`
	Status   string `json:"status"`
	Balance  int64  `json:"diamonds_left"`
}

// RequestPayout moves diamonds to payout clearing; finance approves and
// pays out from the admin console (bank transfer via the payment provider).
func (s *Service) RequestPayout(ctx context.Context, userID, requestID string, diamonds int64) (PayoutResult, error) {
	if requestID == "" {
		return PayoutResult{}, errs.BadRequest("idempotency_key_required", "Send an Idempotency-Key header.")
	}
	if diamonds < s.MinPayout {
		return PayoutResult{}, errs.BadRequest("payout_below_minimum", fmt.Sprintf("Minimum cash-out is %d diamonds.", s.MinPayout))
	}
	var res PayoutResult
	err := db.InTx(ctx, s.userShard(userID).Pool, func(tx pgx.Tx) error {
		acct, err := ledger.Account(ctx, tx, userID, ledger.KindDiamonds)
		if err != nil {
			return err
		}
		bal, err := ledger.LockBalance(ctx, tx, acct)
		if err != nil {
			return err
		}
		if bal < diamonds {
			return errs.New(402, "insufficient_diamonds", "You don't have that many diamonds.")
		}
		pid := events.NewID()
		err = ledger.Post(ctx, tx, pid, "payout", "payout:"+userID+":"+requestID, nil,
			ledger.Leg{AccountID: acct, Amount: -diamonds, User: true},
			ledger.Leg{AccountID: ledger.PlatformPayoutClearing, Amount: diamonds})
		if errors.Is(err, ledger.ErrDuplicate) {
			return errs.Conflict("duplicate_request", "This cash-out was already requested.")
		}
		if err != nil {
			return err
		}
		var recipient, label *string
		if s.Paystack != nil {
			var rc, l string
			var changed *time.Time
			err := tx.QueryRow(ctx, `SELECT recipient_code, bank_name || ' ••' || account_last4, changed_at
				FROM payout_accounts WHERE user_id = $1`, userID).Scan(&rc, &l, &changed)
			if db.IsNoRows(err) {
				return ErrNoPayoutAccount
			}
			if err != nil {
				return err
			}
			if changed != nil && time.Since(*changed) < PayoutAccountCooldown {
				return ErrPayoutCooldown
			}
			recipient, label = &rc, &l
		}
		if _, err := tx.Exec(ctx, `INSERT INTO payouts (id, user_id, diamonds, amount_kobo, recipient_code, bank_label)
			VALUES ($1, $2, $3, $4, $5, COALESCE($6, ''))`, pid, userID, diamonds, diamonds*s.KoboPerDiamond, recipient, label); err != nil {
			return err
		}
		res = PayoutResult{PayoutID: pid, Status: "pending_review", Balance: bal - diamonds}
		return outbox.Insert(ctx, tx, events.TopicWalletEvents, events.MustNew(producer, events.PayoutRequested, userID,
			events.PayoutRequestedData{PayoutID: pid, UserID: userID, Diamonds: diamonds}))
	})
	return res, err
}

// ---- history ----

type Entry struct {
	ID        int64           `json:"id"`
	Kind      string          `json:"kind"`
	Currency  string          `json:"currency"`
	Amount    int64           `json:"amount"`
	Meta      json.RawMessage `json:"meta"`
	CreatedAt time.Time       `json:"created_at"`
}

func (s *Service) History(ctx context.Context, userID string, before int64, limit int) ([]Entry, error) {
	if limit <= 0 || limit > 100 {
		limit = 30
	}
	if before <= 0 {
		before = 1<<63 - 1
	}
	rows, err := s.userShard(userID).Pool.Query(ctx, `SELECT e.id, t.kind, a.kind, e.amount, t.metadata, e.created_at
		FROM accounts a JOIN entries e ON e.account_id = a.id JOIN transactions t ON t.id = e.tx_id
		WHERE a.owner_id = $1 AND a.kind IN ('coins', 'diamonds') AND e.id < $2
		ORDER BY e.id DESC LIMIT $3`, userID, before, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Entry, error) {
		var e Entry
		return e, r.Scan(&e.ID, &e.Kind, &e.Currency, &e.Amount, &e.Meta, &e.CreatedAt)
	})
}
