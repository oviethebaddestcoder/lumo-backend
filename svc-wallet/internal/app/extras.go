package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/errs"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/outbox"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/shard"
	"github.com/oviethebaddestcoder/lumo-backend/svc-wallet/internal/ledger"
	"github.com/oviethebaddestcoder/lumo-backend/svc-wallet/internal/stores"
)

var (
	ErrBackpackEmpty   = errs.New(402, "backpack_empty", "You don't have enough of that gift in your backpack.")
	ErrNoPayoutAccount = errs.New(409, "payout_account_required", "Add a bank account before cashing out.")
	ErrBankUnverified  = errs.BadRequest("bank_account_invalid", "We couldn't verify that account number with the bank.")
)

// ---- levels ----

// Level is a sender's level, from lifetime coins spent on gifts:
// level n starts at 50·(n-1)² coins (2 at 50, 10 at 4,050, 50 at 120,050), capped at 99.
func Level(coinsSpent int64) int {
	if coinsSpent <= 0 {
		return 1
	}
	l := 1 + int(math.Sqrt(float64(coinsSpent)/50))
	return min(l, 99)
}

// LevelStart is the coins spent needed to reach level l.
func LevelStart(l int) int64 { return 50 * int64(l-1) * int64(l-1) }

func addCoinsSpent(ctx context.Context, tx pgx.Tx, userID string, coins int64) error {
	_, err := tx.Exec(ctx, `INSERT INTO user_stats (user_id, coins_spent) VALUES ($1, $2)
		ON CONFLICT (user_id) DO UPDATE SET coins_spent = user_stats.coins_spent + $2, updated_at = now()`, userID, coins)
	return err
}

type UserLevel struct {
	UserID string `json:"user_id"`
	Level  int    `json:"level"`
}

// Levels returns levels for up to 100 users (public: shown on profiles and in chat).
func (s *Service) Levels(ctx context.Context, ids []string) ([]UserLevel, error) {
	byShard := map[*shard.Range][]string{}
	n := 0
	for _, id := range ids {
		if len(id) != 36 {
			continue
		}
		if n++; n > 100 {
			return nil, errs.BadRequest("too_many_ids", "At most 100 IDs per request.")
		}
		r := s.userShard(id)
		byShard[r] = append(byShard[r], id)
	}
	spent := map[string]int64{}
	for r, list := range byShard {
		rows, err := r.Pool.Query(ctx, `SELECT user_id::text, coins_spent FROM user_stats WHERE user_id = ANY($1::uuid[])`, list)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			var c int64
			if err := rows.Scan(&id, &c); err != nil {
				rows.Close()
				return nil, err
			}
			spent[id] = c
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	out := make([]UserLevel, 0, n)
	for _, list := range byShard {
		for _, id := range list {
			out = append(out, UserLevel{UserID: id, Level: Level(spent[id])})
		}
	}
	return out, nil
}

type Stats struct {
	Level          int   `json:"level"`
	CoinsSpent     int64 `json:"coins_spent"`
	LevelStart     int64 `json:"level_start_coins"`
	NextLevelAt    int64 `json:"next_level_coins"`
	DiamondsWeek   int64 `json:"diamonds_week"`
	DiamondsEarned int64 `json:"diamonds_earned_total"`
}

// Stats is the signed-in user's own numbers for the profile and wallet.
func (s *Service) Stats(ctx context.Context, userID string) (Stats, error) {
	var st Stats
	pool := s.userShard(userID).Pool
	err := pool.QueryRow(ctx, `SELECT
			COALESCE((SELECT coins_spent FROM user_stats WHERE user_id = $1), 0),
			COALESCE((SELECT sum(diamonds) FROM gift_credits WHERE receiver_id = $1 AND credited_at > now() - interval '7 days'), 0),
			COALESCE((SELECT sum(diamonds) FROM gift_credits WHERE receiver_id = $1), 0)`, userID).
		Scan(&st.CoinsSpent, &st.DiamondsWeek, &st.DiamondsEarned)
	if err != nil {
		return st, err
	}
	st.Level = Level(st.CoinsSpent)
	st.LevelStart = LevelStart(st.Level)
	st.NextLevelAt = LevelStart(st.Level + 1)
	return st, nil
}

// ---- backpack & daily check-in ----

type Reward struct {
	GiftID string `json:"gift_id"`
	Qty    int    `json:"qty"`
}

// CheckinRewards cycles weekly. Only cheap gifts: these are free.
var CheckinRewards = []Reward{
	{"heart", 5}, {"rose", 1}, {"heart", 10}, {"rose", 2}, {"heart", 15}, {"rose", 3}, {"star", 1},
}

type BackpackItem struct {
	Gift
	Qty int `json:"qty"`
}

type Backpack struct {
	Items          []BackpackItem `json:"items"`
	CheckedInToday bool           `json:"checked_in_today"`
	Streak         int            `json:"streak"`
	NextReward     Reward         `json:"next_reward"`
}

func utcDay(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func (s *Service) Backpack(ctx context.Context, userID string) (Backpack, error) {
	pool := s.userShard(userID).Pool
	bp := Backpack{Items: []BackpackItem{}}
	rows, err := pool.Query(ctx, `SELECT gift_id, qty FROM backpack WHERE user_id = $1 AND qty > 0 ORDER BY gift_id`, userID)
	if err != nil {
		return bp, err
	}
	type row struct {
		id  string
		qty int
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (row, error) {
		var x row
		return x, r.Scan(&x.id, &x.qty)
	})
	if err != nil {
		return bp, err
	}
	for _, x := range list {
		g, ok, err := s.gift(ctx, x.id)
		if err != nil {
			return bp, err
		}
		if ok {
			bp.Items = append(bp.Items, BackpackItem{Gift: g, Qty: x.qty})
		}
	}
	today := utcDay(time.Now())
	var last time.Time
	var streak int
	err = pool.QueryRow(ctx, `SELECT day, streak FROM checkins WHERE user_id = $1 ORDER BY day DESC LIMIT 1`, userID).Scan(&last, &streak)
	switch {
	case db.IsNoRows(err):
	case err != nil:
		return bp, err
	case last.Equal(today):
		bp.CheckedInToday, bp.Streak = true, streak
	case last.Equal(today.AddDate(0, 0, -1)):
		bp.Streak = streak
	}
	bp.NextReward = CheckinRewards[bp.Streak%len(CheckinRewards)]
	if bp.CheckedInToday {
		bp.NextReward = CheckinRewards[streak%len(CheckinRewards)]
	}
	return bp, nil
}

type CheckinResult struct {
	Already bool   `json:"already_checked_in"`
	Streak  int    `json:"streak"`
	Reward  Reward `json:"reward"`
}

// Checkin grants today's reward once per UTC day; consecutive days build a streak.
func (s *Service) Checkin(ctx context.Context, userID string) (CheckinResult, error) {
	var res CheckinResult
	today := utcDay(time.Now())
	err := db.InTx(ctx, s.userShard(userID).Pool, func(tx pgx.Tx) error {
		var prev int
		err := tx.QueryRow(ctx, `SELECT streak FROM checkins WHERE user_id = $1 AND day = $2`, userID, today.AddDate(0, 0, -1)).Scan(&prev)
		if err != nil && !db.IsNoRows(err) {
			return err
		}
		streak := prev + 1
		tag, err := tx.Exec(ctx, `INSERT INTO checkins (user_id, day, streak) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, userID, today, streak)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			res.Already = true
			return tx.QueryRow(ctx, `SELECT streak FROM checkins WHERE user_id = $1 AND day = $2`, userID, today).Scan(&res.Streak)
		}
		r := CheckinRewards[(streak-1)%len(CheckinRewards)]
		if _, err := tx.Exec(ctx, `INSERT INTO backpack (user_id, gift_id, qty) VALUES ($1, $2, $3)
			ON CONFLICT (user_id, gift_id) DO UPDATE SET qty = backpack.qty + $3, updated_at = now()`, userID, r.GiftID, r.Qty); err != nil {
			return err
		}
		res = CheckinResult{Streak: streak, Reward: r}
		return nil
	})
	return res, err
}

// sendFromBackpack spends free gifts. The event carries the gift's coin value
// (for PK scores and room goals) but zero diamonds, so nothing is paid out.
func (s *Service) sendFromBackpack(ctx context.Context, in SendGiftInput, g Gift) (GiftReceipt, error) {
	idem := "bp:" + in.SenderID + ":" + in.RequestID
	value := g.Coins * int64(in.Qty)
	var rec GiftReceipt
	err := db.InTx(ctx, s.userShard(in.SenderID).Pool, func(tx pgx.Tx) error {
		var prior string
		err := tx.QueryRow(ctx, `SELECT gift_send_id::text FROM backpack_sends WHERE idempotency_key = $1`, idem).Scan(&prior)
		if err == nil {
			var giftID, receiver string
			var qty int
			if err := tx.QueryRow(ctx, `SELECT gift_id, receiver_id::text, qty FROM gift_sends WHERE id = $1`, prior).Scan(&giftID, &receiver, &qty); err != nil {
				return err
			}
			if giftID != in.GiftID || receiver != in.ReceiverID || qty != in.Qty {
				return ErrIdemConflict
			}
			c, _, err := ledger.Balances(ctx, tx, in.SenderID)
			rec = GiftReceipt{GiftSendID: prior, Balance: c, Replayed: true}
			return err
		}
		if !db.IsNoRows(err) {
			return err
		}
		var have int
		err = tx.QueryRow(ctx, `SELECT qty FROM backpack WHERE user_id = $1 AND gift_id = $2 FOR UPDATE`, in.SenderID, g.ID).Scan(&have)
		if db.IsNoRows(err) || (err == nil && have < in.Qty) {
			return ErrBackpackEmpty
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE backpack SET qty = qty - $3, updated_at = now() WHERE user_id = $1 AND gift_id = $2`,
			in.SenderID, g.ID, in.Qty); err != nil {
			return err
		}
		sendID := events.NewID()
		if _, err := tx.Exec(ctx, `INSERT INTO gift_sends (id, sender_id, receiver_id, room_id, video_id, gift_id, qty, coins, diamonds, source)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, 'backpack')`, sendID, in.SenderID, in.ReceiverID, in.RoomID, in.VideoID, g.ID, in.Qty, value); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO backpack_sends (idempotency_key, gift_send_id) VALUES ($1, $2)`, idem, sendID); err != nil {
			return err
		}
		ev := events.MustNew(producer, events.GiftSent, in.ReceiverID, events.GiftSentData{
			GiftSendID: sendID, SenderID: in.SenderID, ReceiverID: in.ReceiverID, RoomID: in.RoomID, VideoID: in.VideoID,
			GiftID: g.ID, Qty: in.Qty, Coins: value, Diamonds: 0, SentAt: time.Now().UTC(), FromBackpack: true,
		})
		if err := outbox.Insert(ctx, tx, events.TopicWalletGifts, ev); err != nil {
			return err
		}
		c, _, err := ledger.Balances(ctx, tx, in.SenderID)
		rec = GiftReceipt{GiftSendID: sendID, Balance: c}
		return err
	})
	return rec, err
}

// ---- bank accounts & cash-outs (Paystack transfers) ----

var nuban = regexp.MustCompile(`^\d{10}$`)

type PayoutAccount struct {
	BankCode     string    `json:"bank_code"`
	BankName     string    `json:"bank_name"`
	AccountLast4 string    `json:"account_last4"`
	AccountName  string    `json:"account_name"`
	UpdatedAt    time.Time `json:"updated_at"`
}

var (
	banksMu  sync.Mutex
	banks    []stores.Bank
	banksAt  time.Time
	bankName = map[string]string{}
)

// Banks lists banks for the "add bank account" picker (cached 12 h).
func (s *Service) Banks(ctx context.Context) ([]stores.Bank, error) {
	if s.Paystack == nil {
		return nil, errs.Unavailable("payouts_disabled", "Bank cash-outs aren't available right now.")
	}
	banksMu.Lock()
	defer banksMu.Unlock()
	if banks != nil && time.Since(banksAt) < 12*time.Hour {
		return banks, nil
	}
	list, err := s.Paystack.Banks(ctx)
	if err != nil {
		if banks != nil {
			return banks, nil // stale beats down
		}
		return nil, err
	}
	banks, banksAt = list, time.Now()
	clear(bankName)
	for _, b := range list {
		bankName[b.Code] = b.Name
	}
	return banks, nil
}

// SetPayoutAccount verifies the account with the bank (name enquiry) and
// registers it as a Paystack transfer recipient.
func (s *Service) SetPayoutAccount(ctx context.Context, userID, bankCode, accountNumber string) (PayoutAccount, error) {
	if !nuban.MatchString(accountNumber) {
		return PayoutAccount{}, errs.BadRequest("account_number_invalid", "Enter the 10-digit account number.")
	}
	if _, err := s.Banks(ctx); err != nil {
		return PayoutAccount{}, err
	}
	banksMu.Lock()
	name, ok := bankName[bankCode]
	banksMu.Unlock()
	if !ok {
		return PayoutAccount{}, errs.BadRequest("bank_invalid", "Pick your bank from the list.")
	}
	holder, err := s.Paystack.ResolveAccount(ctx, accountNumber, bankCode)
	if err != nil {
		return PayoutAccount{}, ErrBankUnverified
	}
	rc, err := s.Paystack.CreateRecipient(ctx, holder, accountNumber, bankCode)
	if err != nil {
		return PayoutAccount{}, err
	}
	pa := PayoutAccount{BankCode: bankCode, BankName: name, AccountLast4: accountNumber[6:], AccountName: holder, UpdatedAt: time.Now().UTC()}
	err = db.InTx(ctx, s.userShard(userID).Pool, func(tx pgx.Tx) error {
		var prev *string
		if err := tx.QueryRow(ctx, `SELECT recipient_code FROM payout_accounts WHERE user_id = $1 FOR UPDATE`, userID).Scan(&prev); err != nil && !db.IsNoRows(err) {
			return err
		}
		if prev != nil && *prev == rc {
			return nil // same account again: nothing changes, no cool-down
		}
		if _, err := tx.Exec(ctx, `INSERT INTO payout_accounts (user_id, bank_code, bank_name, account_last4, account_name, recipient_code)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (user_id) DO UPDATE SET bank_code = $2, bank_name = $3, account_last4 = $4, account_name = $5,
				recipient_code = $6, updated_at = now(), changed_at = now()`, userID, bankCode, name, pa.AccountLast4, holder, rc); err != nil {
			return err
		}
		// Security alert: a stolen account's first move is often to swap the bank.
		return outbox.Insert(ctx, tx, events.TopicWalletEvents, events.MustNew(producer, events.PayoutAccountChanged, userID,
			events.PayoutAccountChangedData{UserID: userID, BankName: name, AccountLast4: pa.AccountLast4, FirstTime: prev == nil}))
	})
	return pa, err
}

// PayoutAccountCooldown: cash-outs pause this long after the bank account
// is changed (not when it's first set), so a hijacked account can't be
// emptied before the owner sees the alert.
const PayoutAccountCooldown = 48 * time.Hour

var ErrPayoutCooldown = errs.Conflict("payout_account_cooldown",
	"You changed your bank account recently. For your safety, cash-outs open 48 hours after a change.")

func (s *Service) PayoutAccountOf(ctx context.Context, userID string) (*PayoutAccount, error) {
	var pa PayoutAccount
	err := s.userShard(userID).Pool.QueryRow(ctx, `SELECT bank_code, bank_name, account_last4, account_name, updated_at
		FROM payout_accounts WHERE user_id = $1`, userID).Scan(&pa.BankCode, &pa.BankName, &pa.AccountLast4, &pa.AccountName, &pa.UpdatedAt)
	if db.IsNoRows(err) {
		return nil, nil
	}
	return &pa, err
}

type Payout struct {
	ID         string    `json:"id"`
	UserID     string    `json:"user_id"`
	Diamonds   int64     `json:"diamonds"`
	AmountKobo int64     `json:"amount_kobo"`
	BankLabel  string    `json:"bank_label"`
	Status     string    `json:"status"`
	Note       string    `json:"note,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

const payoutCols = `id::text, user_id::text, diamonds, amount_kobo, bank_label, status, note, created_at, updated_at`

func scanPayout(r pgx.Row) (Payout, error) {
	var p Payout
	err := r.Scan(&p.ID, &p.UserID, &p.Diamonds, &p.AmountKobo, &p.BankLabel, &p.Status, &p.Note, &p.CreatedAt, &p.UpdatedAt)
	return p, err
}

// MyPayouts lists a user's cash-outs, newest first.
func (s *Service) MyPayouts(ctx context.Context, userID string) ([]Payout, error) {
	rows, err := s.userShard(userID).Pool.Query(ctx, `SELECT `+payoutCols+` FROM payouts WHERE user_id = $1 ORDER BY created_at DESC LIMIT 50`, userID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Payout, error) { return scanPayout(r) })
}

// AdminPayouts lists cash-outs in a status across every shard (finance console).
func (s *Service) AdminPayouts(ctx context.Context, status string, limit int) ([]Payout, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	var out []Payout
	for _, r := range s.Shards.All() {
		rows, err := r.Pool.Query(ctx, `SELECT `+payoutCols+` FROM payouts WHERE status = $1 ORDER BY created_at LIMIT $2`, status, limit)
		if err != nil {
			return nil, err
		}
		list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Payout, error) { return scanPayout(r) })
		if err != nil {
			return nil, err
		}
		out = append(out, list...)
	}
	if out == nil {
		out = []Payout{}
	}
	return out, nil
}

// payoutShard finds which shard holds a payout (IDs don't encode the user).
func (s *Service) payoutShard(ctx context.Context, payoutID string) (*shard.Range, error) {
	for _, r := range s.Shards.All() {
		var ok bool
		if err := r.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM payouts WHERE id = $1)`, payoutID).Scan(&ok); err != nil {
			return nil, err
		}
		if ok {
			return r, nil
		}
	}
	return nil, errs.ErrNotFound
}

// ApprovePayout starts the bank transfer. The transfer reference is the
// payout ID, so retries (ours or Paystack's) can never pay twice.
func (s *Service) ApprovePayout(ctx context.Context, payoutID, reviewer string) (Payout, error) {
	if s.Paystack == nil {
		return Payout{}, errs.Unavailable("payouts_disabled", "Bank cash-outs aren't configured.")
	}
	r, err := s.payoutShard(ctx, payoutID)
	if err != nil {
		return Payout{}, err
	}
	var p Payout
	var recipient *string
	err = db.InTx(ctx, r.Pool, func(tx pgx.Tx) error {
		var status, owner string
		if err := tx.QueryRow(ctx, `SELECT status, recipient_code, user_id::text FROM payouts WHERE id = $1 FOR UPDATE`, payoutID).Scan(&status, &recipient, &owner); err != nil {
			return err
		}
		if owner == reviewer {
			return ErrSelfReview
		}
		if recipient == nil || *recipient == "" {
			return ErrNoPayoutAccount
		}
		var err error
		switch status {
		case "pending_review":
			p, err = scanPayout(tx.QueryRow(ctx, `UPDATE payouts SET status = 'processing', reviewed_by = $2, transfer_reference = $1::text,
				updated_at = now() WHERE id = $1 RETURNING `+payoutCols, payoutID, reviewer))
		case "processing":
			// Retry after an unknown outcome. The transfer reference is the
			// payout ID, and Paystack refuses a second transfer with the same
			// reference, so this can't pay twice.
			p, err = scanPayout(tx.QueryRow(ctx, `SELECT `+payoutCols+` FROM payouts WHERE id = $1`, payoutID))
		default:
			return errs.Conflict("payout_not_pending", "This cash-out was already reviewed.")
		}
		return err
	})
	if err != nil {
		return p, err
	}
	// Outside the DB transaction: a network call must not hold row locks.
	state, err := s.Paystack.Transfer(ctx, p.AmountKobo, *recipient, payoutID, "Lumo cash-out")
	if err != nil {
		// Unknown outcome: stays "processing"; the transfer webhook (or a
		// re-approval by finance after checking Paystack) settles it.
		return p, fmt.Errorf("transfer %s: %w", payoutID, err)
	}
	if state == "success" {
		return s.SettlePayout(ctx, payoutID, "paid", "")
	}
	return p, nil
}

var ErrSelfReview = errs.Forbidden("payout_self_review", "Someone else in finance has to review your own cash-out.")

// RejectPayout returns the diamonds to the user. Only a cash-out still
// waiting for review can be rejected; the status check happens under the
// row lock, so a concurrent approval can't also be rejected (which would
// pay the bank transfer and refund the diamonds).
func (s *Service) RejectPayout(ctx context.Context, payoutID, reviewer, reason string) (Payout, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" || len([]rune(reason)) > 300 {
		return Payout{}, errs.BadRequest("reason_invalid", "Give a reason (up to 300 characters).")
	}
	return s.settle(ctx, payoutID, "rejected", reason, reviewer)
}

// SettlePayout moves a payout to a final state exactly once (webhooks and
// the approval path).
func (s *Service) SettlePayout(ctx context.Context, payoutID, final, reason string) (Payout, error) {
	return s.settle(ctx, payoutID, final, reason, "")
}

// settle applies one transition, all under the payout row lock:
//
//	processing     -> paid      diamonds leave the platform (clearing -> issuance)
//	pending_review -> rejected  diamonds back to the user
//	processing     -> failed    diamonds back to the user
//	processing|paid -> reversed (stored as failed) diamonds back to the user
//
// Repeating a transition that already happened is a no-op.
func (s *Service) settle(ctx context.Context, payoutID, final, reason, reviewer string) (Payout, error) {
	r, err := s.payoutShard(ctx, payoutID)
	if err != nil {
		return Payout{}, err
	}
	var p Payout
	err = db.InTx(ctx, r.Pool, func(tx pgx.Tx) error {
		cur, err := scanPayout(tx.QueryRow(ctx, `SELECT `+payoutCols+` FROM payouts WHERE id = $1 FOR UPDATE`, payoutID))
		if err != nil {
			return err
		}
		p = cur
		if reviewer != "" && reviewer == cur.UserID {
			return ErrSelfReview
		}
		var from string // the source of the returned or removed diamonds
		switch {
		case final == "reversed" && cur.Status == "paid":
			from = ledger.PlatformDiamondIssuance
		case final == "reversed" && cur.Status == "processing":
			from = ledger.PlatformPayoutClearing
		case cur.Status == "paid" || cur.Status == "rejected" || cur.Status == "failed":
			return nil // already settled
		case final == "paid" && cur.Status == "processing":
			from = ledger.PlatformPayoutClearing
		case final == "failed" && cur.Status == "processing":
			from = ledger.PlatformPayoutClearing
		case final == "rejected" && cur.Status == "pending_review":
			from = ledger.PlatformPayoutClearing
		default:
			return errs.Conflict("payout_wrong_state", fmt.Sprintf("A %s cash-out can't be marked %s.", strings.ReplaceAll(cur.Status, "_", " "), final))
		}
		var legs []ledger.Leg
		key := "payout_settle:" + payoutID
		if final == "paid" {
			legs = []ledger.Leg{{AccountID: from, Amount: -cur.Diamonds}, {AccountID: ledger.PlatformDiamondIssuance, Amount: cur.Diamonds}}
		} else {
			acct, err := ledger.Account(ctx, tx, cur.UserID, ledger.KindDiamonds)
			if err != nil {
				return err
			}
			legs = []ledger.Leg{{AccountID: from, Amount: -cur.Diamonds}, {AccountID: acct, Amount: cur.Diamonds, User: true}}
			if final == "reversed" {
				key = "payout_reverse:" + payoutID
			}
		}
		if err := ledger.Post(ctx, tx, events.NewID(), "payout_"+final, key, map[string]any{"payout_id": payoutID}, legs...); err != nil {
			if errors.Is(err, ledger.ErrDuplicate) {
				return nil
			}
			return err
		}
		stored := final
		if final == "reversed" {
			stored = "failed"
		}
		p, err = scanPayout(tx.QueryRow(ctx, `UPDATE payouts SET status = $2, note = $3, reviewed_by = COALESCE(NULLIF($4, '')::uuid, reviewed_by),
			updated_at = now() WHERE id = $1 RETURNING `+payoutCols, payoutID, stored, reason, reviewer))
		if err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, events.TopicWalletEvents, events.MustNew(producer, events.PayoutSettled, cur.UserID,
			events.PayoutSettledData{PayoutID: payoutID, UserID: cur.UserID, Diamonds: cur.Diamonds, AmountKobo: cur.AmountKobo, Status: stored, Reason: reason}))
	})
	return p, err
}

// PaystackTransfer handles transfer.success / transfer.failed / transfer.reversed webhooks.
func (s *Service) PaystackTransfer(ctx context.Context, event, reference string) error {
	if len(reference) != 36 { // payout IDs are UUIDs; anything else isn't ours
		return nil
	}
	switch event {
	case "transfer.success":
		_, err := s.SettlePayout(ctx, reference, "paid", "")
		return ignoreWebhookConflict(ignoreNotFound(err))
	case "transfer.failed":
		_, err := s.SettlePayout(ctx, reference, "failed", "The bank transfer didn't go through. Your diamonds were returned.")
		return ignoreWebhookConflict(ignoreNotFound(err))
	case "transfer.reversed":
		_, err := s.SettlePayout(ctx, reference, "reversed", "The bank sent the transfer back. Your diamonds were returned.")
		return ignoreWebhookConflict(ignoreNotFound(err))
	}
	return nil
}

// ignoreWebhookConflict: a webhook for a transition that can't happen (for
// example "success" for a payout nobody approved) is logged and dropped,
// rather than failing and being retried by Paystack forever.
func ignoreWebhookConflict(err error) error {
	var e *errs.Error
	if errors.As(err, &e) && e.Status == http.StatusConflict {
		slog.Warn("paystack transfer webhook ignored", "err", err)
		return nil
	}
	return err
}

func ignoreNotFound(err error) error {
	if errors.Is(err, errs.ErrNotFound) {
		return nil
	}
	return err
}

type DayAmount struct {
	Day      string `json:"day"` // YYYY-MM-DD (UTC)
	Diamonds int64  `json:"diamonds"`
}

// Earnings is diamonds received per UTC day for the last `days` days, zero-filled.
func (s *Service) Earnings(ctx context.Context, userID string, days int) ([]DayAmount, error) {
	if days <= 0 || days > 90 {
		days = 7
	}
	rows, err := s.userShard(userID).Pool.Query(ctx, `SELECT to_char(d, 'YYYY-MM-DD'), COALESCE(sum(c.diamonds), 0)
		FROM generate_series((now() AT TIME ZONE 'UTC')::date - ($2::int - 1), (now() AT TIME ZONE 'UTC')::date, interval '1 day') d
		LEFT JOIN gift_credits c ON c.receiver_id = $1 AND (c.credited_at AT TIME ZONE 'UTC')::date = d::date
		GROUP BY d ORDER BY d`, userID, days)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (DayAmount, error) {
		var x DayAmount
		return x, r.Scan(&x.Day, &x.Diamonds)
	})
}
