// Package ledger is the double-entry core. Every function here runs inside a
// caller's transaction on ONE shard; nothing here ever spans shards.
package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
)

const (
	KindCoins    = "coins"
	KindDiamonds = "diamonds"
)

// Platform accounts (same IDs on every shard).
const (
	PlatformCoinIssuance    = "00000000-0000-0000-0000-000000000001"
	PlatformGiftClearing    = "00000000-0000-0000-0000-000000000002"
	PlatformDiamondIssuance = "00000000-0000-0000-0000-000000000003"
	PlatformPayoutClearing  = "00000000-0000-0000-0000-000000000004"
	PlatformRefunds         = "00000000-0000-0000-0000-000000000005"
)

var (
	ErrInsufficient = errors.New("ledger: insufficient balance")
	ErrDuplicate    = errors.New("ledger: duplicate idempotency key")
)

// Leg is one side of a transaction. User legs update the cached balance;
// platform legs only write entries.
type Leg struct {
	AccountID string
	Amount    int64
	User      bool
}

// Account returns (creating if needed) a user's account ID.
func Account(ctx context.Context, q db.Querier, ownerID, kind string) (string, error) {
	var id string
	err := q.QueryRow(ctx, `INSERT INTO accounts (id, owner_id, kind) VALUES ($1, $2, $3)
		ON CONFLICT (owner_id, kind) DO UPDATE SET kind = EXCLUDED.kind
		RETURNING id::text`, events.NewID(), ownerID, kind).Scan(&id)
	return id, err
}

// LockBalance locks a user account row and returns its balance.
func LockBalance(ctx context.Context, q db.Querier, accountID string) (int64, error) {
	var b int64
	err := q.QueryRow(ctx, `SELECT balance FROM accounts WHERE id = $1 FOR UPDATE`, accountID).Scan(&b)
	return b, err
}

// Balances returns a user's coin and diamond balances (0 if no account yet).
func Balances(ctx context.Context, q db.Querier, ownerID string) (coins, diamonds int64, err error) {
	rows, err := q.Query(ctx, `SELECT kind, balance FROM accounts WHERE owner_id = $1 AND kind IN ('coins', 'diamonds')`, ownerID)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		var b int64
		if err := rows.Scan(&k, &b); err != nil {
			return 0, 0, err
		}
		if k == KindCoins {
			coins = b
		} else {
			diamonds = b
		}
	}
	return coins, diamonds, rows.Err()
}

// Post writes a balanced transaction. It returns ErrDuplicate (and writes
// nothing) if the idempotency key was used before.
func Post(ctx context.Context, tx pgx.Tx, txID, kind, idemKey string, meta any, legs ...Leg) error {
	var sum int64
	for _, l := range legs {
		sum += l.Amount
	}
	if sum != 0 || len(legs) < 2 {
		return fmt.Errorf("ledger: unbalanced transaction (sum %d, legs %d)", sum, len(legs))
	}
	m, _ := json.Marshal(meta)
	tag, err := tx.Exec(ctx, `INSERT INTO transactions (id, kind, idempotency_key, metadata) VALUES ($1, $2, $3, $4)
		ON CONFLICT (idempotency_key) DO NOTHING`, txID, kind, idemKey, m)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrDuplicate
	}
	// Update user balances in a fixed order to avoid deadlocks.
	sort.SliceStable(legs, func(i, j int) bool { return legs[i].AccountID < legs[j].AccountID })
	b := &pgx.Batch{}
	for _, l := range legs {
		b.Queue(`INSERT INTO entries (tx_id, account_id, amount) VALUES ($1, $2, $3)`, txID, l.AccountID, l.Amount)
		if l.User {
			b.Queue(`UPDATE accounts SET balance = balance + $2, updated_at = now() WHERE id = $1`, l.AccountID, l.Amount)
		}
	}
	return tx.SendBatch(ctx, b).Close()
}

// Reconcile checks that every transaction on the shard balances and that
// cached user balances equal the sum of their entries. Run nightly.
func Reconcile(ctx context.Context, q db.Querier) (unbalancedTx, badBalances int64, err error) {
	if err = q.QueryRow(ctx, `SELECT count(*) FROM (SELECT tx_id FROM entries GROUP BY tx_id HAVING sum(amount) <> 0) x`).Scan(&unbalancedTx); err != nil {
		return
	}
	err = q.QueryRow(ctx, `SELECT count(*) FROM accounts a
		LEFT JOIN (SELECT account_id, sum(amount) s FROM entries GROUP BY account_id) e ON e.account_id = a.id
		WHERE a.kind IN ('coins', 'diamonds') AND a.balance <> COALESCE(e.s, 0)`).Scan(&badBalances)
	return
}
