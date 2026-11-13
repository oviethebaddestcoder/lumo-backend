// Package db connects to Postgres (Neon) with pgx and provides transaction
// helpers and an embedded-SQL migration runner.
//
// Neon notes:
//   - Services use the POOLED connection string (host contains "-pooler"),
//     which goes through PgBouncer in transaction mode. Session state does not
//     survive between transactions there, so this package avoids named
//     server-side prepared statements on pooled hosts (cache_describe mode),
//     never uses session advisory locks or LISTEN/NOTIFY, and never SETs
//     session variables.
//   - Migrations use the DIRECT connection string (DATABASE_URL_DIRECT),
//     because they take a session advisory lock.
package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Options struct {
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	// ExecMode overrides the query exec mode: "cache_statement", "cache_describe",
	// "describe_exec", "exec", "simple_protocol". Empty = auto.
	ExecMode string
}

// Open creates a pool and pings it.
func Open(ctx context.Context, dsn string, o Options) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("db: parse dsn: %w", err)
	}
	if o.MaxConns > 0 {
		cfg.MaxConns = o.MaxConns
	}
	if o.MinConns > 0 {
		cfg.MinConns = o.MinConns
	}
	cfg.MaxConnLifetime = 30 * time.Minute
	if o.MaxConnLifetime > 0 {
		cfg.MaxConnLifetime = o.MaxConnLifetime
	}
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second

	mode := o.ExecMode
	if mode == "" && strings.Contains(cfg.ConnConfig.Host, "-pooler") {
		mode = "cache_describe"
	}
	switch mode {
	case "", "cache_statement":
	case "cache_describe":
		cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheDescribe
	case "describe_exec":
		cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	case "exec":
		cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	case "simple_protocol":
		cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	default:
		return nil, fmt.Errorf("db: unknown exec mode %q", mode)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("db: connect: %w", err)
	}
	// Neon computes can scale to zero; the first ping may take a few seconds.
	pingCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db: ping: %w", err)
	}
	return pool, nil
}

// Querier is satisfied by *pgxpool.Pool, *pgxpool.Conn and pgx.Tx.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// Beginner starts transactions.
type Beginner interface {
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
}

// InTx runs fn in a transaction, retrying on serialization failures and
// deadlocks (up to 5 attempts with jittered backoff). fn must be safe to retry.
func InTx(ctx context.Context, b Beginner, fn func(tx pgx.Tx) error) error {
	return InTxOpts(ctx, b, pgx.TxOptions{}, fn)
}

func InTxOpts(ctx context.Context, b Beginner, opts pgx.TxOptions, fn func(tx pgx.Tx) error) error {
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		err = runTx(ctx, b, opts, fn)
		if err == nil || !retryable(err) {
			return err
		}
		backoff := time.Duration(10*(1<<attempt)) * time.Millisecond
		slog.WarnContext(ctx, "retrying transaction", "attempt", attempt+1, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return err
}

func runTx(ctx context.Context, b Beginner, opts pgx.TxOptions, fn func(tx pgx.Tx) error) (err error) {
	tx, err := b.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
			panic(p)
		}
		if err != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func retryable(err error) bool {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return pg.Code == "40001" || pg.Code == "40P01"
	}
	return false
}

// IsUniqueViolation reports a unique constraint error, optionally for a named constraint.
func IsUniqueViolation(err error, constraint string) bool {
	var pg *pgconn.PgError
	if errors.As(err, &pg) && pg.Code == "23505" {
		return constraint == "" || pg.ConstraintName == constraint
	}
	return false
}

// IsNoRows reports pgx.ErrNoRows.
func IsNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }
