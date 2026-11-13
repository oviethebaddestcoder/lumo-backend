package db

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Migrate applies embedded *.up.sql files in version order. File names look
// like 0001_init.up.sql. Each file runs in its own transaction; a session
// advisory lock keeps two pods from migrating at once, so run this against the
// DIRECT (non-pooled) connection string, as a Kubernetes Job before rollout.
//
// Migrations must be backward compatible with the running code (expand, then
// contract in a later release): add columns nullable, backfill in batches,
// create indexes CONCURRENTLY in a file that starts with "-- no-transaction".
func Migrate(ctx context.Context, dsn string, files fs.FS, lockID int64) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("migrate: connect: %w", err)
	}
	defer conn.Close(context.WithoutCancel(ctx))

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockID); err != nil {
		return fmt.Errorf("migrate: lock: %w", err)
	}
	defer conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, lockID)

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version bigint PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("migrate: init table: %w", err)
	}

	applied := map[int64]bool{}
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return err
		}
		applied[v] = true
	}
	rows.Close()

	names, err := fs.Glob(files, "*.up.sql")
	if err != nil {
		return err
	}
	type mig struct {
		v    int64
		name string
	}
	var migs []mig
	for _, n := range names {
		num, _, ok := strings.Cut(n, "_")
		v, err := strconv.ParseInt(num, 10, 64)
		if !ok || err != nil {
			return fmt.Errorf("migrate: bad file name %q", n)
		}
		migs = append(migs, mig{v, n})
	}
	sort.Slice(migs, func(i, j int) bool { return migs[i].v < migs[j].v })

	for _, m := range migs {
		if applied[m.v] {
			continue
		}
		body, err := fs.ReadFile(files, m.name)
		if err != nil {
			return err
		}
		sql := string(body)
		slog.Info("applying migration", "file", m.name)
		if strings.HasPrefix(strings.TrimSpace(sql), "-- no-transaction") {
			for _, stmt := range splitStatements(sql) {
				if _, err := conn.Exec(ctx, stmt); err != nil {
					return fmt.Errorf("migrate: %s: %w", m.name, err)
				}
			}
			if _, err := conn.Exec(ctx, `INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, m.v, m.name); err != nil {
				return err
			}
			continue
		}
		err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, sql); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, m.v, m.name)
			return err
		})
		if err != nil {
			return fmt.Errorf("migrate: %s: %w", m.name, err)
		}
	}
	return nil
}

// splitStatements splits on semicolons at line ends; enough for
// CREATE INDEX CONCURRENTLY files (no functions with bodies there).
func splitStatements(sql string) []string {
	var out []string
	for _, s := range strings.Split(sql, ";\n") {
		var lines []string
		for _, l := range strings.Split(s, "\n") {
			if t := strings.TrimSpace(l); t != "" && !strings.HasPrefix(t, "--") {
				lines = append(lines, l)
			}
		}
		if len(lines) > 0 {
			out = append(out, strings.TrimSuffix(strings.Join(lines, "\n"), ";"))
		}
	}
	return out
}
