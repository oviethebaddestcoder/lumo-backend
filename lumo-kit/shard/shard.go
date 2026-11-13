// Package shard routes a key (usually a user ID) to one of several Postgres
// databases, so a single service can grow past one machine.
//
// Keys hash to one of a fixed number of LOGICAL shards (default 4096, never
// changed after launch). Logical shards map to PHYSICAL databases by range.
// Start with one Neon project holding all 4096; when it gets hot, copy a
// range to a new project (Neon branch + logical replication), flip the range
// in config, and redeploy. No data is rehashed.
//
// Config format (SHARDS env): "0-2047=postgres://a...;2048-4095=postgres://b..."
// or a single DSN for everything.
package shard

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
)

const DefaultLogical = 4096

type Range struct {
	From, To int // inclusive logical shard range
	DSN      string
	Pool     *pgxpool.Pool
	Index    int
}

type Router struct {
	logical int
	ranges  []*Range
}

// Parse reads the SHARDS spec without connecting.
func Parse(spec string, logical int) ([]*Range, error) {
	if logical <= 0 {
		logical = DefaultLogical
	}
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, fmt.Errorf("shard: empty spec")
	}
	if !strings.Contains(spec, "=") {
		return []*Range{{From: 0, To: logical - 1, DSN: spec}}, nil
	}
	var rs []*Range
	for _, part := range strings.Split(spec, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		rng, dsn, ok := strings.Cut(part, "=")
		a, b, ok2 := strings.Cut(rng, "-")
		from, err1 := strconv.Atoi(a)
		to, err2 := strconv.Atoi(b)
		if !ok || !ok2 || err1 != nil || err2 != nil || from > to {
			return nil, fmt.Errorf("shard: bad range %q", rng)
		}
		rs = append(rs, &Range{From: from, To: to, DSN: dsn})
	}
	sort.Slice(rs, func(i, j int) bool { return rs[i].From < rs[j].From })
	next := 0
	for _, r := range rs {
		if r.From != next {
			return nil, fmt.Errorf("shard: ranges must cover 0-%d without gaps (gap at %d)", logical-1, next)
		}
		next = r.To + 1
	}
	if next != logical {
		return nil, fmt.Errorf("shard: ranges end at %d, want %d", next-1, logical-1)
	}
	return rs, nil
}

// Open connects to every physical shard.
func Open(ctx context.Context, spec string, logical int, o db.Options) (*Router, error) {
	rs, err := Parse(spec, logical)
	if err != nil {
		return nil, err
	}
	if logical <= 0 {
		logical = DefaultLogical
	}
	for i, r := range rs {
		r.Index = i
		if r.Pool, err = db.Open(ctx, r.DSN, o); err != nil {
			for _, x := range rs[:i] {
				x.Pool.Close()
			}
			return nil, fmt.Errorf("shard %d-%d: %w", r.From, r.To, err)
		}
	}
	return &Router{logical: logical, ranges: rs}, nil
}

// NewStatic builds a router from open pools (tests).
func NewStatic(logical int, rs ...*Range) *Router {
	for i, r := range rs {
		r.Index = i
	}
	return &Router{logical: logical, ranges: rs}
}

// Logical returns the logical shard for a key.
func (r *Router) Logical(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(r.logical))
}

// For returns the physical shard holding key.
func (r *Router) For(key string) *Range {
	l := r.Logical(key)
	i := sort.Search(len(r.ranges), func(i int) bool { return r.ranges[i].To >= l })
	return r.ranges[i]
}

// All returns every physical shard (relays, migrations, scans).
func (r *Router) All() []*Range { return r.ranges }

func (r *Router) Close() {
	for _, x := range r.ranges {
		if x.Pool != nil {
			x.Pool.Close()
		}
	}
}

// Ping checks every shard (readiness).
func (r *Router) Ping(ctx context.Context) error {
	for _, x := range r.ranges {
		if err := x.Pool.Ping(ctx); err != nil {
			return fmt.Errorf("shard %d: %w", x.Index, err)
		}
	}
	return nil
}
