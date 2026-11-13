// Package redisx creates Redis clients (single node, TLS, or Cluster) and
// provides a distributed rate limiter.
package redisx

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Open connects using REDIS_URL (redis:// or rediss://). If clusterAddrs is
// set, a Cluster client is used instead (comma-separated host:port list).
func Open(ctx context.Context, url, clusterAddrs, password string) (redis.UniversalClient, error) {
	var c redis.UniversalClient
	if clusterAddrs != "" {
		c = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        strings.Split(clusterAddrs, ","),
			Password:     password,
			PoolSize:     200,
			MinIdleConns: 10,
			ReadTimeout:  2 * time.Second,
			WriteTimeout: 2 * time.Second,
		})
	} else {
		opt, err := redis.ParseURL(url)
		if err != nil {
			return nil, fmt.Errorf("redisx: parse url: %w", err)
		}
		opt.PoolSize = 200
		opt.MinIdleConns = 10
		opt.ReadTimeout = 2 * time.Second
		opt.WriteTimeout = 2 * time.Second
		c = redis.NewClient(opt)
	}
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := c.Ping(pctx).Err(); err != nil {
		return nil, fmt.Errorf("redisx: ping: %w", err)
	}
	return c, nil
}

// limiterScript is a fixed-window counter with a hard cap; atomic in Redis.
// Returns {allowed (1/0), current count, ttl ms}.
var limiterScript = redis.NewScript(`
local n = redis.call('INCR', KEYS[1])
if n == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[2]) end
local ttl = redis.call('PTTL', KEYS[1])
if n > tonumber(ARGV[1]) then return {0, n, ttl} end
return {1, n, ttl}
`)

// Limiter enforces "at most Limit actions per Window" per key across all pods.
type Limiter struct {
	R      redis.UniversalClient
	Prefix string
}

// Result of a rate-limit check.
type Result struct {
	Allowed    bool
	Count      int64
	RetryAfter time.Duration
}

// Allow counts one action for key. If Redis is unavailable it fails OPEN for
// reads and callers decide; security-sensitive paths (OTP, login) should
// treat err != nil as "deny".
func (l Limiter) Allow(ctx context.Context, key string, limit int, window time.Duration) (Result, error) {
	k := l.Prefix + key
	res, err := limiterScript.Run(ctx, l.R, []string{k}, limit, window.Milliseconds()).Int64Slice()
	if err != nil {
		return Result{Allowed: true}, fmt.Errorf("redisx: limiter: %w", err)
	}
	r := Result{Allowed: res[0] == 1, Count: res[1]}
	if !r.Allowed {
		r.RetryAfter = time.Duration(res[2]) * time.Millisecond
	}
	return r, nil
}
