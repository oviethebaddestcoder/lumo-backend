// Package cache is a small, sharded, in-process TTL cache for hot read paths
// (user profiles, gift catalog). Each pod has its own copy; keep TTLs short.
package cache

import (
	"hash/fnv"
	"sync"
	"time"
)

const shards = 64

type entry[V any] struct {
	v   V
	exp time.Time
}

type shard[V any] struct {
	mu sync.RWMutex
	m  map[string]entry[V]
}

type Cache[V any] struct {
	ttl      time.Duration
	maxPer   int
	segments [shards]*shard[V]
}

// New creates a cache holding at most maxItems entries for ttl.
func New[V any](ttl time.Duration, maxItems int) *Cache[V] {
	c := &Cache[V]{ttl: ttl, maxPer: max(maxItems/shards, 16)}
	for i := range c.segments {
		c.segments[i] = &shard[V]{m: map[string]entry[V]{}}
	}
	return c
}

func (c *Cache[V]) seg(k string) *shard[V] {
	h := fnv.New32a()
	_, _ = h.Write([]byte(k))
	return c.segments[h.Sum32()%shards]
}

func (c *Cache[V]) Get(k string) (V, bool) {
	s := c.seg(k)
	s.mu.RLock()
	e, ok := s.m[k]
	s.mu.RUnlock()
	if !ok || time.Now().After(e.exp) {
		var zero V
		return zero, false
	}
	return e.v, true
}

func (c *Cache[V]) Set(k string, v V) {
	s := c.seg(k)
	s.mu.Lock()
	if len(s.m) >= c.maxPer {
		// Evict expired first, then arbitrary entries (map order is random).
		now := time.Now()
		for kk, e := range s.m {
			if now.After(e.exp) {
				delete(s.m, kk)
			}
		}
		for kk := range s.m {
			if len(s.m) < c.maxPer {
				break
			}
			delete(s.m, kk)
		}
	}
	s.m[k] = entry[V]{v: v, exp: time.Now().Add(c.ttl)}
	s.mu.Unlock()
}

func (c *Cache[V]) Delete(k string) {
	s := c.seg(k)
	s.mu.Lock()
	delete(s.m, k)
	s.mu.Unlock()
}
