package app

// For You: two-stage recommendation.
//
//  1. Retrieval gathers up to ~900 candidates from cheap Redis lookups:
//     the viewer's top hashtags, creators they engage with, creators they
//     follow, "people who liked what you liked also liked" (co-engagement),
//     global trending, and fresh uploads.
//  2. Ranking scores each candidate (score.go) and re-ranks for variety,
//     reserving discovery slots so new creators and new topics get a chance.
//
// A ranked batch of 3 pages is queued per viewer for 15 minutes; strong
// signals (like, share, follow, "not interested") drop the queue so the very
// next page reflects them.

import (
	"context"
	"log/slog"
	"math"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/cache"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/metrics"
	"github.com/redis/go-redis/v9"
)

const (
	retrTags    = 8
	retrPerTag  = 80
	retrAuthors = 10
	retrFollows = 20
	retrPerAuth = 4
	retrSeeds   = 10
	retrPerSeed = 25
	retrHot     = 200
	retrFresh   = 100
	queuePages  = 3
	queueTTL    = 15 * time.Minute
	anonPool    = 150
)

var (
	servedTotal = metrics.NewCounterVec("feed_served_total", "Videos served in For You / Watch, by rec source and A/B arm.", "kind", "source", "arm")
	rankSeconds = metrics.NewHistogramVec("feed_rank_seconds", "Time to retrieve and rank a batch.", "kind")
)

type ranked struct{ id, src, owner string }

type profile struct {
	tags, authors       map[string]float64 // decayed, normalized to "now"
	topTags, topAuthors []string
	seeds               []string
	follows             map[string]bool
	followList          []string
}

func (s *Service) init() {
	s.once.Do(func() {
		s.metaCache = cache.New[meta](10*time.Minute, 500_000)
		s.statCache = cache.New[Stats](30*time.Second, 500_000)
		s.anonCache = cache.New[[]ranked](30*time.Second, 4)
		s.listCache = cache.New[[]string](5*time.Second, 20_000)
		s.specCache = cache.New[map[string]float64](time.Minute, 4)
		if s.Arms.A == (Weights{}) {
			s.Arms.A, s.Arms.B = DefaultWeights, DefaultWeights
		}
	})
}

// ForYou returns the next page for a viewer. Anonymous viewers get a
// shared, frequently refreshed trending pool.
func (s *Service) ForYou(ctx context.Context, userID, kind string, limit int) ([]Item, error) {
	s.init()
	if kind != "long" {
		kind = "short"
	}
	if limit <= 0 || limit > 20 {
		limit = 10
	}
	var picks []ranked
	arm := "a"
	if userID == "" {
		pool, err := s.anonymous(ctx, kind)
		if err != nil {
			return nil, err
		}
		picks = capAuthors(sample(pool, limit*2), limit)
	} else {
		arm, _ = s.Arms.For(userID)
		var err error
		picks, err = s.nextPage(ctx, userID, kind, limit)
		if err != nil {
			return nil, err
		}
	}
	ids := make([]string, len(picks))
	src := make(map[string]string, len(picks))
	for i, p := range picks {
		ids[i] = p.id
		src[p.id] = p.src
	}
	items, err := s.hydrate(ctx, ids, len(ids))
	if err != nil {
		return nil, err
	}
	for i := range items {
		items[i].Source, items[i].Arm = src[items[i].ID], arm
		servedTotal.WithLabelValues(kind, items[i].Source, arm).Inc()
	}
	if userID != "" && len(items) > 0 {
		s.markSeen(ctx, userID, items)
	}
	return items, nil
}

// nextPage pops from the viewer's queue, refilling it when short.
func (s *Service) nextPage(ctx context.Context, uid, kind string, limit int) ([]ranked, error) {
	qk := queueKey(uid, kind)
	raw, err := s.R.LPopCount(ctx, qk, limit).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	out := decodeQueue(raw)
	if len(out) == limit {
		return out, nil
	}
	fresh, err := s.recommend(ctx, uid, kind, limit, queuePages)
	if err != nil {
		if len(out) > 0 {
			slog.WarnContext(ctx, "feed: rank failed, serving queue remainder", "err", err)
			return out, nil
		}
		return nil, err
	}
	have := map[string]bool{}
	for _, r := range out {
		have[r.id] = true
	}
	rest := make([]string, 0, len(fresh))
	for _, r := range fresh {
		if have[r.id] {
			continue
		}
		if len(out) < limit {
			out = append(out, r)
			have[r.id] = true
			continue
		}
		rest = append(rest, r.id+"|"+r.src)
	}
	pipe := s.R.Pipeline()
	pipe.Del(ctx, qk)
	if len(rest) > 0 {
		vals := make([]any, len(rest))
		for i, v := range rest {
			vals[i] = v
		}
		pipe.RPush(ctx, qk, vals...)
		pipe.Expire(ctx, qk, queueTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		slog.WarnContext(ctx, "feed: queue write failed", "err", err)
	}
	return out, nil
}

func decodeQueue(raw []string) []ranked {
	out := make([]ranked, 0, len(raw))
	for _, v := range raw {
		id, src, _ := strings.Cut(v, "|")
		if len(id) == 36 {
			out = append(out, ranked{id: id, src: src})
		}
	}
	return out
}

// recommend runs retrieval and ranking for one viewer: pages × page items.
func (s *Service) recommend(ctx context.Context, uid, kind string, page, pages int) ([]ranked, error) {
	t0 := time.Now()
	defer func() { rankSeconds.WithLabelValues(kind).Observe(time.Since(t0).Seconds()) }()
	_, w := s.Arms.For(uid)
	prof, err := s.loadProfile(ctx, uid)
	if err != nil {
		return nil, err
	}
	cands, err := s.retrieve(ctx, kind, prof)
	if err != nil {
		return nil, err
	}
	fresh, seen, err := s.dropSeen(ctx, uid, cands)
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	out, err := s.rankSet(ctx, uid, kind, prof, w, fresh, page*pages, page, now)
	if err != nil {
		return nil, err
	}
	// Fresh candidates ran out (small catalogue, or a heavy viewer): rather
	// than an empty feed, bring back videos seen longest ago.
	if need := page*pages - len(out); need > 0 && len(seen) > 0 {
		// Skip the most recent half of what was seen (at most 50), so even a
		// tiny catalogue repeats without showing the last few videos again.
		guard := min(int64(recentRepeatGuard), s.R.ZCard(ctx, seenKey(uid)).Val()/2)
		recent, err := s.R.ZRevRange(ctx, seenKey(uid), 0, max(guard-1, 0)).Result()
		if err != nil {
			return nil, err
		}
		for _, id := range recent {
			delete(seen, id)
		}
		for _, r := range out {
			delete(seen, r.id)
		}
		for _, c := range seen {
			c.src |= srcRepeat
		}
		more, err := s.rankSet(ctx, uid, kind, prof, w, seen, need, page, now)
		if err != nil {
			return nil, err
		}
		out = append(out, more...)
	}
	return out, nil
}

// recentRepeatGuard caps how many of the most recently served videos are
// held back from repeats.
const recentRepeatGuard = 50

// rankSet attaches features, removes hidden or pushed-away creators, scores
// and re-ranks one candidate set.
func (s *Service) rankSet(ctx context.Context, uid, kind string, prof *profile, w Weights, cands map[string]*cand, n, page int, now int64) ([]ranked, error) {
	spec := s.specificity(ctx, kind)
	list, err := s.features(ctx, kind, cands)
	if err != nil {
		return nil, err
	}
	if list, err = s.dropHidden(ctx, uid, list); err != nil {
		return nil, err
	}
	kept := list[:0]
	for _, c := range list {
		if c.owner == uid {
			continue
		}
		c.follows = prof.follows[c.owner]
		c.setAffinity(prof.tags, prof.authors, spec, w.AffScale)
		if c.authAff < -0.6 || c.tagAff < -0.8 {
			continue // the viewer has clearly pushed this away
		}
		c.score = scoreOf(c, w, now, rand.Float64)
		kept = append(kept, c)
	}
	return toRanked(rerank(kept, n, page, w)), nil
}

func toRanked(cs []*cand) []ranked {
	out := make([]ranked, len(cs))
	for i, c := range cs {
		out[i] = ranked{c.id, c.src.label(), c.owner}
	}
	return out
}

// capAuthors keeps at most 2 per creator and n in total.
func capAuthors(rs []ranked, n int) []ranked {
	per := map[string]int{}
	out := make([]ranked, 0, n)
	for _, r := range rs {
		if per[r.owner] >= 2 {
			continue
		}
		per[r.owner]++
		out = append(out, r)
		if len(out) == n {
			break
		}
	}
	return out
}

// loadProfile reads the viewer's interests, normalized to the present.
func (s *Service) loadProfile(ctx context.Context, uid string) (*profile, error) {
	pipe := s.R.Pipeline()
	ep := pipe.Get(ctx, uKey(uid, "e"))
	topT := pipe.ZRevRangeWithScores(ctx, uKey(uid, "t"), 0, 49)
	lowT := pipe.ZRangeByScoreWithScores(ctx, uKey(uid, "t"), &redis.ZRangeBy{Min: "-inf", Max: "(0", Count: 20})
	topA := pipe.ZRevRangeWithScores(ctx, uKey(uid, "a"), 0, 49)
	lowA := pipe.ZRangeByScoreWithScores(ctx, uKey(uid, "a"), &redis.ZRangeBy{Min: "-inf", Max: "(0", Count: 20})
	seeds := pipe.ZRevRange(ctx, uKey(uid, "p"), 0, retrSeeds-1)
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	p := &profile{tags: map[string]float64{}, authors: map[string]float64{}, follows: map[string]bool{}, seeds: seeds.Val()}
	e, _ := ep.Int64()
	f := 1.0
	if e > 0 {
		f = boost(time.Now().Unix(), e)
	}
	add := func(m map[string]float64, top *[]string, zs []redis.Z, positive bool) {
		for _, z := range zs {
			id := z.Member.(string)
			v := z.Score / f
			m[id] = v
			if positive && v > 0.2 {
				*top = append(*top, id)
			}
		}
	}
	add(p.tags, &p.topTags, topT.Val(), true)
	add(p.tags, nil, lowT.Val(), false)
	add(p.authors, &p.topAuthors, topA.Val(), true)
	add(p.authors, nil, lowA.Val(), false)

	// Follows are a strong, durable signal; a social outage just means
	// ranking without them.
	if ids, err := s.followingIDs(ctx, uid); err != nil {
		slog.WarnContext(ctx, "feed: following unavailable", "err", err)
	} else {
		for _, id := range ids {
			p.follows[id] = true
		}
		p.followList = ids
		if len(ids) > retrFollows {
			sampled := append([]string(nil), ids...)
			rand.Shuffle(len(sampled), func(i, j int) { sampled[i], sampled[j] = sampled[j], sampled[i] })
			p.followList = sampled[:retrFollows]
		}
	}
	return p, nil
}

// retrieve gathers candidates from every source in one pipeline. Shared
// lists come from the per-pod list cache when fresh.
func (s *Service) retrieve(ctx context.Context, kind string, p *profile) (map[string]*cand, error) {
	pipe := s.R.Pipeline()
	var authCmds, followCmds []*redis.StringSliceCmd
	var coCmds []*redis.ZSliceCmd
	shared := map[string]*redis.StringSliceCmd{}
	global := func(key string, n int64) {
		if _, ok := s.listCache.Get(key); !ok {
			shared[key] = pipe.ZRevRange(ctx, key, 0, n-1)
		}
	}
	// Generic tags (#naija, #fyp) say little about taste: don't spend
	// retrieval on them.
	spec := s.specificity(ctx, kind)
	tags := make([]string, 0, retrTags)
	for _, t := range p.topTags {
		if v, ok := spec[t]; ok && v < 0.1 {
			continue
		}
		if tags = append(tags, t); len(tags) == retrTags {
			break
		}
	}
	for _, t := range tags {
		global(tagKey(kind, t), retrPerTag)
	}
	global(hotKey(kind), retrHot)
	global(newKey(kind), retrFresh)
	for _, a := range head(p.topAuthors, retrAuthors) {
		authCmds = append(authCmds, pipe.ZRevRange(ctx, authorKey(a), 0, retrPerAuth-1))
	}
	for _, a := range p.followList {
		followCmds = append(followCmds, pipe.ZRevRange(ctx, authorKey(a), 0, retrPerAuth-1))
	}
	for _, seed := range head(p.seeds, retrSeeds) {
		coCmds = append(coCmds, pipe.ZRevRangeWithScores(ctx, coKey(seed), 0, retrPerSeed-1))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	for k, c := range shared {
		s.listCache.Set(k, c.Val())
	}
	list := func(key string) []string { v, _ := s.listCache.Get(key); return v }
	out := map[string]*cand{}
	get := func(id string) *cand {
		c, ok := out[id]
		if !ok {
			c = &cand{id: id}
			out[id] = c
		}
		return c
	}
	for _, t := range tags {
		for _, id := range list(tagKey(kind, t)) {
			get(id).src |= srcTag
		}
	}
	for _, c := range authCmds {
		for _, id := range c.Val() {
			get(id).src |= srcAuthor
		}
	}
	for _, c := range followCmds {
		for _, id := range c.Val() {
			get(id).src |= srcFollow
		}
	}
	for i, c := range coCmds {
		zs := c.Val()
		if len(zs) == 0 {
			continue
		}
		top := zs[0].Score
		recency := 1 / (1 + 0.15*float64(i)) // newer seeds count more
		for _, z := range zs {
			id := z.Member.(string)
			cd := get(id)
			cd.src |= srcCo
			cd.co = math.Max(cd.co, recency*z.Score/top)
		}
	}
	hv := list(hotKey(kind))
	for i, id := range hv {
		c := get(id)
		c.src |= srcHot
		c.hot = 1 - float64(i)/float64(len(hv))
	}
	for _, id := range list(newKey(kind)) {
		get(id).src |= srcFresh
	}
	// Videos the viewer already engaged with are never shown as new, but they
	// stay candidates for the repeat fallback: dropping them outright left a
	// small catalogue with nothing to repeat (found by the preview run).
	for _, seed := range p.seeds {
		if c, ok := out[seed]; ok {
			c.engaged = true
		}
	}
	return out, nil
}

func head(s []string, n int) []string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// dropSeen splits candidates into unseen and already-seen (kept for the
// repeat fallback).
func (s *Service) dropSeen(ctx context.Context, uid string, cands map[string]*cand) (fresh, seen map[string]*cand, err error) {
	seen = map[string]*cand{}
	if len(cands) == 0 {
		return cands, seen, nil
	}
	ids := make([]string, 0, len(cands))
	for id := range cands {
		ids = append(ids, id)
	}
	scores, err := s.R.ZMScore(ctx, seenKey(uid), ids...).Result()
	if err != nil {
		return nil, nil, err
	}
	for i, id := range ids {
		if scores[i] != 0 || cands[id].engaged {
			seen[id] = cands[id]
			delete(cands, id)
		}
	}
	return cands, seen, nil
}

// features attaches metadata and stats, from the in-process caches when
// possible (popular videos are candidates for everyone at once).
func (s *Service) features(ctx context.Context, kind string, cands map[string]*cand) ([]*cand, error) {
	var needMeta, needStats []string
	for id := range cands {
		if _, ok := s.metaCache.Get(id); !ok {
			needMeta = append(needMeta, id)
		}
		if _, ok := s.statCache.Get(id); !ok {
			needStats = append(needStats, id)
		}
	}
	ms, err := s.metas(ctx, needMeta)
	if err != nil {
		return nil, err
	}
	for id, m := range ms {
		s.metaCache.Set(id, m)
	}
	st, err := s.stats(ctx, needStats)
	if err != nil {
		return nil, err
	}
	for id, v := range st {
		s.statCache.Set(id, v)
	}
	out := make([]*cand, 0, len(cands))
	for id, c := range cands {
		m, ok := s.metaCache.Get(id)
		if !ok || m.kind != kind {
			continue // gone, or a different feed (author lists mix kinds)
		}
		c.owner, c.kind, c.created, c.tags = m.owner, m.kind, m.created, m.tags
		c.st, _ = s.statCache.Get(id)
		out = append(out, c)
	}
	return out, nil
}

// dropHidden removes creators the viewer blocked or who blocked them.
func (s *Service) dropHidden(ctx context.Context, uid string, cs []*cand) ([]*cand, error) {
	n, err := s.R.SCard(ctx, uKey(uid, "x")).Result()
	if err != nil || n == 0 {
		return cs, err
	}
	owners := make([]any, 0, len(cs))
	seen := map[string]bool{}
	for _, c := range cs {
		if !seen[c.owner] {
			seen[c.owner] = true
			owners = append(owners, c.owner)
		}
	}
	hit, err := s.R.SMIsMember(ctx, uKey(uid, "x"), owners...).Result()
	if err != nil {
		return nil, err
	}
	hidden := map[string]bool{}
	for i, h := range hit {
		if h {
			hidden[owners[i].(string)] = true
		}
	}
	out := cs[:0]
	for _, c := range cs {
		if !hidden[c.owner] {
			out = append(out, c)
		}
	}
	return out, nil
}

// anonymous ranks a shared pool for signed-out viewers, rebuilt every 30s
// per pod.
func (s *Service) anonymous(ctx context.Context, kind string) ([]ranked, error) {
	if pool, ok := s.anonCache.Get(kind); ok {
		return pool, nil
	}
	cands, err := s.retrieve(ctx, kind, &profile{})
	if err != nil {
		return nil, err
	}
	list, err := s.features(ctx, kind, cands)
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	w := s.Arms.A
	for _, c := range list {
		c.score = scoreOf(c, w, now, rand.Float64)
	}
	pool := toRanked(rerank(list, anonPool, 0, w))
	s.anonCache.Set(kind, pool)
	return pool, nil
}

// sample takes n items from a ranked pool, favouring the top but varying
// between requests.
func sample(pool []ranked, n int) []ranked {
	type k struct {
		r   ranked
		key float64
	}
	ks := make([]k, len(pool))
	for i, r := range pool {
		// Efraimidis–Spirakis weighted sampling: weight decays with rank.
		w := 1 / (1 + float64(i)/20)
		ks[i] = k{r, math.Pow(rand.Float64(), 1/w)}
	}
	// partial selection of the n largest keys
	out := make([]ranked, 0, n)
	used := make([]bool, len(ks))
	for len(out) < n && len(out) < len(ks) {
		best := -1
		for i := range ks {
			if !used[i] && (best < 0 || ks[i].key > ks[best].key) {
				best = i
			}
		}
		used[best] = true
		out = append(out, ks[best].r)
	}
	return out
}

// specificity returns tag -> 0..1 for the most common tags of a feed kind
// (tags not listed are fully specific). Cached per pod for a minute.
func (s *Service) specificity(ctx context.Context, kind string) map[string]float64 {
	if m, ok := s.specCache.Get(kind); ok {
		return m
	}
	m := map[string]float64{}
	pipe := s.R.Pipeline()
	top := pipe.ZRevRangeWithScores(ctx, tagDFKey(kind), 0, 499)
	total := pipe.Get(ctx, vCountKey(kind))
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return m // fall back to treating every tag as specific
	}
	n, _ := total.Float64()
	if n >= 20 { // too few videos to judge
		for _, z := range top.Val() {
			if sp := specificity(z.Score / n); sp < 1 {
				m[z.Member.(string)] = sp
			}
		}
	}
	s.specCache.Set(kind, m)
	return m
}
