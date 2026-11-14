package app

// Viewer interest profiles.
//
// Each viewer has a few small Redis keys, all sharing the {userID} hash tag
// so they live on one Redis Cluster slot:
//
//	fu:{u}:t  ZSET hashtag -> interest (signed, decayed)
//	fu:{u}:a  ZSET creator -> interest (signed, decayed)
//	fu:{u}:p  ZSET videos they engaged with positively -> time (last 50; seeds for co-engagement)
//	fu:{u}:w  ZSET videos already counted for them -> time (dedupe for stats, 2 days)
//	fu:{u}:x  SET  creators never to show (blocks, both directions)
//	fu:{u}:e  epoch for forward decay
//	fu:{u}:q:<kind> LIST pre-ranked next items (15 min)
//
// Decay is "forward decay": a signal at time t is added as
// w * 2^((t-epoch)/halfLife), so ZINCRBY stays a single atomic command and
// older interest fades relative to new without rewriting the set. When the
// multiplier gets large the set is rescaled and the epoch moved.

import (
	"context"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/metrics"
	"github.com/redis/go-redis/v9"
)

const (
	halfLife     = 5 * 24 * 3600.0 // seconds
	rescaleAt    = 40.0            // exponent; 2^40 keeps plenty of float precision
	profileTTL   = 60 * 24 * time.Hour
	recentKeep   = 50
	coSeeds      = 10
	coKeep       = 200
	tagIndexKeep = 1000
	maxTags      = 8
)

var (
	signalsTotal = metrics.NewCounterVec("feed_signals_total", "Viewer signals applied to interest profiles, by kind.", "kind")
	watchTotal   = metrics.NewCounterVec("feed_views_total", "Counted views by rec source and A/B arm, and whether they completed or skipped.", "source", "arm", "outcome")
)

func uKey(uid, part string) string { return "fu:{" + uid + "}:" + part }
func tagKey(kind, tag string) string {
	return "feed:tag:" + kind + ":" + tag
}
func coKey(id string) string { return "feed:co:" + id }
func queueKey(uid, kind string) string {
	return uKey(uid, "q:"+kind)
}

var rescaleScript = redis.NewScript(`
for _, k in ipairs(KEYS) do
  local m = redis.call('ZRANGE', k, 0, -1, 'WITHSCORES')
  for j = 1, #m, 2 do redis.call('ZADD', k, tonumber(m[j+1]) * tonumber(ARGV[1]), m[j]) end
end
return 1`)

// meta is what the feed keeps about a video for ranking.
type meta struct {
	owner   string
	kind    string
	created int64
	dur     int64
	tags    []string
}

func parseMeta(v []any) (meta, bool) {
	// fields: owner, kind, created, dur, tags
	if len(v) < 5 || v[0] == nil {
		return meta{}, false
	}
	m := meta{owner: str(v[0]), kind: str(v[1]), created: toInt(v, 2), dur: toInt(v, 3)}
	if t := str(v[4]); t != "" {
		m.tags = strings.Split(t, ",")
	}
	return m, true
}

var metaFields = []string{"owner", "kind", "created", "dur", "tags"}

func str(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// normTags lowercases, dedupes and caps hashtags.
func normTags(in []string) []string {
	out := make([]string, 0, maxTags)
	seen := map[string]bool{}
	for _, t := range in {
		t = strings.ToLower(strings.TrimPrefix(strings.TrimSpace(t), "#"))
		if t == "" || len(t) > 50 || strings.ContainsAny(t, ", ") || seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, t)
		if len(out) == maxTags {
			break
		}
	}
	return out
}

// signal is one thing a viewer did with a video.
type signal struct {
	uid, vid string
	m        meta
	w        float64 // applied to the video's hashtags and creator
	authorW  float64 // applied to the creator only
	positive bool    // seeds co-engagement
	strong   bool    // invalidates the pre-ranked queue so the next page reacts
	kind     string  // for metrics
}

type userState struct {
	epoch int64
	seeds []string
}

// loadStates reads epochs and recent positives for the users in sigs,
// creating epochs where missing and rescaling profiles that need it.
func (s *Service) loadStates(ctx context.Context, sigs []signal, now int64) (map[string]*userState, error) {
	users := map[string]*userState{}
	for _, g := range sigs {
		users[g.uid] = &userState{}
	}
	pipe := s.R.Pipeline()
	eps := map[string]*redis.StringCmd{}
	seeds := map[string]*redis.StringSliceCmd{}
	for u := range users {
		eps[u] = pipe.Get(ctx, uKey(u, "e"))
		seeds[u] = pipe.ZRevRange(ctx, uKey(u, "p"), 0, coSeeds-1)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	for u, st := range users {
		st.seeds = seeds[u].Val()
		e, err := eps[u].Int64()
		switch {
		case err != nil || e <= 0:
			// First signal: the epoch is now. SETNX so a concurrent writer wins cleanly.
			if ok, err := s.R.SetNX(ctx, uKey(u, "e"), now, profileTTL).Result(); err != nil {
				return nil, err
			} else if !ok {
				e, _ = s.R.Get(ctx, uKey(u, "e")).Int64()
			} else {
				e = now
			}
		case float64(now-e)/halfLife > rescaleAt:
			f := math.Pow(2, -float64(now-e)/halfLife)
			if err := rescaleScript.Run(ctx, s.R, []string{uKey(u, "t"), uKey(u, "a")}, f).Err(); err != nil {
				return nil, err
			}
			if err := s.R.Set(ctx, uKey(u, "e"), now, profileTTL).Err(); err != nil {
				return nil, err
			}
			e = now
		}
		st.epoch = e
	}
	return users, nil
}

func boost(now, epoch int64) float64 { return math.Pow(2, float64(now-epoch)/halfLife) }

// applySignals writes interest, co-engagement and queue invalidation for a
// batch of signals in one pipeline.
func (s *Service) applySignals(ctx context.Context, sigs []signal) error {
	if len(sigs) == 0 {
		return nil
	}
	now := time.Now().Unix()
	users, err := s.loadStates(ctx, sigs, now)
	if err != nil {
		return err
	}
	pipe := s.R.Pipeline()
	touched := map[string]bool{}
	for _, g := range sigs {
		st := users[g.uid]
		f := boost(now, st.epoch)
		if g.w != 0 {
			for _, t := range g.m.tags {
				pipe.ZIncrBy(ctx, uKey(g.uid, "t"), tagShare(g.w, len(g.m.tags))*f, t)
			}
		}
		if aw := g.w + g.authorW; aw != 0 && g.m.owner != "" && g.m.owner != g.uid {
			pipe.ZIncrBy(ctx, uKey(g.uid, "a"), aw*f, g.m.owner)
		}
		if g.positive && g.vid != "" {
			// Item-to-item co-engagement: link this video with the viewer's
			// last few positives, both ways.
			for _, seed := range st.seeds {
				if seed == g.vid {
					continue
				}
				pipe.ZIncrBy(ctx, coKey(seed), 1, g.vid)
				pipe.ZIncrBy(ctx, coKey(g.vid), 1, seed)
				pipe.Expire(ctx, coKey(seed), 30*24*time.Hour)
				if rand.IntN(20) == 0 {
					pipe.ZRemRangeByRank(ctx, coKey(seed), 0, -coKeep-1)
				}
			}
			pipe.Expire(ctx, coKey(g.vid), 30*24*time.Hour)
			pipe.ZAdd(ctx, uKey(g.uid, "p"), redis.Z{Score: float64(now), Member: g.vid})
			st.seeds = append([]string{g.vid}, st.seeds...)
			if len(st.seeds) > coSeeds {
				st.seeds = st.seeds[:coSeeds]
			}
		}
		if g.strong {
			pipe.Del(ctx, queueKey(g.uid, "short"), queueKey(g.uid, "long"))
		}
		touched[g.uid] = true
		signalsTotal.WithLabelValues(g.kind).Inc()
	}
	for u := range touched {
		pipe.ZRemRangeByRank(ctx, uKey(u, "p"), 0, -recentKeep-1)
		for _, k := range []string{"t", "a", "p", "e"} {
			pipe.Expire(ctx, uKey(u, k), profileTTL)
		}
		if rand.IntN(10) == 0 {
			// Keep the 100 strongest dislikes and 300 strongest likes; drop the
			// near-zero middle.
			for _, k := range []string{"t", "a"} {
				trimMiddle(ctx, s.R, pipe, uKey(u, k))
			}
		}
	}
	_, err = pipe.Exec(ctx)
	return err
}

func trimMiddle(ctx context.Context, r redis.UniversalClient, pipe redis.Pipeliner, key string) {
	if n, _ := r.ZCard(ctx, key).Result(); n > 500 {
		pipe.ZRemRangeByRank(ctx, key, 100, -301)
	}
}

// metas reads ranking metadata for videos (nil entries are missing).
func (s *Service) metas(ctx context.Context, ids []string) (map[string]meta, error) {
	out := make(map[string]meta, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	pipe := s.R.Pipeline()
	cmds := make(map[string]*redis.SliceCmd, len(ids))
	for _, id := range ids {
		if _, ok := cmds[id]; !ok {
			cmds[id] = pipe.HMGet(ctx, vKey(id), metaFields...)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	for id, c := range cmds {
		if m, ok := parseMeta(c.Val()); ok {
			out[id] = m
		}
	}
	return out, nil
}

// OnImpressions consumes feed.impressions in batches: per-video stats,
// viewer interest, and trending updates. A view counts once per viewer per
// video per two days, which also makes redelivered batches harmless.
func (s *Service) OnImpressions(ctx context.Context, envs []events.Envelope) error {
	type imp struct {
		d    events.ImpressionData
		isNx *redis.IntCmd
	}
	var imps []*imp
	now := time.Now()
	pipe := s.R.Pipeline()
	for _, e := range envs {
		if e.Type != events.Impression {
			continue
		}
		var d events.ImpressionData
		if e.Decode(&d) != nil || len(d.VideoID) != 36 || len(d.UserID) != 36 {
			continue
		}
		it := &imp{d: d}
		it.isNx = pipe.ZAddNX(ctx, uKey(d.UserID, "w"), redis.Z{Score: float64(now.Unix()), Member: d.VideoID})
		imps = append(imps, it)
	}
	if len(imps) == 0 {
		return nil
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	ids := make([]string, 0, len(imps))
	for _, it := range imps {
		ids = append(ids, it.d.VideoID)
	}
	ms, err := s.metas(ctx, ids)
	if err != nil {
		return err
	}

	var sigs []signal
	stats := s.R.Pipeline()
	videos := map[string]bool{}
	users := map[string]bool{}
	for _, it := range imps {
		users[it.d.UserID] = true
		if it.isNx.Val() == 0 {
			continue // already counted for this viewer
		}
		d := it.d
		m, ok := ms[d.VideoID]
		if !ok {
			continue // removed or private
		}
		w := watchWeight(d.WatchMS, m.dur, d.Loops, d.Skipped, m.kind)
		done := isComplete(d.WatchMS, m.dur)
		skip := w <= -0.6
		ek := engKey(d.VideoID)
		stats.HIncrBy(ctx, ek, "imp", 1)
		stats.HIncrBy(ctx, ek, "watch", min(d.WatchMS, m.dur*4)/1000)
		outcome := "partial"
		switch {
		case done:
			stats.HIncrBy(ctx, ek, "done", 1)
			outcome = "complete"
		case skip:
			stats.HIncrBy(ctx, ek, "skip", 1)
			outcome = "skip"
		}
		if d.Shared {
			stats.HIncrBy(ctx, ek, "shares", 1)
			w += wShare
		}
		stats.Expire(ctx, ek, 30*24*time.Hour)
		videos[d.VideoID] = true
		watchTotal.WithLabelValues(label(d.Source), label(d.Arm), outcome).Inc()
		kind := outcome
		if d.Shared {
			kind = "share"
		}
		// Finishing or sharing a video re-ranks the next page, so the feed
		// follows the viewer within a session (TikTok-like responsiveness).
		sigs = append(sigs, signal{uid: d.UserID, vid: d.VideoID, m: m, w: w, positive: w >= 1, strong: d.Shared || w >= 1, kind: kind})
	}
	for u := range users {
		stats.ZRemRangeByScore(ctx, uKey(u, "w"), "-inf", strconv.FormatInt(now.Add(-48*time.Hour).Unix(), 10))
		stats.Expire(ctx, uKey(u, "w"), 48*time.Hour)
	}
	if _, err := stats.Exec(ctx); err != nil {
		return err
	}
	if err := s.applySignals(ctx, sigs); err != nil {
		return err
	}
	return s.rehot(ctx, keys(videos))
}

var knownLabels = map[string]bool{"co": true, "follow": true, "author": true, "tag": true, "hot": true, "fresh": true, "repeat": true, "other": true, "a": true, "b": true}

// label keeps client-sent metric labels to a known set (unbounded labels
// would blow up Prometheus).
func label(v string) string {
	if knownLabels[v] {
		return v
	}
	return "none"
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// rehot recomputes trending scores for videos whose stats changed.
func (s *Service) rehot(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	ms, err := s.metas(ctx, ids)
	if err != nil {
		return err
	}
	st, err := s.stats(ctx, ids)
	if err != nil {
		return err
	}
	pipe := s.R.Pipeline()
	for id, m := range ms {
		h := hotScore(st[id], m.created)
		pipe.ZAddXX(ctx, hotKey(m.kind), redis.Z{Score: h, Member: id})
		for _, t := range m.tags {
			pipe.ZAdd(ctx, tagKey(m.kind, t), redis.Z{Score: h, Member: id})
			if rand.IntN(50) == 0 {
				pipe.ZRemRangeByRank(ctx, tagKey(m.kind, t), 0, -tagIndexKeep-1)
			}
		}
	}
	_, err = pipe.Exec(ctx)
	return err
}

var statFields = []string{"imp", "done", "skip", "likes", "comments", "shares"}

func (s *Service) stats(ctx context.Context, ids []string) (map[string]Stats, error) {
	out := make(map[string]Stats, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	pipe := s.R.Pipeline()
	cmds := make(map[string]*redis.SliceCmd, len(ids))
	for _, id := range ids {
		if _, ok := cmds[id]; !ok {
			cmds[id] = pipe.HMGet(ctx, engKey(id), statFields...)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	for id, c := range cmds {
		v := c.Val()
		out[id] = Stats{Imp: toInt(v, 0), Done: toInt(v, 1), Skip: toInt(v, 2), Likes: toInt(v, 3), Comments: toInt(v, 4), Shares: toInt(v, 5)}
	}
	return out, nil
}

// ---- viewer controls ----

// NotInterested: "show me less like this". Hides the video and pushes away
// its creator and hashtags.
func (s *Service) NotInterested(ctx context.Context, uid, vid string) error {
	ms, err := s.metas(ctx, []string{vid})
	if err != nil {
		return err
	}
	m, ok := ms[vid]
	if !ok {
		return nil
	}
	if err := s.R.ZAdd(ctx, seenKey(uid), redis.Z{Score: float64(time.Now().Unix()), Member: vid}).Err(); err != nil {
		return err
	}
	return s.applySignals(ctx, []signal{{uid: uid, vid: vid, m: m, w: wNotInterested, authorW: wNotInterAuth, strong: true, kind: "not_interested"}})
}

// Interest is one hashtag on the viewer's "Your interests" screen.
type Interest struct {
	Tag    string  `json:"tag"`
	Weight float64 `json:"weight"` // 0..1
}

func (s *Service) Interests(ctx context.Context, uid string) ([]Interest, error) {
	e, _ := s.R.Get(ctx, uKey(uid, "e")).Int64()
	zs, err := s.R.ZRevRangeByScoreWithScores(ctx, uKey(uid, "t"), &redis.ZRangeBy{Min: "(0", Max: "+inf", Count: 30}).Result()
	if err != nil {
		return nil, err
	}
	f := boost(time.Now().Unix(), e)
	out := make([]Interest, 0, len(zs))
	for _, z := range zs {
		w := aff(z.Score / f)
		if w < 0.05 {
			continue
		}
		out = append(out, Interest{Tag: z.Member.(string), Weight: math.Round(w*100) / 100})
	}
	return out, nil
}

// SetInterests seeds a new viewer's interests from the topics they pick at
// sign-up ("What do you like?"), which fixes the cold start: without it the
// feed can only find their tastes by chance. Each pick counts like a few
// finished videos and fades like any other signal.
func (s *Service) SetInterests(ctx context.Context, uid string, picked []string) error {
	tags := normTags(picked)
	if len(tags) > 10 {
		tags = tags[:10]
	}
	sigs := make([]signal, 0, len(tags))
	for _, t := range tags {
		sigs = append(sigs, signal{uid: uid, m: meta{tags: []string{t}}, w: wPicked, strong: true, kind: "picked"})
	}
	return s.applySignals(ctx, sigs)
}

// Topic is a suggestion for the interest picker.
type Topic struct {
	Tag    string `json:"tag"`
	Videos int64  `json:"videos"`
}

// Topics suggests hashtags for the picker: the most used tags that still
// say something specific (not #naija or #fyp).
func (s *Service) Topics(ctx context.Context, kind string, n int) ([]Topic, error) {
	s.init()
	zs, err := s.R.ZRevRangeWithScores(ctx, tagDFKey(kind), 0, 199).Result()
	if err != nil {
		return nil, err
	}
	spec := s.specificity(ctx, kind)
	out := make([]Topic, 0, n)
	for _, z := range zs {
		t := z.Member.(string)
		if v, ok := spec[t]; (ok && v < 1) || z.Score < 3 {
			continue
		}
		if out = append(out, Topic{Tag: t, Videos: int64(z.Score)}); len(out) == n {
			break
		}
	}
	return out, nil
}

// RemoveInterest drops a hashtag and nudges it negative so it stops coming back.
func (s *Service) RemoveInterest(ctx context.Context, uid, tag string) error {
	tags := normTags([]string{tag})
	if len(tags) == 0 {
		return nil
	}
	if err := s.R.ZRem(ctx, uKey(uid, "t"), tags[0]).Err(); err != nil {
		return err
	}
	return s.applySignals(ctx, []signal{{uid: uid, m: meta{tags: tags}, w: wTopicRemoved, strong: true, kind: "topic_removed"}})
}

// Reset is "Refresh your For You feed": forget interests and history.
// Blocks stay, and so does the per-view dedupe set (fu:{u}:w), so views
// aren't counted twice after a reset.
func (s *Service) Reset(ctx context.Context, uid string) error {
	pipe := s.R.Pipeline()
	pipe.Del(ctx, uKey(uid, "t"), uKey(uid, "a"), uKey(uid, "p"), uKey(uid, "e"), queueKey(uid, "short"), queueKey(uid, "long"))
	pipe.Del(ctx, seenKey(uid))
	_, err := pipe.Exec(ctx)
	signalsTotal.WithLabelValues("reset").Inc()
	return err
}

// ---- social signals (consumer of social.events) ----

func (s *Service) socialSignal(ctx context.Context, e events.Envelope) error {
	switch e.Type {
	case events.Liked, events.Unliked:
		var l events.LikeData
		if e.Decode(&l) != nil || l.UserID == "" {
			return nil
		}
		w, kind := wLike, "like"
		if e.Type == events.Unliked {
			w, kind = -wLike, "unlike"
		}
		return s.videoSignal(ctx, l.UserID, l.VideoID, w, 0, kind)
	case events.Commented:
		var c events.CommentData
		if e.Decode(&c) != nil || c.UserID == "" || c.UserID == c.OwnerID {
			return nil
		}
		return s.videoSignal(ctx, c.UserID, c.VideoID, wComment, 0, "comment")
	case events.Followed, events.Unfollowed:
		var f events.FollowData
		if e.Decode(&f) != nil || f.FollowerID == "" {
			return nil
		}
		w, kind := wFollow, "follow"
		if e.Type == events.Unfollowed {
			w, kind = -wFollow, "unfollow"
		}
		pipe := s.R.Pipeline()
		dropFollowing(ctx, pipe, f.FollowerID)
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
		return s.applySignals(ctx, []signal{{uid: f.FollowerID, m: meta{owner: f.FolloweeID}, authorW: w, strong: true, kind: kind}})
	case events.Blocked, events.Unblocked:
		var b events.BlockData
		if e.Decode(&b) != nil || b.BlockerID == "" || b.BlockedID == "" {
			return nil
		}
		pipe := s.R.Pipeline()
		if e.Type == events.Blocked {
			pipe.SAdd(ctx, uKey(b.BlockerID, "x"), b.BlockedID)
			pipe.SAdd(ctx, uKey(b.BlockedID, "x"), b.BlockerID)
		} else {
			pipe.SRem(ctx, uKey(b.BlockerID, "x"), b.BlockedID)
			pipe.SRem(ctx, uKey(b.BlockedID, "x"), b.BlockerID)
		}
		for _, u := range []string{b.BlockerID, b.BlockedID} {
			pipe.Del(ctx, queueKey(u, "short"), queueKey(u, "long"))
		}
		_, err := pipe.Exec(ctx)
		return err
	case events.Reported:
		var r events.ReportData
		if e.Decode(&r) != nil || r.ReporterID == "" {
			return nil
		}
		switch r.TargetType {
		case "video":
			return s.videoSignal(ctx, r.ReporterID, r.TargetID, wReport, wReportAuth-wReport, "report")
		case "user":
			return s.R.SAdd(ctx, uKey(r.ReporterID, "x"), r.TargetID).Err()
		}
	}
	return nil
}

func (s *Service) videoSignal(ctx context.Context, uid, vid string, w, authorW float64, kind string) error {
	ms, err := s.metas(ctx, []string{vid})
	if err != nil {
		return err
	}
	m, ok := ms[vid]
	if !ok {
		return nil
	}
	return s.applySignals(ctx, []signal{{uid: uid, vid: vid, m: m, w: w, authorW: authorW, positive: w > 0, strong: math.Abs(w) >= 2, kind: kind}})
}
