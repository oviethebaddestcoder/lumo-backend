// Package app is svc-feed: the For You, Following and Watch (long video)
// feeds.
//
// The feed owns no database. Its state lives in Redis and is a projection of
// Kafka topics: media.videos (compacted, so a new cluster can replay it from
// the start), social.events (likes, comments, follows, blocks, reports) and
// feed.impressions (what people actually watched). Lose Redis and you
// rebuild it by resetting the consumer group offsets.
//
// Ranking is a two-stage recommender (recommend.go) over per-viewer
// interest profiles (signals.go) with a hand-tuned, A/B-testable scoring
// formula (score.go).
package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/cache"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/kafkax"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/userdir"
	"github.com/redis/go-redis/v9"
)

const (
	seenKeep   = 3000
	authorKeep = 200

	// Impressions a viewer may report per minute; anything above is dropped.
	impressionsPerMinute = 300
	maxWatchMS           = 6 * 3600 * 1000
)

type Service struct {
	R        redis.UniversalClient
	Users    *userdir.Client
	Producer *kafkax.Producer
	Arms     Arms

	SocialURL     string // http://svc-social
	InternalToken string
	HTTP          *http.Client

	once      sync.Once
	metaCache *cache.Cache[meta]
	statCache *cache.Cache[Stats]
	anonCache *cache.Cache[[]ranked]
}

func vKey(id string) string     { return "feed:v:" + id }
func hotKey(kind string) string { return "feed:hot:" + kind }
func newKey(kind string) string { return "feed:new:" + kind }
func authorKey(o string) string { return "feed:author:" + o }
func engKey(id string) string   { return "feed:eng:" + id }
func seenKey(uid string) string { return "feed:seen:" + uid }

// ---- projection (consumers) ----

// OnVideo applies media.videos state snapshots.
func (s *Service) OnVideo(ctx context.Context, e events.Envelope) error {
	s.init()
	if e.Type != events.VideoState {
		return nil
	}
	var v events.VideoStateData
	if err := e.Decode(&v); err != nil || len(v.ID) != 36 {
		return nil
	}
	kind := v.Kind
	if kind == "replay" {
		kind = "long"
	}
	if kind != "short" && kind != "long" {
		return nil
	}
	old, err := s.metas(ctx, []string{v.ID})
	if err != nil {
		return err
	}
	prev, existed := old[v.ID]
	s.metaCache.Delete(v.ID)

	if v.Status != "ready" || v.Visibility != "public" {
		pipe := s.R.Pipeline()
		for _, k := range []string{"short", "long"} {
			pipe.ZRem(ctx, hotKey(k), v.ID)
			pipe.ZRem(ctx, newKey(k), v.ID)
		}
		if existed {
			for _, t := range prev.tags {
				pipe.ZRem(ctx, tagKey(prev.kind, t), v.ID)
			}
		}
		pipe.ZRem(ctx, authorKey(v.OwnerID), v.ID)
		pipe.Del(ctx, vKey(v.ID), coKey(v.ID))
		_, err := pipe.Exec(ctx)
		return err
	}
	created := v.CreatedAt.Unix()
	if v.PublishedAt != nil {
		created = v.PublishedAt.Unix()
	}
	tags := normTags(v.Hashtags)
	st, err := s.stats(ctx, []string{v.ID})
	if err != nil {
		return err
	}
	h := hotScore(st[v.ID], created)
	pipe := s.R.Pipeline()
	if existed {
		// Drop hashtags the creator edited out.
		keep := map[string]bool{}
		for _, t := range tags {
			keep[t] = true
		}
		for _, t := range prev.tags {
			if !keep[t] || prev.kind != kind {
				pipe.ZRem(ctx, tagKey(prev.kind, t), v.ID)
			}
		}
	}
	pipe.HSet(ctx, vKey(v.ID), "owner", v.OwnerID, "kind", kind, "created", created, "hls", v.HLSURL, "thumb", v.ThumbnailURL,
		"title", v.Title, "desc", v.Description, "dur", v.DurationMS, "w", v.Width, "h", v.Height, "tags", strings.Join(tags, ","))
	pipe.ZAdd(ctx, hotKey(kind), redis.Z{Score: h, Member: v.ID})
	pipe.ZAdd(ctx, newKey(kind), redis.Z{Score: float64(created), Member: v.ID})
	for _, t := range tags {
		pipe.ZAdd(ctx, tagKey(kind, t), redis.Z{Score: h, Member: v.ID})
	}
	pipe.ZAdd(ctx, authorKey(v.OwnerID), redis.Z{Score: float64(created), Member: v.ID})
	pipe.ZRemRangeByRank(ctx, authorKey(v.OwnerID), 0, -authorKeep-1)
	_, err = pipe.Exec(ctx)
	return err
}

// OnSocial updates engagement counters, trending, and viewer interest
// (consumer of social.events).
func (s *Service) OnSocial(ctx context.Context, e events.Envelope) error {
	s.init()
	switch e.Type {
	case events.Liked, events.Unliked, events.Commented, events.CommentDeleted,
		events.Followed, events.Unfollowed, events.Blocked, events.Unblocked, events.Reported:
	default:
		return nil
	}
	// Dedupe redeliveries for a day.
	if ok, err := s.R.SetNX(ctx, "feed:dedupe:"+e.ID, 1, 24*time.Hour).Result(); err != nil || !ok {
		return err
	}
	var field, videoID string
	var delta int64 = 1
	switch e.Type {
	case events.Liked, events.Unliked:
		var l events.LikeData
		if e.Decode(&l) == nil {
			videoID, field = l.VideoID, "likes"
		}
		if e.Type == events.Unliked {
			delta = -1
		}
	case events.Commented, events.CommentDeleted:
		var c events.CommentData
		if e.Decode(&c) == nil {
			videoID, field = c.VideoID, "comments"
		}
		if e.Type == events.CommentDeleted {
			delta = -1
		}
	}
	if videoID != "" {
		pipe := s.R.Pipeline()
		pipe.HIncrBy(ctx, engKey(videoID), field, delta)
		pipe.Expire(ctx, engKey(videoID), 30*24*time.Hour)
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
		if err := s.rehot(ctx, []string{videoID}); err != nil {
			return err
		}
	}
	return s.socialSignal(ctx, e)
}

// ---- reads ----

type Item struct {
	ID           string            `json:"id"`
	Kind         string            `json:"kind"`
	Author       events.UserPublic `json:"author"`
	HLSURL       string            `json:"hls_url"`
	ThumbnailURL string            `json:"thumbnail_url"`
	Title        string            `json:"title,omitempty"`
	Description  string            `json:"description,omitempty"`
	DurationMS   int64             `json:"duration_ms"`
	Width        int64             `json:"width"`
	Height       int64             `json:"height"`
	Likes        int64             `json:"likes"`
	Comments     int64             `json:"comments"`
	CreatedAt    int64             `json:"created_at"`
	// Why it was picked (co | follow | author | tag | hot | fresh) and the
	// ranking A/B arm. Clients send both back on the impression.
	Source string `json:"rec_source,omitempty"`
	Arm    string `json:"rec_arm,omitempty"`
}

func (s *Service) markSeen(ctx context.Context, userID string, items []Item) {
	now := float64(time.Now().Unix())
	zs := make([]redis.Z, len(items))
	for i, it := range items {
		zs[i] = redis.Z{Score: now, Member: it.ID}
	}
	pipe := s.R.Pipeline()
	pipe.ZAdd(ctx, seenKey(userID), zs...)
	pipe.ZRemRangeByRank(ctx, seenKey(userID), 0, -seenKeep-1)
	pipe.Expire(ctx, seenKey(userID), 7*24*time.Hour)
	_, _ = pipe.Exec(ctx)
}

func (s *Service) hydrate(ctx context.Context, ids []string, max int) ([]Item, error) {
	if len(ids) > max {
		ids = ids[:max]
	}
	if len(ids) == 0 {
		return []Item{}, nil
	}
	pipe := s.R.Pipeline()
	vs := make([]*redis.MapStringStringCmd, len(ids))
	es := make([]*redis.SliceCmd, len(ids))
	for i, id := range ids {
		vs[i] = pipe.HGetAll(ctx, vKey(id))
		es[i] = pipe.HMGet(ctx, engKey(id), "likes", "comments")
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	items := make([]Item, 0, len(ids))
	owners := make([]string, 0, len(ids))
	for i, id := range ids {
		m := vs[i].Val()
		if len(m) == 0 {
			continue // removed since it was ranked
		}
		e := es[i].Val()
		it := Item{ID: id, Kind: m["kind"], HLSURL: m["hls"], ThumbnailURL: m["thumb"], Title: m["title"], Description: m["desc"],
			Likes: max64(0, toInt(e, 0)), Comments: max64(0, toInt(e, 1))}
		it.DurationMS, _ = strconv.ParseInt(m["dur"], 10, 64)
		it.Width, _ = strconv.ParseInt(m["w"], 10, 64)
		it.Height, _ = strconv.ParseInt(m["h"], 10, 64)
		it.CreatedAt, _ = strconv.ParseInt(m["created"], 10, 64)
		it.Author.ID = m["owner"]
		owners = append(owners, m["owner"])
		items = append(items, it)
	}
	users, _ := s.Users.Many(ctx, owners)
	out := items[:0]
	for _, it := range items {
		if u, ok := users[it.Author.ID]; ok {
			if u.Status != "" && u.Status != "active" {
				continue // suspended or deleted creator
			}
			it.Author = u
		}
		out = append(out, it)
	}
	return out, nil
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// Following returns recent videos from accounts the viewer follows
// (fan-out on read: no per-follower inbox to write for big creators).
func (s *Service) Following(ctx context.Context, userID string, before int64, limit int) ([]Item, int64, error) {
	s.init()
	if limit <= 0 || limit > 20 {
		limit = 10
	}
	if before <= 0 {
		before = math.MaxInt64
	}
	ids, err := s.followingIDs(ctx, userID)
	if err != nil {
		return nil, 0, err
	}
	if len(ids) > 2000 {
		ids = ids[:2000] // bound the fan-out
	}
	pipe := s.R.Pipeline()
	cmds := make([]*redis.ZSliceCmd, len(ids))
	for i, a := range ids {
		cmds[i] = pipe.ZRevRangeByScoreWithScores(ctx, authorKey(a), &redis.ZRangeBy{Max: "(" + strconv.FormatInt(before, 10), Min: "-inf", Count: int64(limit)})
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, 0, err
	}
	var all []redis.Z
	for _, c := range cmds {
		all = append(all, c.Val()...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Score > all[j].Score })
	if len(all) > limit {
		all = all[:limit]
	}
	vids := make([]string, len(all))
	for i, z := range all {
		vids[i] = z.Member.(string)
	}
	items, err := s.hydrate(ctx, vids, limit)
	var next int64
	if len(all) == limit {
		next = int64(all[len(all)-1].Score)
	}
	return items, next, err
}

// followingIDs returns who the viewer follows. The list is cached in Redis
// (shared by every pod) and the feed worker drops it the moment a follow or
// unfollow event arrives, so a new follow shows up in Following right away.
// A version number guards the race where a slow read of the old list would
// otherwise be cached after the drop.
func (s *Service) followingIDs(ctx context.Context, userID string) ([]string, error) {
	s.init()
	ck, vk := uKey(userID, "f"), uKey(userID, "fv")
	pipe := s.R.Pipeline()
	cached := pipe.Get(ctx, ck)
	ver := pipe.Get(ctx, vk)
	_, _ = pipe.Exec(ctx)
	if raw, err := cached.Bytes(); err == nil {
		var ids []string
		if json.Unmarshal(raw, &ids) == nil {
			return ids, nil
		}
	}
	v, _ := ver.Int64()
	if s.SocialURL == "" {
		return nil, nil
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, s.SocialURL+"/internal/social/users/"+userID+"/following-ids", nil)
	req.Header.Set("X-Internal-Token", s.InternalToken)
	resp, err := s.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("social following: status %d", resp.StatusCode)
	}
	var body struct {
		IDs []string `json:"ids"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&body); err != nil {
		return nil, fmt.Errorf("social following: %w", err)
	}
	if raw, err := json.Marshal(body.IDs); err == nil {
		_ = setIfVersion.Run(ctx, s.R, []string{ck, vk}, raw, int(followingTTL.Seconds()), v).Err()
	}
	return body.IDs, nil
}

const followingTTL = 5 * time.Minute

// setIfVersion caches the list only if no follow change happened since it
// was read (KEYS: cache, version; ARGV: value, ttl, version seen).
var setIfVersion = redis.NewScript(`
local v = tonumber(redis.call('GET', KEYS[2]) or '0')
if v == tonumber(ARGV[3]) then redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2]) return 1 end
return 0`)

// dropFollowing invalidates a viewer's cached follow list (worker side).
func dropFollowing(ctx context.Context, pipe redis.Pipeliner, userID string) {
	pipe.Incr(ctx, uKey(userID, "fv"))
	pipe.Expire(ctx, uKey(userID, "fv"), 24*time.Hour)
	pipe.Del(ctx, uKey(userID, "f"))
}

// Impressions records what the viewer watched: marks it seen and hands it
// to the signal pipeline (feed.impressions, keyed by viewer so one
// viewer's signals stay in order). Over-limit reports are dropped quietly.
func (s *Service) Impressions(ctx context.Context, userID string, items []events.ImpressionData) {
	if len(items) == 0 || len(items) > 50 {
		return
	}
	rl := fmt.Sprintf("feed:rl:imp:%s:%d", userID, time.Now().Unix()/60)
	n, err := s.R.IncrBy(ctx, rl, int64(len(items))).Result()
	if err == nil && n == int64(len(items)) {
		s.R.Expire(ctx, rl, 2*time.Minute)
	}
	if err == nil && n > impressionsPerMinute {
		return
	}
	now := time.Now().UTC()
	zs := make([]redis.Z, 0, len(items))
	for _, it := range items {
		if len(it.VideoID) != 36 {
			continue
		}
		it.WatchMS = min(max(it.WatchMS, 0), maxWatchMS)
		it.Loops = min(max(it.Loops, 0), 50)
		it.Source, it.Arm = label(it.Source), label(it.Arm)
		if len(it.Surface) > 16 {
			it.Surface = it.Surface[:16]
		}
		zs = append(zs, redis.Z{Score: float64(now.Unix()), Member: it.VideoID})
		if s.Producer != nil {
			it.UserID, it.At = userID, now
			s.Producer.PublishAsync(ctx, events.TopicFeedSignals, events.MustNew("svc-feed", events.Impression, userID, it))
		}
	}
	if len(zs) > 0 {
		s.R.ZAdd(ctx, seenKey(userID), zs...)
	}
}

// Trim keeps the global ranking sets bounded (worker cron).
func (s *Service) Trim(ctx context.Context) {
	for _, k := range []string{"short", "long"} {
		s.R.ZRemRangeByRank(ctx, hotKey(k), 0, -200_001)
		s.R.ZRemRangeByRank(ctx, newKey(k), 0, -50_001)
	}
}

func toInt(v []any, i int) int64 {
	if i >= len(v) || v[i] == nil {
		return 0
	}
	n, _ := strconv.ParseInt(strings.TrimSpace(fmt.Sprint(v[i])), 10, 64)
	return n
}
