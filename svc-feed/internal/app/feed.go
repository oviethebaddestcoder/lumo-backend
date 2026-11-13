// Package app is svc-feed: the For You, Following and Watch (long video)
// feeds.
//
// The feed owns no database. Its state lives in Redis and is a projection of
// Kafka topics: media.videos (compacted, so a new cluster can replay it from
// the start) and social.events (engagement). Lose Redis and you rebuild it
// by resetting the consumer group offsets.
//
// Ranking v1 is "hot" ranking (engagement with time decay), plus a slice of
// fresh uploads so new creators get seen, per-user de-duplication and author
// diversity. Swap rank() for a learned model later; the interface stays.
package app

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
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
	hotWindow   = 400
	freshWindow = 100
	seenKeep    = 3000
	authorKeep  = 200
)

type Service struct {
	R        redis.UniversalClient
	Users    *userdir.Client
	Producer *kafkax.Producer

	SocialURL     string // http://svc-social
	InternalToken string
	HTTP          *http.Client
	following     *cache.Cache[[]string]
	once          sync.Once
}

func vKey(id string) string     { return "feed:v:" + id }
func hotKey(kind string) string { return "feed:hot:" + kind }
func newKey(kind string) string { return "feed:new:" + kind }
func authorKey(o string) string { return "feed:author:" + o }
func engKey(id string) string   { return "feed:eng:" + id }
func seenKey(uid string) string { return "feed:seen:" + uid }

// hotScore: every 10x engagement is worth 12.5 hours of recency.
func hotScore(likes, comments, created int64) float64 {
	return math.Log10(math.Max(1, float64(likes+3*comments))) + float64(created)/45000
}

// ---- projection (consumers) ----

// OnVideo applies media.videos state snapshots.
func (s *Service) OnVideo(ctx context.Context, e events.Envelope) error {
	if e.Type != events.VideoState {
		return nil
	}
	var v events.VideoStateData
	if err := e.Decode(&v); err != nil {
		return nil
	}
	kind := v.Kind
	if kind == "replay" {
		kind = "long"
	}
	if v.Status != "ready" || v.Visibility != "public" {
		pipe := s.R.TxPipeline()
		for _, k := range []string{"short", "long"} {
			pipe.ZRem(ctx, hotKey(k), v.ID)
			pipe.ZRem(ctx, newKey(k), v.ID)
		}
		pipe.ZRem(ctx, authorKey(v.OwnerID), v.ID)
		pipe.Del(ctx, vKey(v.ID))
		_, err := pipe.Exec(ctx)
		return err
	}
	created := v.CreatedAt.Unix()
	if v.PublishedAt != nil {
		created = v.PublishedAt.Unix()
	}
	eng, _ := s.R.HMGet(ctx, engKey(v.ID), "likes", "comments").Result()
	likes, comments := toInt(eng, 0), toInt(eng, 1)
	pipe := s.R.TxPipeline()
	pipe.HSet(ctx, vKey(v.ID), "owner", v.OwnerID, "kind", kind, "created", created, "hls", v.HLSURL, "thumb", v.ThumbnailURL,
		"title", v.Title, "desc", v.Description, "dur", v.DurationMS, "w", v.Width, "h", v.Height)
	pipe.ZAdd(ctx, hotKey(kind), redis.Z{Score: hotScore(likes, comments, created), Member: v.ID})
	pipe.ZAdd(ctx, newKey(kind), redis.Z{Score: float64(created), Member: v.ID})
	pipe.ZAdd(ctx, authorKey(v.OwnerID), redis.Z{Score: float64(created), Member: v.ID})
	pipe.ZRemRangeByRank(ctx, authorKey(v.OwnerID), 0, -authorKeep-1)
	_, err := pipe.Exec(ctx)
	return err
}

// OnSocial updates engagement and re-ranks (consumer of social.events).
func (s *Service) OnSocial(ctx context.Context, e events.Envelope) error {
	var field string
	var delta int64 = 1
	var videoID string
	switch e.Type {
	case events.Liked, events.Unliked:
		var l events.LikeData
		if e.Decode(&l) != nil {
			return nil
		}
		videoID, field = l.VideoID, "likes"
		if e.Type == events.Unliked {
			delta = -1
		}
	case events.Commented, events.CommentDeleted:
		var c events.CommentData
		if e.Decode(&c) != nil {
			return nil
		}
		videoID, field = c.VideoID, "comments"
		if e.Type == events.CommentDeleted {
			delta = -1
		}
	default:
		return nil
	}
	// Dedupe redeliveries for a day.
	if ok, err := s.R.SetNX(ctx, "feed:dedupe:"+e.ID, 1, 24*time.Hour).Result(); err != nil || !ok {
		return err
	}
	pipe := s.R.TxPipeline()
	pipe.HIncrBy(ctx, engKey(videoID), field, delta)
	pipe.Expire(ctx, engKey(videoID), 30*24*time.Hour)
	pipe.HMGet(ctx, vKey(videoID), "kind", "created")
	cmds, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}
	meta := cmds[2].(*redis.SliceCmd).Val()
	if meta[0] == nil {
		return nil // not in the feed (private, removed, or not ready)
	}
	eng, _ := s.R.HMGet(ctx, engKey(videoID), "likes", "comments").Result()
	return s.R.ZAddXX(ctx, hotKey(fmt.Sprint(meta[0])), redis.Z{Score: hotScore(toInt(eng, 0), toInt(eng, 1), toInt(meta, 1)), Member: videoID}).Err()
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
}

// ForYou returns the next page for a viewer (anonymous viewers get the
// global ranking without de-duplication).
func (s *Service) ForYou(ctx context.Context, userID, kind string, limit int) ([]Item, error) {
	if kind != "long" {
		kind = "short"
	}
	if limit <= 0 || limit > 20 {
		limit = 10
	}
	hot, err := s.R.ZRevRange(ctx, hotKey(kind), 0, hotWindow-1).Result()
	if err != nil {
		return nil, err
	}
	fresh, err := s.R.ZRevRange(ctx, newKey(kind), 0, freshWindow-1).Result()
	if err != nil {
		return nil, err
	}
	cands := rank(hot, fresh)
	if userID != "" {
		cands, err = s.unseen(ctx, userID, cands)
		if err != nil {
			return nil, err
		}
	}
	items, err := s.hydrate(ctx, cands, limit*3)
	if err != nil {
		return nil, err
	}
	// Author diversity: at most 2 videos per creator per page.
	perAuthor := map[string]int{}
	out := make([]Item, 0, limit)
	for _, it := range items {
		if perAuthor[it.Author.ID] >= 2 {
			continue
		}
		perAuthor[it.Author.ID]++
		out = append(out, it)
		if len(out) == limit {
			break
		}
	}
	if userID != "" && len(out) > 0 {
		s.markSeen(ctx, userID, out)
	}
	return out, nil
}

// rank blends hot and fresh: hot items keep their order with jitter (so two
// refreshes differ), and one in four slots goes to a fresh upload.
func rank(hot, fresh []string) []string {
	type c struct {
		id    string
		score float64
	}
	seen := map[string]bool{}
	var hs []c
	for i, id := range hot {
		seen[id] = true
		hs = append(hs, c{id, float64(i) + rand.Float64()*15})
	}
	sort.Slice(hs, func(i, j int) bool { return hs[i].score < hs[j].score })
	var fs []string
	for _, id := range fresh {
		if !seen[id] {
			fs = append(fs, id)
		}
	}
	rand.Shuffle(len(fs), func(i, j int) { fs[i], fs[j] = fs[j], fs[i] })
	out := make([]string, 0, len(hs)+len(fs))
	hi, fi := 0, 0
	for hi < len(hs) || fi < len(fs) {
		if (len(out)%4 == 3 && fi < len(fs)) || hi >= len(hs) {
			out = append(out, fs[fi])
			fi++
		} else {
			out = append(out, hs[hi].id)
			hi++
		}
	}
	return out
}

func (s *Service) unseen(ctx context.Context, userID string, ids []string) ([]string, error) {
	if len(ids) == 0 {
		return ids, nil
	}
	scores, err := s.R.ZMScore(ctx, seenKey(userID), ids...).Result()
	if err != nil {
		return nil, err
	}
	out := ids[:0:0]
	for i, id := range ids {
		if scores[i] == 0 {
			out = append(out, id)
		}
	}
	return out, nil
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
			continue
		}
		e := es[i].Val()
		it := Item{ID: id, Kind: m["kind"], HLSURL: m["hls"], ThumbnailURL: m["thumb"], Title: m["title"], Description: m["desc"],
			Likes: toInt(e, 0), Comments: toInt(e, 1)}
		it.DurationMS, _ = strconv.ParseInt(m["dur"], 10, 64)
		it.Width, _ = strconv.ParseInt(m["w"], 10, 64)
		it.Height, _ = strconv.ParseInt(m["h"], 10, 64)
		it.CreatedAt, _ = strconv.ParseInt(m["created"], 10, 64)
		it.Author.ID = m["owner"]
		owners = append(owners, m["owner"])
		items = append(items, it)
	}
	users, _ := s.Users.Many(ctx, owners)
	for i := range items {
		if u, ok := users[items[i].Author.ID]; ok {
			items[i].Author = u
		}
	}
	return items, nil
}

// Following returns recent videos from accounts the viewer follows
// (fan-out on read: no per-follower inbox to write for big creators).
func (s *Service) Following(ctx context.Context, userID string, before int64, limit int) ([]Item, int64, error) {
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

func (s *Service) followingIDs(ctx context.Context, userID string) ([]string, error) {
	s.once.Do(func() { s.following = cache.New[[]string](2*time.Minute, 200_000) })
	if ids, ok := s.following.Get(userID); ok {
		return ids, nil
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, s.SocialURL+"/internal/social/users/"+userID+"/following-ids", nil)
	req.Header.Set("X-Internal-Token", s.InternalToken)
	resp, err := s.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var body struct {
		IDs []string `json:"ids"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("social following: status %d: %v", resp.StatusCode, err)
	}
	s.following.Set(userID, body.IDs)
	return body.IDs, nil
}

// Impressions records what the viewer actually watched (dedupe + analytics).
func (s *Service) Impressions(ctx context.Context, userID string, items []events.ImpressionData) {
	if len(items) == 0 || len(items) > 50 {
		return
	}
	now := time.Now().UTC()
	zs := make([]redis.Z, 0, len(items))
	for _, it := range items {
		if len(it.VideoID) != 36 {
			continue
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

// Trim keeps the ranking sets bounded (worker cron).
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
