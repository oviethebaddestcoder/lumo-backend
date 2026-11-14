package app

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/redisx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/userdir"
)

// TestRecommenderLearns runs the whole loop against Redis: catalogue in,
// watch signals in, and the For You feed shifting to what the viewer
// actually watches.
func TestRecommenderLearns(t *testing.T) {
	rurl := os.Getenv("LUMO_TEST_REDIS")
	if rurl == "" {
		t.Skip("set LUMO_TEST_REDIS")
	}
	ctx := context.Background()
	rdb, _ := redisx.Open(ctx, rurl, "", "")
	rdb.FlushDB(ctx)
	id := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var items []events.UserPublic
		for _, x := range strings.Split(r.URL.Query().Get("ids"), ",") {
			items = append(items, events.UserPublic{ID: x, Handle: "h", Status: "active"})
		}
		json.NewEncoder(w).Encode(map[string]any{"items": items})
	}))
	defer id.Close()
	social := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"ids": []string{}})
	}))
	defer social.Close()
	s := &Service{R: rdb, Users: userdir.New(id.URL), SocialURL: social.URL, HTTP: http.DefaultClient}

	// 60 shorts: 30 #cooking from 6 cooks, 30 #football from 6 pundits.
	now := time.Now().UTC()
	topic := map[string]string{}
	owner := map[string]string{}
	var cooking, football []string
	var cooks, pundits []string
	for i := 0; i < 6; i++ {
		cooks, pundits = append(cooks, events.NewID()), append(pundits, events.NewID())
	}
	for i := 0; i < 60; i++ {
		v := events.NewID()
		tag, o := "cooking", cooks[(i/2)%6]
		if i%2 == 1 {
			tag, o = "football", pundits[(i/2)%6]
			football = append(football, v)
		} else {
			cooking = append(cooking, v)
		}
		topic[v], owner[v] = tag, o
		if err := s.OnVideo(ctx, events.MustNew("m", events.VideoState, v, events.VideoStateData{ID: v, OwnerID: o, Kind: "short", Status: "ready",
			Visibility: "public", Hashtags: []string{"#" + strings.ToUpper(tag[:1]) + tag[1:], "lagos"}, DurationMS: 10_000, HLSURL: "https://cdn/" + v,
			CreatedAt: now.Add(-time.Duration(i) * time.Minute)})); err != nil {
			t.Fatal(err)
		}
	}
	if n, _ := rdb.ZCard(ctx, tagKey("short", "cooking")).Result(); n != 30 {
		t.Fatalf("cooking index has %d", n)
	}
	watch := func(uid, vid string, ms int64) events.Envelope {
		return events.MustNew("svc-feed", events.Impression, uid, events.ImpressionData{UserID: uid, VideoID: vid, WatchMS: ms, At: now})
	}

	viewer := events.NewID()
	// Before any signal, a new viewer still gets a full page.
	first, err := s.ForYou(ctx, viewer, "short", 10)
	if err != nil || len(first) != 10 {
		t.Fatalf("cold start: %v %d", err, len(first))
	}
	// They finish every cooking video and swipe past football at once.
	var batch []events.Envelope
	for _, it := range first {
		ms := int64(10_000)
		if topic[it.ID] == "football" {
			ms = 400
		}
		batch = append(batch, watch(viewer, it.ID, ms))
	}
	if err := s.OnImpressions(ctx, batch); err != nil {
		t.Fatal(err)
	}
	// Redelivery of the same batch changes nothing.
	before, _ := rdb.HGet(ctx, engKey(first[0].ID), "imp").Int()
	if err := s.OnImpressions(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if after, _ := rdb.HGet(ctx, engKey(first[0].ID), "imp").Int(); after != before || before != 1 {
		t.Fatalf("impressions not deduped: %d -> %d", before, after)
	}
	ints, err := s.Interests(ctx, viewer)
	if err != nil || len(ints) == 0 || ints[0].Tag != "cooking" {
		t.Fatalf("interests: %v %+v", err, ints)
	}
	for _, in := range ints {
		if in.Tag == "football" {
			t.Fatal("skipped topic listed as an interest")
		}
	}

	// The next pages lean hard into cooking, with discovery slots only.
	s.R.Del(ctx, queueKey(viewer, "short"))
	cook, total := 0, 0
	for p := 0; p < 2; p++ {
		items, err := s.ForYou(ctx, viewer, "short", 10)
		if err != nil {
			t.Fatal(err)
		}
		for i, it := range items {
			total++
			if topic[it.ID] == "cooking" {
				cook++
			} else if i%5 != 4 {
				t.Errorf("page %d slot %d: football outside a discovery slot (source %s)", p, i, it.Source)
			}
			if it.Source == "" || it.Arm != "a" {
				t.Fatalf("missing rec metadata: %+v", it)
			}
		}
	}
	if total < 15 || float64(cook)/float64(total) < 0.75 {
		t.Fatalf("cooking %d of %d after learning", cook, total)
	}

	// "Not interested" on a cook hides that cook.
	banned := cooks[0]
	var vid string
	for _, v := range cooking {
		if owner[v] == banned {
			vid = v
			break
		}
	}
	if err := s.NotInterested(ctx, viewer, vid); err != nil {
		t.Fatal(err)
	}
	// Blocking hides a creator both ways.
	blocked := cooks[1]
	if err := s.OnSocial(ctx, events.MustNew("s", events.Blocked, blocked, events.BlockData{BlockerID: viewer, BlockedID: blocked})); err != nil {
		t.Fatal(err)
	}
	if err := s.R.Del(ctx, seenKey(viewer)).Err(); err != nil { // let everything be eligible again
		t.Fatal(err)
	}
	for p := 0; p < 3; p++ {
		items, _ := s.ForYou(ctx, viewer, "short", 10)
		for _, it := range items {
			if it.Author.ID == banned {
				t.Fatal("creator marked not interested still shown")
			}
			if it.Author.ID == blocked {
				t.Fatal("blocked creator shown")
			}
		}
	}

	// Co-engagement: five people who finished X also finished Y. A new viewer
	// who finishes X gets Y, tagged as "co".
	x, y := cooking[20], football[20]
	var co []events.Envelope
	for i := 0; i < 5; i++ {
		u := events.NewID()
		co = append(co, watch(u, x, 10_000))
		if err := s.OnImpressions(ctx, co[len(co)-1:]); err != nil {
			t.Fatal(err)
		}
		if err := s.OnImpressions(ctx, []events.Envelope{watch(u, y, 10_000)}); err != nil {
			t.Fatal(err)
		}
	}
	if sc, _ := rdb.ZScore(ctx, coKey(x), y).Result(); sc != 5 {
		t.Fatalf("co(x,y) = %v, want 5", sc)
	}
	fan := events.NewID()
	if err := s.OnImpressions(ctx, []events.Envelope{watch(fan, x, 10_000)}); err != nil {
		t.Fatal(err)
	}
	found := false
	for p := 0; p < 3 && !found; p++ {
		items, _ := s.ForYou(ctx, fan, "short", 10)
		for _, it := range items {
			if it.ID == y {
				found = true
				if it.Source != "co" {
					t.Fatalf("Y served via %q, want co", it.Source)
				}
			}
			if it.ID == x {
				t.Fatal("already-watched seed served again")
			}
		}
	}
	if !found {
		t.Fatal("co-engaged video never recommended")
	}

	// Reset forgets interests.
	if err := s.Reset(ctx, viewer); err != nil {
		t.Fatal(err)
	}
	if ints, _ := s.Interests(ctx, viewer); len(ints) != 0 {
		t.Fatalf("interests after reset: %+v", ints)
	}

	// Signed-out viewers get pages too, and nothing is recorded for them.
	anon, err := s.ForYou(ctx, "", "short", 10)
	if err != nil || len(anon) != 10 {
		t.Fatalf("anonymous: %v %d", err, len(anon))
	}

	// Impression reports are rate limited per viewer.
	spammer := events.NewID()
	for i := 0; i < 8; i++ {
		var items []events.ImpressionData
		for j := 0; j < 50; j++ {
			items = append(items, events.ImpressionData{VideoID: events.NewID(), WatchMS: 1})
		}
		s.Impressions(ctx, spammer, items)
	}
	if n, _ := rdb.ZCard(ctx, seenKey(spammer)).Result(); n != impressionsPerMinute {
		t.Fatalf("spammer recorded %d impressions, want cap %d", n, impressionsPerMinute)
	}

	// Active creators hydrate normally (suspended ones are dropped in hydrate).
	if items, _ := s.hydrate(ctx, []string{cooking[0]}, 1); len(items) != 1 {
		t.Fatal("active creator dropped")
	}
}

// A follow must show up in Following at once, not when a cache expires.
func TestFollowInvalidatesCachedFollowList(t *testing.T) {
	rurl := os.Getenv("LUMO_TEST_REDIS")
	if rurl == "" {
		t.Skip("set LUMO_TEST_REDIS")
	}
	ctx := context.Background()
	rdb, _ := redisx.Open(ctx, rurl, "", "")
	rdb.FlushDB(ctx)
	var follows []string
	social := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"ids": follows})
	}))
	defer social.Close()
	s := &Service{R: rdb, SocialURL: social.URL, HTTP: http.DefaultClient}
	viewer, creator := events.NewID(), events.NewID()
	if ids, _ := s.followingIDs(ctx, viewer); len(ids) != 0 {
		t.Fatal("expected no follows")
	}
	follows = []string{creator} // the follow happens in svc-social...
	if ids, _ := s.followingIDs(ctx, viewer); len(ids) != 0 {
		t.Fatal("cache should still answer before the event arrives")
	}
	// ...and its event reaches the feed worker.
	if err := s.OnSocial(ctx, events.MustNew("s", events.Followed, creator, events.FollowData{FollowerID: viewer, FolloweeID: creator})); err != nil {
		t.Fatal(err)
	}
	if ids, _ := s.followingIDs(ctx, viewer); len(ids) != 1 || ids[0] != creator {
		t.Fatalf("after follow event: %v", ids)
	}
	// A read that started before a change must not cache the old list.
	v, _ := rdb.Get(ctx, uKey(viewer, "fv")).Int64()
	rdb.Incr(ctx, uKey(viewer, "fv"))
	rdb.Del(ctx, uKey(viewer, "f"))
	if n, _ := setIfVersion.Run(ctx, rdb, []string{uKey(viewer, "f"), uKey(viewer, "fv")}, `["stale"]`, 300, v).Int(); n != 0 {
		t.Fatal("stale list cached after a newer change")
	}
}

// A viewer who has seen the whole catalogue still gets a feed (oldest-seen
// videos come back as "repeat"), never the ones they just watched.
func TestFeedNeverRunsDry(t *testing.T) {
	rurl := os.Getenv("LUMO_TEST_REDIS")
	if rurl == "" {
		t.Skip("set LUMO_TEST_REDIS")
	}
	ctx := context.Background()
	rdb, _ := redisx.Open(ctx, rurl, "", "")
	rdb.FlushDB(ctx)
	id := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var items []events.UserPublic
		for _, x := range strings.Split(r.URL.Query().Get("ids"), ",") {
			items = append(items, events.UserPublic{ID: x, Status: "active"})
		}
		json.NewEncoder(w).Encode(map[string]any{"items": items})
	}))
	defer id.Close()
	s := &Service{R: rdb, Users: userdir.New(id.URL)}
	now := time.Now()
	for i := 0; i < 9; i++ {
		v := events.NewID()
		s.OnVideo(ctx, events.MustNew("m", events.VideoState, v, events.VideoStateData{ID: v, OwnerID: events.NewID(), Kind: "short", Status: "ready",
			Visibility: "public", Hashtags: []string{"cooking"}, DurationMS: 10_000, CreatedAt: now.Add(-time.Duration(i) * time.Minute)}))
	}
	viewer := events.NewID()
	var lastPage []Item
	seen := map[string]bool{}
	for p := 0; p < 3; p++ {
		items, err := s.ForYou(ctx, viewer, "short", 3)
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range items {
			if seen[it.ID] {
				t.Fatalf("repeat before the catalogue ran out: %s", it.ID)
			}
			seen[it.ID] = true
		}
		lastPage = items
	}
	if len(seen) != 9 {
		t.Fatalf("saw %d of 9", len(seen))
	}
	rdb.Del(ctx, queueKey(viewer, "short"))
	items, err := s.ForYou(ctx, viewer, "short", 3)
	if err != nil || len(items) == 0 {
		t.Fatalf("feed ran dry: %v %d", err, len(items))
	}
	for _, it := range items {
		if it.Source != "repeat" {
			t.Fatalf("expected repeats, got source %q", it.Source)
		}
		for _, l := range lastPage {
			if it.ID == l.ID {
				t.Fatal("repeated a video from the page just watched")
			}
		}
	}
}
