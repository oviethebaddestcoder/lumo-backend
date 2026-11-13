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

func TestFeed(t *testing.T) {
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
			items = append(items, events.UserPublic{ID: x, Handle: "h"})
		}
		json.NewEncoder(w).Encode(map[string]any{"items": items})
	}))
	defer id.Close()
	creatorA, creatorB := events.NewID(), events.NewID()
	social := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"ids": []string{creatorB}})
	}))
	defer social.Close()
	s := &Service{R: rdb, Users: userdir.New(id.URL), SocialURL: social.URL, HTTP: http.DefaultClient}

	now := time.Now().UTC()
	var aVideos, bVideos []string
	for i := 0; i < 6; i++ {
		for _, c := range []struct {
			owner string
			list  *[]string
		}{{creatorA, &aVideos}, {creatorB, &bVideos}} {
			v := events.NewID()
			*c.list = append(*c.list, v)
			s.OnVideo(ctx, events.MustNew("m", events.VideoState, v, events.VideoStateData{ID: v, OwnerID: c.owner, Kind: "short",
				Status: "ready", Visibility: "public", HLSURL: "https://cdn/" + v, CreatedAt: now.Add(time.Duration(i) * time.Minute)}))
		}
	}
	// A private video never enters the feed.
	priv := events.NewID()
	s.OnVideo(ctx, events.MustNew("m", events.VideoState, priv, events.VideoStateData{ID: priv, OwnerID: creatorA, Kind: "short", Status: "ready", Visibility: "private", CreatedAt: now}))
	// Engagement lifts A's first video to the top of the hot ranking.
	for i := 0; i < 50; i++ {
		s.OnSocial(ctx, events.MustNew("s", events.Liked, aVideos[0], events.LikeData{VideoID: aVideos[0], UserID: events.NewID()}))
	}
	top, _ := rdb.ZRevRange(ctx, hotKey("short"), 0, 0).Result()
	if top[0] != aVideos[0] {
		t.Fatalf("liked video not on top")
	}

	viewer := events.NewID()
	seen := map[string]bool{}
	for page := 0; page < 3; page++ {
		items, err := s.ForYou(ctx, viewer, "short", 4)
		if err != nil {
			t.Fatal(err)
		}
		per := map[string]int{}
		for _, it := range items {
			if seen[it.ID] {
				t.Fatalf("video %s shown twice", it.ID)
			}
			if it.ID == priv {
				t.Fatal("private video in feed")
			}
			seen[it.ID] = true
			per[it.Author.ID]++
			if per[it.Author.ID] > 2 {
				t.Fatal("more than 2 videos from one creator on a page")
			}
		}
	}
	if len(seen) != 12 {
		t.Fatalf("saw %d distinct videos over 3 pages, want all 12", len(seen))
	}
	// Removing a video takes it out immediately.
	s.OnVideo(ctx, events.MustNew("m", events.VideoState, aVideos[0], events.VideoStateData{ID: aVideos[0], OwnerID: creatorA, Kind: "short", Status: "removed"}))
	if n, _ := rdb.Exists(ctx, vKey(aVideos[0])).Result(); n != 0 {
		t.Fatal("removed video still cached")
	}
	// Following feed: only creator B, newest first, paginated.
	f1, next, err := s.Following(ctx, viewer, 0, 4)
	if err != nil || len(f1) != 4 || next == 0 {
		t.Fatalf("following p1: %v %d", err, len(f1))
	}
	for _, it := range f1 {
		if it.Author.ID != creatorB {
			t.Fatal("following feed has unfollowed creator")
		}
	}
	f2, _, _ := s.Following(ctx, viewer, next, 4)
	if len(f2) != 2 {
		t.Fatalf("following p2: %d", len(f2))
	}
}
