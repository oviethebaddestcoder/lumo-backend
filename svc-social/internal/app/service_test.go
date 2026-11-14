package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/userdir"
	"github.com/oviethebaddestcoder/lumo-backend/svc-social/internal/app"
	"github.com/oviethebaddestcoder/lumo-backend/svc-social/migrations"
)

func setup(t *testing.T) *app.Service {
	pg := os.Getenv("LUMO_TEST_PG")
	if pg == "" {
		t.Skip("set LUMO_TEST_PG")
	}
	ctx := context.Background()
	admin, _ := pgx.Connect(ctx, pg)
	name := fmt.Sprintf("social_test_%d", time.Now().UnixNano())
	admin.Exec(ctx, "CREATE DATABASE "+name)
	t.Cleanup(func() {
		admin.Exec(context.Background(), "DROP DATABASE "+name+" WITH (FORCE)")
		admin.Close(context.Background())
	})
	cfg, _ := pgx.ParseConfig(pg)
	dsn := fmt.Sprintf("postgres://%s@%s:%d/%s", cfg.User, cfg.Host, cfg.Port, name)
	if err := db.Migrate(ctx, dsn, migrations.FS, migrations.LockID); err != nil {
		t.Fatal(err)
	}
	pool, _ := db.Open(ctx, dsn, db.Options{MaxConns: 30})
	t.Cleanup(pool.Close)
	id := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var items []events.UserPublic
		for _, x := range strings.Split(r.URL.Query().Get("ids"), ",") {
			items = append(items, events.UserPublic{ID: x, DisplayName: "n"})
		}
		json.NewEncoder(w).Encode(map[string]any{"items": items})
	}))
	t.Cleanup(id.Close)
	return &app.Service{Pool: pool, Users: userdir.New(id.URL)}
}

func outboxEvents(t *testing.T, s *app.Service) []events.Envelope {
	rows, err := s.Pool.Query(context.Background(), `SELECT payload FROM outbox ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	var out []events.Envelope
	for rows.Next() {
		var e events.Envelope
		rows.Scan(&e)
		out = append(out, e)
	}
	rows.Close()
	return out
}

func TestFollowLikeCountsAndBlocks(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	star := events.NewID()
	video := events.NewID()
	s.OnVideoState(ctx, events.MustNew("m", events.VideoState, video, events.VideoStateData{ID: video, OwnerID: star, Status: "ready"}))

	// 40 fans follow and like concurrently (twice each: idempotent).
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		fan := events.NewID()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 2; k++ {
				if err := s.Follow(ctx, fan, star); err != nil {
					t.Error(err)
				}
				if err := s.Like(ctx, fan, video, true); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	evs := outboxEvents(t, s)
	if len(evs) != 80 {
		t.Fatalf("events = %d, want 80 (40 follows + 40 likes)", len(evs))
	}
	// Counter consumer: deliver everything, then redeliver (at-least-once).
	if err := s.ApplyCounts(ctx, evs); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyCounts(ctx, evs[:30]); err != nil {
		t.Fatal(err)
	}
	c, _ := s.UserCounts(ctx, star)
	vc, _ := s.VideoCounts(ctx, "", []string{video})
	if c.Followers != 40 || vc[0].Likes != 40 || c.LikesReceived != 40 {
		t.Fatalf("followers=%d likes=%d received=%d", c.Followers, vc[0].Likes, c.LikesReceived)
	}
	if n, err := s.FollowersGained(ctx, star, 7); err != nil || n != 40 {
		t.Fatalf("followers gained %d %v", n, err)
	}
	list, next, _ := s.List(ctx, star, "followers", "", 25)
	if len(list) != 25 || next == "" {
		t.Fatalf("page 1: %d %q", len(list), next)
	}
	list2, _, _ := s.List(ctx, star, "followers", next, 25)
	if len(list2) != 15 {
		t.Fatalf("page 2: %d", len(list2))
	}

	// Blocking stops comments and removes the follow.
	troll := events.NewID()
	s.Follow(ctx, troll, star)
	if err := s.Block(ctx, star, troll, true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Comment(ctx, troll, video, "", "hi"); !errors.Is(err, app.ErrBlocked) {
		t.Fatalf("blocked user commented: %v", err)
	}
	if rel, _ := s.Relationship(ctx, troll, star); rel.Following {
		t.Fatal("follow survived block")
	}
	// Comments: author or video owner can delete; others can't.
	fan := events.NewID()
	cm, err := s.Comment(ctx, fan, video, "", "great video")
	if err != nil {
		t.Fatal(err)
	}
	mine, _, err := s.CommentsOnMyVideos(ctx, star, "", 10)
	if err != nil || len(mine) != 1 || mine[0].ID != cm.ID || mine[0].VideoID != video {
		t.Fatalf("comments on my videos %+v %v", mine, err)
	}
	if err := s.DeleteComment(ctx, events.NewID(), cm.ID, false); err == nil {
		t.Fatal("stranger deleted comment")
	}
	if err := s.DeleteComment(ctx, star, cm.ID, false); err != nil {
		t.Fatal(err)
	}
}
