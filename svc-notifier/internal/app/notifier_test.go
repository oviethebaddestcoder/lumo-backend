package app_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/redisx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/userdir"
	"github.com/oviethebaddestcoder/lumo-backend/svc-notifier/internal/app"
	"github.com/oviethebaddestcoder/lumo-backend/svc-notifier/migrations"
)

type fakeSender struct {
	mu   sync.Mutex
	sent map[string]int
}

func (f *fakeSender) Send(_ context.Context, token string, _ app.Push) error {
	if strings.HasPrefix(token, "dead") {
		return app.ErrBadToken
	}
	f.mu.Lock()
	f.sent[token]++
	f.mu.Unlock()
	return nil
}

// A creator with 2,500 followers goes live: every follower with a device and
// live notifications on gets exactly one push, even if the event repeats.
func TestLiveFanout(t *testing.T) {
	pg, rurl := os.Getenv("LUMO_TEST_PG"), os.Getenv("LUMO_TEST_REDIS")
	if pg == "" || rurl == "" {
		t.Skip("set LUMO_TEST_PG and LUMO_TEST_REDIS")
	}
	ctx := context.Background()
	admin, _ := pgx.Connect(ctx, pg)
	name := fmt.Sprintf("notifier_test_%d", time.Now().UnixNano())
	admin.Exec(ctx, "CREATE DATABASE "+name)
	defer func() {
		admin.Exec(context.Background(), "DROP DATABASE "+name+" WITH (FORCE)")
		admin.Close(context.Background())
	}()
	cfg, _ := pgx.ParseConfig(pg)
	dsn := fmt.Sprintf("postgres://%s@%s:%d/%s", cfg.User, cfg.Host, cfg.Port, name)
	if err := db.Migrate(ctx, dsn, migrations.FS, migrations.LockID); err != nil {
		t.Fatal(err)
	}
	pool, _ := db.Open(ctx, dsn, db.Options{MaxConns: 20})
	defer pool.Close()
	rdb, _ := redisx.Open(ctx, rurl, "", "")
	rdb.FlushDB(ctx)

	host := events.NewID()
	followers := make([]string, 2500)
	for i := range followers {
		followers[i] = events.NewID()
	}
	sort.Strings(followers)
	social := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		after := r.URL.Query().Get("after")
		i := sort.SearchStrings(followers, after)
		if i < len(followers) && followers[i] == after {
			i++
		}
		end := min(i+1000, len(followers))
		json.NewEncoder(w).Encode(map[string]any{"ids": followers[i:end]})
	}))
	defer social.Close()
	ident := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"items": []events.UserPublic{{ID: host, DisplayName: "Kemi"}}})
	}))
	defer ident.Close()

	snd := &fakeSender{sent: map[string]int{}}
	s := &app.Service{Pool: pool, Redis: rdb, Users: userdir.New(ident.URL), Sender: snd, SocialURL: social.URL,
		HTTP: http.DefaultClient, FanoutWorkers: 32}
	for i, f := range followers {
		tok := fmt.Sprintf("fcm-token-%04d-%s", i, f)
		if i == 7 {
			tok = "dead-token-xxxxxxxxxxxxxxx"
		}
		if err := s.Register(ctx, f, tok, "android"); err != nil {
			t.Fatal(err)
		}
	}
	s.SetPrefs(ctx, followers[3], app.Prefs{NewFollower: true, Comments: true, Likes: true, Live: false})

	ev := events.MustNew("svc-live", events.RoomStarted, "room", events.RoomStartedData{RoomID: "room", HostID: host, Title: "Karaoke", StartedAt: time.Now()})
	if err := s.OnLive(ctx, ev); err != nil {
		t.Fatal(err)
	}
	if err := s.OnLive(ctx, ev); err != nil { // redelivery
		t.Fatal(err)
	}
	total := 0
	for _, n := range snd.sent {
		if n != 1 {
			t.Fatal("a device got more than one push")
		}
		total += n
	}
	if total != 2498 { // 2500 - opted out - dead token
		t.Fatalf("sent %d pushes, want 2498", total)
	}
	var dead int
	pool.QueryRow(ctx, `SELECT count(*) FROM devices WHERE token LIKE 'dead%'`).Scan(&dead)
	if dead != 0 {
		t.Fatal("dead token not removed")
	}
}
