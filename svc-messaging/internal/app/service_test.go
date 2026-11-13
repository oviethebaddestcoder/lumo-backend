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
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/realtime"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/redisx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/userdir"
	"github.com/oviethebaddestcoder/lumo-backend/svc-messaging/internal/app"
	"github.com/oviethebaddestcoder/lumo-backend/svc-messaging/migrations"
)

func setup(t *testing.T) *app.Service {
	pg, rurl := os.Getenv("LUMO_TEST_PG"), os.Getenv("LUMO_TEST_REDIS")
	if pg == "" || rurl == "" {
		t.Skip("set LUMO_TEST_PG and LUMO_TEST_REDIS")
	}
	ctx := context.Background()
	admin, _ := pgx.Connect(ctx, pg)
	name := fmt.Sprintf("messaging_test_%d", time.Now().UnixNano())
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
	pool, _ := db.Open(ctx, dsn, db.Options{MaxConns: 20})
	t.Cleanup(pool.Close)
	rdb, err := redisx.Open(ctx, rurl, "", "")
	if err != nil {
		t.Fatal(err)
	}
	rdb.FlushDB(ctx)
	ident := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var items []events.UserPublic
		for _, id := range strings.Split(r.URL.Query().Get("ids"), ",") {
			if id != "" {
				items = append(items, events.UserPublic{ID: id, Handle: "h" + id[:4], DisplayName: "User " + id[:4]})
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"items": items})
	}))
	t.Cleanup(ident.Close)
	return &app.Service{Pool: pool, Redis: rdb, Users: userdir.New(ident.URL), MessagesPerMinute: 1000, NewChatsPerDay: 3}
}

func TestDirectMessages(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	ada, bayo := events.NewID(), events.NewID()

	// Bayo listens on his personal channel.
	sub := s.Redis.Subscribe(ctx, realtime.UserChannel(bayo))
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatal(err)
	}

	// Opening twice (even concurrently) gives one conversation.
	var wg sync.WaitGroup
	ids := make([]string, 4)
	for i := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := s.Open(ctx, ada, bayo)
			if err != nil {
				t.Error(err)
			}
			ids[i] = c.ID
		}()
	}
	wg.Wait()
	for _, id := range ids[1:] {
		if id != ids[0] {
			t.Fatalf("two conversations created: %v", ids)
		}
	}
	conv := ids[0]
	if c, err := s.Open(ctx, bayo, ada); err != nil || c.ID != conv || c.Peer.ID != ada {
		t.Fatalf("reverse open %+v %v", c, err)
	}
	// Empty chats don't show in Bayo's list until a message arrives.
	if list, _, _ := s.Conversations(ctx, bayo, "", 10); len(list) != 0 {
		t.Fatalf("empty chat listed: %+v", list)
	}

	m1, err := s.Send(ctx, ada, conv, "c-1", "  hey Bayo!  ")
	if err != nil || m1.Text != "hey Bayo!" {
		t.Fatalf("send %+v %v", m1, err)
	}
	again, err := s.Send(ctx, ada, conv, "c-1", "hey Bayo!") // retry
	if err != nil || again.ID != m1.ID {
		t.Fatalf("retry duplicated: %+v %v", again, err)
	}
	s.Send(ctx, ada, conv, "c-2", "you there?")

	msg, err := sub.ReceiveTimeout(ctx, 2*time.Second)
	if err != nil || !strings.Contains(fmt.Sprint(msg), "hey Bayo!") {
		t.Fatalf("live push: %v %v", msg, err)
	}

	list, _, err := s.Conversations(ctx, bayo, "", 10)
	if err != nil || len(list) != 1 || list[0].Unread != 2 || list[0].LastPreview != "you there?" || list[0].Peer.ID != ada {
		t.Fatalf("bayo list %+v %v", list, err)
	}
	if n, _ := s.Unread(ctx, bayo); n != 2 {
		t.Fatalf("unread %d", n)
	}
	page, _, _ := s.Messages(ctx, bayo, conv, "", 10)
	if len(page) != 2 || page[0].Text != "you there?" {
		t.Fatalf("messages %+v", page)
	}
	if err := s.MarkRead(ctx, bayo, conv); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.Unread(ctx, bayo); n != 0 {
		t.Fatalf("unread after read %d", n)
	}
	if c, _ := s.Get(ctx, ada, conv); c.PeerReadAt == nil || !c.LastFromMe {
		t.Fatalf("read receipt %+v", c)
	}

	// Outsiders can't read or post.
	eve := events.NewID()
	if _, _, err := s.Messages(ctx, eve, conv, "", 10); err == nil {
		t.Fatal("outsider read messages")
	}
	if _, err := s.Send(ctx, eve, conv, "x", "hi"); err == nil {
		t.Fatal("outsider posted")
	}

	// Unsend: only the sender, and it becomes a tombstone.
	if err := s.Unsend(ctx, bayo, m1.ID); err == nil {
		t.Fatal("recipient unsent a message")
	}
	if err := s.Unsend(ctx, ada, m1.ID); err != nil {
		t.Fatal(err)
	}
	page, _, _ = s.Messages(ctx, bayo, conv, "", 10)
	if !page[1].Deleted || page[1].Text != "" {
		t.Fatalf("tombstone %+v", page[1])
	}

	// Block (from social events) stops messages both ways; unblock restores.
	block := events.MustNew("svc-social", events.Blocked, ada, events.BlockData{BlockerID: bayo, BlockedID: ada})
	s.OnSocial(ctx, block)
	if _, err := s.Send(ctx, ada, conv, "c-3", "hello?"); !errors.Is(err, app.ErrBlocked) {
		t.Fatalf("blocked send: %v", err)
	}
	if _, err := s.Open(ctx, ada, bayo); !errors.Is(err, app.ErrBlocked) {
		t.Fatalf("blocked open: %v", err)
	}
	s.OnSocial(ctx, events.MustNew("svc-social", events.Unblocked, ada, events.BlockData{BlockerID: bayo, BlockedID: ada}))
	if _, err := s.Send(ctx, ada, conv, "c-4", "back"); err != nil {
		t.Fatalf("after unblock: %v", err)
	}

	// Validation and the new-chat daily limit (3 in this test).
	if _, err := s.Send(ctx, ada, conv, "c-5", strings.Repeat("x", 1001)); !errors.Is(err, app.ErrBadText) {
		t.Fatalf("long text: %v", err)
	}
	if _, err := s.Open(ctx, ada, ada); !errors.Is(err, app.ErrSelf) {
		t.Fatalf("self chat: %v", err)
	}
	var lastErr error
	for i := 0; i < 4; i++ {
		_, lastErr = s.Open(ctx, ada, events.NewID())
	}
	if !errors.Is(lastErr, app.ErrTooMany) {
		t.Fatalf("new chat limit: %v", lastErr)
	}

	// Outbox carries one event per stored message (retry excluded).
	var n int
	s.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE topic = 'dm.messages'`).Scan(&n)
	if n != 3 {
		t.Fatalf("outbox events %d, want 3", n)
	}
}
