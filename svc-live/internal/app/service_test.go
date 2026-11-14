package app_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/redisx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/userdir"
	"github.com/oviethebaddestcoder/lumo-backend/svc-live/internal/app"
	"github.com/oviethebaddestcoder/lumo-backend/svc-live/internal/livekit"
	"github.com/oviethebaddestcoder/lumo-backend/svc-live/migrations"
)

func setup(t *testing.T) *app.Service {
	pg, rurl := os.Getenv("LUMO_TEST_PG"), os.Getenv("LUMO_TEST_REDIS")
	if pg == "" || rurl == "" {
		t.Skip("set LUMO_TEST_PG and LUMO_TEST_REDIS")
	}
	ctx := context.Background()
	admin, _ := pgx.Connect(ctx, pg)
	name := fmt.Sprintf("live_test_%d", time.Now().UnixNano())
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
	pool, _ := db.Open(ctx, dsn, db.Options{})
	t.Cleanup(pool.Close)
	rdb, _ := redisx.Open(ctx, rurl, "", "")
	rdb.FlushDB(ctx)

	identity := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var items []events.UserPublic
		for _, id := range strings.Split(r.URL.Query().Get("ids"), ",") {
			items = append(items, events.UserPublic{ID: id, DisplayName: "User " + id[len(id)-4:]})
		}
		json.NewEncoder(w).Encode(map[string]any{"items": items})
	}))
	t.Cleanup(identity.Close)
	lk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"egress_id":"EG_1","ingress_id":"IN_1","url":"rtmp://x","stream_key":"sk"}`))
	}))
	t.Cleanup(lk.Close)
	return &app.Service{
		Pool: pool, Redis: rdb, Users: userdir.New(identity.URL),
		LK:    &livekit.Client{WSURL: "wss://lk", APIURL: lk.URL, APIKey: "key", APISecret: "secret-secret-secret-secret"},
		HLSAt: 3, PKDuration: time.Second, InviteTTL: 30 * time.Second, TokenTTL: time.Hour, HLSBaseURL: "https://cdn",
	}
}

func host() auth.Principal { return auth.Principal{UserID: events.NewID(), Adult: true} }

func webhook(t *testing.T, s *app.Service, body string) livekit.Webhook {
	sum := sha256.Sum256([]byte(body))
	tok, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss": "key", "exp": time.Now().Add(time.Minute).Unix(), "sha256": base64.StdEncoding.EncodeToString(sum[:]),
	}).SignedString([]byte(s.LK.APISecret))
	w, err := s.LK.VerifyWebhook([]byte(body), "Bearer "+tok)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.LK.VerifyWebhook([]byte(body+" "), "Bearer "+tok); err == nil {
		t.Fatal("tampered body accepted")
	}
	return w
}

func TestRoomLifecycleViewersAndGifts(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	h := host()
	if _, _, err := s.Start(ctx, auth.Principal{UserID: events.NewID()}, app.StartInput{Title: "x", Category: "music"}); err == nil {
		t.Fatal("minor allowed to go live")
	}
	room, conn, err := s.Start(ctx, h, app.StartInput{Title: "Friday karaoke", Category: "music", AllowGifts: true, AllowPK: true, GoalCoins: 1000})
	if err != nil || conn.Token == "" || conn.Mode != "webrtc" {
		t.Fatalf("start: %v %+v", err, conn)
	}
	if _, _, err := s.Start(ctx, h, app.StartInput{Title: "again", Category: "music"}); !errors.Is(err, app.ErrAlreadyLive) {
		t.Fatalf("want already live, got %v", err)
	}
	// Viewers join via LiveKit webhooks; the host does not count.
	for _, id := range []string{h.UserID, "v1", "v2", "v3", "v3"} {
		s.HandleWebhook(ctx, webhook(t, s, fmt.Sprintf(`{"event":"participant_joined","room":{"name":%q},"participant":{"identity":%q}}`, room.ID, id)))
	}
	got, _ := s.Get(ctx, room.ID)
	if got.Viewers != 3 || got.Host.DisplayName == "" {
		t.Fatalf("viewers %d host %+v", got.Viewers, got.Host)
	}
	// Gifts are counted once even if Kafka redelivers.
	gift := events.MustNew("svc-wallet", events.GiftSent, h.UserID, events.GiftSentData{GiftSendID: events.NewID(), RoomID: room.ID, ReceiverID: h.UserID, Coins: 100})
	s.OnGift(ctx, gift)
	s.OnGift(ctx, gift)
	got, _ = s.Get(ctx, room.ID)
	if got.GiftCoins != 100 {
		t.Fatalf("gift coins %d", got.GiftCoins)
	}
	list, _ := s.List(ctx, "music", 0, 10)
	if len(list) != 1 || list[0].ID != room.ID {
		t.Fatalf("list %+v", list)
	}
	if err := s.End(ctx, room.ID, "host_ended", "someone-else"); !errors.Is(err, app.ErrNotHost) {
		t.Fatalf("non-host ended room: %v", err)
	}
	if err := s.End(ctx, room.ID, "host_ended", h.UserID); err != nil {
		t.Fatal(err)
	}
	if err := s.End(ctx, room.ID, "livekit_room_finished", ""); err != nil { // idempotent
		t.Fatal(err)
	}
	var status string
	var coins int64
	s.Pool.QueryRow(ctx, `SELECT status, gift_coins FROM rooms WHERE id = $1`, room.ID).Scan(&status, &coins)
	if status != "ended" || coins != 100 {
		t.Fatalf("status %s coins %d", status, coins)
	}
	var started, ended int
	s.Pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE payload->>'type' = 'live.room.started'), count(*) FILTER (WHERE payload->>'type' = 'live.room.ended') FROM outbox`).Scan(&started, &ended)
	if started != 1 || ended != 1 {
		t.Fatalf("outbox started=%d ended=%d", started, ended)
	}
	// Studio: history and 7-day summary for the host only.
	hist, _, err := s.HostRooms(ctx, h.UserID, "", 10)
	if err != nil || len(hist) != 1 || hist[0].Status != "ended" || hist[0].GiftCoins != 100 || hist[0].EndedAt == nil {
		t.Fatalf("host rooms %+v %v", hist, err)
	}
	sum, err := s.HostSummaryFor(ctx, h.UserID, 7)
	if err != nil || sum.Sessions != 1 || sum.GiftCoins != 100 {
		t.Fatalf("summary %+v %v", sum, err)
	}
	if other, _, _ := s.HostRooms(ctx, events.NewID(), "", 10); len(other) != 0 {
		t.Fatalf("leaked rooms %+v", other)
	}
}

func TestPKBattle(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	a, b := host(), host()
	ra, _, _ := s.Start(ctx, a, app.StartInput{Title: "A", Category: "music", AllowPK: true, AllowGifts: true})
	rb, _, _ := s.Start(ctx, b, app.StartInput{Title: "B", Category: "music", AllowPK: true, AllowGifts: true})
	inv, err := s.Invite(ctx, a, rb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Invite(ctx, a, rb.ID); !errors.Is(err, app.ErrPKBusy) {
		t.Fatalf("double invite: %v", err)
	}
	if _, err := s.Respond(ctx, a, inv.ID, true); !errors.Is(err, app.ErrNotHost) {
		t.Fatalf("inviter accepted own invite: %v", err)
	}
	v, err := s.Respond(ctx, b, inv.ID, true)
	if err != nil || v.Status != "active" || v.CrossTok == "" {
		t.Fatalf("accept: %v %+v", err, v)
	}
	for _, g := range []struct {
		room  string
		coins int64
	}{{ra.ID, 50}, {rb.ID, 120}, {ra.ID, 30}} {
		s.OnGift(ctx, events.MustNew("w", events.GiftSent, "k", events.GiftSentData{GiftSendID: events.NewID(), RoomID: g.room, Coins: g.coins}))
	}
	time.Sleep(1100 * time.Millisecond)
	s.Tick(ctx)
	var status, winner string
	var sa, sb int64
	s.Pool.QueryRow(ctx, `SELECT status, score_a, score_b, COALESCE(winner_room::text, '') FROM pk_battles WHERE id = $1`, inv.ID).Scan(&status, &sa, &sb, &winner)
	if status != "ended" || sa != 80 || sb != 120 || winner != rb.ID {
		t.Fatalf("pk result: %s %d-%d winner=%s", status, sa, sb, winner)
	}
}
