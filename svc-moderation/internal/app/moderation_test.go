package app_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/svc-moderation/internal/app"
	"github.com/oviethebaddestcoder/lumo-backend/svc-moderation/migrations"
)

func TestReportQueueAndAction(t *testing.T) {
	pg := os.Getenv("LUMO_TEST_PG")
	if pg == "" {
		t.Skip("set LUMO_TEST_PG")
	}
	ctx := context.Background()
	admin, _ := pgx.Connect(ctx, pg)
	name := fmt.Sprintf("moderation_test_%d", time.Now().UnixNano())
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
	pool, _ := db.Open(ctx, dsn, db.Options{})
	defer pool.Close()
	s := &app.Service{Pool: pool, FlagWords: []string{"scamlink"}}

	spammer, video := events.NewID(), events.NewID()
	for i := 0; i < 3; i++ {
		ev := events.MustNew("svc-social", events.Reported, video, events.ReportData{ReportID: events.NewID(), ReporterID: events.NewID(),
			TargetType: "video", TargetID: video, Reason: "spam"})
		s.OnReport(ctx, ev)
		s.OnReport(ctx, ev) // redelivery
	}
	s.OnChat(ctx, events.MustNew("svc-realtime", events.ChatSent, "r", events.ChatMessageData{ID: "m", RoomID: "r", UserID: spammer, Text: "visit SCAMLINK now"}))
	q, err := s.Queue(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(q) != 2 || q[0].TargetID != video || q[0].OpenReports != 3 || q[1].TargetID != spammer {
		t.Fatalf("queue %+v", q)
	}
	mod := auth.Principal{UserID: events.NewID(), Roles: []string{"moderator"}}
	if _, err := s.Act(ctx, mod, app.ActionInput{TargetType: "video", TargetID: video, Action: "ban", Reason: "x"}); err == nil {
		t.Fatal("ban is not a video action")
	}
	if _, err := s.Act(ctx, mod, app.ActionInput{TargetType: "video", TargetID: video, Action: "remove", Reason: "spam"}); err != nil {
		t.Fatal(err)
	}
	q, _ = s.Queue(ctx, 10)
	if len(q) != 1 || q[0].TargetID != spammer {
		t.Fatalf("video reports should be resolved: %+v", q)
	}
	var n int
	pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE payload->>'type' = 'moderation.action.taken'`).Scan(&n)
	if n != 1 {
		t.Fatalf("action events %d", n)
	}
}
