// Command moderation runs svc-moderation: report queue and actions.
package main

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/app"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/config"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/httpx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/kafkax"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/logx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/outbox"
	"github.com/oviethebaddestcoder/lumo-backend/svc-moderation/internal/api"
	mapp "github.com/oviethebaddestcoder/lumo-backend/svc-moderation/internal/app"
	"github.com/oviethebaddestcoder/lumo-backend/svc-moderation/migrations"
)

func main() {
	app.Main("svc-moderation", app.Commands{"serve": serve, "worker": worker, "migrate": migrate})
}

type cfg struct {
	addr, dsn, direct, jwks string
	flags                   []string
	kafka                   kafkax.Config
}

func load() (cfg, error) {
	e := config.New()
	logx.New("svc-moderation", e.Str("LOG_LEVEL", "info"))
	c := cfg{addr: e.Str("ADDR", ":8080"), dsn: e.Required("DATABASE_URL"), direct: e.Str("DATABASE_URL_DIRECT", ""),
		jwks: e.Str("JWKS_URL", "http://svc-identity/.well-known/jwks.json"), flags: e.List("CHAT_FLAG_WORDS", nil),
		kafka: kafkax.ConfigFromEnv(e, "svc-moderation")}
	for i := range c.flags {
		c.flags[i] = strings.ToLower(c.flags[i])
	}
	if c.direct == "" {
		c.direct = c.dsn
	}
	return c, e.Err()
}

func migrate(ctx context.Context) error {
	c, err := load()
	if err != nil {
		return err
	}
	return db.Migrate(ctx, c.direct, migrations.FS, migrations.LockID)
}

func open(ctx context.Context, c cfg) (*mapp.Service, *pgxpool.Pool, error) {
	pool, err := db.Open(ctx, c.dsn, db.Options{MaxConns: 20})
	if err != nil {
		return nil, nil, err
	}
	return &mapp.Service{Pool: pool, FlagWords: c.flags}, pool, nil
}

func serve(ctx context.Context) error {
	c, err := load()
	if err != nil {
		return err
	}
	s, pool, err := open(ctx, c)
	if err != nil {
		return err
	}
	defer pool.Close()
	a := &api.API{Svc: s, Verifier: &auth.Verifier{Keys: auth.NewRemoteJWKS(c.jwks)}}
	return httpx.NewServer(c.addr, a.Routes(), httpx.Check{Name: "postgres", Fn: pool.Ping}).Run(ctx)
}

func worker(ctx context.Context) error {
	c, err := load()
	if err != nil {
		return err
	}
	s, pool, err := open(ctx, c)
	if err != nil {
		return err
	}
	defer pool.Close()
	prod, err := kafkax.NewProducer(c.kafka, "svc-moderation")
	if err != nil {
		return err
	}
	defer prod.Close()
	reports, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{Group: "moderation-reports", Topics: []string{events.TopicSocialEvents}, Handle: s.OnReport})
	if err != nil {
		return err
	}
	chat, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{Group: "moderation-chat", Topics: []string{events.TopicChatMessages}, Handle: s.OnChat})
	if err != nil {
		return err
	}
	return app.Group(ctx, outbox.NewRelay("moderation", pool, prod).Run, reports.Run, chat.Run,
		httpx.NewServer(c.addr, nil, httpx.Check{Name: "postgres", Fn: pool.Ping}, httpx.Check{Name: "kafka", Fn: prod.Ping}).Run)
}
