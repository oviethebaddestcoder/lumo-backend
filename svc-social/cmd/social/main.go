// Command social runs svc-social: follows, likes, comments, blocks, reports.
package main

import (
	"context"
	"time"

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
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/userdir"
	"github.com/oviethebaddestcoder/lumo-backend/svc-social/internal/api"
	sapp "github.com/oviethebaddestcoder/lumo-backend/svc-social/internal/app"
	"github.com/oviethebaddestcoder/lumo-backend/svc-social/migrations"
)

func main() {
	app.Main("svc-social", app.Commands{"serve": serve, "worker": worker, "migrate": migrate})
}

type cfg struct {
	addr, dsn, direct, jwks, identity, internal string
	kafka                                       kafkax.Config
}

func load() (cfg, error) {
	e := config.New()
	logx.New("svc-social", e.Str("LOG_LEVEL", "info"))
	c := cfg{addr: e.Str("ADDR", ":8080"), dsn: e.Required("DATABASE_URL"), direct: e.Str("DATABASE_URL_DIRECT", ""),
		jwks: e.Str("JWKS_URL", "http://svc-identity/.well-known/jwks.json"), identity: e.Str("IDENTITY_URL", "http://svc-identity"),
		internal: e.Required("INTERNAL_TOKEN"), kafka: kafkax.ConfigFromEnv(e, "svc-social")}
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

func open(ctx context.Context, c cfg) (*sapp.Service, *pgxpool.Pool, error) {
	pool, err := db.Open(ctx, c.dsn, db.Options{MaxConns: 40})
	if err != nil {
		return nil, nil, err
	}
	return &sapp.Service{Pool: pool, Users: userdir.New(c.identity)}, pool, nil
}

func serve(ctx context.Context) error {
	c, err := load()
	if err != nil {
		return err
	}
	svc, pool, err := open(ctx, c)
	if err != nil {
		return err
	}
	defer pool.Close()
	a := &api.API{Svc: svc, Verifier: &auth.Verifier{Keys: auth.NewRemoteJWKS(c.jwks)}, InternalToken: c.internal}
	return httpx.NewServer(c.addr, a.Routes(), httpx.Check{Name: "postgres", Fn: pool.Ping}).Run(ctx)
}

func worker(ctx context.Context) error {
	c, err := load()
	if err != nil {
		return err
	}
	svc, pool, err := open(ctx, c)
	if err != nil {
		return err
	}
	defer pool.Close()
	prod, err := kafkax.NewProducer(c.kafka, "svc-social")
	if err != nil {
		return err
	}
	defer prod.Close()
	counts, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{Group: "social-counts", Topics: []string{events.TopicSocialEvents},
		HandleBatch: svc.ApplyCounts, BatchSize: 2000})
	if err != nil {
		return err
	}
	videos, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{Group: "social-videos", Topics: []string{events.TopicMediaVideos}, Handle: svc.OnVideoState})
	if err != nil {
		return err
	}
	mod, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{Group: "social-moderation", Topics: []string{events.TopicModeration}, Handle: svc.OnModeration})
	if err != nil {
		return err
	}
	return app.Group(ctx, outbox.NewRelay("social", pool, prod).Run, counts.Run, videos.Run, mod.Run,
		func(ctx context.Context) error {
			t := time.NewTicker(10 * time.Minute)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-t.C:
					_ = outbox.PruneProcessed(ctx, pool, 14*24*time.Hour)
				}
			}
		},
		httpx.NewServer(c.addr, nil, httpx.Check{Name: "postgres", Fn: pool.Ping}, httpx.Check{Name: "kafka", Fn: prod.Ping}).Run)
}
