// Command media runs svc-media: uploads and video lifecycle.
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
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/s3x"
	"github.com/oviethebaddestcoder/lumo-backend/svc-media/internal/api"
	mapp "github.com/oviethebaddestcoder/lumo-backend/svc-media/internal/app"
	"github.com/oviethebaddestcoder/lumo-backend/svc-media/migrations"
)

func main() {
	app.Main("svc-media", app.Commands{"serve": serve, "worker": worker, "migrate": migrate})
}

type cfg struct {
	addr, dsn, direct, jwks, cdn string
	s3                           *s3x.Client
	kafka                        kafkax.Config
}

func load() (cfg, error) {
	e := config.New()
	logx.New("svc-media", e.Str("LOG_LEVEL", "info"))
	c := cfg{addr: e.Str("ADDR", ":8080"), dsn: e.Required("DATABASE_URL"), direct: e.Str("DATABASE_URL_DIRECT", ""),
		jwks: e.Str("JWKS_URL", "http://svc-identity/.well-known/jwks.json"), cdn: e.Required("CDN_BASE_URL"),
		kafka: kafkax.ConfigFromEnv(e, "svc-media"),
		s3: &s3x.Client{Endpoint: e.Required("S3_ENDPOINT"), Region: e.Str("S3_REGION", "auto"), Bucket: e.Required("S3_BUCKET"),
			AccessKey: e.Required("S3_ACCESS_KEY"), SecretKey: e.Required("S3_SECRET_KEY")}}
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
	pool, err := db.Open(ctx, c.dsn, db.Options{MaxConns: 30})
	if err != nil {
		return nil, nil, err
	}
	return &mapp.Service{Pool: pool, S3: c.s3, CDNURL: c.cdn}, pool, nil
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
	a := &api.API{Svc: svc, Verifier: &auth.Verifier{Keys: auth.NewRemoteJWKS(c.jwks)}}
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
	prod, err := kafkax.NewProducer(c.kafka, "svc-media")
	if err != nil {
		return err
	}
	defer prod.Close()
	results, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{Group: "media-results", Topics: []string{events.TopicMediaResults}, Handle: svc.OnResult})
	if err != nil {
		return err
	}
	live, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{Group: "media-live", Topics: []string{events.TopicLiveEvents}, Handle: svc.OnLive})
	if err != nil {
		return err
	}
	mod, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{Group: "media-moderation", Topics: []string{events.TopicModeration}, Handle: svc.OnModeration})
	if err != nil {
		return err
	}
	return app.Group(ctx, outbox.NewRelay("media", pool, prod).Run, results.Run, live.Run, mod.Run,
		func(ctx context.Context) error {
			t := time.NewTicker(15 * time.Minute)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-t.C:
					_ = svc.AbortStale(ctx)
				}
			}
		},
		httpx.NewServer(c.addr, nil, httpx.Check{Name: "postgres", Fn: pool.Ping}, httpx.Check{Name: "kafka", Fn: prod.Ping}).Run)
}
