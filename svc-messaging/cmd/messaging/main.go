// Command messaging runs svc-messaging: one-to-one direct messages.
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
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/redisx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/userdir"
	"github.com/oviethebaddestcoder/lumo-backend/svc-messaging/internal/api"
	mapp "github.com/oviethebaddestcoder/lumo-backend/svc-messaging/internal/app"
	"github.com/oviethebaddestcoder/lumo-backend/svc-messaging/migrations"
	"github.com/redis/go-redis/v9"
)

func main() {
	app.Main("svc-messaging", app.Commands{"serve": serve, "worker": worker, "migrate": migrate})
}

type cfg struct {
	addr, dsn, direct, jwks, identity     string
	redisURL, redisCluster, redisPassword string
	perMinute, newPerDay                  int64
	kafka                                 kafkax.Config
}

func load() (cfg, error) {
	e := config.New()
	logx.New("svc-messaging", e.Str("LOG_LEVEL", "info"))
	c := cfg{addr: e.Str("ADDR", ":8080"), dsn: e.Required("DATABASE_URL"), direct: e.Str("DATABASE_URL_DIRECT", ""),
		jwks: e.Str("JWKS_URL", "http://svc-identity/.well-known/jwks.json"), identity: e.Str("IDENTITY_URL", "http://svc-identity"),
		redisURL: e.Str("REDIS_URL", "redis://localhost:6379/0"), redisCluster: e.Str("REDIS_CLUSTER_ADDRS", ""),
		redisPassword: e.Str("REDIS_PASSWORD", ""), perMinute: e.Int64("DM_MESSAGES_PER_MINUTE", 30),
		newPerDay: e.Int64("DM_NEW_CHATS_PER_DAY", 50), kafka: kafkax.ConfigFromEnv(e, "svc-messaging")}
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

func open(ctx context.Context, c cfg) (*mapp.Service, *pgxpool.Pool, redis.UniversalClient, error) {
	pool, err := db.Open(ctx, c.dsn, db.Options{MaxConns: 40})
	if err != nil {
		return nil, nil, nil, err
	}
	rdb, err := redisx.Open(ctx, c.redisURL, c.redisCluster, c.redisPassword)
	if err != nil {
		pool.Close()
		return nil, nil, nil, err
	}
	return &mapp.Service{Pool: pool, Redis: rdb, Users: userdir.New(c.identity),
		MessagesPerMinute: int(c.perMinute), NewChatsPerDay: int(c.newPerDay)}, pool, rdb, nil
}

func serve(ctx context.Context) error {
	c, err := load()
	if err != nil {
		return err
	}
	svc, pool, rdb, err := open(ctx, c)
	if err != nil {
		return err
	}
	defer pool.Close()
	defer rdb.Close()
	a := &api.API{Svc: svc, Verifier: &auth.Verifier{Keys: auth.NewRemoteJWKS(c.jwks)}}
	return httpx.NewServer(c.addr, a.Routes(), httpx.Check{Name: "postgres", Fn: pool.Ping},
		httpx.Check{Name: "redis", Fn: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }}).Run(ctx)
}

func worker(ctx context.Context) error {
	c, err := load()
	if err != nil {
		return err
	}
	svc, pool, rdb, err := open(ctx, c)
	if err != nil {
		return err
	}
	defer pool.Close()
	defer rdb.Close()
	prod, err := kafkax.NewProducer(c.kafka, "svc-messaging")
	if err != nil {
		return err
	}
	defer prod.Close()
	social, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{Group: "messaging-blocks", Topics: []string{events.TopicSocialEvents}, Handle: svc.OnSocial})
	if err != nil {
		return err
	}
	ident, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{Group: "messaging-identity", Topics: []string{events.TopicIdentityUsers}, Handle: svc.OnIdentity})
	if err != nil {
		return err
	}
	return app.Group(ctx, outbox.NewRelay("messaging", pool, prod).Run, social.Run, ident.Run,
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
