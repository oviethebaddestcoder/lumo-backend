// Command live runs svc-live: live rooms, viewers, PK battles (LiveKit).
package main

import (
	"context"
	"time"

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
	"github.com/oviethebaddestcoder/lumo-backend/svc-live/internal/api"
	lapp "github.com/oviethebaddestcoder/lumo-backend/svc-live/internal/app"
	"github.com/oviethebaddestcoder/lumo-backend/svc-live/internal/livekit"
	"github.com/oviethebaddestcoder/lumo-backend/svc-live/migrations"
)

func main() {
	app.Main("svc-live", app.Commands{"serve": serve, "worker": worker, "migrate": migrate})
}

type cfg struct {
	addr, dsn, direct, redis, redisNodes, jwks string
	kafka                                      kafkax.Config
	svc                                        *lapp.Service
}

func load() (cfg, error) {
	e := config.New()
	logx.New("svc-live", e.Str("LOG_LEVEL", "info"))
	c := cfg{
		addr: e.Str("ADDR", ":8080"), dsn: e.Required("DATABASE_URL"), direct: e.Str("DATABASE_URL_DIRECT", ""),
		redis: e.Str("REDIS_URL", "redis://localhost:6379/0"), redisNodes: e.Str("REDIS_CLUSTER_ADDRS", ""),
		jwks: e.Str("JWKS_URL", "http://svc-identity/.well-known/jwks.json"), kafka: kafkax.ConfigFromEnv(e, "svc-live"),
	}
	c.svc = &lapp.Service{
		LK: &livekit.Client{
			WSURL: e.Required("LIVEKIT_WS_URL"), APIURL: e.Required("LIVEKIT_API_URL"),
			APIKey: e.Required("LIVEKIT_API_KEY"), APISecret: e.Required("LIVEKIT_API_SECRET"),
			S3: livekit.S3Output{AccessKey: e.Str("EGRESS_S3_ACCESS_KEY", ""), Secret: e.Str("EGRESS_S3_SECRET", ""),
				Region: e.Str("EGRESS_S3_REGION", "auto"), Endpoint: e.Str("EGRESS_S3_ENDPOINT", ""),
				Bucket: e.Str("EGRESS_S3_BUCKET", ""), ForcePathStyle: true},
		},
		Users:       userdir.New(e.Str("IDENTITY_URL", "http://svc-identity")),
		HLSBaseURL:  e.Str("HLS_BASE_URL", "https://cdn.example.com"),
		HLSAt:       e.Int64("HLS_VIEWER_THRESHOLD", 300),
		PKDuration:  e.Duration("PK_DURATION", 5*time.Minute),
		InviteTTL:   e.Duration("PK_INVITE_TTL", 30*time.Second),
		TokenTTL:    e.Duration("LIVEKIT_TOKEN_TTL", 6*time.Hour),
		ReplayPaths: e.Str("REPLAY_PREFIX", "replays"),
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

func open(ctx context.Context, c cfg) (func(), error) {
	pool, err := db.Open(ctx, c.dsn, db.Options{MaxConns: 30})
	if err != nil {
		return nil, err
	}
	rdb, err := redisx.Open(ctx, c.redis, c.redisNodes, "")
	if err != nil {
		pool.Close()
		return nil, err
	}
	c.svc.Pool, c.svc.Redis = pool, rdb
	return func() { pool.Close(); _ = rdb.Close() }, nil
}

func serve(ctx context.Context) error {
	c, err := load()
	if err != nil {
		return err
	}
	closeFn, err := open(ctx, c)
	if err != nil {
		return err
	}
	defer closeFn()
	a := &api.API{Svc: c.svc, Verifier: &auth.Verifier{Keys: auth.NewRemoteJWKS(c.jwks)}}
	return httpx.NewServer(c.addr, a.Routes(), httpx.Check{Name: "postgres", Fn: c.svc.Pool.Ping}).Run(ctx)
}

func worker(ctx context.Context) error {
	c, err := load()
	if err != nil {
		return err
	}
	closeFn, err := open(ctx, c)
	if err != nil {
		return err
	}
	defer closeFn()
	prod, err := kafkax.NewProducer(c.kafka, "svc-live")
	if err != nil {
		return err
	}
	defer prod.Close()
	gifts, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{Group: "live-gifts", Topics: []string{events.TopicWalletGifts},
		Handle: c.svc.OnGift, StartAtEnd: true})
	if err != nil {
		return err
	}
	mod, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{Group: "live-moderation", Topics: []string{events.TopicModeration},
		Handle: c.svc.OnModeration})
	if err != nil {
		return err
	}
	return app.Group(ctx,
		outbox.NewRelay("live", c.svc.Pool, prod).Run,
		gifts.Run, mod.Run,
		func(ctx context.Context) error {
			t := time.NewTicker(time.Second)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-t.C:
					c.svc.Tick(ctx)
				}
			}
		},
		httpx.NewServer(c.addr, nil, httpx.Check{Name: "postgres", Fn: c.svc.Pool.Ping}, httpx.Check{Name: "kafka", Fn: prod.Ping}).Run,
	)
}
