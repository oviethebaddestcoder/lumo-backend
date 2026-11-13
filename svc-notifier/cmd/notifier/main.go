// Command notifier runs svc-notifier: device registry and push.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/app"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/config"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/gauth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/httpx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/kafkax"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/logx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/redisx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/userdir"
	"github.com/oviethebaddestcoder/lumo-backend/svc-notifier/internal/api"
	napp "github.com/oviethebaddestcoder/lumo-backend/svc-notifier/internal/app"
	"github.com/oviethebaddestcoder/lumo-backend/svc-notifier/migrations"
)

func main() {
	app.Main("svc-notifier", app.Commands{"serve": serve, "worker": worker, "migrate": migrate})
}

type cfg struct {
	addr, dsn, direct, jwks, redis, nodes, identity, social, internal string
	fcmSA                                                             []byte
	kafka                                                             kafkax.Config
}

func load() (cfg, error) {
	e := config.New()
	logx.New("svc-notifier", e.Str("LOG_LEVEL", "info"))
	c := cfg{addr: e.Str("ADDR", ":8080"), dsn: e.Required("DATABASE_URL"), direct: e.Str("DATABASE_URL_DIRECT", ""),
		jwks: e.Str("JWKS_URL", "http://svc-identity/.well-known/jwks.json"), redis: e.Str("REDIS_URL", "redis://localhost:6379/0"),
		nodes: e.Str("REDIS_CLUSTER_ADDRS", ""), identity: e.Str("IDENTITY_URL", "http://svc-identity"),
		social: e.Str("SOCIAL_URL", "http://svc-social"), internal: e.Required("INTERNAL_TOKEN"),
		fcmSA: e.Base64("FCM_SERVICE_ACCOUNT", false), kafka: kafkax.ConfigFromEnv(e, "svc-notifier")}
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

func open(ctx context.Context, c cfg) (*napp.Service, *pgxpool.Pool, error) {
	pool, err := db.Open(ctx, c.dsn, db.Options{MaxConns: 30})
	if err != nil {
		return nil, nil, err
	}
	rdb, err := redisx.Open(ctx, c.redis, c.nodes, "")
	if err != nil {
		return nil, nil, err
	}
	s := &napp.Service{Pool: pool, Redis: rdb, Users: userdir.New(c.identity), SocialURL: c.social, InternalToken: c.internal,
		HTTP: &http.Client{Timeout: 10 * time.Second}, FanoutWorkers: 64}
	if len(c.fcmSA) > 0 {
		ts, err := gauth.New(c.fcmSA, nil, "https://www.googleapis.com/auth/firebase.messaging")
		if err != nil {
			return nil, nil, err
		}
		s.Sender = &napp.FCM{Tokens: ts, ProjectID: ts.ProjectID(), HTTP: &http.Client{Timeout: 10 * time.Second}}
	} else {
		slog.Warn("FCM_SERVICE_ACCOUNT not set: pushes are logged, not sent")
		s.Sender = logSender{}
	}
	return s, pool, nil
}

type logSender struct{}

func (logSender) Send(ctx context.Context, token string, p napp.Push) error {
	slog.InfoContext(ctx, "push (dry run)", "title", p.Title, "body", p.Body)
	return nil
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
	social, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{Group: "notifier-social", Topics: []string{events.TopicSocialEvents},
		Handle: s.OnSocial, StartAtEnd: true})
	if err != nil {
		return err
	}
	live, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{Group: "notifier-live", Topics: []string{events.TopicLiveEvents},
		Handle: s.OnLive, StartAtEnd: true, HandlerTimeout: 30 * time.Minute})
	if err != nil {
		return err
	}
	ident, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{Group: "notifier-identity", Topics: []string{events.TopicIdentityUsers}, Handle: s.OnIdentity})
	if err != nil {
		return err
	}
	wallet, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{Group: "notifier-wallet", Topics: []string{events.TopicWalletEvents}, Handle: s.OnWallet})
	if err != nil {
		return err
	}
	dms, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{Group: "notifier-dm", Topics: []string{events.TopicDMMessages}, Handle: s.OnDM, StartAtEnd: true})
	if err != nil {
		return err
	}
	trim := func(ctx context.Context) error {
		t := time.NewTicker(6 * time.Hour)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
				if err := s.TrimInbox(ctx); err != nil {
					slog.WarnContext(ctx, "inbox trim failed", "err", err)
				}
			}
		}
	}
	return app.Group(ctx, social.Run, live.Run, ident.Run, wallet.Run, dms.Run, trim,
		httpx.NewServer(c.addr, nil, httpx.Check{Name: "postgres", Fn: pool.Ping}).Run)
}
