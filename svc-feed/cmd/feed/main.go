// Command feed runs svc-feed: For You, Following and Watch feeds.
package main

import (
	"context"
	"net/http"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/app"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/config"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/httpx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/kafkax"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/logx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/redisx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/userdir"
	"github.com/oviethebaddestcoder/lumo-backend/svc-feed/internal/api"
	fapp "github.com/oviethebaddestcoder/lumo-backend/svc-feed/internal/app"
)

func main() {
	app.Main("svc-feed", app.Commands{"serve": serve, "worker": worker})
}

type cfg struct {
	addr, jwks, redis, nodes, identity, social, internal string
	kafka                                                kafkax.Config
}

func load() (cfg, error) {
	e := config.New()
	logx.New("svc-feed", e.Str("LOG_LEVEL", "info"))
	c := cfg{addr: e.Str("ADDR", ":8080"), jwks: e.Str("JWKS_URL", "http://svc-identity/.well-known/jwks.json"),
		redis: e.Str("REDIS_URL", "redis://localhost:6379/0"), nodes: e.Str("REDIS_CLUSTER_ADDRS", ""),
		identity: e.Str("IDENTITY_URL", "http://svc-identity"), social: e.Str("SOCIAL_URL", "http://svc-social"),
		internal: e.Required("INTERNAL_TOKEN"), kafka: kafkax.ConfigFromEnv(e, "svc-feed")}
	return c, e.Err()
}

func build(ctx context.Context, c cfg) (*fapp.Service, error) {
	rdb, err := redisx.Open(ctx, c.redis, c.nodes, "")
	if err != nil {
		return nil, err
	}
	arms, err := fapp.ArmsFromEnv()
	if err != nil {
		return nil, err
	}
	return &fapp.Service{R: rdb, Users: userdir.New(c.identity), SocialURL: c.social, InternalToken: c.internal, Arms: arms,
		HTTP: &http.Client{Timeout: 2 * time.Second}}, nil
}

func serve(ctx context.Context) error {
	c, err := load()
	if err != nil {
		return err
	}
	s, err := build(ctx, c)
	if err != nil {
		return err
	}
	prod, err := kafkax.NewProducer(c.kafka, "svc-feed")
	if err != nil {
		return err
	}
	defer prod.Close()
	s.Producer = prod
	a := &api.API{Svc: s, Verifier: &auth.Verifier{Keys: auth.NewRemoteJWKS(c.jwks)}}
	return httpx.NewServer(c.addr, a.Routes(), httpx.Check{Name: "redis", Fn: func(ctx context.Context) error { return s.R.Ping(ctx).Err() }}).Run(ctx)
}

func worker(ctx context.Context) error {
	c, err := load()
	if err != nil {
		return err
	}
	s, err := build(ctx, c)
	if err != nil {
		return err
	}
	videos, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{Group: "feed-videos", Topics: []string{events.TopicMediaVideos}, Handle: s.OnVideo})
	if err != nil {
		return err
	}
	social, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{Group: "feed-social", Topics: []string{events.TopicSocialEvents}, Handle: s.OnSocial})
	if err != nil {
		return err
	}
	// Watch signals: batched, keyed by viewer so each viewer's profile is
	// updated in order by one partition worker.
	signals, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{Group: "feed-signals", Topics: []string{events.TopicFeedSignals},
		HandleBatch: s.OnImpressions, BatchSize: 1000})
	if err != nil {
		return err
	}
	return app.Group(ctx, videos.Run, social.Run, signals.Run,
		func(ctx context.Context) error {
			t := time.NewTicker(10 * time.Minute)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-t.C:
					s.Trim(ctx)
				}
			}
		},
		httpx.NewServer(c.addr, nil, httpx.Check{Name: "redis", Fn: func(ctx context.Context) error { return s.R.Ping(ctx).Err() }}).Run)
}
