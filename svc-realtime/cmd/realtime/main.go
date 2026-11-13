// Command realtime runs svc-realtime: WebSocket chat and room pushes.
package main

import (
	"context"
	"net/http"
	"os"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/app"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/config"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/httpx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/kafkax"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/logx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/redisx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/userdir"
	"github.com/oviethebaddestcoder/lumo-backend/svc-realtime/internal/hub"
)

func main() {
	app.Main("svc-realtime", app.Commands{"serve": serve, "worker": worker})
}

// serve holds WebSocket connections. Scale on connection count
// (realtime_connections); each pod handles MAX_CONNS.
func serve(ctx context.Context) error {
	e := config.New()
	logx.New("svc-realtime", e.Str("LOG_LEVEL", "info"))
	addr := e.Str("ADDR", ":8080")
	jwks := e.Str("JWKS_URL", "http://svc-identity/.well-known/jwks.json")
	rurl, rnodes := e.Str("REDIS_URL", "redis://localhost:6379/0"), e.Str("REDIS_CLUSTER_ADDRS", "")
	origins := e.List("WS_ORIGINS", []string{"lumo.app", "*.lumo.app"})
	kcfg := kafkax.ConfigFromEnv(e, "svc-realtime")
	host, _ := os.Hostname()
	opts := hub.Options{
		MaxConns: e.Int64("MAX_CONNS", 100_000), RoomChatRate: float64(e.Int("ROOM_CHAT_PER_SEC", 30)),
		UserChatRate: 1, BlockedWords: e.List("CHAT_BLOCKED_WORDS", nil), Pod: host,
	}
	idURL := e.Str("IDENTITY_URL", "http://svc-identity")
	if err := e.Err(); err != nil {
		return err
	}
	rdb, err := redisx.Open(ctx, rurl, rnodes, "")
	if err != nil {
		return err
	}
	defer rdb.Close()
	prod, err := kafkax.NewProducer(kcfg, "svc-realtime")
	if err != nil {
		return err
	}
	defer prod.Close()
	h := hub.New(ctx, rdb, userdir.New(idURL), prod, opts)
	v := &auth.Verifier{Keys: auth.NewRemoteJWKS(jwks)}
	mux := http.NewServeMux()
	mux.Handle("GET /v1/rt/rooms/{id}", v.Middleware(true, true)(h.ServeRoom(origins)))
	srv := httpx.NewServer(addr, mux, httpx.Check{Name: "redis", Fn: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }})
	return app.Group(ctx, h.Run, srv.Run)
}

// worker turns Kafka events into room pushes (gift banners, kicks).
func worker(ctx context.Context) error {
	e := config.New()
	logx.New("svc-realtime", e.Str("LOG_LEVEL", "info"))
	addr := e.Str("ADDR", ":8080")
	rurl, rnodes := e.Str("REDIS_URL", "redis://localhost:6379/0"), e.Str("REDIS_CLUSTER_ADDRS", "")
	kcfg := kafkax.ConfigFromEnv(e, "svc-realtime-worker")
	idURL := e.Str("IDENTITY_URL", "http://svc-identity")
	if err := e.Err(); err != nil {
		return err
	}
	rdb, err := redisx.Open(ctx, rurl, rnodes, "")
	if err != nil {
		return err
	}
	defer rdb.Close()
	h := hub.New(ctx, rdb, userdir.New(idURL), nil, hub.Options{MaxConns: 0})
	gifts, err := kafkax.NewConsumer(kcfg, kafkax.ConsumerOptions{Group: "realtime-gifts", Topics: []string{events.TopicWalletGifts},
		Handle: h.OnGift, StartAtEnd: true, MaxAttempts: 2})
	if err != nil {
		return err
	}
	mod, err := kafkax.NewConsumer(kcfg, kafkax.ConsumerOptions{Group: "realtime-moderation", Topics: []string{events.TopicModeration}, Handle: h.OnModeration})
	if err != nil {
		return err
	}
	srv := httpx.NewServer(addr, nil, httpx.Check{Name: "redis", Fn: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }})
	return app.Group(ctx, gifts.Run, mod.Run, srv.Run)
}
