// Command gateway runs the public API edge.
package main

import (
	"context"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/app"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/config"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/httpx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/logx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/redisx"
	"github.com/oviethebaddestcoder/lumo-backend/svc-gateway/internal/gateway"
)

// DefaultRoutes maps path prefixes to Kubernetes service DNS names.
const DefaultRoutes = "/.well-known=http://svc-identity," +
	"/v1/auth=http://svc-identity,/v1/me=http://svc-identity,/v1/users=http://svc-identity," +
	"/v1/wallet=http://svc-wallet," +
	"/v1/live=http://svc-live," +
	"/v1/rt=http://svc-realtime," +
	"/v1/social=http://svc-social," +
	"/v1/media=http://svc-media," +
	"/v1/feed=http://svc-feed," +
	"/v1/notify=http://svc-notifier," +
	"/v1/moderation=http://svc-moderation"

func main() {
	app.Main("svc-gateway", app.Commands{"serve": serve})
}

func serve(ctx context.Context) error {
	e := config.New()
	addr := e.Str("ADDR", ":8080")
	logx.New("svc-gateway", e.Str("LOG_LEVEL", "info"))
	routesSpec := e.Str("ROUTES", DefaultRoutes)
	jwks := e.Str("JWKS_URL", "http://svc-identity/.well-known/jwks.json")
	redisURL := e.Str("REDIS_URL", "redis://localhost:6379/0")
	redisNodes := e.Str("REDIS_CLUSTER_ADDRS", "")
	limits := gateway.Limits{
		PerUser:   e.Int("RATE_PER_USER_MIN", 600),
		PerIP:     e.Int("RATE_PER_IP_MIN", 300),
		AuthPerIP: e.Int("RATE_AUTH_PER_IP_MIN", 60),
	}
	origins := e.List("ALLOWED_ORIGINS", []string{"https://lumo.app", "https://studio.lumo.app"})
	trusted := e.List("TRUSTED_PROXY_CIDRS", []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"})
	if err := e.Err(); err != nil {
		return err
	}
	routes, err := gateway.ParseRoutes(routesSpec)
	if err != nil {
		return err
	}
	rdb, err := redisx.Open(ctx, redisURL, redisNodes, "")
	if err != nil {
		return err
	}
	defer rdb.Close()
	g, err := gateway.New(routes, &auth.Verifier{Keys: auth.NewRemoteJWKS(jwks)}, rdb, limits, origins, trusted)
	if err != nil {
		return err
	}
	srv := httpx.NewServer(addr, g, httpx.Check{Name: "redis", Fn: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }})
	return srv.Run(ctx)
}
