// Command identity runs svc-identity: sign-in, sessions, tokens and profiles.
package main

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"log/slog"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/app"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/httpx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/kafkax"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/logx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/outbox"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/redisx"
	"github.com/oviethebaddestcoder/lumo-backend/svc-identity/internal/api"
	idapp "github.com/oviethebaddestcoder/lumo-backend/svc-identity/internal/app"
	"github.com/oviethebaddestcoder/lumo-backend/svc-identity/internal/config"
	"github.com/oviethebaddestcoder/lumo-backend/svc-identity/internal/otp"
	"github.com/oviethebaddestcoder/lumo-backend/svc-identity/internal/providers"
	"github.com/oviethebaddestcoder/lumo-backend/svc-identity/internal/store"
	"github.com/oviethebaddestcoder/lumo-backend/svc-identity/migrations"
)

func main() {
	app.Main("svc-identity", app.Commands{
		"serve":   serve,
		"worker":  worker,
		"migrate": migrate,
	})
}

func load() (config.Config, error) {
	cfg, err := config.Load()
	logx.New("svc-identity", cfg.LogLevel)
	return cfg, err
}

func migrate(ctx context.Context) error {
	cfg, err := load()
	if err != nil {
		return err
	}
	return db.Migrate(ctx, cfg.DirectURL, migrations.FS, migrations.LockID)
}

func build(ctx context.Context, cfg config.Config) (*idapp.Service, func(), error) {
	pool, err := db.Open(ctx, cfg.DatabaseURL, db.Options{MaxConns: 40})
	if err != nil {
		return nil, nil, err
	}
	rdb, err := redisx.Open(ctx, cfg.RedisURL, cfg.RedisNodes, "")
	if err != nil {
		pool.Close()
		return nil, nil, err
	}
	signer, err := auth.NewSigner(cfg.AccessTTL, cfg.SigningKeys...)
	if err != nil {
		return nil, nil, err
	}

	var sms, email otp.Sender = otp.LogSender{Channel: "sms"}, otp.LogSender{Channel: "email"}
	switch cfg.SMSProvider {
	case "termii":
		sms = otp.Termii{APIKey: cfg.TermiiKey, SenderID: cfg.TermiiSender}
	case "twilio":
		sms = otp.Twilio{AccountSID: cfg.TwilioSID, AuthToken: cfg.TwilioToken, From: cfg.TwilioFrom}
	case "log":
		slog.Warn("SMS_PROVIDER=log: codes are printed to logs; development only")
	default:
		return nil, nil, fmt.Errorf("unknown SMS_PROVIDER %q", cfg.SMSProvider)
	}
	if cfg.EmailProvider == "resend" {
		email = otp.Resend{APIKey: cfg.ResendKey, From: cfg.EmailFrom}
	}

	reg := providers.Registry{}
	addOIDC := func(name string, o config.OIDC) {
		if len(o.Audiences) > 0 {
			reg[name] = providers.NewOIDC(name, o.Issuers, o.Audiences, o.JWKSURL, o.Nonce)
		}
	}
	addOIDC("google", cfg.Google)
	addOIDC("apple", cfg.Apple)
	addOIDC("line", cfg.Line)
	addOIDC("kakao", cfg.Kakao)
	if cfg.FacebookAppID != "" {
		var limited *providers.OIDC
		if len(cfg.FBLimitd.Audiences) > 0 {
			limited = providers.NewOIDC("facebook", cfg.FBLimitd.Issuers, cfg.FBLimitd.Audiences, cfg.FBLimitd.JWKSURL, true)
		}
		reg["facebook"] = providers.NewFacebook(cfg.FacebookAppID, cfg.FacebookAppSecret, cfg.FacebookGraphURL, limited)
	}
	if cfg.XClientID != "" {
		reg["x"] = providers.NewX(cfg.XClientID, cfg.XClientSecret)
	}
	names := make([]string, 0, len(reg))
	for n := range reg {
		names = append(names, n)
	}
	slog.Info("sign-in providers enabled", "providers", names, "otp", []string{"sms", "email"}, "password", true, "qr", true)

	lim := redisx.Limiter{R: rdb, Prefix: "rl:id:"}
	svc := &idapp.Service{
		Pool: pool, Redis: rdb, Limiter: lim, Signer: signer, Providers: reg,
		OTP: &otp.Service{R: rdb, Limiter: lim, Secret: cfg.OTPSecret, Region: cfg.DefaultRegion,
			SMS: sms, Email: email, TestCodes: cfg.OTPTestNumbers},
		RefreshTTL: cfg.RefreshTTL, SessionMaxAge: cfg.SessionMaxAge,
	}
	return svc, func() { pool.Close(); _ = rdb.Close() }, nil
}

func serve(ctx context.Context) error {
	cfg, err := load()
	if err != nil {
		return err
	}
	svc, closeFn, err := build(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeFn()
	keys := auth.StaticKeys{}
	for _, k := range cfg.SigningKeys {
		keys[k.KID] = k.Private.Public().(ed25519.PublicKey)
	}
	a := &api.API{Svc: svc, Verifier: &auth.Verifier{Keys: keys}}
	srv := httpx.NewServer(cfg.Addr, a.Routes(),
		httpx.Check{Name: "postgres", Fn: svc.Pool.Ping},
		httpx.Check{Name: "redis", Fn: func(ctx context.Context) error { return svc.Redis.Ping(ctx).Err() }},
	)
	return srv.Run(ctx)
}

// worker runs the outbox relay, the moderation consumer and cleanup jobs.
func worker(ctx context.Context) error {
	cfg, err := load()
	if err != nil {
		return err
	}
	svc, closeFn, err := build(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeFn()
	prod, err := kafkax.NewProducer(cfg.Kafka, "svc-identity")
	if err != nil {
		return err
	}
	defer prod.Close()
	relay := outbox.NewRelay("identity", svc.Pool, prod)
	mod, err := kafkax.NewConsumer(cfg.Kafka, kafkax.ConsumerOptions{
		Group: "identity-moderation", Topics: []string{events.TopicModeration}, Handle: svc.ApplyModeration,
	})
	if err != nil {
		return err
	}
	srv := httpx.NewServer(cfg.Addr, nil,
		httpx.Check{Name: "postgres", Fn: svc.Pool.Ping},
		httpx.Check{Name: "kafka", Fn: prod.Ping},
	)
	return app.Group(ctx, srv.Run, relay.Run, mod.Run, func(ctx context.Context) error {
		t := time.NewTicker(10 * time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
				if n, err := store.PruneSessions(ctx, svc.Pool); err != nil {
					slog.Error("prune sessions", "err", err)
				} else if n > 0 {
					slog.Info("pruned sessions", "count", n)
				}
				if err := outbox.PruneProcessed(ctx, svc.Pool, 14*24*time.Hour); err != nil {
					slog.Error("prune processed events", "err", err)
				}
			}
		}
	})
}
