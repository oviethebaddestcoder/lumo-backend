// Command wallet runs svc-wallet: coins, gifts, diamonds, purchases, payouts.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/app"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/config"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/gauth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/httpx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/kafkax"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/logx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/metrics"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/outbox"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/shard"
	"github.com/oviethebaddestcoder/lumo-backend/svc-wallet/internal/api"
	wapp "github.com/oviethebaddestcoder/lumo-backend/svc-wallet/internal/app"
	"github.com/oviethebaddestcoder/lumo-backend/svc-wallet/internal/ledger"
	"github.com/oviethebaddestcoder/lumo-backend/svc-wallet/internal/stores"
	"github.com/oviethebaddestcoder/lumo-backend/svc-wallet/migrations"
)

func main() {
	app.Main("svc-wallet", app.Commands{"serve": serve, "worker": worker, "migrate": migrate})
}

type cfg struct {
	addr, jwks          string
	shards, shardsDir   string
	kafka               kafkax.Config
	svc                 *wapp.Service
	logical             int
	googleSA, appleKey  []byte
	googlePkg           string
	appleIssuer, appleK string
	appleBundle         string
	appleSandbox        bool
	paystackKey         string
}

func load() (cfg, error) {
	e := config.New()
	logx.New("svc-wallet", e.Str("LOG_LEVEL", "info"))
	c := cfg{
		addr:         e.Str("ADDR", ":8080"),
		jwks:         e.Str("JWKS_URL", "http://svc-identity/.well-known/jwks.json"),
		shards:       e.Required("SHARDS"),
		shardsDir:    e.Str("SHARDS_DIRECT", ""),
		logical:      e.Int("LOGICAL_SHARDS", shard.DefaultLogical),
		kafka:        kafkax.ConfigFromEnv(e, "svc-wallet"),
		googleSA:     e.Base64("GOOGLE_PLAY_SERVICE_ACCOUNT", false),
		googlePkg:    e.Str("GOOGLE_PLAY_PACKAGE", ""),
		appleKey:     e.Base64("APPLE_IAP_KEY_P8", false),
		appleIssuer:  e.Str("APPLE_IAP_ISSUER_ID", ""),
		appleK:       e.Str("APPLE_IAP_KEY_ID", ""),
		appleBundle:  e.Str("APPLE_BUNDLE_ID", ""),
		appleSandbox: e.Bool("APPLE_IAP_SANDBOX", false),
		paystackKey:  e.Str("PAYSTACK_SECRET_KEY", ""),
	}
	c.svc = &wapp.Service{
		DiamondPercent: e.Int64("DIAMONDS_PER_100_COINS", 50),
		MinPayout:      e.Int64("MIN_PAYOUT_DIAMONDS", 10_000),
		KoboPerDiamond: e.Int64("PAYOUT_KOBO_PER_DIAMOND", 50),
		AccountSecret:  e.Base64("ACCOUNT_TAG_SECRET", true),
	}
	if c.shardsDir == "" {
		c.shardsDir = c.shards
	}
	return c, e.Err()
}

func migrate(ctx context.Context) error {
	c, err := load()
	if err != nil {
		return err
	}
	rs, err := shard.Parse(c.shardsDir, c.logical)
	if err != nil {
		return err
	}
	for i, r := range rs {
		slog.Info("migrating shard", "index", i, "range", fmt.Sprintf("%d-%d", r.From, r.To))
		if err := db.Migrate(ctx, r.DSN, migrations.FS, migrations.LockID); err != nil {
			return fmt.Errorf("shard %d: %w", i, err)
		}
	}
	return nil
}

func build(ctx context.Context, c cfg) (*wapp.Service, error) {
	router, err := shard.Open(ctx, c.shards, c.logical, db.Options{MaxConns: 30})
	if err != nil {
		return nil, err
	}
	s := c.svc
	s.Shards = router
	if len(c.googleSA) > 0 {
		ts, err := gauth.New(c.googleSA, nil, "https://www.googleapis.com/auth/androidpublisher")
		if err != nil {
			return nil, err
		}
		s.Google = &stores.GooglePlay{Package: c.googlePkg, Tokens: ts}
	}
	if len(c.appleKey) > 0 {
		k, err := stores.ParseAppleKey(c.appleKey)
		if err != nil {
			return nil, fmt.Errorf("APPLE_IAP_KEY_P8: %w", err)
		}
		s.Apple = &stores.AppStore{IssuerID: c.appleIssuer, KeyID: c.appleK, Key: k, BundleID: c.appleBundle, Sandbox: c.appleSandbox}
	}
	if c.paystackKey != "" {
		s.Paystack = &stores.Paystack{SecretKey: c.paystackKey}
	}
	slog.Info("wallet ready", "shards", len(router.All()), "google_play", s.Google != nil, "app_store", s.Apple != nil, "paystack", s.Paystack != nil)
	return s, nil
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
	defer s.Shards.Close()
	a := &api.API{Svc: s, Verifier: &auth.Verifier{Keys: auth.NewRemoteJWKS(c.jwks)}}
	return httpx.NewServer(c.addr, a.Routes(), httpx.Check{Name: "shards", Fn: s.Shards.Ping}).Run(ctx)
}

var reconcileBad = metrics.NewGaugeVec("wallet_reconcile_problems", "Unbalanced transactions or mismatched balances found by reconciliation (alert if > 0).", "shard", "kind")

func worker(ctx context.Context) error {
	c, err := load()
	if err != nil {
		return err
	}
	s, err := build(ctx, c)
	if err != nil {
		return err
	}
	defer s.Shards.Close()
	prod, err := kafkax.NewProducer(c.kafka, "svc-wallet")
	if err != nil {
		return err
	}
	defer prod.Close()

	var tasks []func(context.Context) error
	// One outbox relay per shard.
	for _, r := range s.Shards.All() {
		tasks = append(tasks, outbox.NewRelay(fmt.Sprintf("wallet-%d", r.Index), r.Pool, prod).Run)
	}
	credit, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{
		Group: "wallet-credit", Topics: []string{events.TopicWalletGifts}, HandleBatch: s.CreditGifts, BatchSize: 1000,
	})
	if err != nil {
		return err
	}
	tasks = append(tasks, credit.Run)
	if s.Google != nil {
		consume, err := kafkax.NewConsumer(c.kafka, kafkax.ConsumerOptions{
			Group: "wallet-google-consume", Topics: []string{events.TopicWalletEvents},
			Handle: func(ctx context.Context, e events.Envelope) error {
				if e.Type != events.CoinsPurchased {
					return nil
				}
				var p events.CoinsPurchasedData
				if err := e.Decode(&p); err != nil || p.Channel != "google_play" {
					return nil
				}
				if err := s.Google.Consume(ctx, p.ProductID, p.StoreRef); err != nil && !isAlreadyConsumed(err) {
					return err
				}
				return nil
			},
		})
		if err != nil {
			return err
		}
		tasks = append(tasks, consume.Run, every(10*time.Minute, func(ctx context.Context) {
			toks, err := s.Google.Voided(ctx, time.Now().Add(-24*time.Hour))
			if err != nil {
				slog.Error("google voided purchases", "err", err)
				return
			}
			for _, t := range toks {
				if err := s.Reverse(ctx, "google_play", t, "google_voided"); err != nil {
					slog.Error("reverse google purchase", "err", err)
				}
			}
		}))
	}
	tasks = append(tasks,
		every(6*time.Hour, func(ctx context.Context) {
			for _, r := range s.Shards.All() {
				if _, err := r.Pool.Exec(ctx, `SELECT wallet_ensure_partitions(3)`); err != nil {
					slog.Error("ensure partitions", "shard", r.Index, "err", err)
				}
				_, _ = r.Pool.Exec(ctx, `DELETE FROM gift_credits WHERE gift_send_id IN (
					SELECT gift_send_id FROM gift_credits WHERE credited_at < now() - interval '60 days' LIMIT 10000)`)
				_ = outbox.PruneProcessed(ctx, r.Pool, 14*24*time.Hour)
			}
		}),
		every(24*time.Hour, func(ctx context.Context) {
			for _, r := range s.Shards.All() {
				u, b, err := ledger.Reconcile(ctx, r.Pool)
				if err != nil {
					slog.Error("reconcile", "shard", r.Index, "err", err)
					continue
				}
				reconcileBad.WithLabelValues(fmt.Sprint(r.Index), "unbalanced_tx").Set(float64(u))
				reconcileBad.WithLabelValues(fmt.Sprint(r.Index), "balance_mismatch").Set(float64(b))
				if u+b > 0 {
					slog.Error("LEDGER RECONCILIATION FAILED", "shard", r.Index, "unbalanced_tx", u, "balance_mismatch", b)
				}
			}
		}),
		httpx.NewServer(c.addr, nil, httpx.Check{Name: "shards", Fn: s.Shards.Ping}, httpx.Check{Name: "kafka", Fn: prod.Ping}).Run,
	)
	return app.Group(ctx, tasks...)
}

func every(d time.Duration, fn func(context.Context)) func(context.Context) error {
	return func(ctx context.Context) error {
		fn(ctx)
		t := time.NewTicker(d)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
				fn(ctx)
			}
		}
	}
}

func isAlreadyConsumed(err error) bool {
	// Google answers 400 when a token was already consumed; ErrInvalid covers it.
	return err != nil && errors.Is(err, stores.ErrInvalid)
}
