package app_test

// Integration tests against real Postgres and Redis.
// Run: LUMO_TEST_PG=postgres://postgres@localhost:5432/postgres LUMO_TEST_REDIS=redis://localhost:6379/15 go test ./...

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/redisx"
	"github.com/oviethebaddestcoder/lumo-backend/svc-identity/internal/app"
	"github.com/oviethebaddestcoder/lumo-backend/svc-identity/internal/otp"
	"github.com/oviethebaddestcoder/lumo-backend/svc-identity/internal/providers"
	"github.com/oviethebaddestcoder/lumo-backend/svc-identity/migrations"
	"github.com/redis/go-redis/v9"
)

type captureSender struct {
	mu    sync.Mutex
	codes map[string]string
}

func (c *captureSender) Send(_ context.Context, to, code string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.codes[to] = code
	return nil
}

type fakeProvider struct{}

func (fakeProvider) Name() string { return "test" }
func (fakeProvider) Verify(_ context.Context, c providers.Credential) (providers.External, error) {
	if c.IDToken == "bad" {
		return providers.External{}, providers.ErrInvalid
	}
	return providers.External{Provider: "test", Subject: c.IDToken, Email: c.IDToken + "@example.com", EmailVerified: true, Name: "Ada Obi"}, nil
}

type env struct {
	svc      *app.Service
	pool     *pgxpool.Pool
	sms      *captureSender
	verifier *auth.Verifier
	now      time.Time
}

func setup(t *testing.T) *env {
	t.Helper()
	pg, rurl := os.Getenv("LUMO_TEST_PG"), os.Getenv("LUMO_TEST_REDIS")
	if pg == "" || rurl == "" {
		t.Skip("set LUMO_TEST_PG and LUMO_TEST_REDIS to run integration tests")
	}
	ctx := context.Background()
	name := fmt.Sprintf("identity_test_%d", time.Now().UnixNano())
	admin, err := pgx.Connect(ctx, pg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	cfg, _ := pgx.ParseConfig(pg)
	dsn := fmt.Sprintf("postgres://%s@%s:%d/%s", cfg.User, cfg.Host, cfg.Port, name)
	t.Cleanup(func() {
		admin.Exec(context.Background(), "DROP DATABASE "+name+" WITH (FORCE)")
		admin.Close(context.Background())
	})
	if err := db.Migrate(ctx, dsn, migrations.FS, migrations.LockID); err != nil {
		t.Fatal(err)
	}
	pool, err := db.Open(ctx, dsn, db.Options{MaxConns: 30})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	rdb, err := redisx.Open(ctx, rurl, "", "")
	if err != nil {
		t.Fatal(err)
	}
	rdb.FlushDB(ctx)
	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner(15*time.Minute, auth.SigningKey{KID: "k1", Private: priv})
	sms := &captureSender{codes: map[string]string{}}
	lim := redisx.Limiter{R: rdb, Prefix: "t:"}
	e := &env{pool: pool, sms: sms, now: time.Now()}
	e.svc = &app.Service{
		Pool: pool, Redis: rdb, Limiter: lim, Signer: signer,
		OTP: &otp.Service{R: rdb, Limiter: lim, Secret: []byte("0123456789abcdef0123456789abcdef"), Region: "NG",
			SMS: sms, Email: sms, TestCodes: map[string]string{"+2348000000001": "123456"}},
		Providers:  providers.Registry{"test": fakeProvider{}},
		RefreshTTL: 60 * 24 * time.Hour, SessionMaxAge: 365 * 24 * time.Hour,
		Now: func() time.Time { return e.now },
	}
	e.verifier = &auth.Verifier{Keys: auth.StaticKeys{"k1": priv.Public().(ed25519.PublicKey)}}
	return e
}

func dev() app.Device {
	return app.Device{ID: "d1", Name: "Pixel 8", Platform: "android", IP: "10.0.0.1"}
}

func TestPhoneOTPSignUpAndSignIn(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	target, _, err := e.svc.SendOTP(ctx, otp.ChannelSMS, "0803 123 4567", "1.1.1.1")
	if err != nil {
		t.Fatal(err)
	}
	if target != "+2348031234567" {
		t.Fatalf("normalized = %q", target)
	}
	// Second send within a minute is rate limited.
	if _, _, err := e.svc.SendOTP(ctx, otp.ChannelSMS, "+2348031234567", "1.1.1.1"); err == nil {
		t.Fatal("expected rate limit")
	}
	code := e.sms.codes[target]
	if _, err := e.svc.VerifyOTP(ctx, otp.ChannelSMS, target, "000000", dev(), app.NewUserInfo{}); err == nil && code != "000000" {
		t.Fatal("wrong code accepted")
	}
	bd := time.Date(2000, 5, 1, 0, 0, 0, 0, time.UTC)
	res, err := e.svc.VerifyOTP(ctx, otp.ChannelSMS, target, code, dev(), app.NewUserInfo{BirthDate: &bd})
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsNew || res.User.ID == "" || res.NeedsBirthDate {
		t.Fatalf("unexpected result %+v", res)
	}
	p, err := e.verifier.Verify(ctx, res.Tokens.AccessToken)
	if err != nil || p.UserID != res.User.ID || !p.Adult {
		t.Fatalf("token: %+v %v", p, err)
	}
	// Code is single-use.
	if _, err := e.svc.VerifyOTP(ctx, otp.ChannelSMS, target, code, dev(), app.NewUserInfo{}); err == nil {
		t.Fatal("code reused")
	}
	// Reviewer test number signs in to the same flow with a fixed code.
	if _, _, err := e.svc.SendOTP(ctx, otp.ChannelSMS, "+2348000000001", "1.1.1.1"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.VerifyOTP(ctx, otp.ChannelSMS, "+2348000000001", "123456", dev(), app.NewUserInfo{}); err != nil {
		t.Fatal(err)
	}
	var n int
	e.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE payload->>'type' = 'identity.user.registered'`).Scan(&n)
	if n != 2 {
		t.Fatalf("outbox registered events = %d, want 2", n)
	}
}

func TestOTPAttemptLimit(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	target, _, _ := e.svc.SendOTP(ctx, otp.ChannelEmail, "ada@example.com", "1.1.1.2")
	for i := 0; i < 5; i++ {
		e.svc.VerifyOTP(ctx, otp.ChannelEmail, target, "999999", dev(), app.NewUserInfo{})
	}
	if _, err := e.svc.VerifyOTP(ctx, otp.ChannelEmail, target, e.sms.codes[target], dev(), app.NewUserInfo{}); err == nil {
		t.Fatal("correct code accepted after too many attempts")
	}
}

func TestConcurrentFirstSignInCreatesOneUser(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	ids := make(chan string, 25)
	errs := make(chan error, 25)
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := e.svc.SignInWithProvider(ctx, "test", providers.Credential{IDToken: "same-subject"}, dev(), app.NewUserInfo{})
			if err != nil {
				errs <- err
				return
			}
			ids <- res.User.ID
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for id := range ids {
		seen[id] = true
	}
	var users int
	e.pool.QueryRow(ctx, `SELECT count(*) FROM users`).Scan(&users)
	if len(seen) != 1 || users != 1 {
		t.Fatalf("distinct ids %d, users %d; want 1 and 1", len(seen), users)
	}
}

func TestRefreshRotationAndReuseDetection(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	res, err := e.svc.SignInWithProvider(ctx, "test", providers.Credential{IDToken: "u1"}, dev(), app.NewUserInfo{})
	if err != nil {
		t.Fatal(err)
	}
	old := res.Tokens.RefreshToken
	t2, err := e.svc.Refresh(ctx, old, dev())
	if err != nil || t2.RefreshToken == old {
		t.Fatalf("rotate: %v", err)
	}
	// Immediate reuse: treated as a client race, session kept.
	if _, err := e.svc.Refresh(ctx, old, dev()); !errors.Is(err, app.ErrRefreshRace) {
		t.Fatalf("want race, got %v", err)
	}
	t3, err := e.svc.Refresh(ctx, t2.RefreshToken, dev())
	if err != nil {
		t.Fatal(err)
	}
	// Later reuse of a rotated token: theft. Session is revoked.
	e.now = e.now.Add(time.Minute)
	if _, err := e.svc.Refresh(ctx, t2.RefreshToken, dev()); !errors.Is(err, app.ErrRefreshInvalid) {
		t.Fatalf("want invalid, got %v", err)
	}
	if _, err := e.svc.Refresh(ctx, t3.RefreshToken, dev()); !errors.Is(err, app.ErrRefreshInvalid) {
		t.Fatalf("session should be revoked, got %v", err)
	}
	if n, _ := e.svc.Redis.Exists(ctx, auth.RevokedSessionKey(res.Tokens.SessionID)).Result(); n != 1 {
		t.Fatal("revoked session not on denylist")
	}
}

func TestPasswordLoginAndProfile(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	res, _ := e.svc.SignInWithProvider(ctx, "test", providers.Credential{IDToken: "pw"}, dev(), app.NewUserInfo{})
	p := auth.Principal{UserID: res.User.ID, SessionID: res.Tokens.SessionID}
	h := "ada.obi"
	if _, err := e.svc.UpdateProfile(ctx, p.UserID, app.ProfileUpdate{Handle: &h}); err != nil {
		t.Fatal(err)
	}
	if err := e.svc.SetPassword(ctx, p, "", "correct horse battery"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.PasswordLogin(ctx, "@ada.obi", "wrong password", dev()); !errors.Is(err, app.ErrBadCredentials) {
		t.Fatalf("want bad credentials, got %v", err)
	}
	if _, err := e.svc.PasswordLogin(ctx, "ada.obi", "correct horse battery", dev()); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.PasswordLogin(ctx, "pw@example.com", "correct horse battery", dev()); err != nil {
		t.Fatal(err)
	}
	// Another user can't take the handle.
	res2, _ := e.svc.SignInWithProvider(ctx, "test", providers.Credential{IDToken: "other"}, dev(), app.NewUserInfo{})
	if _, err := e.svc.UpdateProfile(ctx, res2.User.ID, app.ProfileUpdate{Handle: &h}); !errors.Is(err, app.ErrHandleTaken) {
		t.Fatalf("want handle taken, got %v", err)
	}
	// Search: handle or display-name prefix, exact/handle match first, @ ignored, wildcards literal.
	if got, err := e.svc.SearchUsers(ctx, "@ADA"); err != nil || len(got) != 2 || got[0].Handle != "ada.obi" {
		t.Fatalf("search: %v %+v", err, got)
	}
	if got, _ := e.svc.SearchUsers(ctx, "a%"); len(got) != 0 {
		t.Fatalf("wildcard leaked: %+v", got)
	}
	if got, _ := e.svc.SearchUsers(ctx, "a"); len(got) != 0 {
		t.Fatalf("1-char query should return nothing: %+v", got)
	}
}

func TestQRLogin(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	phone, _ := e.svc.SignInWithProvider(ctx, "test", providers.Credential{IDToken: "qr"}, dev(), app.NewUserInfo{})
	q, err := e.svc.QRCreate(ctx, app.Device{Name: "Chrome on Mac", Platform: "web"})
	if err != nil {
		t.Fatal(err)
	}
	if st, _, _ := e.svc.QRPoll(ctx, q.ID, q.PollToken, dev()); st != "pending" {
		t.Fatalf("status %q", st)
	}
	if _, _, err := e.svc.QRPoll(ctx, q.ID, "wrong-token", dev()); err == nil {
		t.Fatal("wrong poll token accepted")
	}
	if err := e.svc.QRApprove(ctx, auth.Principal{UserID: phone.User.ID}, q.ID); err != nil {
		t.Fatal(err)
	}
	st, res, err := e.svc.QRPoll(ctx, q.ID, q.PollToken, dev())
	if err != nil || st != "approved" || res.User.ID != phone.User.ID {
		t.Fatalf("poll: %s %v", st, err)
	}
	if st, res, _ := e.svc.QRPoll(ctx, q.ID, q.PollToken, dev()); st != "consumed" || res != nil {
		t.Fatal("QR must be single use")
	}
}

func TestDeleteAccountScrubs(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	res, _ := e.svc.SignInWithProvider(ctx, "test", providers.Credential{IDToken: "gone"}, dev(), app.NewUserInfo{})
	p := auth.Principal{UserID: res.User.ID, SessionID: res.Tokens.SessionID}
	if err := e.svc.DeleteAccount(ctx, p); err != nil {
		t.Fatal(err)
	}
	var email *string
	var status string
	e.pool.QueryRow(ctx, `SELECT email, status FROM users WHERE id = $1`, p.UserID).Scan(&email, &status)
	if email != nil || status != "deleted" {
		t.Fatalf("not scrubbed: %v %s", email, status)
	}
	if _, err := e.svc.Refresh(ctx, res.Tokens.RefreshToken, dev()); err == nil {
		t.Fatal("refresh after delete")
	}
	// Signing in again with the same provider creates a fresh account.
	res2, err := e.svc.SignInWithProvider(ctx, "test", providers.Credential{IDToken: "gone"}, dev(), app.NewUserInfo{})
	if err != nil || res2.User.ID == p.UserID || !res2.IsNew {
		t.Fatalf("re-signup: %v", err)
	}
	_ = redis.Nil
}
