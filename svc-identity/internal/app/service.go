// Package app is svc-identity's business logic.
package app

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"log/slog"
	"math/big"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/errs"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/outbox"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/redisx"
	"github.com/oviethebaddestcoder/lumo-backend/svc-identity/internal/otp"
	"github.com/oviethebaddestcoder/lumo-backend/svc-identity/internal/providers"
	"github.com/oviethebaddestcoder/lumo-backend/svc-identity/internal/store"
	"github.com/redis/go-redis/v9"
)

const producer = "svc-identity"

// refreshRaceWindow: if a refresh token is reused within this window right
// after rotation, assume two concurrent requests from the same app, not theft.
const refreshRaceWindow = 30 * time.Second

var (
	ErrAccountSuspended = errs.Forbidden("account_suspended", "This account is suspended.")
	ErrAccountDeleted   = errs.Forbidden("account_deleted", "This account was deleted.")
	ErrBadCredentials   = errs.Unauthorized("bad_credentials", "Wrong login or password.")
	ErrRefreshInvalid   = errs.Unauthorized("refresh_invalid", "Your session has ended. Sign in again.")
	ErrRefreshRace      = errs.Conflict("refresh_in_progress", "Session was just refreshed. Use the newest token.")
	ErrIdentityTaken    = errs.Conflict("identity_linked_elsewhere", "That account is already linked to another Lumo user.")
	ErrLastLoginMethod  = errs.Conflict("last_login_method", "Add another way to sign in before removing this one.")
	ErrHandleTaken      = errs.Conflict("handle_taken", "That username is taken.")
)

type Service struct {
	Pool      *pgxpool.Pool
	Redis     redis.UniversalClient
	Limiter   redisx.Limiter
	Signer    *auth.Signer
	OTP       *otp.Service
	Providers providers.Registry

	RefreshTTL    time.Duration
	SessionMaxAge time.Duration
	Now           func() time.Time
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Device describes the client signing in.
type Device struct {
	ID        string `json:"device_id"`
	Name      string `json:"device_name"`
	Platform  string `json:"platform"` // ios | android | web
	IP        string `json:"-"`
	UserAgent string `json:"-"`
}

// Tokens are returned on every successful sign-in or refresh.
type Tokens struct {
	AccessToken      string    `json:"access_token"`
	AccessExpiresAt  time.Time `json:"access_expires_at"`
	RefreshToken     string    `json:"refresh_token"`
	RefreshExpiresAt time.Time `json:"refresh_expires_at"`
	TokenType        string    `json:"token_type"`
	SessionID        string    `json:"session_id"`
}

// SignInResult is the response for every sign-in method.
type SignInResult struct {
	Tokens Tokens            `json:"tokens"`
	User   events.UserPublic `json:"user"`
	IsNew  bool              `json:"is_new"`
	// NeedsBirthDate tells the app to ask for a birthday (required for live/gifts).
	NeedsBirthDate bool `json:"needs_birth_date"`
}

// NewUserInfo is optional data captured on sign-up.
type NewUserInfo struct {
	BirthDate   *time.Time
	DisplayName string
}

// ---- sign-in entry points ----

func (s *Service) SendOTP(ctx context.Context, channel, target, ip string) (string, time.Duration, error) {
	norm, err := s.OTP.Normalize(channel, target)
	if err != nil {
		return "", 0, err
	}
	ttl, err := s.OTP.Send(ctx, channel, norm, ip)
	return norm, ttl, err
}

func (s *Service) VerifyOTP(ctx context.Context, channel, target, code string, dev Device, info NewUserInfo) (SignInResult, error) {
	norm, err := s.OTP.Normalize(channel, target)
	if err != nil {
		return SignInResult{}, err
	}
	if err := s.OTP.Verify(ctx, channel, norm, code); err != nil {
		return SignInResult{}, err
	}
	ext := providers.External{Provider: "phone", Subject: norm}
	if channel == otp.ChannelEmail {
		ext = providers.External{Provider: "email", Subject: norm, Email: norm, EmailVerified: true}
	}
	return s.signIn(ctx, ext, dev, info)
}

func (s *Service) SignInWithProvider(ctx context.Context, name string, cred providers.Credential, dev Device, info NewUserInfo) (SignInResult, error) {
	ext, err := s.verifyProvider(ctx, name, cred)
	if err != nil {
		return SignInResult{}, err
	}
	if info.DisplayName == "" {
		info.DisplayName = ext.Name
	}
	return s.signIn(ctx, ext, dev, info)
}

func (s *Service) verifyProvider(ctx context.Context, name string, cred providers.Credential) (providers.External, error) {
	p, err := s.Providers.Get(name)
	if err != nil {
		return providers.External{}, errs.BadRequest("provider_unsupported", "That sign-in option isn't available.")
	}
	ext, err := p.Verify(ctx, cred)
	if err != nil {
		if errors.Is(err, providers.ErrInvalid) {
			return ext, errs.Unauthorized("provider_token_invalid", "Sign-in failed. Please try again.").Wrap(err)
		}
		return ext, errs.Unavailable("provider_unavailable", "That sign-in option is having trouble. Try another.").Wrap(err)
	}
	return ext, nil
}

// PasswordLogin signs in with email, phone or username plus password.
func (s *Service) PasswordLogin(ctx context.Context, login, password string, dev Device) (SignInResult, error) {
	login = strings.TrimSpace(login)
	for _, k := range []struct {
		key   string
		limit int
	}{{"pw:login:" + strings.ToLower(login), 10}, {"pw:ip:" + dev.IP, 50}} {
		res, err := s.Limiter.Allow(ctx, k.key, k.limit, 15*time.Minute)
		if err != nil || !res.Allowed {
			return SignInResult{}, errs.ErrRateLimited.Wrap(err)
		}
	}
	var u store.User
	var err error
	switch {
	case strings.Contains(login, "@"):
		u, err = store.GetUserByEmail(ctx, s.Pool, login)
	case strings.HasPrefix(login, "+") || isDigits(login):
		if norm, nerr := s.OTP.Normalize(otp.ChannelSMS, login); nerr == nil {
			u, err = store.GetUserByPhone(ctx, s.Pool, norm)
		} else {
			err = store.ErrNotFound
		}
	default:
		u, err = store.GetUserByHandle(ctx, s.Pool, strings.ToLower(strings.TrimPrefix(login, "@")))
	}
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return SignInResult{}, err
	}
	hash := dummyHash
	if err == nil && u.PasswordHash != nil {
		hash = *u.PasswordHash
	}
	ok, cerr := CheckPassword(hash, password)
	if err != nil || u.PasswordHash == nil || cerr != nil || !ok {
		return SignInResult{}, ErrBadCredentials
	}
	if err := checkStatus(u, s.now()); err != nil {
		return SignInResult{}, err
	}
	var res SignInResult
	err = db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		t, err := s.createSession(ctx, tx, u, "password", dev)
		res = SignInResult{Tokens: t, User: u.Public(s.now()), NeedsBirthDate: u.BirthDate == nil}
		return err
	})
	return res, err
}

// signIn finds or creates the user for an external identity and opens a session.
func (s *Service) signIn(ctx context.Context, ext providers.External, dev Device, info NewUserInfo) (SignInResult, error) {
	var res SignInResult
	attempt := func() error {
		return db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
			res = SignInResult{}
			uid, err := store.FindIdentity(ctx, tx, ext.Provider, ext.Subject)
			var u store.User
			switch {
			case err == nil:
				if u, err = store.GetUser(ctx, tx, uid); err != nil {
					return err
				}
				if err := checkStatus(u, s.now()); err != nil {
					return err
				}
			case errors.Is(err, store.ErrNotFound):
				// New account. We deliberately do NOT auto-link to an existing
				// account with the same email: that enables account takeover via
				// a provider with weaker email checks. Users link explicitly.
				if u, err = s.createUser(ctx, tx, ext, info); err != nil {
					return err
				}
				res.IsNew = true
			default:
				return err
			}
			t, err := s.createSession(ctx, tx, u, ext.Provider, dev)
			if err != nil {
				return err
			}
			res.Tokens, res.User, res.NeedsBirthDate = t, u.Public(s.now()), u.BirthDate == nil
			return nil
		})
	}
	// Concurrent first-time sign-ins race on the identity, email or phone
	// unique indexes. The loser retries: it finds the winner's account (same
	// identity) or, for an email owned by someone else, signs up without it.
	err := attempt()
	for i := 0; i < 3 && (db.IsUniqueViolation(err, "identities_pkey") ||
		db.IsUniqueViolation(err, "users_email_key") || db.IsUniqueViolation(err, "users_phone_key")); i++ {
		err = attempt()
	}
	return res, err
}

func (s *Service) createUser(ctx context.Context, tx pgx.Tx, ext providers.External, info NewUserInfo) (store.User, error) {
	u := store.User{
		ID:          events.NewID(),
		DisplayName: cleanDisplayName(info.DisplayName),
		AvatarURL:   safeAvatar(ext.AvatarURL),
		BirthDate:   info.BirthDate,
		Status:      "active",
		CreatedAt:   s.now(),
	}
	// Contact details: store only verified ones, and only if not already used.
	switch ext.Provider {
	case "phone":
		p := ext.Subject
		u.Phone = &p
	default:
		if ext.Email != "" && ext.EmailVerified {
			if _, err := store.GetUserByEmail(ctx, tx, ext.Email); errors.Is(err, store.ErrNotFound) {
				e := strings.ToLower(ext.Email)
				u.Email, u.EmailVerified = &e, true
			}
		}
	}
	if u.DisplayName == "" {
		u.DisplayName = "Lumo user"
	}
	base := handleBase(ext, u.DisplayName)
	var err error
	for i := 0; i < 8; i++ {
		u.Handle = base + randomDigits(4+i/2)
		if err = s.insertUserSavepoint(ctx, tx, u); err == nil {
			break
		}
		if !db.IsUniqueViolation(err, "users_handle_key") {
			return u, err
		}
	}
	if err != nil {
		return u, err
	}
	var emailPtr *string
	if ext.Email != "" {
		emailPtr = &ext.Email
	}
	if err := store.InsertIdentity(ctx, tx, ext.Provider, ext.Subject, u.ID, emailPtr); err != nil {
		return u, err
	}
	env := events.MustNew(producer, events.UserRegistered, u.ID, events.UserRegisteredData{User: u.Public(s.now()), Method: ext.Provider})
	return u, outbox.Insert(ctx, tx, events.TopicIdentityUsers, env)
}

// insertUserSavepoint lets a handle collision be retried inside the outer tx.
func (s *Service) insertUserSavepoint(ctx context.Context, tx pgx.Tx, u store.User) error {
	sp, err := tx.Begin(ctx)
	if err != nil {
		return err
	}
	if err := store.InsertUser(ctx, sp, u); err != nil {
		_ = sp.Rollback(ctx)
		return err
	}
	return sp.Commit(ctx)
}

func (s *Service) createSession(ctx context.Context, tx pgx.Tx, u store.User, method string, dev Device) (Tokens, error) {
	raw, hash := newRefreshToken()
	sess := store.Session{
		ID: events.NewID(), UserID: u.ID, RefreshHash: hash, Method: method,
		DeviceID: trunc(dev.ID, 128), DeviceName: trunc(dev.Name, 128), Platform: trunc(dev.Platform, 16),
		IP: dev.IP, UserAgent: trunc(dev.UserAgent, 256), ExpiresAt: s.now().Add(s.RefreshTTL),
	}
	if err := store.InsertSession(ctx, tx, sess); err != nil {
		return Tokens{}, err
	}
	return s.tokens(u, sess.ID, raw, sess.ExpiresAt)
}

func (s *Service) tokens(u store.User, sid, refresh string, refreshExp time.Time) (Tokens, error) {
	at, exp, err := s.Signer.Issue(u.ID, sid, u.Adult(s.now()), u.Roles)
	if err != nil {
		return Tokens{}, err
	}
	return Tokens{AccessToken: at, AccessExpiresAt: exp, RefreshToken: refresh, RefreshExpiresAt: refreshExp,
		TokenType: "Bearer", SessionID: sid}, nil
}

// Refresh rotates the refresh token. Presenting an already-rotated token
// (outside the short race window) means it was stolen: the session dies.
func (s *Service) Refresh(ctx context.Context, raw string, dev Device) (Tokens, error) {
	hash, ok := hashRefresh(raw)
	if !ok {
		return Tokens{}, ErrRefreshInvalid
	}
	var out Tokens
	var reuse *store.Session
	err := db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		reuse = nil
		sess, err := store.SessionByRefreshForUpdate(ctx, tx, hash)
		if errors.Is(err, store.ErrNotFound) {
			prev, perr := store.SessionByPrevRefresh(ctx, tx, hash)
			if perr != nil {
				return ErrRefreshInvalid
			}
			if prev.RotatedAt != nil && s.now().Sub(*prev.RotatedAt) < refreshRaceWindow && prev.RevokedAt == nil {
				return ErrRefreshRace
			}
			reuse = &prev
			return store.RevokeSession(ctx, tx, prev.ID, "refresh_reuse")
		}
		if err != nil {
			return err
		}
		now := s.now()
		if sess.RevokedAt != nil || now.After(sess.ExpiresAt) || now.Sub(sess.CreatedAt) > s.SessionMaxAge {
			return ErrRefreshInvalid
		}
		u, err := store.GetUser(ctx, tx, sess.UserID)
		if err != nil {
			return err
		}
		if err := checkStatus(u, now); err != nil {
			return err
		}
		newRaw, newHash := newRefreshToken()
		exp := now.Add(s.RefreshTTL)
		if err := store.RotateSession(ctx, tx, sess.ID, newHash, hash, exp, dev.IP); err != nil {
			return err
		}
		out, err = s.tokens(u, sess.ID, newRaw, exp)
		return err
	})
	if reuse != nil && err == nil {
		slog.WarnContext(ctx, "refresh token reuse detected; session revoked", "session_id", reuse.ID, "user_id", reuse.UserID, "ip", dev.IP)
		s.denySessions(ctx, reuse.ID)
		return Tokens{}, ErrRefreshInvalid
	}
	return out, err
}

// Logout revokes the session that owns raw.
func (s *Service) Logout(ctx context.Context, p auth.Principal) error {
	if err := store.RevokeSession(ctx, s.Pool, p.SessionID, "logout"); err != nil {
		return err
	}
	s.denySessions(ctx, p.SessionID)
	return nil
}

// LogoutAll revokes every session for the user (optionally keeping the current one).
func (s *Service) LogoutAll(ctx context.Context, p auth.Principal, keepCurrent bool) error {
	keep := ""
	if keepCurrent {
		keep = p.SessionID
	}
	ids, err := store.RevokeUserSessions(ctx, s.Pool, p.UserID, "logout_all", keep)
	if err != nil {
		return err
	}
	s.denySessions(ctx, ids...)
	return nil
}

// denySessions makes already-issued access tokens fail at the gateway now,
// rather than when they expire.
func (s *Service) denySessions(ctx context.Context, ids ...string) {
	if len(ids) == 0 {
		return
	}
	pipe := s.Redis.Pipeline()
	for _, id := range ids {
		pipe.Set(ctx, auth.RevokedSessionKey(id), 1, s.Signer.TTL()+time.Minute)
	}
	if _, err := pipe.Exec(context.WithoutCancel(ctx)); err != nil {
		slog.ErrorContext(ctx, "session denylist write failed", "err", err, "count", len(ids))
	}
}

// ---- helpers ----

func checkStatus(u store.User, now time.Time) error {
	switch u.Status {
	case "deleted":
		return ErrAccountDeleted
	case "suspended":
		if u.SuspendedUntil == nil || now.Before(*u.SuspendedUntil) {
			return ErrAccountSuspended
		}
	}
	return nil
}

func newRefreshToken() (string, []byte) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	raw := "rt_" + base64.RawURLEncoding.EncodeToString(b)
	sum := sha256.Sum256([]byte(raw))
	return raw, sum[:]
}

func hashRefresh(raw string) ([]byte, bool) {
	if !strings.HasPrefix(raw, "rt_") || len(raw) != 46 {
		return nil, false
	}
	sum := sha256.Sum256([]byte(raw))
	return sum[:], true
}

var nonHandle = regexp.MustCompile(`[^a-z0-9_]+`)

func handleBase(ext providers.External, name string) string {
	src := name
	if ext.Email != "" {
		src, _, _ = strings.Cut(ext.Email, "@")
	}
	b := nonHandle.ReplaceAllString(strings.ToLower(src), "")
	if len(b) > 14 {
		b = b[:14]
	}
	if len(b) < 3 {
		b = "user"
	}
	return b
}

func randomDigits(n int) string {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		d, _ := rand.Int(rand.Reader, big.NewInt(10))
		sb.WriteByte(byte('0' + d.Int64()))
	}
	return sb.String()
}

func cleanDisplayName(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	for utf8.RuneCountInString(s) > 30 {
		_, size := utf8.DecodeLastRuneInString(s)
		s = s[:len(s)-size]
	}
	return s
}

func safeAvatar(u string) string {
	if strings.HasPrefix(u, "https://") && len(u) <= 512 {
		return u
	}
	return ""
}

func trunc(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
