package app

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/cache"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/errs"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/outbox"
	"github.com/oviethebaddestcoder/lumo-backend/svc-identity/internal/providers"
	"github.com/oviethebaddestcoder/lumo-backend/svc-identity/internal/store"
	"github.com/redis/go-redis/v9"
)

// Me is the signed-in user's private view of their account.
type Me struct {
	events.UserPublic
	Email       *string          `json:"email,omitempty"`
	Phone       *string          `json:"phone,omitempty"`
	BirthDate   *string          `json:"birth_date,omitempty"`
	HasPassword bool             `json:"has_password"`
	Identities  []store.Identity `json:"identities"`
	Roles       []string         `json:"roles,omitempty"`
}

func (s *Service) Me(ctx context.Context, userID string) (Me, error) {
	u, err := store.GetUser(ctx, s.Pool, userID)
	if errors.Is(err, store.ErrNotFound) {
		return Me{}, errs.ErrNotFound
	}
	if err != nil {
		return Me{}, err
	}
	ids, err := store.ListIdentities(ctx, s.Pool, userID)
	if err != nil {
		return Me{}, err
	}
	m := Me{UserPublic: u.Public(s.now()), Email: u.Email, Phone: maskPhone(u.Phone), HasPassword: u.PasswordHash != nil,
		Identities: ids, Roles: u.Roles}
	if u.BirthDate != nil {
		d := u.BirthDate.Format("2006-01-02")
		m.BirthDate = &d
	}
	return m, nil
}

var publicCache = cache.New[events.UserPublic](30*time.Second, 200_000)

// PublicUser returns a public profile by ID or handle.
func (s *Service) PublicUser(ctx context.Context, idOrHandle string, byHandle bool) (events.UserPublic, error) {
	ck := idOrHandle
	if byHandle {
		ck = "@" + strings.ToLower(idOrHandle)
	}
	if u, ok := publicCache.Get(ck); ok {
		return u, nil
	}
	var u store.User
	var err error
	if byHandle {
		u, err = store.GetUserByHandle(ctx, s.Pool, strings.ToLower(strings.TrimPrefix(idOrHandle, "@")))
	} else {
		u, err = store.GetUser(ctx, s.Pool, idOrHandle)
	}
	if errors.Is(err, store.ErrNotFound) || (err == nil && u.Status == "deleted") {
		return events.UserPublic{}, errs.ErrNotFound
	}
	if err != nil {
		return events.UserPublic{}, err
	}
	pub := u.Public(s.now())
	publicCache.Set(ck, pub)
	return pub, nil
}

// PublicUsers is the batch lookup used by other services (max 100 IDs).
func (s *Service) PublicUsers(ctx context.Context, ids []string) ([]events.UserPublic, error) {
	valid := ids[:0]
	for _, id := range ids {
		if len(id) == 36 {
			valid = append(valid, id)
		}
	}
	if len(valid) == 0 {
		return []events.UserPublic{}, nil
	}
	if len(valid) > 100 {
		return nil, errs.BadRequest("too_many_ids", "At most 100 IDs per request.")
	}
	us, err := store.GetUsers(ctx, s.Pool, valid)
	if err != nil {
		return nil, err
	}
	out := make([]events.UserPublic, 0, len(us))
	for _, u := range us {
		out = append(out, u.Public(s.now()))
	}
	return out, nil
}

// ProfileUpdate is a partial profile change.
type ProfileUpdate struct {
	Handle      *string `json:"handle,omitempty"`
	DisplayName *string `json:"display_name,omitempty"`
	Bio         *string `json:"bio,omitempty"`
	AvatarURL   *string `json:"avatar_url,omitempty"`
	BirthDate   *string `json:"birth_date,omitempty"` // YYYY-MM-DD, settable once
}

var handleRe = regexp.MustCompile(`^[a-z0-9_.]{2,24}$`)

var reservedHandles = []string{"admin", "support", "lumo", "official", "help", "security", "staff", "moderator", "root", "system"}

func (s *Service) UpdateProfile(ctx context.Context, userID string, in ProfileUpdate) (Me, error) {
	var p store.ProfilePatch
	if in.Handle != nil {
		h := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(*in.Handle), "@"))
		if !handleRe.MatchString(h) || strings.HasPrefix(h, ".") || strings.HasSuffix(h, ".") || strings.Contains(h, "..") {
			return Me{}, errs.BadRequest("handle_invalid", "Usernames are 2-24 letters, numbers, _ or .")
		}
		if slices.Contains(reservedHandles, h) || strings.HasPrefix(h, "deleted_") {
			return Me{}, ErrHandleTaken
		}
		p.Handle = &h
	}
	if in.DisplayName != nil {
		n := cleanDisplayName(*in.DisplayName)
		if n == "" {
			return Me{}, errs.BadRequest("display_name_invalid", "Name can't be empty.")
		}
		p.DisplayName = &n
	}
	if in.Bio != nil {
		b := strings.TrimSpace(*in.Bio)
		if utf8.RuneCountInString(b) > 160 {
			return Me{}, errs.BadRequest("bio_too_long", "Bio is limited to 160 characters.")
		}
		p.Bio = &b
	}
	if in.AvatarURL != nil {
		a := safeAvatar(*in.AvatarURL)
		if a == "" && *in.AvatarURL != "" {
			return Me{}, errs.BadRequest("avatar_invalid", "Avatar must be an https URL.")
		}
		p.AvatarURL = &a
	}
	if in.BirthDate != nil {
		d, err := time.Parse("2006-01-02", *in.BirthDate)
		if err != nil || d.After(s.now()) || d.Year() < 1900 {
			return Me{}, errs.BadRequest("birth_date_invalid", "Enter a valid birthday.")
		}
		p.BirthDate = &d
	}
	err := db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		u, err := store.UpdateProfile(ctx, tx, userID, p)
		if db.IsUniqueViolation(err, "users_handle_key") {
			return ErrHandleTaken
		}
		if err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, events.TopicIdentityUsers,
			events.MustNew(producer, events.UserUpdated, u.ID, events.UserUpdatedData{User: u.Public(s.now())}))
	})
	if err != nil {
		return Me{}, err
	}
	publicCache.Delete(userID)
	return s.Me(ctx, userID)
}

// SetPassword sets or changes the password. Changing it signs out other devices.
func (s *Service) SetPassword(ctx context.Context, p auth.Principal, current, next string) error {
	if n := utf8.RuneCountInString(next); n < 8 || n > 128 {
		return errs.BadRequest("password_length", "Use 8 to 128 characters.")
	}
	var revoked []string
	err := db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		u, err := store.GetUserForUpdate(ctx, tx, p.UserID)
		if err != nil {
			return err
		}
		if u.PasswordHash != nil {
			ok, _ := CheckPassword(*u.PasswordHash, current)
			if !ok {
				return errs.Unauthorized("current_password_wrong", "Your current password is wrong.")
			}
		}
		h, err := HashPassword(next)
		if err != nil {
			return err
		}
		if err := store.SetPassword(ctx, tx, u.ID, h); err != nil {
			return err
		}
		revoked, err = store.RevokeUserSessions(ctx, tx, u.ID, "password_changed", p.SessionID)
		return err
	})
	if err == nil {
		s.denySessions(ctx, revoked...)
	}
	return err
}

// LinkProvider attaches another sign-in method to the current account.
func (s *Service) LinkProvider(ctx context.Context, userID, name string, cred providers.Credential) error {
	ext, err := s.verifyProvider(ctx, name, cred)
	if err != nil {
		return err
	}
	var email *string
	if ext.Email != "" {
		email = &ext.Email
	}
	err = store.InsertIdentity(ctx, s.Pool, ext.Provider, ext.Subject, userID, email)
	if db.IsUniqueViolation(err, "identities_pkey") {
		owner, ferr := store.FindIdentity(ctx, s.Pool, ext.Provider, ext.Subject)
		if ferr == nil && owner == userID {
			return nil
		}
		return ErrIdentityTaken
	}
	return err
}

// UnlinkProvider removes a sign-in method, keeping at least one.
func (s *Service) UnlinkProvider(ctx context.Context, userID, provider string) error {
	return db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		u, err := store.GetUserForUpdate(ctx, tx, userID)
		if err != nil {
			return err
		}
		ids, err := store.ListIdentities(ctx, tx, userID)
		if err != nil {
			return err
		}
		remaining := 0
		for _, i := range ids {
			if i.Provider != provider {
				remaining++
			}
		}
		if remaining == 0 && u.PasswordHash == nil {
			return ErrLastLoginMethod
		}
		n, err := store.DeleteIdentity(ctx, tx, userID, provider)
		if err == nil && n == 0 {
			return errs.ErrNotFound
		}
		return err
	})
}

// DeleteAccount erases personal data immediately and signs out everywhere.
func (s *Service) DeleteAccount(ctx context.Context, p auth.Principal) error {
	var revoked []string
	err := db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		if err := store.Scrub(ctx, tx, p.UserID); err != nil {
			return err
		}
		var err error
		if revoked, err = store.RevokeUserSessions(ctx, tx, p.UserID, "account_deleted", ""); err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, events.TopicIdentityUsers,
			events.MustNew(producer, events.UserDeleted, p.UserID, events.UserDeletedData{UserID: p.UserID}))
	})
	if err != nil {
		return err
	}
	s.denySessions(ctx, revoked...)
	s.Redis.Set(context.WithoutCancel(ctx), auth.BannedUserKey(p.UserID), "deleted", s.Signer.TTL()+time.Minute)
	publicCache.Delete(p.UserID)
	return nil
}

// SessionView is a device in the "Manage devices" screen.
type SessionView struct {
	ID         string    `json:"id"`
	DeviceName string    `json:"device_name"`
	Platform   string    `json:"platform"`
	Method     string    `json:"method"`
	IP         string    `json:"ip"`
	CreatedAt  time.Time `json:"created_at"`
	LastUsedAt time.Time `json:"last_used_at"`
	Current    bool      `json:"current"`
}

func (s *Service) Sessions(ctx context.Context, p auth.Principal) ([]SessionView, error) {
	list, err := store.ListSessions(ctx, s.Pool, p.UserID)
	if err != nil {
		return nil, err
	}
	out := make([]SessionView, 0, len(list))
	for _, x := range list {
		out = append(out, SessionView{x.ID, x.DeviceName, x.Platform, x.Method, x.IP, x.CreatedAt, x.LastUsedAt, x.ID == p.SessionID})
	}
	return out, nil
}

// RevokeSessionByID signs out one of the user's devices.
func (s *Service) RevokeSessionByID(ctx context.Context, p auth.Principal, sid string) error {
	tag, err := s.Pool.Exec(ctx, `UPDATE sessions SET revoked_at = now(), revoke_reason = 'user_revoked'
		WHERE id::text = $1 AND user_id = $2 AND revoked_at IS NULL`, sid, p.UserID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errs.ErrNotFound
	}
	s.denySessions(ctx, sid)
	return nil
}

// ApplyModeration bans or unbans a user (consumer of moderation.actions).
func (s *Service) ApplyModeration(ctx context.Context, e events.Envelope) error {
	var a events.ModerationActionData
	if err := e.Decode(&a); err != nil {
		return err
	}
	if a.TargetType != "user" || (a.Action != "ban" && a.Action != "unban") {
		return nil
	}
	var revoked []string
	err := db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		first, err := outbox.MarkProcessed(ctx, tx, "identity-moderation", e.ID)
		if err != nil || !first {
			return err
		}
		status, until := "suspended", a.Until
		if a.Action == "unban" {
			status, until = "active", nil
		}
		u, err := store.SetStatus(ctx, tx, a.TargetID, status, until)
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if status == "suspended" {
			if revoked, err = store.RevokeUserSessions(ctx, tx, u.ID, "banned", ""); err != nil {
				return err
			}
		}
		return outbox.Insert(ctx, tx, events.TopicIdentityUsers,
			events.MustNew(producer, events.UserUpdated, u.ID, events.UserUpdatedData{User: u.Public(s.now())}))
	})
	if err != nil {
		return err
	}
	if a.Action == "ban" {
		s.denySessions(ctx, revoked...)
		s.Redis.Set(ctx, auth.BannedUserKey(a.TargetID), "banned", s.Signer.TTL()+time.Minute)
	} else {
		s.Redis.Del(ctx, auth.BannedUserKey(a.TargetID))
	}
	publicCache.Delete(a.TargetID)
	return nil
}

// ---- QR login (sign in on the web by scanning with the phone app) ----

type QRStart struct {
	ID        string `json:"qr_id"`
	PollToken string `json:"poll_token"`
	Payload   string `json:"qr_payload"`
	ExpiresIn int    `json:"expires_in"`
}

const qrTTL = 2 * time.Minute

func qrKey(id string) string { return "qr:" + id }

func (s *Service) QRCreate(ctx context.Context, dev Device) (QRStart, error) {
	id := events.NewID()
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	poll := base64.RawURLEncoding.EncodeToString(b)
	sum := sha256.Sum256([]byte(poll))
	k := qrKey(id)
	pipe := s.Redis.TxPipeline()
	pipe.HSet(ctx, k, "status", "pending", "poll", hex.EncodeToString(sum[:]), "device", trunc(dev.Name, 64),
		"platform", dev.Platform, "ip", dev.IP, "ua", trunc(dev.UserAgent, 256))
	pipe.Expire(ctx, k, qrTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return QRStart{}, err
	}
	return QRStart{ID: id, PollToken: poll, Payload: "lumo://qr-login?id=" + id, ExpiresIn: int(qrTTL.Seconds())}, nil
}

// QRInfo shows the phone what device is asking to sign in.
func (s *Service) QRInfo(ctx context.Context, id string) (map[string]string, error) {
	m, err := s.Redis.HGetAll(ctx, qrKey(id)).Result()
	if err != nil {
		return nil, err
	}
	if len(m) == 0 || m["status"] != "pending" {
		return nil, errs.NotFound("qr_expired", "This QR code expired. Refresh it on the computer.")
	}
	return map[string]string{"device": m["device"], "platform": m["platform"], "ip": m["ip"]}, nil
}

var qrApprove = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'status') ~= 'pending' then return 0 end
redis.call('HSET', KEYS[1], 'status', 'approved', 'user_id', ARGV[1])
return 1`)

func (s *Service) QRApprove(ctx context.Context, p auth.Principal, id string) error {
	ok, err := qrApprove.Run(ctx, s.Redis, []string{qrKey(id)}, p.UserID).Int()
	if err != nil {
		return err
	}
	if ok != 1 {
		return errs.NotFound("qr_expired", "This QR code expired. Refresh it on the computer.")
	}
	return nil
}

var qrConsume = redis.NewScript(`
local h = redis.call('HMGET', KEYS[1], 'status', 'poll', 'user_id')
if not h[1] then return {'expired', ''} end
if h[2] ~= ARGV[1] then return {'forbidden', ''} end
if h[1] == 'approved' then
  redis.call('HSET', KEYS[1], 'status', 'consumed')
  return {'approved', h[3]}
end
return {h[1], ''}`)

// QRPoll is called by the web page until the phone approves.
func (s *Service) QRPoll(ctx context.Context, id, pollToken string, dev Device) (string, *SignInResult, error) {
	sum := sha256.Sum256([]byte(pollToken))
	res, err := qrConsume.Run(ctx, s.Redis, []string{qrKey(id)}, hex.EncodeToString(sum[:])).StringSlice()
	if err != nil {
		return "", nil, err
	}
	switch res[0] {
	case "forbidden":
		return "", nil, errs.ErrForbidden
	case "approved":
	default:
		return res[0], nil, nil // pending | consumed | expired
	}
	var out SignInResult
	err = db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		u, err := store.GetUser(ctx, tx, res[1])
		if err != nil {
			return err
		}
		if err := checkStatus(u, s.now()); err != nil {
			return err
		}
		t, err := s.createSession(ctx, tx, u, "qr", dev)
		out = SignInResult{Tokens: t, User: u.Public(s.now()), NeedsBirthDate: u.BirthDate == nil}
		return err
	})
	if err != nil {
		return "", nil, err
	}
	return "approved", &out, nil
}

func maskPhone(p *string) *string {
	if p == nil || len(*p) < 6 {
		return p
	}
	m := (*p)[:4] + strings.Repeat("•", len(*p)-7) + (*p)[len(*p)-3:]
	return &m
}

// SearchUsers powers the app's search box (people tab).
func (s *Service) SearchUsers(ctx context.Context, q string) ([]events.UserPublic, error) {
	q = strings.ToLower(strings.TrimPrefix(strings.TrimSpace(q), "@"))
	if len([]rune(q)) < 2 || len(q) > 30 {
		return []events.UserPublic{}, nil
	}
	us, err := store.SearchUsers(ctx, s.Pool, q, 20)
	if err != nil {
		return nil, err
	}
	out := make([]events.UserPublic, 0, len(us))
	for _, u := range us {
		out = append(out, u.Public(s.now()))
	}
	return out, nil
}
