// Package store holds all SQL for svc-identity.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
)

var ErrNotFound = errors.New("store: not found")

type User struct {
	ID             string
	Handle         string
	DisplayName    string
	AvatarURL      string
	Bio            string
	BirthDate      *time.Time
	Email          *string
	EmailVerified  bool
	Phone          *string
	PasswordHash   *string
	Roles          []string
	Verified       bool
	Status         string
	SuspendedUntil *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Adult reports whether the user is 18 or older today.
func (u User) Adult(now time.Time) bool {
	if u.BirthDate == nil {
		return false
	}
	b := *u.BirthDate
	age := now.Year() - b.Year()
	if now.YearDay() < b.YearDay() {
		age--
	}
	return age >= 18
}

// Public is the projection shared with other services via events.
func (u User) Public(now time.Time) events.UserPublic {
	return events.UserPublic{
		ID: u.ID, Handle: u.Handle, DisplayName: u.DisplayName, AvatarURL: u.AvatarURL,
		Bio: u.Bio, Verified: u.Verified, Adult: u.Adult(now), Status: u.Status, CreatedAt: u.CreatedAt,
	}
}

const userCols = `id::text, handle, display_name, avatar_url, bio, birth_date, email, email_verified, phone,
	password_hash, roles, verified, status, suspended_until, created_at, updated_at`

func scanUser(row pgx.Row) (User, error) {
	var u User
	err := row.Scan(&u.ID, &u.Handle, &u.DisplayName, &u.AvatarURL, &u.Bio, &u.BirthDate, &u.Email,
		&u.EmailVerified, &u.Phone, &u.PasswordHash, &u.Roles, &u.Verified, &u.Status, &u.SuspendedUntil,
		&u.CreatedAt, &u.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return u, ErrNotFound
	}
	return u, err
}

func GetUser(ctx context.Context, q db.Querier, id string) (User, error) {
	return scanUser(q.QueryRow(ctx, `SELECT `+userCols+` FROM users WHERE id = $1`, id))
}

func GetUserForUpdate(ctx context.Context, q db.Querier, id string) (User, error) {
	return scanUser(q.QueryRow(ctx, `SELECT `+userCols+` FROM users WHERE id = $1 FOR UPDATE`, id))
}

// GetUsers loads up to 100 users by ID (public profile batch lookups).
func GetUsers(ctx context.Context, q db.Querier, ids []string) ([]User, error) {
	rows, err := q.Query(ctx, `SELECT `+userCols+` FROM users WHERE id = ANY($1::uuid[]) AND status <> 'deleted'`, ids)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (User, error) { return scanUser(r) })
}

func GetUserByHandle(ctx context.Context, q db.Querier, handle string) (User, error) {
	return scanUser(q.QueryRow(ctx, `SELECT `+userCols+` FROM users WHERE handle = $1`, handle))
}

func GetUserByEmail(ctx context.Context, q db.Querier, email string) (User, error) {
	return scanUser(q.QueryRow(ctx, `SELECT `+userCols+` FROM users WHERE lower(email) = lower($1)`, email))
}

func GetUserByPhone(ctx context.Context, q db.Querier, phone string) (User, error) {
	return scanUser(q.QueryRow(ctx, `SELECT `+userCols+` FROM users WHERE phone = $1`, phone))
}

func InsertUser(ctx context.Context, q db.Querier, u User) error {
	_, err := q.Exec(ctx, `INSERT INTO users (id, handle, display_name, avatar_url, birth_date, email, email_verified, phone)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		u.ID, u.Handle, u.DisplayName, u.AvatarURL, u.BirthDate, u.Email, u.EmailVerified, u.Phone)
	return err
}

type ProfilePatch struct {
	Handle      *string
	DisplayName *string
	Bio         *string
	AvatarURL   *string
	BirthDate   *time.Time
}

func UpdateProfile(ctx context.Context, q db.Querier, id string, p ProfilePatch) (User, error) {
	return scanUser(q.QueryRow(ctx, `UPDATE users SET
			handle = COALESCE($2, handle),
			display_name = COALESCE($3, display_name),
			bio = COALESCE($4, bio),
			avatar_url = COALESCE($5, avatar_url),
			birth_date = COALESCE(birth_date, $6),
			updated_at = now()
		WHERE id = $1 RETURNING `+userCols, id, p.Handle, p.DisplayName, p.Bio, p.AvatarURL, p.BirthDate))
}

func SetPassword(ctx context.Context, q db.Querier, id, hash string) error {
	_, err := q.Exec(ctx, `UPDATE users SET password_hash = $2, updated_at = now() WHERE id = $1`, id, hash)
	return err
}

func SetStatus(ctx context.Context, q db.Querier, id, status string, until *time.Time) (User, error) {
	return scanUser(q.QueryRow(ctx, `UPDATE users SET status = $2, suspended_until = $3, updated_at = now()
		WHERE id = $1 RETURNING `+userCols, id, status, until))
}

// Scrub erases personal data for account deletion (NDPA / app-store rules).
func Scrub(ctx context.Context, q db.Querier, id string) error {
	_, err := q.Exec(ctx, `UPDATE users SET status = 'deleted', deleted_at = now(), updated_at = now(),
			handle = 'deleted_' || substr(replace(id::text, '-', ''), 1, 16),
			display_name = 'Deleted user', avatar_url = '', bio = '', birth_date = NULL,
			email = NULL, email_verified = false, phone = NULL, password_hash = NULL
		WHERE id = $1`, id)
	if err != nil {
		return err
	}
	_, err = q.Exec(ctx, `DELETE FROM identities WHERE user_id = $1`, id)
	return err
}

// ---- identities ----

type Identity struct {
	Provider  string    `json:"provider"`
	Subject   string    `json:"-"`
	Email     *string   `json:"email,omitempty"`
	CreatedAt time.Time `json:"linked_at"`
}

// FindIdentity returns the user ID linked to (provider, subject).
func FindIdentity(ctx context.Context, q db.Querier, provider, subject string) (string, error) {
	var uid string
	err := q.QueryRow(ctx, `UPDATE identities SET last_login_at = now() WHERE provider = $1 AND subject = $2 RETURNING user_id::text`,
		provider, subject).Scan(&uid)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	return uid, err
}

func InsertIdentity(ctx context.Context, q db.Querier, provider, subject, userID string, email *string) error {
	_, err := q.Exec(ctx, `INSERT INTO identities (provider, subject, user_id, email) VALUES ($1, $2, $3, $4)`,
		provider, subject, userID, email)
	return err
}

func ListIdentities(ctx context.Context, q db.Querier, userID string) ([]Identity, error) {
	rows, err := q.Query(ctx, `SELECT provider, subject, email, created_at FROM identities WHERE user_id = $1 ORDER BY created_at`, userID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Identity, error) {
		var i Identity
		err := r.Scan(&i.Provider, &i.Subject, &i.Email, &i.CreatedAt)
		return i, err
	})
}

func DeleteIdentity(ctx context.Context, q db.Querier, userID, provider string) (int64, error) {
	tag, err := q.Exec(ctx, `DELETE FROM identities WHERE user_id = $1 AND provider = $2`, userID, provider)
	return tag.RowsAffected(), err
}

// ---- sessions ----

type Session struct {
	ID              string
	UserID          string
	RefreshHash     []byte
	PrevRefreshHash []byte
	RotatedAt       *time.Time
	Method          string
	DeviceID        string
	DeviceName      string
	Platform        string
	IP              string
	UserAgent       string
	CreatedAt       time.Time
	LastUsedAt      time.Time
	ExpiresAt       time.Time
	RevokedAt       *time.Time
}

const sessionCols = `id::text, user_id::text, refresh_hash, prev_refresh_hash, rotated_at, method, device_id,
	device_name, platform, ip, user_agent, created_at, last_used_at, expires_at, revoked_at`

func scanSession(row pgx.Row) (Session, error) {
	var s Session
	err := row.Scan(&s.ID, &s.UserID, &s.RefreshHash, &s.PrevRefreshHash, &s.RotatedAt, &s.Method, &s.DeviceID,
		&s.DeviceName, &s.Platform, &s.IP, &s.UserAgent, &s.CreatedAt, &s.LastUsedAt, &s.ExpiresAt, &s.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return s, ErrNotFound
	}
	return s, err
}

func InsertSession(ctx context.Context, q db.Querier, s Session) error {
	_, err := q.Exec(ctx, `INSERT INTO sessions (id, user_id, refresh_hash, method, device_id, device_name, platform, ip, user_agent, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		s.ID, s.UserID, s.RefreshHash, s.Method, s.DeviceID, s.DeviceName, s.Platform, s.IP, s.UserAgent, s.ExpiresAt)
	return err
}

func SessionByRefreshForUpdate(ctx context.Context, q db.Querier, hash []byte) (Session, error) {
	return scanSession(q.QueryRow(ctx, `SELECT `+sessionCols+` FROM sessions WHERE refresh_hash = $1 FOR UPDATE`, hash))
}

func SessionByPrevRefresh(ctx context.Context, q db.Querier, hash []byte) (Session, error) {
	return scanSession(q.QueryRow(ctx, `SELECT `+sessionCols+` FROM sessions WHERE prev_refresh_hash = $1`, hash))
}

func RotateSession(ctx context.Context, q db.Querier, id string, newHash, oldHash []byte, expires time.Time, ip string) error {
	_, err := q.Exec(ctx, `UPDATE sessions SET refresh_hash = $2, prev_refresh_hash = $3, rotated_at = now(),
		last_used_at = now(), expires_at = $4, ip = $5 WHERE id = $1`, id, newHash, oldHash, expires, ip)
	return err
}

func RevokeSession(ctx context.Context, q db.Querier, id, reason string) error {
	_, err := q.Exec(ctx, `UPDATE sessions SET revoked_at = now(), revoke_reason = $2 WHERE id = $1 AND revoked_at IS NULL`, id, reason)
	return err
}

// RevokeUserSessions revokes every active session and returns their IDs.
func RevokeUserSessions(ctx context.Context, q db.Querier, userID, reason, exceptID string) ([]string, error) {
	rows, err := q.Query(ctx, `UPDATE sessions SET revoked_at = now(), revoke_reason = $2
		WHERE user_id = $1 AND revoked_at IS NULL AND id::text <> $3 RETURNING id::text`, userID, reason, exceptID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func ListSessions(ctx context.Context, q db.Querier, userID string) ([]Session, error) {
	rows, err := q.Query(ctx, `SELECT `+sessionCols+` FROM sessions WHERE user_id = $1 AND revoked_at IS NULL AND expires_at > now()
		ORDER BY last_used_at DESC LIMIT 50`, userID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Session, error) { return scanSession(r) })
}

// PruneSessions deletes long-dead sessions in batches (worker cron).
func PruneSessions(ctx context.Context, q db.Querier) (int64, error) {
	var total int64
	for {
		tag, err := q.Exec(ctx, `DELETE FROM sessions WHERE id IN (
			SELECT id FROM sessions WHERE expires_at < now() - interval '30 days'
			   OR revoked_at < now() - interval '30 days' LIMIT 5000)`)
		if err != nil {
			return total, err
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() < 5000 {
			return total, nil
		}
	}
}
