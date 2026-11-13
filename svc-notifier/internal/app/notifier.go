// Package app is svc-notifier: device registry and push notifications.
//
// It reacts to events (new follower, comment, like milestones, a followed
// creator going live) and sends pushes through Firebase Cloud Messaging,
// which also delivers to iOS via APNs. "Went live" fans out to every
// follower, paged from svc-social with bounded concurrency, in its own
// consumer group so a creator with millions of followers never delays
// everyone else's notifications.
package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/errs"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/gauth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/metrics"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/userdir"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

var sent = metrics.NewCounterVec("push_sent_total", "Push notifications by kind and result.", "kind", "result")

// Push is one notification.
type Push struct {
	Title string
	Body  string
	Data  map[string]string
}

// ErrBadToken means the device token is dead and should be deleted.
var ErrBadToken = errors.New("push: unregistered token")

type Sender interface {
	Send(ctx context.Context, token string, p Push) error
}

// FCM sends through Firebase Cloud Messaging HTTP v1.
type FCM struct {
	Tokens    *gauth.TokenSource
	ProjectID string
	HTTP      *http.Client
}

func (f *FCM) Send(ctx context.Context, token string, p Push) error {
	at, err := f.Tokens.Token(ctx)
	if err != nil {
		return err
	}
	body, _ := json.Marshal(map[string]any{"message": map[string]any{
		"token":        token,
		"notification": map[string]string{"title": p.Title, "body": p.Body},
		"data":         p.Data,
		"android":      map[string]any{"priority": "high"},
		"apns":         map[string]any{"payload": map[string]any{"aps": map[string]any{"sound": "default"}}},
	}})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://fcm.googleapis.com/v1/projects/"+f.ProjectID+"/messages:send", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+at)
	req.Header.Set("Content-Type", "application/json")
	resp, err := f.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	if resp.StatusCode == http.StatusNotFound || (resp.StatusCode == http.StatusBadRequest && bytes.Contains(b, []byte("registration token"))) {
		return ErrBadToken
	}
	return fmt.Errorf("fcm: %d %s", resp.StatusCode, b)
}

type Service struct {
	Pool          *pgxpool.Pool
	Redis         redis.UniversalClient
	Users         *userdir.Client
	Sender        Sender
	SocialURL     string
	InternalToken string
	HTTP          *http.Client
	FanoutWorkers int
}

// ---- device registry ----

func (s *Service) Register(ctx context.Context, userID, token, platform string) error {
	if len(token) < 20 || len(token) > 4096 || !slices.Contains([]string{"ios", "android", "web"}, platform) {
		return errs.BadRequest("device_invalid", "Send a valid push token and platform.")
	}
	// A token moves to whoever signed in last on that device.
	_, err := s.Pool.Exec(ctx, `INSERT INTO devices (token, user_id, platform) VALUES ($1, $2, $3)
		ON CONFLICT (token) DO UPDATE SET user_id = EXCLUDED.user_id, platform = EXCLUDED.platform, updated_at = now()`, token, userID, platform)
	return err
}

func (s *Service) Unregister(ctx context.Context, userID, token string) error {
	_, err := s.Pool.Exec(ctx, `DELETE FROM devices WHERE token = $1 AND user_id = $2`, token, userID)
	return err
}

type Prefs struct {
	NewFollower bool `json:"new_follower"`
	Comments    bool `json:"comments"`
	Likes       bool `json:"likes"`
	Live        bool `json:"live"`
}

func (s *Service) GetPrefs(ctx context.Context, userID string) (Prefs, error) {
	p := Prefs{true, true, true, true}
	err := s.Pool.QueryRow(ctx, `SELECT new_follower, comments, likes, live FROM prefs WHERE user_id = $1`, userID).
		Scan(&p.NewFollower, &p.Comments, &p.Likes, &p.Live)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, nil
	}
	return p, err
}

func (s *Service) SetPrefs(ctx context.Context, userID string, p Prefs) error {
	_, err := s.Pool.Exec(ctx, `INSERT INTO prefs (user_id, new_follower, comments, likes, live) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (user_id) DO UPDATE SET new_follower = $2, comments = $3, likes = $4, live = $5`, userID, p.NewFollower, p.Comments, p.Likes, p.Live)
	return err
}

// ---- sending ----

var prefColumn = map[string]string{"follow": "new_follower", "comment": "comments", "like": "likes", "live": "live"}

// sendTo pushes to every device of the given users who allow this kind.
func (s *Service) sendTo(ctx context.Context, kind string, userIDs []string, p Push) error {
	col, ok := prefColumn[kind]
	if !ok || len(userIDs) == 0 {
		return nil
	}
	rows, err := s.Pool.Query(ctx, `SELECT d.token FROM devices d LEFT JOIN prefs p ON p.user_id = d.user_id
		WHERE d.user_id = ANY($1::uuid[]) AND COALESCE(p.`+col+`, true)`, userIDs)
	if err != nil {
		return err
	}
	tokens, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(max(s.FanoutWorkers, 1))
	for _, t := range tokens {
		g.Go(func() error {
			err := s.Sender.Send(gctx, t, p)
			switch {
			case err == nil:
				sent.WithLabelValues(kind, "ok").Inc()
			case errors.Is(err, ErrBadToken):
				sent.WithLabelValues(kind, "dead_token").Inc()
				_, _ = s.Pool.Exec(gctx, `DELETE FROM devices WHERE token = $1`, t)
			default:
				sent.WithLabelValues(kind, "error").Inc()
				slog.WarnContext(gctx, "push failed", "kind", kind, "err", err)
			}
			return nil // one bad device never fails the batch
		})
	}
	return g.Wait()
}

// once guards against duplicate pushes when Kafka redelivers.
func (s *Service) once(ctx context.Context, key string, ttl time.Duration) bool {
	ok, err := s.Redis.SetNX(ctx, "notify:once:"+key, 1, ttl).Result()
	return err == nil && ok
}

// OnSocial handles follows, comments and like milestones.
func (s *Service) OnSocial(ctx context.Context, e events.Envelope) error {
	if !s.once(ctx, e.ID, 24*time.Hour) {
		return nil
	}
	switch e.Type {
	case events.Followed:
		var f events.FollowData
		if e.Decode(&f) != nil {
			return nil
		}
		// One "new follower" push per pair per day (follow/unfollow spam).
		if !s.once(ctx, "f:"+f.FollowerID+":"+f.FolloweeID, 24*time.Hour) {
			return nil
		}
		u, _ := s.Users.Get(ctx, f.FollowerID)
		return s.sendTo(ctx, "follow", []string{f.FolloweeID}, Push{Title: u.DisplayName, Body: "started following you",
			Data: map[string]string{"type": "follow", "user_id": f.FollowerID}})
	case events.Commented:
		var c events.CommentData
		if e.Decode(&c) != nil || c.UserID == c.OwnerID {
			return nil
		}
		u, _ := s.Users.Get(ctx, c.UserID)
		return s.sendTo(ctx, "comment", []string{c.OwnerID}, Push{Title: u.DisplayName + " commented", Body: truncate(c.Text, 120),
			Data: map[string]string{"type": "comment", "video_id": c.VideoID, "comment_id": c.CommentID}})
	case events.Liked:
		var l events.LikeData
		if e.Decode(&l) != nil || l.UserID == l.OwnerID {
			return nil
		}
		n, err := s.Redis.Incr(ctx, "notify:likes:"+l.VideoID).Result()
		if err != nil {
			return err
		}
		s.Redis.Expire(ctx, "notify:likes:"+l.VideoID, 30*24*time.Hour)
		if !isMilestone(n) {
			return nil
		}
		return s.sendTo(ctx, "like", []string{l.OwnerID}, Push{Title: "Your video is taking off", Body: fmt.Sprintf("%d people liked your video", n),
			Data: map[string]string{"type": "likes", "video_id": l.VideoID}})
	}
	return nil
}

func isMilestone(n int64) bool {
	return slices.Contains([]int64{1, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 1000000}, n)
}

// OnLive notifies every follower when a creator goes live.
func (s *Service) OnLive(ctx context.Context, e events.Envelope) error {
	if e.Type != events.RoomStarted {
		return nil
	}
	var r events.RoomStartedData
	if e.Decode(&r) != nil || time.Since(r.StartedAt) > 10*time.Minute {
		return nil // stale after an outage: the live may be over
	}
	host, _ := s.Users.Get(ctx, r.HostID)
	p := Push{Title: host.DisplayName + " is live", Body: r.Title, Data: map[string]string{"type": "live", "room_id": r.RoomID}}
	after := ""
	for {
		page, err := s.followers(ctx, r.HostID, after)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			return nil
		}
		// Resume point survives a crash mid fan-out.
		if !s.once(ctx, e.ID+":"+page[0], 24*time.Hour) {
			after = page[len(page)-1]
			continue
		}
		if err := s.sendTo(ctx, "live", page, p); err != nil {
			return err
		}
		after = page[len(page)-1]
	}
}

func (s *Service) followers(ctx context.Context, userID, after string) ([]string, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
		s.SocialURL+"/internal/social/users/"+userID+"/follower-ids?limit=1000&after="+after, nil)
	req.Header.Set("X-Internal-Token", s.InternalToken)
	resp, err := s.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var body struct {
		IDs []string `json:"ids"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("social followers: %d %v", resp.StatusCode, err)
	}
	return body.IDs, nil
}

// OnIdentity removes devices of deleted accounts.
func (s *Service) OnIdentity(ctx context.Context, e events.Envelope) error {
	if e.Type != events.UserDeleted {
		return nil
	}
	var d events.UserDeletedData
	if e.Decode(&d) != nil {
		return nil
	}
	_, err := s.Pool.Exec(ctx, `DELETE FROM devices WHERE user_id = $1`, d.UserID)
	return err
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}
