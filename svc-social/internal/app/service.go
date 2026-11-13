// Package app is svc-social: follows, likes, comments, blocks, reports.
//
// Writes insert the edge and an outbox event in one transaction. Counters
// (followers, likes, comments) are NOT updated inline: the counters consumer
// reads this service's own social.events and applies one UPDATE per key per
// batch, so a viral video doesn't turn its counter row into a global lock.
package app

import (
	"context"
	"errors"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/errs"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/outbox"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/userdir"
)

const producer = "svc-social"

var (
	ErrSelf         = errs.BadRequest("self_action", "You can't do that to yourself.")
	ErrBlocked      = errs.Forbidden("blocked", "You can't interact with this account.")
	ErrVideoMissing = errs.NotFound("video_not_found", "This video isn't available.")
)

type Service struct {
	Pool  *pgxpool.Pool
	Users *userdir.Client
}

func (s *Service) blockedEither(ctx context.Context, q db.Querier, a, b string) (bool, error) {
	var x bool
	err := q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM blocks WHERE (blocker_id = $1 AND blocked_id = $2) OR (blocker_id = $2 AND blocked_id = $1))`, a, b).Scan(&x)
	return x, err
}

// ---- follows ----

func (s *Service) Follow(ctx context.Context, follower, followee string) error {
	if follower == followee {
		return ErrSelf
	}
	return db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		if b, err := s.blockedEither(ctx, tx, follower, followee); err != nil {
			return err
		} else if b {
			return ErrBlocked
		}
		tag, err := tx.Exec(ctx, `INSERT INTO follows (follower_id, followee_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, follower, followee)
		if err != nil || tag.RowsAffected() == 0 {
			return err // already following: no-op, no event
		}
		if _, err := tx.Exec(ctx, `INSERT INTO followers (followee_id, follower_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, followee, follower); err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, events.TopicSocialEvents, events.MustNew(producer, events.Followed, followee,
			events.FollowData{FollowerID: follower, FolloweeID: followee}))
	})
}

func (s *Service) Unfollow(ctx context.Context, follower, followee string) error {
	return db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `DELETE FROM follows WHERE follower_id = $1 AND followee_id = $2`, follower, followee)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM followers WHERE followee_id = $1 AND follower_id = $2`, followee, follower); err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, events.TopicSocialEvents, events.MustNew(producer, events.Unfollowed, followee,
			events.FollowData{FollowerID: follower, FolloweeID: followee}))
	})
}

type Edge struct {
	User      events.UserPublic `json:"user"`
	CreatedAt time.Time         `json:"followed_at"`
}

// List returns followers (dir="followers") or following, newest first.
// cursor is the RFC3339Nano created_at of the last item.
func (s *Service) List(ctx context.Context, userID, dir, cursor string, limit int) ([]Edge, string, error) {
	if limit <= 0 || limit > 100 {
		limit = 30
	}
	before := time.Now().Add(time.Hour)
	if cursor != "" {
		if t, err := time.Parse(time.RFC3339Nano, cursor); err == nil {
			before = t
		}
	}
	q := `SELECT followee_id::text, created_at FROM follows WHERE follower_id = $1 AND created_at < $2 ORDER BY created_at DESC LIMIT $3`
	if dir == "followers" {
		q = `SELECT follower_id::text, created_at FROM followers WHERE followee_id = $1 AND created_at < $2 ORDER BY created_at DESC LIMIT $3`
	}
	rows, err := s.Pool.Query(ctx, q, userID, before, limit)
	if err != nil {
		return nil, "", err
	}
	type row struct {
		id string
		at time.Time
	}
	rs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (row, error) {
		var x row
		return x, r.Scan(&x.id, &x.at)
	})
	if err != nil {
		return nil, "", err
	}
	ids := make([]string, len(rs))
	for i, r := range rs {
		ids[i] = r.id
	}
	users, _ := s.Users.Many(ctx, ids)
	out := make([]Edge, 0, len(rs))
	for _, r := range rs {
		u, ok := users[r.id]
		if !ok {
			u = events.UserPublic{ID: r.id}
		}
		out = append(out, Edge{User: u, CreatedAt: r.at})
	}
	next := ""
	if len(rs) == limit {
		next = rs[len(rs)-1].at.Format(time.RFC3339Nano)
	}
	return out, next, nil
}

// FollowerIDs pages through follower IDs for internal fan-out (notifier).
func (s *Service) FollowerIDs(ctx context.Context, userID, after string, limit int) ([]string, error) {
	if limit <= 0 || limit > 5000 {
		limit = 1000
	}
	if after == "" {
		after = "00000000-0000-0000-0000-000000000000"
	}
	rows, err := s.Pool.Query(ctx, `SELECT follower_id::text FROM followers WHERE followee_id = $1 AND follower_id > $2
		ORDER BY follower_id LIMIT $3`, userID, after, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// FollowingIDs returns who a user follows (feed fan-out on read).
func (s *Service) FollowingIDs(ctx context.Context, userID string, limit int) ([]string, error) {
	rows, err := s.Pool.Query(ctx, `SELECT followee_id::text FROM follows WHERE follower_id = $1 ORDER BY created_at DESC LIMIT $2`, userID, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

type Counts struct {
	Followers     int64 `json:"followers"`
	Following     int64 `json:"following"`
	LikesReceived int64 `json:"likes_received"`
}

func (s *Service) UserCounts(ctx context.Context, userID string) (Counts, error) {
	var c Counts
	err := s.Pool.QueryRow(ctx, `SELECT followers, following, likes_received FROM user_counts WHERE user_id = $1`, userID).Scan(&c.Followers, &c.Following, &c.LikesReceived)
	if db.IsNoRows(err) {
		return c, nil
	}
	return c, err
}

type Relationship struct {
	Following  bool `json:"following"`
	FollowedBy bool `json:"followed_by"`
	Blocking   bool `json:"blocking"`
}

func (s *Service) Relationship(ctx context.Context, me, other string) (Relationship, error) {
	var r Relationship
	err := s.Pool.QueryRow(ctx, `SELECT
		EXISTS (SELECT 1 FROM follows WHERE follower_id = $1 AND followee_id = $2),
		EXISTS (SELECT 1 FROM follows WHERE follower_id = $2 AND followee_id = $1),
		EXISTS (SELECT 1 FROM blocks WHERE blocker_id = $1 AND blocked_id = $2)`, me, other).Scan(&r.Following, &r.FollowedBy, &r.Blocking)
	return r, err
}

// ---- likes ----

func (s *Service) videoOwner(ctx context.Context, q db.Querier, videoID string) (string, error) {
	var owner, status string
	err := q.QueryRow(ctx, `SELECT owner_id::text, status FROM videos WHERE video_id = $1`, videoID).Scan(&owner, &status)
	if db.IsNoRows(err) || (err == nil && status != "ready") {
		return "", ErrVideoMissing
	}
	return owner, err
}

func (s *Service) Like(ctx context.Context, userID, videoID string, like bool) error {
	return db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		owner, err := s.videoOwner(ctx, tx, videoID)
		if err != nil {
			return err
		}
		q, typ := `INSERT INTO likes (video_id, user_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, events.Liked
		if !like {
			q, typ = `DELETE FROM likes WHERE video_id = $1 AND user_id = $2`, events.Unliked
		}
		tag, err := tx.Exec(ctx, q, videoID, userID)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		return outbox.Insert(ctx, tx, events.TopicSocialEvents, events.MustNew(producer, typ, videoID,
			events.LikeData{UserID: userID, VideoID: videoID, OwnerID: owner}))
	})
}

type VideoCounts struct {
	VideoID  string `json:"video_id"`
	Likes    int64  `json:"likes"`
	Comments int64  `json:"comments"`
	Liked    bool   `json:"liked_by_me"`
}

// VideoCounts returns counts for up to 50 videos, plus whether `me` liked each.
func (s *Service) VideoCounts(ctx context.Context, me string, ids []string) ([]VideoCounts, error) {
	if len(ids) > 50 {
		ids = ids[:50]
	}
	ids = slices.DeleteFunc(ids, func(id string) bool { return len(id) != 36 })
	rows, err := s.Pool.Query(ctx, `SELECT v.id::text, COALESCE(c.likes, 0), COALESCE(c.comments, 0),
			($2 <> '' AND EXISTS (SELECT 1 FROM likes l WHERE l.video_id = v.id AND l.user_id::text = $2))
		FROM unnest($1::uuid[]) AS v(id) LEFT JOIN video_counts c ON c.video_id = v.id`, ids, me)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (VideoCounts, error) {
		var v VideoCounts
		return v, r.Scan(&v.VideoID, &v.Likes, &v.Comments, &v.Liked)
	})
}

// ---- comments ----

type Comment struct {
	ID        string            `json:"id"`
	VideoID   string            `json:"video_id"`
	ParentID  *string           `json:"parent_id,omitempty"`
	User      events.UserPublic `json:"user"`
	Text      string            `json:"text"`
	CreatedAt time.Time         `json:"created_at"`
}

func (s *Service) Comment(ctx context.Context, userID, videoID, parentID, text string) (Comment, error) {
	text = strings.TrimSpace(text)
	if n := utf8.RuneCountInString(text); n < 1 || n > 500 {
		return Comment{}, errs.BadRequest("comment_invalid", "Comments are 1 to 500 characters.")
	}
	c := Comment{ID: events.NewID(), VideoID: videoID, Text: text, CreatedAt: time.Now().UTC()}
	if parentID != "" {
		c.ParentID = &parentID
	}
	err := db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		owner, err := s.videoOwner(ctx, tx, videoID)
		if err != nil {
			return err
		}
		if b, err := s.blockedEither(ctx, tx, userID, owner); err != nil {
			return err
		} else if b {
			return ErrBlocked
		}
		if _, err := tx.Exec(ctx, `INSERT INTO comments (id, video_id, user_id, parent_id, text, created_at) VALUES ($1, $2, $3, $4, $5, $6)`,
			c.ID, videoID, userID, c.ParentID, text, c.CreatedAt); err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, events.TopicSocialEvents, events.MustNew(producer, events.Commented, videoID,
			events.CommentData{CommentID: c.ID, VideoID: videoID, OwnerID: owner, UserID: userID, ParentID: parentID, Text: text}))
	})
	if err != nil {
		return c, err
	}
	c.User, _ = s.Users.Get(ctx, userID)
	return c, nil
}

func (s *Service) Comments(ctx context.Context, videoID, parentID, cursor string, limit int) ([]Comment, string, error) {
	if limit <= 0 || limit > 50 {
		limit = 20
	}
	if cursor == "" {
		cursor = "ffffffff-ffff-ffff-ffff-ffffffffffff"
	}
	q := `SELECT id::text, video_id::text, parent_id::text, user_id::text, text, created_at FROM comments
		WHERE video_id = $1 AND parent_id IS NULL AND deleted_at IS NULL AND id < $2 ORDER BY id DESC LIMIT $3`
	arg := videoID
	if parentID != "" {
		q = `SELECT id::text, video_id::text, parent_id::text, user_id::text, text, created_at FROM comments
			WHERE parent_id = $1 AND deleted_at IS NULL AND id < $2 ORDER BY id DESC LIMIT $3`
		arg = parentID
	}
	rows, err := s.Pool.Query(ctx, q, arg, cursor, limit)
	if err != nil {
		return nil, "", err
	}
	var uids []string
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Comment, error) {
		var c Comment
		var uid string
		err := r.Scan(&c.ID, &c.VideoID, &c.ParentID, &uid, &c.Text, &c.CreatedAt)
		c.User.ID = uid
		uids = append(uids, uid)
		return c, err
	})
	if err != nil {
		return nil, "", err
	}
	users, _ := s.Users.Many(ctx, uids)
	for i := range list {
		if u, ok := users[list[i].User.ID]; ok {
			list[i].User = u
		}
	}
	next := ""
	if len(list) == limit {
		next = list[len(list)-1].ID
	}
	return list, next, nil
}

// DeleteComment: the author or the video owner may delete.
func (s *Service) DeleteComment(ctx context.Context, userID, commentID string, moderator bool) error {
	return db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		var author, videoID, owner string
		err := tx.QueryRow(ctx, `SELECT c.user_id::text, c.video_id::text, COALESCE(v.owner_id::text, '') FROM comments c
			LEFT JOIN videos v ON v.video_id = c.video_id WHERE c.id = $1 AND c.deleted_at IS NULL FOR UPDATE OF c`, commentID).Scan(&author, &videoID, &owner)
		if db.IsNoRows(err) {
			return errs.ErrNotFound
		}
		if err != nil {
			return err
		}
		if !moderator && userID != author && userID != owner {
			return errs.ErrForbidden
		}
		if _, err := tx.Exec(ctx, `UPDATE comments SET deleted_at = now() WHERE id = $1`, commentID); err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, events.TopicSocialEvents, events.MustNew(producer, events.CommentDeleted, videoID,
			events.CommentData{CommentID: commentID, VideoID: videoID, OwnerID: owner, UserID: author}))
	})
}

// ---- blocks & reports ----

func (s *Service) Block(ctx context.Context, blocker, blocked string, block bool) error {
	if blocker == blocked {
		return ErrSelf
	}
	if !block {
		return db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
			tag, err := tx.Exec(ctx, `DELETE FROM blocks WHERE blocker_id = $1 AND blocked_id = $2`, blocker, blocked)
			if err != nil || tag.RowsAffected() == 0 {
				return err
			}
			return outbox.Insert(ctx, tx, events.TopicSocialEvents, events.MustNew(producer, events.Unblocked, blocked,
				events.BlockData{BlockerID: blocker, BlockedID: blocked}))
		})
	}
	if err := db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO blocks (blocker_id, blocked_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, blocker, blocked)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		return outbox.Insert(ctx, tx, events.TopicSocialEvents, events.MustNew(producer, events.Blocked, blocked,
			events.BlockData{BlockerID: blocker, BlockedID: blocked}))
	}); err != nil {
		return err
	}
	// Blocking removes follows both ways.
	_ = s.Unfollow(ctx, blocker, blocked)
	return s.Unfollow(ctx, blocked, blocker)
}

var reportReasons = []string{"spam", "nudity", "violence", "harassment", "hate", "minor_safety", "scam", "self_harm", "other"}

func (s *Service) Report(ctx context.Context, reporter string, in events.ReportData) (string, error) {
	if !slices.Contains([]string{"user", "video", "comment", "room", "message"}, in.TargetType) || len(in.TargetID) == 0 || len(in.TargetID) > 64 {
		return "", errs.BadRequest("report_invalid", "Choose what you're reporting.")
	}
	if !slices.Contains(reportReasons, in.Reason) {
		return "", errs.BadRequest("reason_invalid", "Choose a reason.")
	}
	if utf8.RuneCountInString(in.Details) > 1000 {
		return "", errs.BadRequest("details_too_long", "Keep details under 1000 characters.")
	}
	in.ReportID, in.ReporterID = events.NewID(), reporter
	return in.ReportID, outbox.Insert(ctx, s.Pool, events.TopicSocialEvents, events.MustNew(producer, events.Reported, in.TargetID, in))
}

// ---- consumers ----

// ApplyCounts is the batch consumer of this service's own social.events.
// One UPDATE per user/video per batch, deduped per event.
func (s *Service) ApplyCounts(ctx context.Context, envs []events.Envelope) error {
	type delta struct{ a, b, c int64 }
	users := map[string]*delta{}  // followers, following, likes received
	videos := map[string]*delta{} // likes, comments
	ids := make([]string, 0, len(envs))
	bump := func(m map[string]*delta, k string, a, b int64) {
		d := m[k]
		if d == nil {
			d = &delta{}
			m[k] = d
		}
		d.a += a
		d.b += b
	}
	bumpLikesReceived := func(owner string, n int64) {
		if owner == "" {
			return
		}
		bump(users, owner, 0, 0)
		users[owner].c += n
	}
	return db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		ids = ids[:0]
		for _, e := range envs {
			ids = append(ids, e.ID)
		}
		rows, err := tx.Query(ctx, `INSERT INTO processed_events (consumer, event_id) SELECT 'social-counts', unnest($1::text[])
			ON CONFLICT DO NOTHING RETURNING event_id`, ids)
		if err != nil {
			return err
		}
		fresh, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		isFresh := map[string]bool{}
		for _, id := range fresh {
			isFresh[id] = true
		}
		clear(users)
		clear(videos)
		for _, e := range envs {
			if !isFresh[e.ID] {
				continue
			}
			switch e.Type {
			case events.Followed, events.Unfollowed:
				var f events.FollowData
				if e.Decode(&f) != nil {
					continue
				}
				n := int64(1)
				if e.Type == events.Unfollowed {
					n = -1
				}
				bump(users, f.FolloweeID, n, 0)
				bump(users, f.FollowerID, 0, n)
			case events.Liked, events.Unliked:
				var l events.LikeData
				if e.Decode(&l) != nil {
					continue
				}
				n := int64(1)
				if e.Type == events.Unliked {
					n = -1
				}
				bump(videos, l.VideoID, n, 0)
				bumpLikesReceived(l.OwnerID, n)
			case events.Commented, events.CommentDeleted:
				var c events.CommentData
				if e.Decode(&c) != nil {
					continue
				}
				n := int64(1)
				if e.Type == events.CommentDeleted {
					n = -1
				}
				bump(videos, c.VideoID, 0, n)
			}
		}
		for _, k := range sortedKeys(users) {
			d := users[k]
			if _, err := tx.Exec(ctx, `INSERT INTO user_counts (user_id, followers, following, likes_received) VALUES ($1, GREATEST($2, 0), GREATEST($3, 0), GREATEST($4, 0))
				ON CONFLICT (user_id) DO UPDATE SET followers = GREATEST(user_counts.followers + $2, 0), following = GREATEST(user_counts.following + $3, 0),
					likes_received = GREATEST(user_counts.likes_received + $4, 0)`,
				k, d.a, d.b, d.c); err != nil {
				return err
			}
		}
		for _, k := range sortedKeys(videos) {
			d := videos[k]
			if _, err := tx.Exec(ctx, `INSERT INTO video_counts (video_id, likes, comments) VALUES ($1, GREATEST($2, 0), GREATEST($3, 0))
				ON CONFLICT (video_id) DO UPDATE SET likes = GREATEST(video_counts.likes + $2, 0), comments = GREATEST(video_counts.comments + $3, 0)`,
				k, d.a, d.b); err != nil {
				return err
			}
		}
		return nil
	})
}

func sortedKeys[V any](m map[string]V) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// OnVideoState keeps the videos projection (consumer of media.videos).
func (s *Service) OnVideoState(ctx context.Context, e events.Envelope) error {
	if e.Type != events.VideoState {
		return nil
	}
	var v events.VideoStateData
	if err := e.Decode(&v); err != nil {
		return nil
	}
	_, err := s.Pool.Exec(ctx, `INSERT INTO videos (video_id, owner_id, status) VALUES ($1, $2, $3)
		ON CONFLICT (video_id) DO UPDATE SET status = EXCLUDED.status`, v.ID, v.OwnerID, v.Status)
	return err
}

// OnModeration removes comments taken down by moderators.
func (s *Service) OnModeration(ctx context.Context, e events.Envelope) error {
	var a events.ModerationActionData
	if err := e.Decode(&a); err != nil {
		return nil
	}
	if a.TargetType == "comment" && a.Action == "remove" {
		err := s.DeleteComment(ctx, a.ActorID, a.TargetID, true)
		if errors.Is(err, errs.ErrNotFound) {
			return nil
		}
		return err
	}
	return nil
}
