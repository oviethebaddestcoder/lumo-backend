// Package app is svc-live's business logic: rooms, viewers, PK battles.
//
// State split:
//   - Postgres (via outbox) holds durable facts: rooms, PK battles, results.
//   - Redis holds hot, high-churn state: viewer sets, ranking, PK scores.
//     Everything in Redis can be rebuilt or safely lost.
//   - Room pushes (viewers, PK score, room ended) go to Redis pub/sub on
//     realtime.RoomChannel; svc-realtime fans them out to WebSockets.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/realtime"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/errs"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/metrics"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/outbox"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/userdir"
	"github.com/oviethebaddestcoder/lumo-backend/svc-live/internal/livekit"
	"github.com/redis/go-redis/v9"
)

const producer = "svc-live"

var Categories = []string{"music", "talk", "dance", "gaming", "cooking", "sports", "education", "other"}

var (
	ErrAlreadyLive = errs.Conflict("already_live", "You're already live. End that stream first.")
	ErrRoomEnded   = errs.NotFound("room_ended", "This live has ended.")
	ErrNotHost     = errs.Forbidden("not_host", "Only the host can do that.")
	ErrPKBusy      = errs.Conflict("pk_busy", "One of the rooms is already in a PK battle.")

	liveRooms = metrics.NewGaugeVec("live_rooms_active", "Live rooms on the platform (sampled).", "category")
)

type Service struct {
	Pool        *pgxpool.Pool
	Redis       redis.UniversalClient
	LK          *livekit.Client
	Users       *userdir.Client
	HLSBaseURL  string // CDN base for egress output, e.g. https://cdn.lumo.app
	HLSAt       int64  // viewers at which HLS egress starts
	PKDuration  time.Duration
	TokenTTL    time.Duration
	InviteTTL   time.Duration
	ReplayPaths string // object key prefix for replays
}

// ---- Redis keys ----

func roomKey(id string) string     { return "live:room:" + id }
func viewersKey(id string) string  { return "live:viewers:" + id }
func rankKey(cat string) string    { return "live:rank:" + cat }
func pkKey(id string) string       { return "live:pk:" + id }
func giftSeenKey(id string) string { return "live:giftseen:" + id }

// ---- rooms ----

type StartInput struct {
	Title      string `json:"title"`
	Category   string `json:"category"`
	Source     string `json:"source"` // app | rtmp
	AllowPK    bool   `json:"allow_pk"`
	AllowGifts bool   `json:"allow_gifts"`
	SaveReplay bool   `json:"save_replay"`
	GoalCoins  int64  `json:"goal_coins"`
	GoalLabel  string `json:"goal_label"`
}

type Room struct {
	ID         string            `json:"id"`
	HostID     string            `json:"host_id"`
	Host       events.UserPublic `json:"host"`
	Title      string            `json:"title"`
	Category   string            `json:"category"`
	Viewers    int64             `json:"viewers"`
	GiftCoins  int64             `json:"gift_coins"`
	GoalCoins  int64             `json:"goal_coins,omitempty"`
	GoalLabel  string            `json:"goal_label,omitempty"`
	PKID       string            `json:"pk_id,omitempty"`
	StartedAt  int64             `json:"started_at"`
	AllowGifts bool              `json:"allow_gifts"`
}

type Connection struct {
	Mode     string `json:"mode"` // webrtc | hls
	URL      string `json:"url"`
	Token    string `json:"token,omitempty"`
	RTMPURL  string `json:"rtmp_url,omitempty"`
	RTMPKey  string `json:"rtmp_stream_key,omitempty"`
	RoomName string `json:"room_name"`
}

func (s *Service) Start(ctx context.Context, p auth.Principal, in StartInput) (Room, Connection, error) {
	if !p.Adult {
		return Room{}, Connection{}, errs.ErrAdultOnly
	}
	if l := len([]rune(in.Title)); l < 1 || l > 80 {
		return Room{}, Connection{}, errs.BadRequest("title_invalid", "Title must be 1 to 80 characters.")
	}
	if !slices.Contains(Categories, in.Category) {
		return Room{}, Connection{}, errs.BadRequest("category_invalid", "Pick a category.")
	}
	if in.Source == "" {
		in.Source = "app"
	}
	host, _ := s.Users.Get(ctx, p.UserID)
	var stream struct{ ingressID, url, key string }
	if in.Source == "rtmp" {
		if err := s.Pool.QueryRow(ctx, `SELECT ingress_id, rtmp_url, stream_key FROM stream_keys WHERE user_id = $1`, p.UserID).
			Scan(&stream.ingressID, &stream.url, &stream.key); err != nil {
			return Room{}, Connection{}, errs.BadRequest("no_stream_key", "Create a stream key in Creator Studio first.")
		}
	}
	id := events.NewID()
	now := time.Now().UTC()
	err := db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO rooms (id, host_id, title, category, source, allow_pk, allow_gifts, save_replay, goal_coins, goal_label, started_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			id, p.UserID, in.Title, in.Category, in.Source, in.AllowPK, in.AllowGifts, in.SaveReplay, max(in.GoalCoins, 0), in.GoalLabel, now)
		if db.IsUniqueViolation(err, "rooms_one_live_per_host") {
			return ErrAlreadyLive
		}
		if err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, events.TopicLiveEvents, events.MustNew(producer, events.RoomStarted, id,
			events.RoomStartedData{RoomID: id, HostID: p.UserID, Title: in.Title, Category: in.Category, StartedAt: now}))
	})
	if err != nil {
		return Room{}, Connection{}, err
	}

	pipe := s.Redis.TxPipeline()
	pipe.HSet(ctx, roomKey(id), "host_id", p.UserID, "title", in.Title, "category", in.Category, "viewers", 0,
		"gift_coins", 0, "started_at", now.Unix(), "goal_coins", in.GoalCoins, "goal_label", in.GoalLabel,
		"allow_gifts", strconv.FormatBool(in.AllowGifts), "allow_pk", strconv.FormatBool(in.AllowPK))
	pipe.Expire(ctx, roomKey(id), 24*time.Hour)
	pipe.ZAdd(ctx, rankKey("all"), redis.Z{Score: 0, Member: id})
	pipe.ZAdd(ctx, rankKey(in.Category), redis.Z{Score: 0, Member: id})
	if _, err := pipe.Exec(ctx); err != nil {
		slog.ErrorContext(ctx, "room cache write failed", "room_id", id, "err", err)
	}

	if in.SaveReplay {
		go s.startReplay(context.WithoutCancel(ctx), id, p.UserID)
	}
	room := Room{ID: id, HostID: p.UserID, Host: host, Title: in.Title, Category: in.Category, StartedAt: now.Unix(),
		GoalCoins: in.GoalCoins, GoalLabel: in.GoalLabel, AllowGifts: in.AllowGifts}
	if in.Source == "rtmp" {
		if err := s.LK.PointIngress(ctx, stream.ingressID, id); err != nil {
			_ = s.End(context.WithoutCancel(ctx), id, "ingress_failed", "")
			return Room{}, Connection{}, errs.Unavailable("live_unavailable", "Couldn't connect your stream key. Try again.").Wrap(err)
		}
		return room, Connection{Mode: "rtmp", RTMPURL: stream.url, RTMPKey: stream.key, RoomName: id}, nil
	}
	tok, err := s.LK.Token(p.UserID, host.DisplayName, id, true, s.TokenTTL)
	if err != nil {
		return Room{}, Connection{}, err
	}
	return room, Connection{Mode: "webrtc", URL: s.LK.WSURL, Token: tok, RoomName: id}, nil
}

func (s *Service) startReplay(ctx context.Context, roomID, hostID string) {
	eg, err := s.LK.StartReplay(ctx, roomID, fmt.Sprintf("%s/%s/%s.mp4", s.ReplayPaths, hostID, roomID))
	if err != nil {
		slog.ErrorContext(ctx, "replay egress failed", "room_id", roomID, "err", err)
		return
	}
	_, _ = s.Pool.Exec(ctx, `UPDATE rooms SET replay_egress_id = $2 WHERE id = $1`, roomID, eg)
}

// Get returns a live room from Redis (or the DB if it has ended).
func (s *Service) Get(ctx context.Context, id string) (Room, error) {
	rooms, err := s.hydrate(ctx, []string{id})
	if err != nil {
		return Room{}, err
	}
	if len(rooms) == 0 {
		return Room{}, ErrRoomEnded
	}
	return rooms[0], nil
}

// List returns live rooms ranked by viewers and gifts.
func (s *Service) List(ctx context.Context, category string, offset, limit int) ([]Room, error) {
	if category == "" || category == "popular" {
		category = "all"
	}
	if limit <= 0 || limit > 50 {
		limit = 20
	}
	ids, err := s.Redis.ZRevRange(ctx, rankKey(category), int64(offset), int64(offset+limit-1)).Result()
	if err != nil {
		return nil, err
	}
	return s.hydrate(ctx, ids)
}

func (s *Service) hydrate(ctx context.Context, ids []string) ([]Room, error) {
	if len(ids) == 0 {
		return []Room{}, nil
	}
	pipe := s.Redis.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HGetAll(ctx, roomKey(id))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	rooms := make([]Room, 0, len(ids))
	hostIDs := make([]string, 0, len(ids))
	for i, c := range cmds {
		m := c.Val()
		if len(m) == 0 {
			continue
		}
		r := Room{ID: ids[i], HostID: m["host_id"], Title: m["title"], Category: m["category"], PKID: m["pk_id"],
			GoalLabel: m["goal_label"], AllowGifts: m["allow_gifts"] != "false"}
		r.Viewers, _ = strconv.ParseInt(m["viewers"], 10, 64)
		r.GiftCoins, _ = strconv.ParseInt(m["gift_coins"], 10, 64)
		r.GoalCoins, _ = strconv.ParseInt(m["goal_coins"], 10, 64)
		r.StartedAt, _ = strconv.ParseInt(m["started_at"], 10, 64)
		rooms = append(rooms, r)
		hostIDs = append(hostIDs, r.HostID)
	}
	users, err := s.Users.Many(ctx, hostIDs)
	if err != nil {
		slog.WarnContext(ctx, "host profiles unavailable", "err", err)
	}
	for i := range rooms {
		if u, ok := users[rooms[i].HostID]; ok {
			rooms[i].Host = u
		}
	}
	return rooms, nil
}

// Join returns how the viewer should watch: WebRTC for small rooms (sub-second
// latency, can interact), HLS via CDN once the room is big.
func (s *Service) Join(ctx context.Context, viewerID, roomID string) (Connection, error) {
	m, err := s.Redis.HMGet(ctx, roomKey(roomID), "host_id", "viewers", "hls_url").Result()
	if err != nil {
		return Connection{}, err
	}
	if m[0] == nil {
		return Connection{}, ErrRoomEnded
	}
	viewers, _ := strconv.ParseInt(fmt.Sprint(m[1]), 10, 64)
	// The host reconnecting (app restart, network change) gets a publish
	// token for its own room, never the HLS fallback.
	isHost := fmt.Sprint(m[0]) == viewerID
	if hls, _ := m[2].(string); !isHost && hls != "" && viewers >= s.HLSAt {
		return Connection{Mode: "hls", URL: hls, RoomName: roomID}, nil
	}
	name := ""
	if u, err := s.Users.Get(ctx, viewerID); err == nil {
		name = u.DisplayName
	}
	tok, err := s.LK.Token(viewerID, name, roomID, isHost, s.TokenTTL)
	if err != nil {
		return Connection{}, err
	}
	return Connection{Mode: "webrtc", URL: s.LK.WSURL, Token: tok, RoomName: roomID}, nil
}

// End closes a room. Safe to call more than once (host, webhook, moderation).
func (s *Service) End(ctx context.Context, roomID, reason, requesterID string) error {
	var hostID, hlsEgress string
	var giftCoins int64
	gc, _ := s.Redis.HGet(ctx, roomKey(roomID), "gift_coins").Int64()
	peak, _ := s.Redis.HGet(ctx, roomKey(roomID), "peak").Int64()
	ended := false
	err := db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `UPDATE rooms SET status = 'ended', ended_at = now(), end_reason = $2,
				gift_coins = GREATEST(gift_coins, $3), peak_viewers = GREATEST(peak_viewers, $4)
			WHERE id = $1 AND status = 'live' AND ($5 = '' OR host_id::text = $5)
			RETURNING host_id::text, hls_egress_id, gift_coins`, roomID, reason, gc, peak, requesterID).Scan(&hostID, &hlsEgress, &giftCoins)
		if db.IsNoRows(err) {
			if requesterID != "" {
				var exists bool
				_ = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM rooms WHERE id = $1 AND status = 'live')`, roomID).Scan(&exists)
				if exists {
					return ErrNotHost
				}
			}
			return nil // already ended
		}
		if err != nil {
			return err
		}
		ended = true
		// Close any open PK involving this room.
		if _, err := tx.Exec(ctx, `UPDATE pk_battles SET status = CASE WHEN status = 'invited' THEN 'expired' ELSE 'ended' END
			WHERE (room_a = $1 OR room_b = $1) AND status IN ('invited', 'active')`, roomID); err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, events.TopicLiveEvents, events.MustNew(producer, events.RoomEnded, roomID,
			events.RoomEndedData{RoomID: roomID, HostID: hostID, EndedAt: time.Now().UTC(), PeakViewers: peak, GiftCoins: giftCoins, Reason: reason}))
	})
	if err != nil || !ended {
		return err
	}
	cat, _ := s.Redis.HGet(ctx, roomKey(roomID), "category").Result()
	pipe := s.Redis.Pipeline()
	pipe.ZRem(ctx, rankKey("all"), roomID)
	if cat != "" {
		pipe.ZRem(ctx, rankKey(cat), roomID)
	}
	pipe.Del(ctx, roomKey(roomID), viewersKey(roomID))
	_, _ = pipe.Exec(ctx)
	s.push(ctx, roomID, realtime.TypeRoomEnded, map[string]string{"reason": reason})
	bg := context.WithoutCancel(ctx)
	go func() {
		_ = s.LK.StopEgress(bg, hlsEgress)
		if reason != "livekit_room_finished" {
			if err := s.LK.DeleteRoom(bg, roomID); err != nil {
				slog.Warn("livekit delete room", "room_id", roomID, "err", err)
			}
		}
	}()
	return nil
}

// push sends an ephemeral message to everyone in a room via svc-realtime.
func (s *Service) push(ctx context.Context, roomID, typ string, data any) {
	b, _ := json.Marshal(realtime.Message{Type: typ, RoomID: roomID, At: time.Now().UTC(), Data: data})
	if err := s.Redis.Publish(ctx, realtime.RoomChannel(roomID), b).Err(); err != nil {
		slog.WarnContext(ctx, "realtime push failed", "room_id", roomID, "type", typ, "err", err)
	}
}

// ---- stream keys (OBS) ----

type StreamKey struct {
	RTMPURL   string `json:"rtmp_url"`
	StreamKey string `json:"stream_key"`
}

func (s *Service) StreamKey(ctx context.Context, p auth.Principal, rotate bool) (StreamKey, error) {
	var k StreamKey
	var ingressID string
	err := s.Pool.QueryRow(ctx, `SELECT ingress_id, rtmp_url, stream_key FROM stream_keys WHERE user_id = $1`, p.UserID).Scan(&ingressID, &k.RTMPURL, &k.StreamKey)
	if err == nil && !rotate && k.RTMPURL != "" && k.StreamKey != "" {
		return k, nil
	}
	if err != nil && !db.IsNoRows(err) {
		return k, err
	}
	if ingressID != "" {
		_ = s.LK.DeleteIngress(ctx, ingressID)
	}
	host, _ := s.Users.Get(ctx, p.UserID)
	in, err := s.LK.CreateIngress(ctx, "studio-"+p.UserID, "idle-"+p.UserID, p.UserID, host.DisplayName)
	if err == nil && (in.ID == "" || in.URL == "" || in.StreamKey == "") {
		// Never store (and keep showing) a half-made key.
		err = fmt.Errorf("livekit: ingress missing id, url or stream key")
	}
	if err != nil {
		return StreamKey{}, errs.Unavailable("live_unavailable", "Couldn't create a stream key right now.").Wrap(err)
	}
	_, err = s.Pool.Exec(ctx, `INSERT INTO stream_keys (user_id, ingress_id, rtmp_url, stream_key) VALUES ($1, $2, $3, $4)
		ON CONFLICT (user_id) DO UPDATE SET ingress_id = EXCLUDED.ingress_id, rtmp_url = EXCLUDED.rtmp_url,
		stream_key = EXCLUDED.stream_key, created_at = now()`, p.UserID, in.ID, in.URL, in.StreamKey)
	return StreamKey{RTMPURL: in.URL, StreamKey: in.StreamKey}, err
}
