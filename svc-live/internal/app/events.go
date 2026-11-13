package app

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/realtime"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/outbox"
	"github.com/oviethebaddestcoder/lumo-backend/svc-live/internal/livekit"
	"github.com/redis/go-redis/v9"
)

// rankScore weights viewers and recent gifting for discovery ranking.
func rankScore(viewers, giftCoins int64) float64 { return float64(viewers) + float64(giftCoins)/10 }

// HandleWebhook applies LiveKit room, participant and egress events.
func (s *Service) HandleWebhook(ctx context.Context, w livekit.Webhook) error {
	switch w.Event {
	case "participant_joined", "participant_left":
		if w.Room == nil || w.Participant == nil {
			return nil
		}
		return s.viewerChange(ctx, w.Room.Name, w.Participant.Identity, w.Event == "participant_joined")
	case "room_finished":
		if w.Room != nil {
			return s.End(ctx, w.Room.Name, "livekit_room_finished", "")
		}
	case "egress_ended":
		if w.EgressInfo == nil || len(w.EgressInfo.FileResults) == 0 {
			return nil
		}
		room := w.EgressInfo.RoomName
		var hostID, title, replayEgress string
		if err := s.Pool.QueryRow(ctx, `SELECT host_id::text, title, replay_egress_id FROM rooms WHERE id::text = $1`, room).
			Scan(&hostID, &title, &replayEgress); err != nil || replayEgress != w.EgressInfo.EgressID {
			return nil
		}
		return outbox.Insert(ctx, s.Pool, events.TopicLiveEvents, events.MustNew(producer, events.ReplayReady, room,
			events.ReplayReadyData{RoomID: room, HostID: hostID, Title: title, ObjectKey: w.EgressInfo.FileResults[0].Filename}))
	}
	return nil
}

var viewerScript = redis.NewScript(`
local host = redis.call('HGET', KEYS[1], 'host_id')
if not host then return -1 end
if ARGV[1] == host or ARGV[1] == redis.call('HGET', KEYS[1], 'pk_host') then
  return tonumber(redis.call('HGET', KEYS[1], 'viewers') or '0')
end
if ARGV[2] == '1' then redis.call('SADD', KEYS[2], ARGV[1]) else redis.call('SREM', KEYS[2], ARGV[1]) end
redis.call('EXPIRE', KEYS[2], 86400)
local n = redis.call('SCARD', KEYS[2])
redis.call('HSET', KEYS[1], 'viewers', n)
local peak = tonumber(redis.call('HGET', KEYS[1], 'peak') or '0')
if n > peak then redis.call('HSET', KEYS[1], 'peak', n) end
return n`)

func (s *Service) viewerChange(ctx context.Context, roomID, identity string, joined bool) error {
	j := "0"
	if joined {
		j = "1"
	}
	n, err := viewerScript.Run(ctx, s.Redis, []string{roomKey(roomID), viewersKey(roomID)}, identity, j).Int64()
	if err != nil || n < 0 {
		return err
	}
	s.rerank(ctx, roomID)
	// Throttle viewer-count pushes to one per 2s per room.
	if ok, _ := s.Redis.SetNX(ctx, "live:vthrottle:"+roomID, 1, 2*time.Second).Result(); ok {
		s.push(ctx, roomID, realtime.TypeViewers, realtime.Viewers{Count: n})
	}
	if joined && n >= s.HLSAt {
		s.maybeStartHLS(ctx, roomID)
	}
	return nil
}

func (s *Service) rerank(ctx context.Context, roomID string) {
	m, err := s.Redis.HMGet(ctx, roomKey(roomID), "viewers", "gift_coins", "category").Result()
	if err != nil || m[2] == nil {
		return
	}
	v, _ := strconv.ParseInt(fmt.Sprint(m[0]), 10, 64)
	g, _ := strconv.ParseInt(fmt.Sprint(m[1]), 10, 64)
	score := rankScore(v, g)
	pipe := s.Redis.Pipeline()
	pipe.ZAddXX(ctx, rankKey("all"), redis.Z{Score: score, Member: roomID})
	pipe.ZAddXX(ctx, rankKey(fmt.Sprint(m[2])), redis.Z{Score: score, Member: roomID})
	_, _ = pipe.Exec(ctx)
}

func (s *Service) maybeStartHLS(ctx context.Context, roomID string) {
	if ok, _ := s.Redis.SetNX(ctx, "live:hlsstart:"+roomID, 1, 24*time.Hour).Result(); !ok {
		return
	}
	go func() {
		bg := context.WithoutCancel(ctx)
		prefix := "live/" + roomID
		id, err := s.LK.StartHLS(bg, roomID, prefix)
		if err != nil {
			slog.Error("start HLS egress", "room_id", roomID, "err", err)
			s.Redis.Del(bg, "live:hlsstart:"+roomID)
			return
		}
		url := s.HLSBaseURL + "/" + prefix + "/live.m3u8"
		s.Redis.HSet(bg, roomKey(roomID), "hls_url", url)
		_, _ = s.Pool.Exec(bg, `UPDATE rooms SET hls_egress_id = $2, hls_url = $3 WHERE id = $1`, roomID, id, url)
		slog.Info("HLS egress started", "room_id", roomID)
	}()
}

// OnGift (consumer of wallet.gifts) updates room totals, the goal bar,
// ranking and PK scores. Redis-only, deduped by gift_send_id.
func (s *Service) OnGift(ctx context.Context, e events.Envelope) error {
	if e.Type != events.GiftSent {
		return nil
	}
	var g events.GiftSentData
	if err := e.Decode(&g); err != nil {
		return nil
	}
	if g.RoomID == "" {
		return nil
	}
	if ok, err := s.Redis.SetNX(ctx, giftSeenKey(g.GiftSendID), 1, 6*time.Hour).Result(); err != nil || !ok {
		return err
	}
	total, err := s.Redis.HIncrBy(ctx, roomKey(g.RoomID), "gift_coins", g.Coins).Result()
	if err != nil {
		return err
	}
	s.rerank(ctx, g.RoomID)
	if goal, _ := s.Redis.HGet(ctx, roomKey(g.RoomID), "goal_coins").Int64(); goal > 0 {
		s.push(ctx, g.RoomID, realtime.TypeGoal, realtime.Goal{Coins: total, Target: goal})
	}
	pkID, _ := s.Redis.HGet(ctx, roomKey(g.RoomID), "pk_id").Result()
	if pkID == "" {
		return nil
	}
	pk, err := s.Redis.HGetAll(ctx, pkKey(pkID)).Result()
	if err != nil || len(pk) == 0 {
		return err
	}
	field := "a"
	if pk["room_b"] == g.RoomID {
		field = "b"
	}
	if _, err := s.Redis.HIncrBy(ctx, pkKey(pkID), field, g.Coins).Result(); err != nil {
		return err
	}
	s.pushPKScore(ctx, pkID)
	return nil
}

func (s *Service) pushPKScore(ctx context.Context, pkID string) {
	pk, err := s.Redis.HGetAll(ctx, pkKey(pkID)).Result()
	if err != nil || len(pk) == 0 {
		return
	}
	a, _ := strconv.ParseInt(pk["a"], 10, 64)
	b, _ := strconv.ParseInt(pk["b"], 10, 64)
	ends, _ := strconv.ParseInt(pk["ends_at"], 10, 64)
	msg := realtime.PKScore{PKID: pkID, RoomA: pk["room_a"], RoomB: pk["room_b"], ScoreA: a, ScoreB: b, EndsAt: ends}
	s.push(ctx, pk["room_a"], realtime.TypePKScore, msg)
	s.push(ctx, pk["room_b"], realtime.TypePKScore, msg)
}

// OnModeration ends rooms for end_room actions and banned hosts.
func (s *Service) OnModeration(ctx context.Context, e events.Envelope) error {
	var a events.ModerationActionData
	if err := e.Decode(&a); err != nil {
		return nil
	}
	switch {
	case a.TargetType == "room" && a.Action == "end_room":
		return s.End(ctx, a.TargetID, "moderation", "")
	case a.TargetType == "user" && a.Action == "ban":
		var roomID string
		err := s.Pool.QueryRow(ctx, `SELECT id::text FROM rooms WHERE host_id = $1 AND status = 'live'`, a.TargetID).Scan(&roomID)
		if err != nil {
			return nil
		}
		return s.End(ctx, roomID, "host_banned", "")
	case a.TargetType == "user" && a.Action == "mute" && a.RoomID != "":
		ttl := time.Hour
		if a.Until != nil {
			ttl = time.Until(*a.Until)
		}
		pipe := s.Redis.TxPipeline()
		pipe.SAdd(ctx, "live:muted:"+a.RoomID, a.TargetID) // svc-realtime checks this set
		pipe.Expire(ctx, "live:muted:"+a.RoomID, max(ttl, time.Minute))
		_, err := pipe.Exec(ctx)
		return err
	}
	return nil
}
