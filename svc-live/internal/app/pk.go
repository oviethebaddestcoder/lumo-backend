package app

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/realtime"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/errs"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/outbox"
)

// PK battle flow:
//  1. Host A invites host B's room (POST /v1/live/pk/invite).
//  2. B accepts within InviteTTL. Each host gets a publish token for the
//     OTHER room, so both cameras appear in both rooms (the app shows them
//     side by side). Viewers stay in their own room.
//  3. Gifts to either room add to that side's score (OnGift).
//  4. The worker ends due battles (Tick), writes the result and announces it.

type PKView struct {
	ID       string `json:"pk_id"`
	Status   string `json:"status"`
	RoomA    string `json:"room_a"`
	RoomB    string `json:"room_b"`
	EndsAt   int64  `json:"ends_at,omitempty"`
	CrossTok string `json:"cross_token,omitempty"` // publish into the opponent's room
	CrossURL string `json:"cross_url,omitempty"`
}

func (s *Service) liveRoomOf(ctx context.Context, q db.Querier, hostID string) (id string, allowPK bool, err error) {
	err = q.QueryRow(ctx, `SELECT id::text, allow_pk FROM rooms WHERE host_id = $1 AND status = 'live'`, hostID).Scan(&id, &allowPK)
	if db.IsNoRows(err) {
		return "", false, errs.BadRequest("not_live", "Go live before starting a PK battle.")
	}
	return
}

func (s *Service) Invite(ctx context.Context, p auth.Principal, toRoom string) (PKView, error) {
	fromRoom, allow, err := s.liveRoomOf(ctx, s.Pool, p.UserID)
	if err != nil {
		return PKView{}, err
	}
	var hostB string
	var allowB bool
	if err := s.Pool.QueryRow(ctx, `SELECT host_id::text, allow_pk FROM rooms WHERE id = $1 AND status = 'live'`, toRoom).Scan(&hostB, &allowB); err != nil {
		return PKView{}, ErrRoomEnded
	}
	if !allow || !allowB {
		return PKView{}, errs.Forbidden("pk_disabled", "PK battles are turned off for one of the rooms.")
	}
	if fromRoom == toRoom {
		return PKView{}, errs.BadRequest("pk_self", "Pick another host's room.")
	}
	id := events.NewID()
	_, err = s.Pool.Exec(ctx, `INSERT INTO pk_battles (id, room_a, room_b, host_a, host_b, duration_s) VALUES ($1, $2, $3, $4, $5, $6)`,
		id, fromRoom, toRoom, p.UserID, hostB, int(s.PKDuration.Seconds()))
	if db.IsUniqueViolation(err, "pk_open_a") || db.IsUniqueViolation(err, "pk_open_b") {
		return PKView{}, ErrPKBusy
	}
	if err != nil {
		return PKView{}, err
	}
	// Only the invited host's app acts on this message.
	from, _ := s.Users.Get(ctx, p.UserID)
	s.pushTo(ctx, toRoom, hostB, realtime.TypeSystem, map[string]any{"kind": "pk_invite", "pk_id": id, "from_room": fromRoom, "from_host": from})
	return PKView{ID: id, Status: "invited", RoomA: fromRoom, RoomB: toRoom}, nil
}

func (s *Service) pushTo(ctx context.Context, roomID, userID, typ string, data any) {
	b, _ := json.Marshal(realtime.Message{Type: typ, RoomID: roomID, Target: userID, At: time.Now().UTC(), Data: data})
	s.Redis.Publish(ctx, realtime.RoomChannel(roomID), b)
}

func (s *Service) Respond(ctx context.Context, p auth.Principal, pkID string, accept bool) (PKView, error) {
	var v PKView
	var hostA, hostB string
	var ends time.Time
	err := db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		var status string
		var dur int
		err := tx.QueryRow(ctx, `SELECT status, room_a::text, room_b::text, host_a::text, host_b::text, duration_s FROM pk_battles WHERE id = $1 FOR UPDATE`, pkID).
			Scan(&status, &v.RoomA, &v.RoomB, &hostA, &hostB, &dur)
		if db.IsNoRows(err) {
			return errs.ErrNotFound
		}
		if err != nil {
			return err
		}
		if hostB != p.UserID {
			return ErrNotHost
		}
		if status != "invited" {
			return errs.Conflict("pk_not_pending", "This invite is no longer open.")
		}
		v.ID = pkID
		if !accept {
			v.Status = "declined"
			_, err := tx.Exec(ctx, `UPDATE pk_battles SET status = 'declined' WHERE id = $1`, pkID)
			return err
		}
		ends = time.Now().Add(time.Duration(dur) * time.Second).UTC()
		v.Status, v.EndsAt = "active", ends.Unix()
		if _, err := tx.Exec(ctx, `UPDATE pk_battles SET status = 'active', started_at = now(), ends_at = $2 WHERE id = $1`, pkID, ends); err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, events.TopicLivePK, events.MustNew(producer, events.PKStarted, pkID,
			events.PKStartedData{PKID: pkID, RoomA: v.RoomA, RoomB: v.RoomB, HostA: hostA, HostB: hostB, EndsAt: ends}))
	})
	if err != nil || !accept {
		return v, err
	}
	pipe := s.Redis.TxPipeline()
	pipe.HSet(ctx, pkKey(pkID), "room_a", v.RoomA, "room_b", v.RoomB, "a", 0, "b", 0, "ends_at", ends.Unix())
	pipe.Expire(ctx, pkKey(pkID), time.Until(ends)+time.Hour)
	pipe.HSet(ctx, roomKey(v.RoomA), "pk_id", pkID, "pk_host", hostB)
	pipe.HSet(ctx, roomKey(v.RoomB), "pk_id", pkID, "pk_host", hostA)
	if _, err := pipe.Exec(ctx); err != nil {
		return v, err
	}
	started := map[string]any{"pk_id": pkID, "room_a": v.RoomA, "room_b": v.RoomB, "host_a": hostA, "host_b": hostB, "ends_at_unix": ends.Unix()}
	s.push(ctx, v.RoomA, realtime.TypePKStarted, started)
	s.push(ctx, v.RoomB, realtime.TypePKStarted, started)
	// The accepting host (B) publishes into room A; host A gets its cross
	// token from GET /v1/live/pk/{id}.
	tok, err := s.LK.Token(p.UserID, "", v.RoomA, true, s.TokenTTL)
	v.CrossTok, v.CrossURL = tok, s.LK.WSURL
	return v, err
}

// CrossToken gives a host the token to publish into the opponent's room.
func (s *Service) CrossToken(ctx context.Context, p auth.Principal, pkID string) (PKView, error) {
	var v PKView
	var hostA, hostB, status string
	var ends *time.Time
	if err := s.Pool.QueryRow(ctx, `SELECT status, room_a::text, room_b::text, host_a::text, host_b::text, ends_at FROM pk_battles WHERE id = $1`, pkID).
		Scan(&status, &v.RoomA, &v.RoomB, &hostA, &hostB, &ends); err != nil {
		return v, errs.ErrNotFound
	}
	v.ID, v.Status = pkID, status
	if ends != nil {
		v.EndsAt = ends.Unix()
	}
	if status != "active" {
		return v, nil
	}
	other := ""
	switch p.UserID {
	case hostA:
		other = v.RoomB
	case hostB:
		other = v.RoomA
	default:
		return v, nil
	}
	tok, err := s.LK.Token(p.UserID, "", other, true, s.TokenTTL)
	v.CrossTok, v.CrossURL = tok, s.LK.WSURL
	return v, err
}

// Tick ends due battles and expires stale invites. Several workers may run
// it at once: SKIP LOCKED gives each battle to exactly one of them.
func (s *Service) Tick(ctx context.Context) {
	if _, err := s.Pool.Exec(ctx, `UPDATE pk_battles SET status = 'expired'
		WHERE status = 'invited' AND invited_at < now() - make_interval(secs => $1)`, s.InviteTTL.Seconds()); err != nil {
		slog.ErrorContext(ctx, "expire pk invites", "err", err)
	}
	for {
		var done bool
		err := db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
			var id, ra, rb string
			err := tx.QueryRow(ctx, `SELECT id::text, room_a::text, room_b::text FROM pk_battles
				WHERE status = 'active' AND ends_at <= now() ORDER BY ends_at LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&id, &ra, &rb)
			if db.IsNoRows(err) {
				done = true
				return nil
			}
			if err != nil {
				return err
			}
			sc, _ := s.Redis.HMGet(ctx, pkKey(id), "a", "b").Result()
			a, _ := strconv.ParseInt(toStr(sc, 0), 10, 64)
			b, _ := strconv.ParseInt(toStr(sc, 1), 10, 64)
			var winner *string
			switch {
			case a > b:
				winner = &ra
			case b > a:
				winner = &rb
			}
			if _, err := tx.Exec(ctx, `UPDATE pk_battles SET status = 'ended', score_a = $2, score_b = $3, winner_room = $4 WHERE id = $1`, id, a, b, winner); err != nil {
				return err
			}
			res := events.PKEndedData{PKID: id, RoomA: ra, RoomB: rb, ScoreA: a, ScoreB: b}
			if winner != nil {
				res.WinnerRoom = *winner
			}
			if err := outbox.Insert(ctx, tx, events.TopicLivePK, events.MustNew(producer, events.PKEnded, id, res)); err != nil {
				return err
			}
			s.Redis.HDel(ctx, roomKey(ra), "pk_id", "pk_host")
			s.Redis.HDel(ctx, roomKey(rb), "pk_id", "pk_host")
			s.push(ctx, ra, realtime.TypePKEnded, res)
			s.push(ctx, rb, realtime.TypePKEnded, res)
			return nil
		})
		if err != nil {
			slog.ErrorContext(ctx, "end pk", "err", err)
			return
		}
		if done {
			return
		}
	}
}

func toStr(v []any, i int) string {
	if i < len(v) {
		if s, ok := v[i].(string); ok {
			return s
		}
	}
	return "0"
}
