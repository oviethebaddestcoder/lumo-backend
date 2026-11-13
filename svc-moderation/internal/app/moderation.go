// Package app is svc-moderation: the report queue and moderator actions.
//
// Reports arrive as events (from svc-social, and automatic flags from the
// chat scanner). Moderators work the queue, ordered by how many open reports
// a target has. An action (ban, remove, end_room, mute) is written with an
// outbox event on moderation.actions; each owning service applies it:
// identity bans and revokes sessions, media removes videos, social deletes
// comments, live ends rooms, realtime kicks from chat.
package app

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/errs"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/outbox"
)

var allowed = map[string][]string{
	"user":    {"ban", "unban", "mute", "remove"},
	"video":   {"remove"},
	"comment": {"remove"},
	"room":    {"end_room"},
}

type Service struct {
	Pool      *pgxpool.Pool
	FlagWords []string // chat words that create an automatic report
}

// OnReport stores user reports (consumer of social.events).
func (s *Service) OnReport(ctx context.Context, e events.Envelope) error {
	if e.Type != events.Reported {
		return nil
	}
	var r events.ReportData
	if e.Decode(&r) != nil {
		return nil
	}
	_, err := s.Pool.Exec(ctx, `INSERT INTO reports (id, reporter_id, target_type, target_id, reason, details)
		VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (id) DO NOTHING`, r.ReportID, r.ReporterID, r.TargetType, r.TargetID, r.Reason, r.Details)
	return err
}

// OnChat flags chat messages containing listed words (consumer of
// chat.messages). Plug an ML classifier in here for production scale.
func (s *Service) OnChat(ctx context.Context, e events.Envelope) error {
	if e.Type != events.ChatSent || len(s.FlagWords) == 0 {
		return nil
	}
	var m events.ChatMessageData
	if e.Decode(&m) != nil {
		return nil
	}
	lower := strings.ToLower(m.Text)
	for _, w := range s.FlagWords {
		if strings.Contains(lower, w) {
			_, err := s.Pool.Exec(ctx, `INSERT INTO reports (id, reporter_id, target_type, target_id, reason, details)
				VALUES ($1, 'system', 'user', $2, 'harassment', $3) ON CONFLICT (id) DO NOTHING`,
				events.NewID(), m.UserID, "chat in room "+m.RoomID+": "+m.Text)
			return err
		}
	}
	return nil
}

type QueueItem struct {
	TargetType  string    `json:"target_type"`
	TargetID    string    `json:"target_id"`
	OpenReports int       `json:"open_reports"`
	Reasons     []string  `json:"reasons"`
	FirstAt     time.Time `json:"first_reported_at"`
	ReportIDs   []string  `json:"report_ids"`
}

// Queue returns targets with open reports, most-reported first.
func (s *Service) Queue(ctx context.Context, limit int) ([]QueueItem, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	rows, err := s.Pool.Query(ctx, `SELECT target_type, target_id, count(*), array_agg(DISTINCT reason), min(created_at),
			(array_agg(id::text ORDER BY created_at))[1:50]
		FROM reports WHERE status = 'open' GROUP BY target_type, target_id
		ORDER BY count(*) DESC, min(created_at) LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (QueueItem, error) {
		var q QueueItem
		return q, r.Scan(&q.TargetType, &q.TargetID, &q.OpenReports, &q.Reasons, &q.FirstAt, &q.ReportIDs)
	})
}

type ActionInput struct {
	TargetType string     `json:"target_type"`
	TargetID   string     `json:"target_id"`
	Action     string     `json:"action"`
	Reason     string     `json:"reason"`
	Until      *time.Time `json:"until,omitempty"`
	RoomID     string     `json:"room_id,omitempty"`
}

// Act records a moderator action, resolves the target's open reports and
// publishes moderation.actions in the same transaction.
func (s *Service) Act(ctx context.Context, p auth.Principal, in ActionInput) (string, error) {
	if !slices.Contains(allowed[in.TargetType], in.Action) {
		return "", errs.BadRequest("action_invalid", "That action doesn't apply to this target.")
	}
	if strings.TrimSpace(in.Reason) == "" {
		return "", errs.BadRequest("reason_required", "Give a reason (it's shown in the audit log).")
	}
	id := events.NewID()
	err := db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO actions (id, target_type, target_id, action, reason, actor_id, room_id, until)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, id, in.TargetType, in.TargetID, in.Action, in.Reason, p.UserID, in.RoomID, in.Until); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE reports SET status = 'actioned', action_id = $3, resolved_by = $4, resolved_at = now()
			WHERE target_type = $1 AND target_id = $2 AND status = 'open'`, in.TargetType, in.TargetID, id, p.UserID); err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, events.TopicModeration, events.MustNew("svc-moderation", events.ModerationAction, in.TargetID,
			events.ModerationActionData{ActionID: id, TargetType: in.TargetType, TargetID: in.TargetID, Action: in.Action,
				Reason: in.Reason, ActorID: p.UserID, Until: in.Until, RoomID: in.RoomID}))
	})
	return id, err
}

// Dismiss closes a target's open reports without action.
func (s *Service) Dismiss(ctx context.Context, p auth.Principal, targetType, targetID string) error {
	_, err := s.Pool.Exec(ctx, `UPDATE reports SET status = 'dismissed', resolved_by = $3, resolved_at = now()
		WHERE target_type = $1 AND target_id = $2 AND status = 'open'`, targetType, targetID, p.UserID)
	return err
}

// History is the audit log for a target.
func (s *Service) History(ctx context.Context, targetType, targetID string) ([]map[string]any, error) {
	rows, err := s.Pool.Query(ctx, `SELECT id::text, action, reason, actor_id::text, created_at FROM actions
		WHERE target_type = $1 AND target_id = $2 ORDER BY created_at DESC LIMIT 100`, targetType, targetID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (map[string]any, error) {
		var id, action, reason, actor string
		var at time.Time
		err := r.Scan(&id, &action, &reason, &actor, &at)
		return map[string]any{"id": id, "action": action, "reason": reason, "actor_id": actor, "created_at": at}, err
	})
}
