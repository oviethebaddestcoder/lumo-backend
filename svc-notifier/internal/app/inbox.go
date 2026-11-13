package app

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
)

type InboxItem struct {
	ID          int64             `json:"id"`
	Kind        string            `json:"kind"`
	ActorID     *string           `json:"actor_id,omitempty"`
	ActorName   string            `json:"actor_name,omitempty"`
	ActorAvatar string            `json:"actor_avatar,omitempty"`
	Title       string            `json:"title"`
	Body        string            `json:"body,omitempty"`
	Data        map[string]string `json:"data"`
	CreatedAt   time.Time         `json:"created_at"`
	Read        bool              `json:"read"`
}

// addInbox records an activity item for one user. actorID may be "".
func (s *Service) addInbox(ctx context.Context, userID, kind, actorID string, p Push) error {
	var actor *string
	name, avatar := "", ""
	if actorID != "" {
		actor = &actorID
		if s.Users != nil {
			if u, err := s.Users.Get(ctx, actorID); err == nil {
				name, avatar = u.DisplayName, u.AvatarURL
			}
		}
	}
	data, _ := json.Marshal(p.Data)
	_, err := s.Pool.Exec(ctx, `INSERT INTO inbox (user_id, kind, actor_id, actor_name, actor_avatar, title, body, data)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, userID, kind, actor, name, avatar, p.Title, p.Body, data)
	return err
}

// Inbox lists a user's activity, newest first. cursor is the last ID seen.
func (s *Service) Inbox(ctx context.Context, userID, cursor string, limit int) ([]InboxItem, string, error) {
	if limit <= 0 || limit > 50 {
		limit = 30
	}
	before, _ := strconv.ParseInt(cursor, 10, 64)
	if before <= 0 {
		before = 1<<63 - 1
	}
	rows, err := s.Pool.Query(ctx, `SELECT i.id, i.kind, i.actor_id::text, i.actor_name, i.actor_avatar, i.title, i.body, i.data, i.created_at,
			i.id <= COALESCE(r.last_read_id, 0)
		FROM inbox i LEFT JOIN inbox_reads r ON r.user_id = i.user_id
		WHERE i.user_id = $1 AND i.id < $2 ORDER BY i.id DESC LIMIT $3`, userID, before, limit)
	if err != nil {
		return nil, "", err
	}
	items, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (InboxItem, error) {
		var it InboxItem
		var data []byte
		err := r.Scan(&it.ID, &it.Kind, &it.ActorID, &it.ActorName, &it.ActorAvatar, &it.Title, &it.Body, &data, &it.CreatedAt, &it.Read)
		if err == nil {
			_ = json.Unmarshal(data, &it.Data)
		}
		return it, err
	})
	next := ""
	if len(items) == limit {
		next = strconv.FormatInt(items[len(items)-1].ID, 10)
	}
	return items, next, err
}

// Unread counts unread items (capped at 99 for the badge).
func (s *Service) Unread(ctx context.Context, userID string) (int, error) {
	var n int
	err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT 1 FROM inbox
		WHERE user_id = $1 AND id > COALESCE((SELECT last_read_id FROM inbox_reads WHERE user_id = $1), 0) LIMIT 99) x`, userID).Scan(&n)
	return n, err
}

// MarkRead marks everything up to the newest item as read.
func (s *Service) MarkRead(ctx context.Context, userID string) error {
	_, err := s.Pool.Exec(ctx, `INSERT INTO inbox_reads (user_id, last_read_id)
		SELECT $1, COALESCE(max(id), 0) FROM inbox WHERE user_id = $1
		ON CONFLICT (user_id) DO UPDATE SET last_read_id = GREATEST(inbox_reads.last_read_id, EXCLUDED.last_read_id)`, userID)
	return err
}

// TrimInbox deletes items older than 90 days (worker cron).
func (s *Service) TrimInbox(ctx context.Context) error {
	_, err := s.Pool.Exec(ctx, `DELETE FROM inbox WHERE created_at < now() - interval '90 days'`)
	return err
}

// OnWallet tells users when a cash-out settles.
func (s *Service) OnWallet(ctx context.Context, e events.Envelope) error {
	if e.Type != events.PayoutSettled || !s.once(ctx, e.ID, 24*time.Hour) {
		return nil
	}
	var d events.PayoutSettledData
	if e.Decode(&d) != nil {
		return nil
	}
	naira := fmt.Sprintf("₦%d.%02d", d.AmountKobo/100, d.AmountKobo%100)
	p := Push{Data: map[string]string{"type": "payout", "payout_id": d.PayoutID}}
	switch d.Status {
	case "paid":
		p.Title, p.Body = "Cash-out paid", naira+" is on its way to your bank account."
	case "rejected":
		p.Title, p.Body = "Cash-out declined", "Your diamonds were returned. "+d.Reason
	default:
		p.Title, p.Body = "Cash-out failed", "The bank transfer didn't go through. Your diamonds were returned."
	}
	if err := s.addInbox(ctx, d.UserID, "payout", "", p); err != nil {
		return err
	}
	return s.sendTo(ctx, "account", []string{d.UserID}, p)
}

// OnDM pushes "new message" to the recipient (not stored in the activity
// inbox: chats have their own unread counts). Bursts collapse to one push
// per conversation per 30 s.
func (s *Service) OnDM(ctx context.Context, e events.Envelope) error {
	if e.Type != events.DMSent || !s.once(ctx, e.ID, 24*time.Hour) {
		return nil
	}
	var d events.DMSentData
	if e.Decode(&d) != nil || d.RecipientID == "" {
		return nil
	}
	if !s.once(ctx, "dm:"+d.ConversationID+":"+d.RecipientID, 30*time.Second) {
		return nil
	}
	u, _ := s.Users.Get(ctx, d.SenderID)
	name := u.DisplayName
	if name == "" {
		name = "New message"
	}
	return s.sendTo(ctx, "message", []string{d.RecipientID}, Push{Title: name, Body: d.Preview,
		Data: map[string]string{"type": "dm", "conversation_id": d.ConversationID, "user_id": d.SenderID}})
}
