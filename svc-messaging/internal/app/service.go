// Package app is svc-messaging: one-to-one direct messages.
//
// Sending a message writes it, bumps both members' inbox rows and inserts a
// dm.messages outbox event in ONE transaction. After commit the message is
// pushed live to both users' personal realtime channels (Redis pub/sub,
// best effort: the apps also refetch on reconnect). The notifier turns the
// Kafka event into a push notification for the recipient.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/realtime"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/errs"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/outbox"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/redisx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/userdir"
	"github.com/redis/go-redis/v9"
)

const producer = "svc-messaging"

var (
	ErrBlocked  = errs.Forbidden("blocked", "You can't message this account.")
	ErrSelf     = errs.BadRequest("self_message", "You can't message yourself.")
	ErrNotInDM  = errs.ErrNotFound
	ErrTooFast  = errs.New(429, "slow_down", "You're sending messages too fast.")
	ErrTooMany  = errs.New(429, "too_many_new_chats", "You've started too many new chats today. Try again tomorrow.")
	ErrBadText  = errs.BadRequest("text_invalid", "Messages are 1 to 1000 characters.")
	ErrClientID = errs.BadRequest("client_id_required", "Send a client_id (max 64 chars) so retries don't duplicate.")
)

type Service struct {
	Pool  *pgxpool.Pool
	Redis redis.UniversalClient
	Users *userdir.Client
	// Limits (defaults if zero).
	MessagesPerMinute int
	NewChatsPerDay    int
}

func (s *Service) limiter() redisx.Limiter { return redisx.Limiter{R: s.Redis, Prefix: "rl:dm:"} }

type Conversation struct {
	ID            string            `json:"id"`
	Peer          events.UserPublic `json:"peer"`
	LastMessageAt *time.Time        `json:"last_message_at,omitempty"`
	LastPreview   string            `json:"last_preview"`
	LastFromMe    bool              `json:"last_from_me"`
	Unread        int               `json:"unread"`
	PeerReadAt    *time.Time        `json:"peer_read_at,omitempty"`
}

type Message struct {
	ID             string    `json:"id"`
	ConversationID string    `json:"conversation_id"`
	SenderID       string    `json:"sender_id"`
	ClientID       string    `json:"client_id,omitempty"`
	Text           string    `json:"text"`
	CreatedAt      time.Time `json:"created_at"`
	Deleted        bool      `json:"deleted,omitempty"`
}

func (s *Service) blockedEither(ctx context.Context, q db.Querier, a, b string) (bool, error) {
	var x bool
	err := q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM blocks WHERE (blocker_id = $1 AND blocked_id = $2) OR (blocker_id = $2 AND blocked_id = $1))`, a, b).Scan(&x)
	return x, err
}

// Open returns the conversation between me and other, creating it if needed.
func (s *Service) Open(ctx context.Context, me, other string) (Conversation, error) {
	if len(other) != 36 {
		return Conversation{}, errs.BadRequest("user_invalid", "Bad user ID.")
	}
	if me == other {
		return Conversation{}, ErrSelf
	}
	if blocked, err := s.blockedEither(ctx, s.Pool, me, other); err != nil || blocked {
		if err == nil {
			err = ErrBlocked
		}
		return Conversation{}, err
	}
	if _, err := s.Users.Get(ctx, other); err != nil {
		return Conversation{}, errs.ErrNotFound
	}
	lo, hi := me, other
	if hi < lo {
		lo, hi = hi, lo
	}
	var id string
	err := s.Pool.QueryRow(ctx, `SELECT id::text FROM conversations WHERE user_low = $1 AND user_high = $2`, lo, hi).Scan(&id)
	if db.IsNoRows(err) {
		// New chat: rate limited to stop spam runs.
		limit := s.NewChatsPerDay
		if limit <= 0 {
			limit = 50
		}
		if r, _ := s.limiter().Allow(ctx, "new:"+me, limit, 24*time.Hour); !r.Allowed {
			return Conversation{}, ErrTooMany
		}
		err = db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
			id = events.NewID()
			tag, err := tx.Exec(ctx, `INSERT INTO conversations (id, user_low, user_high) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, id, lo, hi)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 { // created concurrently
				return tx.QueryRow(ctx, `SELECT id::text FROM conversations WHERE user_low = $1 AND user_high = $2`, lo, hi).Scan(&id)
			}
			_, err = tx.Exec(ctx, `INSERT INTO members (user_id, conversation_id, peer_id, hidden) VALUES ($1, $3, $2, false), ($2, $3, $1, true)`, me, other, id)
			return err
		})
	}
	if err != nil {
		return Conversation{}, err
	}
	// Opening an existing chat un-hides it for me.
	if _, err := s.Pool.Exec(ctx, `UPDATE members SET hidden = false WHERE user_id = $1 AND conversation_id = $2 AND hidden`, me, id); err != nil {
		return Conversation{}, err
	}
	return s.conversation(ctx, me, id)
}

const convCols = `m.conversation_id::text, m.peer_id::text, m.last_message_at, m.last_preview, COALESCE(m.last_sender_id::text = m.user_id::text, false),
	m.unread, p.read_at`

func scanConv(r pgx.Row) (Conversation, string, error) {
	var c Conversation
	var peer string
	err := r.Scan(&c.ID, &peer, &c.LastMessageAt, &c.LastPreview, &c.LastFromMe, &c.Unread, &c.PeerReadAt)
	return c, peer, err
}

func (s *Service) conversation(ctx context.Context, me, id string) (Conversation, error) {
	c, peer, err := scanConv(s.Pool.QueryRow(ctx, `SELECT `+convCols+` FROM members m
		JOIN members p ON p.conversation_id = m.conversation_id AND p.user_id = m.peer_id
		WHERE m.user_id = $1 AND m.conversation_id = $2`, me, id))
	if db.IsNoRows(err) {
		return c, ErrNotInDM
	}
	if err != nil {
		return c, err
	}
	c.Peer, _ = s.Users.Get(ctx, peer)
	if c.Peer.ID == "" {
		c.Peer = events.UserPublic{ID: peer}
	}
	return c, nil
}

// Conversations lists my chats, most recent first. cursor is the
// last_message_at (RFC3339Nano) of the last item on the previous page.
func (s *Service) Conversations(ctx context.Context, me, cursor string, limit int) ([]Conversation, string, error) {
	if limit <= 0 || limit > 50 {
		limit = 30
	}
	before := time.Now().Add(time.Hour)
	if t, err := time.Parse(time.RFC3339Nano, cursor); err == nil {
		before = t
	}
	rows, err := s.Pool.Query(ctx, `SELECT `+convCols+` FROM members m
		JOIN members p ON p.conversation_id = m.conversation_id AND p.user_id = m.peer_id
		WHERE m.user_id = $1 AND NOT m.hidden AND m.last_message_at IS NOT NULL AND m.last_message_at < $2
		ORDER BY m.last_message_at DESC LIMIT $3`, me, before, limit)
	if err != nil {
		return nil, "", err
	}
	type row struct {
		c    Conversation
		peer string
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (row, error) {
		c, p, err := scanConv(r)
		return row{c, p}, err
	})
	if err != nil {
		return nil, "", err
	}
	ids := make([]string, len(list))
	for i, x := range list {
		ids[i] = x.peer
	}
	users, _ := s.Users.Many(ctx, ids)
	out := make([]Conversation, len(list))
	for i, x := range list {
		x.c.Peer = users[x.peer]
		if x.c.Peer.ID == "" {
			x.c.Peer = events.UserPublic{ID: x.peer}
		}
		out[i] = x.c
	}
	next := ""
	if len(out) == limit && out[len(out)-1].LastMessageAt != nil {
		next = out[len(out)-1].LastMessageAt.Format(time.RFC3339Nano)
	}
	return out, next, nil
}

func (s *Service) member(ctx context.Context, q db.Querier, me, convID string) (peer string, err error) {
	err = q.QueryRow(ctx, `SELECT peer_id::text FROM members WHERE user_id = $1 AND conversation_id = $2`, me, convID).Scan(&peer)
	if db.IsNoRows(err) {
		return "", ErrNotInDM
	}
	return peer, err
}

// Messages pages backwards through a conversation (cursor = oldest ID seen).
func (s *Service) Messages(ctx context.Context, me, convID, cursor string, limit int) ([]Message, string, error) {
	if len(convID) != 36 {
		return nil, "", ErrNotInDM
	}
	if _, err := s.member(ctx, s.Pool, me, convID); err != nil {
		return nil, "", err
	}
	if limit <= 0 || limit > 100 {
		limit = 40
	}
	if len(cursor) != 36 {
		cursor = "ffffffff-ffff-ffff-ffff-ffffffffffff"
	}
	rows, err := s.Pool.Query(ctx, `SELECT id::text, conversation_id::text, sender_id::text, client_id,
			CASE WHEN deleted_at IS NULL THEN text ELSE '' END, created_at, deleted_at IS NOT NULL
		FROM messages WHERE conversation_id = $1 AND id < $2 ORDER BY id DESC LIMIT $3`, convID, cursor, limit)
	if err != nil {
		return nil, "", err
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Message, error) {
		var m Message
		return m, r.Scan(&m.ID, &m.ConversationID, &m.SenderID, &m.ClientID, &m.Text, &m.CreatedAt, &m.Deleted)
	})
	next := ""
	if len(list) == limit {
		next = list[len(list)-1].ID
	}
	return list, next, err
}

func preview(text string) string {
	text = strings.Join(strings.Fields(text), " ")
	if utf8.RuneCountInString(text) > 80 {
		return string([]rune(text)[:80]) + "…"
	}
	return text
}

// Send stores a message. Retrying with the same client_id returns the original.
func (s *Service) Send(ctx context.Context, me, convID, clientID, text string) (Message, error) {
	text = strings.TrimSpace(text)
	if text == "" || utf8.RuneCountInString(text) > 1000 || !utf8.ValidString(text) {
		return Message{}, ErrBadText
	}
	if clientID == "" || len(clientID) > 64 {
		return Message{}, ErrClientID
	}
	if len(convID) != 36 {
		return Message{}, ErrNotInDM
	}
	perMin := s.MessagesPerMinute
	if perMin <= 0 {
		perMin = 30
	}
	if r, _ := s.limiter().Allow(ctx, "msg:"+me, perMin, time.Minute); !r.Allowed {
		return Message{}, ErrTooFast
	}
	var m Message
	var peer string
	replay := false
	err := db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		var err error
		if peer, err = s.member(ctx, tx, me, convID); err != nil {
			return err
		}
		if blocked, err := s.blockedEither(ctx, tx, me, peer); err != nil || blocked {
			if err == nil {
				err = ErrBlocked
			}
			return err
		}
		m = Message{ID: events.NewID(), ConversationID: convID, SenderID: me, ClientID: clientID, Text: text, CreatedAt: time.Now().UTC()}
		_, err = tx.Exec(ctx, `INSERT INTO messages (id, conversation_id, sender_id, client_id, text, created_at) VALUES ($1, $2, $3, $4, $5, $6)`,
			m.ID, convID, me, clientID, text, m.CreatedAt)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return errReplay
		}
		if err != nil {
			return err
		}
		pv := preview(text)
		if _, err := tx.Exec(ctx, `UPDATE members SET last_message_at = $3, last_preview = $4, last_sender_id = $1, hidden = false,
				unread = CASE WHEN user_id = $1 THEN 0 ELSE unread + 1 END,
				read_at = CASE WHEN user_id = $1 THEN $3 ELSE read_at END
			WHERE conversation_id = $2`, me, convID, m.CreatedAt, pv); err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, events.TopicDMMessages, events.MustNew(producer, events.DMSent, convID, events.DMSentData{
			MessageID: m.ID, ConversationID: convID, SenderID: me, RecipientID: peer, Preview: pv, SentAt: m.CreatedAt}))
	})
	if errors.Is(err, errReplay) {
		replay = true
		err = s.Pool.QueryRow(ctx, `SELECT id::text, conversation_id::text, sender_id::text, client_id, text, created_at FROM messages
			WHERE sender_id = $1 AND client_id = $2`, me, clientID).Scan(&m.ID, &m.ConversationID, &m.SenderID, &m.ClientID, &m.Text, &m.CreatedAt)
	}
	if err != nil {
		return Message{}, err
	}
	if !replay {
		s.push(ctx, []string{me, peer}, realtime.TypeDM, realtime.DM{ID: m.ID, ConversationID: convID, SenderID: me, Text: m.Text,
			ClientID: clientID, CreatedAt: m.CreatedAt})
	}
	return m, nil
}

var errReplay = errors.New("replay")

// MarkRead clears my unread count and tells the sender (read receipt).
func (s *Service) MarkRead(ctx context.Context, me, convID string) error {
	if len(convID) != 36 {
		return ErrNotInDM
	}
	now := time.Now().UTC()
	var peer string
	err := s.Pool.QueryRow(ctx, `UPDATE members SET unread = 0, read_at = $3 WHERE user_id = $1 AND conversation_id = $2 RETURNING peer_id::text`,
		me, convID, now).Scan(&peer)
	if db.IsNoRows(err) {
		return ErrNotInDM
	}
	if err != nil {
		return err
	}
	s.push(ctx, []string{peer, me}, realtime.TypeDMRead, realtime.DMRead{ConversationID: convID, UserID: me, ReadAt: now})
	return nil
}

// Unsend replaces my message with a tombstone.
func (s *Service) Unsend(ctx context.Context, me, messageID string) error {
	if len(messageID) != 36 {
		return errs.ErrNotFound
	}
	tag, err := s.Pool.Exec(ctx, `UPDATE messages SET deleted_at = now() WHERE id = $1 AND sender_id = $2 AND deleted_at IS NULL`, messageID, me)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errs.ErrNotFound
	}
	return nil
}

// Hide removes a chat from my list (it reappears on a new message).
func (s *Service) Hide(ctx context.Context, me, convID string) error {
	if len(convID) != 36 {
		return ErrNotInDM
	}
	_, err := s.Pool.Exec(ctx, `UPDATE members SET hidden = true, unread = 0 WHERE user_id = $1 AND conversation_id = $2`, me, convID)
	return err
}

// Unread is the total unread messages across chats (badge, capped at 999).
func (s *Service) Unread(ctx context.Context, me string) (int, error) {
	var n int
	err := s.Pool.QueryRow(ctx, `SELECT LEAST(COALESCE(sum(unread), 0), 999) FROM members WHERE user_id = $1 AND NOT hidden`, me).Scan(&n)
	return n, err
}

// push delivers to each user's personal realtime channel (best effort).
func (s *Service) push(ctx context.Context, users []string, typ string, data any) {
	if s.Redis == nil {
		return
	}
	b, _ := json.Marshal(realtime.Message{Type: typ, At: time.Now().UTC(), Data: data})
	for _, u := range users {
		if err := s.Redis.Publish(ctx, realtime.UserChannel(u), b).Err(); err != nil {
			slog.WarnContext(ctx, "dm live push failed", "err", err)
		}
	}
}

// OnSocial keeps the block projection current.
func (s *Service) OnSocial(ctx context.Context, e events.Envelope) error {
	if e.Type != events.Blocked && e.Type != events.Unblocked {
		return nil
	}
	var b events.BlockData
	if e.Decode(&b) != nil || len(b.BlockerID) != 36 || len(b.BlockedID) != 36 {
		return nil
	}
	q := `INSERT INTO blocks (blocker_id, blocked_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`
	if e.Type == events.Unblocked {
		q = `DELETE FROM blocks WHERE blocker_id = $1 AND blocked_id = $2`
	}
	_, err := s.Pool.Exec(ctx, q, b.BlockerID, b.BlockedID)
	return err
}

// OnIdentity hides chats with deleted accounts.
func (s *Service) OnIdentity(ctx context.Context, e events.Envelope) error {
	if e.Type != events.UserDeleted {
		return nil
	}
	var d events.UserDeletedData
	if e.Decode(&d) != nil || len(d.UserID) != 36 {
		return nil
	}
	_, err := s.Pool.Exec(ctx, `UPDATE members SET hidden = true WHERE peer_id = $1 OR user_id = $1`, d.UserID)
	return err
}

// Get returns one of my conversations.
func (s *Service) Get(ctx context.Context, me, convID string) (Conversation, error) {
	if len(convID) != 36 {
		return Conversation{}, ErrNotInDM
	}
	return s.conversation(ctx, me, convID)
}
