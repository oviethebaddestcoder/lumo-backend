package hub

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/realtime"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/errs"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/httpx"
)

type client struct {
	h      *Hub
	conn   *websocket.Conn
	userID string
	roomID string
	send   chan []byte
	done   chan struct{}
	once   sync.Once
	chat   *bucket
}

func (c *client) enqueue(b []byte) {
	select {
	case c.send <- b:
	case <-c.done:
	default:
		dropped.WithLabelValues(c.h.opt.Pod).Inc()
		go c.close(websocket.StatusPolicyViolation, "too slow")
	}
}

func (c *client) close(code websocket.StatusCode, reason string) {
	c.once.Do(func() {
		close(c.done)
		go func() {
			_ = c.conn.Close(code, reason)
		}()
	})
}

// closeAfterFlush gives the writer a moment to deliver a final message.
func (c *client) closeAfterFlush(code websocket.StatusCode, reason string) {
	go func() {
		time.Sleep(200 * time.Millisecond)
		c.close(code, reason)
	}()
}

// ServeRoom upgrades GET /v1/rt/rooms/{id} to a WebSocket.
func (h *Hub) ServeRoom(origins []string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, err := auth.User(r)
		if err != nil {
			httpx.Error(w, r, err)
			return
		}
		roomID := r.PathValue("id")
		if len(roomID) != 36 {
			httpx.Error(w, r, errs.BadRequest("room_invalid", "Bad room ID."))
			return
		}
		if h.conns.Load() >= h.opt.MaxConns {
			w.Header().Set("Retry-After", "2")
			httpx.Error(w, r, errs.Unavailable("pod_full", "Server busy, reconnecting."))
			return
		}
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: origins, CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		conn.SetReadLimit(4096)
		c := &client{h: h, conn: conn, userID: p.UserID, roomID: roomID, send: make(chan []byte, h.opt.SendBuffer),
			done: make(chan struct{}), chat: newBucket(h.opt.UserChatRate, 3)}
		ctx := context.WithoutCancel(r.Context())
		if err := h.join(ctx, c); err != nil {
			slog.ErrorContext(ctx, "join room", "err", err)
			conn.Close(websocket.StatusInternalError, "try again")
			return
		}
		h.conns.Add(1)
		defer func() {
			h.leave(c)
			h.conns.Add(-1)
			c.close(websocket.StatusNormalClosure, "")
		}()
		hello, _ := json.Marshal(realtime.Message{Type: realtime.TypeSystem, RoomID: roomID, At: time.Now().UTC(), Data: map[string]string{"kind": "connected"}})
		c.enqueue(hello)
		go c.writeLoop(ctx)
		c.readLoop(ctx)
	}
}

func (c *client) writeLoop(ctx context.Context) {
	ping := time.NewTicker(25 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-c.done:
			return
		case b := <-c.send:
			wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := c.conn.Write(wctx, websocket.MessageText, b)
			cancel()
			if err != nil {
				c.close(websocket.StatusGoingAway, "write failed")
				return
			}
		case <-ping.C:
			pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := c.conn.Ping(pctx)
			cancel()
			if err != nil {
				c.close(websocket.StatusGoingAway, "ping timeout")
				return
			}
		}
	}
}

type inbound struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func (c *client) readLoop(ctx context.Context) {
	for {
		_, data, err := c.conn.Read(ctx)
		if err != nil {
			return
		}
		var in inbound
		if json.Unmarshal(data, &in) != nil {
			continue
		}
		if in.Type == "chat" {
			c.chatMessage(ctx, in.Text)
		}
	}
}

func (c *client) reply(typ string, data any) {
	b, _ := json.Marshal(realtime.Message{Type: typ, RoomID: c.roomID, At: time.Now().UTC(), Data: data})
	c.enqueue(b)
}

func (c *client) chatMessage(ctx context.Context, text string) {
	text = strings.TrimSpace(text)
	if text == "" || utf8.RuneCountInString(text) > 200 || !utf8.ValidString(text) {
		c.reply(realtime.TypeError, map[string]string{"code": "chat_invalid", "message": "Messages are 1 to 200 characters."})
		return
	}
	if !c.chat.allow() {
		c.reply(realtime.TypeError, map[string]string{"code": "chat_slow_down", "message": "You're sending messages too fast."})
		return
	}
	if muted, _ := c.h.rdb.SIsMember(ctx, "live:muted:"+c.roomID, c.userID).Result(); muted {
		c.reply(realtime.TypeError, map[string]string{"code": "chat_muted", "message": "You're muted in this room."})
		return
	}
	u, _ := c.h.users.Get(ctx, c.userID)
	msg := realtime.Message{Type: realtime.TypeChat, RoomID: c.roomID, At: time.Now().UTC(),
		Data: realtime.Chat{ID: events.NewID(), UserID: c.userID, Handle: u.Handle, DisplayName: u.DisplayName, Text: c.h.filter(text)}}

	c.h.mu.RLock()
	r := c.h.rooms[c.roomID]
	c.h.mu.RUnlock()
	if r != nil && !r.chat.allow() {
		// Huge room: past the per-pod chat rate, the sender still sees their
		// message but it isn't broadcast (TikTok-style chat sampling).
		chatSampled.WithLabelValues(c.h.opt.Pod).Inc()
		b, _ := json.Marshal(msg)
		c.enqueue(b)
	} else if err := c.h.Publish(ctx, c.roomID, msg); err != nil {
		c.reply(realtime.TypeError, map[string]string{"code": "chat_failed", "message": "Message not sent."})
		return
	}
	if c.h.prod != nil {
		chat := msg.Data.(realtime.Chat)
		c.h.prod.PublishAsync(ctx, events.TopicChatMessages, events.MustNew("svc-realtime", events.ChatSent, c.roomID,
			events.ChatMessageData{ID: chat.ID, RoomID: c.roomID, UserID: c.userID, Text: text, SentAt: msg.At}))
	}
}
