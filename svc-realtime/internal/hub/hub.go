// Package hub is the WebSocket fan-out engine.
//
// Concurrency model (per pod):
//   - Each connection has two goroutines: a reader (client -> server: chat,
//     pings) and a writer (server -> client) fed by a buffered channel.
//   - One Redis pub/sub connection per pod. The pod subscribes to a room's
//     channel when its first local client joins and unsubscribes when the
//     last leaves, so Redis only sends a pod the rooms it actually serves.
//   - One dispatcher goroutine reads pub/sub and copies each message into the
//     local clients' channels without blocking. A client whose buffer is full
//     (slow network) is disconnected rather than slowing everyone else.
//
// Any pod can publish to a room; every pod with viewers in it delivers.
package hub

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/realtime"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/kafkax"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/metrics"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/userdir"
	"github.com/redis/go-redis/v9"
)

var (
	connsGauge  = metrics.NewGaugeVec("realtime_connections", "Open WebSocket connections on this pod.", "pod")
	roomsGauge  = metrics.NewGaugeVec("realtime_rooms", "Rooms with at least one local client.", "pod")
	dropped     = metrics.NewCounterVec("realtime_slow_clients_total", "Clients dropped for not keeping up.", "pod")
	delivered   = metrics.NewCounterVec("realtime_messages_delivered_total", "Messages written to clients.", "type")
	chatSampled = metrics.NewCounterVec("realtime_chat_sampled_total", "Chat messages shown only to the sender because the room was over its rate.", "pod")
)

type Options struct {
	MaxConns     int64
	SendBuffer   int
	RoomChatRate float64 // chat messages per second broadcast per room per pod
	UserChatRate float64
	BlockedWords []string
	Pod          string
}

type Hub struct {
	opt   Options
	rdb   redis.UniversalClient
	ps    *redis.PubSub
	users *userdir.Client
	prod  *kafkax.Producer

	mu    sync.RWMutex
	rooms map[string]*room
	conns atomic.Int64
}

type room struct {
	clients map[*client]struct{}
	chat    *bucket
}

func New(ctx context.Context, rdb redis.UniversalClient, users *userdir.Client, prod *kafkax.Producer, o Options) *Hub {
	if o.SendBuffer <= 0 {
		o.SendBuffer = 256
	}
	if o.RoomChatRate <= 0 {
		o.RoomChatRate = 30
	}
	if o.UserChatRate <= 0 {
		o.UserChatRate = 1
	}
	for i, w := range o.BlockedWords {
		o.BlockedWords[i] = strings.ToLower(w)
	}
	return &Hub{opt: o, rdb: rdb, ps: rdb.Subscribe(ctx), users: users, prod: prod, rooms: map[string]*room{}}
}

// Run dispatches pub/sub messages until ctx ends, then closes every client
// with "going away" so apps reconnect to another pod.
func (h *Hub) Run(ctx context.Context) error {
	ch := h.ps.Channel(redis.WithChannelSize(10000))
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			h.closeAll()
			return h.ps.Close()
		case <-tick.C:
			h.mu.RLock()
			roomsGauge.WithLabelValues(h.opt.Pod).Set(float64(len(h.rooms)))
			h.mu.RUnlock()
			connsGauge.WithLabelValues(h.opt.Pod).Set(float64(h.conns.Load()))
		case m, ok := <-ch:
			if !ok {
				return nil
			}
			h.dispatch(strings.TrimPrefix(m.Channel, "rt:room:"), []byte(m.Payload))
		}
	}
}

func (h *Hub) dispatch(roomID string, payload []byte) {
	var head struct {
		Type   string `json:"type"`
		Target string `json:"target"`
	}
	_ = json.Unmarshal(payload, &head)
	h.mu.RLock()
	r := h.rooms[roomID]
	if r == nil {
		h.mu.RUnlock()
		return
	}
	targets := make([]*client, 0, len(r.clients))
	for c := range r.clients {
		if head.Target == "" || head.Target == c.userID {
			targets = append(targets, c)
		}
	}
	h.mu.RUnlock()
	for _, c := range targets {
		c.enqueue(payload)
		if head.Type == realtime.TypeKick {
			c.closeAfterFlush(websocket.StatusPolicyViolation, "removed by moderator")
		}
	}
	delivered.WithLabelValues(head.Type).Add(float64(len(targets)))
}

func (h *Hub) join(ctx context.Context, c *client) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.rooms[c.roomID]
	if r == nil {
		if err := h.ps.Subscribe(ctx, realtime.RoomChannel(c.roomID)); err != nil {
			return err
		}
		r = &room{clients: map[*client]struct{}{}, chat: newBucket(h.opt.RoomChatRate, h.opt.RoomChatRate*2)}
		h.rooms[c.roomID] = r
	}
	r.clients[c] = struct{}{}
	return nil
}

func (h *Hub) leave(c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.rooms[c.roomID]
	if r == nil {
		return
	}
	delete(r.clients, c)
	if len(r.clients) == 0 {
		delete(h.rooms, c.roomID)
		_ = h.ps.Unsubscribe(context.Background(), realtime.RoomChannel(c.roomID))
	}
}

func (h *Hub) closeAll() {
	h.mu.RLock()
	var all []*client
	for _, r := range h.rooms {
		for c := range r.clients {
			all = append(all, c)
		}
	}
	h.mu.RUnlock()
	for _, c := range all {
		c.close(websocket.StatusGoingAway, "server restarting")
	}
}

// Publish sends a message to a room across all pods.
func (h *Hub) Publish(ctx context.Context, roomID string, m realtime.Message) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return h.rdb.Publish(ctx, realtime.RoomChannel(roomID), b).Err()
}

// OnGift (consumer of wallet.gifts) shows the gift banner in the room.
func (h *Hub) OnGift(ctx context.Context, e events.Envelope) error {
	if e.Type != events.GiftSent {
		return nil
	}
	var g events.GiftSentData
	if err := e.Decode(&g); err != nil || g.RoomID == "" {
		return nil
	}
	// Old gifts (consumer catching up after an outage) are not worth showing.
	if time.Since(g.SentAt) > 2*time.Minute {
		return nil
	}
	sender, _ := h.users.Get(ctx, g.SenderID)
	return h.Publish(ctx, g.RoomID, realtime.Message{Type: realtime.TypeGift, RoomID: g.RoomID, At: time.Now().UTC(),
		Data: map[string]any{"gift": realtime.Gift{SenderID: g.SenderID, ReceiverID: g.ReceiverID, GiftID: g.GiftID, Qty: g.Qty, Coins: g.Coins},
			"sender": map[string]string{"handle": sender.Handle, "display_name": sender.DisplayName, "avatar_url": sender.AvatarURL}}})
}

// OnModeration kicks users removed from a room.
func (h *Hub) OnModeration(ctx context.Context, e events.Envelope) error {
	var a events.ModerationActionData
	if err := e.Decode(&a); err != nil {
		return nil
	}
	if a.TargetType == "user" && (a.Action == "remove" || a.Action == "ban") && a.RoomID != "" {
		return h.Publish(ctx, a.RoomID, realtime.Message{Type: realtime.TypeKick, RoomID: a.RoomID, Target: a.TargetID, At: time.Now().UTC(),
			Data: map[string]string{"reason": a.Reason}})
	}
	return nil
}

func (h *Hub) filter(text string) string {
	if len(h.opt.BlockedWords) == 0 {
		return text
	}
	lower := strings.ToLower(text)
	out := []rune(text)
	for _, w := range h.opt.BlockedWords {
		for i := strings.Index(lower, w); i >= 0; {
			start := len([]rune(lower[:i]))
			for j := start; j < start+len([]rune(w)) && j < len(out); j++ {
				out[j] = '*'
			}
			next := strings.Index(lower[i+len(w):], w)
			if next < 0 {
				break
			}
			i += len(w) + next
		}
	}
	return string(out)
}

// bucket is a small token bucket (not goroutine-safe; callers hold a lock).
type bucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newBucket(rate, burst float64) *bucket {
	return &bucket{rate: rate, burst: burst, tokens: burst, last: time.Now()}
}

func (b *bucket) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.tokens = min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
