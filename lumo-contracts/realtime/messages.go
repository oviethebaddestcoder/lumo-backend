// Package realtime defines the messages pushed to clients over WebSocket.
//
// Any service can push to a room by publishing a Message as JSON to the Redis
// channel RoomChannel(roomID). Every realtime pod subscribes to the rooms its
// clients are in and fans messages out. Redis pub/sub is for ephemeral,
// fire-and-forget delivery; anything durable goes through Kafka first.
package realtime

import "time"

const (
	TypeChat      = "chat"
	TypeGift      = "gift"
	TypeViewers   = "viewers"
	TypeGoal      = "goal"
	TypePKStarted = "pk_started"
	TypePKScore   = "pk_score"
	TypePKEnded   = "pk_ended"
	TypeRoomEnded = "room_ended"
	TypeKick      = "kick"
	TypeSystem    = "system"
	TypeError     = "error"
)

// Message is the envelope sent to WebSocket clients.
type Message struct {
	Type   string    `json:"type"`
	RoomID string    `json:"room_id,omitempty"`
	At     time.Time `json:"at"`
	// Target, when set, limits delivery to one user in the room (e.g. kick).
	Target string `json:"target,omitempty"`
	Data   any    `json:"data,omitempty"`
}

// RoomChannel is the Redis pub/sub channel for a room.
func RoomChannel(roomID string) string { return "rt:room:" + roomID }

type Chat struct {
	ID          string `json:"id"`
	UserID      string `json:"user_id"`
	Handle      string `json:"handle"`
	DisplayName string `json:"display_name"`
	Text        string `json:"text"`
}

type Gift struct {
	SenderID   string `json:"sender_id"`
	ReceiverID string `json:"receiver_id"`
	GiftID     string `json:"gift_id"`
	Qty        int    `json:"qty"`
	Coins      int64  `json:"coins"`
}

type Viewers struct {
	Count int64 `json:"count"`
}

type Goal struct {
	Coins  int64 `json:"coins"`
	Target int64 `json:"target"`
}

type PKScore struct {
	PKID   string `json:"pk_id"`
	RoomA  string `json:"room_a"`
	RoomB  string `json:"room_b"`
	ScoreA int64  `json:"score_a"`
	ScoreB int64  `json:"score_b"`
	EndsAt int64  `json:"ends_at_unix"`
}
