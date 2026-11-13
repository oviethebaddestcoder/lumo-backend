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

	// Personal channel (/v1/rt/me) message types.
	TypeDM     = "dm"      // data: DM
	TypeDMRead = "dm_read" // data: DMRead
	TypeNotify = "notify"  // data: {"unread": n} — inbox badge changed
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

// UserRoom is the pseudo-room holding one user's personal connections.
func UserRoom(userID string) string { return "u:" + userID }

// UserChannel is the Redis channel for messages to one user's devices.
func UserChannel(userID string) string { return RoomChannel(UserRoom(userID)) }

// DM is a direct message delivered live to both participants.
type DM struct {
	ID             string    `json:"id"`
	ConversationID string    `json:"conversation_id"`
	SenderID       string    `json:"sender_id"`
	Text           string    `json:"text"`
	ClientID       string    `json:"client_id,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

// DMRead tells the other participant how far a conversation was read.
type DMRead struct {
	ConversationID string    `json:"conversation_id"`
	UserID         string    `json:"user_id"`
	ReadAt         time.Time `json:"read_at"`
}

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
