// Package events defines the wire contract for every Kafka event in Lumo:
// the envelope, topic names, event types and payloads.
//
// Rules (enforced in review, not by code):
//   - Changes are additive only. Never rename or retype a field; add a new one.
//   - Breaking changes get a new event type with a higher Version.
//   - Every event is keyed so that events about the same entity stay in order.
package events

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Envelope wraps every event published to Kafka.
type Envelope struct {
	ID         string          `json:"event_id"`
	Type       string          `json:"type"`
	Version    int             `json:"version"`
	OccurredAt time.Time       `json:"occurred_at"`
	Producer   string          `json:"producer"`
	Key        string          `json:"key"`
	TraceID    string          `json:"trace_id,omitempty"`
	Data       json.RawMessage `json:"data"`
}

// New builds an envelope with a fresh time-ordered ID.
func New(producer, typ, key string, data any) (Envelope, error) {
	if typ == "" || key == "" {
		return Envelope{}, errors.New("events: type and key are required")
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return Envelope{}, fmt.Errorf("events: marshal %s: %w", typ, err)
	}
	return Envelope{
		ID:         NewID(),
		Type:       typ,
		Version:    1,
		OccurredAt: time.Now().UTC(),
		Producer:   producer,
		Key:        key,
		Data:       raw,
	}, nil
}

// MustNew is New for payloads that cannot fail to marshal (plain structs).
func MustNew(producer, typ, key string, data any) Envelope {
	e, err := New(producer, typ, key, data)
	if err != nil {
		panic(err)
	}
	return e
}

// Decode unmarshals the payload into v.
func (e Envelope) Decode(v any) error {
	if err := json.Unmarshal(e.Data, v); err != nil {
		return fmt.Errorf("events: decode %s %s: %w", e.Type, e.ID, err)
	}
	return nil
}

// NewID returns a UUIDv7 string (RFC 9562): 48-bit unix ms timestamp + random.
// Time-ordered IDs keep B-tree inserts append-mostly, which matters at scale.
func NewID() string {
	var b [16]byte
	ms := uint64(time.Now().UnixMilli())
	binary.BigEndian.PutUint64(b[0:8], ms<<16)
	if _, err := rand.Read(b[6:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x70 // version 7
	b[8] = (b[8] & 0x3f) | 0x80 // variant RFC 9562
	var s [36]byte
	hex.Encode(s[0:8], b[0:4])
	s[8] = '-'
	hex.Encode(s[9:13], b[4:6])
	s[13] = '-'
	hex.Encode(s[14:18], b[6:8])
	s[18] = '-'
	hex.Encode(s[19:23], b[8:10])
	s[23] = '-'
	hex.Encode(s[24:], b[10:])
	return string(s[:])
}
