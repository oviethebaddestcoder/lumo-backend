// Package livekit is a small client for LiveKit (self-hosted or Cloud):
// access tokens, webhook verification, and the Twirp server API for rooms,
// ingress (RTMP from OBS) and egress (HLS for big rooms, MP4 replays).
// It avoids the full server SDK to keep the dependency tree small.
package livekit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type Client struct {
	WSURL     string // wss://live.example.com (given to apps)
	APIURL    string // https://live.example.com (server API)
	APIKey    string
	APISecret string
	HTTP      *http.Client

	// Object storage that egress writes to (HLS segments, replays).
	S3 S3Output
}

type S3Output struct {
	AccessKey      string `json:"access_key"`
	Secret         string `json:"secret"`
	Region         string `json:"region"`
	Endpoint       string `json:"endpoint"`
	Bucket         string `json:"bucket"`
	ForcePathStyle bool   `json:"force_path_style"`
}

// VideoGrant mirrors LiveKit's video grant claims.
type VideoGrant struct {
	Room           string `json:"room,omitempty"`
	RoomJoin       bool   `json:"roomJoin,omitempty"`
	RoomCreate     bool   `json:"roomCreate,omitempty"`
	RoomAdmin      bool   `json:"roomAdmin,omitempty"`
	RoomRecord     bool   `json:"roomRecord,omitempty"`
	IngressAdmin   bool   `json:"ingressAdmin,omitempty"`
	CanPublish     *bool  `json:"canPublish,omitempty"`
	CanSubscribe   *bool  `json:"canSubscribe,omitempty"`
	CanPublishData *bool  `json:"canPublishData,omitempty"`
	Hidden         bool   `json:"hidden,omitempty"`
}

type claims struct {
	jwt.RegisteredClaims
	Name     string     `json:"name,omitempty"`
	Video    VideoGrant `json:"video"`
	Metadata string     `json:"metadata,omitempty"`
	Sha256   string     `json:"sha256,omitempty"`
}

func b(v bool) *bool { return &v }

// Token issues a participant token.
func (c *Client) Token(identity, name, room string, publish bool, ttl time.Duration) (string, error) {
	now := time.Now()
	cl := claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer: c.APIKey, Subject: identity, ID: identity,
			NotBefore: jwt.NewNumericDate(now.Add(-10 * time.Second)), ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		},
		Name: name,
		Video: VideoGrant{Room: room, RoomJoin: true, CanPublish: b(publish), CanSubscribe: b(true),
			CanPublishData: b(false)},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, cl).SignedString([]byte(c.APISecret))
}

func (c *Client) adminToken(g VideoGrant) (string, error) {
	now := time.Now()
	cl := claims{RegisteredClaims: jwt.RegisteredClaims{Issuer: c.APIKey, Subject: "svc-live",
		NotBefore: jwt.NewNumericDate(now.Add(-10 * time.Second)), ExpiresAt: jwt.NewNumericDate(now.Add(5 * time.Minute))}, Video: g}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, cl).SignedString([]byte(c.APISecret))
}

// ---- webhooks ----

type Webhook struct {
	Event string `json:"event"`
	ID    string `json:"id"`
	Room  *struct {
		Name string `json:"name"`
	} `json:"room"`
	Participant *struct {
		Identity string `json:"identity"`
	} `json:"participant"`
	EgressInfo *struct {
		EgressID    string `json:"egressId"`
		RoomName    string `json:"roomName"`
		Status      string `json:"status"`
		FileResults []struct {
			Filename string `json:"filename"`
			Location string `json:"location"`
		} `json:"fileResults"`
	} `json:"egressInfo"`
}

var ErrBadWebhook = errors.New("livekit: invalid webhook signature")

// VerifyWebhook checks the Authorization JWT (signed with the API secret)
// and that its sha256 claim matches the body.
func (c *Client) VerifyWebhook(body []byte, authHeader string) (Webhook, error) {
	tok := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
	var cl claims
	_, err := jwt.ParseWithClaims(tok, &cl, func(*jwt.Token) (any, error) { return []byte(c.APISecret), nil },
		jwt.WithValidMethods([]string{"HS256"}), jwt.WithIssuer(c.APIKey), jwt.WithLeeway(time.Minute))
	if err != nil {
		return Webhook{}, fmt.Errorf("%w: %v", ErrBadWebhook, err)
	}
	sum := sha256.Sum256(body)
	if cl.Sha256 != base64.StdEncoding.EncodeToString(sum[:]) {
		return Webhook{}, fmt.Errorf("%w: body hash mismatch", ErrBadWebhook)
	}
	var w Webhook
	if err := json.Unmarshal(body, &w); err != nil {
		return Webhook{}, err
	}
	return w, nil
}

// ---- server API (Twirp JSON) ----

func (c *Client) twirp(ctx context.Context, service, method string, grant VideoGrant, in any, out any) error {
	tok, err := c.adminToken(grant)
	if err != nil {
		return err
	}
	body, _ := json.Marshal(in)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.APIURL, "/")+"/twirp/livekit."+service+"/"+method, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	hc := c.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("livekit %s/%s: %d %s", service, method, resp.StatusCode, rb)
	}
	if out != nil {
		return json.Unmarshal(rb, out)
	}
	return nil
}

// pick reads a field by proto name or JSON camelCase name.
func pick(m map[string]any, names ...string) string {
	for _, n := range names {
		if v, ok := m[n].(string); ok && v != "" {
			return v
		}
	}
	return ""
}

// DeleteRoom disconnects everyone and closes the room.
func (c *Client) DeleteRoom(ctx context.Context, room string) error {
	return c.twirp(ctx, "RoomService", "DeleteRoom", VideoGrant{RoomCreate: true}, map[string]string{"room": room}, nil)
}

// RemoveParticipant kicks one participant (moderation).
func (c *Client) RemoveParticipant(ctx context.Context, room, identity string) error {
	return c.twirp(ctx, "RoomService", "RemoveParticipant", VideoGrant{RoomAdmin: true, Room: room},
		map[string]string{"room": room, "identity": identity}, nil)
}

// Ingress is an RTMP endpoint for OBS.
type Ingress struct {
	ID        string `json:"ingress_id"`
	URL       string `json:"url"`
	StreamKey string `json:"stream_key"`
}

func (c *Client) CreateIngress(ctx context.Context, name, room, identity, displayName string) (Ingress, error) {
	var out map[string]any
	err := c.twirp(ctx, "Ingress", "CreateIngress", VideoGrant{IngressAdmin: true}, map[string]any{
		"input_type": "RTMP_INPUT", "name": name, "room_name": room,
		"participant_identity": identity, "participant_name": displayName,
	}, &out)
	if err != nil {
		return Ingress{}, err
	}
	return Ingress{ID: pick(out, "ingress_id", "ingressId"), URL: pick(out, "url"), StreamKey: pick(out, "stream_key", "streamKey")}, nil
}

// PointIngress moves an existing ingress (stable stream key) to a new room.
func (c *Client) PointIngress(ctx context.Context, ingressID, room string) error {
	return c.twirp(ctx, "Ingress", "UpdateIngress", VideoGrant{IngressAdmin: true},
		map[string]any{"ingress_id": ingressID, "room_name": room}, nil)
}

func (c *Client) DeleteIngress(ctx context.Context, ingressID string) error {
	return c.twirp(ctx, "Ingress", "DeleteIngress", VideoGrant{IngressAdmin: true}, map[string]any{"ingress_id": ingressID}, nil)
}

// StartHLS starts a composite egress writing an HLS playlist to object storage.
func (c *Client) StartHLS(ctx context.Context, room, prefix string) (string, error) {
	var out map[string]any
	err := c.twirp(ctx, "Egress", "StartRoomCompositeEgress", VideoGrant{RoomRecord: true}, map[string]any{
		"room_name": room, "layout": "speaker",
		"segment_outputs": []map[string]any{{
			"filename_prefix": prefix + "/seg", "playlist_name": prefix + "/index.m3u8",
			"live_playlist_name": prefix + "/live.m3u8", "segment_duration": 2, "s3": c.S3,
		}},
	}, &out)
	return pick(out, "egress_id", "egressId"), err
}

// StartReplay records the room to an MP4 file.
func (c *Client) StartReplay(ctx context.Context, room, path string) (string, error) {
	var out map[string]any
	err := c.twirp(ctx, "Egress", "StartRoomCompositeEgress", VideoGrant{RoomRecord: true}, map[string]any{
		"room_name": room, "layout": "speaker",
		"file_outputs": []map[string]any{{"file_type": "MP4", "filepath": path, "s3": c.S3}},
	}, &out)
	return pick(out, "egress_id", "egressId"), err
}

func (c *Client) StopEgress(ctx context.Context, egressID string) error {
	if egressID == "" {
		return nil
	}
	return c.twirp(ctx, "Egress", "StopEgress", VideoGrant{RoomRecord: true}, map[string]any{"egress_id": egressID}, nil)
}
