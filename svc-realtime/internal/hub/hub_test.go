package hub_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/realtime"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/redisx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/userdir"
	"github.com/oviethebaddestcoder/lumo-backend/svc-realtime/internal/hub"
)

// Two pods (two hubs) share Redis: chat sent on pod 1 reaches a viewer on
// pod 2, a service push reaches both, and a targeted kick hits one user only.
func TestFanOutAcrossPods(t *testing.T) {
	rurl := os.Getenv("LUMO_TEST_REDIS")
	if rurl == "" {
		t.Skip("set LUMO_TEST_REDIS")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rdb, err := redisx.Open(ctx, rurl, "", "")
	if err != nil {
		t.Fatal(err)
	}
	identity := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var items []events.UserPublic
		for _, id := range strings.Split(r.URL.Query().Get("ids"), ",") {
			items = append(items, events.UserPublic{ID: id, Handle: "h_" + id, DisplayName: "Name " + id})
		}
		json.NewEncoder(w).Encode(map[string]any{"items": items})
	}))
	defer identity.Close()

	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner(time.Hour, auth.SigningKey{KID: "k", Private: priv})
	v := &auth.Verifier{Keys: auth.StaticKeys{"k": priv.Public().(ed25519.PublicKey)}}

	pod := func() *httptest.Server {
		h := hub.New(ctx, rdb, userdir.New(identity.URL), nil, hub.Options{MaxConns: 100, BlockedWords: []string{"badword"}})
		go h.Run(ctx)
		mux := http.NewServeMux()
		mux.Handle("GET /v1/rt/rooms/{id}", v.Middleware(true, true)(h.ServeRoom([]string{"*"})))
		mux.Handle("GET /v1/rt/me", v.Middleware(true, true)(h.ServeMe([]string{"*"})))
		return httptest.NewServer(mux)
	}
	p1, p2 := pod(), pod()
	defer p1.Close()
	defer p2.Close()

	room := events.NewID()
	dial := func(srv *httptest.Server, user string) *websocket.Conn {
		tok, _, _ := signer.Issue(user, "s-"+user, true, nil)
		u := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/rt/rooms/" + room + "?access_token=" + tok
		c, _, err := websocket.Dial(ctx, u, nil)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	read := func(c *websocket.Conn, typ string) realtime.Message {
		for {
			_, b, err := c.Read(ctx)
			if err != nil {
				t.Fatalf("read %s: %v", typ, err)
			}
			var m realtime.Message
			json.Unmarshal(b, &m)
			if m.Type == typ {
				return m
			}
		}
	}
	alice, bob := dial(p1, "alice"), dial(p2, "bob")
	read(alice, realtime.TypeSystem)
	read(bob, realtime.TypeSystem)
	time.Sleep(200 * time.Millisecond) // subscriptions settle

	alice.Write(ctx, websocket.MessageText, []byte(`{"type":"chat","text":"hello badword"}`))
	got := read(bob, realtime.TypeChat)
	data, _ := json.Marshal(got.Data)
	if !strings.Contains(string(data), `"text":"hello *******"`) || !strings.Contains(string(data), `"display_name":"Name alice"`) {
		t.Fatalf("chat on pod 2: %s", data)
	}

	// A service (e.g. svc-live) pushes the viewer count.
	b, _ := json.Marshal(realtime.Message{Type: realtime.TypeViewers, RoomID: room, Data: realtime.Viewers{Count: 42}})
	rdb.Publish(ctx, realtime.RoomChannel(room), b)
	read(alice, realtime.TypeViewers)
	read(bob, realtime.TypeViewers)

	// Moderator removes bob: only bob gets the kick and is disconnected.
	k, _ := json.Marshal(realtime.Message{Type: realtime.TypeKick, RoomID: room, Target: "bob"})
	rdb.Publish(ctx, realtime.RoomChannel(room), k)
	read(bob, realtime.TypeKick)
	for {
		if _, _, err := bob.Read(ctx); err != nil {
			if websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
				t.Fatalf("bob closed with %v", err)
			}
			break
		}
	}
	// Alice is still connected and receives later messages.
	rdb.Publish(ctx, realtime.RoomChannel(room), b)
	read(alice, realtime.TypeViewers)

	// Personal channel: a DM published for carol reaches both of her devices
	// (on different pods) and nobody else; chat frames on it are ignored.
	me := func(srv *httptest.Server, user string) *websocket.Conn {
		tok, _, _ := signer.Issue(user, "s2-"+user, true, nil)
		c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/v1/rt/me?access_token="+tok, nil)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	phone, laptop, dave := me(p1, "carol"), me(p2, "carol"), me(p1, "dave")
	read(phone, realtime.TypeSystem)
	read(laptop, realtime.TypeSystem)
	read(dave, realtime.TypeSystem)
	time.Sleep(200 * time.Millisecond)
	phone.Write(ctx, websocket.MessageText, []byte(`{"type":"chat","text":"should be ignored"}`))
	dm, _ := json.Marshal(realtime.Message{Type: realtime.TypeDM, Data: realtime.DM{ID: "m1", ConversationID: "c1", SenderID: "dave", Text: "hi carol"}})
	rdb.Publish(ctx, realtime.UserChannel("carol"), dm)
	for _, c := range []*websocket.Conn{phone, laptop} {
		got := read(c, realtime.TypeDM)
		raw, _ := json.Marshal(got.Data)
		if !strings.Contains(string(raw), `"text":"hi carol"`) {
			t.Fatalf("dm payload %s", raw)
		}
	}
	rdb.Publish(ctx, realtime.UserChannel("dave"), b) // dave gets his own only
	if m := read(dave, realtime.TypeViewers); m.Type != realtime.TypeViewers {
		t.Fatal("dave missed his message")
	}
}
