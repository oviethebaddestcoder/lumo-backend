package journey

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type rtMsg struct {
	Type   string          `json:"type"`
	RoomID string          `json:"room_id"`
	Data   json.RawMessage `json:"data"`
}

// wsConn is a signed-in WebSocket through the gateway, like the apps open.
type wsConn struct {
	c    *websocket.Conn
	msgs chan rtMsg
}

func dialWS(t *testing.T, u *user, path string) *wsConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	url := strings.Replace(gw, "http", "ws", 1) + path
	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer " + u.token}, "Origin": {"http://localhost:3000"}}})
	if err != nil {
		t.Fatalf("websocket %s: %v", path, err)
	}
	w := &wsConn{c: c, msgs: make(chan rtMsg, 64)}
	go func() {
		for {
			_, b, err := c.Read(context.Background())
			if err != nil {
				close(w.msgs)
				return
			}
			var m rtMsg
			if json.Unmarshal(b, &m) == nil {
				w.msgs <- m
			}
		}
	}()
	t.Cleanup(func() { c.Close(websocket.StatusNormalClosure, "") })
	w.expect(t, "system", 5*time.Second, nil) // "connected"
	return w
}

// expect waits for a message of the given type that also satisfies ok.
func (w *wsConn) expect(t *testing.T, typ string, timeout time.Duration, ok func(rtMsg) bool) rtMsg {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case m, open := <-w.msgs:
			if !open {
				t.Fatalf("websocket closed while waiting for %q", typ)
			}
			if m.Type == typ && (ok == nil || ok(m)) {
				return m
			}
		case <-deadline:
			t.Fatalf("no %q message within %v", typ, timeout)
		}
	}
}

func (w *wsConn) send(t *testing.T, v any) {
	t.Helper()
	b, _ := json.Marshal(v)
	if err := w.c.Write(context.Background(), websocket.MessageText, b); err != nil {
		t.Fatal(err)
	}
}
