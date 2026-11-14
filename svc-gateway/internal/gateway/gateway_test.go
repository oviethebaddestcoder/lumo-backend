package gateway

import (
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/redisx"
)

func TestGateway(t *testing.T) {
	rurl := os.Getenv("LUMO_TEST_REDIS")
	if rurl == "" {
		t.Skip("set LUMO_TEST_REDIS")
	}
	ctx := t.Context()
	rdb, err := redisx.Open(ctx, rurl, "", "")
	if err != nil {
		t.Fatal(err)
	}
	rdb.FlushDB(ctx)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Seen-IP", r.Header.Get("X-Forwarded-For"))
		w.Header().Set("X-Seen-Auth", r.Header.Get("Authorization"))
		_, _ = io.WriteString(w, "wallet:"+r.URL.Path)
	}))
	defer upstream.Close()

	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner(time.Minute, auth.SigningKey{KID: "k", Private: priv})
	routes, err := ParseRoutes("/v1/wallet=" + upstream.URL + ",/v1=" + upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	g, err := New(routes, &auth.Verifier{Keys: auth.StaticKeys{"k": priv.Public().(ed25519.PublicKey)}}, rdb,
		Limits{PerUser: 3, PerIP: 2, AuthPerIP: 2}, []string{"https://lumo.app"}, []string{"127.0.0.0/8"})
	if err != nil {
		t.Fatal(err)
	}
	gw := httptest.NewServer(g)
	defer gw.Close()

	do := func(path, token, xff string) *http.Response {
		req, _ := http.NewRequest(http.MethodGet, gw.URL+path, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if xff != "" {
			req.Header.Set("X-Forwarded-For", xff)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}

	// Client IP comes from the last untrusted hop, not a spoofed first value.
	if resp := do("/v1/wallet/x", "", "6.6.6.6, 41.58.1.2"); resp.StatusCode != 200 || resp.Header.Get("X-Seen-IP") != "41.58.1.2" {
		t.Fatalf("ip: %d %q", resp.StatusCode, resp.Header.Get("X-Seen-IP"))
	}
	// Anonymous per-IP limit (2/min): third request from the same IP is rejected.
	do("/v1/wallet/x", "", "41.58.1.2")
	if resp := do("/v1/wallet/x", "", "41.58.1.2"); resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("want 429, got %d", resp.StatusCode)
	}
	// Bad token rejected before reaching the service.
	if resp := do("/v1/wallet/x", "garbage", "41.58.1.3"); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", resp.StatusCode)
	}
	tok, _, _ := signer.Issue("user-1", "sess-1", true, nil)
	if resp := do("/v1/wallet/x", tok, "41.58.1.4"); resp.StatusCode != 200 {
		t.Fatalf("valid token: %d", resp.StatusCode)
	}
	// Revoked session is refused immediately.
	rdb.Set(ctx, auth.RevokedSessionKey("sess-1"), 1, time.Minute)
	if resp := do("/v1/wallet/x", tok, "41.58.1.4"); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("revoked: %d", resp.StatusCode)
	}
	// Unknown prefix is 404.
	if resp := do("/nope", "", "41.58.1.9"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("no route: %d", resp.StatusCode)
	}
}

func TestCanonicalPath(t *testing.T) {
	ok := []string{"/v1/feed/foryou", "/v1/media/hashtags/jollof/videos", "/v1/users/", "/v1/media/hashtags/na%C3%AFja/videos"}
	bad := []string{
		"/v1/media/../../internal/x", "/v1/media/%2e%2e/%2e%2e/internal/x", "/v1/x%2f..%2f..%2finternal",
		"/v1//feed", "/v1/./feed", "/v1/feed%5c..%5cinternal", "/v1/a%00b",
	}
	for _, p := range ok {
		r := httptest.NewRequest(http.MethodGet, "http://gw"+p, nil)
		if !canonicalPath(r) {
			t.Errorf("rejected %s", p)
		}
	}
	for _, p := range bad {
		r := httptest.NewRequest(http.MethodGet, "http://gw/", nil)
		u, err := url.Parse("http://gw" + p) // keeps dot segments, like a raw request line
		if err != nil {
			t.Fatal(err)
		}
		r.URL = u
		if canonicalPath(r) {
			t.Errorf("accepted %s (path %q)", p, u.Path)
		}
	}
}
