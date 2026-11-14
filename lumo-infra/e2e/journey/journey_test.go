// Package journey drives real user journeys through the gateway against
// the whole running stack (see ../run.sh). Every step talks HTTP to the
// gateway exactly like the apps do, and waits for the asynchronous effects
// (Kafka consumers in other services) to land.
package journey

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var (
	gw       = env("LUMO_E2E_GATEWAY", "http://localhost:18080")
	paystack = env("LUMO_E2E_PAYSTACK", "http://localhost:19100")
	pgURL    = env("LUMO_E2E_PG", "postgres://postgres:postgres@localhost:5432")
	psSecret = env("LUMO_E2E_PAYSTACK_SECRET", "sk_test_e2e")
	hc       = &http.Client{Timeout: 15 * time.Second}
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func newID() string {
	var b [16]byte
	rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// user is a signed-in test account.
type user struct {
	name, email, code string
	id, token         string
	refresh           string
}

type resp struct {
	status int
	body   []byte
	header http.Header
}

func (r resp) json(t *testing.T, v any) {
	t.Helper()
	if err := json.Unmarshal(r.body, v); err != nil {
		t.Fatalf("decode %s: %v", r.body, err)
	}
}

// call sends one request through the gateway.
func call(t *testing.T, u *user, method, path string, body any, headers ...string) resp {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, gw+path, rd)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if u != nil && u.token != "" {
		req.Header.Set("Authorization", "Bearer "+u.token)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	res, err := hc.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return resp{res.StatusCode, b, res.Header}
}

// must is call that expects a status. Any 2xx satisfies an expected 2xx
// (which success code a route returns is not what these journeys test).
func must(t *testing.T, want int, u *user, method, path string, body any, headers ...string) resp {
	t.Helper()
	r := call(t, u, method, path, body, headers...)
	if r.status != want && !(want/100 == 2 && r.status/100 == 2) {
		t.Fatalf("%s %s: status %d, want %d: %s", method, path, r.status, want, r.body)
	}
	return r
}

// eventually retries f until it returns "" (success) or the deadline passes.
func eventually(t *testing.T, what string, timeout time.Duration, f func() string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := ""
	for time.Now().Before(deadline) {
		if last = f(); last == "" {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s: %s", timeout, what, last)
}

func signIn(t *testing.T, u *user) {
	t.Helper()
	must(t, 200, nil, "POST", "/v1/auth/otp/send", map[string]string{"channel": "email", "target": u.email})
	r := must(t, 200, nil, "POST", "/v1/auth/otp/verify", map[string]any{
		"channel": "email", "target": u.email, "code": u.code,
		"device":     map[string]string{"device_id": "e2e-" + u.name, "device_name": "e2e " + u.name, "platform": "android"},
		"birth_date": "1995-05-05", "display_name": strings.ToUpper(u.name[:1]) + u.name[1:],
	})
	var out struct {
		Tokens struct {
			AccessToken  string `json:"access_token"`
			RefreshToken string `json:"refresh_token"`
		} `json:"tokens"`
		User struct {
			ID string `json:"id"`
		} `json:"user"`
		NeedsBirthDate bool `json:"needs_birth_date"`
	}
	r.json(t, &out)
	if out.Tokens.AccessToken == "" || out.User.ID == "" || out.NeedsBirthDate {
		t.Fatalf("sign-in %s: %s", u.name, r.body)
	}
	u.id, u.token, u.refresh = out.User.ID, out.Tokens.AccessToken, out.Tokens.RefreshToken
}

func refresh(t *testing.T, u *user) {
	t.Helper()
	var out struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		Tokens       *struct {
			AccessToken  string `json:"access_token"`
			RefreshToken string `json:"refresh_token"`
		} `json:"tokens"`
	}
	must(t, 200, nil, "POST", "/v1/auth/refresh", map[string]any{
		"refresh_token": u.refresh,
		"device":        map[string]string{"device_id": "e2e-" + u.name, "device_name": "e2e " + u.name, "platform": "android"},
	}).json(t, &out)
	if out.Tokens != nil {
		out.AccessToken, out.RefreshToken = out.Tokens.AccessToken, out.Tokens.RefreshToken
	}
	if out.AccessToken == "" {
		t.Fatal("refresh returned no token")
	}
	u.token, u.refresh = out.AccessToken, out.RefreshToken
}

func psql(t *testing.T, db, sql string) string {
	t.Helper()
	out, err := exec.Command("psql", pgURL+"/"+db+"?sslmode=disable", "-tAc", sql).CombinedOutput()
	if err != nil {
		t.Fatalf("psql %s: %v: %s", db, err, out)
	}
	return strings.TrimSpace(string(out))
}

func paystackSigned(t *testing.T, payload map[string]any) resp {
	t.Helper()
	b, _ := json.Marshal(payload)
	m := hmac.New(sha512.New, []byte(psSecret))
	m.Write(b)
	req, _ := http.NewRequest("POST", gw+"/v1/wallet/webhooks/paystack", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Paystack-Signature", hex.EncodeToString(m.Sum(nil)))
	res, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	return resp{status: res.StatusCode, body: body}
}

type feedItem struct {
	ID     string `json:"id"`
	Source string `json:"rec_source"`
	Arm    string `json:"rec_arm"`
	Author struct {
		ID string `json:"id"`
	} `json:"author"`
	HLS string `json:"hls_url"`
}

func forYou(t *testing.T, u *user) []feedItem {
	t.Helper()
	var out struct {
		Items []feedItem `json:"items"`
	}
	must(t, 200, u, "GET", "/v1/feed/foryou?limit=20", nil).json(t, &out)
	return out.Items
}

func TestJourney(t *testing.T) {
	creator := &user{name: "creator", email: "creator@e2e.test", code: "111111"}
	fan := &user{name: "fan", email: "fan@e2e.test", code: "222222"}
	finance := &user{name: "finance", email: "finance@e2e.test", code: "333333"}
	mod := &user{name: "mod", email: "mod@e2e.test", code: "444444"}
	stranger := &user{name: "stranger", email: "stranger@e2e.test", code: "555555"}
	var videoID string

	step := func(name string, f func(t *testing.T)) {
		if !t.Run(name, f) {
			t.FailNow() // later steps depend on earlier ones
		}
	}

	step("sign up five accounts and set roles", func(t *testing.T) {
		for _, u := range []*user{creator, fan, finance, mod, stranger} {
			signIn(t, u)
		}
		must(t, 200, creator, "PATCH", "/v1/me", map[string]string{"handle": "chef_ada", "bio": "Jollof every day"})
		var me struct {
			Handle string `json:"handle"`
		}
		must(t, 200, creator, "GET", "/v1/me", nil).json(t, &me)
		if me.Handle != "chef_ada" {
			t.Fatalf("handle %q", me.Handle)
		}
		// Roles are granted by an admin in the database; a fresh sign-in
		// picks them up.
		psql(t, "e2e_identity", fmt.Sprintf(`UPDATE users SET roles = '{finance}' WHERE id = '%s'`, finance.id))
		psql(t, "e2e_identity", fmt.Sprintf(`UPDATE users SET roles = '{moderator}' WHERE id = '%s'`, mod.id))
		// Refresh reloads the user, so the new roles land without signing in again.
		refresh(t, finance)
		refresh(t, mod)
		// Profiles reach other services through identity.users (userdir).
		eventually(t, "public profile by handle", 10*time.Second, func() string {
			r := call(t, nil, "GET", "/v1/users/by-handle/chef_ada", nil)
			if r.status != 200 {
				return fmt.Sprint(r.status)
			}
			return ""
		})
	})

	step("gateway refuses bad requests", func(t *testing.T) {
		must(t, 401, nil, "GET", "/v1/me", nil)
		must(t, 401, &user{token: "not-a-jwt"}, "GET", "/v1/me", nil)
		must(t, 404, nil, "GET", "/internal/social/users/x/following-ids", nil)
		must(t, 400, nil, "GET", "/v1/media/x%2f..%2f..%2finternal", nil)
		must(t, 403, fan, "GET", "/v1/wallet/admin/payouts", nil)
		must(t, 403, fan, "GET", "/v1/moderation/queue", nil)
	})

	step("upload, transcode, publish", func(t *testing.T) {
		dir := t.TempDir()
		src := filepath.Join(dir, "clip.mp4")
		out, err := exec.Command("ffmpeg", "-loglevel", "error", "-f", "lavfi", "-i", "testsrc=size=720x1280:rate=30:duration=4",
			"-f", "lavfi", "-i", "sine=frequency=440:duration=4", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", "-shortest", src).CombinedOutput()
		if err != nil {
			t.Fatalf("ffmpeg: %v %s", err, out)
		}
		data, _ := os.ReadFile(src)
		var up struct {
			VideoID  string   `json:"video_id"`
			PartSize int64    `json:"part_size"`
			PartURLs []string `json:"part_urls"`
		}
		must(t, 201, creator, "POST", "/v1/media/uploads", map[string]any{
			"kind": "short", "size_bytes": len(data), "content_type": "video/mp4",
			"title": "Party jollof", "description": "Smoky party jollof #cooking #lagos", "visibility": "public",
		}).json(t, &up)
		if up.VideoID == "" || len(up.PartURLs) == 0 {
			t.Fatalf("upload start: %+v", up)
		}
		videoID = up.VideoID
		var parts []map[string]any
		for i, u := range up.PartURLs {
			from := int64(i) * up.PartSize
			to := min(from+up.PartSize, int64(len(data)))
			req, _ := http.NewRequest("PUT", u, bytes.NewReader(data[from:to]))
			res, err := hc.Do(req)
			if err != nil || res.StatusCode != 200 {
				t.Fatalf("part %d: %v %v", i+1, err, res)
			}
			res.Body.Close()
			parts = append(parts, map[string]any{"number": i + 1, "etag": res.Header.Get("ETag")})
		}
		must(t, 200, creator, "POST", "/v1/media/uploads/"+videoID+"/complete", map[string]any{"parts": parts})
		var v struct {
			Status     string   `json:"status"`
			HLS        string   `json:"hls_url"`
			DurationMS int64    `json:"duration_ms"`
			Hashtags   []string `json:"hashtags"`
		}
		eventually(t, "transcoded video", 120*time.Second, func() string {
			must(t, 200, creator, "GET", "/v1/media/videos/"+videoID, nil).json(t, &v)
			if v.Status != "ready" {
				return v.Status
			}
			return ""
		})
		if v.HLS == "" || v.DurationMS < 3000 || len(v.Hashtags) != 2 {
			t.Fatalf("ready video %+v", v)
		}
		res, err := hc.Get(v.HLS)
		if err != nil || res.StatusCode != 200 {
			t.Fatalf("HLS playlist %s: %v %v", v.HLS, err, res)
		}
		pl, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if !strings.HasPrefix(string(pl), "#EXTM3U") {
			t.Fatalf("not an HLS playlist: %.80s", pl)
		}
	})

	step("video reaches For You", func(t *testing.T) {
		eventually(t, "video in fan's For You", 20*time.Second, func() string {
			must(t, 204, fan, "POST", "/v1/feed/me/reset", nil) // nothing marked seen while waiting
			for _, it := range forYou(t, fan) {
				if it.ID == videoID {
					if it.Source == "" || it.Arm == "" || it.Author.ID != creator.id {
						return fmt.Sprintf("bad item %+v", it)
					}
					return ""
				}
			}
			return "not yet"
		})
		// Signed-out viewers see it too.
		eventually(t, "video in signed-out feed", 40*time.Second, func() string {
			var out struct {
				Items []feedItem `json:"items"`
			}
			must(t, 200, nil, "GET", "/v1/feed/foryou?limit=20", nil).json(t, &out)
			for _, it := range out.Items {
				if it.ID == videoID {
					return ""
				}
			}
			return fmt.Sprintf("%d items", len(out.Items))
		})
	})

	step("follow, like, comment, watch", func(t *testing.T) {
		must(t, 204, fan, "POST", "/v1/social/follows/"+creator.id, nil)
		must(t, 204, fan, "POST", "/v1/social/videos/"+videoID+"/like", nil)
		must(t, 201, fan, "POST", "/v1/social/videos/"+videoID+"/comments", map[string]string{"text": "This looks amazing 🔥"})
		must(t, 202, fan, "POST", "/v1/feed/impressions", map[string]any{"items": []map[string]any{
			{"video_id": videoID, "watch_ms": 9000, "source": "tag", "arm": "a", "surface": "foryou"},
		}})

		eventually(t, "creator counts", 15*time.Second, func() string {
			var c struct {
				Followers     int `json:"followers"`
				LikesReceived int `json:"likes_received"`
			}
			must(t, 200, nil, "GET", "/v1/social/users/"+creator.id+"/counts", nil).json(t, &c)
			if c.Followers != 1 || c.LikesReceived != 1 {
				return fmt.Sprintf("%+v", c)
			}
			return ""
		})
		eventually(t, "views counted", 20*time.Second, func() string {
			var v struct {
				Views int `json:"views"`
			}
			must(t, 200, nil, "GET", "/v1/media/videos/"+videoID, nil).json(t, &v)
			if v.Views < 1 {
				return "0 views"
			}
			return ""
		})
		eventually(t, "creator's activity inbox", 20*time.Second, func() string {
			var in struct {
				Items []struct {
					Kind string `json:"kind"`
				} `json:"items"`
			}
			must(t, 200, creator, "GET", "/v1/notify/inbox", nil).json(t, &in)
			kinds := map[string]bool{}
			for _, it := range in.Items {
				kinds[it.Kind] = true
			}
			if !kinds["follow"] || !kinds["comment"] {
				return fmt.Sprintf("%v", kinds)
			}
			return ""
		})
		eventually(t, "following feed", 15*time.Second, func() string {
			var out struct {
				Items []feedItem `json:"items"`
			}
			must(t, 200, fan, "GET", "/v1/feed/following", nil).json(t, &out)
			if len(out.Items) == 0 || out.Items[0].ID != videoID {
				return fmt.Sprintf("%d items", len(out.Items))
			}
			return ""
		})
		eventually(t, "fan's interests learned", 20*time.Second, func() string {
			var out struct {
				Items []struct {
					Tag string `json:"tag"`
				} `json:"items"`
			}
			must(t, 200, fan, "GET", "/v1/feed/me/interests", nil).json(t, &out)
			for _, i := range out.Items {
				if i.Tag == "cooking" {
					return ""
				}
			}
			return fmt.Sprintf("%+v", out.Items)
		})
	})

	step("buy coins on the web and send gifts", func(t *testing.T) {
		var cat struct {
			Gifts []struct {
				ID    string `json:"id"`
				Coins int64  `json:"coins"`
			} `json:"gifts"`
			Packs []struct {
				ID    string `json:"id"`
				Coins int64  `json:"coins"`
				Web   int64  `json:"web_amount_kobo"`
			} `json:"packs"`
		}
		must(t, 200, nil, "GET", "/v1/wallet/catalog", nil).json(t, &cat)
		pack := cat.Packs[0]
		for _, p := range cat.Packs { // the biggest pack sold on the web
			if p.Web > 0 && p.Coins > pack.Coins {
				pack = p
			}
		}
		if pack.Web == 0 {
			t.Fatal("no pack is sold on the web")
		}
		var co struct {
			URL string `json:"authorization_url"`
			Ref string `json:"reference"`
		}
		must(t, 200, fan, "POST", "/v1/wallet/web/checkout", map[string]string{"pack_id": pack.ID, "email": fan.email, "callback_url": "http://localhost:3000/wallet"}).json(t, &co)
		if co.Ref == "" || co.URL == "" {
			t.Fatalf("checkout %+v", co)
		}
		// A forged webhook is refused.
		b, _ := json.Marshal(map[string]any{"event": "charge.success", "data": map[string]string{"reference": co.Ref}})
		req, _ := http.NewRequest("POST", gw+"/v1/wallet/webhooks/paystack", bytes.NewReader(b))
		req.Header.Set("X-Paystack-Signature", "00")
		if res, _ := hc.Do(req); res == nil || res.StatusCode != 401 {
			t.Fatalf("forged webhook accepted: %v", res)
		}
		// Before payment, the webhook can't credit anything.
		if r := paystackSigned(t, map[string]any{"event": "charge.success", "data": map[string]string{"reference": co.Ref}}); r.status != 200 {
			t.Fatalf("webhook before payment: %d %s", r.status, r.body)
		}
		var bal struct {
			Coins    int64 `json:"coins"`
			Diamonds int64 `json:"diamonds"`
		}
		must(t, 200, fan, "GET", "/v1/wallet", nil).json(t, &bal)
		if bal.Coins != 0 {
			t.Fatalf("credited before payment: %+v", bal)
		}
		if res, err := hc.Post(paystack+"/__pay/"+co.Ref, "application/json", nil); err != nil || res.StatusCode != 200 {
			t.Fatalf("pay: %v %v", err, res)
		}
		for i := 0; i < 3; i++ { // Paystack retries; credit once
			if r := paystackSigned(t, map[string]any{"event": "charge.success", "data": map[string]string{"reference": co.Ref}}); r.status != 200 {
				t.Fatalf("webhook: %d %s", r.status, r.body)
			}
		}
		must(t, 200, fan, "GET", "/v1/wallet", nil).json(t, &bal)
		if bal.Coins != pack.Coins {
			t.Fatalf("coins %d, want %d", bal.Coins, pack.Coins)
		}

		gift := cat.Gifts[0]
		qty := int64(200*2) / gift.Coins // enough for ≥200 diamonds at 50%
		if qty < 1 {
			qty = 1
		}
		if qty*gift.Coins > pack.Coins {
			t.Fatalf("pack too small for the test: %d coins, need %d", pack.Coins, qty*gift.Coins)
		}
		key := newID()
		for i := 0; i < 2; i++ { // same Idempotency-Key twice: one charge
			must(t, 200, fan, "POST", "/v1/wallet/gifts/send", map[string]any{"receiver_id": creator.id, "gift_id": gift.ID, "qty": qty, "video_id": videoID},
				"Idempotency-Key", key)
		}
		must(t, 200, fan, "GET", "/v1/wallet", nil).json(t, &bal)
		if bal.Coins != pack.Coins-qty*gift.Coins {
			t.Fatalf("coins after gift %d, want %d", bal.Coins, pack.Coins-qty*gift.Coins)
		}
		want := qty * gift.Coins / 2
		eventually(t, "creator credited diamonds", 20*time.Second, func() string {
			var cb struct {
				Diamonds int64 `json:"diamonds"`
			}
			must(t, 200, creator, "GET", "/v1/wallet", nil).json(t, &cb)
			if cb.Diamonds != want {
				return fmt.Sprintf("%d diamonds, want %d", cb.Diamonds, want)
			}
			return ""
		})
		var st struct {
			Level int   `json:"level"`
			Spent int64 `json:"coins_spent"`
		}
		must(t, 200, fan, "GET", "/v1/wallet/stats", nil).json(t, &st)
		if st.Spent != qty*gift.Coins || st.Level < 1 {
			t.Fatalf("fan stats %+v", st)
		}
	})

	step("cash out to a bank, approved by finance", func(t *testing.T) {
		must(t, 200, creator, "PUT", "/v1/wallet/payout-account", map[string]string{"bank_code": "058", "account_number": "0123456789"})
		var p struct {
			ID     string `json:"payout_id"`
			Status string `json:"status"`
		}
		must(t, 202, creator, "POST", "/v1/wallet/payouts", map[string]int{"diamonds": 100}, "Idempotency-Key", newID()).json(t, &p)
		if p.Status != "pending_review" {
			t.Fatalf("payout %+v", p)
		}
		var q struct {
			Items []struct {
				ID         string `json:"id"`
				AmountKobo int64  `json:"amount_kobo"`
				BankLabel  string `json:"bank_label"`
			} `json:"items"`
		}
		must(t, 200, finance, "GET", "/v1/wallet/admin/payouts?status=pending_review", nil).json(t, &q)
		if len(q.Items) != 1 || q.Items[0].ID != p.ID || q.Items[0].AmountKobo != 5000 || q.Items[0].BankLabel != "GTBank ••6789" {
			t.Fatalf("finance queue %s", must(t, 200, finance, "GET", "/v1/wallet/admin/payouts?status=pending_review", nil).body)
		}
		must(t, 200, finance, "POST", "/v1/wallet/admin/payouts/"+p.ID+"/approve", nil)
		res, _ := hc.Get(paystack + "/__transfers")
		var tr struct {
			Data map[string]int64 `json:"data"`
		}
		json.NewDecoder(res.Body).Decode(&tr)
		res.Body.Close()
		if tr.Data[p.ID] != 5000 {
			t.Fatalf("transfers at Paystack: %+v", tr.Data)
		}
		if r := paystackSigned(t, map[string]any{"event": "transfer.success", "data": map[string]string{"reference": p.ID}}); r.status != 200 {
			t.Fatalf("transfer webhook: %d %s", r.status, r.body)
		}
		var mine struct {
			Items []struct {
				ID, Status string
			} `json:"items"`
		}
		must(t, 200, creator, "GET", "/v1/wallet/payouts", nil).json(t, &mine)
		if len(mine.Items) != 1 || mine.Items[0].Status != "paid" {
			t.Fatalf("creator payouts %+v", mine.Items)
		}
		eventually(t, "payout push in the inbox", 20*time.Second, func() string {
			var in struct {
				Items []struct {
					Kind, Title string
				} `json:"items"`
			}
			must(t, 200, creator, "GET", "/v1/notify/inbox", nil).json(t, &in)
			for _, it := range in.Items {
				if it.Kind == "payout" && it.Title == "Cash-out paid" {
					return ""
				}
			}
			return fmt.Sprintf("%+v", in.Items)
		})
		// Changing the bank account pauses cash-outs.
		must(t, 200, creator, "PUT", "/v1/wallet/payout-account", map[string]string{"bank_code": "044", "account_number": "0987654321"})
		if r := call(t, creator, "POST", "/v1/wallet/payouts", map[string]int{"diamonds": 100}, "Idempotency-Key", newID()); r.status != 409 {
			t.Fatalf("cash-out right after bank change: %d %s", r.status, r.body)
		}
	})

	step("direct messages", func(t *testing.T) {
		var conv struct {
			ID string `json:"id"`
		}
		r := call(t, fan, "POST", "/v1/dm/conversations", map[string]string{"user_id": creator.id})
		if r.status != 200 && r.status != 201 {
			t.Fatalf("open conversation: %d %s", r.status, r.body)
		}
		r.json(t, &conv)
		cid := newID()
		var m1, m2 struct {
			ID string `json:"id"`
		}
		r1 := call(t, fan, "POST", "/v1/dm/conversations/"+conv.ID+"/messages", map[string]string{"text": "Can I get the recipe?", "client_id": cid})
		r2 := call(t, fan, "POST", "/v1/dm/conversations/"+conv.ID+"/messages", map[string]string{"text": "Can I get the recipe?", "client_id": cid})
		if r1.status/100 != 2 || r2.status/100 != 2 {
			t.Fatalf("send: %d %s / %d %s", r1.status, r1.body, r2.status, r2.body)
		}
		r1.json(t, &m1)
		r2.json(t, &m2)
		if m1.ID == "" || m1.ID != m2.ID {
			t.Fatalf("retry made a second message: %s vs %s", m1.ID, m2.ID)
		}
		var list struct {
			Items []struct {
				ID          string `json:"id"`
				Unread      int    `json:"unread"`
				LastPreview string `json:"last_preview"`
			} `json:"items"`
		}
		must(t, 200, creator, "GET", "/v1/dm/conversations", nil).json(t, &list)
		if len(list.Items) != 1 || list.Items[0].Unread != 1 || list.Items[0].LastPreview != "Can I get the recipe?" {
			t.Fatalf("creator inbox %+v", list.Items)
		}
		must(t, 204, creator, "POST", "/v1/dm/conversations/"+conv.ID+"/read", nil)
		var un struct {
			Count int `json:"count"`
		}
		must(t, 200, creator, "GET", "/v1/dm/unread", nil).json(t, &un)
		if un.Count != 0 {
			t.Fatalf("unread after read: %d", un.Count)
		}
		// A stranger can't read someone else's conversation.
		if r := call(t, stranger, "GET", "/v1/dm/conversations/"+conv.ID+"/messages", nil); r.status != 404 && r.status != 403 {
			t.Fatalf("stranger read a conversation: %d", r.status)
		}
	})

	step("blocking hides people everywhere", func(t *testing.T) {
		// The stranger can see the video before the block.
		eventually(t, "video in stranger's feed", 20*time.Second, func() string {
			must(t, 204, stranger, "POST", "/v1/feed/me/reset", nil)
			for _, it := range forYou(t, stranger) {
				if it.ID == videoID {
					return ""
				}
			}
			return "not yet"
		})
		must(t, 204, creator, "POST", "/v1/social/blocks/"+stranger.id, nil)
		eventually(t, "blocked creator gone from the stranger's feed", 20*time.Second, func() string {
			must(t, 204, stranger, "POST", "/v1/feed/me/reset", nil)
			for _, it := range forYou(t, stranger) {
				if it.Author.ID == creator.id {
					return "still shown"
				}
			}
			return ""
		})
		r := call(t, stranger, "POST", "/v1/dm/conversations", map[string]string{"user_id": creator.id})
		if r.status/100 == 2 {
			var c struct {
				ID string `json:"id"`
			}
			r.json(t, &c)
			r = call(t, stranger, "POST", "/v1/dm/conversations/"+c.ID+"/messages", map[string]string{"text": "hi", "client_id": newID()})
		}
		eventually(t, "DM refused after block", 15*time.Second, func() string {
			r := call(t, stranger, "POST", "/v1/dm/conversations", map[string]string{"user_id": creator.id})
			if r.status == 403 {
				return ""
			}
			return fmt.Sprintf("%d %s", r.status, r.body)
		})
	})

	step("report, moderate, remove", func(t *testing.T) {
		must(t, 204, fan, "POST", "/v1/social/reports", map[string]string{"target_type": "video", "target_id": videoID, "reason": "spam", "details": "e2e"})
		eventually(t, "report in the moderation queue", 20*time.Second, func() string {
			var q struct {
				Items []struct {
					TargetID string `json:"target_id"`
				} `json:"items"`
			}
			must(t, 200, mod, "GET", "/v1/moderation/queue", nil).json(t, &q)
			for _, it := range q.Items {
				if it.TargetID == videoID {
					return ""
				}
			}
			return fmt.Sprintf("%d items", len(q.Items))
		})
		must(t, 201, mod, "POST", "/v1/moderation/actions", map[string]string{"target_type": "video", "target_id": videoID, "action": "remove", "reason": "e2e removal"})
		eventually(t, "removed video gone", 20*time.Second, func() string {
			r := call(t, fan, "GET", "/v1/media/videos/"+videoID, nil)
			if r.status != 404 {
				return fmt.Sprintf("%d %s", r.status, r.body)
			}
			return ""
		})
		eventually(t, "removed video out of every feed", 20*time.Second, func() string {
			must(t, 204, fan, "POST", "/v1/feed/me/reset", nil)
			for _, it := range forYou(t, fan) {
				if it.ID == videoID {
					return "still in For You"
				}
			}
			var out struct {
				Items []feedItem `json:"items"`
			}
			must(t, 200, fan, "GET", "/v1/feed/following", nil).json(t, &out)
			if len(out.Items) != 0 {
				return "still in Following"
			}
			return ""
		})
	})

	step("go live, viewer joins, gifts in the room", func(t *testing.T) {
		var cats struct {
			Items []string `json:"items"`
		}
		must(t, 200, nil, "GET", "/v1/live/categories", nil).json(t, &cats)
		var cr struct {
			Room struct {
				ID string `json:"id"`
			} `json:"room"`
			Connection struct {
				Mode  string `json:"mode"`
				Token string `json:"token"`
			} `json:"connection"`
		}
		must(t, 201, creator, "POST", "/v1/live/rooms", map[string]any{"title": "Cooking live", "category": cats.Items[0], "source": "app",
			"allow_pk": true, "allow_gifts": true, "save_replay": false, "goal_coins": 100, "goal_label": "New pot"}).json(t, &cr)
		if cr.Room.ID == "" || cr.Connection.Token == "" {
			t.Fatalf("create room %+v", cr)
		}
		eventually(t, "room listed", 10*time.Second, func() string {
			var rooms struct {
				Items []struct {
					ID string `json:"id"`
				} `json:"items"`
			}
			must(t, 200, nil, "GET", "/v1/live/rooms", nil).json(t, &rooms)
			for _, r := range rooms.Items {
				if r.ID == cr.Room.ID {
					return ""
				}
			}
			return fmt.Sprintf("%d rooms", len(rooms.Items))
		})
		var conn struct {
			Mode  string `json:"mode"`
			Token string `json:"token"`
		}
		must(t, 200, fan, "POST", "/v1/live/rooms/"+cr.Room.ID+"/join", nil).json(t, &conn)
		if conn.Token == "" && conn.Mode == "webrtc" {
			t.Fatalf("viewer connection %+v", conn)
		}
		var before struct {
			Diamonds int64 `json:"diamonds"`
		}
		must(t, 200, creator, "GET", "/v1/wallet", nil).json(t, &before)
		must(t, 200, fan, "POST", "/v1/wallet/gifts/send", map[string]any{"receiver_id": creator.id, "gift_id": "rose", "qty": 2, "room_id": cr.Room.ID},
			"Idempotency-Key", newID())
		eventually(t, "room gift total and host diamonds", 20*time.Second, func() string {
			var room struct {
				GiftCoins int64 `json:"gift_coins"`
			}
			must(t, 200, nil, "GET", "/v1/live/rooms/"+cr.Room.ID, nil).json(t, &room)
			var after struct {
				Diamonds int64 `json:"diamonds"`
			}
			must(t, 200, creator, "GET", "/v1/wallet", nil).json(t, &after)
			if room.GiftCoins != 20 || after.Diamonds != before.Diamonds+10 {
				return fmt.Sprintf("room gift_coins=%d, diamonds %d -> %d", room.GiftCoins, before.Diamonds, after.Diamonds)
			}
			return ""
		})
		must(t, 403, fan, "POST", "/v1/live/rooms/"+cr.Room.ID+"/end", nil)
		must(t, 204, creator, "POST", "/v1/live/rooms/"+cr.Room.ID+"/end", nil)
		eventually(t, "ended room off the list", 10*time.Second, func() string {
			var rooms struct {
				Items []struct {
					ID string `json:"id"`
				} `json:"items"`
			}
			must(t, 200, nil, "GET", "/v1/live/rooms", nil).json(t, &rooms)
			for _, r := range rooms.Items {
				if r.ID == cr.Room.ID {
					return "still listed"
				}
			}
			return ""
		})
	})

	step("delete account signs out everywhere", func(t *testing.T) {
		must(t, 204, stranger, "DELETE", "/v1/me", nil)
		eventually(t, "token refused after deletion", 10*time.Second, func() string {
			if r := call(t, stranger, "GET", "/v1/me", nil); r.status != 401 {
				return fmt.Sprint(r.status)
			}
			return ""
		})
		if r := call(t, nil, "GET", "/v1/users/"+stranger.id, nil); r.status != 404 {
			t.Fatalf("deleted user still public: %d %s", r.status, r.body)
		}
	})
}
