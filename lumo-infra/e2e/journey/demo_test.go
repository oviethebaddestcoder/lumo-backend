package journey

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestDemo fills a running stack with realistic demo content (creators,
// transcoded videos, follows, likes, comments, gifts, a pending cash-out,
// DMs, a live room) for screenshots and manual poking. Opt in:
//
//	LUMO_E2E_DEMO=1 go test ./journey -run Demo -v
//
// Sign in afterwards with loadN@e2e.test and code 000000 (N in the table below).
func TestDemo(t *testing.T) {
	if os.Getenv("LUMO_E2E_DEMO") == "" {
		t.Skip("set LUMO_E2E_DEMO=1")
	}
	type creator struct {
		n                 int
		name, handle, bio string
		color1, color2    string
		videos            [][2]string // title, description with hashtags
	}
	creators := []creator{
		{1, "Ada's Kitchen", "ada_cooks", "Naija food, 60 seconds at a time 🍲", "0xE85D3A", "0x7A1F0E", [][2]string{
			{"Smoky party jollof", "The secret is the bottom pot #cooking #jollof #lagos"},
			{"Suya at home", "No grill? No wahala #cooking #suya"},
			{"Puff-puff in 5 minutes", "Sunday treat #cooking #snacks"},
		}},
		{2, "Tunde Laughs", "tunde_laughs", "Skits every Friday 😂", "0x3A7BE8", "0x101B4A", [][2]string{
			{"When NEPA takes light", "Every Nigerian knows #comedy #skits"},
			{"Danfo conductor energy", "Oshodi oya! #comedy #lagos"},
		}},
		{3, "Zee Moves", "zee_moves", "Afrobeats choreography 💃", "0xB83AE8", "0x3B0E4A", [][2]string{
			{"Amapiano footwork tutorial", "Step by step #dance #amapiano"},
			{"Owambe dance challenge", "Your turn #dance #afrobeats #challenge"},
		}},
		{4, "Kemi Style", "kemi_style", "Ankara looks for every budget", "0x2FB58A", "0x0B3B2C", [][2]string{
			{"3 ways to tie a gele", "Wedding season ready #fashion #gele"},
			{"Ankara on a budget", "Yaba market haul #fashion #lagos"},
		}},
	}
	viewer := &user{name: "chioma", email: "load5@e2e.test", code: "000000"}
	finance := &user{name: "finance", email: "load6@e2e.test", code: "000000"}
	fans := []*user{viewer}
	for i := 7; i <= 12; i++ {
		fans = append(fans, &user{name: fmt.Sprint("fan", i), email: fmt.Sprintf("load%d@e2e.test", i), code: "000000"})
	}
	users := map[int]*user{}
	for _, c := range creators {
		u := &user{name: c.handle, email: fmt.Sprintf("load%d@e2e.test", c.n), code: "000000"}
		signIn(t, u)
		must(t, 200, u, "PATCH", "/v1/me", map[string]string{"display_name": c.name, "handle": c.handle, "bio": c.bio})
		users[c.n] = u
	}
	for _, f := range append(fans, finance) {
		signIn(t, f)
	}
	must(t, 200, viewer, "PATCH", "/v1/me", map[string]string{"display_name": "Chioma", "handle": "chioma_n", "bio": "Here for the jollof"})
	psql(t, "e2e_identity", fmt.Sprintf(`UPDATE users SET roles = '{finance}' WHERE id = '%s'`, finance.id))
	refresh(t, finance)

	// Upload and transcode every video.
	dir := t.TempDir()
	font := "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
	var all []string
	owner := map[string]*user{}
	for _, c := range creators {
		for i, v := range c.videos {
			src := filepath.Join(dir, fmt.Sprintf("%s-%d.mp4", c.handle, i))
			title := strings.ReplaceAll(v[0], "'", "")
			filter := fmt.Sprintf("drawtext=fontfile=%s:text='%s':fontcolor=white:fontsize=58:x=(w-text_w)/2:y=h*0.42:shadowcolor=black@0.5:shadowx=3:shadowy=3,"+
				"drawtext=fontfile=%s:text='@%s':fontcolor=white@0.85:fontsize=36:x=(w-text_w)/2:y=h*0.42+90", font, title, font, c.handle)
			out, err := exec.Command("ffmpeg", "-loglevel", "error", "-f", "lavfi", "-i",
				fmt.Sprintf("gradients=s=720x1280:c0=%s:c1=%s:speed=0.02:duration=6:rate=30", c.color1, c.color2),
				"-f", "lavfi", "-i", "sine=frequency=330:duration=6", "-vf", filter,
				"-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", "-shortest", src).CombinedOutput()
			if err != nil {
				t.Fatalf("ffmpeg: %v %s", err, out)
			}
			id := upload(t, users[c.n], src, v[0], v[1])
			all = append(all, id)
			owner[id] = users[c.n]
		}
	}
	for _, id := range all {
		eventually(t, "transcode "+id, 120*time.Second, func() string {
			var v struct {
				Status string `json:"status"`
			}
			must(t, 200, owner[id], "GET", "/v1/media/videos/"+id, nil).json(t, &v)
			if v.Status != "ready" {
				return v.Status
			}
			return ""
		})
	}

	// Videos reach other services through media.videos events; wait until
	// social knows them (the feed only shows a video after the same event).
	for _, id := range all {
		eventually(t, "social knows "+id, 30*time.Second, func() string {
			if r := call(t, fans[len(fans)-1], "POST", "/v1/social/videos/"+id+"/like", nil); r.status/100 != 2 {
				return fmt.Sprint(r.status)
			}
			return ""
		})
	}

	// Social graph and engagement.
	comments := []string{"This is fire 🔥", "Abeg send recipe", "😂😂😂 too real", "Watching from Abuja!", "Pls do part 2", "Na wa o, see talent"}
	for fi, f := range fans {
		for _, c := range creators {
			if (fi+c.n)%3 != 0 || f == viewer {
				must(t, 204, f, "POST", "/v1/social/follows/"+users[c.n].id, nil)
			}
		}
		for vi, id := range all {
			if (fi+vi)%2 == 0 {
				must(t, 204, f, "POST", "/v1/social/videos/"+id+"/like", nil)
			}
			if (fi+vi)%4 == 0 {
				must(t, 201, f, "POST", "/v1/social/videos/"+id+"/comments", map[string]string{"text": comments[(fi+vi)%len(comments)]})
			}
		}
		var imps []map[string]any
		for vi, id := range all {
			imps = append(imps, map[string]any{"video_id": id, "watch_ms": 3000 + (fi*vi*911)%9000, "surface": "foryou"})
		}
		must(t, 202, f, "POST", "/v1/feed/impressions", map[string]any{"items": imps})
	}
	for _, c := range creators[1:] {
		must(t, 204, users[c.n], "POST", "/v1/social/follows/"+users[1].id, nil)
	}

	// Money: Chioma buys coins on the web and gifts Ada; Ada cashes out.
	topUp(t, viewer, "coins_7000")
	for i, g := range []struct {
		id  string
		qty int
	}{{"star", 12}, {"rose", 30}, {"heart", 99}} {
		must(t, 200, viewer, "POST", "/v1/wallet/gifts/send", map[string]any{"receiver_id": users[1].id, "gift_id": g.id, "qty": g.qty, "video_id": all[i]},
			"Idempotency-Key", newID())
	}
	for _, f := range fans[1:4] {
		topUp(t, f, "coins_350")
		must(t, 200, f, "POST", "/v1/wallet/gifts/send", map[string]any{"receiver_id": users[3].id, "gift_id": "rose", "qty": 20, "video_id": all[5]}, "Idempotency-Key", newID())
	}
	must(t, 200, viewer, "POST", "/v1/wallet/checkin", nil)
	eventually(t, "Ada's diamonds", 20*time.Second, func() string {
		var b struct {
			Diamonds int64 `json:"diamonds"`
		}
		must(t, 200, users[1], "GET", "/v1/wallet", nil).json(t, &b)
		if b.Diamonds < 500 {
			return fmt.Sprint(b.Diamonds)
		}
		return ""
	})
	must(t, 200, users[1], "PUT", "/v1/wallet/payout-account", map[string]string{"bank_code": "058", "account_number": "0123456789"})
	must(t, 202, users[1], "POST", "/v1/wallet/payouts", map[string]int{"diamonds": 400}, "Idempotency-Key", newID())

	// DMs between Chioma and Ada.
	var conv struct {
		ID string `json:"id"`
	}
	must(t, 200, viewer, "POST", "/v1/dm/conversations", map[string]string{"user_id": users[1].id}).json(t, &conv)
	for i, m := range []struct {
		from *user
		text string
	}{{viewer, "Hi Ada! Your jollof video made me hungry 😅"}, {users[1], "Haha thank you Chioma! Did you try it?"}, {viewer, "Trying it this Sunday. Which rice do you use?"}, {users[1], "Long grain parboiled, and don't skip the bay leaves 🍃"}} {
		must(t, 201, m.from, "POST", "/v1/dm/conversations/"+conv.ID+"/messages", map[string]string{"text": m.text, "client_id": fmt.Sprintf("%s-%d", conv.ID[:8], i)})
	}

	// Tunde is live right now.
	var cats struct {
		Items []string `json:"items"`
	}
	must(t, 200, nil, "GET", "/v1/live/categories", nil).json(t, &cats)
	cat := cats.Items[0]
	for _, c := range cats.Items {
		if strings.EqualFold(c, "comedy") {
			cat = c
		}
	}
	must(t, 201, users[2], "POST", "/v1/live/rooms", map[string]any{"title": "Friday skits LIVE 😂", "category": cat, "source": "app",
		"allow_pk": true, "allow_gifts": true, "save_replay": false, "goal_coins": 5000, "goal_label": "New ring light"})

	t.Logf("demo ready. viewer=%s (load5), creator Ada=%s (load1), finance=%s (load6); %d videos", viewer.id, users[1].id, finance.id, len(all))
	for _, id := range all[:1] {
		t.Logf("share page: http://localhost:3000/v/%s", id)
	}
}

func upload(t *testing.T, u *user, path, title, desc string) string {
	t.Helper()
	data, _ := os.ReadFile(path)
	var up struct {
		VideoID  string   `json:"video_id"`
		PartSize int64    `json:"part_size"`
		PartURLs []string `json:"part_urls"`
	}
	must(t, 201, u, "POST", "/v1/media/uploads", map[string]any{"kind": "short", "size_bytes": len(data), "content_type": "video/mp4",
		"title": title, "description": desc, "visibility": "public"}).json(t, &up)
	var parts []map[string]any
	for i, pu := range up.PartURLs {
		from := int64(i) * up.PartSize
		to := min(from+up.PartSize, int64(len(data)))
		req, _ := httpNewPut(pu, data[from:to])
		res, err := hc.Do(req)
		if err != nil || res.StatusCode != 200 {
			t.Fatalf("part %d: %v %v", i+1, err, res)
		}
		res.Body.Close()
		parts = append(parts, map[string]any{"number": i + 1, "etag": res.Header.Get("ETag")})
	}
	must(t, 200, u, "POST", "/v1/media/uploads/"+up.VideoID+"/complete", map[string]any{"parts": parts})
	return up.VideoID
}

func httpNewPut(url string, b []byte) (*http.Request, error) {
	return http.NewRequest("PUT", url, bytes.NewReader(b))
}

// topUp buys a coin pack through the real web checkout and a signed webhook.
func topUp(t *testing.T, u *user, pack string) {
	t.Helper()
	var co struct {
		Ref string `json:"reference"`
	}
	must(t, 200, u, "POST", "/v1/wallet/web/checkout", map[string]string{"pack_id": pack, "email": u.email, "callback_url": "http://localhost:3000/wallet"}).json(t, &co)
	if res, err := hc.Post(paystack+"/__pay/"+co.Ref, "application/json", nil); err != nil {
		t.Fatal(err)
	} else {
		res.Body.Close()
	}
	b, _ := json.Marshal(map[string]any{"event": "charge.success", "data": map[string]string{"reference": co.Ref}})
	m := hmac.New(sha512.New, []byte(psSecret))
	m.Write(b)
	req, _ := http.NewRequest("POST", gw+"/v1/wallet/webhooks/paystack", bytes.NewReader(b))
	req.Header.Set("X-Paystack-Signature", hex.EncodeToString(m.Sum(nil)))
	res, err := hc.Do(req)
	if err != nil || res.StatusCode != 200 {
		t.Fatalf("top-up webhook: %v %v", err, res)
	}
	res.Body.Close()
}
