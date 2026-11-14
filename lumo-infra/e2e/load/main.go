// Command load drives a realistic traffic mix through the gateway of a
// running e2e stack (run.sh with ONLY_UP=1) and reports latency per route.
//
//	go run ./load -users 50 -vus 100 -duration 60s
//
// Setup: signs in the load accounts (load1..N@e2e.test, code 000000), tops
// each up with coins through the real web checkout + signed webhook, and
// seeds a catalogue of videos straight onto media.videos so the feed has
// something to rank.
package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	mrand "math/rand/v2"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	gw       = flag.String("gateway", "http://localhost:18080", "gateway URL")
	paystack = flag.String("paystack", "http://localhost:19100", "fake Paystack URL")
	secret   = flag.String("paystack-secret", "sk_test_e2e", "Paystack secret (webhook signing)")
	brokers  = flag.String("kafka", "localhost:19092", "Kafka brokers")
	nUsers   = flag.Int("users", 50, "accounts to sign in (load1..N@e2e.test)")
	vus      = flag.Int("vus", 100, "concurrent virtual users")
	dur      = flag.Duration("duration", 60*time.Second, "test length")
	videos   = flag.Int("videos", 2000, "videos to seed")
	hc       = &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 512, MaxConnsPerHost: 0}}
)

type user struct{ id, token string }

func do(method, path, token string, body any, hdr ...string) (int, []byte, error) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, *gw+path, rd)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	res, err := hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res.StatusCode, b, nil
}

func uuid() string {
	var b [16]byte
	rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

func signIn(i int) (user, error) {
	email := fmt.Sprintf("load%d@e2e.test", i)
	if st, b, err := do("POST", "/v1/auth/otp/send", "", map[string]string{"channel": "email", "target": email}); err != nil || st/100 != 2 {
		return user{}, fmt.Errorf("otp send %s: %d %s %v", email, st, b, err)
	}
	st, b, err := do("POST", "/v1/auth/otp/verify", "", map[string]any{"channel": "email", "target": email, "code": "000000",
		"device": map[string]string{"device_id": fmt.Sprint("load-", i), "device_name": "load", "platform": "android"}, "birth_date": "1995-05-05"})
	if err != nil || st/100 != 2 {
		return user{}, fmt.Errorf("otp verify %s: %d %s %v", email, st, b, err)
	}
	var out struct {
		Tokens struct {
			AccessToken string `json:"access_token"`
		} `json:"tokens"`
		User struct {
			ID string `json:"id"`
		} `json:"user"`
	}
	json.Unmarshal(b, &out)
	return user{out.User.ID, out.Tokens.AccessToken}, nil
}

func topUp(u user) error {
	st, b, err := do("POST", "/v1/wallet/web/checkout", u.token, map[string]string{"pack_id": "coins_7000", "email": "load@e2e.test", "callback_url": "http://localhost/wallet"})
	if err != nil || st/100 != 2 {
		return fmt.Errorf("checkout %d %s %v", st, b, err)
	}
	var co struct {
		Ref string `json:"reference"`
	}
	json.Unmarshal(b, &co)
	if res, err := hc.Post(*paystack+"/__pay/"+co.Ref, "application/json", nil); err != nil {
		return err
	} else {
		res.Body.Close()
	}
	payload, _ := json.Marshal(map[string]any{"event": "charge.success", "data": map[string]string{"reference": co.Ref}})
	m := hmac.New(sha512.New, []byte(*secret))
	m.Write(payload)
	req, _ := http.NewRequest("POST", *gw+"/v1/wallet/webhooks/paystack", bytes.NewReader(payload))
	req.Header.Set("X-Paystack-Signature", hex.EncodeToString(m.Sum(nil)))
	res, err := hc.Do(req)
	if err != nil {
		return err
	}
	res.Body.Close()
	return nil
}

func seed(owners []user) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...))
	if err != nil {
		return err
	}
	defer cl.Close()
	tags := []string{"cooking", "football", "afrobeats", "comedy", "fashion", "tech", "dance", "lagos", "naija", "music", "skits", "travel"}
	now := time.Now().UTC()
	var recs []*kgo.Record
	for i := 0; i < *videos; i++ {
		id := uuid()
		o := owners[i%len(owners)]
		d := events.VideoStateData{ID: id, OwnerID: o.id, Kind: "short", Status: "ready", Visibility: "public",
			Hashtags: []string{tags[mrand.IntN(len(tags))], tags[mrand.IntN(len(tags))]}, DurationMS: int64(8000 + mrand.IntN(50000)),
			HLSURL: "http://localhost:19000/lumo-media/hls/" + id + "/master.m3u8", Title: fmt.Sprint("Load video ", i),
			CreatedAt: now.Add(-time.Duration(mrand.IntN(7*24*60)) * time.Minute)}
		env := events.MustNew("svc-media", events.VideoState, id, d)
		b, _ := json.Marshal(env)
		recs = append(recs, &kgo.Record{Topic: events.TopicMediaVideos, Key: []byte(id), Value: b,
			Headers: []kgo.RecordHeader{{Key: "event_type", Value: []byte(env.Type)}, {Key: "event_id", Value: []byte(env.ID)}}})
	}
	return cl.ProduceSync(context.Background(), recs...).FirstErr()
}

// ---- measurement ----

type stats struct {
	mu   sync.Mutex
	lat  map[string][]time.Duration
	errs map[string]int
	code map[string]map[int]int
}

func (s *stats) add(route string, d time.Duration, status int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lat[route] = append(s.lat[route], d)
	if err != nil || status >= 400 {
		s.errs[route]++
		if s.code[route] == nil {
			s.code[route] = map[int]int{}
		}
		s.code[route][status]++
	}
}

func pct(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	return ds[min(len(ds)-1, int(float64(len(ds))*p))]
}

func main() {
	flag.Parse()
	log.SetFlags(0)
	t0 := time.Now()
	users := make([]user, *nUsers)
	var wg sync.WaitGroup
	var setupErr atomic.Value
	sem := make(chan struct{}, 10)
	for i := range users {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			u, err := signIn(i + 1)
			if err == nil {
				err = topUp(u)
			}
			if err != nil {
				setupErr.Store(err)
			}
			users[i] = u
		}(i)
	}
	wg.Wait()
	if err, _ := setupErr.Load().(error); err != nil {
		log.Fatalf("setup: %v", err)
	}
	if err := seed(users); err != nil {
		log.Fatalf("seed: %v", err)
	}
	// Wait for the feed to project the catalogue.
	for i := 0; ; i++ {
		_, b, _ := do("GET", "/v1/feed/foryou?limit=20", users[0].token, nil)
		var out struct {
			Items []json.RawMessage `json:"items"`
		}
		json.Unmarshal(b, &out)
		if len(out.Items) >= 10 || i > 100 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	log.Printf("setup: %d users signed in and topped up, %d videos seeded in %v", len(users), *videos, time.Since(t0).Round(time.Millisecond))

	st := &stats{lat: map[string][]time.Duration{}, errs: map[string]int{}, code: map[string]map[int]int{}}
	stop := time.Now().Add(*dur)
	var total atomic.Int64
	for v := 0; v < *vus; v++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			u := users[v%len(users)]
			var seen []string
			owner := map[string]string{}
			for time.Now().Before(stop) {
				r := mrand.IntN(100)
				var route string
				start := time.Now()
				var status int
				var err error
				var body []byte
				switch {
				case r < 55:
					route = "GET /v1/feed/foryou"
					status, body, err = do("GET", "/v1/feed/foryou?limit=10", u.token, nil)
					var out struct {
						Items []struct {
							ID     string `json:"id"`
							Author struct {
								ID string `json:"id"`
							} `json:"author"`
						} `json:"items"`
					}
					json.Unmarshal(body, &out)
					for _, it := range out.Items {
						seen = append(seen, it.ID)
						owner[it.ID] = it.Author.ID
					}
					if len(seen) > 50 {
						seen = seen[len(seen)-50:]
					}
				case r < 75 && len(seen) > 0:
					route = "POST /v1/feed/impressions"
					var items []map[string]any
					for i := 0; i < 5 && i < len(seen); i++ {
						items = append(items, map[string]any{"video_id": seen[mrand.IntN(len(seen))], "watch_ms": mrand.IntN(30000), "source": "tag", "arm": "a"})
					}
					status, _, err = do("POST", "/v1/feed/impressions", u.token, map[string]any{"items": items})
				case r < 85 && len(seen) > 0:
					route = "POST /v1/social/videos/{id}/like"
					status, _, err = do("POST", "/v1/social/videos/"+seen[mrand.IntN(len(seen))]+"/like", u.token, nil)
				case r < 90:
					route = "GET /v1/me"
					status, _, err = do("GET", "/v1/me", u.token, nil)
				case r < 95:
					route = "GET /v1/wallet"
					status, _, err = do("GET", "/v1/wallet", u.token, nil)
				default:
					if len(seen) == 0 {
						continue
					}
					vid := seen[mrand.IntN(len(seen))]
					if owner[vid] == u.id || owner[vid] == "" {
						continue
					}
					route = "POST /v1/wallet/gifts/send"
					status, _, err = do("POST", "/v1/wallet/gifts/send", u.token, map[string]any{"receiver_id": owner[vid], "gift_id": "rose", "qty": 1, "video_id": vid},
						"Idempotency-Key", uuid())
				}
				if route == "" {
					continue
				}
				st.add(route, time.Since(start), status, err)
				total.Add(1)
			}
		}(v)
	}
	wg.Wait()

	elapsed := dur.Seconds()
	fmt.Printf("\n%d virtual users, %d accounts, %v: %d requests, %.0f req/s\n\n", *vus, *nUsers, *dur, total.Load(), float64(total.Load())/elapsed)
	tw := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "route\treq/s\tp50\tp95\tp99\terrors\t")
	routes := make([]string, 0, len(st.lat))
	for r := range st.lat {
		routes = append(routes, r)
	}
	sort.Strings(routes)
	errTotal := 0
	for _, r := range routes {
		ds := st.lat[r]
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		errTotal += st.errs[r]
		e := fmt.Sprint(st.errs[r])
		if len(st.code[r]) > 0 {
			e += fmt.Sprintf(" %v", st.code[r])
		}
		fmt.Fprintf(tw, "%s\t%.0f\t%v\t%v\t%v\t%s\t\n", r, float64(len(ds))/elapsed, pct(ds, .5).Round(100*time.Microsecond),
			pct(ds, .95).Round(100*time.Microsecond), pct(ds, .99).Round(100*time.Microsecond), e)
	}
	tw.Flush()
	fmt.Printf("\nerror rate %.3f%%\n", 100*float64(errTotal)/float64(max(total.Load(), 1)))
}
