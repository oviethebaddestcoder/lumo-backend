package app

// Offline evaluation of the For You recommender with simulated viewers.
//
// Each simulated viewer has hidden tastes (2-4 favourite topics out of 40)
// and every video has a hidden quality. A viewer finishes a video with a
// probability that rises with taste match and quality; finished videos are
// sometimes liked. Every page is served by the real ForYou code, and every
// view and like goes back through the real signal pipeline (OnImpressions,
// OnSocial), exactly like production. Policies are compared on the same
// catalogue and viewers:
//
//   - random:      no ranking at all
//   - popular:     global quality, trending and freshness, no personal signals
//   - lumo:        the default weights
//
//	LUMO_SIM=1 LUMO_TEST_REDIS=redis://localhost:6379/9 go test ./internal/app -run Sim -v
//
// Simulations are a sanity check of the mechanics, not a substitute for an
// A/B test on real viewers (RANK_B_WEIGHTS).

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/redisx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/userdir"
)

const (
	simTopics   = 40
	simCreators = 300
	simVideos   = 2000
	simViewers  = 300
	simRounds   = 12 // pages per viewer
	simPage     = 10
)

type simVideo struct {
	id, owner string
	topic     int
	quality   float64 // 0..1
	created   time.Time
}

type simViewer struct {
	id    string
	taste [simTopics]float64 // 0..1
}

type simWorld struct {
	videos  []simVideo
	byID    map[string]*simVideo
	viewers []simViewer
	newCr   map[string]bool // creators who joined this week
}

func newWorld(seed uint64) *simWorld {
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	w := &simWorld{byID: map[string]*simVideo{}, newCr: map[string]bool{}}
	creators := make([]string, simCreators)
	specialty := make([]int, simCreators)
	for i := range creators {
		creators[i] = events.NewID()
		specialty[i] = r.IntN(simTopics)
		if i%10 == 0 {
			w.newCr[creators[i]] = true
		}
	}
	now := time.Now()
	for i := 0; i < simVideos; i++ {
		c := r.IntN(simCreators)
		topic := specialty[c]
		if r.Float64() < 0.2 {
			topic = r.IntN(simTopics)
		}
		age := time.Duration(r.IntN(14*24)) * time.Hour
		if w.newCr[creators[c]] {
			age = time.Duration(r.IntN(48)) * time.Hour
		}
		v := simVideo{id: events.NewID(), owner: creators[c], topic: topic, quality: betaish(r), created: now.Add(-age)}
		w.videos = append(w.videos, v)
	}
	for i := range w.videos {
		w.byID[w.videos[i].id] = &w.videos[i]
	}
	for i := 0; i < simViewers; i++ {
		v := simViewer{id: events.NewID()}
		for k := 0; k < 2+r.IntN(3); k++ {
			v.taste[r.IntN(simTopics)] = 0.6 + 0.4*r.Float64()
		}
		w.viewers = append(w.viewers, v)
	}
	return w
}

// betaish: most videos are average, a few are great.
func betaish(r *rand.Rand) float64 { return math.Pow(r.Float64(), 1.6) }

// pFinish is the hidden truth: taste match matters most, quality helps.
func pFinish(taste, quality float64) float64 {
	return 1 / (1 + math.Exp(-(4.5*taste + 2.0*quality - 3.2)))
}

type simResult struct {
	policy                                   string
	finish, finishFirst3, tasteHit, coverage float64
	newCreatorShare, top10Share              float64
}

func runPolicy(t *testing.T, policy string, arms Arms, seed uint64, onboarding bool) simResult {
	ctx := context.Background()
	rdb, _ := redisx.Open(ctx, os.Getenv("LUMO_TEST_REDIS"), "", "")
	rdb.FlushDB(ctx)
	id := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var items []events.UserPublic
		for _, x := range strings.Split(r.URL.Query().Get("ids"), ",") {
			items = append(items, events.UserPublic{ID: x, Status: "active"})
		}
		json.NewEncoder(w).Encode(map[string]any{"items": items})
	}))
	defer id.Close()
	s := &Service{R: rdb, Users: userdir.New(id.URL), Arms: arms}
	w := newWorld(seed)
	for _, v := range w.videos {
		if err := s.OnVideo(ctx, events.MustNew("m", events.VideoState, v.id, events.VideoStateData{ID: v.id, OwnerID: v.owner, Kind: "short",
			Status: "ready", Visibility: "public", Hashtags: []string{fmt.Sprint("topic", v.topic), "naija"}, DurationMS: 15_000, CreatedAt: v.created})); err != nil {
			t.Fatal(err)
		}
	}
	r := rand.New(rand.NewPCG(seed+1, seed+2))
	if onboarding {
		// 70% of new viewers pick topics at sign-up: one they really like,
		// and sometimes one they don't.
		for _, v := range w.viewers {
			if r.Float64() >= 0.7 {
				continue
			}
			var liked []int
			for t, x := range v.taste {
				if x > 0 {
					liked = append(liked, t)
				}
			}
			picks := []string{fmt.Sprint("topic", liked[r.IntN(len(liked))])}
			if r.Float64() < 0.3 {
				picks = append(picks, fmt.Sprint("topic", r.IntN(simTopics)))
			}
			if err := s.SetInterests(ctx, v.id, picks); err != nil {
				t.Fatal(err)
			}
		}
	}
	var served, finished, served3, finished3 int
	var taste float64
	distinct := map[string]bool{}
	perCreator := map[string]int{}
	for round := 0; round < simRounds; round++ {
		for vi := range w.viewers {
			viewer := &w.viewers[vi]
			items, err := s.ForYou(ctx, viewer.id, "short", simPage)
			if err != nil {
				t.Fatal(err)
			}
			var imps []events.Envelope
			for _, it := range items {
				v := w.byID[it.ID]
				tm := viewer.taste[v.topic]
				p := pFinish(tm, v.quality)
				done := r.Float64() < p
				watch := int64(15_000)
				if !done {
					watch = int64(500 + r.IntN(6000))
				}
				imps = append(imps, events.MustNew("f", events.Impression, viewer.id, events.ImpressionData{UserID: viewer.id, VideoID: v.id, WatchMS: watch}))
				if done && r.Float64() < 0.25 {
					if err := s.OnSocial(ctx, events.MustNew("s", events.Liked, v.id, events.LikeData{UserID: viewer.id, VideoID: v.id, OwnerID: v.owner})); err != nil {
						t.Fatal(err)
					}
				}
				served++
				taste += tm
				distinct[v.id] = true
				perCreator[v.owner]++
				if done {
					finished++
				}
				if round < 3 {
					served3++
					if done {
						finished3++
					}
				}
			}
			if err := s.OnImpressions(ctx, imps); err != nil {
				t.Fatal(err)
			}
		}
	}
	counts := make([]int, 0, len(perCreator))
	newShare := 0
	for c, n := range perCreator {
		counts = append(counts, n)
		if w.newCr[c] {
			newShare += n
		}
	}
	sort.Sort(sort.Reverse(sort.IntSlice(counts)))
	top := 0
	for i := 0; i < len(counts) && i < simCreators/10; i++ {
		top += counts[i]
	}
	return simResult{
		policy: policy, finish: float64(finished) / float64(served), finishFirst3: float64(finished3) / float64(served3),
		tasteHit: taste / float64(served), coverage: float64(len(distinct)) / simVideos,
		newCreatorShare: float64(newShare) / float64(served), top10Share: float64(top) / float64(served),
	}
}

func TestSimRecommender(t *testing.T) {
	if os.Getenv("LUMO_SIM") == "" || os.Getenv("LUMO_TEST_REDIS") == "" {
		t.Skip("set LUMO_SIM=1 and LUMO_TEST_REDIS")
	}
	random := Weights{Jitter: 10}
	popular := Weights{Quality: 1.5, Fresh: 0.35, Trending: 0.3, Explore: 0.25, Jitter: 0.05, SameAuthor: 0.6, SameTag: 0.15}
	type policy struct {
		name    string
		w       Weights
		onboard bool
	}
	policies := []policy{{"random", random, false}, {"popular", popular, false}, {"lumo", DefaultWeights, false}, {"lumo+picker", DefaultWeights, true}}
	extra := os.Getenv("LUMO_SIM_WEIGHTS") // try a candidate weight set: JSON
	if extra != "" {
		cand := DefaultWeights
		if err := json.Unmarshal([]byte(extra), &cand); err != nil {
			t.Fatal(err)
		}
		policies = append(policies, policy{"candidate", cand, true})
	}
	var res []simResult
	for _, p := range policies {
		var sum simResult
		seeds := []uint64{11, 22, 33}
		if os.Getenv("LUMO_SIM_QUICK") != "" {
			seeds = seeds[:1]
		}
		for _, sd := range seeds {
			r := runPolicy(t, p.name, Arms{A: p.w, B: p.w}, sd, p.onboard)
			k := float64(len(seeds))
			sum.finish += r.finish / k
			sum.finishFirst3 += r.finishFirst3 / k
			sum.tasteHit += r.tasteHit / k
			sum.coverage += r.coverage / k
			sum.newCreatorShare += r.newCreatorShare / k
			sum.top10Share += r.top10Share / k
		}
		sum.policy = p.name
		res = append(res, sum)
	}
	t.Logf("%d viewers × %d pages of %d, %d videos, %d creators (10%% new this week), 3 seeds", simViewers, simRounds, simPage, simVideos, simCreators)
	t.Logf("%-12s %9s %14s %10s %9s %13s %15s", "policy", "finished", "first 3 pages", "taste hit", "coverage", "new creators", "top 10% creators")
	for _, r := range res {
		t.Logf("%-12s %8.1f%% %13.1f%% %9.2f %8.1f%% %12.1f%% %14.1f%%", r.policy, 100*r.finish, 100*r.finishFirst3, r.tasteHit, 100*r.coverage, 100*r.newCreatorShare, 100*r.top10Share)
	}
	// Guard rails: personalization must clearly beat both baselines, and
	// must not starve new creators.
	lumo, pop, rnd, picker := res[2], res[1], res[0], res[3]
	if picker.finishFirst3 < lumo.finishFirst3*1.3 {
		t.Errorf("interest picker should fix cold start: first pages %.3f vs %.3f", picker.finishFirst3, lumo.finishFirst3)
	}
	if lumo.finish < pop.finish*1.15 || lumo.finish < rnd.finish*1.3 {
		t.Errorf("lumo completion %.3f does not clearly beat popular %.3f / random %.3f", lumo.finish, pop.finish, rnd.finish)
	}
	if lumo.newCreatorShare < 0.5*rnd.newCreatorShare {
		t.Errorf("new creators starved: %.3f vs %.3f under random", lumo.newCreatorShare, rnd.newCreatorShare)
	}
}
