package app

import (
	"fmt"
	"math"
	"testing"
)

func TestWatchWeight(t *testing.T) {
	cases := []struct {
		name          string
		watch, dur    int64
		loops         int
		skipped       bool
		kind          string
		wantMin, wMax float64
	}{
		{"instant swipe", 600, 15_000, 0, false, "short", -0.6, -0.6},
		{"client says skipped", 5000, 15_000, 0, true, "short", -0.6, -0.6},
		{"bailed early", 3000, 15_000, 0, false, "short", -0.15, -0.15},
		{"half watched", 7500, 15_000, 0, false, "short", 0.19, 0.21},
		{"finished", 15_000, 15_000, 0, false, "short", 1, 1},
		{"looped twice (inferred)", 45_000, 15_000, 0, false, "short", 1.79, 1.81},
		{"loops capped", 15_000, 15_000, 40, false, "short", 2.19, 2.21},
		{"long: 20 minutes of 30", 20 * 60_000, 30 * 60_000, 0, false, "long", 2.5, 2.5},
		{"long: 10s of 30 min", 10_000, 30 * 60_000, 0, false, "long", -0.3, -0.3},
		{"long: finished 4 min", 4 * 60_000, 4 * 60_000, 0, false, "long", 1.5, 1.5},
		{"unknown duration", 15_000, 0, 0, false, "short", 1, 1},
	}
	for _, c := range cases {
		got := watchWeight(c.watch, c.dur, c.loops, c.skipped, c.kind)
		if got < c.wantMin-1e-9 || got > c.wMax+1e-9 {
			t.Errorf("%s: got %.3f, want %.2f..%.2f", c.name, got, c.wantMin, c.wMax)
		}
	}
}

func TestQualityPrefersEvidence(t *testing.T) {
	tiny := quality(Stats{Imp: 3, Likes: 3, Done: 3})
	big := quality(Stats{Imp: 300_000, Likes: 60_000, Done: 200_000})
	if tiny >= big {
		t.Fatalf("3/3 (%.3f) should not beat 300k views with 20%% likes (%.3f)", tiny, big)
	}
	skippy := quality(Stats{Imp: 10_000, Likes: 500, Done: 1000, Skip: 8000})
	if skippy >= quality(Stats{Imp: 10_000, Likes: 500, Done: 1000, Skip: 1000}) {
		t.Fatal("skips must lower quality")
	}
	if q := quality(Stats{}); q < -0.5 || q > 0.5 {
		t.Fatalf("new video quality %.3f should sit near neutral", q)
	}
	// Likes from other surfaces with no feed impressions don't blow up the rate.
	if q := quality(Stats{Likes: 50}); q > 1 {
		t.Fatalf("quality %.3f out of range", q)
	}
	if exploreBonus(0) != 1 || exploreBonus(500) >= exploreBonus(50) {
		t.Fatal("explore bonus must start at 1 and fade")
	}
}

func TestHotScoreOrdersByEngagementAndAge(t *testing.T) {
	now := int64(1_800_000_000)
	if hotScore(Stats{Likes: 1000}, now) <= hotScore(Stats{Likes: 10}, now) {
		t.Fatal("more engagement must rank higher")
	}
	if hotScore(Stats{Likes: 100}, now) <= hotScore(Stats{Likes: 100}, now-48*3600) {
		t.Fatal("newer must rank higher at equal engagement")
	}
}

func TestAffinity(t *testing.T) {
	c := &cand{owner: "o", tags: []string{"cooking", "jollof", "football"}}
	c.setAffinity(map[string]float64{"cooking": 8, "football": -2}, map[string]float64{"o": 4}, nil, 0)
	if c.tagAff <= 0.4 || c.tagAff >= aff(8) {
		t.Fatalf("tagAff %.3f should be strong but pulled down by a disliked tag", c.tagAff)
	}
	if c.authAff <= 0.7 {
		t.Fatalf("authAff %.3f", c.authAff)
	}
	if tagShare(4, 16) != 1 {
		t.Fatal("signal must be spread over hashtags")
	}
}

func mk(n int, owner func(int) string, score func(int) float64) []*cand {
	cs := make([]*cand, n)
	for i := range cs {
		cs[i] = &cand{id: fmt.Sprint(i), owner: owner(i), score: score(i), src: srcTag, tagAff: 0.8, tags: []string{fmt.Sprint("t", i%3)}}
	}
	return cs
}

func TestRerankCapsCreatorsPerPage(t *testing.T) {
	// One creator dominates the scores.
	cs := mk(40, func(i int) string {
		if i < 20 {
			return "star"
		}
		return fmt.Sprint("c", i)
	}, func(i int) float64 { return 10 - float64(i)*0.1 })
	out := rerank(cs, 20, 10, DefaultWeights)
	if len(out) != 20 {
		t.Fatalf("got %d", len(out))
	}
	for p := 0; p < 2; p++ {
		n := 0
		for _, c := range out[p*10 : p*10+10] {
			if c.owner == "star" {
				n++
			}
		}
		if n != 2 {
			t.Fatalf("page %d has %d from the top creator, want exactly 2", p, n)
		}
	}
}

func TestRerankReservesDiscovery(t *testing.T) {
	cs := mk(30, func(i int) string { return fmt.Sprint("c", i) }, func(i int) float64 { return 10 - float64(i)*0.1 })
	// One low-scoring video from outside the viewer's interests.
	explore := &cand{id: "x", owner: "new", score: -5, src: srcFresh}
	cs = append(cs, explore)
	out := rerank(cs, 10, 10, DefaultWeights)
	if out[4] != explore {
		t.Fatalf("slot 5 should be the discovery pick, got %s", out[4].id)
	}
	w := DefaultWeights
	w.ExploreEvery = 0
	for _, c := range rerank(cs, 10, 10, w) {
		if c == explore {
			t.Fatal("discovery off: low score must not be picked")
		}
	}
}

func TestArms(t *testing.T) {
	arms := Arms{A: DefaultWeights, B: DefaultWeights, BPercent: 30}
	arms.B.Tag = 2
	b := 0
	for i := 0; i < 10_000; i++ {
		uid := fmt.Sprintf("user-%d", i)
		arm, w := arms.For(uid)
		if again, _ := arms.For(uid); again != arm {
			t.Fatal("arm must be stable per user")
		}
		if arm == "b" {
			b++
			if w.Tag != 2 {
				t.Fatal("arm b uses B weights")
			}
		}
	}
	if b < 2700 || b > 3300 {
		t.Fatalf("arm b got %d of 10000, want ~3000", b)
	}
	if arm, _ := arms.For(""); arm != "a" {
		t.Fatal("anonymous is arm a")
	}
}

func TestArmsFromEnv(t *testing.T) {
	t.Setenv("RANK_WEIGHTS", `{"tag":1.7}`)
	t.Setenv("RANK_B_WEIGHTS", `{"co":2}`)
	t.Setenv("RANK_B_PERCENT", "10")
	a, err := ArmsFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if a.A.Tag != 1.7 || a.A.Author != DefaultWeights.Author || a.B.Co != 2 || a.B.Tag != 1.7 || a.BPercent != 10 {
		t.Fatalf("%+v", a)
	}
	t.Setenv("RANK_B_PERCENT", "101")
	if _, err := ArmsFromEnv(); err == nil {
		t.Fatal("percent out of range accepted")
	}
}

func TestNormTagsAndLabels(t *testing.T) {
	got := normTags([]string{"#Cooking", "cooking", " Jollof ", "", "a,b", "x y", "1", "2", "3", "4", "5", "6", "7"})
	if len(got) != maxTags || got[0] != "cooking" || got[1] != "jollof" {
		t.Fatalf("%v", got)
	}
	if label("co") != "co" || label("<script>") != "none" {
		t.Fatal("labels must be allow-listed")
	}
}

func TestSampleFavoursTop(t *testing.T) {
	pool := make([]ranked, 150)
	for i := range pool {
		pool[i] = ranked{id: fmt.Sprint(i), owner: fmt.Sprint(i)}
	}
	top := 0
	for r := 0; r < 200; r++ {
		got := sample(pool, 10)
		if len(got) != 10 {
			t.Fatal("short sample")
		}
		seen := map[string]bool{}
		for _, g := range got {
			if seen[g.id] {
				t.Fatal("duplicate in sample")
			}
			seen[g.id] = true
			var n int
			fmt.Sscan(g.id, &n)
			if n < 50 {
				top++
			}
		}
	}
	if top < 1000 { // uniform would be 33%; we want at least half from the top third
		t.Fatalf("only %d/2000 picks from the top third", top)
	}
}

func TestSpecificityIgnoresGenericTags(t *testing.T) {
	if specificity(0.1) != 1 || specificity(0.7) != 0 || math.Abs(specificity(0.375)-0.5) > 1e-9 {
		t.Fatal("specificity curve")
	}
	c := &cand{owner: "o", tags: []string{"naija", "cooking"}}
	c.setAffinity(map[string]float64{"naija": 9}, nil, map[string]float64{"naija": 0}, 4)
	if c.tagAff != 0 {
		t.Fatalf("a tag on every video must not count as interest: %.3f", c.tagAff)
	}
}
