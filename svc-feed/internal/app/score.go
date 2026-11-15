package app

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"strconv"
)

// Weights is the ranking formula. Defaults are hand-tuned and meant to be
// replaced by numbers learned from feed.impressions. Override them with
// RANK_WEIGHTS (JSON), and A/B test a second set with RANK_B_WEIGHTS plus
// RANK_B_PERCENT (share of users, by stable hash of user ID). Every served
// item carries its arm, and clients echo it back on the impression.
type Weights struct {
	Tag      float64 `json:"tag"`      // hashtag interest
	Author   float64 `json:"author"`   // creator interest
	Follow   float64 `json:"follow"`   // the viewer follows the creator
	Co       float64 `json:"co"`       // "people who liked what you liked also liked"
	Quality  float64 `json:"quality"`  // smoothed engagement per impression, minus skips
	Fresh    float64 `json:"fresh"`    // recency
	Trending float64 `json:"trending"` // position in the global hot list
	Explore  float64 `json:"explore"`  // uncertainty bonus for videos with few impressions
	Jitter   float64 `json:"jitter"`   // randomness so refreshes differ
	// AffScale: how much decayed signal makes a "strong" interest (interest
	// = tanh(signal / AffScale)). Lower reacts faster to fewer signals.
	AffScale float64 `json:"aff_scale"`

	// Re-ranking.
	SameAuthor   float64 `json:"same_author"`   // penalty per earlier pick from the same creator
	SameTag      float64 `json:"same_tag"`      // penalty per recent pick with the same main hashtag
	ExploreEvery int     `json:"explore_every"` // one slot in N is reserved for discovery (0 = off)
}

// DefaultWeights were chosen with the offline simulation (sim_test.go):
// stronger, faster-reacting taste terms and lighter global popularity
// lifted simulated completion from 19% to 43% over random-like ranking
// while spreading views across more creators. Confirm with an A/B test on
// real viewers before trusting any change.
var DefaultWeights = Weights{
	Tag: 2.0, Author: 1.2, Follow: 0.5, Co: 0.8, Quality: 0.8, Fresh: 0.35, Trending: 0.15, Explore: 0.25, Jitter: 0.05,
	AffScale: 1.5, SameAuthor: 0.6, SameTag: 0.15, ExploreEvery: 5,
}

// Arms picks the weight set for a user.
type Arms struct {
	A, B     Weights
	BPercent int // 0..100
}

func (a Arms) For(userID string) (string, Weights) {
	if a.BPercent <= 0 || userID == "" {
		return "a", a.A
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte("rank-ab:" + userID))
	if int(h.Sum32()%100) < a.BPercent {
		return "b", a.B
	}
	return "a", a.A
}

// ArmsFromEnv reads RANK_WEIGHTS, RANK_B_WEIGHTS and RANK_B_PERCENT. Unset
// fields keep their defaults.
func ArmsFromEnv() (Arms, error) {
	arms := Arms{A: DefaultWeights, B: DefaultWeights}
	if v := os.Getenv("RANK_WEIGHTS"); v != "" {
		if err := json.Unmarshal([]byte(v), &arms.A); err != nil {
			return arms, fmt.Errorf("RANK_WEIGHTS: %w", err)
		}
		arms.B = arms.A
	}
	if v := os.Getenv("RANK_B_WEIGHTS"); v != "" {
		if err := json.Unmarshal([]byte(v), &arms.B); err != nil {
			return arms, fmt.Errorf("RANK_B_WEIGHTS: %w", err)
		}
	}
	if v := os.Getenv("RANK_B_PERCENT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil || p < 0 || p > 100 {
			return arms, fmt.Errorf("RANK_B_PERCENT must be 0..100")
		}
		arms.BPercent = p
	}
	return arms, nil
}

// ---- signals ----

// Signal weights. Positive means "more like this".
const (
	wLike          = 2.0
	wComment       = 2.5
	wShare         = 3.0
	wFollow        = 4.0 // creator only
	wNotInterested = -2.0
	wNotInterAuth  = -4.0 // extra, creator only
	wReport        = -3.0
	wReportAuth    = -6.0
	wTopicRemoved  = -3.0
	wPicked        = 2.5 // a topic chosen in the sign-up picker
)

// watchWeight turns one view into an interest signal. For short videos
// completion and replays matter; for long videos, minutes watched.
func watchWeight(watchMS, durMS int64, loops int, skipped bool, kind string) float64 {
	if durMS <= 0 {
		durMS = 15_000
	}
	if watchMS < 0 {
		watchMS = 0
	}
	r := float64(watchMS) / float64(durMS)
	if skipped || (watchMS < 1500 && r < 0.2) {
		return -0.6
	}
	if kind == "long" || durMS > 3*60_000 {
		minutes := float64(watchMS) / 60_000
		if minutes < 0.5 && r < 0.5 {
			return -0.3
		}
		w := math.Min(2.5, 0.25*minutes)
		if r >= 0.9 {
			w += 0.5
		}
		return w
	}
	switch {
	case r < 0.3:
		return -0.15
	case r < 0.8:
		return 0.4 * r
	}
	// Watching past the end means it looped.
	if inferred := int(r) - 1; inferred > loops {
		loops = inferred
	}
	return 1.0 + 0.4*math.Min(float64(loops), 3)
}

// isComplete: counts as a finished view for the quality score.
func isComplete(watchMS, durMS int64) bool {
	return durMS > 0 && float64(watchMS) >= 0.8*float64(durMS)
}

// tagShare spreads a signal over a video's hashtags, so piling 30 tags onto
// a video doesn't buy 30x the reach.
func tagShare(w float64, n int) float64 {
	if n <= 0 {
		return 0
	}
	return w / math.Sqrt(float64(n))
}

// aff squashes a decayed interest score to -1..1.
func aff(v float64) float64 { return affScaled(v, 4) }

func affScaled(v, scale float64) float64 {
	if scale <= 0 {
		scale = 4
	}
	return math.Tanh(v / scale)
}

// specificity is how much a hashtag says about a video: 1 for tags on up
// to 15% of the catalogue, falling to 0 for tags on 60% or more (#naija,
// #fyp on nearly everything), like inverse document frequency. Broad but
// real topics in a young catalogue (#comedy on 35%) still count, partly.
func specificity(share float64) float64 {
	switch {
	case share <= 0.15:
		return 1
	case share >= 0.60:
		return 0
	}
	return (0.60 - share) / 0.45
}

// ---- video statistics ----

// Stats are lifetime per-video counters.
type Stats struct {
	Imp, Done, Skip, Likes, Comments, Shares int64
}

// quality is engagement per impression with a Bayesian prior, so a video
// with 3 views and 3 likes doesn't beat one with 300k views and 60k likes.
// Range is roughly -0.5..1.
func quality(st Stats) float64 {
	const prior = 30.0 // pseudo-impressions
	pos := float64(st.Likes) + 2*float64(st.Comments) + 3*float64(st.Shares) + float64(st.Done)
	// Likes can come from other surfaces (profile, search), so impressions
	// can lag; never let the rate exceed what we can see.
	n := math.Max(float64(st.Imp), float64(st.Likes+st.Comments+st.Shares))
	eng := (pos + prior*0.25) / (n + prior)
	skip := (float64(st.Skip) + prior*0.3) / (n + prior)
	return math.Tanh(eng - 0.6*skip)
}

// exploreBonus is 1 for a video nobody has seen yet and fades as evidence
// comes in (≈0.3 at 500 impressions): upper-confidence-bound style.
func exploreBonus(imp int64) float64 {
	if imp < 0 {
		imp = 0
	}
	return 1 / math.Sqrt(float64(imp)/50+1)
}

// freshness decays with age: a 3-day time constant for shorts, 2 weeks for long videos.
func freshness(created, now int64, kind string) float64 {
	age := math.Max(0, float64(now-created)) / 3600
	tau := 72.0
	if kind == "long" {
		tau = 336
	}
	return math.Exp(-age / tau)
}

// hotScore orders the global trending list: every 10x engagement is worth
// 12.5 hours of recency.
func hotScore(st Stats, created int64) float64 {
	eng := float64(st.Likes) + 3*float64(st.Comments) + 4*float64(st.Shares) + 0.5*float64(st.Done) - 0.25*float64(st.Skip)
	return math.Log10(math.Max(1, eng)) + float64(created)/45000
}

// ---- candidates ----

type source uint8

const (
	srcCo source = 1 << iota
	srcAuthor
	srcFollow
	srcTag
	srcHot
	srcFresh
	srcRepeat // already seen, brought back because fresh candidates ran out
)

// label is the main reason a video was picked (most personal first).
func (s source) label() string {
	switch {
	case s&srcRepeat != 0:
		return "repeat"
	case s&srcCo != 0:
		return "co"
	case s&srcFollow != 0:
		return "follow"
	case s&srcAuthor != 0:
		return "author"
	case s&srcTag != 0:
		return "tag"
	case s&srcHot != 0:
		return "hot"
	case s&srcFresh != 0:
		return "fresh"
	}
	return "other"
}

type cand struct {
	id      string
	src     source
	co      float64 // 0..1
	hot     float64 // 0..1, 1 = top of trending
	owner   string
	kind    string
	created int64
	tags    []string
	st      Stats
	follows bool
	engaged bool // the viewer liked, shared or finished it: never "fresh"

	tagAff, authAff float64
	score           float64
}

// discovery: outside what we know the viewer likes.
func (c *cand) discovery() bool {
	return c.src&(srcHot|srcFresh) != 0 && c.tagAff < 0.15 && c.authAff < 0.15 && !c.follows
}

func (c *cand) mainTag() string {
	if len(c.tags) == 0 {
		return ""
	}
	return c.tags[0]
}

// setAffinity fills tagAff and authAff from the viewer's interests: the best
// matching hashtag, pulled down by any hashtag they've pushed away, each
// weighted by how specific the tag is (spec: tag -> 0..1; missing = 1).
func (c *cand) setAffinity(tags, authors map[string]float64, spec map[string]float64, scale float64) {
	best, worst := 0.0, 0.0
	for _, t := range c.tags {
		if v, ok := tags[t]; ok {
			k := 1.0
			if s, ok := spec[t]; ok {
				k = s
			}
			a := k * affScaled(v, scale)
			best = math.Max(best, a)
			worst = math.Min(worst, a)
		}
	}
	c.tagAff = best + worst
	c.authAff = affScaled(authors[c.owner], scale)
}

// scoreOf is the ranking formula.
func scoreOf(c *cand, w Weights, now int64, rnd func() float64) float64 {
	s := w.Tag*c.tagAff + w.Author*c.authAff + w.Co*c.co + w.Quality*quality(c.st) +
		w.Fresh*freshness(c.created, now, c.kind) + w.Trending*c.hot + w.Explore*exploreBonus(c.st.Imp)
	if c.follows {
		s += w.Follow
	}
	if rnd != nil && w.Jitter > 0 {
		s += w.Jitter * rnd()
	}
	return s
}

// rerank picks n videos greedily: best adjusted score first, with
// penalties for repeating a creator or hashtag, a hard cap of 2 per
// creator per page, and every ExploreEvery-th slot given to discovery so
// the feed never collapses into one topic.
func rerank(cs []*cand, n, page int, w Weights) []*cand {
	if page <= 0 {
		page = n
	}
	used := make([]bool, len(cs))
	perAuthor := map[string]int{}
	var recentTags []string
	out := make([]*cand, 0, n)
	pick := func(discoveryOnly bool) int {
		best, bestScore := -1, math.Inf(-1)
		for i, c := range cs {
			if used[i] || perAuthor[c.owner] >= 2 || (discoveryOnly && !c.discovery()) {
				continue
			}
			s := c.score - w.SameAuthor*float64(perAuthor[c.owner])
			if t := c.mainTag(); t != "" {
				for _, r := range recentTags {
					if r == t {
						s -= w.SameTag
					}
				}
			}
			if s > bestScore {
				best, bestScore = i, s
			}
		}
		return best
	}
	for len(out) < n {
		if len(out) > 0 && len(out)%page == 0 {
			clear(perAuthor) // new page
		}
		i := -1
		if w.ExploreEvery > 0 && len(out)%w.ExploreEvery == w.ExploreEvery-1 {
			i = pick(true)
		}
		if i < 0 {
			i = pick(false)
		}
		if i < 0 {
			break
		}
		used[i] = true
		c := cs[i]
		out = append(out, c)
		perAuthor[c.owner]++
		recentTags = append(recentTags, c.mainTag())
		if len(recentTags) > 5 {
			recentTags = recentTags[1:]
		}
	}
	return out
}
