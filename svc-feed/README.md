# svc-feed

Feed: For You, Following, Watch.

Part of the Lumo backend: independent Go services that talk through Kafka.
Shared code comes from the versioned modules `lumo-kit` and `lumo-contracts`;
nothing here imports another service. See `lumo-infra` for the whole picture.

## What it does

Owns no database. Its state in Redis is a projection of `media.videos` (compacted, so it can be rebuilt from the start), `social.events` and `feed.impressions`.

### For You recommender

A two-stage recommender, rebuilt from the viewer's own behaviour in real time.

**1. Signals → interest profiles** (`signals.go`). Every view, like, comment, share, follow, skip, "not interested" and report adjusts the viewer's interest in the video's **hashtags** and **creator**:

| Signal | Weight |
|---|---|
| Swiped away (<1.5 s) | −0.6 |
| Watched < 30% | −0.15 |
| Watched 30–80% | +0.4 × fraction |
| Finished | +1.0, +0.4 per replay (max 3) |
| Long video | +0.25 per minute (max 2.5), +0.5 if finished |
| Like / comment / share | +2 / +2.5 / +3 |
| Follow | +4 on the creator |
| Not interested | −2 topics, −6 creator |
| Report video | −3 topics, −6 creator |

Interest builds as tanh(signal / 1.5). Weights are spread across a video's hashtags (÷√n), so hashtag stuffing doesn't pay. Interest fades with a **5-day half-life** (forward decay, so each update is a single atomic `ZINCRBY`). A view counts once per viewer per video per 2 days, which also makes Kafka redelivery harmless.

**2. Retrieval** (`recommend.go`) gathers ~900 candidates per viewer from: their top 8 hashtags, top 10 creators, 20 followed creators, **co-engagement** ("people who finished what you finished also finished…", an item-to-item graph built from every positive view), global trending, and fresh uploads. Seen videos, blocked creators (both ways) and anything the viewer clearly pushed away are removed.

**3. Ranking** (`score.go`) scores each candidate:

`tag interest + creator interest + follow + co-engagement + quality + freshness + trending + exploration + small jitter`

- **Quality** is engagement per impression with a Bayesian prior (30 pseudo-views), minus skip rate, so 3 likes on 3 views can't beat 60k likes on 300k.
- **Exploration** is an upper-confidence bonus that starts at 1 for unseen videos and fades with evidence, so new creators get a fair test.

**4. Re-ranking** is greedy, with these rules:
- at most 2 videos per creator per page;
- a penalty for repeating a hashtag within 5 slots;
- **every 5th slot reserved for discovery** outside the viewer's known interests, to avoid filter bubbles.

Three pages are queued per viewer for 15 minutes. A like, share, follow or "not interested" drops the queue, so the very next swipe reflects it.

**Tuning and A/B tests:** set `RANK_WEIGHTS` (JSON) to change the formula. Set `RANK_B_WEIGHTS` and `RANK_B_PERCENT` to run a second arm on a stable share of users. Each item carries `rec_source` and `rec_arm`, and clients echo both on the impression. Prometheus metric `feed_views_total{source,arm,outcome}` gives completion and skip rate per retrieval path and arm. `feed_served_total` and `feed_rank_seconds` cover volume and latency.

**Measured** (local Redis, 20k videos, 400 hashtags, 2k active viewers): full re-rank of 30 items p50 2.5 ms, p95 4.9 ms; a page from the queue p50 0.5 ms.

**Never runs dry.** When a viewer has seen every fresh candidate (small catalogue, heavy viewer), the feed brings back the videos they saw longest ago (`rec_source=repeat`). It never repeats the most recent half of what they saw (at most 50).

**Generic hashtags count for less.** A tag on 15% or fewer of videos counts fully, falling to zero at 60% (#naija, #fyp), like inverse document frequency. Common tags also don't use up retrieval slots.

**Sign-up interest picker.** `GET /v1/feed/topics` suggests popular specific tags, and `POST /v1/feed/me/interests` seeds a new viewer's profile. Each pick counts like a few finished videos and fades normally. The mobile app shows it once after sign-up.

### Offline evaluation (`internal/app/sim_test.go`)

A simulation with 300 viewers who have hidden tastes, and 2,000 videos with hidden quality:
- each viewer watches 12 pages of 10;
- every page comes from the real `ForYou` code;
- every watch, skip and like goes back through the real signal pipeline.

CI runs it whenever svc-feed changes. Average of 3 seeds:

| Policy | Finished | First 3 pages | Taste match | Catalogue reached | Views to top 10% creators |
|---|---|---|---|---|---|
| Random order | 20.0% | 16.8% | 0.13 | 96% | 30% |
| Popularity only | 13.6% | 14.1% | 0.06 | 7% | 54% |
| Lumo (before tuning, 14 Nov) | 18.8% | 14.9% | 0.11 | 29% | 65% |
| **Lumo** | **34.7%** | 24.0% | 0.31 | 77% | 42% |
| **Lumo + interest picker** | **41.9%** | **47.0%** | 0.40 | 100% | 35% |

The simulation found three fixes that took finished views from 18.8% to 34.7%:
1. discounting generic tags;
2. re-ranking after every finished video (before, the next 2 pages were pre-ranked and ignored it);
3. fetching 80 videos per interest instead of 30.

Weights were then tuned on it. To try a candidate weight set, run `LUMO_SIM=1 LUMO_SIM_WEIGHTS='{"tag":2.5}' go test ./internal/app -run Sim -v`.

Simulated viewers are not real ones. Confirm any change with an A/B test (`RANK_B_WEIGHTS`) before trusting it.

**Signed-out viewers** get a shared trending pool, re-ranked every 30 s per pod and sampled so pages differ.

### Following

Fan-out on read: followee IDs come from svc-social (cached 2 min, at most 2,000 followees), then each creator's recent videos are merged.

## API

| Route | Purpose |
|---|---|
| `GET /v1/feed/foryou` | Short-video For You (signed in or not) |
| `GET /v1/feed/watch` | Long-video For You |
| `GET /v1/feed/following?cursor=` | Newest from followed creators |
| `POST /v1/feed/impressions` | `{items:[{video_id, watch_ms, loops, skipped, shared, source, arm, surface}]}`, max 50 per call, 300 per minute per viewer |
| `POST /v1/feed/not-interested` | `{video_id}` |
| `GET /v1/feed/me/interests` | The viewer's top hashtags (for a "Your interests" screen) |
| `DELETE /v1/feed/me/interests/{tag}` | Remove a topic |
| `POST /v1/feed/me/interests` | `{tags:[...]}` from the sign-up picker (1–20) |
| `GET /v1/feed/topics` | Suggested topics for the picker (public, cached 5 min) |
| `POST /v1/feed/me/reset` | "Refresh your For You": forget interests and history (blocks stay) |

## Events

- **Publishes:** `feed.impressions` (keyed by viewer)
- **Consumes:** `media.videos`, `social.events`, `feed.impressions` (group `feed-signals`, batches of 1,000)

## Run locally

```bash
make deps        # once: resolves modules, writes go.sum
cp .env.example .env
go run ./cmd/feed serve     # HTTP API on :8080
go run ./cmd/feed worker    # Kafka consumers + outbox relay
make test         # integration tests (Postgres + Redis from lumo-infra)
```

## Commands

| Command | Runs | Scale on |
|---|---|---|
| `serve` | HTTP API | CPU / requests (HPA) |
| `worker` | Kafka consumers, outbox relay, cron jobs | consumer lag (KEDA) |

Health: `GET /healthz` (process), `GET /readyz` (dependencies; fails first on shutdown so the load balancer drains the pod). Metrics: `GET /metrics` (Prometheus).

## Scaling notes

Redis-bound: use Redis Cluster. Per-viewer keys share a `{userID}` hash tag, so one viewer's profile lives on one slot. Video metadata and stats are cached in-process (10 min / 30 s), because popular videos are candidates for everyone at once. Rebuild after data loss by resetting consumer groups `feed-videos`, `feed-social` and `feed-signals` to the earliest offset (interest profiles rebuild from the last 3 days of `feed.impressions`).

This service has no database, so instead of an outbox its consumers are made idempotent with Redis dedupe keys.

## Config

See `.env.example`. Secrets come from the `lumo-common` and `svc-feed` Kubernetes secrets.
