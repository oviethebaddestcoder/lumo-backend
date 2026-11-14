# End-to-end and load tests

Runs **every Lumo service together** (gateway, identity, wallet, live, realtime, social, media, transcoder, feed, notifier, messaging, moderation). The 22 processes are each service's `serve` and `worker`, the transcoder and the gateway. They use real Postgres and Redis. Kafka, S3, Paystack and the LiveKit API are in-memory fakes from `fakes/`, so no Docker is needed.

```bash
lumo-infra/e2e/run.sh                 # build, start, run the journeys, stop
KEEP=1 lumo-infra/e2e/run.sh          # leave it running (logs in /tmp/lumo-e2e/logs)
ONLY_UP=1 lumo-infra/e2e/run.sh       # start only, then:
(cd lumo-infra/e2e && go run ./load -vus 50 -duration 60s)
```

Needs Go, Postgres (superuser URL in `PG_URL`, default `postgres://postgres:postgres@localhost:5432`), Redis (`E2E_REDIS_URL`, default db 5), ffmpeg, openssl, psql and redis-cli. CI runs it on every push (`e2e` job). Each run starts from fresh databases.

## Journeys (`journey/`)

Every step talks to the gateway exactly as the apps do, then waits for the effects in other services:

1. Five accounts sign up with email codes; finance and moderator roles take effect on token refresh.
2. The gateway refuses missing or forged tokens, internal routes, encoded-slash paths, and admin routes without the role.
3. A creator uploads a real MP4 in multipart. It is transcoded by ffmpeg to HLS, and the playlist is served.
4. The video reaches For You for a signed-in viewer (with `rec_source` and `rec_arm`) and the signed-out pool.
5. Follow, like, comment and a watched view lead to:
   - creator counts;
   - the view count;
   - the creator's activity inbox;
   - the Following feed;
   - the viewer's learned interests.
6. Web coin top-up:
   - a forged webhook is refused;
   - a webhook before payment credits nothing;
   - a paid webhook delivered 3 times credits once.
   Then a gift sent twice with one Idempotency-Key is charged once, and the creator gets diamonds across wallet shards.
7. Bank cash-out:
   - finance sees it in the queue with the right amount and bank label, and approves it;
   - a Paystack transfer goes out and its webhook marks it paid;
   - a push lands in the inbox;
   - after a bank-account change, a new cash-out is refused for 48 hours.
8. Direct messages:
   - a retried send creates one message;
   - unread counts update when read;
   - a stranger can't read the conversation.
9. A block removes the creator from the stranger's feed and stops DMs.
10. A report reaches the moderation queue; removing the video takes it off every feed.
11. Live room: create, list, viewer joins, gifts update the room total and host diamonds, only the host can end it.
12. Account deletion revokes tokens and removes the public profile.

Bugs this caught on its first runs (all fixed):
- Following didn't show a new follow for 2 minutes.
- No coin pack had a web price.
- The payout-rate default paid creators about 1.25%.
- The per-IP sign-in code limit was too tight for shared carrier IPs.

## Load (`load/`)

Setup signs in N accounts, tops each one up through the real checkout and webhook, and seeds 2,000 videos onto `media.videos`. The traffic mix is then:

| Share | Request |
|---|---|
| 55% | For You pages |
| 20% | watch reports |
| 10% | likes |
| 5% | `GET /v1/me` |
| 5% | `GET /v1/wallet` |
| 5% | gifts |

**Measured 14 Nov 2026.** Setup: 2 CPU cores and 7 GB shared by all 22 processes, Postgres, Redis and the load generator; 50 virtual users for 45 s.

| Route | req/s | p50 | p95 | p99 | Errors |
|---|---|---|---|---|---|
| GET /v1/feed/foryou | 439 | 83 ms | 151 ms | 191 ms | 0 |
| POST /v1/feed/impressions | 156 | 42 ms | 86 ms | 113 ms | 0 |
| POST /v1/social/videos/{id}/like | 83 | 21 ms | 55 ms | 76 ms | 0 |
| POST /v1/wallet/gifts/send | 40 | 27 ms | 70 ms | 92 ms | 0 |
| GET /v1/me | 39 | 20 ms | 54 ms | 79 ms | 0 |
| GET /v1/wallet | 40 | 19 ms | 51 ms | 70 ms | 0 |

**Total: ~800 req/s with 0 errors, roughly 400 req/s per core for the whole platform.** The latency is CPU queueing on a saturated box. Profiling shows For You costs about 1 ms of work, mostly Redis round trips.

These numbers are a floor, not production capacity. In production each service runs on its own nodes and scales out horizontally (stateless pods, Kafka partitions, sharded wallet, Redis Cluster). Run this against a staging cluster sized like production before launch.
