# lumo-infra: start here

The Lumo backend is a set of **independent Go services** (one Go module and
one container image each, living as folders of the `lumo-backend` repo) that
communicate through **Kafka events**. This folder holds what they share at
runtime: topics, local stack, Kubernetes base, alerts, scripts, docs.

> Services never import each other. They share only `lumo-kit` and
> `lumo-contracts`, wired in with `replace` directives to the sibling folders,
> so any folder can be split back into its own repo later.

## Repos

| Repo | Owns | Data | Talks to others via |
|---|---|---|---|
| `lumo-contracts` | event + WebSocket message schemas | none | imported (versioned) |
| `lumo-kit` | shared plumbing (Kafka, outbox, auth, DB, HTTP) | none | imported (versioned) |
| `svc-gateway` | public entry, auth check, rate limits | Redis | HTTP proxy |
| `svc-identity` | accounts, all sign-in methods, sessions, JWKS | Neon `identity` | `identity.users` |
| `svc-wallet` | coins, gifts, diamonds, purchases, payouts | Neon `wallet` (sharded) | `wallet.gifts`, `wallet.events` |
| `svc-live` | live rooms, viewers, PK battles (LiveKit) | Neon `live` + Redis | `live.events`, `live.pk`, Redis pushes |
| `svc-realtime` | WebSocket chat and room pushes | Redis | `chat.messages` |
| `svc-social` | follows, likes, comments, blocks, reports | Neon `social` | `social.events` |
| `svc-media` | uploads, video lifecycle | Neon `media` + R2/S3 | `media.videos`, `media.events`, `media.jobs` |
| `worker-transcoder` | ffmpeg HLS ladder | R2/S3 | `media.job_results` |
| `svc-feed` | For You / Following / Watch | Redis (projection) | `feed.impressions` |
| `svc-notifier` | push notifications (FCM/APNs), activity inbox | Neon `notifier` | consumes only |
| `svc-messaging` | 1:1 direct messages, read receipts | Neon `messaging` + Redis (live delivery) | `dm.messages` |
| `svc-moderation` | report queue, moderator actions | Neon `moderation` | `moderation.actions` |

No service reads another's database. A service that needs another's data
either keeps a **projection** from that service's events (social keeps video
owners from `media.videos`; feed keeps videos and engagement) or makes a
cached read (`userdir` profile lookups, social's internal follower pages).

## How it fits together

```mermaid
flowchart LR
  app[React Native app] & web[Next.js] --> gw[svc-gateway]
  gw --> id[identity] & wal[wallet] & live[live] & soc[social] & med[media] & feed[feed] & rt[realtime] & noti[notifier] & mod[moderation]
  app <-. WebRTC .-> lk[(LiveKit)]
  app -. multipart upload .-> s3[(R2 / S3)]
  s3 --> cdn[(CDN: HLS)] --> app
  subgraph kafka [Kafka]
    t1[identity.users]:::t; t2[wallet.gifts]:::t; t3[social.events]:::t; t4[media.videos]:::t; t5[media.jobs]:::t; t6[live.events]:::t; t7[moderation.actions]:::t
  end
  id --> t1; wal --> t2; soc --> t3; med --> t4 & t5; live --> t6; mod --> t7
  t2 --> wal & live & rt
  t3 --> soc & feed & noti & mod
  t4 --> feed & soc
  t5 --> tc[worker-transcoder] --> med
  t6 --> med & noti
  t7 --> id & med & soc & live & rt
  live & rt <--> redis[(Redis pub/sub)]
  classDef t fill:#eee,stroke:#999
```

### One gift, end to end

1. App calls `POST /v1/wallet/gifts/send` with an `Idempotency-Key`.
2. **svc-wallet** (sender's shard, one transaction): lock coin balance → check → balanced ledger entries → `gift_sends` row → outbox row. Returns the new balance (~10 ms).
3. The outbox relay publishes `wallet.gift.sent` (keyed by receiver).
4. In parallel, each in its own consumer group:
   - **svc-wallet / wallet-credit** credits the host's diamonds in batches (one row update per host per batch).
   - **svc-live** adds to the room total, goal bar, ranking and PK score, and pushes the score to the room.
   - **svc-realtime** pushes the gift banner to everyone watching.
5. Viewers see the banner within ~100 ms; the host's diamond balance updates within ~1 s.

Any step can crash and restart: the outbox guarantees the event exists if the
debit committed, consumers dedupe on IDs, and Kafka redelivers anything not
yet acknowledged.

## Engineering rules (all services)

- **Outbox, always.** Events are written in the same DB transaction as the change; a relay publishes them. No dual writes.
- **Idempotent consumers.** At-least-once delivery is assumed everywhere. Dedupe on `event_id` or a natural key.
- **Retries, then DLQ.** A failing event is retried with backoff, then sent to `<topic>.dlq` with the error. A partition never stalls on one bad message.
- **Order by key.** Each topic is keyed so events about one entity stay ordered; parallelism comes from partitions (one goroutine per partition in `kafkax`).
- **Money is a ledger.** Balanced double-entry, idempotency keys, nightly reconciliation.
- **Services verify tokens themselves** (Ed25519 JWT via JWKS); the gateway adds revocation and rate limits.
- **Graceful everything.** `/readyz` fails first on SIGTERM, in-flight work finishes, consumers commit before giving up partitions.

## Getting started

```bash
# 1. Clone, then resolve modules and write go.sum in every module (commit the go.sum files)
git clone https://github.com/oviethebaddestcoder/lumo-backend && cd lumo-backend
for d in lumo-contracts lumo-kit svc-* worker-*; do (cd $d && go mod tidy); done

# 4. Local stack + topics
docker compose -f lumo-infra/local/docker-compose.yml up -d
KAFKA_BROKERS=localhost:19092 RF=1 lumo-infra/scripts/create-topics.sh

# 5. Secrets, then run services (each in its own terminal / IDE run config)
lumo-infra/scripts/gen-secrets.sh > .secrets.env
cd svc-identity && cp .env.example .env   # paste secrets, set DATABASE_URL=postgres://postgres:postgres@localhost:5432/identity
go run ./cmd/identity migrate && go run ./cmd/identity serve
```

Working on several repos at once locally? Create a `go.work` **outside** the
repos (`go work init ./lumo-kit ./svc-wallet ...`) so local edits to lumo-kit
are picked up without tagging. Don't commit it.

**Tests:** every repo runs `make test` against Postgres 16 + Redis 7. Kafka
code is tested with franz-go's in-memory cluster. CI does the same with
service containers.

## Neon

- One Neon project per database-owning service (`scripts/neon-setup.sh`), so one service's load or migration never touches another's.
- Apps use the **pooled** connection string (PgBouncer, transaction mode); `lumo-kit/db` detects `-pooler` hosts and avoids server-side prepared statements, session advisory locks and LISTEN/NOTIFY, which don't survive transaction pooling. The outbox picks its leader with a lease row for the same reason.
- Migrations use the **direct** string (`DATABASE_URL_DIRECT`), run as a Kubernetes Job before each rollout.
- Add read replicas for read-heavy services (identity profile lookups, social lists).
- Turn off scale-to-zero on production computes (the first query after a cold start is slow), and set autoscaling limits per project.
- Pick the Neon region (and Kafka, Redis, Kubernetes) by measuring latency from your users. For Nigeria, compare the nearest European and South African regions.

## Production checklist

| Piece | Recommended | Notes |
|---|---|---|
| Kubernetes | managed (EKS / GKE) | 3 zones; KEDA + metrics-server; Argo CD (`k8s/argocd-apps.yaml`) |
| Kafka | managed (Confluent Cloud, Aiven, MSK, Redpanda Cloud) | SASL/SCRAM + TLS; RF 3, `min.insync.replicas` 2; `scripts/create-topics.sh` |
| Redis | managed Redis Cluster | gateway rate limits/denylist, live state, feed, pub/sub |
| Postgres | Neon, one project per service | wallet sharded by range from day one (one project to start) |
| Video | LiveKit Cloud or self-hosted LiveKit near users | egress writes HLS/MP4 to R2 |
| Storage + CDN | Cloudflare R2 + Cloudflare CDN | no egress fees; long cache on `hls/` |
| Secrets | External Secrets Operator | `lumo-common` + one secret per service |
| Observability | Prometheus + Grafana + Loki | `k8s/alerts.yaml`; add Kafka lag exporter |
| SMS | Termii (Nigeria) + Twilio fallback | rate limits already in identity |
| Push | Firebase Cloud Messaging | |

## Scaling toward 200M users

Everything is horizontally scalable, and the known hot spots are handled in
the design. It has **not** been load-tested at that size: that is the next
step (k6 scripts per endpoint, plus a Kafka replay test) before launch.

| Pressure | Where | Already handled | When it bites, do |
|---|---|---|---|
| Request volume | all APIs | stateless pods, HPA on CPU, per-user/IP rate limits | more pods; cache public GETs at the CDN |
| Event volume | consumers | one goroutine per partition, batch consumers, KEDA on lag | raise partitions (48 → 96) on hot topics |
| Celebrity counters | likes, followers, gifts to one host | batched counter consumers (one UPDATE per key per batch) | larger batches |
| Money writes | wallet | 4096 logical shards over N Neon projects | move a shard range to a new project |
| Big tables | follows, likes | hash partitions (16) | more partitions, or shard social like wallet |
| Huge live rooms | realtime, video | HLS via CDN past a threshold, chat sampling, slow-client drops | more realtime pods; more LiveKit nodes |
| "Went live" pushes | notifier | paged fan-out, bounded concurrency, separate consumer group | FCM topic messaging per creator |
| Feed reads | feed | Redis only, no DB, bounded ranking sets | Redis Cluster shards; precomputed per-user candidates |
| Token checks | every request | local Ed25519 verify, cached JWKS | nothing: it's CPU-cheap |

## Known gaps (next work)

- **Internal calls are HTTP/JSON, not gRPC.** The plan doc said gRPC. There are only two synchronous internal reads (profiles, follower pages), and events carry everything else, so I kept the build simpler. Add gRPC if more synchronous calls appear.
- **Events are JSON with a versioned envelope.** Move to Protobuf + a schema registry when several teams publish to the same topics.
- Tracing propagates W3C `traceparent` through HTTP, Kafka and logs; plug in the OpenTelemetry exporter for a trace UI.
- Payout bank transfers and KYC (BVN/NIN) need a provider integration; payouts stop at `pending_review`.
- Moderation scanning is a keyword list; connect an ML vendor for images, video and text.
- The admin console, Creator Studio and web wallet UIs live in the Next.js app.
- Search (users, hashtags) and DMs are not built yet.
