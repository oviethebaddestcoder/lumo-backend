# lumo-backend

The Go microservices behind Lumo: short video, live streaming with PK battles, gifting and long-form Watch.
The mobile app is in [`lumo-mobile`](https://github.com/oviethebaddestcoder/lumo-mobile).

This is one Git repo, but the **services are still independent**:
- every folder is its own Go module with its own `Dockerfile`, Kubernetes manifests, database and deploy;
- services never import each other;
- services communicate only through Kafka events and the gateway;
- they share just two internal modules, `lumo-kit` (platform code) and `lumo-contracts` (event and realtime schemas), wired in with `replace` directives to the sibling folders.

Any folder can later be moved into its own repo.

| Folder | What it owns |
|---|---|
| `svc-gateway` | Public edge: JWT check, rate limits, routing, WebSocket proxy |
| `svc-identity` | Accounts; OTP, password and Google/Apple/Facebook/X/LINE/Kakao sign-in; QR login; sessions; people search |
| `svc-wallet` | Coins and diamonds: double-entry ledger, gifts, store receipts, levels, backpack and daily check-in, bank cash-outs via Paystack with finance approval (sharded Neon) |
| `svc-live` | Live rooms, LiveKit tokens, RTMP ingress, HLS egress, PK battles |
| `svc-realtime` | Room WebSockets (chat, gifts, viewers, PK scores) and each user's personal channel (DMs), with Redis fan-out |
| `svc-social` | Follows, likes (and likes received per creator), comments, blocks, reports |
| `svc-media` | Video uploads (S3 multipart), metadata, view counts, hashtag pages, profile photo uploads |
| `worker-transcoder` | ffmpeg HLS ladder |
| `svc-feed` | For You, Following and Watch feeds |
| `svc-notifier` | Push through FCM, notification preferences, the in-app activity inbox |
| `svc-messaging` | Direct messages: chats, read receipts, unsend, block-aware, live delivery on `/v1/rt/me` |
| `svc-moderation` | Report queue and enforcement |
| `lumo-kit`, `lumo-contracts` | Shared platform code and event schemas |
| `lumo-infra` | Kafka topics, local docker-compose, Kubernetes base, scripts, **architecture docs** |

## Getting started

```bash
# 1. Resolve modules and write go.sum in every module (commit the go.sum files afterwards)
for d in lumo-contracts lumo-kit svc-* worker-*; do (cd $d && go mod tidy); done

# 2. Local Postgres, Redis, Kafka and LiveKit, then create the topics
docker compose -f lumo-infra/local/docker-compose.yml up -d
KAFKA_BROKERS=localhost:19092 RF=1 lumo-infra/scripts/create-topics.sh

# 3. Tests for one service (they need Postgres 16 and Redis 7 running)
cd svc-wallet && make test

# 4. Build an image (always from the repo root)
docker build -f svc-gateway/Dockerfile -t svc-gateway .
```

Read **`lumo-infra/README.md`** next. It covers:
- the architecture and event flow;
- Neon setup (pooled vs direct URLs, one project per service);
- secrets;
- the production checklist and the scaling plan.

## CI

`.github/workflows/ci.yml` tests only the modules a push changes. A change to `lumo-kit` or `lumo-contracts` tests everything.

On `main`, CI also builds and pushes `ghcr.io/oviethebaddestcoder/<service>:<sha>` for every changed service.

Requirements: Go 1.26, Docker, Postgres 16 and Redis 7 for tests.
