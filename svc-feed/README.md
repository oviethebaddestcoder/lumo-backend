# svc-feed

Feed: For You, Following, Watch.

Part of the Lumo backend: independent Go services that talk through Kafka.
Shared code comes from the versioned modules `lumo-kit` and `lumo-contracts`;
nothing here imports another service. See `lumo-infra` for the whole picture.

## What it does

Owns no database. Its state in Redis is a projection of `media.videos` (compacted, so it can be rebuilt from the start) and `social.events`. **Ranking v1:** hot score (engagement with time decay) blended with fresh uploads (1 in 4 slots) so new creators get seen, jitter so refreshes differ, per-user de-duplication (last 3,000 seen), and at most 2 videos per creator per page. **Following** is fan-out on read: followee IDs from svc-social (cached 2 min), then each creator's recent videos, merged. Swap `rank()` for a learned model later; `feed.impressions` (watch time) is already flowing for training.

## API

`GET /v1/feed/foryou` · `GET /v1/feed/watch` (long videos) · `GET /v1/feed/following?cursor=` · `POST /v1/feed/impressions`

## Events

- **Publishes:** `feed.impressions`
- **Consumes:** `media.videos`, `social.events`

All events are published through the transactional outbox (written in the
same DB transaction as the change), and every consumer is idempotent.

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

Redis-bound: use Redis Cluster. Rebuild after data loss by resetting consumer groups `feed-videos` and `feed-social` to the earliest offset.

## Config

See `.env.example`. Secrets come from the `lumo-common` and `svc-feed` Kubernetes secrets.
