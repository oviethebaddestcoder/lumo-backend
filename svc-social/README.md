# svc-social

Social: follows, likes, comments, blocks, reports.

Part of the Lumo backend: independent Go services that talk through Kafka.
Shared code comes from the versioned modules `lumo-kit` and `lumo-contracts`;
nothing here imports another service. See `lumo-infra` for the whole picture.

## What it does

Edge tables (`follows`, `followers`, `likes`) are hash-partitioned (16 ways) so each partition stays small at billions of rows. Writes insert the edge and an outbox event in one transaction. **Counters are not updated inline**: the `social-counts` consumer reads this service's own `social.events` and applies one `UPDATE` per user/video per batch, so a viral video's counter row never becomes a global lock. Video ownership comes from a local projection of `media.videos`. Blocking removes follows both ways and stops comments.

## API

`POST|DELETE /v1/social/follows/{user_id}` · `GET /v1/social/users/{id}/followers|following|counts|relationship` · `POST|DELETE /v1/social/videos/{id}/like` · `GET /v1/social/videos/counts?ids=` · `GET|POST /v1/social/videos/{id}/comments` · `DELETE /v1/social/comments/{id}` · `POST|DELETE /v1/social/blocks/{user_id}` · `POST /v1/social/reports` · internal: `/internal/social/users/{id}/follower-ids`, `/following-ids`

## Events

- **Publishes:** `social.events`
- **Consumes:** `social.events` (counters), `media.videos` (projection), `moderation.actions` (remove comments)

All events are published through the transactional outbox (written in the
same DB transaction as the change), and every consumer is idempotent.

## Run locally

```bash
make deps        # once: resolves modules, writes go.sum
cp .env.example .env
go run ./cmd/social migrate
go run ./cmd/social serve     # HTTP API on :8080
go run ./cmd/social worker    # Kafka consumers + outbox relay
make test         # integration tests (Postgres + Redis from lumo-infra)
```

## Commands

| Command | Runs | Scale on |
|---|---|---|
| `serve` | HTTP API | CPU / requests (HPA) |
| `worker` | Kafka consumers, outbox relay, cron jobs | consumer lag (KEDA) |
| `migrate` | DB migrations (Kubernetes Job, PreSync) | n/a |

Health: `GET /healthz` (process), `GET /readyz` (dependencies; fails first on shutdown so the load balancer drains the pod). Metrics: `GET /metrics` (Prometheus).

## Scaling notes

Largest table is `follows` (+ its mirror `followers`). If one Neon project can't hold it, move to more partitions or shard by user ID with lumo-kit/shard.

## Config

See `.env.example`. Secrets come from the `lumo-common` and `svc-social` Kubernetes secrets.
