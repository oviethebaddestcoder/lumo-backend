# svc-moderation

Moderation: report queue and actions.

Part of the Lumo backend: independent Go services that talk through Kafka.
Shared code comes from the versioned modules `lumo-kit` and `lumo-contracts`;
nothing here imports another service. See `lumo-infra` for the whole picture.

## What it does

Reports arrive as events (users via svc-social; automatic flags from the chat scanner). Moderators (role `moderator` in identity's `users.roles`) work a queue ordered by report count. An action is written with a `moderation.actions` event in one transaction, and each owning service applies it: identity bans and revokes sessions, media removes videos, social deletes comments, live ends rooms, realtime kicks from chat. The chat scanner is a keyword list today; plug an ML classifier (text, image, video) into `OnChat` and a video scanner on `media.events`.

## API

`GET /v1/moderation/queue` · `POST /v1/moderation/actions` · `POST /v1/moderation/targets/{type}/{id}/dismiss` · `GET /v1/moderation/targets/{type}/{id}/history`

## Events

- **Publishes:** `moderation.actions`
- **Consumes:** `social.events` (reports), `chat.messages`

All events are published through the transactional outbox (written in the
same DB transaction as the change), and every consumer is idempotent.

## Run locally

```bash
make deps        # once: resolves modules, writes go.sum
cp .env.example .env
go run ./cmd/moderation migrate
go run ./cmd/moderation serve     # HTTP API on :8080
go run ./cmd/moderation worker    # Kafka consumers + outbox relay
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

Small. Grow the team and tooling (the admin UI lives in the Next.js app) before the service.

## Config

See `.env.example`. Secrets come from the `lumo-common` and `svc-moderation` Kubernetes secrets.
