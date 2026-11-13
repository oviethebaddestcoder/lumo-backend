# svc-notifier

Notifier: push notifications.

Part of the Lumo backend: independent Go services that talk through Kafka.
Shared code comes from the versioned modules `lumo-kit` and `lumo-contracts`;
nothing here imports another service. See `lumo-infra` for the whole picture.

## What it does

Device registry plus pushes through Firebase Cloud Messaging (which also delivers to iOS via APNs). Pushes: new follower (once per pair per day), comments, like milestones (1, 10, 50, 100, ...), and "X is live" to every follower. The live fan-out pages followers from svc-social 1,000 at a time and sends with 64 concurrent workers, in its own consumer group so a creator with millions of followers never delays other notifications. Dead tokens are deleted; user preferences are respected; redeliveries never double-push.

## API

`PUT /v1/notify/devices` · `DELETE /v1/notify/devices/{token}` · `GET|PUT /v1/notify/prefs`

## Events

- **Publishes:** none
- **Consumes:** `social.events`, `live.events`, `identity.users` (deleted accounts)

All events are published through the transactional outbox (written in the
same DB transaction as the change), and every consumer is idempotent.

## Run locally

```bash
make deps        # once: resolves modules, writes go.sum
cp .env.example .env
go run ./cmd/notifier migrate
go run ./cmd/notifier serve     # HTTP API on :8080
go run ./cmd/notifier worker    # Kafka consumers + outbox relay
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

Bound by FCM quotas. For creators with millions of followers, move to FCM topic messaging (subscribe on follow) to send one request per live start.

## Config

See `.env.example`. Secrets come from the `lumo-common` and `svc-notifier` Kubernetes secrets.
