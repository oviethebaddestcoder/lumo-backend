# svc-realtime

Realtime: WebSocket chat and room pushes.

Part of the Lumo backend: independent Go services that talk through Kafka.
Shared code comes from the versioned modules `lumo-kit` and `lumo-contracts`;
nothing here imports another service. See `lumo-infra` for the whole picture.

## What it does

Holds WebSocket connections (`GET /v1/rt/rooms/{id}`). **Concurrency model per pod:**
- a reader and a writer goroutine per connection; the writer drains a buffered channel,
- one Redis pub/sub connection per pod, subscribed only to rooms that have local viewers,
- one dispatcher goroutine that copies each message into clients' channels without blocking; a client that can't keep up is disconnected instead of slowing the room.

Any service pushes to a room by publishing JSON (`lumo-contracts/realtime.Message`) to `rt:room:{id}`. Chat is rate limited per user (1/s), filtered, checked against the room mute list, and **sampled** in huge rooms (past `ROOM_CHAT_PER_SEC` per pod the sender still sees their message, but it isn't broadcast). Chat copies go to Kafka for moderation. On shutdown clients are closed with "going away" so apps reconnect to another pod.

## API

`GET /v1/rt/rooms/{id}` (WebSocket; token in `Authorization` or `?access_token=`). Client → server: `{"type":"chat","text":"..."}`. Server → client types: `chat`, `gift`, `viewers`, `goal`, `pk_started`, `pk_score`, `pk_ended`, `room_ended`, `kick`, `system`, `error`.

## Events

- **Publishes:** `chat.messages`
- **Consumes:** `wallet.gifts` (gift banners), `moderation.actions` (kick)

All events are published through the transactional outbox (written in the
same DB transaction as the change), and every consumer is idempotent.

## Run locally

```bash
make deps        # once: resolves modules, writes go.sum
cp .env.example .env
go run ./cmd/realtime serve     # HTTP API on :8080
go run ./cmd/realtime worker    # Kafka consumers + outbox relay
make test         # integration tests (Postgres + Redis from lumo-infra)
```

## Commands

| Command | Runs | Scale on |
|---|---|---|
| `serve` | HTTP API | CPU / requests (HPA) |
| `worker` | Kafka consumers, outbox relay, cron jobs | consumer lag (KEDA) |

Health: `GET /healthz` (process), `GET /readyz` (dependencies; fails first on shutdown so the load balancer drains the pod). Metrics: `GET /metrics` (Prometheus).

## Scaling notes

Scale `serve` pods on `realtime_connections`. Raise the pod's file-descriptor limit. Redis pub/sub fan-out works into the millions of concurrent viewers across pods; for very large rooms prefer HLS + chat sampling (already on).

## Config

See `.env.example`. Secrets come from the `lumo-common` and `svc-realtime` Kubernetes secrets.
