# svc-live

Live: rooms, viewers, PK battles (LiveKit).

Part of the Lumo backend: independent Go services that talk through Kafka.
Shared code comes from the versioned modules `lumo-kit` and `lumo-contracts`;
nothing here imports another service. See `lumo-infra` for the whole picture.

## What it does

Creates live rooms and hands out LiveKit tokens; video never passes through Go.
- **Go live** from the app (WebRTC publish) or OBS (stable RTMP stream key via LiveKit Ingress). One live room per host (unique index).
- **Watch**: small rooms get a WebRTC subscriber token (sub-second latency). Past `HLS_VIEWER_THRESHOLD` viewers, an HLS egress starts and new viewers get the CDN playlist.
- **Viewers** are counted from LiveKit webhooks into Redis (host excluded); counts are pushed to the room at most every 2s.
- **Ranking** for the Live tab: viewers + recent gift coins, per category, in Redis sorted sets.
- **PK battles**: invite → accept → each host gets a publish token for the other room (both cameras in both rooms); gifts add to that side's score; a ticker ends battles and writes the result (`SKIP LOCKED`, safe with many workers).
- **Replays**: optional MP4 egress; `live.replay.ready` lets svc-media import it as a long video.

## API

`GET /v1/live/rooms?category=` · `GET /v1/live/rooms/{id}` · `POST /v1/live/rooms` · `POST /v1/live/rooms/{id}/join` · `POST /v1/live/rooms/{id}/end` · `GET /v1/live/stream-key` · `POST /v1/live/stream-key/rotate` · `POST /v1/live/pk/invite` · `POST /v1/live/pk/{id}/accept|decline` · `GET /v1/live/pk/{id}` · `POST /v1/live/webhooks/livekit`

## Events

- **Publishes:** `live.events` (`room.started`, `room.ended`, `replay.ready`), `live.pk`; Redis pushes to `rt:room:{id}`
- **Consumes:** `wallet.gifts` (room totals, goal bar, PK score), `moderation.actions` (end room, ban host, mute)

All events are published through the transactional outbox (written in the
same DB transaction as the change), and every consumer is idempotent.

## Run locally

```bash
make deps        # once: resolves modules, writes go.sum
cp .env.example .env
go run ./cmd/live migrate
go run ./cmd/live serve     # HTTP API on :8080
go run ./cmd/live worker    # Kafka consumers + outbox relay
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

Postgres load is small (one row per stream). Hot state is in Redis. Media capacity is LiveKit's: run LiveKit nodes in the region closest to your viewers and scale them on bandwidth.

## Config

See `.env.example`. Secrets come from the `lumo-common` and `svc-live` Kubernetes secrets.
