# svc-media

Media: uploads and the video lifecycle.

Part of the Lumo backend: independent Go services that talk through Kafka.
Shared code comes from the versioned modules `lumo-kit` and `lumo-contracts`;
nothing here imports another service. See `lumo-infra` for the whole picture.

## What it does

Video bytes never pass through Go. The app gets presigned multipart URLs (16 MiB parts, resumable), uploads straight to S3/R2, then calls complete. That queues a transcode job; worker-transcoder reports back; the video becomes `ready` and its full state goes out on the compacted `media.videos` topic. Feed, social and others read that instead of calling this service. Abandoned uploads are aborted after 24h. Live replays are imported once per room. Image uploads (avatars, thumbnails) get a presigned PUT with `Content-Length` and `Content-Type` signed in, so S3 itself refuses a file bigger than the approved size (max 5 MB) or of another type. The bucket CORS must allow the `Content-Type` header.

## API

`POST /v1/media/uploads` · `POST /v1/media/uploads/{id}/parts` · `POST /v1/media/uploads/{id}/complete` · `GET|PATCH|DELETE /v1/media/videos/{id}` · `GET /v1/media/users/{id}/videos?kind=`

## Events

- **Publishes:** `media.videos` (compacted state), `media.events`, `media.jobs`
- **Consumes:** `media.job_results`, `live.events` (replays), `moderation.actions`

All events are published through the transactional outbox (written in the
same DB transaction as the change), and every consumer is idempotent.

## Run locally

```bash
make deps        # once: resolves modules, writes go.sum
cp .env.example .env
go run ./cmd/media migrate
go run ./cmd/media serve     # HTTP API on :8080
go run ./cmd/media worker    # Kafka consumers + outbox relay
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

Metadata only; small. Storage and CDN carry the load: serve `hls/` through your CDN with long cache headers (segments are immutable).

## Config

See `.env.example`. Secrets come from the `lumo-common` and `svc-media` Kubernetes secrets.
