# worker-transcoder

Transcoder: HLS ladder with ffmpeg.

Part of the Lumo backend: independent Go services that talk through Kafka.
Shared code comes from the versioned modules `lumo-kit` and `lumo-contracts`;
nothing here imports another service. See `lumo-infra` for the whole picture.

## What it does

Consumes `media.jobs`, reads the source straight from object storage (presigned URL), and builds adaptive HLS: 360p (data-saver default), 540p, 720p and 1080p, never upscaling, portrait-aware, 4s segments, plus a thumbnail. Uploads with 16 parallel PUTs and reports on `media.job_results`. Output paths come from the video ID, so a repeated job just overwrites. `MAX_JOBS` bounds ffmpeg processes per pod; on shutdown the job is cancelled and another pod redoes it.

## API

none (worker only)

## Events

- **Publishes:** `media.job_results`
- **Consumes:** `media.jobs`

All events are published through the transactional outbox (written in the
same DB transaction as the change), and every consumer is idempotent.

## Run locally

```bash
make deps        # once: resolves modules, writes go.sum
cp .env.example .env
go run ./cmd/transcoder worker    # Kafka consumers + outbox relay
make test         # integration tests (Postgres + Redis from lumo-infra)
```

## Commands

| Command | Runs | Scale on |
|---|---|---|
| `worker` | Kafka consumers, outbox relay, cron jobs | consumer lag (KEDA) |

Health: `GET /healthz` (process), `GET /readyz` (dependencies; fails first on shutdown so the load balancer drains the pod). Metrics: `GET /metrics` (Prometheus).

## Scaling notes

CPU-bound. Scale on `media.jobs` lag (KEDA manifest included); give pods 4+ cores and fast scratch disk. For very high volume, switch the encoder to GPU (NVENC) nodes.

## Config

See `.env.example`. Secrets come from the `lumo-common` and `worker-transcoder` Kubernetes secrets.
