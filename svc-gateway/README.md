# svc-gateway

Gateway: the only public entry point.

Part of the Lumo backend: independent Go services that talk through Kafka.
Shared code comes from the versioned modules `lumo-kit` and `lumo-contracts`;
nothing here imports another service. See `lumo-infra` for the whole picture.

## What it does

Routes `/v1/<service>/...` to the owning service by longest prefix and passes WebSockets through. Per request it:
- resolves the real client IP (walks `X-Forwarded-For` past trusted proxies, so it can't be spoofed),
- verifies the access token if present and checks the session/ban denylist in Redis (instant logout),
- rate limits per user (signed in) or per IP (anonymous), stricter on `/v1/auth`,
- enforces CORS for the web app, body size limits and security headers.

Services verify tokens again themselves; the gateway's job is to stop bad traffic early. It never routes `/internal/...`.

## API

Everything under `/v1` and `/.well-known` (see `ROUTES`).

## Events

- **Publishes:** none
- **Consumes:** none

All events are published through the transactional outbox (written in the
same DB transaction as the change), and every consumer is idempotent.

## Run locally

```bash
make deps        # once: resolves modules, writes go.sum
cp .env.example .env
go run ./cmd/gateway serve     # HTTP API on :8080
make test         # integration tests (Postgres + Redis from lumo-infra)
```

## Commands

| Command | Runs | Scale on |
|---|---|---|
| `serve` | HTTP API | CPU / requests (HPA) |

Health: `GET /healthz` (process), `GET /readyz` (dependencies; fails first on shutdown so the load balancer drains the pod). Metrics: `GET /metrics` (Prometheus).

## Scaling notes

Stateless; scale on CPU. Put it behind your cloud load balancer / CDN (Cloudflare) with WebSocket support enabled.

## Config

See `.env.example`. Secrets come from the `lumo-common` and `svc-gateway` Kubernetes secrets.
