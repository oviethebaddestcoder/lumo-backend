# lumo-kit

The shared Go library every Lumo service imports (versioned module, tag releases `v0.x.y`).
It holds plumbing, never business logic.

| Package | What |
|---|---|
| `app` | `serve` / `worker` / `migrate` entrypoint, signal handling, errgroup |
| `config` | typed env vars; all missing ones reported at once |
| `logx`, `trace` | JSON logs with request/trace IDs; W3C `traceparent` over HTTP and Kafka |
| `httpx` | server with graceful drain, `/healthz` `/readyz` `/metrics`, strict JSON, errors, internal-only guard |
| `errs` | typed errors that map to HTTP status codes |
| `auth` | Ed25519 JWT issue/verify, JWKS (cached, rotation-aware), middleware, denylist keys |
| `db` | pgx pool tuned for Neon's pooler, transactions with retry, embedded migrations |
| `shard` | 4096 logical shards mapped to physical databases by range |
| `outbox` | transactional outbox + single-leader relay (lease row, pooler-safe), consumer dedupe |
| `kafkax` | idempotent producer; consumer with a goroutine per partition, retries, DLQ, batch mode |
| `redisx` | Redis / Redis Cluster client, distributed rate limiter |
| `userdir` | cached batch lookups of public profiles from svc-identity |
| `cache` | sharded in-process TTL cache |
| `gauth` | Google service-account tokens (Play Developer API, FCM) |
| `s3x` | SigV4 presign + multipart for S3 / R2 / MinIO (checked against AWS's reference signature) |
| `metrics` | Prometheus registry and shared RED / Kafka / outbox metrics |

```bash
make deps   # or: go mod tidy
LUMO_TEST_PG=postgres://postgres@localhost:5432/postgres go test -race ./...
```

Release: `git tag v0.1.1 && git push --tags`, then bump `lumo-kit` in each service.
