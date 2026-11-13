# svc-identity

Identity: sign-in, sessions, tokens, profiles.

Part of the Lumo backend: independent Go services that talk through Kafka.
Shared code comes from the versioned modules `lumo-kit` and `lumo-contracts`;
nothing here imports another service. See `lumo-infra` for the whole picture.

## What it does

Owns every way to sign in and every account. Issues short-lived Ed25519 access tokens (15 min) and rotating refresh tokens (60 days sliding). Every other service verifies tokens locally against this service's JWKS.

**Sign-in methods** (TikTok's set, minus Instagram, which has no consumer login API):

| Method | Endpoint | What the app sends |
|---|---|---|
| Phone OTP | `POST /v1/auth/otp/send` then `/otp/verify` | `channel: "sms"`, phone, 6-digit code |
| Email OTP | same, `channel: "email"` | email, code |
| Password | `POST /v1/auth/password/login` | email / phone / @handle + password |
| Google | `POST /v1/auth/oauth/google` | `id_token` from Google Sign-In |
| Apple | `POST /v1/auth/oauth/apple` | `id_token`, raw `nonce`, `full_name` (first time) |
| Facebook | `POST /v1/auth/oauth/facebook` | `access_token` (or `id_token` from iOS Limited Login) |
| X (Twitter) | `POST /v1/auth/oauth/x` | `code`, `code_verifier`, `redirect_uri` (OAuth 2 PKCE) |
| LINE / Kakao | `POST /v1/auth/oauth/line` / `kakao` | `id_token` |
| QR (web) | `POST /v1/auth/qr`, phone approves `/qr/{id}/approve`, web polls `GET /qr/{id}` | |

**Security choices**
- OTP codes are stored only as HMACs, expire in 5 min, allow 5 guesses; sending is rate limited per number and per IP (SMS bills).
- Refresh tokens are stored as SHA-256 hashes and rotate on every use. Reusing an old one revokes the session (token theft detection); a 30s window absorbs app races.
- Logout, password change, bans and deletion write the session to a Redis denylist the gateway checks on every request, so revocation is instant.
- Accounts are never auto-linked by email (a provider with weaker email checks could take over an account). Users link methods explicitly.
- Passwords: Argon2id. Phone numbers normalized to E.164 (default region NG).
- `DELETE /v1/me` scrubs personal data immediately (NDPA, App Store rule 5.1.1(v)).
- `OTP_TEST_ACCOUNTS` gives store reviewers a fixed code.

## API

`GET /.well-known/jwks.json` · auth endpoints above · `POST /v1/auth/refresh` · `POST /v1/auth/logout` · `POST /v1/auth/logout-all` · `GET|PATCH|DELETE /v1/me` · `PUT /v1/me/password` · `GET /v1/me/sessions` · `DELETE /v1/me/sessions/{id}` · `POST|DELETE /v1/me/identities/{provider}` · `GET /v1/users/{id}` · `GET /v1/users/by-handle/{handle}` · `GET /v1/users?ids=` (batch, used by other services)

## Events

- **Publishes:** `identity.users` (compacted): `identity.user.registered`, `.updated`, `.deleted`
- **Consumes:** `moderation.actions` (ban / unban)

All events are published through the transactional outbox (written in the
same DB transaction as the change), and every consumer is idempotent.

## Run locally

```bash
make deps        # once: resolves modules, writes go.sum
cp .env.example .env
go run ./cmd/identity migrate
go run ./cmd/identity serve     # HTTP API on :8080
go run ./cmd/identity worker    # Kafka consumers + outbox relay
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

Read-heavy (`/v1/users` lookups are cached 30s in-process and by callers). At 200M users the `users` table is ~100 GB with its indexes: fine for one Neon project with read replicas for `GET /v1/users*`. Sessions are pruned 30 days after expiry. If writes outgrow one database, shard by user ID the way svc-wallet does (lumo-kit/shard).

## Config

See `.env.example`. Secrets come from the `lumo-common` and `svc-identity` Kubernetes secrets.
