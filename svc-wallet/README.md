# svc-wallet

Wallet: coins, gifts, diamonds, purchases, payouts.

Part of the Lumo backend: independent Go services that talk through Kafka.
Shared code comes from the versioned modules `lumo-kit` and `lumo-contracts`;
nothing here imports another service. See `lumo-infra` for the whole picture.

## What it does

A **sharded double-entry ledger**. Users map to one of 4096 logical shards, which map to physical Neon databases by range (`SHARDS`). Every money operation is a single-database transaction.

- **Send gift** (`POST /v1/wallet/gifts/send`, `Idempotency-Key` header required): locks the sender's coin row, checks the balance, writes balanced entries and `wallet.gift.sent` via the outbox, all in one transaction on the sender's shard. A retried request returns the original receipt; it never charges twice.
- **Credit hosts**: the `wallet-credit` consumer reads `wallet.gifts` (keyed by receiver, so one host's gifts are ordered) in batches, dedupes on `gift_send_id`, and applies **one** balance update per host per batch on the host's shard. A host receiving thousands of gifts a second costs one row update per batch, not thousands.
- **Purchases**: Google Play (`obfuscatedAccountId` must match the user), App Store (`appAccountToken` must be the user ID), Paystack for the web wallet. Each receipt is claimed once **globally** (`purchase_claims` on the shard chosen by the receipt), then credited. Refunds/chargebacks reverse coins (balance may go negative).
- **Payouts**: diamonds move to payout clearing with status `pending_review`; finance approves and pays out.
- **Reconciliation** runs nightly: every transaction must sum to zero and cached balances must equal their entries (`wallet_reconcile_problems` metric, alert if > 0).

**Store rules:** coins sold inside the Android app must use Google Play Billing in markets without Google's alternative-billing programs (Nigeria is not in them); iOS uses the App Store. Paystack (card, bank transfer, USSD) is for the web wallet and must not be linked from the app.

## API

`GET /v1/wallet` · `GET /v1/wallet/catalog` · `GET /v1/wallet/history` · `GET /v1/wallet/purchase-tag` · `POST /v1/wallet/gifts/send` · `POST /v1/wallet/purchases/google` · `POST /v1/wallet/purchases/apple` · `POST /v1/wallet/web/checkout` · `POST /v1/wallet/payouts` · webhooks: `/v1/wallet/webhooks/paystack`, `/v1/wallet/webhooks/apple`

## Events

- **Publishes:** `wallet.gifts` (`wallet.gift.sent`), `wallet.events` (`coins.purchased`, `coins.reversed`, `payout.requested`)
- **Consumes:** `wallet.gifts` (credit hosts), `wallet.events` (retry Google consume)

All events are published through the transactional outbox (written in the
same DB transaction as the change), and every consumer is idempotent.

## Run locally

```bash
make deps        # once: resolves modules, writes go.sum
cp .env.example .env
go run ./cmd/wallet migrate
go run ./cmd/wallet serve     # HTTP API on :8080
go run ./cmd/wallet worker    # Kafka consumers + outbox relay
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

Start with one Neon project holding all 4096 logical shards. When it gets hot, branch it, move a range to the new project with logical replication, then change `SHARDS`. No rehashing. The outbox relay runs per shard.

## Config

See `.env.example`. Secrets come from the `lumo-common` and `svc-wallet` Kubernetes secrets.
