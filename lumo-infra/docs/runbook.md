# On-call runbook

One section per alert in `lumo-infra/k8s/alerts.yaml`. Each one says what the alert means, what to check first, and how to fix it. **page** alerts wake someone up; **ticket** alerts wait for working hours.

The dashboard `Lumo overview` (`lumo-infra/k8s/dashboards.yaml`) has a panel for every alert. Logs are JSON with `trace_id`. Search one trace ID across services to follow a request through the gateway, the service and the Kafka consumers.

---

## Availability

### High5xx
A route is failing more than 2% of requests.
1. Dashboard → *Errors by route*: which service? Did it start right after a deploy (Argo CD history)?
2. Logs: `level=ERROR service=<svc>`. Look for database (`pgx`), Redis or upstream errors.
3. Deploy-related: roll back in Argo CD. Database-related: check Neon status and connection counts (pooled URL, max 10k).

### SlowP99
p99 latency above 1 s for 10 minutes.
1. CPU-saturated pods? Scale the deployment (HPA max) or check KEDA.
2. A slow query: Neon → *Monitoring* → top queries. Every list query is cursor-paginated, so a full scan means a missing index.
3. Feed routes: see FeedRankSlow.

### GatewayUpstreamErrors
The gateway can't reach a service.
1. `kubectl -n lumo get pods -l app=<upstream>`: crash-looping, or zero ready replicas?
2. Readiness failing: `kubectl logs`, then `/readyz` shows which dependency (db, redis, kafka) is down.

### PodCrashLooping
1. `kubectl -n lumo logs <pod> --previous`. A config error at boot names the missing variable (`config: X is required`).
2. OOMKilled: raise memory limits. The transcoder needs about 1 GB per concurrent job.

## Events

### OutboxBacklog
Events written by a service are older than 30 s and still unpublished, so other services can't see them. Gifts don't credit, notifications don't send.
1. Is Kafka healthy? Check broker pods and disk.
2. OutboxNoLeader firing too? Nobody is relaying; see below.
3. The relay logs `outbox publish` errors with the reason.
Nothing is lost: rows stay in the outbox until published, and consumers are idempotent.

### OutboxNoLeader
No worker pod holds the relay lease for a service.
1. Are that service's `worker` pods running? The lease lives in Postgres (`outbox_lease`), so the database must be reachable.
2. Restarting the worker deployment is safe.

### EventsDeadLettered
A consumer gave up on an event after its retries and moved it to `<topic>.dlq`.
1. Read the record: `rpk topic consume <topic>.dlq -n 5`. The headers `dlq_error`, `dlq_group` and `dlq_source` say why.
2. Fix the cause (usually a bug or bad data), then replay: `rpk topic consume <topic>.dlq | rpk topic produce <topic>`. Consumers dedupe by event ID, so replays are safe.

### ConsumerRetrying
A consumer keeps retrying, meaning a dependency (database, Redis, another service) is failing. Find the failing dependency in that service's logs. Lag will grow until it recovers.

### ConsumerLagGrowing
A money or messaging consumer is more than 10k events behind and falling further behind.
1. Workers scale on lag through KEDA, up to the partition count. Are they at max? Then the topic needs more partitions (`lumo-infra/kafka/topics.yaml`; you can add partitions, never remove them).
2. A slow handler: `kafka_event_handle_seconds` by group shows which one.

## Money

### LedgerReconciliationFailed
**Stop and page finance.** The nightly check found a transaction whose legs don't sum to zero, or a cached balance that doesn't match the ledger.
1. Logs: `LEDGER RECONCILIATION FAILED` with the shard and counts.
2. Freeze cash-outs: finance stops approving in `/admin/payouts`.
3. Find the transaction IDs: `SELECT tx_id, sum(amount) FROM entries GROUP BY tx_id HAVING sum(amount) <> 0` on that shard. The ledger is append-only, so fix it with a correcting transaction, never an UPDATE.

### PayoutStuckProcessing
A cash-out was approved and the transfer sent, but Paystack hasn't sent a success or failure webhook for over an hour.
1. Paystack dashboard → Transfers → search the payout ID (it's the transfer reference).
2. If Paystack shows it **successful**: the webhook was lost. Use Paystack's *resend webhook*, or re-run the transfer webhook from Paystack's event log.
3. If it shows **failed**, or doesn't exist: in `/admin/payouts` → *Processing* → **Retry transfer**. The reference is the payout ID and Paystack refuses duplicates, so a retry can never pay twice.
4. Check the Paystack balance: transfers fail when it's empty.

### PayoutReviewBacklog
Cash-outs have waited more than 2 days for finance review. Creators notice this quickly. Review them in `/admin/payouts`.

### PaystackWebhookRejected
Webhooks are failing signature checks.
- Right after a key rotation: `PAYSTACK_SECRET_KEY` must match the key in the Paystack dashboard. Webhooks are signed with that key.
- Otherwise someone is sending forged webhooks. Forged ones are rejected, so no money moves. Check the source IPs in the gateway logs.

### GiftsStopped
Rooms are live but no gifts have gone through in 15 minutes: revenue has stopped.
1. Try sending a gift yourself. What error comes back?
2. `wallet` logs. Database shards up? Is `wallet_reconcile_problems` 0?
3. No coins coming in? Check purchase verification errors (Google Play / App Store service accounts, Paystack status).

## Abuse

### OTPSendSpike
SMS code sends are running at 3× yesterday's rate. This is usually **SMS pumping**: bots request codes to premium-rate numbers and the SMS bill grows.
1. Logs: `otp` sends grouped by number prefix and country. Pumping clusters on a few prefixes, often outside Nigeria.
2. Immediate controls:
   - lower `OTP_PER_IP_PER_HOUR`;
   - at your SMS provider, block the abused country codes and turn on its fraud guard;
   - require email or Google sign-in for the affected prefixes.
3. Normal traffic can also spike after a marketing push or a viral moment, so check before blocking.

### OTPSendFailures
More than 10% of codes fail to send, so people can't sign in. Check the SMS or email provider's status page and API credentials, and confirm the account balance isn't exhausted.

## Product

### FeedCompletionDropped
The share of For You views watched to the end is 20%+ below the same hour yesterday.
1. A ranking change (`RANK_WEIGHTS`, or the B arm)? Compare `feed_views_total` by `arm`, and roll back the env change if one arm dropped.
2. A player or CDN problem: completion collapses for every source at once. Check HLS errors in the app crash reporting and the CDN.
3. By `source`: if only one retrieval path dropped (say `co`), its data went bad.

### FeedRankSlow
Ranking p95 above 50 ms. Redis is the usual cause: check redis_exporter latency and memory. The feed keeps only Redis state, so you can add Redis Cluster shards.

### TranscodeFailing
More than 5% of uploads fail to transcode.
1. `transcode failed` logs carry the ffmpeg error.
2. A spike of one error type often means a new phone or codec. Reproduce with the source file from S3 (`uploads/<user_id>/<video_id>/source`).
3. S3 errors mean checking credentials and bucket policy.

### PushFailing
More than 20% of pushes fail. Check the FCM service account (key rotated?) and the FCM quota. `dead_token` results are normal (uninstalled apps) and aren't counted here.

### RealtimeDroppingClients
WebSocket clients that can't keep up are being disconnected. This happens in very large rooms on slow networks. Chat sampling should kick in (`realtime_chat_sampled_total`). If it doesn't, check realtime pod CPU and scale out.
