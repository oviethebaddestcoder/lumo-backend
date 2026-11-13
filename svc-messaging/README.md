# svc-messaging

One-to-one direct messages.

## What it does

- A conversation has exactly two members. Each member row holds that user's ordering, last preview and unread count, so listing chats is one index scan.
- Sending writes the message, updates both member rows and inserts a `dm.messages` outbox event in **one transaction**.
- After commit the message is pushed live to both users' personal realtime channels (`/v1/rt/me` in svc-realtime) via Redis pub/sub. That push is best effort: apps refetch on reconnect.
- **Idempotent sends:** `client_id` is unique per sender, so a retried send returns the original message.
- **Blocks:** a local projection of social block events stops messages in both directions.
- **Rate limits:** messages per minute and new chats per day (stops spam runs). New chats stay hidden for the recipient until the first message arrives.

## API

`GET|POST /v1/dm/conversations` · `GET|DELETE /v1/dm/conversations/{id}` · `GET|POST /v1/dm/conversations/{id}/messages` · `POST /v1/dm/conversations/{id}/read` · `DELETE /v1/dm/messages/{id}` · `GET /v1/dm/unread`

## Events

- **Publishes:** `dm.messages` (the notifier turns these into "new message" pushes)
- **Consumes:** `social.events` (block projection), `identity.users` (hide chats with deleted accounts)

## Run locally

```bash
make deps
cp .env.example .env
go run ./cmd/messaging migrate
go run ./cmd/messaging serve     # API
go run ./cmd/messaging worker    # outbox relay + consumers
make test                        # needs Postgres 16 + Redis 7
```
