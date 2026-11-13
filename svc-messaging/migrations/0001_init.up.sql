-- Direct messages (1:1). A conversation has exactly two members; each member
-- row carries that user's inbox ordering and unread count, so listing a
-- user's conversations is one index range scan.

CREATE TABLE conversations (
  id          uuid PRIMARY KEY,
  user_low    uuid NOT NULL,     -- the smaller user ID
  user_high   uuid NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  CHECK (user_low < user_high),
  UNIQUE (user_low, user_high)
);

CREATE TABLE members (
  user_id          uuid NOT NULL,
  conversation_id  uuid NOT NULL REFERENCES conversations (id),
  peer_id          uuid NOT NULL,
  last_message_at  timestamptz,
  last_preview     text NOT NULL DEFAULT '',
  last_sender_id   uuid,
  unread           int NOT NULL DEFAULT 0 CHECK (unread >= 0),
  read_at          timestamptz,
  hidden           boolean NOT NULL DEFAULT false,
  PRIMARY KEY (user_id, conversation_id)
);
CREATE INDEX members_inbox ON members (user_id, last_message_at DESC NULLS LAST) WHERE NOT hidden;

-- Messages are time-ordered by their UUIDv7 ID.
CREATE TABLE messages (
  id               uuid PRIMARY KEY,
  conversation_id  uuid NOT NULL,
  sender_id        uuid NOT NULL,
  client_id        text NOT NULL,
  text             text NOT NULL CHECK (char_length(text) BETWEEN 1 AND 1000),
  created_at       timestamptz NOT NULL DEFAULT now(),
  deleted_at       timestamptz
);
CREATE INDEX messages_conv ON messages (conversation_id, id DESC);
-- A retried send (same client_id) never duplicates.
CREATE UNIQUE INDEX messages_client ON messages (sender_id, client_id);

-- Projection of social blocks (either direction stops messaging).
CREATE TABLE blocks (
  blocker_id  uuid NOT NULL,
  blocked_id  uuid NOT NULL,
  PRIMARY KEY (blocker_id, blocked_id)
);

CREATE TABLE outbox (
  id bigserial PRIMARY KEY, topic text NOT NULL, key text NOT NULL, payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE outbox_lease (name text PRIMARY KEY, holder text NOT NULL, expires_at timestamptz NOT NULL);
CREATE TABLE processed_events (
  consumer text NOT NULL, event_id text NOT NULL, processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
);
CREATE INDEX processed_events_at ON processed_events (processed_at);
