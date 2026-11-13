-- Identity service schema. Owned by svc-identity only; no other service reads it.

CREATE TABLE users (
  id               uuid PRIMARY KEY,
  handle           text NOT NULL,
  display_name     text NOT NULL,
  avatar_url       text NOT NULL DEFAULT '',
  bio              text NOT NULL DEFAULT '',
  birth_date       date,
  email            text,
  email_verified   boolean NOT NULL DEFAULT false,
  phone            text,
  password_hash    text,
  roles            text[] NOT NULL DEFAULT '{}',
  verified         boolean NOT NULL DEFAULT false,
  status           text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended', 'deleted')),
  suspended_until  timestamptz,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  deleted_at       timestamptz,
  CONSTRAINT users_handle_format CHECK (handle ~ '^[a-z0-9_.]{2,24}$'),
  CONSTRAINT users_display_name_len CHECK (char_length(display_name) BETWEEN 1 AND 30),
  CONSTRAINT users_bio_len CHECK (char_length(bio) <= 160)
);
CREATE UNIQUE INDEX users_handle_key ON users (handle);
CREATE UNIQUE INDEX users_email_key ON users (lower(email)) WHERE email IS NOT NULL;
CREATE UNIQUE INDEX users_phone_key ON users (phone) WHERE phone IS NOT NULL;

-- One row per way to sign in. Phone and email OTP logins are identities too
-- (provider 'phone' / 'email'), so every login method resolves the same way.
CREATE TABLE identities (
  provider       text NOT NULL,
  subject        text NOT NULL,
  user_id        uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
  email          text,
  created_at     timestamptz NOT NULL DEFAULT now(),
  last_login_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (provider, subject)
);
CREATE INDEX identities_user ON identities (user_id);

-- One row per signed-in device. Refresh tokens are stored as SHA-256 hashes
-- and rotate on every use; presenting an old one revokes the session.
CREATE TABLE sessions (
  id                 uuid PRIMARY KEY,
  user_id            uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
  refresh_hash       bytea NOT NULL,
  prev_refresh_hash  bytea,
  rotated_at         timestamptz,
  method             text NOT NULL,
  device_id          text NOT NULL DEFAULT '',
  device_name        text NOT NULL DEFAULT '',
  platform           text NOT NULL DEFAULT '',
  ip                 text NOT NULL DEFAULT '',
  user_agent         text NOT NULL DEFAULT '',
  created_at         timestamptz NOT NULL DEFAULT now(),
  last_used_at       timestamptz NOT NULL DEFAULT now(),
  expires_at         timestamptz NOT NULL,
  revoked_at         timestamptz,
  revoke_reason      text
);
CREATE UNIQUE INDEX sessions_refresh_hash ON sessions (refresh_hash);
CREATE INDEX sessions_prev_refresh_hash ON sessions (prev_refresh_hash) WHERE prev_refresh_hash IS NOT NULL;
CREATE INDEX sessions_user_active ON sessions (user_id) WHERE revoked_at IS NULL;
CREATE INDEX sessions_expires ON sessions (expires_at);

-- Transactional outbox + consumer dedupe (see lumo-kit/outbox).
CREATE TABLE outbox (
  id bigserial PRIMARY KEY,
  topic text NOT NULL,
  key text NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE outbox_lease (
  name text PRIMARY KEY,
  holder text NOT NULL,
  expires_at timestamptz NOT NULL
);
CREATE TABLE processed_events (
  consumer text NOT NULL,
  event_id text NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
);
CREATE INDEX processed_events_at ON processed_events (processed_at);
