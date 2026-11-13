-- In-app activity inbox (the bell). Personal events only; "X is live" stays
-- push-only because it fans out to every follower.
CREATE TABLE IF NOT EXISTS inbox (
  id            bigserial PRIMARY KEY,
  user_id       uuid NOT NULL,
  kind          text NOT NULL,            -- follow | comment | likes | payout
  actor_id      uuid,
  actor_name    text NOT NULL DEFAULT '',
  actor_avatar  text NOT NULL DEFAULT '',
  title         text NOT NULL,
  body          text NOT NULL DEFAULT '',
  data          jsonb NOT NULL DEFAULT '{}',
  created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS inbox_user ON inbox (user_id, id DESC);
CREATE INDEX IF NOT EXISTS inbox_age ON inbox (created_at);
CREATE TABLE IF NOT EXISTS inbox_reads (
  user_id       uuid PRIMARY KEY,
  last_read_id  bigint NOT NULL DEFAULT 0
);
