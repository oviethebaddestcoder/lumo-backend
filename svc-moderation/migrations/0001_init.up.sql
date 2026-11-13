CREATE TABLE reports (
  id           uuid PRIMARY KEY,
  reporter_id  text NOT NULL,       -- user ID, or 'system' for automatic flags
  target_type  text NOT NULL,
  target_id    text NOT NULL,
  reason       text NOT NULL,
  details      text NOT NULL DEFAULT '',
  status       text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'actioned', 'dismissed')),
  action_id    uuid,
  resolved_by  uuid,
  resolved_at  timestamptz,
  created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX reports_open ON reports (created_at) WHERE status = 'open';
CREATE INDEX reports_target ON reports (target_type, target_id);

CREATE TABLE actions (
  id           uuid PRIMARY KEY,
  target_type  text NOT NULL,
  target_id    text NOT NULL,
  action       text NOT NULL,
  reason       text NOT NULL,
  actor_id     uuid NOT NULL,
  room_id      text NOT NULL DEFAULT '',
  until        timestamptz,
  created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX actions_target ON actions (target_type, target_id, created_at DESC);

CREATE TABLE outbox (
  id bigserial PRIMARY KEY, topic text NOT NULL, key text NOT NULL, payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE outbox_lease (name text PRIMARY KEY, holder text NOT NULL, expires_at timestamptz NOT NULL);
