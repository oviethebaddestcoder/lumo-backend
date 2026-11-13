CREATE TABLE rooms (
  id                uuid PRIMARY KEY,
  host_id           uuid NOT NULL,
  title             text NOT NULL CHECK (char_length(title) BETWEEN 1 AND 80),
  category          text NOT NULL,
  source            text NOT NULL DEFAULT 'app' CHECK (source IN ('app', 'rtmp')),
  status            text NOT NULL DEFAULT 'live' CHECK (status IN ('live', 'ended')),
  allow_pk          boolean NOT NULL DEFAULT true,
  allow_gifts       boolean NOT NULL DEFAULT true,
  save_replay       boolean NOT NULL DEFAULT false,
  goal_coins        bigint NOT NULL DEFAULT 0,
  goal_label        text NOT NULL DEFAULT '',
  hls_url           text NOT NULL DEFAULT '',
  hls_egress_id     text NOT NULL DEFAULT '',
  replay_egress_id  text NOT NULL DEFAULT '',
  peak_viewers      bigint NOT NULL DEFAULT 0,
  gift_coins        bigint NOT NULL DEFAULT 0,
  started_at        timestamptz NOT NULL DEFAULT now(),
  ended_at          timestamptz,
  end_reason        text
);
-- One live room per host at a time.
CREATE UNIQUE INDEX rooms_one_live_per_host ON rooms (host_id) WHERE status = 'live';
CREATE INDEX rooms_host_history ON rooms (host_id, started_at DESC);
CREATE INDEX rooms_live ON rooms (started_at) WHERE status = 'live';

-- Stable RTMP stream key per creator (OBS is configured once).
CREATE TABLE stream_keys (
  user_id     uuid PRIMARY KEY,
  ingress_id  text NOT NULL,
  rtmp_url    text NOT NULL,
  stream_key  text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE pk_battles (
  id           uuid PRIMARY KEY,
  room_a       uuid NOT NULL REFERENCES rooms (id),
  room_b       uuid NOT NULL REFERENCES rooms (id),
  host_a       uuid NOT NULL,
  host_b       uuid NOT NULL,
  status       text NOT NULL DEFAULT 'invited' CHECK (status IN ('invited', 'active', 'ended', 'declined', 'expired')),
  duration_s   int NOT NULL,
  invited_at   timestamptz NOT NULL DEFAULT now(),
  started_at   timestamptz,
  ends_at      timestamptz,
  score_a      bigint NOT NULL DEFAULT 0,
  score_b      bigint NOT NULL DEFAULT 0,
  winner_room  uuid,
  CHECK (room_a <> room_b)
);
-- A room is in at most one open PK (invited or active), on either side.
CREATE UNIQUE INDEX pk_open_a ON pk_battles (room_a) WHERE status IN ('invited', 'active');
CREATE UNIQUE INDEX pk_open_b ON pk_battles (room_b) WHERE status IN ('invited', 'active');
CREATE INDEX pk_due ON pk_battles (ends_at) WHERE status = 'active';
CREATE INDEX pk_invites ON pk_battles (invited_at) WHERE status = 'invited';

CREATE TABLE outbox (
  id bigserial PRIMARY KEY,
  topic text NOT NULL,
  key text NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE outbox_lease (name text PRIMARY KEY, holder text NOT NULL, expires_at timestamptz NOT NULL);
CREATE TABLE processed_events (
  consumer text NOT NULL, event_id text NOT NULL, processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
);
CREATE INDEX processed_events_at ON processed_events (processed_at);
