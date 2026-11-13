CREATE TABLE videos (
  id             uuid PRIMARY KEY,
  owner_id       uuid NOT NULL,
  kind           text NOT NULL CHECK (kind IN ('short', 'long', 'replay')),
  status         text NOT NULL CHECK (status IN ('uploading', 'processing', 'ready', 'failed', 'removed')),
  visibility     text NOT NULL DEFAULT 'public' CHECK (visibility IN ('public', 'followers', 'private')),
  title          text NOT NULL DEFAULT '',
  description    text NOT NULL DEFAULT '',
  hashtags       text[] NOT NULL DEFAULT '{}',
  source_key     text NOT NULL,
  hls_url        text NOT NULL DEFAULT '',
  thumbnail_url  text NOT NULL DEFAULT '',
  duration_ms    bigint NOT NULL DEFAULT 0,
  width          int NOT NULL DEFAULT 0,
  height         int NOT NULL DEFAULT 0,
  source_ref     text,            -- e.g. live room ID for replays (dedupe)
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  published_at   timestamptz
);
CREATE INDEX videos_owner ON videos (owner_id, kind, id DESC) WHERE status = 'ready';
CREATE UNIQUE INDEX videos_source_ref ON videos (source_ref) WHERE source_ref IS NOT NULL;
CREATE INDEX videos_stuck ON videos (updated_at) WHERE status IN ('uploading', 'processing');

CREATE TABLE uploads (
  video_id    uuid PRIMARY KEY REFERENCES videos (id),
  upload_id   text NOT NULL,
  object_key  text NOT NULL,
  part_size   bigint NOT NULL,
  parts       int NOT NULL,
  expires_at  timestamptz NOT NULL,
  completed   boolean NOT NULL DEFAULT false
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
