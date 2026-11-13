-- Social graph and engagement. Edge tables are hash-partitioned so each
-- partition stays small enough to vacuum and index quickly at billions of rows.

CREATE TABLE follows (
  follower_id  uuid NOT NULL,
  followee_id  uuid NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (follower_id, followee_id)
) PARTITION BY HASH (follower_id);

-- Same edges keyed the other way, for "followers of X" lists.
CREATE TABLE followers (
  followee_id  uuid NOT NULL,
  follower_id  uuid NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (followee_id, follower_id)
) PARTITION BY HASH (followee_id);

CREATE TABLE likes (
  video_id    uuid NOT NULL,
  user_id     uuid NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (video_id, user_id)
) PARTITION BY HASH (video_id);

DO $$
BEGIN
  FOR i IN 0..15 LOOP
    EXECUTE format('CREATE TABLE follows_p%s PARTITION OF follows FOR VALUES WITH (MODULUS 16, REMAINDER %s)', i, i);
    EXECUTE format('CREATE TABLE followers_p%s PARTITION OF followers FOR VALUES WITH (MODULUS 16, REMAINDER %s)', i, i);
    EXECUTE format('CREATE TABLE likes_p%s PARTITION OF likes FOR VALUES WITH (MODULUS 16, REMAINDER %s)', i, i);
  END LOOP;
END $$;
CREATE INDEX follows_recent ON follows (follower_id, created_at DESC);
CREATE INDEX followers_recent ON followers (followee_id, created_at DESC);

CREATE TABLE comments (
  id          uuid PRIMARY KEY,
  video_id    uuid NOT NULL,
  user_id     uuid NOT NULL,
  parent_id   uuid,
  text        text NOT NULL CHECK (char_length(text) BETWEEN 1 AND 500),
  created_at  timestamptz NOT NULL DEFAULT now(),
  deleted_at  timestamptz
);
CREATE INDEX comments_video ON comments (video_id, id DESC) WHERE deleted_at IS NULL AND parent_id IS NULL;
CREATE INDEX comments_replies ON comments (parent_id, id) WHERE deleted_at IS NULL AND parent_id IS NOT NULL;

CREATE TABLE blocks (
  blocker_id  uuid NOT NULL,
  blocked_id  uuid NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (blocker_id, blocked_id)
);

-- Counters are updated in batches by the counter consumer, never inline.
CREATE TABLE user_counts (
  user_id    uuid PRIMARY KEY,
  followers  bigint NOT NULL DEFAULT 0,
  following  bigint NOT NULL DEFAULT 0
);
CREATE TABLE video_counts (
  video_id  uuid PRIMARY KEY,
  likes     bigint NOT NULL DEFAULT 0,
  comments  bigint NOT NULL DEFAULT 0
);

-- Projection of media.videos: who owns a video and whether it's visible.
CREATE TABLE videos (
  video_id  uuid PRIMARY KEY,
  owner_id  uuid NOT NULL,
  status    text NOT NULL
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
