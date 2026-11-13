CREATE TABLE devices (
  token       text PRIMARY KEY,
  user_id     uuid NOT NULL,
  platform    text NOT NULL CHECK (platform IN ('ios', 'android', 'web')),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX devices_user ON devices (user_id);

CREATE TABLE prefs (
  user_id       uuid PRIMARY KEY,
  new_follower  boolean NOT NULL DEFAULT true,
  comments      boolean NOT NULL DEFAULT true,
  likes         boolean NOT NULL DEFAULT true,
  live          boolean NOT NULL DEFAULT true
);
