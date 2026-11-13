-- View counts (batched from feed.impressions) and hashtag search.
ALTER TABLE videos ADD COLUMN IF NOT EXISTS views bigint NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS videos_hashtags ON videos USING gin (hashtags) WHERE status = 'ready' AND visibility = 'public';
