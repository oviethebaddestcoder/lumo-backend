-- Total likes across a creator's videos, for the profile header.
ALTER TABLE user_counts ADD COLUMN IF NOT EXISTS likes_received bigint NOT NULL DEFAULT 0;
