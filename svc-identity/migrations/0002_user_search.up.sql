-- Prefix search on @handle and display name.
CREATE INDEX IF NOT EXISTS users_handle_prefix ON users (handle text_pattern_ops) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS users_name_prefix ON users (lower(display_name) text_pattern_ops) WHERE status = 'active';
