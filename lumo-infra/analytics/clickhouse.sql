-- Optional analytics: ClickHouse reads Kafka directly (no service code).
-- Powers Creator Studio charts and feed-ranking training data.
CREATE TABLE kafka_impressions (raw String)
ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'feed.impressions',
  kafka_group_name = 'clickhouse-impressions', kafka_format = 'JSONAsString';

CREATE TABLE impressions (
  at DateTime, user_id UUID, video_id UUID, watch_ms UInt32
) ENGINE = MergeTree PARTITION BY toYYYYMM(at) ORDER BY (video_id, at);

CREATE MATERIALIZED VIEW mv_impressions TO impressions AS
SELECT parseDateTimeBestEffort(JSONExtractString(raw, 'data', 'at')) AS at,
       toUUID(JSONExtractString(raw, 'data', 'user_id')) AS user_id,
       toUUID(JSONExtractString(raw, 'data', 'video_id')) AS video_id,
       JSONExtractUInt(raw, 'data', 'watch_ms') AS watch_ms
FROM kafka_impressions;

CREATE TABLE kafka_gifts (raw String)
ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'wallet.gifts',
  kafka_group_name = 'clickhouse-gifts', kafka_format = 'JSONAsString';

CREATE TABLE gifts (
  at DateTime, sender_id UUID, receiver_id UUID, room_id String, gift_id LowCardinality(String), coins UInt64, diamonds UInt64
) ENGINE = MergeTree PARTITION BY toYYYYMM(at) ORDER BY (receiver_id, at);

CREATE MATERIALIZED VIEW mv_gifts TO gifts AS
SELECT parseDateTimeBestEffort(JSONExtractString(raw, 'data', 'sent_at')) AS at,
       toUUID(JSONExtractString(raw, 'data', 'sender_id')) AS sender_id,
       toUUID(JSONExtractString(raw, 'data', 'receiver_id')) AS receiver_id,
       JSONExtractString(raw, 'data', 'room_id') AS room_id,
       JSONExtractString(raw, 'data', 'gift_id') AS gift_id,
       JSONExtractUInt(raw, 'data', 'coins') AS coins,
       JSONExtractUInt(raw, 'data', 'diamonds') AS diamonds
FROM kafka_gifts;
