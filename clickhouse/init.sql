CREATE TABLE movie_events_queue
(
    event_id UUID,
    user_id String,
    movie_id String,
    event_type Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3, 'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id String,
    progress_seconds Int32
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_flush_interval_ms = 1000;

CREATE TABLE movie_events AS movie_events_queue
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (toDate(timestamp), event_type, user_id);

CREATE MATERIALIZED VIEW movie_events_mv TO movie_events AS SELECT * FROM movie_events_queue;

CREATE TABLE daily_metrics
(
    date Date,
    metric LowCardinality(String),
    value Float64,
    computed_at DateTime
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (date, metric);

CREATE TABLE top_movies
(
    date Date,
    rank UInt8,
    movie_id String,
    views UInt64,
    computed_at DateTime
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (date, rank);
