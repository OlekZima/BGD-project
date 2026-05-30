CREATE TABLE IF NOT EXISTS bronze_dlq (
    dlq_id          BIGINT        GENERATED ALWAYS AS IDENTITY,
    event_id        VARCHAR(64),
    source_topic    VARCHAR(100),
    partition_num   INTEGER,
    kafka_offset    BIGINT,
    error_message   VARCHAR(2000) NOT NULL,
    error_ts        TIMESTAMP     DEFAULT now() NOT NULL,
    raw_payload     TEXT,
    CONSTRAINT pk_bronze_dlq PRIMARY KEY (dlq_id)
);

-- Kafka-sourced rows deduplicated by event_id; NULL (batch failures) are exempt.
CREATE UNIQUE INDEX IF NOT EXISTS uix_bronze_dlq_event
    ON bronze_dlq (event_id) WHERE event_id IS NOT NULL;
