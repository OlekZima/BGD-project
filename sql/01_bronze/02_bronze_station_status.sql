-- Idempotent key: (station_id, station_status_last_reported) — supports Kafka replay.
-- station_status_last_reported is a Unix epoch in milliseconds from the GBFS feed.
CREATE TABLE IF NOT EXISTS bronze_station_status (
    station_id                    VARCHAR(50)   NOT NULL,
    num_bikes_available           INTEGER,
    num_ebikes_available          INTEGER,
    num_bikes_disabled            INTEGER,
    num_docks_available           INTEGER,
    num_docks_disabled            INTEGER,
    is_installed                  VARCHAR(5),
    is_renting                    VARCHAR(5),
    is_returning                  VARCHAR(5),
    station_status_last_reported  BIGINT        NOT NULL,
    ingested_at                   TIMESTAMP     DEFAULT now() NOT NULL,
    source_file                   VARCHAR(200),
    source_mode                   VARCHAR(10),
    event_id                      VARCHAR(64),
    kafka_topic                   VARCHAR(100),
    kafka_partition               INTEGER,
    kafka_offset                  BIGINT,
    CONSTRAINT pk_bronze_station_status PRIMARY KEY (station_id, station_status_last_reported),
    CONSTRAINT ck_bronze_ss_mode        CHECK (source_mode IN ('batch', 'stream'))
);

CREATE INDEX IF NOT EXISTS idx_bronze_ss_ingested
    ON bronze_station_status (ingested_at);
