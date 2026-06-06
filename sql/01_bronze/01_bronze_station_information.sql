-- Raw station metadata as delivered — last write wins per station_id.
-- No type coercion or domain constraints at this layer.
CREATE TABLE IF NOT EXISTS bronze_station_information (
    station_id                       VARCHAR(50)   NOT NULL,
    station_name                     VARCHAR(200),
    lat                              NUMERIC(11,7),
    lon                              NUMERIC(11,7),
    region_id                        VARCHAR(20),
    capacity                         INTEGER,
    has_kiosk                        VARCHAR(5),
    station_information_last_updated BIGINT,
    missing_station_information      VARCHAR(5),
    ingested_at                      TIMESTAMP     DEFAULT now() NOT NULL,
    source_file                      VARCHAR(200),
    source_mode                      VARCHAR(10),
    event_id                         VARCHAR(64),
    kafka_topic                      VARCHAR(100),
    kafka_partition                  INTEGER,
    kafka_offset                     BIGINT,
    CONSTRAINT pk_bronze_station_info  PRIMARY KEY (station_id),
    CONSTRAINT ck_bronze_si_mode       CHECK (source_mode IN ('batch', 'stream'))
);
