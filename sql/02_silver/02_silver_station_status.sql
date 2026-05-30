-- Silver: validated status snapshots, enriched with region_id from station info.
-- last_reported_ts is station_status_last_reported (epoch ms ÷ 1000) cast to TIMESTAMP.
CREATE TABLE IF NOT EXISTS silver_station_status (
    station_id                    VARCHAR(50)   NOT NULL,
    num_bikes_available           INTEGER       NOT NULL,
    num_ebikes_available          INTEGER,
    num_bikes_disabled            INTEGER       NOT NULL,
    num_docks_available           INTEGER       NOT NULL,
    num_docks_disabled            INTEGER,
    is_installed                  SMALLINT      NOT NULL,
    is_renting                    SMALLINT      NOT NULL,
    is_returning                  SMALLINT      NOT NULL,
    station_status_last_reported  BIGINT        NOT NULL,
    last_reported_ts              TIMESTAMP     NOT NULL,
    region_id                     VARCHAR(20),
    bronze_ingested_at            TIMESTAMP     NOT NULL,
    silver_loaded_at              TIMESTAMP     DEFAULT now() NOT NULL,
    CONSTRAINT pk_silver_station_status   PRIMARY KEY (station_id, station_status_last_reported),
    CONSTRAINT ck_silver_ss_bikes_avail   CHECK (num_bikes_available >= 0),
    CONSTRAINT ck_silver_ss_docks_avail   CHECK (num_docks_available >= 0),
    CONSTRAINT ck_silver_ss_bikes_dis     CHECK (num_bikes_disabled  >= 0),
    CONSTRAINT ck_silver_ss_docks_dis     CHECK (num_docks_disabled  >= 0),
    CONSTRAINT ck_silver_ss_installed     CHECK (is_installed IN (0, 1)),
    CONSTRAINT ck_silver_ss_renting       CHECK (is_renting   IN (0, 1)),
    CONSTRAINT ck_silver_ss_returning     CHECK (is_returning IN (0, 1)),
    CONSTRAINT fk_silver_ss_station       FOREIGN KEY (station_id)
        REFERENCES silver_station_information (station_id)
);

CREATE INDEX IF NOT EXISTS idx_silver_ss_reported_ts
    ON silver_station_status (last_reported_ts);

CREATE INDEX IF NOT EXISTS idx_silver_ss_region_ts
    ON silver_station_status (region_id, last_reported_ts);
