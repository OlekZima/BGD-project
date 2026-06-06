-- Gold: daily aggregates per station — one row per (station_id, station_date).
-- avg_utilization = avg(num_bikes_available / capacity), range 0–1 not 0–100.
CREATE TABLE IF NOT EXISTS gold_station_daily (
    station_id          VARCHAR(50)   NOT NULL,
    station_name        VARCHAR(200),
    station_date        DATE          NOT NULL,
    avg_bikes           NUMERIC(7,2),
    avg_docks           NUMERIC(7,2),
    avg_utilization     NUMERIC(8,4),
    measurements_count  BIGINT,
    refreshed_at        TIMESTAMP     DEFAULT now() NOT NULL,
    CONSTRAINT pk_gold_station_daily  PRIMARY KEY (station_id, station_date),
    CONSTRAINT ck_gold_sd_utilization CHECK (avg_utilization    BETWEEN 0 AND 1),
    CONSTRAINT ck_gold_sd_avg_bikes   CHECK (avg_bikes          >= 0),
    CONSTRAINT ck_gold_sd_avg_docks   CHECK (avg_docks          >= 0),
    CONSTRAINT ck_gold_sd_mcount      CHECK (measurements_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_gold_sd_date
    ON gold_station_daily (station_date);
