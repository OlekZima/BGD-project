-- Gold: current aggregated availability per Citi Bike region.
CREATE TABLE IF NOT EXISTS gold_region_summary (
    region_id               VARCHAR(20)   NOT NULL,
    station_count           INTEGER,
    total_capacity          BIGINT,
    total_bikes_available   BIGINT,
    avg_availability_pct    NUMERIC(5,2),
    pct_stations_renting    NUMERIC(5,2),
    refreshed_at            TIMESTAMP     DEFAULT now() NOT NULL,
    CONSTRAINT pk_gold_region_summary    PRIMARY KEY (region_id),
    CONSTRAINT ck_gold_rs_avail_pct      CHECK (avg_availability_pct  BETWEEN 0 AND 100),
    CONSTRAINT ck_gold_rs_pct_renting    CHECK (pct_stations_renting  BETWEEN 0 AND 100)
);
