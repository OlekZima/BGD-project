-- Gold: historical aggregate statistics per station over all Silver snapshots.
CREATE TABLE IF NOT EXISTS gold_station_summary (
    station_id              VARCHAR(50)   NOT NULL,
    station_name            VARCHAR(200),
    region_id               VARCHAR(20),
    capacity                INTEGER,
    snapshot_count          BIGINT,
    avg_bikes_available     NUMERIC(7,2),
    max_bikes_available     INTEGER,
    min_bikes_available     INTEGER,
    avg_docks_available     NUMERIC(7,2),
    avg_utilization         NUMERIC(8,4),
    avg_availability_pct    NUMERIC(5,2),
    pct_time_renting        NUMERIC(5,2),
    pct_time_installed      NUMERIC(5,2),
    first_seen_ts           TIMESTAMP,
    last_seen_ts            TIMESTAMP,
    refreshed_at            TIMESTAMP     DEFAULT now() NOT NULL,
    CONSTRAINT pk_gold_station_summary   PRIMARY KEY (station_id),
    CONSTRAINT ck_gold_ss_utilization    CHECK (avg_utilization      BETWEEN 0 AND 1),
    CONSTRAINT ck_gold_ss_avail_pct      CHECK (avg_availability_pct BETWEEN 0 AND 100),
    CONSTRAINT ck_gold_ss_pct_renting    CHECK (pct_time_renting     BETWEEN 0 AND 100),
    CONSTRAINT ck_gold_ss_pct_installed  CHECK (pct_time_installed   BETWEEN 0 AND 100)
);
