-- Gold: current availability snapshot — one row per station, latest status only.
CREATE TABLE IF NOT EXISTS gold_station_availability (
    station_id              VARCHAR(50)   NOT NULL,
    station_name            VARCHAR(200),
    region_id               VARCHAR(20),
    lat                     NUMERIC(11,7),
    lon                     NUMERIC(11,7),
    capacity                INTEGER,
    num_bikes_available     INTEGER,
    num_ebikes_available    INTEGER,
    num_docks_available     INTEGER,
    is_installed            SMALLINT,
    is_renting              SMALLINT,
    is_returning            SMALLINT,
    availability_pct        NUMERIC(5,2),
    last_reported_ts        TIMESTAMP,
    refreshed_at            TIMESTAMP     DEFAULT now() NOT NULL,
    CONSTRAINT pk_gold_station_avail    PRIMARY KEY (station_id),
    CONSTRAINT ck_gold_sa_avail_pct     CHECK (availability_pct BETWEEN 0 AND 100),
    CONSTRAINT ck_gold_sa_installed     CHECK (is_installed  IN (0, 1)),
    CONSTRAINT ck_gold_sa_renting       CHECK (is_renting    IN (0, 1)),
    CONSTRAINT ck_gold_sa_returning     CHECK (is_returning  IN (0, 1))
);

CREATE INDEX IF NOT EXISTS idx_gold_sa_region
    ON gold_station_availability (region_id);
