-- Silver: deduplicated, type-validated station metadata.
CREATE TABLE IF NOT EXISTS silver_station_information (
    station_id                       VARCHAR(50)   NOT NULL,
    station_name                     VARCHAR(200)  NOT NULL,
    lat                              NUMERIC(11,7) NOT NULL,
    lon                              NUMERIC(11,7) NOT NULL,
    region_id                        VARCHAR(20),
    capacity                         INTEGER       NOT NULL,
    has_kiosk                        SMALLINT,
    station_information_last_updated TIMESTAMP     NOT NULL,
    missing_station_information      SMALLINT,
    bronze_ingested_at               TIMESTAMP     NOT NULL,
    silver_loaded_at                 TIMESTAMP     DEFAULT now() NOT NULL,
    CONSTRAINT pk_silver_station_info    PRIMARY KEY (station_id),
    CONSTRAINT ck_silver_si_capacity     CHECK (capacity >= 0),
    CONSTRAINT ck_silver_si_has_kiosk    CHECK (has_kiosk                   IN (0, 1)),
    CONSTRAINT ck_silver_si_missing      CHECK (missing_station_information IN (0, 1)),
    CONSTRAINT ck_silver_si_lat          CHECK (lat  BETWEEN  -90 AND  90),
    CONSTRAINT ck_silver_si_lon          CHECK (lon  BETWEEN -180 AND 180)
);
