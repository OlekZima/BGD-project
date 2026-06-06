CREATE TABLE IF NOT EXISTS pipeline_watermark (
    source_name         VARCHAR(50)  NOT NULL,
    last_station_id     VARCHAR(50),
    last_reported_ts    TIMESTAMP,
    rows_loaded         BIGINT       DEFAULT 0   NOT NULL,
    last_load_ts        TIMESTAMP    DEFAULT now() NOT NULL,
    CONSTRAINT pk_pipeline_watermark PRIMARY KEY (source_name)
);
