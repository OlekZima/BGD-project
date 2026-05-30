CREATE TABLE IF NOT EXISTS pipeline_log (
    run_id          BIGINT        GENERATED ALWAYS AS IDENTITY,
    run_ts          TIMESTAMP     DEFAULT now() NOT NULL,
    pipeline_name   VARCHAR(100)  NOT NULL,
    layer           VARCHAR(10)   NOT NULL,
    status          VARCHAR(10)   NOT NULL,
    rows_processed  BIGINT        DEFAULT 0,
    error_message   VARCHAR(2000),
    duration_sec    NUMERIC(10,2),
    CONSTRAINT pk_pipeline_log        PRIMARY KEY (run_id),
    CONSTRAINT ck_pipeline_log_status CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    CONSTRAINT ck_pipeline_log_layer  CHECK (layer  IN ('BRONZE', 'SILVER', 'GOLD'))
);
