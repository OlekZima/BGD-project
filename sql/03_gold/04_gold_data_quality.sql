-- Gold: append-only data-quality audit log.
CREATE TABLE IF NOT EXISTS gold_data_quality (
    dq_id           BIGINT        GENERATED ALWAYS AS IDENTITY,
    run_ts          TIMESTAMP     DEFAULT now() NOT NULL,
    metric_name     VARCHAR(100)  NOT NULL,
    metric_value    NUMERIC,
    threshold       NUMERIC,
    unit            VARCHAR(20),
    dataset         VARCHAR(100),
    status          VARCHAR(4)    NOT NULL,
    CONSTRAINT pk_gold_dq        PRIMARY KEY (dq_id),
    CONSTRAINT ck_gold_dq_status CHECK (status IN ('PASS', 'FAIL'))
);

CREATE INDEX IF NOT EXISTS idx_gold_dq_run_ts
    ON gold_data_quality (run_ts);
