CREATE TABLE IF NOT EXISTS ${table} (
    kafka_topic     TEXT        NOT NULL,
    kafka_partition INTEGER     NOT NULL,
    kafka_offset    BIGINT      NOT NULL,
    service         TEXT        NOT NULL,
    host            TEXT        NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,
    ingest_time     TIMESTAMPTZ NOT NULL,
    level           TEXT        NOT NULL,
    message         TEXT        NOT NULL,
    trace_id        TEXT,
    status_code     INTEGER,
    latency_ms      BIGINT,
    format          TEXT        NOT NULL,
    region          TEXT        NOT NULL,
    env             TEXT        NOT NULL,
    sampled         BOOLEAN     NOT NULL DEFAULT FALSE,
    raw             TEXT        NOT NULL,
    PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS ${table}_service_time_idx ON ${table} (service, event_time);