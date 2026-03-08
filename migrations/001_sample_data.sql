CREATE TABLE events (
    id BIGSERIAL,
    entity_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value NUMERIC NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    tags JSONB,
    PRIMARY KEY (id, timestamp)
) PARTITION BY RANGE (timestamp);