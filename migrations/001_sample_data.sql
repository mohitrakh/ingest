-- Create the events table
CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    entity_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value NUMERIC NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    tags JSONB
);

-- Create required indexes
CREATE INDEX idx_events_timestamp ON events (timestamp);
CREATE INDEX idx_events_entity_timestamp ON events (entity_id, timestamp);
CREATE INDEX idx_events_metric_timestamp ON events (metric_name, timestamp);