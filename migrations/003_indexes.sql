CREATE INDEX idx_events_timestamp
ON events (timestamp);

CREATE INDEX idx_events_entity_timestamp
ON events (entity_id, timestamp);

CREATE INDEX idx_events_metric_timestamp
ON events (metric_name, timestamp);