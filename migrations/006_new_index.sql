-- no transaction

CREATE INDEX IF NOT EXISTS idx_metric_time_entity
ON events (metric_name, timestamp, entity_id);