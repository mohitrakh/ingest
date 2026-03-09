use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{prelude::FromRow, types::BigDecimal};

#[derive(Deserialize, Debug)]
pub struct IngestEvent {
    pub entity_id: String,
    pub metric_name: String,
    pub metric_value: f64,
    pub timestamp: DateTime<Utc>,
    pub region: String,
    pub env: String,
}

#[derive(Deserialize)]
pub struct AnalyticsQuery {
    pub metric: String,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub group_by: Option<String>, // "entity"
    pub p95: Option<bool>,
}

#[derive(Serialize, FromRow)]
pub struct AnalyticsResult {
    pub entity_id: Option<String>,
    pub count: i64,
    pub avg: Option<BigDecimal>,
    pub p95: Option<BigDecimal>,
}
