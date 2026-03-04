use std::sync::atomic::{AtomicU64, Ordering};

use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
};
use serde_json::{Value, json};
use sqlx::PgPool;

use crate::types::{AnalyticsQuery, AnalyticsResult, IngestEvent};

static INSERT_COUNTER: AtomicU64 = AtomicU64::new(0);

pub async fn ingest_event(
    State(pool): State<PgPool>,
    Json(payload): Json<IngestEvent>,
) -> Result<StatusCode, (StatusCode, String)> {
    INSERT_COUNTER.fetch_add(1, Ordering::Relaxed);
    if payload.entity_id.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "entity_id cannot be empty".into()));
    }

    if payload.metric_name.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "metric_name cannot be empty".into(),
        ));
    }

    let result = sqlx::query!(
        r#"
        INSERT INTO events (entity_id, metric_name, metric_value, timestamp, tags)
        VALUES ($1, $2, $3, $4, $5)
        "#,
        payload.entity_id,
        payload.metric_name,
        payload.metric_value,
        payload.timestamp,
        payload.tags
    )
    .execute(&pool)
    .await;

    match result {
        Ok(_) => Ok(StatusCode::CREATED),
        Err(e) => {
            eprintln!("DB Error: {:?}", e);
            Err((StatusCode::INTERNAL_SERVER_ERROR, "Database error".into()))
        }
    }
}

pub async fn analytics_handler(
    State(pool): State<PgPool>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<Json<Vec<AnalyticsResult>>, (StatusCode, String)> {
    if params.metric.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "metric is required".into()));
    }

    let include_p95 = params.p95.unwrap_or(false);
    let group_by_entity = params.group_by.as_deref() == Some("entity");

    // Build query with consistent column projection
    let query = if group_by_entity {
        if include_p95 {
            r#"
            SELECT 
                entity_id,
                COUNT(*) as count,
                AVG(metric_value) as avg,
                percentile_cont(0.95) 
                    WITHIN GROUP (ORDER BY metric_value) as p95
            FROM events
            WHERE metric_name = $1
              AND timestamp >= $2
              AND timestamp <= $3
            GROUP BY entity_id
            "#
        } else {
            r#"
            SELECT 
                entity_id,
                COUNT(*) as count,
                AVG(metric_value) as avg,
                NULL as p95
            FROM events
            WHERE metric_name = $1
              AND timestamp >= $2
              AND timestamp <= $3
            GROUP BY entity_id
            "#
        }
    } else {
        if include_p95 {
            r#"
            SELECT 
                NULL as entity_id,
                COUNT(*) as count,
                AVG(metric_value) as avg,
                percentile_cont(0.95) 
                    WITHIN GROUP (ORDER BY metric_value) as p95
            FROM events
            WHERE metric_name = $1
              AND timestamp >= $2
              AND timestamp <= $3
            "#
        } else {
            r#"
            SELECT 
                NULL as entity_id,
                COUNT(*) as count,
                AVG(metric_value) as avg,
                NULL as p95
            FROM events
            WHERE metric_name = $1
              AND timestamp >= $2
              AND timestamp <= $3
            "#
        }
    };

    let rows = sqlx::query_as::<sqlx::Postgres, AnalyticsResult>(query)
        .bind(params.metric)
        .bind(params.from)
        .bind(params.to)
        .fetch_all(&pool)
        .await
        .map_err(|e| {
            eprintln!("DB error: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("DB error: {}", e),
            )
        })?;

    Ok(Json(rows))
}
