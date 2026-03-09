use std::fmt::Write;
use std::{
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

use sqlx::{PgConnection, PgPool};
use tokio::sync::mpsc::Receiver;

use crate::{INSERT_COUNTER, types::IngestEvent};

const BATCH_SIZE: usize = 10000;
const FLUSH_INTERVAL: Duration = Duration::from_millis(100);

pub async fn worker(mut rx: Receiver<IngestEvent>, pool: PgPool) {
    let mut batch: Vec<IngestEvent> = Vec::with_capacity(BATCH_SIZE);
    let mut last_flush = Instant::now();

    loop {
        tokio::select! {

            Some(event) = rx.recv() => {
                batch.push(event);

                if batch.len() >= BATCH_SIZE {
                    flush_batch(&pool, &mut batch).await;
                    last_flush = Instant::now();
                }
            }

            _ = tokio::time::sleep(FLUSH_INTERVAL) => {
                if !batch.is_empty() && last_flush.elapsed() >= FLUSH_INTERVAL {
                    flush_batch(&pool, &mut batch).await;
                    last_flush = Instant::now();
                }
            }
        }
    }
}

async fn flush_batch(pool: &PgPool, batch: &mut Vec<IngestEvent>) {
    if batch.is_empty() {
        return;
    }

    let mut csv_data = String::with_capacity(batch.len() * 96);

    for event in batch.iter() {
        let entity_id = escape_csv(&event.entity_id);
        let metric_name = escape_csv(&event.metric_name);
        let region = escape_csv(&event.region);
        let env = escape_csv(&event.env);

        writeln!(
            &mut csv_data,
            "{},{},{},{},{},{}",
            entity_id, metric_name, event.metric_value, event.timestamp, region, env,
        )
        .unwrap();
    }

    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to acquire connection: {:?}", e);
            return;
        }
    };

    if let Err(e) = do_copy(&mut conn, csv_data.as_bytes()).await {
        eprintln!("COPY failed: {:?}", e);
        return;
    }

    INSERT_COUNTER.fetch_add(batch.len() as u64, Ordering::Relaxed);

    batch.clear();
}

/// Escape a string field for CSV: wrap in quotes and double any internal quotes.
fn escape_csv(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

async fn do_copy(conn: &mut PgConnection, data: &[u8]) -> Result<(), sqlx::Error> {
    let mut copy = conn
        .copy_in_raw(
            "COPY events (
        entity_id,
        metric_name,
        metric_value,
        timestamp,
        region,
        env
    )
    FROM STDIN WITH (FORMAT csv, DELIMITER ',', NULL '')",
        )
        .await?;

    // Send data in chunks — avoids holding the entire buffer in one syscall
    copy.send(data).await?;

    // Finish the COPY stream; this commits the data to Postgres
    let rows_inserted = copy.finish().await?;

    println!("COPY inserted {} rows", rows_inserted);
    Ok(())
}
