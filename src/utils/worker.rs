use std::fmt::Write;
use std::{
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

use sqlx::{PgConnection, PgPool};
use tokio::sync::mpsc::Receiver;

use crate::{INSERT_COUNTER, types::IngestEvent};

const BATCH_SIZE: usize = 50000;
const FLUSH_INTERVAL: Duration = Duration::from_millis(100);

const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

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

    let payload = build_binary_payload(batch);
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to acquire connection: {:?}", e);
            return;
        }
    };

    if let Err(e) = do_copy(&mut conn, payload).await {
        eprintln!("COPY failed: {:?}", e);
        return;
    }

    INSERT_COUNTER.fetch_add(batch.len() as u64, Ordering::Relaxed);

    batch.clear();
}

use bytes::{BufMut, Bytes, BytesMut};

// Replace your csv_data string with this
fn build_binary_payload(batch: &[IngestEvent]) -> Bytes {
    // Rough capacity: header(19) + trailer(2) + per row ~80 bytes
    let mut buf = BytesMut::with_capacity(19 + 2 + batch.len() * 80);

    // ── File header ──────────────────────────────────────────
    buf.put_slice(b"PGCOPY\n\xff\r\n\0"); // magic, exactly 11 bytes
    buf.put_i32(0); // flags: no OIDs
    buf.put_i32(0); // header extension area length

    // ── Tuples ───────────────────────────────────────────────
    for event in batch {
        buf.put_i16(6); // number of fields in this row
        let pg_micros = event.timestamp.timestamp_micros() - PG_EPOCH_OFFSET_MICROS;
        // entity_id — text, raw UTF-8
        let b = event.entity_id.as_bytes();
        buf.put_i32(b.len() as i32);
        buf.put_slice(b);

        // metric_name — text
        let b = event.metric_name.as_bytes();
        buf.put_i32(b.len() as i32);
        buf.put_slice(b);

        // metric_value — float8 (f64), 8 bytes big-endian
        buf.put_i32(8);
        buf.put_f64(event.metric_value);

        // timestamp — int8 (i64), 8 bytes big-endian
        buf.put_i32(8);
        buf.put_i64(pg_micros);

        // region — text
        let b = event.region.as_bytes();
        buf.put_i32(b.len() as i32);
        buf.put_slice(b);

        // env — text
        let b = event.env.as_bytes();
        buf.put_i32(b.len() as i32);
        buf.put_slice(b);
    }

    // ── File trailer ─────────────────────────────────────────
    buf.put_i16(-1);

    buf.freeze()
}

async fn do_copy(conn: &mut PgConnection, data: Bytes) -> Result<(), sqlx::Error> {
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
            FROM STDIN WITH (FORMAT binary)", // ← only change here
        )
        .await?;

    copy.send(data).await?;
    let rows_inserted = copy.finish().await?;
    println!("COPY inserted {} rows", rows_inserted);
    Ok(())
}
