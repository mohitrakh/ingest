use axum::{
    Router,
    routing::{get, post},
};
use dotenvy::dotenv;
use sqlx::PgPool;
use std::{
    env,
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::time::{Duration, interval};
mod handlers;
mod types;
use crate::{
    handlers::ingest::{analytics_handler, ingest_event},
    utils::{
        buffer::{EventSender, create_buffer},
        db::connect_to_db,
        worker::worker,
    },
};
mod utils;

pub static INSERT_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub sender: EventSender,
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    let (tx, rx) = create_buffer(10_000);

    let pool = connect_to_db().await;
    tokio::spawn(worker(rx, pool.clone()));
    let state = AppState {
        db: pool,
        sender: tx,
    };

    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/ingest", post(ingest_event))
        .route("/analytics", get(analytics_handler))
        .with_state(state);

    let port = env::var("PORT")
        .expect("Please set the env")
        .parse()
        .expect("PORT must be valid number");

    let addr = SocketAddr::from(([127, 0, 0, 1], port));

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

    println!("Server started url: http://localhost:{}", port);

    tokio::spawn(async {
        let mut interval = interval(Duration::from_secs(1));
        let mut last = 0;

        loop {
            interval.tick().await;
            let current = INSERT_COUNTER.load(Ordering::Relaxed);
            let diff = current - last;
            last = current;

            println!("Inserts/sec: {} | Total: {}", diff, current);
        }
    });

    axum::serve(listener, app).await.unwrap();
}
