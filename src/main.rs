use axum::{
    Router,
    routing::{get, post},
};
use dotenvy::dotenv;
use std::{env, net::SocketAddr};
mod handlers;
mod types;
use crate::{
    handlers::ingest::{analytics_handler, ingest_event},
    utils::db::connect_to_db,
};
mod utils;

#[tokio::main]
async fn main() {
    dotenv().ok();
    let pool = connect_to_db().await;

    let port = env::var("PORT")
        .expect("Please set the env")
        .parse()
        .expect("PORT must be valid number");

    let addr = SocketAddr::from(([127, 0, 0, 1], port));

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/ingest", post(ingest_event))
        .route("/analytics", get(analytics_handler))
        .with_state(pool);

    println!("Server started url: http://localhost:{}", port);

    axum::serve(listener, app).await.unwrap();
}
