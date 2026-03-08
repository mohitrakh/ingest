use std::env;

use sqlx::{PgPool, postgres::PgPoolOptions};

pub async fn connect_to_db() -> PgPool {
    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must be set in your .env File");

    println!("Database URL: {}", database_url);

    let pool = PgPoolOptions::new()
        .max_connections(250)
        .connect(&database_url)
        .await
        .expect("Failed to create database connection pool");

    // Run migrations
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    pool
}
