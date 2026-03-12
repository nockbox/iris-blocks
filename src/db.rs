//! Database connection pooling for iris-blocks (SQLite, async via diesel-async).

use diesel::prelude::*;
use diesel::sql_query;
use diesel_async::{sync_connection_wrapper::SyncConnectionWrapper, AsyncConnection, RunQueryDsl};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations/sqlite");

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

pub type Db = diesel::sqlite::Sqlite;

pub type DbConnection = SqliteConnection;

/// Async-compatible SQLite connection (sync diesel wrapped for use with bb8).
pub type AsyncDbConnection = SyncConnectionWrapper<SqliteConnection>;

#[derive(thiserror::Error, Debug)]
pub enum DbError {
    #[error(transparent)]
    QueryError(#[from] diesel::result::Error),
    #[error(transparent)]
    ConnectionError(#[from] diesel::result::ConnectionError),
    #[error(transparent)]
    MigrationError(#[from] diesel_migrations::MigrationError),
}

// ---------------------------------------------------------------------------
// Pool wrapper
// ---------------------------------------------------------------------------

pub async fn new_conn(database_url: &str, _max_size: u32) -> Result<AsyncDbConnection, DbError> {
    let mut conn = AsyncDbConnection::establish(database_url).await?;

    sql_query("PRAGMA foreign_keys = ON;")
        .execute(&mut conn)
        .await?;

    Ok(conn)
}

// ---------------------------------------------------------------------------
// Migrations
// ---------------------------------------------------------------------------

/// Run all pending migrations via a direct sync connection.
pub async fn run_migrations(conn: &mut AsyncDbConnection) {
    log::debug!("Running migrations");
    conn.spawn_blocking(|conn| {
        conn.run_pending_migrations(MIGRATIONS)
            .expect("Failed to run migrations");
        Ok(())
    })
    .await
    .expect("Failed to run migrations");
}
