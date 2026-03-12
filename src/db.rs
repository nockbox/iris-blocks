//! Database connection pooling for iris-blocks (SQLite, async via diesel-async).

use crate::rt;
use diesel::prelude::*;
use diesel::sql_query;
use diesel_async::{sync_connection_wrapper::SyncConnectionWrapper, AsyncConnection, RunQueryDsl};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use futures::{future::BoxFuture, FutureExt};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations/sqlite");

pub struct DbRuntime;

impl diesel_async::sync_connection_wrapper::SpawnBlocking for DbRuntime {
    fn spawn_blocking<'a, R>(
        &mut self,
        task: impl FnOnce() -> R + Send + 'static,
    ) -> BoxFuture<'a, Result<R, Box<dyn std::error::Error + Send + Sync + 'static>>>
    where
        R: Send + 'static,
    {
        rt::spawn_blocking(|| Ok(task())).boxed()
    }

    fn get_runtime() -> Self {
        Self
    }
}

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

pub type Db = diesel::sqlite::Sqlite;

pub type DbConnection = SqliteConnection;

/// Async-compatible SQLite connection (sync diesel wrapped for use with bb8).
pub type AsyncDbConnection = SyncConnectionWrapper<SqliteConnection, DbRuntime>;

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

pub async fn new_conn(database_url: &str) -> Result<AsyncDbConnection, DbError> {
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
