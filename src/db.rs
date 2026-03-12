//! Database connection pooling for iris-blocks (SQLite, async via diesel-async).

use diesel::prelude::*;
use diesel::sql_query;
use diesel_async::{
    pooled_connection::{
        bb8::{self, Pool, PooledConnection, RunError},
        AsyncDieselConnectionManager, ManagerConfig, PoolError,
    },
    sync_connection_wrapper::SyncConnectionWrapper,
    AsyncConnection, RunQueryDsl,
};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use futures::FutureExt;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations/sqlite");

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

pub type Db = diesel::sqlite::Sqlite;

pub type DbConnection = SqliteConnection;

/// Async-compatible SQLite connection (sync diesel wrapped for use with bb8).
pub type AsyncDbConnection = SyncConnectionWrapper<SqliteConnection>;

/// bb8 connection pool over [`AsyncDbConnection`].
/// Note: `Pool<C>` in diesel-async takes the *connection* type, not the manager.
pub type DbPool = Pool<AsyncDbConnection>;

/// An owned connection checked out of the pool (no lifetime parameter).
pub type OwnedConnection = PooledConnection<'static, AsyncDbConnection>;

pub use bb8::RunError as PoolRunError;

// ---------------------------------------------------------------------------
// Pool wrapper
// ---------------------------------------------------------------------------

pub async fn new_pool(database_url: &str, max_size: u32) -> Result<DbPool, PoolError> {
    let mut config = ManagerConfig::<AsyncDbConnection>::default();

    config.custom_setup = Box::new(|url| {
        async move {
            let mut conn = AsyncDbConnection::establish(url).await?;

            sql_query("PRAGMA foreign_keys = ON;")
                .execute(&mut conn)
                .await
                .map_err(ConnectionError::CouldntSetupConfiguration)?;

            Ok::<AsyncDbConnection, ConnectionError>(conn)
        }
        .boxed()
    });

    let manager =
        AsyncDieselConnectionManager::<AsyncDbConnection>::new_with_config(database_url, config);

    Pool::builder().max_size(max_size).build(manager).await
}

// ---------------------------------------------------------------------------
// Migrations
// ---------------------------------------------------------------------------

/// Run all pending migrations via a direct sync connection.
/// Call once at startup before creating the pool.
pub fn run_migrations(database_url: &str) {
    let mut conn =
        SqliteConnection::establish(database_url).expect("Failed to connect for migrations");
    conn.run_pending_migrations(MIGRATIONS)
        .expect("Failed to run migrations");
}
