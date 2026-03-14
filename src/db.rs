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
        #[tracing::instrument(skip_all)]
        fn db_task<R>(
            task: impl FnOnce() -> R + Send + 'static,
        ) -> Result<R, Box<dyn std::error::Error + Send + Sync + 'static>> {
            Ok(task())
        }
        rt::spawn_blocking(|| db_task(task)).boxed()
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

/// Layer names in reverse order (L4 down to L1).
/// L0 is excluded because removing it would destroy all data.
const LAYERS_REVERSE: &[&str] = &["l4", "l3", "l2", "l1"];

/// Revert migrations from L4 down to (and including) the given layer.
/// After this, call `run_migrations` to re-apply the up migrations.
pub async fn remove_layers_down_to(conn: &mut AsyncDbConnection, target_layer: &str) {
    use diesel::migration::MigrationSource;

    let target_layer = target_layer.to_string();
    conn.spawn_blocking(move |conn| {
        let migrations =
            MigrationSource::<Db>::migrations(&MIGRATIONS).expect("Failed to get migrations");

        // Determine which layers to revert (L4 down to target)
        let target_idx = LAYERS_REVERSE
            .iter()
            .position(|l| *l == target_layer)
            .unwrap_or_else(|| {
                panic!("Unknown layer: {target_layer}. Expected one of: l1, l2, l3, l4")
            });

        let layers_to_revert = &LAYERS_REVERSE[..=target_idx];

        for layer in layers_to_revert {
            let suffix = format!("_{layer}");
            if let Some(migration) =
                migrations
                    .iter()
                    .find(|m: &&Box<dyn diesel::migration::Migration<Db>>| {
                        m.name().to_string().ends_with(&suffix)
                    })
            {
                log::info!("Reverting migration for {layer}: {}", migration.name());
                if let Err(e) = conn.revert_migration(migration.as_ref()) {
                    log::warn!("Failed to revert {layer} (may not be applied): {e}");
                }
            } else {
                log::warn!("No migration found for layer {layer}");
            }
        }

        Ok(())
    })
    .await
    .expect("Failed to revert migrations");
}
