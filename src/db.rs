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

const _: () = {
    const fn is_send<T: Send>() {}
    is_send::<AsyncDbConnection>();
    is_send::<tokio::sync::Mutex<AsyncDbConnection>>();

    const fn is_send_ref<'a>(_: &'a ()) {
        is_send::<&'a tokio::sync::Mutex<AsyncDbConnection>>();
    }
};

#[derive(thiserror::Error, Debug)]
pub enum DbError {
    #[error(transparent)]
    QueryError(#[from] diesel::result::Error),
    #[error(transparent)]
    ConnectionError(#[from] diesel::result::ConnectionError),
    #[error(transparent)]
    MigrationError(#[from] diesel_migrations::MigrationError),
    #[error("blocking task failed: {0}")]
    BlockingTask(String),
    #[error("invalid layer '{0}', expected one of: l1, l2, l3, l4")]
    InvalidLayer(String),
}

fn diesel_unknown(message: impl Into<String>) -> diesel::result::Error {
    diesel::result::Error::DatabaseError(
        diesel::result::DatabaseErrorKind::Unknown,
        Box::new(message.into()),
    )
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
pub async fn run_migrations(conn: &mut AsyncDbConnection) -> Result<(), DbError> {
    log::debug!("Running migrations");
    conn.spawn_blocking(|conn: &mut DbConnection| {
        conn.run_pending_migrations(MIGRATIONS)
            .map_err(|e| diesel_unknown(e.to_string()))?;
        Ok(())
    })
    .await
    .map_err(|e| DbError::BlockingTask(e.to_string()))?;
    Ok(())
}

/// Layer names in reverse order (L4 down to L1).
/// L0 is excluded because removing it would destroy all data.
const LAYERS_REVERSE: &[&str] = &["l4", "l3", "l2", "l1"];

/// Revert migrations from L4 down to (and including) the given layer.
/// After this, call `run_migrations` to re-apply the up migrations.
pub async fn remove_layers_down_to(
    conn: &mut AsyncDbConnection,
    target_layer: &str,
) -> Result<(), DbError> {
    use diesel::migration::MigrationSource;

    if !LAYERS_REVERSE.iter().any(|l| *l == target_layer) {
        return Err(DbError::InvalidLayer(target_layer.to_string()));
    }

    let target_layer = target_layer.to_string();
    conn.spawn_blocking(move |conn: &mut DbConnection| {
        let migrations = MigrationSource::<Db>::migrations(&MIGRATIONS)
            .map_err(|e| diesel_unknown(e.to_string()))?;

        // Determine which layers to revert (L4 down to target)
        let target_idx = LAYERS_REVERSE
            .iter()
            .position(|l| *l == target_layer)
            .ok_or_else(|| diesel_unknown(format!("invalid layer '{target_layer}'")))?;

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
    .map_err(|e| DbError::BlockingTask(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel::sql_types::{BigInt, Text};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(diesel::QueryableByName)]
    struct CountRow {
        #[diesel(sql_type = BigInt)]
        count: i64,
    }

    fn test_db_path() -> PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!("iris-blocks-db-test-{ts}.sqlite"))
    }

    async fn table_exists(conn: &mut AsyncDbConnection, table: &str) -> bool {
        let row = sql_query(
            "SELECT COUNT(*) AS count
             FROM sqlite_master
             WHERE type = 'table' AND name = ?1",
        )
        .bind::<Text, _>(table.to_string())
        .get_result::<CountRow>(conn)
        .await
        .expect("sqlite_master query");
        row.count > 0
    }

    #[tokio::test]
    async fn remove_layers_validates_input_layer() {
        let path = test_db_path();
        let mut conn = new_conn(path.to_str().expect("db path"))
            .await
            .expect("open sqlite");
        run_migrations(&mut conn).await.expect("migrations");

        let err = remove_layers_down_to(&mut conn, "l9")
            .await
            .expect_err("invalid layer should error");
        assert!(matches!(err, DbError::InvalidLayer(_)));

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn remove_and_reapply_layers_preserves_integrity() {
        let path = test_db_path();
        let mut conn = new_conn(path.to_str().expect("db path"))
            .await
            .expect("open sqlite");
        run_migrations(&mut conn).await.expect("migrations");
        assert!(table_exists(&mut conn, "credits").await);
        assert!(table_exists(&mut conn, "name_info").await);

        remove_layers_down_to(&mut conn, "l3")
            .await
            .expect("revert migrations");
        assert!(!table_exists(&mut conn, "credits").await);
        assert!(!table_exists(&mut conn, "name_info").await);
        assert!(table_exists(&mut conn, "tx_outputs").await);

        run_migrations(&mut conn).await.expect("migrations");
        assert!(table_exists(&mut conn, "credits").await);
        assert!(table_exists(&mut conn, "name_info").await);

        let _ = std::fs::remove_file(path);
    }
}
