use crate::layers::l2::schema::{pkh_to_pk, PkhToPk};
use crate::layers::shared_schema::{DbDigest, DbPublicKey};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use iris_crypto::PublicKey;
use iris_ztd::Digest;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Clone, Serialize)]
pub enum AddressType {
    Pkh,
    DbPublicKey,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub enum VersionScope {
    All,
    V0Only,
    V1Only,
}

#[derive(Debug, Clone, Serialize)]
pub struct AddressInfo {
    pub input: String,
    pub address_type: AddressType,
    pub scope: VersionScope,
    pub pkh: String,
    pub db_public_key: Option<String>,
}

#[derive(Debug, Error)]
pub enum AddressError {
    #[error("invalid address format: {0}")]
    InvalidAddress(String),
    #[error(transparent)]
    Diesel(#[from] diesel::result::Error),
}

pub async fn resolve_address(
    conn: &mut crate::db::AsyncDbConnection,
    address: &str,
) -> Result<AddressInfo, AddressError> {
    let normalized = address.trim();
    if normalized.is_empty() {
        return Err(AddressError::InvalidAddress(address.to_string()));
    }

    // Parse as public key first so PK inputs keep strict V0 semantics.
    if let Ok(pk) = PublicKey::try_from(normalized) {
        let db_pk = DbPublicKey::from(pk);
        if let Some(row) = pkh_to_pk::table
            .filter(pkh_to_pk::pk.eq(db_pk))
            .first::<PkhToPk>(conn)
            .await
            .optional()?
        {
            return Ok(AddressInfo {
                input: normalized.to_string(),
                address_type: AddressType::DbPublicKey,
                scope: VersionScope::V0Only,
                pkh: row.pkh.to_string(),
                db_public_key: Some(row.pk.to_string()),
            });
        } else {
            return Ok(AddressInfo {
                input: normalized.to_string(),
                address_type: AddressType::DbPublicKey,
                scope: VersionScope::V0Only,
                pkh: "-".to_string(),
                db_public_key: Some(db_pk.to_string()),
            });
        }
    }

    let digest = Digest::try_from(normalized)
        .map_err(|_| AddressError::InvalidAddress(address.to_string()))?;
    let digest = DbDigest::from(digest);

    if let Some(row) = pkh_to_pk::table
        .filter(pkh_to_pk::pkh.eq(digest))
        .first::<PkhToPk>(conn)
        .await
        .optional()?
    {
        Ok(AddressInfo {
            input: normalized.to_string(),
            address_type: AddressType::Pkh,
            scope: VersionScope::V1Only,
            pkh: row.pkh.to_string(),
            db_public_key: Some(row.pk.to_string()),
        })
    } else {
        Ok(AddressInfo {
            input: normalized.to_string(),
            address_type: AddressType::Pkh,
            scope: VersionScope::V1Only,
            pkh: digest.to_string(),
            db_public_key: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel::sql_query;
    use diesel_async::RunQueryDsl;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    const TEST_PKH: &str = "BrsEhMCqBBLyXgoXDYz4QvEGrP7wDYW1d86eiegKxQMr87vzphu3HEg";
    const TEST_PK: &str = "38uf8YFwX8hZJNC6eDum74gUgPSWqXYkntH7bBMkhjFoAuJDd5woqcv6LsomXG926a9UbW5kKn7dXRkjAoeXq28WHKrHbpcD3rFzbPymYwpPPdHTStDvbZsRkzsZGnvtxLJT";
    const OTHER_PKH: &str = "3b3hugV8xcMfGApTUZLEwzfzPLnoDLZpZbSTtSNeEzJAKkCVkfHpuW2";

    fn test_db_path(prefix: &str) -> PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!("iris-blocks-{prefix}-{ts}.sqlite"))
    }

    async fn setup_conn() -> (crate::db::AsyncDbConnection, PathBuf) {
        let path = test_db_path("address-resolve");
        let mut conn = crate::db::new_conn(path.to_str().expect("db path"))
            .await
            .expect("open sqlite");
        crate::db::run_migrations(&mut conn)
            .await
            .expect("run migrations");
        (conn, path)
    }

    #[tokio::test]
    async fn mapped_pkh_resolves_to_v1_only() {
        let (mut conn, path) = setup_conn().await;
        sql_query(
            "INSERT INTO blocks (id, height, version, parent, timestamp, msg, jam)
             VALUES ('b1', 1, 1, 'p0', 0, NULL, x'00')",
        )
        .execute(&mut conn)
        .await
        .expect("insert block");
        sql_query(
            "INSERT INTO pkh_to_pk (pkh, pk, height, block_id)
             VALUES (?1, ?2, 1, 'b1')",
        )
        .bind::<diesel::sql_types::Text, _>(TEST_PKH)
        .bind::<diesel::sql_types::Text, _>(TEST_PK)
        .execute(&mut conn)
        .await
        .expect("insert pkh_to_pk");

        let resolved = resolve_address(&mut conn, TEST_PKH).await.expect("resolve");
        assert!(matches!(resolved.address_type, AddressType::Pkh));
        assert_eq!(resolved.scope, VersionScope::V1Only);
        assert_eq!(resolved.pkh, TEST_PKH);
        assert_eq!(resolved.db_public_key.as_deref(), Some(TEST_PK));

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn mapped_pk_resolves_to_v0_only() {
        let (mut conn, path) = setup_conn().await;
        sql_query(
            "INSERT INTO blocks (id, height, version, parent, timestamp, msg, jam)
             VALUES ('b1', 1, 1, 'p0', 0, NULL, x'00')",
        )
        .execute(&mut conn)
        .await
        .expect("insert block");
        sql_query(
            "INSERT INTO pkh_to_pk (pkh, pk, height, block_id)
             VALUES (?1, ?2, 1, 'b1')",
        )
        .bind::<diesel::sql_types::Text, _>(TEST_PKH)
        .bind::<diesel::sql_types::Text, _>(TEST_PK)
        .execute(&mut conn)
        .await
        .expect("insert pkh_to_pk");

        let resolved = resolve_address(&mut conn, TEST_PK).await.expect("resolve");
        assert!(matches!(resolved.address_type, AddressType::DbPublicKey));
        assert_eq!(resolved.scope, VersionScope::V0Only);
        assert_eq!(resolved.pkh, TEST_PKH);
        assert_eq!(resolved.db_public_key.as_deref(), Some(TEST_PK));

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn unmapped_pkh_resolves_to_v1_only_without_pk() {
        let (mut conn, path) = setup_conn().await;
        let resolved = resolve_address(&mut conn, OTHER_PKH)
            .await
            .expect("resolve");
        assert!(matches!(resolved.address_type, AddressType::Pkh));
        assert_eq!(resolved.scope, VersionScope::V1Only);
        assert_eq!(resolved.pkh, OTHER_PKH);
        assert_eq!(resolved.db_public_key, None);

        let _ = std::fs::remove_file(path);
    }
}
