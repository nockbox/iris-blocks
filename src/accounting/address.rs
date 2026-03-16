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

    // First try to treat the input as a DB-formatted PK.
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
                scope: VersionScope::All,
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
        // TODO: restrict to v1 only?
        return Ok(AddressInfo {
            input: normalized.to_string(),
            address_type: AddressType::Pkh,
            scope: VersionScope::All,
            pkh: row.pkh.to_string(),
            db_public_key: Some(row.pk.to_string()),
        });
    } else {
        return Ok(AddressInfo {
            input: normalized.to_string(),
            address_type: AddressType::Pkh,
            scope: VersionScope::All,
            pkh: digest.to_string(),
            db_public_key: None,
        });
    }
}
