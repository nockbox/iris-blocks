use crate::layers::l3::schema::{pk_to_pkh, PkToPkh};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use iris_crypto::PublicKey;
use iris_ztd::{jam, Digest, Hashable, NounEncode};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Clone, Serialize)]
pub enum AddressType {
    Pkh,
    DbPublicKey,
    V0RawPublicKey,
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

pub fn convert_raw_v0_pk_to_db_pk(raw_pk: &str) -> Result<(String, String), AddressError> {
    let raw_bytes = bs58::decode(raw_pk)
        .into_vec()
        .map_err(|_| AddressError::InvalidAddress(raw_pk.to_string()))?;

    // Cheetah public keys are 97 bytes in big-endian encoding (1 prefix + 96 coordinate bytes).
    // Reject mismatched lengths so we never panic inside iris-crypto slicing.
    if raw_bytes.len() != 97 {
        return Err(AddressError::InvalidAddress(raw_pk.to_string()));
    }

    let pk = PublicKey::from_be_bytes(&raw_bytes);
    let db_pk = bs58::encode(jam(pk.to_noun())).into_string();
    let pkh = pk.hash().to_string();
    Ok((db_pk, pkh))
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
    if let Ok(db_pk) = crate::layers::shared_schema::DbPublicKey::try_from(normalized) {
        if let Some(row) = pk_to_pkh::table
            .filter(pk_to_pkh::pk.eq(db_pk))
            .first::<PkToPkh>(conn)
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
        }
    }

    // Next, treat input as PKH digest if possible.
    if Digest::try_from(normalized).is_ok() {
        return Ok(AddressInfo {
            input: normalized.to_string(),
            address_type: AddressType::Pkh,
            scope: VersionScope::V1Only,
            pkh: normalized.to_string(),
            db_public_key: None,
        });
    }

    // Finally, try legacy V0 raw PK (bs58(pk.to_be_bytes()) form).
    if let Ok((db_pk, pkh)) = convert_raw_v0_pk_to_db_pk(normalized) {
        return Ok(AddressInfo {
            input: normalized.to_string(),
            address_type: AddressType::V0RawPublicKey,
            scope: VersionScope::V0Only,
            pkh,
            db_public_key: Some(db_pk),
        });
    }

    Err(AddressError::InvalidAddress(address.to_string()))
}
