//! L2 layer: transaction internals + hash reversals + spend conditions.

use crate::layers::shared_schema::{DbDigest, DbPublicKey};
use diesel::prelude::*;

// --- L2.1: Transaction internals ---

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::DigestSql;

    tx_spends (txid, z) {
        txid -> DigestSql,
        z -> Integer,
        version -> Integer,
        first -> DigestSql,
        last -> DigestSql,
        fee -> BigInt,
        height -> Integer,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::DigestSql;

    tx_seeds (txid, z, idx) {
        txid -> DigestSql,
        z -> Integer,
        idx -> Integer,
        amount -> BigInt,
        first -> DigestSql,
        height -> Integer,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::DigestSql;

    tx_outputs (txid, idx) {
        txid -> DigestSql,
        idx -> Integer,
        first -> DigestSql,
        last -> DigestSql,
        assets -> BigInt,
        height -> Integer,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::{DigestSql, PublicKeySql};

    tx_signers (txid, z, pk) {
        txid -> DigestSql,
        z -> Integer,
        pk -> PublicKeySql,
        height -> Integer,
    }
}

// --- L2.2: Hash reversals ---

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::DigestSql;

    name_to_lock (first) {
        first -> DigestSql,
        root -> DigestSql,
        height -> Integer,
        block_id -> DigestSql,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::{DigestSql, PublicKeySql};

    pkh_to_pk (pkh) {
        pkh -> DigestSql,
        pk -> PublicKeySql,
        height -> Integer,
        block_id -> DigestSql,
    }
}

// --- L2.3: Spend condition retrieval ---

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::DigestSql;

    lock_tree (root, axis) {
        root -> DigestSql,
        height -> Integer,
        axis -> Integer,
        hash -> DigestSql,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::DigestSql;

    spend_conditions (hash) {
        hash -> DigestSql,
        txid -> DigestSql,
        z -> Nullable<Integer>,
        height -> Integer,
        jam -> Binary,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    tx_spends,
    tx_seeds,
    tx_outputs,
    tx_signers,
    name_to_lock,
    pkh_to_pk,
    lock_tree,
    spend_conditions
);

// --- L2.1 structs ---

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = tx_spends, treat_none_as_default_value = false)]
pub struct TxSpend {
    pub txid: DbDigest,
    pub z: i32,
    pub version: i32,
    pub first: DbDigest,
    pub last: DbDigest,
    pub fee: i64,
    pub height: i32,
}

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = tx_seeds, treat_none_as_default_value = false)]
pub struct TxSeed {
    pub txid: DbDigest,
    pub z: i32,
    pub idx: i32,
    pub amount: i64,
    pub first: DbDigest,
    pub height: i32,
}

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = tx_outputs, treat_none_as_default_value = false)]
pub struct TxOutput {
    pub txid: DbDigest,
    pub idx: i32,
    pub first: DbDigest,
    pub last: DbDigest,
    pub assets: i64,
    pub height: i32,
}

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = tx_signers, treat_none_as_default_value = false)]
pub struct TxSigner {
    pub txid: DbDigest,
    pub z: i32,
    pub pk: DbPublicKey,
    pub height: i32,
}

// --- L2.2 structs ---

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = name_to_lock, treat_none_as_default_value = false)]
pub struct NameToLock {
    pub first: DbDigest,
    pub root: DbDigest,
    pub height: i32,
    pub block_id: DbDigest,
}

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = pkh_to_pk, treat_none_as_default_value = false)]
pub struct PkhToPk {
    pub pkh: DbDigest,
    pub pk: DbPublicKey,
    pub height: i32,
    pub block_id: DbDigest,
}

// --- L2.3 structs ---

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = lock_tree, treat_none_as_default_value = false)]
pub struct LockTree {
    pub root: DbDigest,
    pub height: i32,
    pub axis: i32,
    pub hash: DbDigest,
}

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = spend_conditions, treat_none_as_default_value = false)]
pub struct SpendConditionRow {
    pub hash: DbDigest,
    pub txid: DbDigest,
    pub z: Option<i32>,
    pub height: i32,
    pub jam: Vec<u8>,
}
