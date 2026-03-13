//! L4 layer: double-entry accounting.

use crate::layers::shared_schema::{DbDigest, DbPublicKey};
use diesel::prelude::*;

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::{DigestSql, PublicKeySql};

    debits (txid, pk) {
        txid -> DigestSql,
        pk -> PublicKeySql,
        sole_owner -> Bool,
        amount -> BigInt,
        fee -> BigInt,
        height -> Integer,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::DigestSql;

    credits (txid, idx) {
        txid -> DigestSql,
        idx -> Integer,
        recipient_type -> Text,
        recipient -> Text,
        amount -> BigInt,
        height -> Integer,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::DigestSql;

    coinbase_credits (block_id, idx) {
        block_id -> DigestSql,
        idx -> Integer,
        recipient_type -> Text,
        recipient -> Text,
        amount -> BigInt,
        height -> Integer,
    }
}

diesel::allow_tables_to_appear_in_same_query!(debits, credits, coinbase_credits);

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = debits, treat_none_as_default_value = false)]
pub struct Debit {
    pub txid: DbDigest,
    pub pk: DbPublicKey,
    pub sole_owner: bool,
    pub amount: i64,
    pub fee: i64,
    pub height: i32,
}

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = credits, treat_none_as_default_value = false)]
pub struct Credit {
    pub txid: DbDigest,
    pub idx: i32,
    pub recipient_type: String,
    pub recipient: String,
    pub amount: i64,
    pub height: i32,
}

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = coinbase_credits, treat_none_as_default_value = false)]
pub struct CoinbaseCredit {
    pub block_id: DbDigest,
    pub idx: i32,
    pub recipient_type: String,
    pub recipient: String,
    pub amount: i64,
    pub height: i32,
}
