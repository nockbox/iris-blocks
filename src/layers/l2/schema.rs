//! L2 layer: transaction internals.

use crate::layers::shared_schema::{NoteName, TxId};
use diesel::prelude::*;

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

    tx_seeds (txid, idx) {
        txid -> DigestSql,
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
    use crate::layers::shared_schema::sql_types::DigestSql;

    tx_signers (txid, z, pk) {
        txid -> DigestSql,
        z -> Integer,
        pk -> Text,
        height -> Integer,
    }
}

diesel::allow_tables_to_appear_in_same_query!(tx_spends, tx_seeds, tx_outputs, tx_signers);

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = tx_spends, treat_none_as_default_value = false)]
pub struct TxSpend {
    pub txid: TxId,
    pub z: i32,
    pub version: i32,
    pub first: NoteName,
    pub last: NoteName,
    pub fee: i64,
    pub height: i32,
}

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = tx_seeds, treat_none_as_default_value = false)]
pub struct TxSeed {
    pub txid: TxId,
    pub idx: i32,
    pub amount: i64,
    pub first: NoteName,
    pub height: i32,
}

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = tx_outputs, treat_none_as_default_value = false)]
pub struct TxOutput {
    pub txid: TxId,
    pub idx: i32,
    pub first: NoteName,
    pub last: NoteName,
    pub assets: i64,
    pub height: i32,
}

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = tx_signers, treat_none_as_default_value = false)]
pub struct TxSigner {
    pub txid: TxId,
    pub z: i32,
    pub pk: String,
    pub height: i32,
}
