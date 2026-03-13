//! L3 layer: double-entry accounting ledger.

use crate::layers::shared_schema::DbDigest;
use diesel::prelude::*;

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::DigestSql;

    credits (txid, first, height) {
        txid -> Nullable<DigestSql>,
        first -> DigestSql,
        height -> Integer,
        block_id -> DigestSql,
        amount -> BigInt,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::DigestSql;

    debits (txid, first, height) {
        txid -> Nullable<DigestSql>,
        first -> Nullable<DigestSql>,
        height -> Integer,
        block_id -> DigestSql,
        amount -> BigInt,
        fee -> BigInt,
    }
}

diesel::allow_tables_to_appear_in_same_query!(credits, debits);

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = credits, treat_none_as_default_value = false)]
pub struct Credit {
    pub txid: Option<DbDigest>,
    pub first: DbDigest,
    pub height: i32,
    pub block_id: DbDigest,
    pub amount: i64,
}

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = debits, treat_none_as_default_value = false)]
pub struct Debit {
    pub txid: Option<DbDigest>,
    pub first: Option<DbDigest>,
    pub height: i32,
    pub block_id: DbDigest,
    pub amount: i64,
    pub fee: i64,
}
