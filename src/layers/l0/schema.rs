//! L0 layer: blocks and transactions.

use diesel::prelude::*;

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

use crate::layers::shared_schema::{BlockId, TxId};

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::DigestSql;

    blocks (id) {
        id -> DigestSql,
        height -> Integer,
        version -> Integer,
        parent -> DigestSql,
        timestamp -> BigInt,
        msg -> Nullable<Text>,
        jam -> Binary,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::DigestSql;

    transactions (id) {
        id -> DigestSql,
        block_id -> DigestSql,
        height -> Integer,
        version -> Integer,
        fee -> BigInt,
        total_size -> Integer,
        jam -> Binary,
    }
}

diesel::joinable!(transactions -> blocks (block_id));
diesel::allow_tables_to_appear_in_same_query!(blocks, transactions);

// ---------------------------------------------------------------------------
// Queryable models
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = blocks, treat_none_as_default_value = false)]
pub struct Block {
    pub id: BlockId,
    pub height: i32,
    pub version: i32,
    pub parent: BlockId,
    pub timestamp: i64,
    pub msg: Option<String>,
    pub jam: Vec<u8>,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = blocks)]
pub struct JamlessBlock {
    pub id: BlockId,
    pub height: i32,
    pub version: i32,
    pub parent: BlockId,
    pub timestamp: i64,
    pub msg: Option<String>,
}

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = transactions, treat_none_as_default_value = false)]
pub struct Transaction {
    pub id: TxId,
    pub block_id: BlockId,
    pub height: i32,
    pub version: i32,
    pub fee: i64,
    pub total_size: i32,
    pub jam: Vec<u8>,
}
