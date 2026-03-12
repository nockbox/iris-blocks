//! L1 layer: balance tracking.

use crate::layers::{
    l0::schema::{Block, Transaction},
    shared_schema::{BlockId, NoteName, TxId},
};
use diesel::prelude::*;

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::DigestSql;

    notes (first, last) {
        first -> DigestSql,
        last -> DigestSql,
        version -> Integer,
        assets -> BigInt,
        coinbase -> Bool,
        created_txid -> Nullable<DigestSql>,
        spent_txid -> Nullable<DigestSql>,
        created_height -> Integer,
        spent_height -> Nullable<Integer>,
        created_bid -> DigestSql,
        spent_bid -> Nullable<DigestSql>,
        jam -> Binary,
    }
}

// ---------------------------------------------------------------------------
// Queryable models
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = notes, treat_none_as_default_value = false)]
pub struct Note {
    pub first: NoteName,
    pub last: NoteName,
    pub version: i32,
    pub assets: i64,
    pub coinbase: bool,
    pub created_txid: Option<TxId>,
    pub spent_txid: Option<TxId>,
    pub created_height: i32,
    pub spent_height: Option<i32>,
    pub created_bid: BlockId,
    pub spent_bid: Option<BlockId>,
    pub jam: Vec<u8>,
}

impl Note {
    pub fn coinbase(block: &Block, note: iris_nockchain_types::Note) -> Self {
        use iris_ztd::{jam, NounEncode};
        Self {
            first: note.name().first.into(),
            last: note.name().last.into(),
            version: note.version() as _,
            assets: note.assets().0 as _,
            coinbase: true,
            created_txid: None,
            spent_txid: None,
            created_height: block.height,
            spent_height: None,
            created_bid: block.id,
            spent_bid: None,
            jam: jam(note.to_noun()),
        }
    }

    pub fn tx_output(block: &Block, tx: &Transaction, note: iris_nockchain_types::Note) -> Self {
        use iris_ztd::{jam, NounEncode};
        Self {
            first: note.name().first.into(),
            last: note.name().last.into(),
            version: note.version() as _,
            assets: note.assets().0 as _,
            coinbase: false,
            created_txid: Some(tx.id),
            spent_txid: None,
            created_height: block.height,
            spent_height: None,
            created_bid: block.id,
            spent_bid: None,
            jam: jam(note.to_noun()),
        }
    }
}

#[derive(Debug, Clone, Queryable, Selectable, AsChangeset, Identifiable)]
#[diesel(table_name = notes, primary_key(first, last), treat_none_as_default_value = false)]
pub struct SpendNote {
    pub first: NoteName,
    pub last: NoteName,
    pub spent_height: i32,
    pub spent_bid: BlockId,
    pub spent_txid: TxId,
}
