use crate::address::{AddressInfo, VersionScope};
use chrono::{DateTime, TimeZone, Utc};
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Bool, Integer, Nullable, Text};
use diesel_async::RunQueryDsl;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueryError {
    #[error(transparent)]
    Diesel(#[from] diesel::result::Error),
    #[error("not found: {0}")]
    NotFound(String),
}

#[derive(Debug, Clone, Serialize)]
pub struct WalletBalance {
    pub address: AddressInfo,
    /// Primary balance: sum of all unspent note assets (V0 + V1).
    pub balance_nicks: i64,
    pub unspent_note_count: i64,
    pub unspent_v0_nicks: i64,
    pub unspent_v1_nicks: i64,
    /// Double-entry accounting fields. received - spent MUST equal balance_nicks.
    pub received_nicks: i64,
    pub tx_credits_nicks: i64,
    pub coinbase_credits_nicks: i64,
    pub spent_nicks: i64,
    pub fees_nicks: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TxSpendDetail {
    pub z: i32,
    pub version: i32,
    pub first: String,
    pub last: String,
    pub fee_nicks: i64,
    pub note_assets_nicks: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TxSignerDetail {
    pub z: i32,
    pub pk: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TxOutputDetail {
    pub idx: i32,
    pub first: String,
    pub last: String,
    pub assets_nicks: i64,
    pub recipient_type: Option<String>,
    pub recipient: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TxCreditDetail {
    pub idx: i32,
    pub recipient_type: String,
    pub recipient: String,
    pub amount_nicks: i64,
    pub block_height: i32,
    pub block_timestamp: i64,
    pub block_unix_timestamp: Option<i64>,
    pub block_time_utc: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TxDebitDetail {
    pub pk: String,
    pub sole_owner: bool,
    pub amount_nicks: i64,
    pub fee_nicks: i64,
    pub block_height: i32,
    pub block_timestamp: i64,
    pub block_unix_timestamp: Option<i64>,
    pub block_time_utc: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransactionDetail {
    pub txid: String,
    pub block_id: String,
    pub block_height: i32,
    pub block_timestamp: i64,
    pub block_unix_timestamp: Option<i64>,
    pub block_time_utc: String,
    pub version: i32,
    pub fee_nicks: i64,
    pub total_size: i32,
    pub spends: Vec<TxSpendDetail>,
    pub signers: Vec<TxSignerDetail>,
    pub outputs: Vec<TxOutputDetail>,
    pub credits: Vec<TxCreditDetail>,
    pub debits: Vec<TxDebitDetail>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CoinbaseCreditDetail {
    pub idx: i32,
    pub recipient_type: String,
    pub recipient: String,
    pub amount_nicks: i64,
    pub block_height: i32,
    pub block_timestamp: i64,
    pub block_unix_timestamp: Option<i64>,
    pub block_time_utc: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BlockTransaction {
    pub txid: String,
    pub version: i32,
    pub fee_nicks: i64,
    pub total_size: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct BlockDetail {
    pub id: String,
    pub block_height: i32,
    pub version: i32,
    pub parent: String,
    pub block_timestamp: i64,
    pub block_unix_timestamp: Option<i64>,
    pub block_time_utc: String,
    pub msg: Option<String>,
    pub transactions: Vec<BlockTransaction>,
    pub coinbase_credits: Vec<CoinbaseCreditDetail>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LayerStatus {
    pub layer: String,
    pub next_block_height: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableCount {
    pub table: String,
    pub count: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SyncStatus {
    pub layers: Vec<LayerStatus>,
    pub table_counts: Vec<TableCount>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LedgerEntry {
    pub block_height: i32,
    pub block_timestamp: i64,
    pub block_unix_timestamp: Option<i64>,
    pub block_time_utc: String,
    pub entry_type: String,
    pub txid: Option<String>,
    pub block_id: Option<String>,
    pub recipient_type: Option<String>,
    pub recipient: Option<String>,
    pub amount_nicks: i64,
    pub fee_nicks: i64,
    pub counterparties: Option<String>,
    pub running_balance_nicks: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalletTxSummary {
    pub txid: String,
    pub first_block_height: i32,
    pub first_block_timestamp: i64,
    pub first_block_unix_timestamp: Option<i64>,
    pub first_block_time_utc: String,
    pub direction: String,
    pub incoming_nicks: i64,
    pub outgoing_nicks: i64,
    pub fee_nicks: i64,
    pub net_nicks: i64,
}

fn chain_timestamp_to_unix_seconds(ts: i64) -> Option<i64> {
    // Chain timestamps are stored in a biased @da-like second format (u64).
    // The bias/epoch is not 2^63 relative to unix; it is the @da unix offset.
    // See urbit @da epoch constant: 0x8000000cce9e0d80.
    const DA_UNIX_EPOCH_BIASED_SECONDS: i128 = 0x8000_000c_ce9e_0d80;

    let raw_u64 = ts as u64;
    let unix_seconds_i128 = raw_u64 as i128 - DA_UNIX_EPOCH_BIASED_SECONDS;
    i64::try_from(unix_seconds_i128).ok()
}

fn format_chain_timestamp_utc(ts: i64) -> String {
    let Some(unix_seconds) = chain_timestamp_to_unix_seconds(ts) else {
        return format!("invalid({ts})");
    };
    let dt_opt: Option<DateTime<Utc>> = Utc.timestamp_opt(unix_seconds, 0).single();
    match dt_opt {
        Some(dt) => dt.to_rfc3339(),
        None => format!("invalid({ts})"),
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AuditReport {
    pub balance: WalletBalance,
    pub transactions: Vec<WalletTxSummary>,
    pub ledger: Vec<LedgerEntry>,
}

#[derive(QueryableByName)]
struct SumCountRow {
    #[diesel(sql_type = BigInt)]
    sum_nicks: i64,
    #[diesel(sql_type = BigInt)]
    note_count: i64,
}

#[derive(QueryableByName)]
struct VersionSumRow {
    #[diesel(sql_type = Integer)]
    version: i32,
    #[diesel(sql_type = BigInt)]
    sum_nicks: i64,
}

#[derive(QueryableByName)]
struct SumRow {
    #[diesel(sql_type = BigInt)]
    sum_nicks: i64,
}

#[derive(QueryableByName)]
struct TxBaseRow {
    #[diesel(sql_type = Text)]
    txid: String,
    #[diesel(sql_type = Text)]
    block_id: String,
    #[diesel(sql_type = Integer)]
    height: i32,
    #[diesel(sql_type = BigInt)]
    block_timestamp: i64,
    #[diesel(sql_type = Integer)]
    version: i32,
    #[diesel(sql_type = BigInt)]
    fee: i64,
    #[diesel(sql_type = Integer)]
    total_size: i32,
}

#[derive(QueryableByName)]
struct TxSpendRow {
    #[diesel(sql_type = Integer)]
    z: i32,
    #[diesel(sql_type = Integer)]
    version: i32,
    #[diesel(sql_type = Text)]
    first: String,
    #[diesel(sql_type = Text)]
    last: String,
    #[diesel(sql_type = BigInt)]
    fee: i64,
    #[diesel(sql_type = BigInt)]
    note_assets: i64,
}

#[derive(QueryableByName)]
struct TxSignerRow {
    #[diesel(sql_type = Integer)]
    z: i32,
    #[diesel(sql_type = Text)]
    pk: String,
}

#[derive(QueryableByName)]
struct TxOutputRow {
    #[diesel(sql_type = Integer)]
    idx: i32,
    #[diesel(sql_type = Text)]
    first: String,
    #[diesel(sql_type = Text)]
    last: String,
    #[diesel(sql_type = BigInt)]
    assets: i64,
    #[diesel(sql_type = Nullable<Text>)]
    recipient_type: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    recipient: Option<String>,
}

#[derive(QueryableByName)]
struct TxCreditRow {
    #[diesel(sql_type = Integer)]
    idx: i32,
    #[diesel(sql_type = Text)]
    recipient_type: String,
    #[diesel(sql_type = Text)]
    recipient: String,
    #[diesel(sql_type = BigInt)]
    amount: i64,
    #[diesel(sql_type = Integer)]
    block_height: i32,
    #[diesel(sql_type = BigInt)]
    block_timestamp: i64,
}

#[derive(QueryableByName)]
struct TxDebitRow {
    #[diesel(sql_type = Text)]
    pk: String,
    #[diesel(sql_type = Bool)]
    sole_owner: bool,
    #[diesel(sql_type = BigInt)]
    amount: i64,
    #[diesel(sql_type = BigInt)]
    fee: i64,
    #[diesel(sql_type = Integer)]
    block_height: i32,
    #[diesel(sql_type = BigInt)]
    block_timestamp: i64,
}

#[derive(QueryableByName)]
struct BlockBaseRow {
    #[diesel(sql_type = Text)]
    id: String,
    #[diesel(sql_type = Integer)]
    height: i32,
    #[diesel(sql_type = Integer)]
    version: i32,
    #[diesel(sql_type = Text)]
    parent: String,
    #[diesel(sql_type = BigInt)]
    timestamp: i64,
    #[diesel(sql_type = Nullable<Text>)]
    msg: Option<String>,
}

#[derive(QueryableByName)]
struct BlockTxRow {
    #[diesel(sql_type = Text)]
    txid: String,
    #[diesel(sql_type = Integer)]
    version: i32,
    #[diesel(sql_type = BigInt)]
    fee: i64,
    #[diesel(sql_type = Integer)]
    total_size: i32,
}

#[derive(QueryableByName)]
struct CoinbaseRow {
    #[diesel(sql_type = Integer)]
    idx: i32,
    #[diesel(sql_type = Text)]
    recipient_type: String,
    #[diesel(sql_type = Text)]
    recipient: String,
    #[diesel(sql_type = BigInt)]
    amount: i64,
    #[diesel(sql_type = Integer)]
    block_height: i32,
    #[diesel(sql_type = BigInt)]
    block_timestamp: i64,
}

#[derive(QueryableByName)]
struct LayerRow {
    #[diesel(sql_type = Text)]
    layer: String,
    #[diesel(sql_type = Integer)]
    next_block_height: i32,
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

#[derive(QueryableByName)]
struct LedgerRow {
    #[diesel(sql_type = Integer)]
    block_height: i32,
    #[diesel(sql_type = BigInt)]
    block_timestamp: i64,
    #[diesel(sql_type = Text)]
    entry_type: String,
    #[diesel(sql_type = Nullable<Text>)]
    txid: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    block_id: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    recipient_type: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    recipient: Option<String>,
    #[diesel(sql_type = BigInt)]
    amount_nicks: i64,
    #[diesel(sql_type = BigInt)]
    fee_nicks: i64,
    #[diesel(sql_type = Nullable<Text>)]
    counterparties: Option<String>,
}

pub async fn wallet_balance(
    conn: &mut crate::db::AsyncDbConnection,
    address: AddressInfo,
) -> Result<WalletBalance, QueryError> {
    let pkh = address.pkh.clone();
    let scope = address.scope.clone();
    let note_version_filter = match scope {
        VersionScope::All => "",
        VersionScope::V0Only => " AND n.version = 0",
        VersionScope::V1Only => " AND n.version >= 1",
    };

    let unspent_query = format!(
        "SELECT COALESCE(SUM(n.assets), 0) AS sum_nicks, COUNT(*) AS note_count
         FROM notes n
         INNER JOIN name_owners no ON n.first = no.first
         WHERE no.pkh = ?1
           AND n.spent_txid IS NULL{note_version_filter}"
    );
    let unspent = sql_query(unspent_query)
    .bind::<Text, _>(pkh.clone())
    .get_result::<SumCountRow>(conn)
    .await?;

    let by_version = sql_query(
        "SELECT n.version AS version, COALESCE(SUM(n.assets), 0) AS sum_nicks
         FROM notes n
         INNER JOIN name_owners no ON n.first = no.first
         WHERE no.pkh = ?1
           AND n.spent_txid IS NULL
         GROUP BY n.version",
    )
    .bind::<Text, _>(pkh.clone())
    .load::<VersionSumRow>(conn)
    .await?;

    let (unspent_v0_nicks, unspent_v1_nicks) = if scope == VersionScope::All {
        let mut v0 = 0i64;
        let mut v1 = 0i64;
        for row in by_version {
            if row.version == 0 {
                v0 = row.sum_nicks;
            } else {
                v1 += row.sum_nicks;
            }
        }
        (v0, v1)
    } else {
        (0, 0)
    };

    let tx_credits_sql = match scope {
        VersionScope::V0Only => {
            "SELECT COALESCE(SUM(amount), 0) AS sum_nicks
             FROM credits
             WHERE recipient_type = 'v0pk'
               AND recipient IN (SELECT pk FROM pk_to_pkh WHERE pkh = ?1)"
        }
        VersionScope::V1Only => {
            "SELECT COALESCE(SUM(amount), 0) AS sum_nicks
             FROM credits
             WHERE (recipient_type = 'pk' AND recipient IN (
                     SELECT pk FROM pk_to_pkh WHERE pkh = ?1
                   ))
                OR (recipient_type = 'pkh' AND recipient = ?1)"
        }
        VersionScope::All => {
            "SELECT COALESCE(SUM(amount), 0) AS sum_nicks
             FROM credits
             WHERE (recipient_type IN ('pk', 'v0pk') AND recipient IN (
                     SELECT pk FROM pk_to_pkh WHERE pkh = ?1
                   ))
                OR (recipient_type = 'pkh' AND recipient = ?1)"
        }
    };
    let tx_credits_nicks = sql_query(tx_credits_sql)
    .bind::<Text, _>(pkh.clone())
    .get_result::<SumRow>(conn)
    .await?
    .sum_nicks;

    let coinbase_sql = match scope {
        VersionScope::V0Only => {
            "SELECT COALESCE(SUM(amount), 0) AS sum_nicks
             FROM coinbase_credits
             WHERE recipient_type = 'v0pk'
               AND recipient IN (SELECT pk FROM pk_to_pkh WHERE pkh = ?1)"
        }
        VersionScope::V1Only => {
            "SELECT COALESCE(SUM(amount), 0) AS sum_nicks
             FROM coinbase_credits
             WHERE (recipient_type = 'pk' AND recipient IN (
                     SELECT pk FROM pk_to_pkh WHERE pkh = ?1
                   ))
                OR (recipient_type = 'pkh' AND recipient = ?1)"
        }
        VersionScope::All => {
            "SELECT COALESCE(SUM(amount), 0) AS sum_nicks
             FROM coinbase_credits
             WHERE (recipient_type IN ('pk', 'v0pk') AND recipient IN (
                     SELECT pk FROM pk_to_pkh WHERE pkh = ?1
                   ))
                OR (recipient_type = 'pkh' AND recipient = ?1)"
        }
    };
    let coinbase_credits_nicks = sql_query(coinbase_sql)
    .bind::<Text, _>(pkh.clone())
    .get_result::<SumRow>(conn)
    .await?
    .sum_nicks;

    let spent_query = format!(
        "SELECT COALESCE(SUM(n.assets), 0) AS sum_nicks
         FROM notes n
         INNER JOIN name_owners no ON n.first = no.first
         WHERE no.pkh = ?1
           AND n.spent_txid IS NOT NULL{note_version_filter}"
    );
    let spent_nicks = sql_query(spent_query)
    .bind::<Text, _>(pkh.clone())
    .get_result::<SumRow>(conn)
    .await?
    .sum_nicks;

    let fees_query = format!(
        "SELECT COALESCE(SUM(t.fee), 0) AS sum_nicks
         FROM transactions t
         WHERE t.id IN (
             SELECT DISTINCT n.spent_txid
             FROM notes n
             INNER JOIN name_owners no ON n.first = no.first
             WHERE no.pkh = ?1
               AND n.spent_txid IS NOT NULL{note_version_filter}
         )"
    );
    let fees_nicks = sql_query(fees_query)
        .bind::<Text, _>(pkh.clone())
        .get_result::<SumRow>(conn)
        .await?
        .sum_nicks;

    let balance_nicks = unspent.sum_nicks;
    let received_nicks = tx_credits_nicks + coinbase_credits_nicks;
    let accounting_nicks = received_nicks - spent_nicks;
    if balance_nicks != accounting_nicks {
        tracing::warn!(
            balance_nicks,
            accounting_nicks,
            "accounting mismatch: received - spent != unspent notes"
        );
    }

    Ok(WalletBalance {
        address,
        balance_nicks,
        unspent_note_count: unspent.note_count,
        unspent_v0_nicks,
        unspent_v1_nicks,
        received_nicks,
        tx_credits_nicks,
        coinbase_credits_nicks,
        spent_nicks,
        fees_nicks,
    })
}

pub async fn resolve_address(
    conn: &mut crate::db::AsyncDbConnection,
    address: &str,
) -> Result<AddressInfo, crate::address::AddressError> {
    crate::address::resolve_address(conn, address).await
}

pub async fn transaction_detail(
    conn: &mut crate::db::AsyncDbConnection,
    txid: &str,
) -> Result<TransactionDetail, QueryError> {
    let base = sql_query(
        "SELECT t.id AS txid, t.block_id, t.height, COALESCE(b.timestamp, 0) AS block_timestamp, t.version, t.fee, t.total_size
         FROM transactions t
         LEFT JOIN blocks b ON b.height = t.height
         WHERE t.id = ?1
         LIMIT 1",
    )
    .bind::<Text, _>(txid.to_string())
    .get_result::<TxBaseRow>(conn)
    .await
    .optional()?;

    let Some(base) = base else {
        return Err(QueryError::NotFound(format!("transaction {txid}")));
    };

    let spends = sql_query(
        "SELECT s.z, s.version, s.first, s.last, s.fee, COALESCE(n.assets, 0) AS note_assets
         FROM tx_spends s
         LEFT JOIN notes n ON n.first = s.first AND n.last = s.last
         WHERE s.txid = ?1
         ORDER BY s.z",
    )
    .bind::<Text, _>(txid.to_string())
    .load::<TxSpendRow>(conn)
    .await?
    .into_iter()
    .map(|r| TxSpendDetail {
        z: r.z,
        version: r.version,
        first: r.first,
        last: r.last,
        fee_nicks: r.fee,
        note_assets_nicks: r.note_assets,
    })
    .collect();

    let signers = sql_query(
        "SELECT z, pk
         FROM tx_signers
         WHERE txid = ?1
         ORDER BY z, pk",
    )
    .bind::<Text, _>(txid.to_string())
    .load::<TxSignerRow>(conn)
    .await?
    .into_iter()
    .map(|r| TxSignerDetail { z: r.z, pk: r.pk })
    .collect();

    let outputs = sql_query(
        "SELECT o.idx, o.first, o.last, o.assets, c.recipient_type, c.recipient
         FROM tx_outputs o
         LEFT JOIN credits c ON c.txid = o.txid AND c.idx = o.idx
         WHERE o.txid = ?1
         ORDER BY o.idx",
    )
    .bind::<Text, _>(txid.to_string())
    .load::<TxOutputRow>(conn)
    .await?
    .into_iter()
    .map(|r| TxOutputDetail {
        idx: r.idx,
        first: r.first,
        last: r.last,
        assets_nicks: r.assets,
        recipient_type: r.recipient_type,
        recipient: r.recipient,
    })
    .collect();

    let credits = sql_query(
        "SELECT c.idx, c.recipient_type, c.recipient, c.amount, c.height AS block_height, COALESCE(b.timestamp, 0) AS block_timestamp
         FROM credits c
         LEFT JOIN blocks b ON b.height = c.height
         WHERE c.txid = ?1
         ORDER BY c.idx",
    )
    .bind::<Text, _>(txid.to_string())
    .load::<TxCreditRow>(conn)
    .await?
    .into_iter()
    .map(|r| TxCreditDetail {
        idx: r.idx,
        recipient_type: r.recipient_type,
        recipient: r.recipient,
        amount_nicks: r.amount,
        block_height: r.block_height,
        block_timestamp: r.block_timestamp,
        block_unix_timestamp: chain_timestamp_to_unix_seconds(r.block_timestamp),
        block_time_utc: format_chain_timestamp_utc(r.block_timestamp),
    })
    .collect();

    let debits = sql_query(
        "SELECT d.pk, d.sole_owner, d.amount, d.fee, d.height AS block_height, COALESCE(b.timestamp, 0) AS block_timestamp
         FROM debits d
         LEFT JOIN blocks b ON b.height = d.height
         WHERE d.txid = ?1
         ORDER BY d.pk",
    )
    .bind::<Text, _>(txid.to_string())
    .load::<TxDebitRow>(conn)
    .await?
    .into_iter()
    .map(|r| TxDebitDetail {
        pk: r.pk,
        sole_owner: r.sole_owner,
        amount_nicks: r.amount,
        fee_nicks: r.fee,
        block_height: r.block_height,
        block_timestamp: r.block_timestamp,
        block_unix_timestamp: chain_timestamp_to_unix_seconds(r.block_timestamp),
        block_time_utc: format_chain_timestamp_utc(r.block_timestamp),
    })
    .collect();

    Ok(TransactionDetail {
        txid: base.txid,
        block_id: base.block_id,
        block_height: base.height,
        block_timestamp: base.block_timestamp,
        block_unix_timestamp: chain_timestamp_to_unix_seconds(base.block_timestamp),
        block_time_utc: format_chain_timestamp_utc(base.block_timestamp),
        version: base.version,
        fee_nicks: base.fee,
        total_size: base.total_size,
        spends,
        signers,
        outputs,
        credits,
        debits,
    })
}

pub async fn block_detail(
    conn: &mut crate::db::AsyncDbConnection,
    block: &str,
) -> Result<BlockDetail, QueryError> {
    let maybe_height = block.parse::<i32>().ok();
    let base = if let Some(height) = maybe_height {
        sql_query(
            "SELECT id, height, version, parent, timestamp, msg
             FROM blocks
             WHERE height = ?1
             ORDER BY id
             LIMIT 1",
        )
        .bind::<Integer, _>(height)
        .get_result::<BlockBaseRow>(conn)
        .await
        .optional()?
    } else {
        sql_query(
            "SELECT id, height, version, parent, timestamp, msg
             FROM blocks
             WHERE id = ?1
             LIMIT 1",
        )
        .bind::<Text, _>(block.to_string())
        .get_result::<BlockBaseRow>(conn)
        .await
        .optional()?
    };

    let Some(base) = base else {
        return Err(QueryError::NotFound(format!("block {block}")));
    };

    let block_id = base.id.clone();
    let transactions = sql_query(
        "SELECT id AS txid, version, fee, total_size
         FROM transactions
         WHERE block_id = ?1
         ORDER BY id",
    )
    .bind::<Text, _>(block_id.clone())
    .load::<BlockTxRow>(conn)
    .await?
    .into_iter()
    .map(|r| BlockTransaction {
        txid: r.txid,
        version: r.version,
        fee_nicks: r.fee,
        total_size: r.total_size,
    })
    .collect();

    let coinbase_credits = sql_query(
        "SELECT cc.idx, cc.recipient_type, cc.recipient, cc.amount, cc.height AS block_height, COALESCE(b.timestamp, 0) AS block_timestamp
         FROM coinbase_credits cc
         LEFT JOIN blocks b ON b.height = cc.height
         WHERE cc.block_id = ?1
         ORDER BY cc.idx",
    )
    .bind::<Text, _>(block_id)
    .load::<CoinbaseRow>(conn)
    .await?
    .into_iter()
    .map(|r| CoinbaseCreditDetail {
        idx: r.idx,
        recipient_type: r.recipient_type,
        recipient: r.recipient,
        amount_nicks: r.amount,
        block_height: r.block_height,
        block_timestamp: r.block_timestamp,
        block_unix_timestamp: chain_timestamp_to_unix_seconds(r.block_timestamp),
        block_time_utc: format_chain_timestamp_utc(r.block_timestamp),
    })
    .collect();

    Ok(BlockDetail {
        id: base.id,
        block_height: base.height,
        version: base.version,
        parent: base.parent,
        block_timestamp: base.timestamp,
        block_unix_timestamp: chain_timestamp_to_unix_seconds(base.timestamp),
        block_time_utc: format_chain_timestamp_utc(base.timestamp),
        msg: base.msg,
        transactions,
        coinbase_credits,
    })
}

pub async fn sync_status(conn: &mut crate::db::AsyncDbConnection) -> Result<SyncStatus, QueryError> {
    let layers = sql_query(
        "SELECT layer, next_block_height
         FROM layer_metadata
         ORDER BY layer",
    )
    .load::<LayerRow>(conn)
    .await?
    .into_iter()
    .map(|r| LayerStatus {
        layer: r.layer,
        next_block_height: r.next_block_height,
    })
    .collect::<Vec<_>>();

    let tables = [
        "blocks",
        "transactions",
        "notes",
        "tx_spends",
        "tx_seeds",
        "tx_outputs",
        "tx_signers",
        "lock_names",
        "locks",
        "lock_paths",
        "lock_owners",
        "name_owners",
        "pk_to_pkh",
        "debits",
        "credits",
        "coinbase_credits",
    ];

    let mut table_counts = Vec::with_capacity(tables.len());
    for table in tables {
        let q = format!("SELECT COUNT(*) AS count FROM {table}");
        let row = sql_query(&q).get_result::<CountRow>(conn).await?;
        table_counts.push(TableCount {
            table: table.to_string(),
            count: row.count,
        });
    }

    Ok(SyncStatus {
        layers,
        table_counts,
    })
}

pub async fn audit_report(
    conn: &mut crate::db::AsyncDbConnection,
    address: AddressInfo,
) -> Result<AuditReport, QueryError> {
    let balance = wallet_balance(conn, address.clone()).await?;
    let pkh = address.pkh.clone();
    let scope = address.scope.clone();
    let note_version_filter = match scope {
        VersionScope::All => "",
        VersionScope::V0Only => " AND n.version = 0",
        VersionScope::V1Only => " AND n.version >= 1",
    };

    let mut ledger = Vec::new();

    let credit_sql = match scope {
        VersionScope::V0Only => {
            "SELECT c.height AS block_height,
                    COALESCE(b.timestamp, 0) AS block_timestamp,
                    'credit' AS entry_type,
                    c.txid AS txid,
                    t.block_id AS block_id,
                    c.recipient_type AS recipient_type,
                    c.recipient AS recipient,
                    c.amount AS amount_nicks,
                    0 AS fee_nicks,
                    (SELECT GROUP_CONCAT(DISTINCT s.pk) FROM tx_signers s WHERE s.txid = c.txid) AS counterparties
             FROM credits c
             LEFT JOIN transactions t ON t.id = c.txid
             LEFT JOIN blocks b ON b.height = c.height
             WHERE c.recipient_type = 'v0pk'
               AND c.recipient IN (SELECT pk FROM pk_to_pkh WHERE pkh = ?1)
             ORDER BY c.height, c.txid, c.idx"
        }
        VersionScope::V1Only => {
            "SELECT c.height AS block_height,
                    COALESCE(b.timestamp, 0) AS block_timestamp,
                    'credit' AS entry_type,
                    c.txid AS txid,
                    t.block_id AS block_id,
                    c.recipient_type AS recipient_type,
                    c.recipient AS recipient,
                    c.amount AS amount_nicks,
                    0 AS fee_nicks,
                    (SELECT GROUP_CONCAT(DISTINCT s.pk) FROM tx_signers s WHERE s.txid = c.txid) AS counterparties
             FROM credits c
             LEFT JOIN transactions t ON t.id = c.txid
             LEFT JOIN blocks b ON b.height = c.height
             WHERE (c.recipient_type = 'pk' AND c.recipient IN (
                     SELECT pk FROM pk_to_pkh WHERE pkh = ?1
                   ))
                OR (c.recipient_type = 'pkh' AND c.recipient = ?1)
             ORDER BY c.height, c.txid, c.idx"
        }
        VersionScope::All => {
            "SELECT c.height AS block_height,
                    COALESCE(b.timestamp, 0) AS block_timestamp,
                    'credit' AS entry_type,
                    c.txid AS txid,
                    t.block_id AS block_id,
                    c.recipient_type AS recipient_type,
                    c.recipient AS recipient,
                    c.amount AS amount_nicks,
                    0 AS fee_nicks,
                    (SELECT GROUP_CONCAT(DISTINCT s.pk) FROM tx_signers s WHERE s.txid = c.txid) AS counterparties
             FROM credits c
             LEFT JOIN transactions t ON t.id = c.txid
             LEFT JOIN blocks b ON b.height = c.height
             WHERE (c.recipient_type IN ('pk', 'v0pk') AND c.recipient IN (
                     SELECT pk FROM pk_to_pkh WHERE pkh = ?1
                   ))
                OR (c.recipient_type = 'pkh' AND c.recipient = ?1)
             ORDER BY c.height, c.txid, c.idx"
        }
    };
    let credit_rows = sql_query(credit_sql)
    .bind::<Text, _>(pkh.clone())
    .load::<LedgerRow>(conn)
    .await?;
    ledger.extend(credit_rows.into_iter().map(|r| LedgerEntry {
        block_height: r.block_height,
        block_timestamp: r.block_timestamp,
        block_unix_timestamp: chain_timestamp_to_unix_seconds(r.block_timestamp),
        block_time_utc: format_chain_timestamp_utc(r.block_timestamp),
        entry_type: r.entry_type,
        txid: r.txid,
        block_id: r.block_id,
        recipient_type: r.recipient_type,
        recipient: r.recipient,
        amount_nicks: r.amount_nicks,
        fee_nicks: r.fee_nicks,
        counterparties: r.counterparties,
        running_balance_nicks: 0,
    }));

    let coinbase_sql = match scope {
        VersionScope::V0Only => {
            "SELECT cc.height AS block_height,
                    COALESCE(b.timestamp, 0) AS block_timestamp,
                    'coinbase' AS entry_type,
                    NULL AS txid,
                    cc.block_id AS block_id,
                    cc.recipient_type AS recipient_type,
                    cc.recipient AS recipient,
                    cc.amount AS amount_nicks,
                    0 AS fee_nicks,
                    NULL AS counterparties
             FROM coinbase_credits cc
             LEFT JOIN blocks b ON b.height = cc.height
             WHERE cc.recipient_type = 'v0pk'
               AND cc.recipient IN (SELECT pk FROM pk_to_pkh WHERE pkh = ?1)
             ORDER BY cc.height, cc.idx"
        }
        VersionScope::V1Only => {
            "SELECT cc.height AS block_height,
                    COALESCE(b.timestamp, 0) AS block_timestamp,
                    'coinbase' AS entry_type,
                    NULL AS txid,
                    cc.block_id AS block_id,
                    cc.recipient_type AS recipient_type,
                    cc.recipient AS recipient,
                    cc.amount AS amount_nicks,
                    0 AS fee_nicks,
                    NULL AS counterparties
             FROM coinbase_credits cc
             LEFT JOIN blocks b ON b.height = cc.height
             WHERE (cc.recipient_type = 'pk' AND cc.recipient IN (
                     SELECT pk FROM pk_to_pkh WHERE pkh = ?1
                   ))
                OR (cc.recipient_type = 'pkh' AND cc.recipient = ?1)
             ORDER BY cc.height, cc.idx"
        }
        VersionScope::All => {
            "SELECT cc.height AS block_height,
                    COALESCE(b.timestamp, 0) AS block_timestamp,
                    'coinbase' AS entry_type,
                    NULL AS txid,
                    cc.block_id AS block_id,
                    cc.recipient_type AS recipient_type,
                    cc.recipient AS recipient,
                    cc.amount AS amount_nicks,
                    0 AS fee_nicks,
                    NULL AS counterparties
             FROM coinbase_credits cc
             LEFT JOIN blocks b ON b.height = cc.height
             WHERE (cc.recipient_type IN ('pk', 'v0pk') AND cc.recipient IN (
                     SELECT pk FROM pk_to_pkh WHERE pkh = ?1
                   ))
                OR (cc.recipient_type = 'pkh' AND cc.recipient = ?1)
             ORDER BY cc.height, cc.idx"
        }
    };
    let coinbase_rows = sql_query(coinbase_sql)
    .bind::<Text, _>(pkh.clone())
    .load::<LedgerRow>(conn)
    .await?;
    ledger.extend(coinbase_rows.into_iter().map(|r| LedgerEntry {
        block_height: r.block_height,
        block_timestamp: r.block_timestamp,
        block_unix_timestamp: chain_timestamp_to_unix_seconds(r.block_timestamp),
        block_time_utc: format_chain_timestamp_utc(r.block_timestamp),
        entry_type: r.entry_type,
        txid: r.txid,
        block_id: r.block_id,
        recipient_type: r.recipient_type,
        recipient: r.recipient,
        amount_nicks: r.amount_nicks,
        fee_nicks: r.fee_nicks,
        counterparties: r.counterparties,
        running_balance_nicks: 0,
    }));

    let spent_sql = format!(
        "SELECT n.spent_height AS block_height,
                COALESCE(b.timestamp, 0) AS block_timestamp,
                'debit' AS entry_type,
                n.spent_txid AS txid,
                NULL AS block_id,
                NULL AS recipient_type,
                NULL AS recipient,
                n.assets AS amount_nicks,
                CASE
                    WHEN ROW_NUMBER() OVER (
                        PARTITION BY n.spent_txid
                        ORDER BY n.first, n.last
                    ) = 1 THEN COALESCE(t.fee, 0)
                    ELSE 0
                END AS fee_nicks,
                (SELECT GROUP_CONCAT(DISTINCT c.recipient) FROM credits c WHERE c.txid = n.spent_txid) AS counterparties
         FROM notes n
         INNER JOIN name_owners no ON n.first = no.first
         LEFT JOIN transactions t ON t.id = n.spent_txid
         LEFT JOIN blocks b ON b.height = n.spent_height
         WHERE no.pkh = ?1
           AND n.spent_txid IS NOT NULL{note_version_filter}
         ORDER BY n.spent_height, n.spent_txid, n.first, n.last"
    );
    let spent_rows = sql_query(spent_sql)
    .bind::<Text, _>(pkh)
    .load::<LedgerRow>(conn)
    .await?;

    ledger.extend(spent_rows.into_iter().map(|r| LedgerEntry {
        block_height: r.block_height,
        block_timestamp: r.block_timestamp,
        block_unix_timestamp: chain_timestamp_to_unix_seconds(r.block_timestamp),
        block_time_utc: format_chain_timestamp_utc(r.block_timestamp),
        entry_type: r.entry_type,
        txid: r.txid,
        block_id: r.block_id,
        recipient_type: r.recipient_type,
        recipient: r.recipient,
        amount_nicks: r.amount_nicks,
        fee_nicks: r.fee_nicks,
        counterparties: r.counterparties,
        running_balance_nicks: 0,
    }));

    ledger.sort_by(|a, b| {
        a.block_height
            .cmp(&b.block_height)
            .then_with(|| a.block_timestamp.cmp(&b.block_timestamp))
            .then_with(|| a.txid.cmp(&b.txid))
            .then_with(|| a.entry_type.cmp(&b.entry_type))
    });

    let mut running_balance_nicks = 0i64;
    for entry in &mut ledger {
        if entry.entry_type == "debit" {
            running_balance_nicks -= entry.amount_nicks;
        } else {
            running_balance_nicks += entry.amount_nicks;
        }
        entry.running_balance_nicks = running_balance_nicks;
    }

    use std::collections::BTreeMap;
    let mut tx_map: BTreeMap<String, WalletTxSummary> = BTreeMap::new();
    for entry in &ledger {
        let Some(txid) = entry.txid.clone() else {
            continue;
        };
        let summary = tx_map.entry(txid.clone()).or_insert(WalletTxSummary {
            txid,
            first_block_height: entry.block_height,
            first_block_timestamp: entry.block_timestamp,
            first_block_unix_timestamp: entry.block_unix_timestamp,
            first_block_time_utc: entry.block_time_utc.clone(),
            direction: "incoming".to_string(),
            incoming_nicks: 0,
            outgoing_nicks: 0,
            fee_nicks: 0,
            net_nicks: 0,
        });

        if entry.block_height < summary.first_block_height {
            summary.first_block_height = entry.block_height;
            summary.first_block_timestamp = entry.block_timestamp;
            summary.first_block_unix_timestamp = entry.block_unix_timestamp;
            summary.first_block_time_utc = entry.block_time_utc.clone();
        }
        match entry.entry_type.as_str() {
            "debit" => {
                summary.outgoing_nicks += entry.amount_nicks;
                summary.fee_nicks += entry.fee_nicks;
            }
            _ => {
                summary.incoming_nicks += entry.amount_nicks;
            }
        }
        summary.net_nicks = summary.incoming_nicks - summary.outgoing_nicks;
        summary.direction = if summary.incoming_nicks > 0 && summary.outgoing_nicks > 0 {
            "both".to_string()
        } else if summary.outgoing_nicks > 0 {
            "outgoing".to_string()
        } else {
            "incoming".to_string()
        };
    }

    let transactions = tx_map.into_values().collect();
    Ok(AuditReport {
        balance,
        transactions,
        ledger,
    })
}

pub async fn wallet_ledger(
    conn: &mut crate::db::AsyncDbConnection,
    address: AddressInfo,
) -> Result<Vec<LedgerEntry>, QueryError> {
    Ok(audit_report(conn, address).await?.ledger)
}
