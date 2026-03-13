use super::address::{AddressInfo, VersionScope};
use chrono::{DateTime, TimeZone, Utc};
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Integer, Nullable, Text};
use diesel_async::RunQueryDsl;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
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
    pub first: String,
    pub recipient_type: Option<String>,
    pub recipient: Option<String>,
    pub amount_nicks: i64,
    pub block_height: i32,
    pub block_timestamp: i64,
    pub block_time_utc: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TxDebitDetail {
    pub first: String,
    pub amount_nicks: i64,
    pub fee_nicks: i64,
    pub block_height: i32,
    pub block_timestamp: i64,
    pub block_time_utc: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransactionDetail {
    pub txid: String,
    pub block_id: String,
    pub block_height: i32,
    pub block_timestamp: i64,
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
    pub first: String,
    pub amount_nicks: i64,
    pub block_height: i32,
    pub block_timestamp: i64,
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
    pub first_block_time_utc: String,
    pub direction: String,
    pub incoming_nicks: i64,
    pub outgoing_nicks: i64,
    pub fee_nicks: i64,
    pub net_nicks: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuditFlowRow {
    pub block_height: i32,
    pub block_id: Option<String>,
    pub txid: Option<String>,
    pub block_timestamp: i64,
    pub block_time_utc: String,
    pub entry_type: String,
    pub recipient_type: Option<String>,
    pub recipient: Option<String>,
    pub amount_nicks: i64,
    pub fee_nicks: i64,
    pub running_balance_nicks: i64,
}

fn format_chain_timestamp_utc(ts: i64) -> String {
    let dt_opt: Option<DateTime<Utc>> = Utc.timestamp_opt(ts, 0).single();
    match dt_opt {
        Some(dt) => dt.to_rfc3339(),
        None => format!("invalid({ts})"),
    }
}

fn recipient_matches_wallet(
    _scope: VersionScope,
    pkh: &str,
    wallet_pks: &HashSet<String>,
    recipient_type: Option<&str>,
    recipient: Option<&str>,
) -> bool {
    let Some(recipient_type) = recipient_type else {
        return false;
    };
    let Some(recipient) = recipient else {
        return false;
    };
    match recipient_type {
        "pk" => wallet_pks.contains(recipient),
        "pkh" => recipient == pkh,
        _ => false,
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AuditReport {
    pub balance: WalletBalance,
    pub transactions: Vec<WalletTxSummary>,
    pub flows: Vec<AuditFlowRow>,
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
struct PkRow {
    #[diesel(sql_type = Text)]
    pk: String,
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
    #[diesel(sql_type = Text)]
    first: String,
    #[diesel(sql_type = Nullable<Text>)]
    recipient_type: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    recipient: Option<String>,
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
    first: String,
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
    #[diesel(sql_type = Text)]
    first: String,
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

/// Wallet balance derived from L1 notes + L3 credits/debits.
///
/// The wallet is identified by `pkh`.  We use `pkh_to_pk` (L2.2) to look up
/// known public keys, then query notes that belong to the wallet via credits.
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

    // Unspent notes owned by this wallet (via credit_info recipient resolution)
    let unspent_query = format!(
        "SELECT COALESCE(SUM(n.assets), 0) AS sum_nicks, COUNT(*) AS note_count
         FROM notes n
         WHERE n.spent_txid IS NULL{note_version_filter}
           AND (
             n.first IN (
               SELECT ci.first FROM credit_info ci
               WHERE ci.recipient_type = 'pkh' AND ci.recipient = ?1
             )
             OR n.first IN (
               SELECT ci.first FROM credit_info ci
               WHERE ci.recipient_type = 'pk' AND ci.recipient IN (
                 SELECT pk FROM pkh_to_pk WHERE pkh = ?1
               )
             )
             OR (n.coinbase = 1 AND n.first IN (
               SELECT c.first FROM credits c
               WHERE c.txid IS NULL AND c.first IN (
                 SELECT ntl.first FROM name_to_lock ntl
                 WHERE ntl.first IN (
                   SELECT ci2.first FROM credit_info ci2
                   WHERE ci2.recipient_type = 'pkh' AND ci2.recipient = ?1
                 )
               )
             ))
           )"
    );
    let unspent = sql_query(unspent_query)
        .bind::<Text, _>(pkh.clone())
        .get_result::<SumCountRow>(conn)
        .await?;

    let by_version = sql_query(
        "SELECT n.version AS version, COALESCE(SUM(n.assets), 0) AS sum_nicks
         FROM notes n
         WHERE n.spent_txid IS NULL
           AND (
             n.first IN (
               SELECT ci.first FROM credit_info ci
               WHERE ci.recipient_type = 'pkh' AND ci.recipient = ?1
             )
             OR n.first IN (
               SELECT ci.first FROM credit_info ci
               WHERE ci.recipient_type = 'pk' AND ci.recipient IN (
                 SELECT pk FROM pkh_to_pk WHERE pkh = ?1
               )
             )
           )
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

    // TX credits (non-coinbase) for this wallet
    let tx_credits_sql = "SELECT COALESCE(SUM(c.amount), 0) AS sum_nicks
         FROM credits c
         WHERE c.txid IS NOT NULL
           AND (
             c.first IN (
               SELECT ci.first FROM credit_info ci
               WHERE ci.recipient_type = 'pkh' AND ci.recipient = ?1
             )
             OR c.first IN (
               SELECT ci.first FROM credit_info ci
               WHERE ci.recipient_type = 'pk' AND ci.recipient IN (
                 SELECT pk FROM pkh_to_pk WHERE pkh = ?1
               )
             )
           )";
    let tx_credits_nicks = sql_query(tx_credits_sql)
        .bind::<Text, _>(pkh.clone())
        .get_result::<SumRow>(conn)
        .await?
        .sum_nicks;

    // Coinbase credits (txid IS NULL) for this wallet
    let coinbase_sql = "SELECT COALESCE(SUM(c.amount), 0) AS sum_nicks
         FROM credits c
         WHERE c.txid IS NULL
           AND (
             c.first IN (
               SELECT ci.first FROM credit_info ci
               WHERE ci.recipient_type = 'pkh' AND ci.recipient = ?1
             )
             OR c.first IN (
               SELECT ci.first FROM credit_info ci
               WHERE ci.recipient_type = 'pk' AND ci.recipient IN (
                 SELECT pk FROM pkh_to_pk WHERE pkh = ?1
               )
             )
           )";
    let coinbase_credits_nicks = sql_query(coinbase_sql)
        .bind::<Text, _>(pkh.clone())
        .get_result::<SumRow>(conn)
        .await?
        .sum_nicks;

    // Spent notes
    let spent_query = format!(
        "SELECT COALESCE(SUM(d.amount), 0) AS sum_nicks
         FROM debits d
         WHERE d.first IN (
             SELECT ci.first FROM credit_info ci
             WHERE (ci.recipient_type = 'pkh' AND ci.recipient = ?1)
                OR (ci.recipient_type = 'pk' AND ci.recipient IN (
                     SELECT pk FROM pkh_to_pk WHERE pkh = ?1
                   ))
         )"
    );
    let spent_nicks = sql_query(spent_query)
        .bind::<Text, _>(pkh.clone())
        .get_result::<SumRow>(conn)
        .await?
        .sum_nicks;

    // Fees
    let fees_sql = "SELECT COALESCE(SUM(d.fee), 0) AS sum_nicks
         FROM debits d
         WHERE d.first IN (
             SELECT ci.first FROM credit_info ci
             WHERE (ci.recipient_type = 'pkh' AND ci.recipient = ?1)
                OR (ci.recipient_type = 'pk' AND ci.recipient IN (
                     SELECT pk FROM pkh_to_pk WHERE pkh = ?1
                   ))
         )";
    let fees_nicks = sql_query(fees_sql)
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
) -> Result<AddressInfo, super::address::AddressError> {
    super::address::resolve_address(conn, address).await
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
        "SELECT o.idx, o.first, o.last, o.assets, ci.recipient_type, ci.recipient
         FROM tx_outputs o
         LEFT JOIN credit_info ci ON ci.txid = o.txid AND ci.first = o.first
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

    // Credits from L3
    let credits = sql_query(
        "SELECT c.first, ci.recipient_type, ci.recipient, c.amount, c.height AS block_height, COALESCE(b.timestamp, 0) AS block_timestamp
         FROM credits c
         LEFT JOIN credit_info ci ON ci.txid = c.txid AND ci.first = c.first AND ci.height = c.height
         LEFT JOIN blocks b ON b.height = c.height
         WHERE c.txid = ?1
         ORDER BY c.first",
    )
    .bind::<Text, _>(txid.to_string())
    .load::<TxCreditRow>(conn)
    .await?
    .into_iter()
    .map(|r| TxCreditDetail {
        first: r.first,
        recipient_type: r.recipient_type,
        recipient: r.recipient,
        amount_nicks: r.amount,
        block_height: r.block_height,
        block_timestamp: r.block_timestamp,
        block_time_utc: format_chain_timestamp_utc(r.block_timestamp),
    })
    .collect();

    // Debits from L3
    let debits = sql_query(
        "SELECT d.first, d.amount, d.fee, d.height AS block_height, COALESCE(b.timestamp, 0) AS block_timestamp
         FROM debits d
         LEFT JOIN blocks b ON b.height = d.height
         WHERE d.txid = ?1
         ORDER BY d.first",
    )
    .bind::<Text, _>(txid.to_string())
    .load::<TxDebitRow>(conn)
    .await?
    .into_iter()
    .map(|r| TxDebitDetail {
        first: r.first,
        amount_nicks: r.amount,
        fee_nicks: r.fee,
        block_height: r.block_height,
        block_timestamp: r.block_timestamp,
        block_time_utc: format_chain_timestamp_utc(r.block_timestamp),
    })
    .collect();

    Ok(TransactionDetail {
        txid: base.txid,
        block_id: base.block_id,
        block_height: base.height,
        block_timestamp: base.block_timestamp,
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
        "SELECT c.first, c.amount, c.height AS block_height, COALESCE(b.timestamp, 0) AS block_timestamp
         FROM credits c
         LEFT JOIN blocks b ON b.height = c.height
         WHERE c.block_id = ?1 AND c.txid IS NULL
         ORDER BY c.first",
    )
    .bind::<Text, _>(block_id)
    .load::<CoinbaseRow>(conn)
    .await?
    .into_iter()
    .map(|r| CoinbaseCreditDetail {
        first: r.first,
        amount_nicks: r.amount,
        block_height: r.block_height,
        block_timestamp: r.block_timestamp,
        block_time_utc: format_chain_timestamp_utc(r.block_timestamp),
    })
    .collect();

    Ok(BlockDetail {
        id: base.id,
        block_height: base.height,
        version: base.version,
        parent: base.parent,
        block_timestamp: base.timestamp,
        block_time_utc: format_chain_timestamp_utc(base.timestamp),
        msg: base.msg,
        transactions,
        coinbase_credits,
    })
}

pub async fn sync_status(
    conn: &mut crate::db::AsyncDbConnection,
) -> Result<SyncStatus, QueryError> {
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
        "name_to_lock",
        "pkh_to_pk",
        "lock_tree",
        "spend_conditions",
        "credits",
        "debits",
        "credit_info",
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

    let wallet_pks = sql_query("SELECT pk FROM pkh_to_pk WHERE pkh = ?1")
        .bind::<Text, _>(pkh.clone())
        .load::<PkRow>(conn)
        .await?
        .into_iter()
        .map(|r| r.pk)
        .collect::<HashSet<_>>();

    let mut ledger = Vec::new();

    // Credits for this wallet (non-coinbase)
    let credit_sql =
        "SELECT c.height AS block_height,
                COALESCE(b.timestamp, 0) AS block_timestamp,
                'credit' AS entry_type,
                c.txid AS txid,
                t.block_id AS block_id,
                ci.recipient_type AS recipient_type,
                ci.recipient AS recipient,
                c.amount AS amount_nicks,
                0 AS fee_nicks,
                (SELECT GROUP_CONCAT(DISTINCT s.pk) FROM tx_signers s WHERE s.txid = c.txid) AS counterparties
         FROM credits c
         LEFT JOIN transactions t ON t.id = c.txid
         LEFT JOIN blocks b ON b.height = c.height
         LEFT JOIN credit_info ci ON ci.txid = c.txid AND ci.first = c.first AND ci.height = c.height
         WHERE c.txid IS NOT NULL
           AND (
             (ci.recipient_type = 'pkh' AND ci.recipient = ?1)
             OR (ci.recipient_type = 'pk' AND ci.recipient IN (
                   SELECT pk FROM pkh_to_pk WHERE pkh = ?1
                 ))
           )
         ORDER BY c.height, c.txid, c.first";
    let credit_rows = sql_query(credit_sql)
        .bind::<Text, _>(pkh.clone())
        .load::<LedgerRow>(conn)
        .await?;
    ledger.extend(credit_rows.into_iter().map(|r| LedgerEntry {
        block_height: r.block_height,
        block_timestamp: r.block_timestamp,
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

    // Coinbase credits for this wallet
    let coinbase_sql =
        "SELECT c.height AS block_height,
                COALESCE(b.timestamp, 0) AS block_timestamp,
                'coinbase' AS entry_type,
                NULL AS txid,
                c.block_id AS block_id,
                ci.recipient_type AS recipient_type,
                ci.recipient AS recipient,
                c.amount AS amount_nicks,
                0 AS fee_nicks,
                NULL AS counterparties
         FROM credits c
         LEFT JOIN blocks b ON b.height = c.height
         LEFT JOIN credit_info ci ON ci.txid = c.txid AND ci.first = c.first AND ci.height = c.height
         WHERE c.txid IS NULL
           AND (
             (ci.recipient_type = 'pkh' AND ci.recipient = ?1)
             OR (ci.recipient_type = 'pk' AND ci.recipient IN (
                   SELECT pk FROM pkh_to_pk WHERE pkh = ?1
                 ))
           )
         ORDER BY c.height, c.first";
    let coinbase_rows = sql_query(coinbase_sql)
        .bind::<Text, _>(pkh.clone())
        .load::<LedgerRow>(conn)
        .await?;
    ledger.extend(coinbase_rows.into_iter().map(|r| LedgerEntry {
        block_height: r.block_height,
        block_timestamp: r.block_timestamp,
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

    // Debits: spent notes owned by this wallet
    let spent_sql = format!(
        "SELECT n.spent_height AS block_height,
                COALESCE(b.timestamp, 0) AS block_timestamp,
                'debit' AS entry_type,
                n.spent_txid AS txid,
                t.block_id AS block_id,
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
                (SELECT GROUP_CONCAT(DISTINCT ci2.recipient) FROM credit_info ci2 WHERE ci2.txid = n.spent_txid) AS counterparties
         FROM notes n
         LEFT JOIN transactions t ON t.id = n.spent_txid
         LEFT JOIN blocks b ON b.height = n.spent_height
         WHERE n.spent_txid IS NOT NULL{note_version_filter}
           AND (
             n.first IN (
               SELECT ci.first FROM credit_info ci
               WHERE (ci.recipient_type = 'pkh' AND ci.recipient = ?1)
                  OR (ci.recipient_type = 'pk' AND ci.recipient IN (
                       SELECT pk FROM pkh_to_pk WHERE pkh = ?1
                     ))
             )
           )
         ORDER BY n.spent_height, n.spent_txid, n.first, n.last"
    );
    let spent_rows = sql_query(spent_sql)
        .bind::<Text, _>(pkh.clone())
        .load::<LedgerRow>(conn)
        .await?;

    ledger.extend(spent_rows.into_iter().map(|r| LedgerEntry {
        block_height: r.block_height,
        block_timestamp: r.block_timestamp,
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

    let mut tx_map: BTreeMap<String, WalletTxSummary> = BTreeMap::new();
    for entry in &ledger {
        let Some(txid) = entry.txid.clone() else {
            continue;
        };
        let summary = tx_map.entry(txid.clone()).or_insert(WalletTxSummary {
            txid,
            first_block_height: entry.block_height,
            first_block_timestamp: entry.block_timestamp,
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

    let mut transactions = tx_map.into_values().collect::<Vec<_>>();
    transactions.sort_by(|a, b| {
        a.first_block_height
            .cmp(&b.first_block_height)
            .then_with(|| a.first_block_timestamp.cmp(&b.first_block_timestamp))
            .then_with(|| a.txid.cmp(&b.txid))
    });

    // Summary flow rows
    let mut flows = Vec::new();

    // Incoming rows from ledger
    for entry in &ledger {
        if entry.entry_type == "credit" || entry.entry_type == "coinbase" {
            flows.push(AuditFlowRow {
                block_height: entry.block_height,
                block_id: entry.block_id.clone(),
                txid: entry.txid.clone(),
                block_timestamp: entry.block_timestamp,
                block_time_utc: entry.block_time_utc.clone(),
                entry_type: if entry.entry_type == "coinbase" {
                    "coinbase".to_string()
                } else {
                    "incoming".to_string()
                },
                recipient_type: entry.recipient_type.clone(),
                recipient: entry.recipient.clone(),
                amount_nicks: entry.amount_nicks,
                fee_nicks: 0,
                running_balance_nicks: 0,
            });
        }
    }

    // Outgoing rows: recipient-level credits from wallet-spend txs excluding wallet recipients.
    let outgoing_sql = format!(
        "SELECT ci.height AS block_height,
                COALESCE(b.timestamp, 0) AS block_timestamp,
                'outgoing' AS entry_type,
                ci.txid AS txid,
                t.block_id AS block_id,
                ci.recipient_type AS recipient_type,
                ci.recipient AS recipient,
                c.amount AS amount_nicks,
                0 AS fee_nicks,
                NULL AS counterparties
         FROM credit_info ci
         INNER JOIN credits c ON c.txid = ci.txid AND c.first = ci.first AND c.height = ci.height
         INNER JOIN transactions t ON t.id = ci.txid
         LEFT JOIN blocks b ON b.height = ci.height
         WHERE EXISTS (
             SELECT 1
             FROM notes n
             WHERE n.spent_txid = ci.txid
               AND n.spent_txid IS NOT NULL{note_version_filter}
               AND n.first IN (
                 SELECT ci3.first FROM credit_info ci3
                 WHERE (ci3.recipient_type = 'pkh' AND ci3.recipient = ?1)
                    OR (ci3.recipient_type = 'pk' AND ci3.recipient IN (
                         SELECT pk FROM pkh_to_pk WHERE pkh = ?1
                       ))
               )
         )
         ORDER BY ci.height, ci.txid, ci.first"
    );
    let outgoing_rows = sql_query(outgoing_sql)
        .bind::<Text, _>(pkh.clone())
        .load::<LedgerRow>(conn)
        .await?;
    for row in outgoing_rows {
        if recipient_matches_wallet(
            scope.clone(),
            &pkh,
            &wallet_pks,
            row.recipient_type.as_deref(),
            row.recipient.as_deref(),
        ) {
            continue;
        }
        flows.push(AuditFlowRow {
            block_height: row.block_height,
            block_id: row.block_id,
            txid: row.txid,
            block_timestamp: row.block_timestamp,
            block_time_utc: format_chain_timestamp_utc(row.block_timestamp),
            entry_type: "outgoing".to_string(),
            recipient_type: row.recipient_type,
            recipient: row.recipient,
            amount_nicks: row.amount_nicks,
            fee_nicks: 0,
            running_balance_nicks: 0,
        });
    }

    // One fee assignment per tx
    let mut tx_fee_map: HashMap<String, i64> = HashMap::new();
    for entry in &ledger {
        if entry.entry_type == "debit" {
            if let Some(txid) = entry.txid.as_ref() {
                *tx_fee_map.entry(txid.clone()).or_insert(0) += entry.fee_nicks;
            }
        }
    }

    flows.sort_by(|a, b| {
        a.block_height
            .cmp(&b.block_height)
            .then_with(|| a.block_timestamp.cmp(&b.block_timestamp))
            .then_with(|| a.txid.cmp(&b.txid))
            .then_with(|| a.entry_type.cmp(&b.entry_type))
            .then_with(|| a.recipient_type.cmp(&b.recipient_type))
            .then_with(|| a.recipient.cmp(&b.recipient))
    });

    for (txid, fee) in tx_fee_map {
        if fee == 0 {
            continue;
        }
        let outgoing_idx = flows
            .iter()
            .position(|row| row.txid.as_deref() == Some(&txid) && row.entry_type == "outgoing");
        let any_idx = flows
            .iter()
            .position(|row| row.txid.as_deref() == Some(&txid));
        if let Some(idx) = outgoing_idx.or(any_idx) {
            flows[idx].fee_nicks = fee;
        }
    }

    let mut running_flows_nicks = 0i64;
    for row in &mut flows {
        let mut delta = if row.entry_type == "outgoing" {
            -row.amount_nicks
        } else {
            row.amount_nicks
        };
        delta -= row.fee_nicks;
        running_flows_nicks += delta;
        row.running_balance_nicks = running_flows_nicks;
    }

    Ok(AuditReport {
        balance,
        transactions,
        flows,
        ledger,
    })
}

pub async fn wallet_ledger(
    conn: &mut crate::db::AsyncDbConnection,
    address: AddressInfo,
) -> Result<Vec<LedgerEntry>, QueryError> {
    Ok(audit_report(conn, address).await?.ledger)
}
