use super::address::{AddressInfo, AddressType, VersionScope};
use crate::layers::{
    l0::schema::{blocks, transactions},
    l2::schema::tx_signers,
    l3::schema::{credits, debits},
    shared_schema::{layer_metadata, DbDigest, DbPublicKey},
};
use chrono::{DateTime, TimeZone, Utc};
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Integer, Nullable, Text};
use diesel_async::RunQueryDsl;
use iris_ztd::Digest;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueryError {
    #[error(transparent)]
    Diesel(#[from] diesel::result::Error),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("accounting invariant violated: {0}")]
    InvariantViolation(String),
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
    // Primary path: block timestamps are stored as plain Unix seconds.
    if let Some(dt) = Utc.timestamp_opt(ts, 0).single() {
        return dt.to_rfc3339();
    }

    // Compatibility path: older snapshots may contain Urbit @da-biased seconds.
    const DA_UNIX_EPOCH_BIASED_SECONDS: u64 = 0x8000_000c_ce9e_0d80;
    let raw_u64 = ts as u64;
    if raw_u64 < DA_UNIX_EPOCH_BIASED_SECONDS {
        return format!("invalid({ts})");
    }
    let unix_seconds_u64 = raw_u64 - DA_UNIX_EPOCH_BIASED_SECONDS;
    let Ok(unix_seconds) = i64::try_from(unix_seconds_u64) else {
        return format!("invalid({ts})");
    };
    let dt_opt: Option<DateTime<Utc>> = Utc.timestamp_opt(unix_seconds, 0).single();
    match dt_opt {
        Some(dt) => dt.to_rfc3339(),
        None => format!("invalid({ts})"),
    }
}

fn address_type_tag(address_type: &AddressType) -> &'static str {
    match address_type {
        AddressType::Pkh => "pkh",
        AddressType::DbPublicKey => "pk",
    }
}

fn parse_db_digest(input: &str) -> Option<DbDigest> {
    Digest::try_from(input).ok().map(DbDigest::from)
}

fn wallet_owner_match_clause(owner_type_col: &str, owner_col: &str) -> String {
    format!(
        "(
            (?3 = 'pkh' AND {owner_type_col} = 'pkh' AND {owner_col} = ?1)
            OR (?3 = 'pk' AND ?2 IS NOT NULL AND {owner_type_col} = 'pk' AND {owner_col} = ?2)
        )"
    )
}

fn wallet_name_info_filter(alias: &str) -> String {
    wallet_owner_match_clause(&format!("{alias}.owner_type"), &format!("{alias}.owner"))
}

fn latest_name_info_subquery(alias: &str) -> String {
    format!(
        "SELECT {alias}.first FROM name_info {alias}
         WHERE {alias}.height = (
             SELECT MAX(ni2.height) FROM name_info ni2 WHERE ni2.first = {alias}.first
         )"
    )
}

fn validate_balance_invariant(balance_nicks: i64, accounting_nicks: i64) -> Result<(), QueryError> {
    if balance_nicks == accounting_nicks {
        return Ok(());
    }
    #[cfg(test)]
    {
        return Err(QueryError::InvariantViolation(format!(
            "received-spent={} but unspent balance={}",
            accounting_nicks, balance_nicks
        )));
    }
    #[cfg(not(test))]
    {
        tracing::warn!(
            balance_nicks,
            accounting_nicks,
            "accounting mismatch: received - spent != unspent notes"
        );
        Ok(())
    }
}

fn recipient_matches_wallet(
    address_type: &AddressType,
    pkh: &str,
    wallet_db_pk: Option<&str>,
    recipient_type: Option<&str>,
    recipient: Option<&str>,
) -> bool {
    let Some(recipient_type) = recipient_type else {
        return false;
    };
    let Some(recipient) = recipient else {
        return false;
    };
    match address_type {
        AddressType::Pkh => recipient_type == "pkh" && recipient == pkh,
        AddressType::DbPublicKey => {
            recipient_type == "pk" && wallet_db_pk.map(|pk| pk == recipient).unwrap_or(false)
        }
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

/// Compute wallet balance and accounting totals from indexed layers.
///
/// Ownership is resolved through `name_info` and filtered by `AddressType`:
/// `pkh` owners for V1-style wallet queries and `pk` owners for V0-style queries.
pub async fn wallet_balance(
    conn: &mut crate::db::AsyncDbConnection,
    address: AddressInfo,
) -> Result<WalletBalance, QueryError> {
    let pkh = address.pkh.clone();
    let db_public_key = address.db_public_key.clone();
    let owner_tag = address_type_tag(&address.address_type).to_string();
    let scope = address.scope.clone();
    let note_version_filter = match scope {
        VersionScope::All => "",
        VersionScope::V0Only => " AND n.version = 0",
        VersionScope::V1Only => " AND n.version >= 1",
    };
    let wallet_ni_filter = wallet_name_info_filter("ni");
    let wallet_name_info_latest = latest_name_info_subquery("ni");

    // Unspent notes currently owned by the requested wallet identity.
    let unspent_query = format!(
        "SELECT COALESCE(SUM(n.assets), 0) AS sum_nicks, COUNT(*) AS note_count
         FROM notes n
         WHERE n.spent_txid IS NULL{note_version_filter}
           AND n.first IN (
             {wallet_name_info_latest}
             AND {wallet_ni_filter}
           )"
    );
    let unspent = sql_query(unspent_query)
        .bind::<Text, _>(pkh.clone())
        .bind::<Nullable<Text>, _>(db_public_key.clone())
        .bind::<Text, _>(owner_tag.clone())
        .get_result::<SumCountRow>(conn)
        .await?;

    let by_version_sql = format!(
        "SELECT n.version AS version, COALESCE(SUM(n.assets), 0) AS sum_nicks
         FROM notes n
         WHERE n.spent_txid IS NULL
           AND n.first IN (
             {wallet_name_info_latest}
             AND {wallet_ni_filter}
           )
         GROUP BY n.version"
    );
    let by_version = sql_query(by_version_sql)
        .bind::<Text, _>(pkh.clone())
        .bind::<Nullable<Text>, _>(db_public_key.clone())
        .bind::<Text, _>(owner_tag.clone())
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

    // Non-coinbase credits attributed to this wallet.
    let tx_credits_sql = format!(
        "SELECT COALESCE(SUM(c.amount), 0) AS sum_nicks
         FROM credits c
         WHERE c.txid IS NOT NULL
           AND EXISTS (
             SELECT 1 FROM notes n
             WHERE n.first = c.first{note_version_filter}
           )
           AND c.first IN (
             {wallet_name_info_latest}
             AND {wallet_ni_filter}
           )"
    );
    let tx_credits_nicks = sql_query(tx_credits_sql)
        .bind::<Text, _>(pkh.clone())
        .bind::<Nullable<Text>, _>(db_public_key.clone())
        .bind::<Text, _>(owner_tag.clone())
        .get_result::<SumRow>(conn)
        .await?
        .sum_nicks;

    // Coinbase credits attributed to this wallet (`txid IS NULL`).
    let coinbase_sql = format!(
        "SELECT COALESCE(SUM(c.amount), 0) AS sum_nicks
         FROM credits c
         WHERE c.txid IS NULL
           AND EXISTS (
             SELECT 1 FROM notes n
             WHERE n.first = c.first{note_version_filter}
           )
           AND c.first IN (
             {wallet_name_info_latest}
             AND {wallet_ni_filter}
           )"
    );
    let coinbase_credits_nicks = sql_query(coinbase_sql)
        .bind::<Text, _>(pkh.clone())
        .bind::<Nullable<Text>, _>(db_public_key.clone())
        .bind::<Text, _>(owner_tag.clone())
        .get_result::<SumRow>(conn)
        .await?
        .sum_nicks;

    // Debits: value spent from notes owned by this wallet.
    let spent_query = format!(
        "SELECT COALESCE(SUM(d.amount), 0) AS sum_nicks
         FROM debits d
         WHERE d.first IN (
             SELECT n.first FROM notes n
             WHERE n.first = d.first{note_version_filter}
         )
           AND d.first IN (
             {wallet_name_info_latest}
             AND {wallet_ni_filter}
         )"
    );
    let spent_nicks = sql_query(spent_query)
        .bind::<Text, _>(pkh.clone())
        .bind::<Nullable<Text>, _>(db_public_key.clone())
        .bind::<Text, _>(owner_tag.clone())
        .get_result::<SumRow>(conn)
        .await?
        .sum_nicks;

    // Fees paid by this wallet's spends.
    let fees_sql = format!(
        "SELECT COALESCE(SUM(d.fee), 0) AS sum_nicks
         FROM debits d
         WHERE d.first IN (
             SELECT n.first FROM notes n
             WHERE n.first = d.first{note_version_filter}
         )
           AND d.first IN (
             {wallet_name_info_latest}
             AND {wallet_ni_filter}
         )"
    );
    let fees_nicks = sql_query(fees_sql)
        .bind::<Text, _>(pkh.clone())
        .bind::<Nullable<Text>, _>(db_public_key)
        .bind::<Text, _>(owner_tag)
        .get_result::<SumRow>(conn)
        .await?
        .sum_nicks;

    let balance_nicks = unspent.sum_nicks;
    let received_nicks = tx_credits_nicks + coinbase_credits_nicks;
    let accounting_nicks = received_nicks - spent_nicks;
    validate_balance_invariant(balance_nicks, accounting_nicks)?;

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
    let Some(txid_digest) = parse_db_digest(txid) else {
        return Err(QueryError::NotFound(format!("transaction {txid}")));
    };
    let base = transactions::table
        .inner_join(blocks::table.on(blocks::height.eq(transactions::height)))
        .filter(transactions::id.eq(txid_digest))
        .select((
            transactions::id,
            transactions::block_id,
            transactions::height,
            blocks::timestamp,
            transactions::version,
            transactions::fee,
            transactions::total_size,
        ))
        .first::<(DbDigest, DbDigest, i32, i64, i32, i64, i32)>(conn)
        .await
        .optional()?
        .map(|r| TxBaseRow {
            txid: r.0.to_string(),
            block_id: r.1.to_string(),
            height: r.2,
            block_timestamp: r.3,
            version: r.4,
            fee: r.5,
            total_size: r.6,
        });

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

    let signers = tx_signers::table
        .filter(tx_signers::txid.eq(txid_digest))
        .order((tx_signers::z.asc(), tx_signers::pk.asc()))
        .select((tx_signers::z, tx_signers::pk))
        .load::<(i32, DbPublicKey)>(conn)
        .await?
        .into_iter()
        .map(|(z, pk)| TxSignerDetail {
            z,
            pk: pk.to_string(),
        })
        .collect();

    let outputs = sql_query(
        "SELECT o.idx, o.first, o.last, o.assets,
                ni.owner_type AS recipient_type,
                ni.owner AS recipient
         FROM tx_outputs o
         LEFT JOIN name_info ni
           ON ni.first = o.first
          AND ni.height = (
              SELECT MAX(ni2.height) FROM name_info ni2 WHERE ni2.first = o.first
          )
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

    // L3 credits associated with this tx.
    let credits = sql_query(
        "SELECT c.first,
                ni.owner_type AS recipient_type,
                ni.owner AS recipient,
                c.amount,
                c.height AS block_height,
                COALESCE(b.timestamp, 0) AS block_timestamp
         FROM credits c
         LEFT JOIN name_info ni
           ON ni.first = c.first
          AND ni.height = (
              SELECT MAX(ni2.height) FROM name_info ni2 WHERE ni2.first = c.first
          )
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

    // L3 debits associated with this tx.
    let debit_rows = debits::table
        .filter(debits::txid.eq(Some(txid_digest)))
        .order(debits::first.asc())
        .select((debits::first, debits::amount, debits::fee, debits::height))
        .load::<(Option<DbDigest>, i64, i64, i32)>(conn)
        .await?;
    let heights = debit_rows.iter().map(|(_, _, _, h)| *h).collect::<Vec<_>>();
    let ts_by_height = if heights.is_empty() {
        HashMap::new()
    } else {
        blocks::table
            .filter(blocks::height.eq_any(heights))
            .select((blocks::height, blocks::timestamp))
            .load::<(i32, i64)>(conn)
            .await?
            .into_iter()
            .collect::<HashMap<_, _>>()
    };
    let debits = debit_rows
        .into_iter()
        .map(|r| {
            let ts = *ts_by_height.get(&r.3).unwrap_or(&0);
            TxDebitDetail {
                first: r
                    .0
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                amount_nicks: r.1,
                fee_nicks: r.2,
                block_height: r.3,
                block_timestamp: ts,
                block_time_utc: format_chain_timestamp_utc(ts),
            }
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
        blocks::table
            .filter(blocks::height.eq(height))
            .order(blocks::id.asc())
            .select((
                blocks::id,
                blocks::height,
                blocks::version,
                blocks::parent,
                blocks::timestamp,
                blocks::msg,
            ))
            .first::<(DbDigest, i32, i32, DbDigest, i64, Option<String>)>(conn)
            .await
            .optional()?
            .map(|r| BlockBaseRow {
                id: r.0.to_string(),
                height: r.1,
                version: r.2,
                parent: r.3.to_string(),
                timestamp: r.4,
                msg: r.5,
            })
    } else {
        if let Some(block_id) = parse_db_digest(block) {
            blocks::table
                .filter(blocks::id.eq(block_id))
                .select((
                    blocks::id,
                    blocks::height,
                    blocks::version,
                    blocks::parent,
                    blocks::timestamp,
                    blocks::msg,
                ))
                .first::<(DbDigest, i32, i32, DbDigest, i64, Option<String>)>(conn)
                .await
                .optional()?
                .map(|r| BlockBaseRow {
                    id: r.0.to_string(),
                    height: r.1,
                    version: r.2,
                    parent: r.3.to_string(),
                    timestamp: r.4,
                    msg: r.5,
                })
        } else {
            None
        }
    };

    let Some(base) = base else {
        return Err(QueryError::NotFound(format!("block {block}")));
    };

    let block_id = base.id.clone();
    let block_id_digest =
        parse_db_digest(&block_id).ok_or_else(|| QueryError::NotFound(format!("block {block}")))?;
    let transactions = transactions::table
        .filter(transactions::block_id.eq(block_id_digest))
        .order(transactions::id.asc())
        .select((
            transactions::id,
            transactions::version,
            transactions::fee,
            transactions::total_size,
        ))
        .load::<(DbDigest, i32, i64, i32)>(conn)
        .await?
        .into_iter()
        .map(|r| BlockTransaction {
            txid: r.0.to_string(),
            version: r.1,
            fee_nicks: r.2,
            total_size: r.3,
        })
        .collect();

    let coinbase_rows = credits::table
        .filter(credits::block_id.eq(block_id_digest))
        .filter(credits::txid.is_null())
        .order(credits::first.asc())
        .select((credits::first, credits::amount, credits::height))
        .load::<(DbDigest, i64, i32)>(conn)
        .await?;
    let heights = coinbase_rows.iter().map(|(_, _, h)| *h).collect::<Vec<_>>();
    let ts_by_height = if heights.is_empty() {
        HashMap::new()
    } else {
        blocks::table
            .filter(blocks::height.eq_any(heights))
            .select((blocks::height, blocks::timestamp))
            .load::<(i32, i64)>(conn)
            .await?
            .into_iter()
            .collect::<HashMap<_, _>>()
    };
    let coinbase_credits = coinbase_rows
        .into_iter()
        .map(|r| {
            let ts = *ts_by_height.get(&r.2).unwrap_or(&0);
            CoinbaseCreditDetail {
                first: r.0.to_string(),
                amount_nicks: r.1,
                block_height: r.2,
                block_timestamp: ts,
                block_time_utc: format_chain_timestamp_utc(ts),
            }
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
    let layers = layer_metadata::table
        .order(layer_metadata::layer.asc())
        .select((layer_metadata::layer, layer_metadata::next_block_height))
        .load::<(String, i32)>(conn)
        .await?
        .into_iter()
        .map(|r| LayerStatus {
            layer: r.0,
            next_block_height: r.1,
        })
        .collect::<Vec<_>>();

    // Intentionally kept as raw SQL: this path needs dynamic table names for
    // status introspection, which Diesel's typed query DSL does not model
    // ergonomically without a parallel hard-coded query list.
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
        "name_info",
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
    let db_public_key = address.db_public_key.clone();
    let owner_tag = address_type_tag(&address.address_type).to_string();
    let scope = address.scope.clone();
    let note_version_filter = match scope {
        VersionScope::All => "",
        VersionScope::V0Only => " AND n.version = 0",
        VersionScope::V1Only => " AND n.version >= 1",
    };
    let wallet_ni_filter = wallet_name_info_filter("ni");
    let wallet_ni3_filter = wallet_name_info_filter("ni3");
    let wallet_name_info_latest = latest_name_info_subquery("ni");
    let wallet_name_info_latest_ni3 = latest_name_info_subquery("ni3");

    let mut ledger = Vec::new();

    // Intentionally raw SQL: this query relies on correlated subqueries and
    // SQLite aggregation (`GROUP_CONCAT(DISTINCT ...)`) that are critical to
    // accounting semantics and are migrated in a dedicated hardening pass.
    // Non-coinbase ledger credits for the wallet.
    let credit_sql = format!(
        "SELECT c.height AS block_height,
                COALESCE(b.timestamp, 0) AS block_timestamp,
                'credit' AS entry_type,
                c.txid AS txid,
                t.block_id AS block_id,
                ni.owner_type AS recipient_type,
                ni.owner AS recipient,
                c.amount AS amount_nicks,
                0 AS fee_nicks,
                (SELECT GROUP_CONCAT(DISTINCT s.pk) FROM tx_signers s WHERE s.txid = c.txid) AS counterparties
         FROM credits c
         LEFT JOIN transactions t ON t.id = c.txid
         LEFT JOIN blocks b ON b.height = c.height
         LEFT JOIN name_info ni
           ON ni.first = c.first
          AND ni.height = (
              SELECT MAX(ni2.height) FROM name_info ni2 WHERE ni2.first = c.first
          )
         WHERE c.txid IS NOT NULL
           AND c.first IN (
             {wallet_name_info_latest}
             AND {wallet_ni_filter}
           )
         ORDER BY c.height, c.txid, c.first"
    );
    let credit_rows = sql_query(credit_sql)
        .bind::<Text, _>(pkh.clone())
        .bind::<Nullable<Text>, _>(db_public_key.clone())
        .bind::<Text, _>(owner_tag.clone())
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

    // Coinbase ledger credits for the wallet.
    let coinbase_sql = format!(
        "SELECT c.height AS block_height,
                COALESCE(b.timestamp, 0) AS block_timestamp,
                'coinbase' AS entry_type,
                NULL AS txid,
                c.block_id AS block_id,
                ni.owner_type AS recipient_type,
                ni.owner AS recipient,
                c.amount AS amount_nicks,
                0 AS fee_nicks,
                NULL AS counterparties
         FROM credits c
         LEFT JOIN blocks b ON b.height = c.height
         LEFT JOIN name_info ni
           ON ni.first = c.first
          AND ni.height = (
              SELECT MAX(ni2.height) FROM name_info ni2 WHERE ni2.first = c.first
          )
         WHERE c.txid IS NULL
           AND c.first IN (
             {wallet_name_info_latest}
             AND {wallet_ni_filter}
           )
         ORDER BY c.height, c.first"
    );
    let coinbase_rows = sql_query(coinbase_sql)
        .bind::<Text, _>(pkh.clone())
        .bind::<Nullable<Text>, _>(db_public_key.clone())
        .bind::<Text, _>(owner_tag.clone())
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

    // Intentionally raw SQL: uses window functions and nested correlated
    // subqueries for fee and counterparty attribution.
    // Ledger debits for notes spent by this wallet.
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
                (SELECT GROUP_CONCAT(DISTINCT ni2.owner)
                 FROM credits c2
                 INNER JOIN name_info ni2 ON ni2.first = c2.first
                 WHERE c2.txid = n.spent_txid
                   AND ni2.height = (
                       SELECT MAX(ni2max.height)
                       FROM name_info ni2max
                       WHERE ni2max.first = c2.first
                   )) AS counterparties
         FROM notes n
         LEFT JOIN transactions t ON t.id = n.spent_txid
         LEFT JOIN blocks b ON b.height = n.spent_height
         WHERE n.spent_txid IS NOT NULL{note_version_filter}
           AND (
             n.first IN (
              {wallet_name_info_latest}
              AND {wallet_ni_filter}
             )
           )
         ORDER BY n.spent_height, n.spent_txid, n.first, n.last"
    );
    let spent_rows = sql_query(spent_sql)
        .bind::<Text, _>(pkh.clone())
        .bind::<Nullable<Text>, _>(db_public_key.clone())
        .bind::<Text, _>(owner_tag.clone())
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

    // Build summary-flow rows (accounting view used by default CSV output).
    let mut flows = Vec::new();
    let wallet_spend_txids = ledger
        .iter()
        .filter(|entry| entry.entry_type == "debit")
        .filter_map(|entry| entry.txid.clone())
        .collect::<HashSet<_>>();

    // Incoming flow rows come directly from ledger credits/coinbase credits.
    for entry in &ledger {
        if entry.entry_type == "credit" || entry.entry_type == "coinbase" {
            // Omit self-refund credits from summary rows. They remain visible in
            // note-level output, while summary output focuses on external flows.
            if entry.entry_type == "credit"
                && entry
                    .txid
                    .as_ref()
                    .map(|txid| wallet_spend_txids.contains(txid))
                    .unwrap_or(false)
            {
                continue;
            }
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

    // Intentionally raw SQL: outgoing flow shaping depends on EXISTS filters and
    // latest-name resolution that are kept stable until dedicated migration.
    // Outgoing flow rows are recipient-level credits for transactions where this
    // wallet spent notes, excluding recipients that resolve back to this wallet.
    let outgoing_sql = format!(
        "SELECT c.height AS block_height,
                COALESCE(b.timestamp, 0) AS block_timestamp,
                'outgoing' AS entry_type,
                c.txid AS txid,
                t.block_id AS block_id,
                ni.owner_type AS recipient_type,
                ni.owner AS recipient,
                c.amount AS amount_nicks,
                0 AS fee_nicks,
                NULL AS counterparties
         FROM name_info ni
         INNER JOIN credits c ON c.first = ni.first
         INNER JOIN transactions t ON t.id = c.txid
         LEFT JOIN blocks b ON b.height = c.height
         WHERE ni.height = (
             SELECT MAX(ni2.height) FROM name_info ni2 WHERE ni2.first = ni.first
         )
           AND c.txid IS NOT NULL
           AND EXISTS (
             SELECT 1
             FROM notes n
             WHERE n.spent_txid = c.txid
               AND n.spent_txid IS NOT NULL{note_version_filter}
               AND n.first IN (
                 {wallet_name_info_latest_ni3}
                 AND {wallet_ni3_filter}
               )
         )
         ORDER BY c.height, c.txid, c.first"
    );
    let outgoing_rows = sql_query(outgoing_sql)
        .bind::<Text, _>(pkh.clone())
        .bind::<Nullable<Text>, _>(db_public_key.clone())
        .bind::<Text, _>(owner_tag.clone())
        .load::<LedgerRow>(conn)
        .await?;
    for row in outgoing_rows {
        if recipient_matches_wallet(
            &address.address_type,
            &pkh,
            db_public_key.as_deref(),
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

    // Assign each transaction fee once in summary rows.
    let mut tx_fee_map: HashMap<String, i64> = HashMap::new();
    let mut tx_fee_anchor: HashMap<String, (i32, Option<String>, i64, String)> = HashMap::new();
    for entry in &ledger {
        if entry.entry_type == "debit" {
            if let Some(txid) = entry.txid.as_ref() {
                *tx_fee_map.entry(txid.clone()).or_insert(0) += entry.fee_nicks;
                tx_fee_anchor.entry(txid.clone()).or_insert((
                    entry.block_height,
                    entry.block_id.clone(),
                    entry.block_timestamp,
                    entry.block_time_utc.clone(),
                ));
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
        } else if let Some((block_height, block_id, block_timestamp, block_time_utc)) =
            tx_fee_anchor.get(&txid)
        {
            // Preserve accounting invariants in summary/default CSV. If a spend tx
            // has no remaining incoming/outgoing row after filtering self-refunds,
            // emit a fee-only row so running balance still reflects the deduction.
            flows.push(AuditFlowRow {
                block_height: *block_height,
                block_id: block_id.clone(),
                txid: Some(txid.clone()),
                block_timestamp: *block_timestamp,
                block_time_utc: block_time_utc.clone(),
                entry_type: "outgoing".to_string(),
                recipient_type: None,
                recipient: None,
                amount_nicks: 0,
                fee_nicks: fee,
                running_balance_nicks: 0,
            });
        }
    }

    // Fee-only rows may be appended after the initial sort above.
    // Re-apply canonical ordering before computing running balances.
    flows.sort_by(|a, b| {
        a.block_height
            .cmp(&b.block_height)
            .then_with(|| a.block_timestamp.cmp(&b.block_timestamp))
            .then_with(|| a.txid.cmp(&b.txid))
            .then_with(|| a.entry_type.cmp(&b.entry_type))
            .then_with(|| a.recipient_type.cmp(&b.recipient_type))
            .then_with(|| a.recipient.cmp(&b.recipient))
    });

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

    // Keep transaction identifiers explicit in reporting views. Coinbase rows do
    // not have canonical tx ids in storage, so emit stable synthetic references
    // to avoid blank `txid` values in text/json/CSV exports.
    for entry in &mut ledger {
        if entry.txid.is_none() {
            let anchor = entry
                .block_id
                .clone()
                .unwrap_or_else(|| format!("h{}", entry.block_height));
            entry.txid = Some(format!("coinbase@{anchor}"));
        }
    }
    for row in &mut flows {
        if row.txid.is_none() {
            let anchor = row
                .block_id
                .clone()
                .unwrap_or_else(|| format!("h{}", row.block_height));
            row.txid = Some(format!("coinbase@{anchor}"));
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounting::address::{AddressType, VersionScope};
    use diesel::sql_query;
    use diesel_async::RunQueryDsl;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    const DA_UNIX_EPOCH_BIASED_SECONDS: u64 = 0x8000_000c_ce9e_0d80;

    fn test_db_path(prefix: &str) -> PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!("iris-blocks-{prefix}-{ts}.sqlite"))
    }

    fn da_biased(unix_seconds: i64) -> i64 {
        (unix_seconds as u64).wrapping_add(DA_UNIX_EPOCH_BIASED_SECONDS) as i64
    }

    #[test]
    fn format_chain_timestamp_utc_supports_plain_unix_seconds() {
        let unix_seconds = 1_741_557_600i64;
        let expected = Utc
            .timestamp_opt(unix_seconds, 0)
            .single()
            .expect("valid unix timestamp")
            .to_rfc3339();
        assert_eq!(format_chain_timestamp_utc(unix_seconds), expected);
    }

    #[test]
    fn format_chain_timestamp_utc_supports_legacy_da_biased_seconds() {
        let unix_seconds = 1_741_557_600i64;
        let biased = da_biased(unix_seconds);
        let expected = Utc
            .timestamp_opt(unix_seconds, 0)
            .single()
            .expect("valid unix timestamp")
            .to_rfc3339();
        assert_eq!(format_chain_timestamp_utc(biased), expected);
    }

    async fn setup_conn() -> (crate::db::AsyncDbConnection, PathBuf) {
        let path = test_db_path("accounting-query");
        let mut conn = crate::db::new_conn(path.to_str().expect("db path"))
            .await
            .expect("open sqlite");
        crate::db::run_migrations(&mut conn)
            .await
            .expect("run migrations");
        (conn, path)
    }

    async fn seed_balance_fixture(conn: &mut crate::db::AsyncDbConnection) {
        let b1_ts = da_biased(1_741_557_600);
        let b2_ts = da_biased(1_741_557_800);

        sql_query(
            "INSERT INTO blocks (id, height, version, parent, timestamp, msg, jam)
             VALUES ('b1', 1, 1, 'p0', ?1, NULL, x'00'),
                    ('b2', 2, 1, 'b1', ?2, NULL, x'00')",
        )
        .bind::<BigInt, _>(b1_ts)
        .bind::<BigInt, _>(b2_ts)
        .execute(conn)
        .await
        .expect("insert blocks");

        sql_query(
            "INSERT INTO transactions (id, block_id, height, version, fee, total_size, jam)
             VALUES ('tx1', 'b2', 2, 1, 5, 200, x'00')",
        )
        .execute(conn)
        .await
        .expect("insert tx");

        sql_query(
            "INSERT INTO notes (
                 first, last, version, assets, coinbase,
                 created_txid, spent_txid, created_height, spent_height,
                 created_bid, spent_bid, jam
             ) VALUES
             ('ncb', 'l1', 0, 50, 1, NULL, 'tx1', 1, 2, 'b1', 'b2', x'00'),
             ('nout_ext', 'l2', 1, 20, 0, 'tx1', NULL, 2, NULL, 'b2', NULL, x'00'),
             ('nout_self', 'l3', 1, 25, 0, 'tx1', NULL, 2, NULL, 'b2', NULL, x'00')",
        )
        .execute(conn)
        .await
        .expect("insert notes");

        sql_query(
            "INSERT INTO credits (txid, first, height, block_id, amount) VALUES
             (NULL, 'ncb', 1, 'b1', 50),
             ('tx1', 'nout_ext', 2, 'b2', 20),
             ('tx1', 'nout_self', 2, 'b2', 25)",
        )
        .execute(conn)
        .await
        .expect("insert credits");

        sql_query(
            "INSERT INTO debits (txid, first, height, block_id, amount, fee)
             VALUES ('tx1', 'ncb', 2, 'b2', 50, 5)",
        )
        .execute(conn)
        .await
        .expect("insert debits");

        sql_query(
            "INSERT INTO name_info (first, height, version, owner_type, owner) VALUES
             ('ncb', 1, 0, 'pk', 'wallet_pk'),
             ('nout_ext', 2, 1, 'pkh', 'other_pkh'),
             ('nout_self', 2, 1, 'pkh', 'wallet_pkh')",
        )
        .execute(conn)
        .await
        .expect("insert name_info");

        sql_query(
            "INSERT INTO pkh_to_pk (pkh, pk, height, block_id)
             VALUES ('wallet_pkh', 'wallet_pk', 1, 'b1')",
        )
        .execute(conn)
        .await
        .expect("insert pkh_to_pk");
    }

    #[tokio::test]
    async fn db_public_key_v0_scope_includes_coinbase() {
        let (mut conn, path) = setup_conn().await;
        seed_balance_fixture(&mut conn).await;

        let address = AddressInfo {
            input: "wallet_pk".to_string(),
            address_type: AddressType::DbPublicKey,
            scope: VersionScope::V0Only,
            pkh: "-".to_string(),
            db_public_key: Some("wallet_pk".to_string()),
        };

        let balance = wallet_balance(&mut conn, address.clone())
            .await
            .expect("wallet_balance");
        assert_eq!(balance.coinbase_credits_nicks, 50);
        assert_eq!(balance.unspent_v0_nicks, 0);
        assert_eq!(balance.balance_nicks, 0);

        let audit = audit_report(&mut conn, address)
            .await
            .expect("audit_report");
        assert!(audit
            .ledger
            .iter()
            .any(|e| e.entry_type == "coinbase" && e.amount_nicks == 50));

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn pkh_v1_only_accounting_invariants_hold() {
        let (mut conn, path) = setup_conn().await;
        seed_balance_fixture(&mut conn).await;

        let address = AddressInfo {
            input: "wallet_pkh".to_string(),
            address_type: AddressType::Pkh,
            scope: VersionScope::V1Only,
            pkh: "wallet_pkh".to_string(),
            db_public_key: Some("wallet_pk".to_string()),
        };

        let balance = wallet_balance(&mut conn, address.clone())
            .await
            .expect("wallet_balance");
        assert_eq!(balance.balance_nicks, 25);
        assert_eq!(balance.tx_credits_nicks, 25);
        assert_eq!(balance.coinbase_credits_nicks, 0);
        assert_eq!(balance.spent_nicks, 0);
        assert_eq!(balance.fees_nicks, 0);
        assert_eq!(
            balance.received_nicks - balance.spent_nicks,
            balance.balance_nicks
        );

        let audit = audit_report(&mut conn, address)
            .await
            .expect("audit_report");
        assert!(!audit.ledger.iter().any(|e| e.entry_type == "coinbase"));
        let outgoing_rows = audit
            .flows
            .iter()
            .filter(|f| f.entry_type == "outgoing")
            .collect::<Vec<_>>();
        assert_eq!(outgoing_rows.len(), 0);

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn strict_split_keeps_pkh_v1_and_pk_v0_separate() {
        let (mut conn, path) = setup_conn().await;
        seed_balance_fixture(&mut conn).await;

        let v1_address = AddressInfo {
            input: "wallet_pkh".to_string(),
            address_type: AddressType::Pkh,
            scope: VersionScope::V1Only,
            pkh: "wallet_pkh".to_string(),
            db_public_key: Some("wallet_pk".to_string()),
        };
        let v1 = wallet_balance(&mut conn, v1_address)
            .await
            .expect("v1 balance");
        assert_eq!(v1.unspent_v0_nicks, 0);
        assert_eq!(v1.unspent_v1_nicks, 0);
        assert_eq!(v1.balance_nicks, 25);
        assert_eq!(v1.coinbase_credits_nicks, 0);
        assert_eq!(v1.tx_credits_nicks, 25);
        assert_eq!(v1.spent_nicks, 0);

        let v0_address = AddressInfo {
            input: "wallet_pk".to_string(),
            address_type: AddressType::DbPublicKey,
            scope: VersionScope::V0Only,
            pkh: "-".to_string(),
            db_public_key: Some("wallet_pk".to_string()),
        };
        let v0 = wallet_balance(&mut conn, v0_address)
            .await
            .expect("v0 balance");
        assert_eq!(v0.balance_nicks, 0);
        assert_eq!(v0.coinbase_credits_nicks, 50);
        assert_eq!(v0.tx_credits_nicks, 0);
        assert_eq!(v0.spent_nicks, 50);
        assert_eq!(v0.fees_nicks, 5);

        let _ = std::fs::remove_file(path);
    }

    async fn seed_summary_refund_fixture(conn: &mut crate::db::AsyncDbConnection) {
        sql_query(
            "INSERT INTO blocks (id, height, version, parent, timestamp, msg, jam)
             VALUES ('b1', 1, 1, 'p0', 1, NULL, x'00'),
                    ('b2', 2, 1, 'b1', 2, NULL, x'00')",
        )
        .execute(conn)
        .await
        .expect("insert blocks");

        sql_query(
            "INSERT INTO transactions (id, block_id, height, version, fee, total_size, jam)
             VALUES ('tx_in', 'b1', 1, 1, 0, 100, x'00'),
                    ('tx_spend', 'b2', 2, 1, 5, 200, x'00')",
        )
        .execute(conn)
        .await
        .expect("insert transactions");

        sql_query(
            "INSERT INTO notes (
                 first, last, version, assets, coinbase,
                 created_txid, spent_txid, created_height, spent_height,
                 created_bid, spent_bid, jam
             ) VALUES
             ('wallet_in', 'l0', 1, 50, 0, 'tx_in', 'tx_spend', 1, 2, 'b1', 'b2', x'00'),
             ('wallet_change', 'l1', 1, 25, 0, 'tx_spend', NULL, 2, NULL, 'b2', NULL, x'00'),
             ('external_out', 'l2', 1, 20, 0, 'tx_spend', NULL, 2, NULL, 'b2', NULL, x'00')",
        )
        .execute(conn)
        .await
        .expect("insert notes");

        sql_query(
            "INSERT INTO credits (txid, first, height, block_id, amount) VALUES
             ('tx_in', 'wallet_in', 1, 'b1', 50),
             ('tx_spend', 'wallet_change', 2, 'b2', 25),
             ('tx_spend', 'external_out', 2, 'b2', 20)",
        )
        .execute(conn)
        .await
        .expect("insert credits");

        sql_query(
            "INSERT INTO debits (txid, first, height, block_id, amount, fee)
             VALUES ('tx_spend', 'wallet_in', 2, 'b2', 50, 5)",
        )
        .execute(conn)
        .await
        .expect("insert debits");

        sql_query(
            "INSERT INTO name_info (first, height, version, owner_type, owner) VALUES
             ('wallet_in', 1, 1, 'pkh', 'wallet_pkh'),
             ('wallet_change', 2, 1, 'pkh', 'wallet_pkh'),
             ('external_out', 2, 1, 'pkh', 'other_pkh')",
        )
        .execute(conn)
        .await
        .expect("insert name_info");
    }

    #[tokio::test]
    async fn summary_omits_refund_change_for_wallet_spend_tx() {
        let (mut conn, path) = setup_conn().await;
        seed_summary_refund_fixture(&mut conn).await;

        let address = AddressInfo {
            input: "wallet_pkh".to_string(),
            address_type: AddressType::Pkh,
            scope: VersionScope::V1Only,
            pkh: "wallet_pkh".to_string(),
            db_public_key: None,
        };

        let audit = audit_report(&mut conn, address)
            .await
            .expect("audit_report");
        assert!(audit
            .flows
            .iter()
            .all(|row| !(row.txid.as_deref() == Some("tx_spend") && row.entry_type == "incoming")));

        let outgoing_rows = audit
            .flows
            .iter()
            .filter(|row| row.txid.as_deref() == Some("tx_spend") && row.entry_type == "outgoing")
            .collect::<Vec<_>>();
        assert_eq!(outgoing_rows.len(), 1);
        assert_eq!(outgoing_rows[0].recipient.as_deref(), Some("other_pkh"));
        assert_eq!(outgoing_rows[0].amount_nicks, 20);
        assert_eq!(outgoing_rows[0].fee_nicks, 5);

        let _ = std::fs::remove_file(path);
    }

    async fn seed_summary_fee_only_fixture(conn: &mut crate::db::AsyncDbConnection) {
        sql_query(
            "INSERT INTO blocks (id, height, version, parent, timestamp, msg, jam)
             VALUES ('b10', 10, 1, 'p0', 10, NULL, x'00'),
                    ('b11', 11, 1, 'b10', 11, NULL, x'00')",
        )
        .execute(conn)
        .await
        .expect("insert blocks");

        sql_query(
            "INSERT INTO transactions (id, block_id, height, version, fee, total_size, jam)
             VALUES ('tx_seed', 'b10', 10, 1, 0, 100, x'00'),
                    ('tx_churn', 'b11', 11, 1, 5, 200, x'00')",
        )
        .execute(conn)
        .await
        .expect("insert transactions");

        sql_query(
            "INSERT INTO notes (
                 first, last, version, assets, coinbase,
                 created_txid, spent_txid, created_height, spent_height,
                 created_bid, spent_bid, jam
             ) VALUES
             ('wallet_seed', 'l0', 1, 50, 0, 'tx_seed', 'tx_churn', 10, 11, 'b10', 'b11', x'00'),
             ('wallet_change2', 'l1', 1, 45, 0, 'tx_churn', NULL, 11, NULL, 'b11', NULL, x'00')",
        )
        .execute(conn)
        .await
        .expect("insert notes");

        sql_query(
            "INSERT INTO credits (txid, first, height, block_id, amount) VALUES
             ('tx_seed', 'wallet_seed', 10, 'b10', 50),
             ('tx_churn', 'wallet_change2', 11, 'b11', 45)",
        )
        .execute(conn)
        .await
        .expect("insert credits");

        sql_query(
            "INSERT INTO debits (txid, first, height, block_id, amount, fee)
             VALUES ('tx_churn', 'wallet_seed', 11, 'b11', 50, 5)",
        )
        .execute(conn)
        .await
        .expect("insert debits");

        sql_query(
            "INSERT INTO name_info (first, height, version, owner_type, owner) VALUES
             ('wallet_seed', 10, 1, 'pkh', 'wallet_pkh'),
             ('wallet_change2', 11, 1, 'pkh', 'wallet_pkh')",
        )
        .execute(conn)
        .await
        .expect("insert name_info");
    }

    #[tokio::test]
    async fn summary_keeps_fee_only_row_when_refund_rows_omitted() {
        let (mut conn, path) = setup_conn().await;
        seed_summary_fee_only_fixture(&mut conn).await;

        let address = AddressInfo {
            input: "wallet_pkh".to_string(),
            address_type: AddressType::Pkh,
            scope: VersionScope::V1Only,
            pkh: "wallet_pkh".to_string(),
            db_public_key: None,
        };

        let audit = audit_report(&mut conn, address)
            .await
            .expect("audit_report");

        let fee_rows = audit
            .flows
            .iter()
            .filter(|row| row.txid.as_deref() == Some("tx_churn"))
            .collect::<Vec<_>>();
        assert_eq!(fee_rows.len(), 1);
        assert_eq!(fee_rows[0].entry_type, "outgoing");
        assert_eq!(fee_rows[0].amount_nicks, 0);
        assert_eq!(fee_rows[0].fee_nicks, 5);
        assert_eq!(fee_rows[0].recipient_type, None);
        assert_eq!(fee_rows[0].recipient, None);

        let final_running = audit
            .flows
            .last()
            .expect("at least one flow row")
            .running_balance_nicks;
        assert_eq!(final_running, audit.balance.balance_nicks);
        assert_eq!(final_running, 45);

        let _ = std::fs::remove_file(path);
    }
}
