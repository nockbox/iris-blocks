use crate::address::AddressInfo;
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
    pub unspent_nicks: i64,
    pub unspent_note_count: i64,
    pub unspent_v0_nicks: i64,
    pub unspent_v1_nicks: i64,
    pub tx_credits_nicks: i64,
    pub coinbase_credits_nicks: i64,
    pub debits_nicks: i64,
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
    pub height: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct TxDebitDetail {
    pub pk: String,
    pub sole_owner: bool,
    pub amount_nicks: i64,
    pub fee_nicks: i64,
    pub height: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransactionDetail {
    pub txid: String,
    pub block_id: String,
    pub height: i32,
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
    pub height: i32,
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
    pub height: i32,
    pub version: i32,
    pub parent: String,
    pub timestamp: i64,
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
    pub height: i32,
    pub entry_type: String,
    pub txid: Option<String>,
    pub block_id: Option<String>,
    pub idx: Option<i32>,
    pub recipient_type: Option<String>,
    pub recipient: Option<String>,
    pub amount_nicks: i64,
    pub fee_nicks: i64,
    pub sole_owner: Option<bool>,
    pub counterparties: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalletTxSummary {
    pub txid: String,
    pub first_height: i32,
    pub direction: String,
    pub incoming_nicks: i64,
    pub outgoing_nicks: i64,
    pub fee_nicks: i64,
    pub net_nicks: i64,
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
    height: i32,
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
    height: i32,
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
    height: i32,
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
    height: i32,
    #[diesel(sql_type = Text)]
    entry_type: String,
    #[diesel(sql_type = Nullable<Text>)]
    txid: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    block_id: Option<String>,
    #[diesel(sql_type = Nullable<Integer>)]
    idx: Option<i32>,
    #[diesel(sql_type = Nullable<Text>)]
    recipient_type: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    recipient: Option<String>,
    #[diesel(sql_type = BigInt)]
    amount_nicks: i64,
    #[diesel(sql_type = BigInt)]
    fee_nicks: i64,
    #[diesel(sql_type = Nullable<Bool>)]
    sole_owner: Option<bool>,
    #[diesel(sql_type = Nullable<Text>)]
    counterparties: Option<String>,
}

fn tx_recipient_filter(db_public_key: Option<&str>) -> String {
    match db_public_key {
        Some(_) => {
            "( (recipient_type IN ('pk','v0pk') AND recipient = ?1) OR (recipient_type = 'pkh' AND recipient = ?2) )".to_string()
        }
        None => "(recipient_type = 'pkh' AND recipient = ?1)".to_string(),
    }
}

pub async fn wallet_balance(
    conn: &mut crate::db::AsyncDbConnection,
    address: AddressInfo,
) -> Result<WalletBalance, QueryError> {
    let pkh = address.pkh.clone();
    let db_pk = address.db_public_key.clone();

    let unspent = sql_query(
        "SELECT COALESCE(SUM(n.assets), 0) AS sum_nicks, COUNT(*) AS note_count
         FROM notes n
         INNER JOIN name_owners no ON n.first = no.first
         WHERE no.pkh = ?1
           AND n.spent_txid IS NULL",
    )
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

    let mut unspent_v0_nicks = 0i64;
    let mut unspent_v1_nicks = 0i64;
    for row in by_version {
        if row.version == 0 {
            unspent_v0_nicks = row.sum_nicks;
        } else {
            unspent_v1_nicks += row.sum_nicks;
        }
    }

    let credit_query = format!(
        "SELECT COALESCE(SUM(amount), 0) AS sum_nicks FROM credits WHERE {}",
        tx_recipient_filter(db_pk.as_deref())
    );
    let tx_credits_nicks = match db_pk.as_deref() {
        Some(pk) => {
            sql_query(&credit_query)
                .bind::<Text, _>(pk.to_string())
                .bind::<Text, _>(pkh.clone())
                .get_result::<SumRow>(conn)
                .await?
                .sum_nicks
        }
        None => {
            sql_query(&credit_query)
                .bind::<Text, _>(pkh.clone())
                .get_result::<SumRow>(conn)
                .await?
                .sum_nicks
        }
    };

    let coinbase_query = format!(
        "SELECT COALESCE(SUM(amount), 0) AS sum_nicks FROM coinbase_credits WHERE {}",
        tx_recipient_filter(db_pk.as_deref())
    );
    let coinbase_credits_nicks = match db_pk.as_deref() {
        Some(pk) => {
            sql_query(&coinbase_query)
                .bind::<Text, _>(pk.to_string())
                .bind::<Text, _>(pkh.clone())
                .get_result::<SumRow>(conn)
                .await?
                .sum_nicks
        }
        None => {
            sql_query(&coinbase_query)
                .bind::<Text, _>(pkh.clone())
                .get_result::<SumRow>(conn)
                .await?
                .sum_nicks
        }
    };

    let (debits_nicks, fees_nicks) = if let Some(pk) = db_pk {
        #[derive(QueryableByName)]
        struct DebitAggRow {
            #[diesel(sql_type = BigInt)]
            amount_sum: i64,
            #[diesel(sql_type = BigInt)]
            fee_sum: i64,
        }
        let row = sql_query(
            "SELECT COALESCE(SUM(amount), 0) AS amount_sum, COALESCE(SUM(fee), 0) AS fee_sum
             FROM debits
             WHERE pk = ?1",
        )
        .bind::<Text, _>(pk)
        .get_result::<DebitAggRow>(conn)
        .await?;
        (row.amount_sum, row.fee_sum)
    } else {
        (0, 0)
    };

    Ok(WalletBalance {
        address,
        unspent_nicks: unspent.sum_nicks,
        unspent_note_count: unspent.note_count,
        unspent_v0_nicks,
        unspent_v1_nicks,
        tx_credits_nicks,
        coinbase_credits_nicks,
        debits_nicks,
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
        "SELECT id AS txid, block_id, height, version, fee, total_size
         FROM transactions
         WHERE id = ?1
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
        "SELECT idx, recipient_type, recipient, amount, height
         FROM credits
         WHERE txid = ?1
         ORDER BY idx",
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
        height: r.height,
    })
    .collect();

    let debits = sql_query(
        "SELECT pk, sole_owner, amount, fee, height
         FROM debits
         WHERE txid = ?1
         ORDER BY pk",
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
        height: r.height,
    })
    .collect();

    Ok(TransactionDetail {
        txid: base.txid,
        block_id: base.block_id,
        height: base.height,
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
        "SELECT idx, recipient_type, recipient, amount, height
         FROM coinbase_credits
         WHERE block_id = ?1
         ORDER BY idx",
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
        height: r.height,
    })
    .collect();

    Ok(BlockDetail {
        id: base.id,
        height: base.height,
        version: base.version,
        parent: base.parent,
        timestamp: base.timestamp,
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
    let db_pk = address.db_public_key.clone();

    let mut ledger = Vec::new();

    let credit_filter = tx_recipient_filter(db_pk.as_deref());
    let credit_q = format!(
        "SELECT c.height AS height,
                'credit' AS entry_type,
                c.txid AS txid,
                NULL AS block_id,
                c.idx AS idx,
                c.recipient_type AS recipient_type,
                c.recipient AS recipient,
                c.amount AS amount_nicks,
                0 AS fee_nicks,
                NULL AS sole_owner,
                (SELECT GROUP_CONCAT(DISTINCT s.pk) FROM tx_signers s WHERE s.txid = c.txid) AS counterparties
         FROM credits c
         WHERE {}
         ORDER BY c.height, c.txid, c.idx",
        credit_filter
    );

    let credit_rows = match db_pk.as_deref() {
        Some(pk) => {
            sql_query(&credit_q)
                .bind::<Text, _>(pk.to_string())
                .bind::<Text, _>(pkh.clone())
                .load::<LedgerRow>(conn)
                .await?
        }
        None => {
            sql_query(&credit_q)
                .bind::<Text, _>(pkh.clone())
                .load::<LedgerRow>(conn)
                .await?
        }
    };
    ledger.extend(credit_rows.into_iter().map(|r| LedgerEntry {
        height: r.height,
        entry_type: r.entry_type,
        txid: r.txid,
        block_id: r.block_id,
        idx: r.idx,
        recipient_type: r.recipient_type,
        recipient: r.recipient,
        amount_nicks: r.amount_nicks,
        fee_nicks: r.fee_nicks,
        sole_owner: r.sole_owner,
        counterparties: r.counterparties,
    }));

    let coinbase_filter = tx_recipient_filter(db_pk.as_deref());
    let coinbase_q = format!(
        "SELECT cc.height AS height,
                'coinbase' AS entry_type,
                NULL AS txid,
                cc.block_id AS block_id,
                cc.idx AS idx,
                cc.recipient_type AS recipient_type,
                cc.recipient AS recipient,
                cc.amount AS amount_nicks,
                0 AS fee_nicks,
                NULL AS sole_owner,
                NULL AS counterparties
         FROM coinbase_credits cc
         WHERE {}
         ORDER BY cc.height, cc.idx",
        coinbase_filter
    );
    let coinbase_rows = match db_pk.as_deref() {
        Some(pk) => {
            sql_query(&coinbase_q)
                .bind::<Text, _>(pk.to_string())
                .bind::<Text, _>(pkh.clone())
                .load::<LedgerRow>(conn)
                .await?
        }
        None => {
            sql_query(&coinbase_q)
                .bind::<Text, _>(pkh.clone())
                .load::<LedgerRow>(conn)
                .await?
        }
    };
    ledger.extend(coinbase_rows.into_iter().map(|r| LedgerEntry {
        height: r.height,
        entry_type: r.entry_type,
        txid: r.txid,
        block_id: r.block_id,
        idx: r.idx,
        recipient_type: r.recipient_type,
        recipient: r.recipient,
        amount_nicks: r.amount_nicks,
        fee_nicks: r.fee_nicks,
        sole_owner: r.sole_owner,
        counterparties: r.counterparties,
    }));

    if let Some(pk) = db_pk {
        let debit_rows = sql_query(
            "SELECT d.height AS height,
                    'debit' AS entry_type,
                    d.txid AS txid,
                    NULL AS block_id,
                    NULL AS idx,
                    'pk' AS recipient_type,
                    d.pk AS recipient,
                    d.amount AS amount_nicks,
                    d.fee AS fee_nicks,
                    d.sole_owner AS sole_owner,
                    (SELECT GROUP_CONCAT(DISTINCT c.recipient) FROM credits c WHERE c.txid = d.txid) AS counterparties
             FROM debits d
             WHERE d.pk = ?1
             ORDER BY d.height, d.txid",
        )
        .bind::<Text, _>(pk)
        .load::<LedgerRow>(conn)
        .await?;

        ledger.extend(debit_rows.into_iter().map(|r| LedgerEntry {
            height: r.height,
            entry_type: r.entry_type,
            txid: r.txid,
            block_id: r.block_id,
            idx: r.idx,
            recipient_type: r.recipient_type,
            recipient: r.recipient,
            amount_nicks: r.amount_nicks,
            fee_nicks: r.fee_nicks,
            sole_owner: r.sole_owner,
            counterparties: r.counterparties,
        }));
    }

    ledger.sort_by(|a, b| {
        a.height
            .cmp(&b.height)
            .then_with(|| a.txid.cmp(&b.txid))
            .then_with(|| a.idx.cmp(&b.idx))
            .then_with(|| a.entry_type.cmp(&b.entry_type))
    });

    use std::collections::BTreeMap;
    let mut tx_map: BTreeMap<String, WalletTxSummary> = BTreeMap::new();
    for entry in &ledger {
        let Some(txid) = entry.txid.clone() else {
            continue;
        };
        let summary = tx_map.entry(txid.clone()).or_insert(WalletTxSummary {
            txid,
            first_height: entry.height,
            direction: "incoming".to_string(),
            incoming_nicks: 0,
            outgoing_nicks: 0,
            fee_nicks: 0,
            net_nicks: 0,
        });

        if entry.height < summary.first_height {
            summary.first_height = entry.height;
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
