pub mod schema;

use super::{layer::*, shared_schema::*};
use crate::chain_activations::ChainActivations;
use crate::db::AsyncDbConnection;
use crate::rt::{self, RtBound, RtSync};
use crate::scry::{NounError, ScryError, ScryFailed, Scryable};
use crate::StringDigest;
use clap::Parser;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use futures::channel::{mpsc, oneshot};
use futures::{FutureExt, StreamExt};
use iris_nockchain_types::BlockHeight;
use iris_ztd::{jam, Digest, NounEncode};
use log::*;
use schema::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;
use tokio::sync::Mutex;

mod block_range_manager;
use block_range_manager::BlockRangeManager;

#[derive(Debug, Clone, Parser)]
pub struct L0Config {
    #[command(flatten)]
    pub block_range_config: block_range_manager::BlockRangeConfig,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub store_pow: bool,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub verify_outputs: bool,
}

impl Default for L0Config {
    fn default() -> Self {
        Self {
            block_range_config: Default::default(),
            store_pow: false,
            verify_outputs: true,
        }
    }
}

pub struct L0Client<S: Scryable, D: AsRef<dyn LayerDependency> = Arc<dyn LayerDependency>> {
    conn: Arc<Mutex<AsyncDbConnection>>,
    client: Option<S>,
    manager: Option<BlockRangeManager<S>>,
    dependents: Box<[D]>,
    activations: ChainActivations,
    config: L0Config,
    stats_tx: watch::Sender<Option<<Self as LayerBase>::Stats>>,
    stats_rx: watch::Receiver<Option<<Self as LayerBase>::Stats>>,
    query_rx: mpsc::UnboundedReceiver<L0Request<S>>,
}

pub enum L0Request<S> {
    Query {
        sql: String,
        responder: oneshot::Sender<Result<Vec<serde_json::Value>, String>>,
    },
    Export {
        responder: oneshot::Sender<Result<Vec<u8>, String>>,
    },
    UpdateRpc {
        client: Option<S>,
    },
}

#[derive(Clone)]
pub struct L0Handle<S> {
    tx: mpsc::UnboundedSender<L0Request<S>>,
}

impl<S: Scryable> L0Handle<S> {
    pub async fn query(&self, sql: String) -> Result<Vec<serde_json::Value>, String> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .unbounded_send(L0Request::Query { sql, responder: tx })
            .map_err(|e| e.to_string())?;
        rx.await.map_err(|e| e.to_string())?
    }

    pub async fn export(&self) -> Result<Vec<u8>, String> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .unbounded_send(L0Request::Export { responder: tx })
            .map_err(|e| e.to_string())?;
        rx.await.map_err(|e| e.to_string())?
    }

    pub fn update_rpc(&self, client: Option<S>) -> Result<(), String> {
        self.tx
            .unbounded_send(L0Request::UpdateRpc { client })
            .map_err(|e| e.to_string())
    }
}

#[derive(Debug, Error)]
pub enum L0Error {
    #[error(transparent)]
    DieselError(#[from] diesel::result::Error),
    #[error(transparent)]
    TonicError(#[from] tonic::Status),
    #[error("No new blocks and no genesis block stored")]
    NoNewBlocksNoGenesis,
    #[error("No new blocks")]
    NoNewBlocks(FixedLayerMetadata, Digest, BlockHeight, bool),
    #[error("Unable to parse blocks range response")]
    UnableToParseBlocksRangeResponse,
    #[error(
        "Block {0} missing pow. Are you peeking with --store-pow=true --block-range-scry-no-pow?"
    )]
    BlockMissingPow(Digest),
    #[error("TX {0} invalid on block {1} ({2})")]
    TransactionInvalid(Digest, BlockHeight, Digest),
    #[error(transparent)]
    Noun(#[from] NounError),
    #[error(transparent)]
    ScryFailed(#[from] ScryFailed),
    #[error("Chain reoverted")]
    Reverted,
    #[error("Unable to pull elders at block {1} on height {0}")]
    UnableToPullElders(BlockHeight, Digest),
    #[error(transparent)]
    LayerError(#[from] LayerError),
    #[error("gRPC connection needed, but not provided")]
    GrpcNeeded,
    #[error("numeric value out of range for {field}: {value}")]
    ValueOutOfRange { field: &'static str, value: u64 },
}

impl From<ScryError> for L0Error {
    fn from(value: ScryError) -> Self {
        match value {
            ScryError::Noun(n) => L0Error::Noun(n),
            ScryError::ScryFailed(s) => L0Error::ScryFailed(s),
            ScryError::Tonic(t) => L0Error::TonicError(t),
        }
    }
}

impl<S: Scryable, D: AsRef<dyn LayerDependency> + RtBound + RtSync> L0Client<S, D> {
    fn checked_u64_to_i64(value: u64, field: &'static str) -> Result<i64, L0Error> {
        i64::try_from(value).map_err(|_| L0Error::ValueOutOfRange { field, value })
    }

    fn checked_u64_to_i32(value: u64, field: &'static str) -> Result<i32, L0Error> {
        i32::try_from(value).map_err(|_| L0Error::ValueOutOfRange { field, value })
    }

    pub fn new(
        conn: Arc<Mutex<AsyncDbConnection>>,
        client: Option<S>,
        config: L0Config,
        activations: ChainActivations,
        dependents: impl Into<Box<[D]>>,
    ) -> (Self, L0Handle<S>) {
        let dependents = dependents.into();
        let (stats_tx, stats_rx) = Self::verify_dependents(&dependents).unwrap();
        let (query_tx, query_rx) = mpsc::unbounded();
        (
            Self {
                conn,
                client: client.clone(),
                manager: client
                    .map(|client| BlockRangeManager::new(client, config.block_range_config)),
                activations,
                dependents,
                config,
                stats_tx,
                stats_rx,
                query_rx,
            },
            L0Handle { tx: query_tx },
        )
    }

    async fn revert_to(
        &self,
        conn: &mut AsyncDbConnection,
        new_next_block_height: u32,
    ) -> Result<FixedLayerMetadata, L0Error> {
        debug!("Reverting l0 to next_block={new_next_block_height}");

        let metadata = FixedLayerMetadata {
            layer: "l0",
            next_block_height: new_next_block_height as _,
        };

        for dep in self.dependents.iter().rev().map(AsRef::as_ref) {
            dep.expire_blocks(conn, metadata).await?;
        }

        trace!("Dropping transactions");

        diesel::delete(transactions::table)
            .filter(transactions::height.ge(new_next_block_height as i32))
            .execute(conn)
            .await?;

        trace!("Dropping blocks");

        diesel::delete(blocks::table)
            .filter(blocks::height.ge(new_next_block_height as i32))
            .execute(conn)
            .await?;

        trace!("Setting metadata");

        Self::update_layer_metadata(&metadata).execute(conn).await?;

        Err(L0Error::Reverted)
    }

    fn chain_reorged<'a>(
        &'a self,
        conn: &'a mut AsyncDbConnection,
        mismatch_block_height: BlockHeight,
        mismatch_block: Digest,
    ) -> impl core::future::Future<Output = Result<FixedLayerMetadata, L0Error>> + Send + 'a {
        self.chain_reorged_impl(conn, mismatch_block_height, mismatch_block)
    }

    async fn chain_reorged_impl<'a>(
        &'a self,
        conn: &'a mut AsyncDbConnection,
        mut mismatch_block_height: BlockHeight,
        mut mismatch_block: Digest,
    ) -> Result<FixedLayerMetadata, L0Error> {
        // Reorg recovery: walk ancestor candidates ("elders") until we find
        // a block hash that matches local state, then roll back above it.
        debug!("Chain reorg detected at height {mismatch_block_height}. Finding common ancestor");

        let mut client = self.client.clone().ok_or(L0Error::GrpcNeeded)?;

        while mismatch_block_height > 0 {
            let mut client = client.clone();
            trace!("Querying elders for block {mismatch_block} {mismatch_block_height}");
            let Some(Some((last_bid, blocks))): Option<Option<(BlockHeight, Vec<Digest>)>> =
                async move {
                    client
                        .remote_scry(("elders", StringDigest(mismatch_block), 0))
                        .await
                }
                .await?
            else {
                return Err(L0Error::UnableToPullElders(
                    mismatch_block_height,
                    mismatch_block,
                ));
            };

            let cur_blocks = blocks::table
                .select(JamlessBlock::as_select())
                .filter(blocks::height.le(last_bid as i32))
                .order_by(blocks::height.desc())
                .limit(blocks.len() as i64)
                .load::<JamlessBlock>(conn)
                .await?;

            for (remote, local) in blocks.into_iter().zip(cur_blocks) {
                if remote == *local.id {
                    debug!(
                        "Common ancestor {remote} found at height {}. Expiring older blocks",
                        local.height
                    );
                    break;
                }
                mismatch_block_height = local.height as BlockHeight;
                mismatch_block = remote;
            }
        }

        self.revert_to(conn, mismatch_block_height).await
    }

    #[tracing::instrument(skip_all)]
    async fn update_blocks_impl(
        &mut self,
        conn: &mut AsyncDbConnection,
    ) -> Result<FixedLayerMetadata, L0Error> {
        trace!("Updating blocks");

        let Some(mdata) = Self::layer_metadata(conn).await? else {
            debug!("Metadata not set. Triggering reset");
            return self.revert_to(conn, 0).await;
        };

        let cur_tail: Option<JamlessBlock> = blocks::table
            .select(JamlessBlock::as_select())
            .order_by(blocks::height.desc())
            .limit(1)
            .load::<JamlessBlock>(conn)
            .await?
            .pop();

        let cur_tail = if let Some(last) = cur_tail {
            if last.height + 1 > mdata.next_block_height {
                debug!(
                    "Current tail ({}) is not below the target next block height ({})",
                    last.height, mdata.next_block_height
                );
                return self
                    .revert_to(
                        conn,
                        core::cmp::min(last.height + 1, mdata.next_block_height) as _,
                    )
                    .await;
            }
            trace!("Current tail at height {}", last.height);
            Some((last, mdata))
        } else {
            trace!("No blocks in database");
            None
        };

        let next_height_start = cur_tail
            .as_ref()
            .map(|(b, _)| b.height as u64 + 1)
            .unwrap_or_default();

        let Some(manager) = self.manager.as_mut() else {
            warn!("No gRPC connection available. Will not pull blocks");
            if let Some((tail_block, mdata)) = cur_tail {
                return Err(L0Error::NoNewBlocks(
                    mdata,
                    tail_block.id.into(),
                    tail_block.height as BlockHeight,
                    false,
                ));
            } else {
                return Err(L0Error::GrpcNeeded);
            }
        };

        let Some(Some(new_blocks)) = manager.scry_blocks(next_height_start).await? else {
            return Err(L0Error::UnableToParseBlocksRangeResponse);
        };
        //let new_blocks: Vec<(BlockHeight, Digest, iris_nockchain_types::Page, iris_ztd::ZMap<Digest, iris_nockchain_types::Tx>)> = vec![];

        match (cur_tail, new_blocks.is_empty()) {
            (Some((tail_block, mdata)), true) => {
                return Err(L0Error::NoNewBlocks(
                    mdata,
                    tail_block.id.into(),
                    tail_block.height as BlockHeight,
                    false,
                ))
            }
            (Some((tail_block, _)), false) => {
                if new_blocks[0].0 != 0 && new_blocks[0].2.parent() != *tail_block.id {
                    return self
                        .chain_reorged(conn, new_blocks[0].0, new_blocks[0].1)
                        .await;
                }
            }
            (None, true) => return Err(L0Error::NoNewBlocksNoGenesis),
            _ => (),
        }

        let mut create_blocks = vec![];
        let mut create_txs = vec![];

        for (height, bid, mut block, txs) in new_blocks {
            debug!(
                "Processing block {bid} at height {height}. Num transactions: {}",
                txs.len()
            );

            let pow_jam = match (self.config.store_pow, block.pow_mut()) {
                (true, Some(pow)) => Some(jam(pow.to_noun())),
                (true, None) => return Err(L0Error::BlockMissingPow(bid)),
                (false, _) => None,
            };

            *block.pow_mut() = None;

            create_blocks.push(Block {
                id: bid.into(),
                height: height as _,
                version: block.version() as _,
                parent: block.parent().into(),
                timestamp: Self::checked_u64_to_i64(
                    block.timestamp().as_unix_seconds().unwrap(),
                    "blocks.timestamp",
                )?,
                msg: block.msg().try_into().ok(),
                jam: jam(block.to_noun()),
                pow_jam,
            });
            for (txid, tx) in txs {
                let rtx = tx.raw();

                if self.config.verify_outputs {
                    let mut chain_outputs = tx.outputs().notes();
                    chain_outputs.sort_by_key(|n| n.name());
                    let mut outputs = rtx.outputs(height, self.activations.tx_engine(height));
                    outputs.sort_by_key(|n| n.name());

                    // Optional integrity check: verify local output derivation
                    // matches outputs provided by chain data.
                    if chain_outputs != outputs {
                        trace!(
                            "Transaction {} invalid.\nChain outputs: {:?}\nComputed outputs: {:?}",
                            txid,
                            chain_outputs,
                            outputs
                        );
                        return Err(L0Error::TransactionInvalid(txid, height, bid));
                    }
                }

                create_txs.push(Transaction {
                    id: txid.into(),
                    block_id: bid.into(),
                    height: height as _,
                    version: rtx.version() as _,
                    fee: Self::checked_u64_to_i64(rtx.total_fees().0, "transactions.fee")?,
                    total_size: Self::checked_u64_to_i32(
                        tx.total_size() as u64,
                        "transactions.total_size",
                    )?,
                    jam: jam(tx.to_noun()),
                });

                crate::rt::yield_now().await;
            }
        }

        let metadata = FixedLayerMetadata {
            layer: self.layer(),
            next_block_height: create_blocks.last().unwrap().height + 1,
        };

        let cur_txs: i64 = transactions::table.count().get_result(conn).await?;

        let new_stats = L0Stats {
            next_block_height: metadata.next_block_height as _,
            total_txs: cur_txs as u64 + create_txs.len() as u64,
        };

        conn.spawn_blocking(move |conn| {
            use diesel::query_dsl::methods::ExecuteDsl;
            conn.transaction(move |conn| {
                let q1 = diesel::insert_into(blocks::table).values(create_blocks);
                let q2 = diesel::insert_into(transactions::table).values(create_txs);
                let q3 = Self::update_layer_metadata(&metadata);

                ExecuteDsl::execute(q1, conn)?;
                ExecuteDsl::execute(q2, conn)?;
                ExecuteDsl::execute(q3, conn)?;
                Ok(())
            })
        })
        .await?;

        self.stats_tx.send(Some(new_stats)).ok();

        let metadata = FixedLayerMetadata {
            layer: self.layer(),
            next_block_height: 0,
        };

        Ok(metadata)
    }

    #[tracing::instrument(skip_all)]
    async fn update_blocks(&mut self) -> Result<(), L0Error> {
        let mut res = Ok(());

        // NOTE: we do not re-acquire the lock inside.
        let conn = self.conn.clone();
        let mut conn = conn.lock().await;
        let conn = &mut *conn;

        let metadata = match self.update_blocks_impl(conn).await {
            Ok(metadata) => metadata,
            Err(L0Error::NoNewBlocks(metadata, a, b, _)) => {
                res = Err(L0Error::NoNewBlocks(metadata, a, b, false));
                metadata
            }
            Err(e) => return Err(e),
        };

        let mut cur_metadata = metadata;
        for dep in self.dependents.iter().map(AsRef::as_ref) {
            trace!("Updating {}", dep.layer());
            cur_metadata = dep.update_blocks(conn, cur_metadata).await?;
        }
        let deps_has_more = cur_metadata.next_block_height != metadata.next_block_height;

        // Preserve `deps_has_more` in the NoNewBlocks result so callers can
        // keep advancing dependent layers even when L0 itself is caught up.
        if let Err(L0Error::NoNewBlocks(m, a, b, _)) = res {
            res = Err(L0Error::NoNewBlocks(m, a, b, deps_has_more));
        }

        res
    }

    pub fn run(self) -> impl core::future::Future<Output = ()> + RtBound {
        self.run_impl()
    }

    #[tracing::instrument(skip_all)]
    async fn run_impl(mut self) {
        loop {
            match self.update_blocks().await {
                // New blocks were ingested; continue immediately.
                Ok(_) => {}
                Err(L0Error::Reverted) => {
                    debug!("Chain reverted. Restarting...");
                }
                Err(L0Error::NoNewBlocks(metadata, block, height, deps_has_more)) => {
                    debug!("Chain up-to-date at block {block}, height {height}");
                    if deps_has_more {
                        self.deps_loop_and_process_queries(
                            std::time::Duration::from_secs(30),
                            metadata,
                        )
                        .await;
                    } else {
                        self.sleep_and_process_queries(std::time::Duration::from_secs(30))
                            .await;
                    }
                }
                Err(e)
                    if matches!(
                        e,
                        L0Error::ScryFailed(_)
                            | L0Error::UnableToParseBlocksRangeResponse
                            | L0Error::Noun(_)
                    ) =>
                {
                    debug!("Failed to get new blocks: {e}. Sleeping for 30 seconds.");
                    self.sleep_and_process_queries(std::time::Duration::from_secs(30))
                        .await;
                }
                Err(e) => {
                    error!("Error updating blocks: {e} ({e:?})");
                    self.sleep_and_process_queries(std::time::Duration::from_secs(30))
                        .await;
                }
            }
            crate::rt::yield_now().await;
            self.process_queries().await;
        }
    }

    async fn process_queries(&mut self) {
        while let Some(req) = self.query_rx.next().now_or_never().flatten() {
            self.process_query(req).await;
        }
    }

    async fn process_query(&mut self, req: L0Request<S>) {
        let conn = self.conn.clone();
        let mut conn = conn.lock().await;
        match req {
            L0Request::Query { sql, responder } => {
                let res = conn
                    .spawn_blocking(
                        move |conn| -> Result<Vec<serde_json::Value>, diesel::result::Error> {
                            crate::sqlite_raw::raw_query_json(conn, &sql).map_err(|e| {
                                diesel::result::Error::DatabaseError(
                                    diesel::result::DatabaseErrorKind::Unknown,
                                    Box::new(e),
                                )
                            })
                        },
                    )
                    .await;
                let res = match res {
                    Ok(val) => Ok(val),
                    Err(e) => Err(e.to_string()),
                };
                responder.send(res).ok();
            }
            L0Request::Export { responder } => {
                let res = conn
                    .spawn_blocking(move |conn| -> Result<Vec<u8>, diesel::result::Error> {
                        crate::sqlite_raw::serialize_db(conn).map_err(|e| {
                            diesel::result::Error::DatabaseError(
                                diesel::result::DatabaseErrorKind::Unknown,
                                Box::new(e),
                            )
                        })
                    })
                    .await;
                let res = match res {
                    Ok(val) => Ok(val),
                    Err(e) => Err(e.to_string()),
                };
                responder.send(res).ok();
            }
            L0Request::UpdateRpc { client } => {
                info!("Updating RPC client");
                self.manager = client
                    .as_ref()
                    .map(|c| BlockRangeManager::new(c.clone(), self.config.block_range_config));
                self.client = client;
            }
        }
    }

    async fn sleep_and_process_queries(&mut self, duration: std::time::Duration) {
        let sleep = rt::sleep(duration).fuse();
        let mut sleep = core::pin::pin!(sleep);
        loop {
            futures::select! {
                _ = sleep => break,
                req = self.query_rx.next() => {
                    let Some(req) = req else {
                        sleep.await;
                        break;
                    };
                    self.process_query(req).await;
                }
            }
        }
    }

    /// Like `sleep_and_process_queries`, but keeps calling deps while they
    /// have more work. Stops when all deps return false or duration expires.
    async fn deps_loop_and_process_queries(
        &mut self,
        duration: std::time::Duration,
        metadata: FixedLayerMetadata,
    ) {
        let deadline = rt::sleep(duration).fuse();
        let mut deadline = core::pin::pin!(deadline);

        loop {
            // Drain any pending query requests first.
            self.process_queries().await;

            // Stop once the wait deadline is reached.
            if (&mut deadline).now_or_never().is_some() {
                break;
            }

            let conn = self.conn.clone();
            let mut conn = conn.lock().await;

            // Let dependent layers advance while time remains.
            let mut cur_metadata = metadata;
            for dep in self.dependents.iter().map(AsRef::as_ref) {
                match dep.update_blocks(&mut conn, cur_metadata).await {
                    Ok(new_metadata) => {
                        cur_metadata = new_metadata;
                    }
                    Err(e) => {
                        error!("Error updating dependency {}: {e:?}", dep.layer());
                        break;
                    }
                }
            }
            let has_more = cur_metadata.next_block_height != metadata.next_block_height;

            core::mem::drop(conn);

            if !has_more {
                // Dependencies are caught up; just wait while still serving queries.
                loop {
                    futures::select! {
                        _ = deadline => break,
                        req = self.query_rx.next() => {
                            let Some(req) = req else {
                                deadline.await;
                                break;
                            };
                            self.process_query(req).await;
                        }
                    }
                }
                break;
            }

            crate::rt::yield_now().await;
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(from_wasm_abi, into_wasm_abi))]
pub struct L0Stats {
    pub next_block_height: u32,
    pub total_txs: u64,
}

impl<S: Scryable, D: AsRef<dyn LayerDependency>> LayerBase for L0Client<S, D> {
    const DEPEND_ON_LAYERS: &'static [&'static str] = &[];
    const LAYER: &'static str = "l0";
    type Stats = L0Stats;
    fn stats_handle(&self) -> watch::Receiver<Option<Self::Stats>> {
        self.stats_rx.clone()
    }
}
