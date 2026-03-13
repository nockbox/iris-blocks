pub mod schema;

use super::{layer::*, shared_schema::*};
use crate::chain_activations::ChainActivations;
use crate::db::AsyncDbConnection;
use crate::rt;
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

mod block_range_manager;
use block_range_manager::BlockRangeManager;

#[derive(Debug, Clone, Parser)]
pub struct L0Config {
    #[command(flatten)]
    pub block_range_config: block_range_manager::BlockRangeConfig,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub store_pow: bool,
}

impl Default for L0Config {
    fn default() -> Self {
        Self {
            block_range_config: Default::default(),
            store_pow: false,
        }
    }
}

pub struct L0Client<S: Scryable> {
    conn: AsyncDbConnection,
    client: Option<S>,
    manager: Option<BlockRangeManager<S>>,
    dependencies: Vec<Arc<dyn LayerDependency>>,
    activations: ChainActivations,
    config: L0Config,
    stats_tx: watch::Sender<Option<<Self as LayerBase>::Stats>>,
    stats_rx: watch::Receiver<Option<<Self as LayerBase>::Stats>>,
    query_rx: mpsc::UnboundedReceiver<DbQueryRequest>,
}

pub struct DbQueryRequest {
    pub sql: String,
    pub responder: oneshot::Sender<Result<Vec<serde_json::Value>, String>>,
}

#[derive(Clone)]
pub struct DbQueryHandle {
    tx: mpsc::UnboundedSender<DbQueryRequest>,
}

impl DbQueryHandle {
    pub async fn query(&self, sql: String) -> Result<Vec<serde_json::Value>, String> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .unbounded_send(DbQueryRequest { sql, responder: tx })
            .map_err(|e| e.to_string())?;
        rx.await.map_err(|e| e.to_string())?
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
    NoNewBlocks(FixedLayerMetadata, Digest, BlockHeight),
    #[error("Unable to parse blocks range response")]
    UnableToParseBlocksRangeResponse,
    #[error("Block missing pow")]
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

impl<S: Scryable> L0Client<S> {
    pub fn new(
        conn: AsyncDbConnection,
        client: Option<S>,
        config: L0Config,
        activations: ChainActivations,
        dependencies: Vec<Arc<dyn LayerDependency>>,
    ) -> (Self, DbQueryHandle) {
        let (stats_tx, stats_rx) = Self::verify_dependencies(&dependencies).unwrap();
        let (query_tx, query_rx) = mpsc::unbounded();
        (
            Self {
                conn,
                client: client.clone(),
                manager: client
                    .map(|client| BlockRangeManager::new(client, config.block_range_config)),
                activations,
                dependencies,
                config,
                stats_tx,
                stats_rx,
                query_rx,
            },
            DbQueryHandle { tx: query_tx },
        )
    }

    async fn revert_to(
        &mut self,
        new_next_block_height: u32,
    ) -> Result<FixedLayerMetadata, L0Error> {
        debug!("Reverting l0 to next_block={new_next_block_height}");

        let metadata = FixedLayerMetadata {
            layer: "l0",
            next_block_height: new_next_block_height as _,
        };

        for dep in self.dependencies.iter() {
            dep.expire_blocks(&mut self.conn, metadata).await?;
        }

        trace!("Dropping transactions");

        diesel::delete(transactions::table)
            .filter(transactions::height.ge(new_next_block_height as i32))
            .execute(&mut self.conn)
            .await?;

        trace!("Dropping blocks");

        diesel::delete(blocks::table)
            .filter(blocks::height.ge(new_next_block_height as i32))
            .execute(&mut self.conn)
            .await?;

        trace!("Setting metadata");

        Self::update_layer_metadata(&metadata)
            .execute(&mut self.conn)
            .await?;

        Err(L0Error::Reverted)
    }

    async fn chain_reorged(
        &mut self,
        mut mismatch_block_height: BlockHeight,
        mismatch_block: Digest,
    ) -> Result<FixedLayerMetadata, L0Error> {
        // Recovery - walk up the elders until we find a common ancestor
        debug!("Chain reorg detected at height {mismatch_block_height}. Finding common ancestor");

        let client = self.client.as_mut().ok_or(L0Error::GrpcNeeded)?;

        while mismatch_block_height > 0 {
            let Some(Some((last_bid, blocks))): Option<Option<(BlockHeight, Vec<Digest>)>> = client
                .remote_scry(("elders", StringDigest(mismatch_block)))
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
                .load::<JamlessBlock>(&mut self.conn)
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
            }
        }

        self.revert_to(mismatch_block_height).await
    }

    #[tracing::instrument(skip_all)]
    async fn update_blocks_impl(&mut self) -> Result<FixedLayerMetadata, L0Error> {
        trace!("Updating blocks");

        let Some(mdata) = Self::layer_metadata(&mut self.conn).await? else {
            debug!("Metadata not set. Triggering reset");
            return self.revert_to(0).await;
        };

        let cur_tail: Option<JamlessBlock> = blocks::table
            .select(JamlessBlock::as_select())
            .order_by(blocks::height.desc())
            .limit(1)
            .load::<JamlessBlock>(&mut self.conn)
            .await?
            .pop();

        let cur_tail = if let Some(last) = cur_tail {
            if last.height + 1 > mdata.next_block_height {
                debug!(
                    "Current tail ({}) is not below the target next block height ({})",
                    last.height, mdata.next_block_height
                );
                return self
                    .revert_to(core::cmp::min(last.height + 1, mdata.next_block_height) as _)
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
                ));
            } else {
                return Err(L0Error::GrpcNeeded);
            }
        };

        let Some(Some(new_blocks)) = manager.scry_blocks(next_height_start).await? else {
            return Err(L0Error::UnableToParseBlocksRangeResponse);
        };

        match (cur_tail, new_blocks.is_empty()) {
            (Some((tail_block, mdata)), true) => {
                return Err(L0Error::NoNewBlocks(
                    mdata,
                    tail_block.id.into(),
                    tail_block.height as BlockHeight,
                ))
            }
            (Some((tail_block, _)), false) => {
                if new_blocks[0].0 != 0 && new_blocks[0].2.parent() != *tail_block.id {
                    return self.chain_reorged(new_blocks[0].0, new_blocks[0].1).await;
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
            if !self.config.store_pow {
                *block.pow_mut() = None;
            } else if block.pow_mut().is_none() {
                return Err(L0Error::BlockMissingPow(bid));
            }

            create_blocks.push(Block {
                id: bid.into(),
                height: height as _,
                version: block.version() as _,
                parent: block.parent().into(),
                timestamp: block.timestamp() as _,
                msg: block.msg().try_into().ok(),
                jam: jam(block.to_noun()),
            });
            for (txid, tx) in txs {
                let rtx = tx.raw();

                let mut chain_outputs = tx.outputs().notes();
                chain_outputs.sort_by_key(|n| n.name());
                let mut outputs = rtx.outputs(height, self.activations.tx_engine(height));
                outputs.sort_by_key(|n| n.name());

                // Verify that iris computes outputs correctly
                if chain_outputs != outputs {
                    trace!(
                        "Transaction {} invalid.\nChain outputs: {:?}\nComputed outputs: {:?}",
                        txid,
                        chain_outputs,
                        outputs
                    );
                    return Err(L0Error::TransactionInvalid(txid, height, bid));
                }

                create_txs.push(Transaction {
                    id: txid.into(),
                    block_id: bid.into(),
                    height: height as _,
                    version: rtx.version() as _,
                    fee: rtx.total_fees().0 as _,
                    total_size: tx.total_size() as _,
                    jam: jam(rtx.to_noun()),
                });

                crate::rt::yield_now().await;
            }
        }

        let metadata = FixedLayerMetadata {
            layer: self.layer(),
            next_block_height: create_blocks.last().unwrap().height + 1,
        };

        let cur_txs: i64 = transactions::table
            .count()
            .get_result(&mut self.conn)
            .await?;

        let new_stats = L0Stats {
            next_block_height: metadata.next_block_height as _,
            total_txs: cur_txs as u64 + create_txs.len() as u64,
        };

        self.conn
            .spawn_blocking(move |conn| {
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

        Ok(metadata)
    }

    #[tracing::instrument(skip_all)]
    async fn update_blocks(&mut self) -> Result<(), L0Error> {
        let mut res = Ok(());

        let metadata = match self.update_blocks_impl().await {
            Ok(metadata) => metadata,
            Err(L0Error::NoNewBlocks(metadata, a, b)) => {
                res = Err(L0Error::NoNewBlocks(metadata, a, b));
                metadata
            }
            Err(e) => return Err(e),
        };

        for dep in self.dependencies.iter() {
            trace!("Updating {}", dep.layer());
            dep.update_blocks(&mut self.conn, metadata).await?;
        }

        res
    }

    #[tracing::instrument(skip_all)]
    pub async fn run(mut self) {
        loop {
            match self.update_blocks().await {
                // We updated successfully, continue without sleeping
                Ok(_) => {}
                Err(L0Error::Reverted) => {
                    debug!("Chain reverted. Restarting...");
                }
                Err(L0Error::NoNewBlocks(_metadata, block, height)) => {
                    debug!("Chain up-to-date at block {block}, height {height}");
                    self.sleep_and_process_queries(std::time::Duration::from_secs(30))
                        .await;
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
                    error!("Error updating blocks: {e}");
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

    async fn process_query(&mut self, req: DbQueryRequest) {
        let sql = req.sql;
        let res = self
            .conn
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
        req.responder.send(res).ok();
    }

    async fn sleep_and_process_queries(&mut self, duration: std::time::Duration) {
        let sleep = rt::sleep(duration).fuse();
        let mut sleep = core::pin::pin!(sleep);
        loop {
            futures::select! {
                _ = sleep => break,
                req = self.query_rx.next() => {
                    let Some(req) = req else { break; };
                    self.process_query(req).await;
                }
            }
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

impl<S: Scryable> LayerBase for L0Client<S> {
    const ACCEPT_LAYERS: &'static [&'static str] = &[];
    const LAYER: &'static str = "l0";
    type Stats = L0Stats;
    fn stats_handle(&self) -> watch::Receiver<Option<Self::Stats>> {
        self.stats_rx.clone()
    }
}
