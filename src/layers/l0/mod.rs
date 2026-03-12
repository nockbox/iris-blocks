pub mod schema;

use super::{layer::*, shared_schema::*};
use crate::chain_activations::ChainActivations;
use crate::db::{AsyncDbConnection, DbPool};
use crate::StringDigest;
use clap::Parser;
use diesel::prelude::*;
use diesel_async::{AsyncConnection, RunQueryDsl};
use iris_grpc_proto::pb::private::v1::{
    nock_app_service_client::NockAppServiceClient, nock_app_service_server::NockAppService,
    peek_response::Result as PeekResult, *,
};
use iris_nockchain_types::{BlockHeight, Page, Tx};
use iris_ztd::{cue, jam, Digest, NounDecode, NounEncode, ZMap};
use log::*;
use schema::*;
use std::collections::VecDeque;
use std::sync::Arc;
use thiserror::Error;
use tonic::transport::Channel;

mod block_range_manager;
use block_range_manager::{BlockRangeManager, ScryBlocksResult};

#[derive(Debug, Clone, Parser)]
pub struct L0Config {
    #[command(flatten)]
    pub block_range_config: block_range_manager::BlockRangeConfig,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub store_pow: bool,
}

pub struct L0Client {
    pool: DbPool,
    client: Option<NockAppServiceClient<Channel>>,
    manager: Option<BlockRangeManager>,
    dependencies: Vec<Arc<dyn LayerDependency>>,
    activations: ChainActivations,
    config: L0Config,
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
    #[error("Invalid blocks range response")]
    InvalidBlocksRangeResponse,
    #[error("Unable to parse blocks range response")]
    UnableToParseBlocksRangeResponse,
    #[error("Block missing pow")]
    BlockMissingPow(Digest),
    #[error("TX {0} invalid on block {1} ({2})")]
    TransactionInvalid(Digest, BlockHeight, Digest),
    #[error("Noun cue error")]
    NounCueError,
    #[error("Noun decode error")]
    NounDecodeError,
    #[error("Chain reoverted")]
    Reverted,
    #[error("Unable to pull elders at block {1} on height {0}")]
    UnableToPullElders(BlockHeight, Digest),
    #[error(transparent)]
    LayerError(#[from] LayerError),
    #[error("gRPC connection needed, but not provided")]
    GrpcNeeded,
}

async fn remote_scry<T: NounDecode>(
    client: &mut NockAppServiceClient<Channel>,
    path: impl NounEncode,
) -> Result<T, L0Error> {
    let peek_req = PeekRequest {
        pid: 0,
        path: jam(path.to_noun()),
    };

    let peek_res = client.peek(peek_req).await?.into_inner();
    let Some(PeekResult::Data(peek_blob)) = peek_res.result else {
        return Err(L0Error::InvalidBlocksRangeResponse);
    };
    let peek_noun = cue(&peek_blob).ok_or(L0Error::NounCueError)?;
    NounDecode::from_noun(&peek_noun).ok_or(L0Error::NounDecodeError)
}

impl L0Client {
    pub fn new(
        pool: DbPool,
        channel: Option<Channel>,
        config: L0Config,
        activations: ChainActivations,
        dependencies: Vec<Arc<dyn LayerDependency>>,
    ) -> Self {
        let client = channel.map(NockAppServiceClient::new);
        Self::verify_dependencies(&dependencies).unwrap();
        Self {
            pool,
            client: client.clone(),
            manager: client.map(|client| BlockRangeManager::new(client, config.block_range_config)),
            activations,
            dependencies,
            config,
        }
    }

    async fn revert_to(
        &mut self,
        new_next_block_height: u32,
        conn: &mut AsyncDbConnection,
    ) -> Result<FixedLayerMetadata, L0Error> {
        debug!("Reverting l0 to next_block={new_next_block_height}");

        let metadata = FixedLayerMetadata {
            layer: "l0",
            next_block_height: new_next_block_height as _,
        };

        for dep in self.dependencies.iter() {
            dep.expire_blocks(conn, metadata).await;
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

    async fn chain_reorged(
        &mut self,
        mut mismatch_block_height: BlockHeight,
        mut mismatch_block: Digest,
        conn: &mut AsyncDbConnection,
    ) -> Result<FixedLayerMetadata, L0Error> {
        // Recovery - walk up the elders until we find a common ancestor
        debug!("Chain reorg detected at height {mismatch_block_height}. Finding common ancestor");

        let client = self.client.as_mut().ok_or(L0Error::GrpcNeeded)?;

        while mismatch_block_height > 0 {
            let Some(Some((last_bid, blocks))): Option<Option<(BlockHeight, Vec<Digest>)>> =
                remote_scry(client, ("elders", StringDigest(mismatch_block))).await?
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
            }
        }

        self.revert_to(mismatch_block_height, conn).await
    }

    #[tracing::instrument(skip_all)]
    async fn update_blocks_impl(&mut self) -> Result<FixedLayerMetadata, L0Error> {
        let mut conn = self.pool.get_owned().await.unwrap();

        trace!("Updating blocks");

        let Some(mdata) = Self::layer_metadata(&mut conn).await? else {
            debug!("Metadata not set. Triggering reset");
            return self.revert_to(0, &mut conn).await;
        };

        let cur_tail: Option<JamlessBlock> = blocks::table
            .select(JamlessBlock::as_select())
            .order_by(blocks::height.desc())
            .limit(1)
            .load::<JamlessBlock>(&mut conn)
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
                        core::cmp::min(last.height + 1, mdata.next_block_height) as _,
                        &mut conn,
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
                ));
            } else {
                return Err(L0Error::GrpcNeeded);
            }
        };

        let Some(Some(mut new_blocks)) = manager.scry_blocks(next_height_start).await? else {
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
                    return self
                        .chain_reorged(new_blocks[0].0, new_blocks[0].1, &mut conn)
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
                })
            }
        }

        let metadata = FixedLayerMetadata {
            layer: self.layer(),
            next_block_height: create_blocks.last().unwrap().height as i32 + 1,
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
            dep.update_blocks(self.pool.clone(), metadata).await?;
        }

        res
    }

    #[tracing::instrument(skip_all)]
    pub async fn run(mut self) {
        loop {
            match self.update_blocks().await {
                // We updated successfully, continue without sleeping
                Ok(_) => (),
                Err(L0Error::Reverted) => {
                    debug!("Chain reverted. Restarting...");
                    continue;
                }
                Err(L0Error::NoNewBlocks(_, block, height)) => {
                    debug!("Chain up-to-date at block {block}, height {height}");
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                }
                Err(e)
                    if matches!(
                        e,
                        L0Error::InvalidBlocksRangeResponse
                            | L0Error::UnableToParseBlocksRangeResponse
                            | L0Error::NounCueError
                            | L0Error::NounDecodeError
                    ) =>
                {
                    debug!("Failed to get new blocks: {e}. Sleeping for 30 seconds.");
                    // We'll try again in a bit
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                }
                Err(e) => {
                    error!("Error updating blocks: {e}");
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                }
            }
        }
    }
}

impl LayerBase for L0Client {
    const ACCEPT_LAYERS: &'static [&'static str] = &[];
    const LAYER: &'static str = "l0";
}
