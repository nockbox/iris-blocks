pub mod schema;

use super::{l0::schema::blocks, l1::schema::notes, layer::*, shared_schema::*};
use crate::chain_activations::ChainActivations;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use log::*;
use schema::*;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::Instrument;

pub struct L3Client {
    #[allow(dead_code)]
    activations: ChainActivations,
    deps: Vec<Arc<dyn LayerDependency>>,
    _stats_tx: watch::Sender<Option<<Self as LayerBase>::Stats>>,
    stats_rx: watch::Receiver<Option<<Self as LayerBase>::Stats>>,
}

impl L3Client {
    pub fn new(activations: ChainActivations, deps: Vec<Arc<dyn LayerDependency>>) -> Self {
        let (stats_tx, stats_rx) = Self::verify_dependencies(&deps).unwrap();
        Self {
            activations,
            deps,
            _stats_tx: stats_tx,
            stats_rx,
        }
    }
}

impl LayerBase for L3Client {
    const ACCEPT_LAYERS: &'static [&'static str] = &["l2"];
    const LAYER: &'static str = "l3";
    type Stats = ();
    fn stats_handle(&self) -> watch::Receiver<Option<Self::Stats>> {
        self.stats_rx.clone()
    }
}

impl LayerImpl for L3Client {
    #[tracing::instrument(skip_all)]
    async fn expire_blocks_impl<'a>(
        &'a self,
        conn: &'a mut crate::db::AsyncDbConnection,
        mut metadata: FixedLayerMetadata,
    ) -> Result<(), LayerErrorSource> {
        let cur_metadata = Self::layer_metadata(conn)
            .await?
            .unwrap_or(FixedLayerMetadata {
                layer: Self::LAYER,
                next_block_height: 0,
            });

        if cur_metadata.next_block_height < metadata.next_block_height {
            metadata = cur_metadata;
        }

        for dep in &self.deps {
            dep.expire_blocks(conn, metadata).await?;
        }

        metadata.layer = Self::LAYER;

        trace!("Dropping debits");
        diesel::delete(debits::table)
            .filter(debits::height.ge(metadata.next_block_height))
            .execute(conn)
            .await?;
        trace!("Dropping credits");
        diesel::delete(credits::table)
            .filter(credits::height.ge(metadata.next_block_height))
            .execute(conn)
            .await?;

        Self::update_layer_metadata(&metadata).execute(conn).await?;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn update_blocks_impl<'a>(
        &'a self,
        conn: &'a mut crate::db::AsyncDbConnection,
        metadata: FixedLayerMetadata,
    ) -> Result<(), LayerErrorSource> {
        if metadata.next_block_height == 0 {
            for dep in &self.deps {
                dep.update_blocks(conn, metadata).await?;
            }
            return Ok(());
        }

        let cur_metadata = Self::layer_metadata(conn).await?;
        let start_block_height = cur_metadata
            .as_ref()
            .map(|m| m.next_block_height)
            .unwrap_or_default() as u32;
        let end_block_height = metadata.next_block_height as u32 - 1;

        if start_block_height > end_block_height {
            let dep_metadata = cur_metadata.unwrap_or(metadata);
            for dep in &self.deps {
                dep.update_blocks(conn, dep_metadata).await?;
            }
            return Ok(());
        }

        self.expire_blocks_impl(
            conn,
            FixedLayerMetadata {
                layer: Self::LAYER,
                next_block_height: start_block_height as i32,
            },
        )
        .await?;

        trace!("Syncing credits/debits from {start_block_height} to {end_block_height}");
        let step = 100u32;
        let mut cur_metadata = FixedLayerMetadata {
            layer: Self::LAYER,
            next_block_height: start_block_height as i32,
        };

        for block_height in (start_block_height..=end_block_height).step_by(step as usize) {
            let block_range_span =
                tracing::info_span!("l3_update_block_range", block_height, end_block_height);
            let last_block_height = core::cmp::min(block_height + step - 1, end_block_height);

            async {
                for height in block_height..=last_block_height {
                    let h = height as i32;

                    // Get block_id
                    let block_id: DbDigest = blocks::table
                        .filter(blocks::height.eq(h))
                        .select(blocks::id)
                        .first::<DbDigest>(conn)
                        .await
                        .unwrap_or(DbDigest(iris_ztd::Digest::from_bytes(&[0; 32])));

                    let mut block_credits = vec![];
                    let mut block_debits = vec![];

                    // Credits from notes created at this height.
                    // Group by (txid, first) since multiple notes can share
                    // the same `first`. One credit per unique (txid, first).
                    let created_notes = notes::table
                        .filter(notes::created_height.eq(h))
                        .select((notes::first, notes::created_txid, notes::assets))
                        .load::<(DbDigest, Option<DbDigest>, i64)>(conn)
                        .await?;

                    let mut credit_map: std::collections::BTreeMap<
                        (Option<DbDigest>, DbDigest),
                        i64,
                    > = std::collections::BTreeMap::new();
                    for (first, created_txid, assets) in created_notes {
                        *credit_map.entry((created_txid, first)).or_insert(0) += assets;
                    }
                    for ((created_txid, first), amount) in credit_map {
                        block_credits.push(Credit {
                            txid: created_txid,
                            first,
                            height: h,
                            block_id,
                            amount,
                        });
                    }

                    // Debits from notes spent at this height.
                    // Group by (txid, first) since multiple notes can share
                    // the same `first` (lock-derived name). One debit per
                    // unique (txid, first) with summed amounts.
                    let spent_notes = notes::table
                        .filter(notes::spent_height.eq(h))
                        .select((notes::first, notes::spent_txid, notes::assets))
                        .load::<(DbDigest, Option<DbDigest>, i64)>(conn)
                        .await?;

                    // Accumulate (txid, first) → total_amount
                    let mut debit_map: std::collections::BTreeMap<
                        (Option<DbDigest>, DbDigest),
                        i64,
                    > = std::collections::BTreeMap::new();
                    for (first, spent_txid, assets) in spent_notes {
                        *debit_map.entry((spent_txid, first)).or_insert(0) += assets;
                    }

                    // Fee per (txid, first) from L2 tx_spends
                    for ((spent_txid, first), amount) in debit_map {
                        use super::l2::schema::tx_spends;
                        let fee = if let Some(ref txid) = spent_txid {
                            tx_spends::table
                                .filter(tx_spends::txid.eq(*txid).and(tx_spends::first.eq(first)))
                                .select(tx_spends::fee)
                                .first::<i64>(conn)
                                .await
                                .unwrap_or(0)
                        } else {
                            0
                        };

                        block_debits.push(Debit {
                            txid: spent_txid,
                            first: Some(first),
                            height: h,
                            block_id,
                            amount,
                            fee,
                        });
                    }

                    cur_metadata = FixedLayerMetadata {
                        layer: Self::LAYER,
                        next_block_height: h + 1,
                    };
                    let next_metadata = cur_metadata;

                    conn.spawn_blocking(move |conn| {
                        use diesel::query_dsl::methods::ExecuteDsl;
                        conn.transaction(move |conn| {
                            if !block_credits.is_empty() {
                                ExecuteDsl::execute(
                                    diesel::insert_into(credits::table).values(&block_credits),
                                    conn,
                                )?;
                            }
                            if !block_debits.is_empty() {
                                //for block_debit in block_debits {
                                //log::trace!("New debit: {block_debit:?}");
                                ExecuteDsl::execute(
                                    diesel::insert_into(debits::table).values(&block_debits),
                                    conn,
                                )?;
                                //}
                            }
                            ExecuteDsl::execute(Self::update_layer_metadata(&next_metadata), conn)?;
                            Ok(())
                        })
                    })
                    .instrument(tracing::info_span!("l3_commit_block", height))
                    .await?;

                    crate::rt::yield_now().await;
                }

                Ok::<(), LayerErrorSource>(())
            }
            .instrument(block_range_span)
            .await?;
        }

        for dep in &self.deps {
            dep.update_blocks(conn, cur_metadata).await?;
        }

        Ok(())
    }
}
