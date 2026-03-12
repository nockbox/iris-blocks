pub mod schema;

use super::{l0::schema::*, layer::*, shared_schema::*};
use crate::chain_activations::ChainActivations;
use core::future::Future;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use iris_nockchain_types::{Nicks, Page, RawTx};
use iris_ztd::{cue, NounDecode};
use log::*;
use schema::*;
use std::collections::BTreeSet;
use std::sync::Arc;
use tracing::Instrument;

pub struct L1Client {
    activations: ChainActivations,
    deps: Vec<Arc<dyn LayerDependency>>,
}

impl L1Client {
    pub fn new(activations: ChainActivations, deps: Vec<Arc<dyn LayerDependency>>) -> Self {
        Self::verify_dependencies(&deps).unwrap();
        Self { activations, deps }
    }
}

impl LayerBase for L1Client {
    const ACCEPT_LAYERS: &'static [&'static str] = &["l0"];
    const LAYER: &'static str = "l1";
}

impl LayerImpl for L1Client {
    fn expire_blocks_impl<'a>(
        &'a self,
        conn: &'a mut crate::db::AsyncDbConnection,
        mut metadata: FixedLayerMetadata,
    ) -> impl Future<Output = Result<(), LayerErrorSource>> + Send + 'a {
        async move {
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
                dep.expire_blocks(conn, metadata).await;
            }

            metadata.layer = Self::LAYER;

            trace!("Dropping notes");

            diesel::delete(notes::table)
                .filter(notes::created_height.ge(metadata.next_block_height))
                .execute(conn)
                .await?;

            Self::update_layer_metadata(&metadata).execute(conn).await?;

            Ok(())
        }
    }

    fn update_blocks_impl(
        &self,
        pool: crate::db::DbPool,
        metadata: FixedLayerMetadata,
    ) -> impl Future<Output = Result<(), LayerErrorSource>> + Send + '_ {
        let update_span = tracing::info_span!("l1_update_blocks");

        async move {
            if metadata.next_block_height == 0 {
                return Ok(());
            }

            let mut conn = pool.get().await?;
            let cur_metadata = Self::layer_metadata(&mut conn).await?;
            let start_block_height = cur_metadata
                .as_ref()
                .map(|m| m.next_block_height)
                .unwrap_or_default() as u32;
            let end_block_height = metadata.next_block_height as u32 - 1;

            if start_block_height > end_block_height {
                return Ok(());
            }

            self.expire_blocks_impl(&mut conn, FixedLayerMetadata {
                layer: Self::LAYER,
                next_block_height: start_block_height as _,
            }).await?;

            trace!("Syncing note balances from {start_block_height} to {end_block_height}");
            let constants = self.activations.constants();

            let step = 100u32;

            for block_height in (start_block_height..=end_block_height).step_by(step as usize) {
                let block_range_span = tracing::info_span!("l1_update_block_range", block_height, end_block_height);
                trace!("Syncing block {block_height}");
                let last_block_height = core::cmp::min(block_height + step - 1, end_block_height);

                let block_span = tracing::info_span!("l1_db_get_block_range", block_height, end_block_height);
                let blocks = blocks::table
                    .filter(
                        blocks::height
                            .ge(block_height as i32)
                            .and(blocks::height.le(last_block_height as i32)),
                    )
                    .order_by(blocks::height)
                    .load::<Block>(&mut conn)
                    .instrument(block_span)
                    .await?;

                async {
                    for block in blocks {
                        let block_height = block.height as u32;
                        let get_txs_span = tracing::info_span!("l1_db_get_txs", block.height);
                        let mut new_notes = vec![];
                        let mut spent_notes = vec![];

                        let page =
                            Page::from_noun(&cue(&block.jam).ok_or(LayerErrorSource::NounCue(block_height, *block.id))?)
                                .ok_or(LayerErrorSource::NounDecode(block_height, *block.id))?;

                        for note in page.coinbase(constants) {
                            new_notes.push(Note::coinbase(&block, note));
                        }

                        let txs: Vec<Transaction> = transactions::table
                            .filter(transactions::height.eq(block_height as i32))
                            .load::<Transaction>(&mut conn)
                            .instrument(get_txs_span)
                            .await?;

                        for tx in txs {
                            let raw =
                                RawTx::from_noun(&cue(&tx.jam).ok_or(LayerErrorSource::NounCue(block_height, *tx.id))?)
                                    .ok_or(LayerErrorSource::NounDecode(block_height, *tx.id))?;
                            let outputs = raw.outputs(block_height, self.activations.tx_engine(block_height));
                            let fees = Nicks::from(tx.fee as u64);

                            let names = raw.input_names();

                            let mut names_cond = names
                                .iter()
                                .map(|n| {
                                    notes::first
                                        .eq(NoteName(n.first))
                                        .and(notes::last.eq(NoteName(n.last)))
                                })
                                .collect::<Vec<_>>();

                            let mut query = notes::table.into_boxed();
                            for cond in names_cond {
                                query = query.or_filter(cond);
                            }

                            let input_notes = query.load::<Note>(&mut conn).await?;

                            if input_notes.len() != names.len() {
                                let got_names = input_notes.iter().map(|n| (*n.first, *n.last)).collect::<BTreeSet<_>>();
                                let mut missing_names = vec![];
                                for name in &names {
                                    if !got_names.contains(&(name.first, name.last)) {
                                        missing_names.push(name);
                                    }
                                }
                                return Err(LayerErrorSource::OtherError(format!(
                                    "tx {} missing inputs: {:?}",
                                    *tx.id, missing_names,
                                )));
                            }

                            let input_assets = input_notes
                                .iter()
                                .map(|v| Nicks::from(v.assets as u64))
                                .sum::<Nicks>();
                            let output_assets = outputs.iter().map(|v| v.assets()).sum::<Nicks>();

                            if input_assets != output_assets + fees {
                                return Err(LayerErrorSource::OtherError(format!(
                                    "tx {} not balanced. inp={}; out={}; fee={}",
                                    *tx.id, input_assets, output_assets, fees
                                )));
                            }

                            for note in input_notes {
                                if note.spent_bid.is_some() || note.spent_txid.is_some() {
                                    return Err(LayerErrorSource::OtherError(format!(
                                        "tx {} invalid. note [{} {}] already spent on block {:?} tx {:?}",
                                        *tx.id, *note.first, *note.last, note.spent_bid.map(|v| v.to_string()), note.spent_txid.map(|v| v.to_string())
                                    )));
                                }
                                spent_notes.push(SpendNote {
                                    first: note.first,
                                    last: note.last,
                                    spent_bid: block.id,
                                    spent_height: block.height,
                                    spent_txid: tx.id,
                                });
                            }

                            for note in outputs {
                                new_notes.push(Note::tx_output(&block, &tx, note));
                            }
                        }

                        let metadata = FixedLayerMetadata {
                            layer: Self::LAYER,
                            next_block_height: block_height as i32 + 1,
                        };

                        conn.spawn_blocking(move |conn| {
                            use diesel::query_dsl::methods::ExecuteDsl;
                            conn.transaction(move |conn| {
                                let q1 = diesel::insert_into(notes::table).values(&new_notes);
                                ExecuteDsl::execute(q1, conn)?;

                                // TODO: batch update
                                for note in spent_notes {
                                    let q2 = diesel::update(&note).set(&note);
                                    let updated_notes = ExecuteDsl::execute(q2, conn)?;
                                    if updated_notes != 1 {
                                        return Err(diesel::result::Error::DatabaseError(
                                            diesel::result::DatabaseErrorKind::UniqueViolation,
                                            Box::new(format!("expected to only update note [{} {}] but updated {} rows", *note.first, *note.last, updated_notes)),
                                        ));
                                    }
                                }

                                let q3 = Self::update_layer_metadata(&metadata);
                                ExecuteDsl::execute(q3, conn)?;
                                Ok(())
                            })
                        })
                        .instrument(tracing::info_span!("l1_commit_block", block_height))
                        .await?;
                    }

                    Ok(())
                }
                .instrument(block_range_span)
                .await?;
            }

            Ok(())
        }
        .instrument(update_span)
    }
}
