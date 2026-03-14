pub mod schema;

use super::{l0::schema::*, layer::*, shared_schema::*};
use crate::chain_activations::ChainActivations;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use iris_nockchain_types::{Nicks, Page, Tx};
use iris_ztd::{cue, NounDecode};
use log::*;
use schema::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::Instrument;

pub struct L1Client {
    activations: ChainActivations,
    deps: Vec<Arc<dyn LayerDependency>>,
    stats_tx: watch::Sender<Option<<Self as LayerBase>::Stats>>,
    stats_rx: watch::Receiver<Option<<Self as LayerBase>::Stats>>,
}

impl L1Client {
    pub fn new(activations: ChainActivations, deps: Vec<Arc<dyn LayerDependency>>) -> Self {
        let (stats_tx, stats_rx) = Self::verify_dependencies(&deps).unwrap();
        Self {
            activations,
            deps,
            stats_tx,
            stats_rx,
        }
    }
}

impl LayerBase for L1Client {
    const ACCEPT_LAYERS: &'static [&'static str] = &["l0"];
    const LAYER: &'static str = "l1";
    type Stats = L1Stats;
    fn stats_handle(&self) -> watch::Receiver<Option<Self::Stats>> {
        self.stats_rx.clone()
    }
}

impl LayerImpl for L1Client {
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

        trace!("Dropping notes");

        diesel::delete(notes::table)
            .filter(notes::created_height.ge(metadata.next_block_height))
            .execute(conn)
            .await?;

        diesel::update(notes::table)
            .filter(notes::spent_height.ge(metadata.next_block_height))
            .set((
                notes::spent_height.eq(None::<i32>),
                notes::spent_bid.eq(None::<DbDigest>),
                notes::spent_txid.eq(None::<DbDigest>),
            ))
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
    ) -> Result<bool, LayerErrorSource> {
        if metadata.next_block_height == 0 {
            let mut has_more = false;
            for dep in &self.deps {
                has_more |= dep.update_blocks(conn, metadata).await?;
            }
            return Ok(has_more);
        }

        let cur_metadata = Self::layer_metadata(conn).await?;
        let start_block_height = cur_metadata
            .as_ref()
            .map(|m| m.next_block_height)
            .unwrap_or_default() as u32;
        let end_block_height = metadata.next_block_height as u32 - 1;

        if start_block_height > end_block_height {
            let dep_metadata = cur_metadata.unwrap_or(metadata);
            let mut has_more = false;
            for dep in &self.deps {
                has_more |= dep.update_blocks(conn, dep_metadata).await?;
            }
            return Ok(has_more);
        }

        self.expire_blocks_impl(
            conn,
            FixedLayerMetadata {
                layer: Self::LAYER,
                next_block_height: start_block_height as _,
            },
        )
        .await?;

        let step = 100u32;
        let last_block_height = core::cmp::min(start_block_height + step - 1, end_block_height);

        trace!("Syncing note balances from {start_block_height} to {last_block_height}");
        let constants = self.activations.constants();

        let mut cur_metadata = FixedLayerMetadata {
            layer: Self::LAYER,
            next_block_height: start_block_height as i32,
        };

        let block_range_span = tracing::info_span!(
            "l1_update_block_range",
            start_block_height,
            last_block_height
        );

        let block_span = tracing::info_span!(
            "l1_db_get_block_range",
            start_block_height,
            last_block_height
        );
        let blocks = blocks::table
            .filter(
                blocks::height
                    .ge(start_block_height as i32)
                    .and(blocks::height.le(last_block_height as i32)),
            )
            .order_by(blocks::height)
            .load::<Block>(conn)
            .instrument(block_span)
            .await?;

        async {
            for block in blocks {
                let block_height = block.height as u32;
                let get_txs_span = tracing::info_span!("l1_db_get_txs", block.height);
                let mut new_notes = vec![];
                let mut spent_notes = vec![];

                let page = Page::from_noun(
                    &cue(&block.jam).ok_or(LayerErrorSource::NounCue(block_height, *block.id))?,
                )
                .ok_or(LayerErrorSource::NounDecode(block_height, *block.id))?;

                for note in page.coinbase(constants) {
                    new_notes.push(Note::coinbase(&block, note));
                }

                let txs: Vec<Transaction> = transactions::table
                    .filter(transactions::height.eq(block_height as i32))
                    .load::<Transaction>(conn)
                    .instrument(get_txs_span)
                    .await?;

                for tx in txs {
                    let vtx = Tx::from_noun(
                        &cue(&tx.jam).ok_or(LayerErrorSource::NounCue(block_height, *tx.id))?,
                    )
                    .ok_or(LayerErrorSource::NounDecode(block_height, *tx.id))?;
                    let outputs = vtx.outputs().notes();
                    let fees = Nicks::from(tx.fee as u64);

                    let names = vtx.input_names();

                    let names_cond = names
                        .iter()
                        .map(|n| {
                            notes::first
                                .eq(DbDigest(n.first))
                                .and(notes::last.eq(DbDigest(n.last)))
                        })
                        .collect::<Vec<_>>();

                    let mut query = notes::table.into_boxed();
                    for cond in names_cond {
                        query = query.or_filter(cond);
                    }

                    let input_notes = query.load::<Note>(conn).await?;

                    if input_notes.len() != names.len() {
                        let got_names = input_notes
                            .iter()
                            .map(|n| (*n.first, *n.last))
                            .collect::<BTreeSet<_>>();
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
                                *tx.id,
                                *note.first,
                                *note.last,
                                note.spent_bid.map(|v| v.to_string()),
                                note.spent_txid.map(|v| v.to_string())
                            )));
                        }
                        spent_notes.push((
                            NoteName {
                                first: note.first,
                                last: note.last,
                            },
                            SpendNote {
                                spent_bid: block.id,
                                spent_height: block.height,
                                spent_txid: tx.id,
                            },
                        ));
                    }

                    for note in outputs {
                        new_notes.push(Note::tx_output(&block, &tx, note));
                    }

                    crate::rt::yield_now().await;
                }

                cur_metadata = FixedLayerMetadata {
                    layer: Self::LAYER,
                    next_block_height: block_height as i32 + 1,
                };
                let metadata = cur_metadata;

                conn.spawn_blocking(move |conn| {
                    use diesel::query_dsl::methods::ExecuteDsl;
                    conn.transaction(move |conn| {
                        let insert_span = tracing::info_span!("l1_db_insert_notes");
                        let insert_guard = insert_span.enter();
                        let q1 = diesel::insert_into(notes::table).values(&new_notes);
                        ExecuteDsl::execute(q1, conn)?;
                        core::mem::drop(insert_guard);

                        // TODO: batch update
                        for (name, note) in spent_notes {
                            let update_span = tracing::info_span!("l1_db_update_note");
                            let _update_guard = update_span.enter();
                            let q2 = diesel::update(&name).set(&note);
                            let updated_notes = ExecuteDsl::execute(q2, conn)?;
                            if updated_notes != 1 {
                                return Err(diesel::result::Error::DatabaseError(
                                    diesel::result::DatabaseErrorKind::UniqueViolation,
                                    Box::new(format!(
                                        "expected to only update note [{} {}] but updated {} rows",
                                        *name.first, *name.last, updated_notes
                                    )),
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

            let cur_note_count = notes::table.count().get_result::<i64>(conn).await?;
            let cur_spent_note_count = notes::table
                .filter(notes::spent_txid.is_not_null())
                .count()
                .get_result::<i64>(conn)
                .await?;

            let new_stats = L1Stats {
                total_notes: cur_note_count as u64,
                spent_notes: cur_spent_note_count as u64,
            };

            self.stats_tx.send(Some(new_stats)).unwrap();

            Ok(())
        }
        .instrument(block_range_span)
        .await?;

        let self_has_more = last_block_height < end_block_height;
        let mut has_more = self_has_more;
        for dep in &self.deps {
            has_more |= dep.update_blocks(conn, cur_metadata).await?;
        }

        Ok(has_more)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(from_wasm_abi, into_wasm_abi))]
pub struct L1Stats {
    pub total_notes: u64,
    pub spent_notes: u64,
}
