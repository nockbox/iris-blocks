pub mod schema;

use super::{
    l0::schema::blocks,
    l1::schema::notes,
    l2::schema::{name_to_lock, spend_conditions},
    l3::schema::credits,
    layer::*,
    shared_schema::*,
};
use crate::chain_activations::ChainActivations;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use iris_nockchain_types::v1::SpendCondition;
use iris_nockchain_types::Page;
use iris_ztd::{cue, Hashable, NounDecode};
use log::*;
use schema::*;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::Instrument;

pub struct L4Client {
    activations: ChainActivations,
    deps: Vec<Arc<dyn LayerDependency>>,
    _stats_tx: watch::Sender<Option<<Self as LayerBase>::Stats>>,
    stats_rx: watch::Receiver<Option<<Self as LayerBase>::Stats>>,
}

impl L4Client {
    pub fn new(activations: ChainActivations, deps: Vec<Arc<dyn LayerDependency>>) -> Self {
        let (stats_tx, stats_rx) = Self::verify_dependencies(&deps).unwrap();
        Self {
            activations,
            deps,
            _stats_tx: stats_tx,
            stats_rx,
        }
    }

    /// Determine recipient_type and recipient from a V1 spend condition.
    fn resolve_recipient(sc: &SpendCondition) -> (String, String) {
        let pkhs: Vec<_> = sc.pkh().collect();
        if pkhs.len() == 1 && pkhs[0].hashes.len() == 1 {
            if let Some(pkh) = pkhs[0].hashes.iter().next() {
                return ("pkh".to_string(), pkh.to_string());
            } else {
                ("musig".to_string(), sc.hash().to_string())
            }
        } else if pkhs.len() > 1 {
            ("musig".to_string(), sc.hash().to_string())
        } else {
            ("lock".to_string(), sc.hash().to_string())
        }
    }

    /// Resolve a V0 note's recipient by decoding its JAM and extracting the sig pubkeys.
    fn resolve_v0_recipient(note_jam: &[u8]) -> Result<(String, String), LayerErrorSource> {
        let noun = cue(note_jam)
            .ok_or_else(|| LayerErrorSource::OtherError("failed to cue v0 note".to_string()))?;
        let note = iris_nockchain_types::v0::NoteV0::from_noun(&noun)
            .ok_or_else(|| LayerErrorSource::OtherError("failed to decode v0 note".to_string()))?;
        if note.sig.pubkeys.len() == 1 {
            let pk = note
                .sig
                .pubkeys
                .iter()
                .next()
                .expect("v0 note has 1 pubkey but iter empty");
            Ok(("pk".to_string(), DbPublicKey::from(*pk).to_string()))
        } else {
            // Multi-sig V0 note
            Ok(("musig".to_string(), note.sig.hash().to_string()))
        }
    }

    /// Resolve a V1 credit recipient via name_to_lock → spend_conditions.
    async fn resolve_v1_recipient(
        conn: &mut crate::db::AsyncDbConnection,
        first: DbDigest,
    ) -> Result<(String, String), LayerErrorSource> {
        let root: DbDigest = name_to_lock::table
            .filter(name_to_lock::first.eq(first))
            .select(name_to_lock::root)
            .first::<DbDigest>(conn)
            .await
            .map_err(|_| {
                LayerErrorSource::OtherError(format!(
                    "missing name_to_lock entry for first={first}"
                ))
            })?;

        let sc_row = spend_conditions::table
            .filter(spend_conditions::hash.eq(root))
            .first::<super::l2::schema::SpendConditionRow>(conn)
            .await
            .optional()?;

        let Some(sc_row) = sc_row else {
            return Ok(("lock".to_string(), root.to_string()));
        };

        let sc = SpendCondition::from_noun(&cue(&sc_row.jam).ok_or_else(|| {
            LayerErrorSource::OtherError(format!("failed to cue spend condition for root={root}"))
        })?)
        .ok_or_else(|| {
            LayerErrorSource::OtherError(format!(
                "failed to decode spend condition for root={root}"
            ))
        })?;

        Ok(Self::resolve_recipient(&sc))
    }

    /// Build a map from coinbase note `first` → (recipient_type, recipient).
    /// For V0: sig contains public keys → recipient is the pk itself.
    /// For V1: the CoinbaseSplit key IS the PKH directly.
    fn coinbase_recipients(
        page: &Page,
        constants: iris_nockchain_types::BlockchainConstants,
    ) -> BTreeMap<DbDigest, (String, String)> {
        let notes = page.coinbase(constants);
        let mut map = BTreeMap::new();
        match page {
            Page::V0(p) => {
                for (note, (sig, _)) in notes.iter().zip(p.coinbase.0.iter()) {
                    let first = DbDigest(note.name().first);
                    let pk = sig
                        .pubkeys
                        .iter()
                        .next()
                        .expect("v0 coinbase sig must have at least one pubkey");
                    map.insert(
                        first,
                        ("pk".to_string(), DbPublicKey::from(*pk).to_string()),
                    );
                }
            }
            Page::V1(p) => {
                for (note, (pkh, _)) in notes.iter().zip(p.coinbase.0.iter()) {
                    let first = DbDigest(note.name().first);
                    map.insert(first, ("pkh".to_string(), pkh.to_string()));
                }
            }
        }
        map
    }
}

impl LayerBase for L4Client {
    const ACCEPT_LAYERS: &'static [&'static str] = &["l3"];
    const LAYER: &'static str = "l4";
    type Stats = ();
    fn stats_handle(&self) -> watch::Receiver<Option<Self::Stats>> {
        self.stats_rx.clone()
    }
}

impl LayerImpl for L4Client {
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

        let min_height: Option<i32> = credit_info::table
            .filter(credit_info::updated_height.ge(metadata.next_block_height))
            .select(diesel::dsl::min(credit_info::height))
            .first::<Option<i32>>(conn)
            .await?;

        if let Some(min_h) = min_height {
            diesel::delete(credit_info::table)
                .filter(credit_info::height.ge(min_h))
                .execute(conn)
                .await?;
            metadata.next_block_height = min_h;
        }

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

        trace!("Syncing credit_info from {start_block_height} to {end_block_height}");
        let constants = self.activations.constants();
        let step = 100u32;
        let mut cur_metadata = FixedLayerMetadata {
            layer: Self::LAYER,
            next_block_height: start_block_height as i32,
        };

        for block_height in (start_block_height..=end_block_height).step_by(step as usize) {
            let block_range_span =
                tracing::info_span!("l4_update_block_range", block_height, end_block_height);
            let last_block_height = core::cmp::min(block_height + step - 1, end_block_height);

            async {
                for height in block_height..=last_block_height {
                    let h = height as i32;

                    let mut block_info = vec![];

                    // ── Non-coinbase credits (txid IS NOT NULL) ──
                    let tx_credits = credits::table
                        .filter(credits::height.eq(h))
                        .filter(credits::txid.is_not_null())
                        .select((credits::txid, credits::first))
                        .load::<(Option<DbDigest>, DbDigest)>(conn)
                        .await?;

                    for (txid_opt, first) in tx_credits {
                        let txid = txid_opt.expect("txid IS NOT NULL but got None");

                        // Determine note version to branch V0 vs V1
                        let note_version: i32 = notes::table
                            .filter(notes::first.eq(first))
                            .select(notes::version)
                            .first::<i32>(conn)
                            .await
                            .map_err(|_| {
                                LayerErrorSource::OtherError(format!(
                                    "missing note for first={first} txid={txid}"
                                ))
                            })?;

                        let (recipient_type, recipient) = if note_version == 0 {
                            // V0: decode note JAM to extract sig pubkeys
                            let note_jam: Vec<u8> = notes::table
                                .filter(notes::first.eq(first))
                                .select(notes::jam)
                                .first::<Vec<u8>>(conn)
                                .await?;
                            Self::resolve_v0_recipient(&note_jam)?
                        } else {
                            // V1: name_to_lock → spend_conditions
                            Self::resolve_v1_recipient(conn, first).await?
                        };

                        block_info.push(CreditInfo {
                            txid: Some(txid),
                            first,
                            height: h,
                            updated_height: h,
                            recipient_type,
                            recipient,
                        });
                    }

                    // ── Coinbase credits (txid IS NULL) ──
                    let coinbase_credits = credits::table
                        .filter(credits::height.eq(h))
                        .filter(credits::txid.is_null())
                        .select(credits::first)
                        .load::<DbDigest>(conn)
                        .await?;

                    if !coinbase_credits.is_empty() {
                        let block_jam: Vec<u8> = blocks::table
                            .filter(blocks::height.eq(h))
                            .select(blocks::jam)
                            .first::<Vec<u8>>(conn)
                            .await?;

                        let page = Page::from_noun(
                            &cue(&block_jam).ok_or(LayerErrorSource::OtherError(
                                format!("failed to cue block at height {h}"),
                            ))?,
                        )
                        .ok_or(LayerErrorSource::OtherError(
                            format!("failed to decode block page at height {h}"),
                        ))?;

                        let cb_recipients = Self::coinbase_recipients(&page, constants);

                        for first in coinbase_credits {
                            let (recipient_type, recipient) = cb_recipients
                                .get(&first)
                                .ok_or_else(|| {
                                    LayerErrorSource::OtherError(format!(
                                        "coinbase credit first={first} not found in block page at height {h}"
                                    ))
                                })?
                                .clone();

                            block_info.push(CreditInfo {
                                txid: None,
                                first,
                                height: h,
                                updated_height: h,
                                recipient_type,
                                recipient,
                            });
                        }
                    }

                    cur_metadata = FixedLayerMetadata {
                        layer: Self::LAYER,
                        next_block_height: h + 1,
                    };
                    let next_metadata = cur_metadata;

                    conn.spawn_blocking(move |conn| {
                        use diesel::query_dsl::methods::ExecuteDsl;
                        conn.transaction(move |conn| {
                            if !block_info.is_empty() {
                                ExecuteDsl::execute(
                                    diesel::insert_into(credit_info::table).values(&block_info),
                                    conn,
                                )?;
                            }
                            ExecuteDsl::execute(
                                Self::update_layer_metadata(&next_metadata),
                                conn,
                            )?;
                            Ok(())
                        })
                    })
                    .instrument(tracing::info_span!("l4_commit_block", height))
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
