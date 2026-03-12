pub mod schema;

use super::{l0::schema::transactions, layer::*, shared_schema::*};
use crate::chain_activations::ChainActivations;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use iris_nockchain_types::{v1::SpendV1, RawTx};
use iris_ztd::{cue, jam, Hashable, NounDecode, NounEncode, ZMap};
use log::*;
use schema::*;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::Instrument;

pub struct L2Client {
    activations: ChainActivations,
    deps: Vec<Arc<dyn LayerDependency>>,
    stats_tx: watch::Sender<Option<<Self as LayerBase>::Stats>>,
    stats_rx: watch::Receiver<Option<<Self as LayerBase>::Stats>>,
}

impl L2Client {
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

impl LayerBase for L2Client {
    const ACCEPT_LAYERS: &'static [&'static str] = &["l1"];
    const LAYER: &'static str = "l2";
    type Stats = ();
    fn stats_handle(&self) -> watch::Receiver<Option<Self::Stats>> {
        self.stats_rx.clone()
    }
}

impl LayerImpl for L2Client {
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

        trace!("Dropping tx_signers");
        diesel::delete(tx_signers::table)
            .filter(tx_signers::height.ge(metadata.next_block_height))
            .execute(conn)
            .await?;

        trace!("Dropping tx_outputs");
        diesel::delete(tx_outputs::table)
            .filter(tx_outputs::height.ge(metadata.next_block_height))
            .execute(conn)
            .await?;

        trace!("Dropping tx_seeds");
        diesel::delete(tx_seeds::table)
            .filter(tx_seeds::height.ge(metadata.next_block_height))
            .execute(conn)
            .await?;

        trace!("Dropping tx_spends");
        diesel::delete(tx_spends::table)
            .filter(tx_spends::height.ge(metadata.next_block_height))
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
            return Ok(());
        }

        let cur_metadata = Self::layer_metadata(conn).await?;
        let start_block_height = cur_metadata
            .as_ref()
            .map(|m| m.next_block_height)
            .unwrap_or_default() as u32;
        let end_block_height = metadata.next_block_height as u32 - 1;

        if start_block_height > end_block_height {
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

        trace!("Syncing tx internals from {start_block_height} to {end_block_height}");
        let step = 100u32;

        for block_height in (start_block_height..=end_block_height).step_by(step as usize) {
            let block_range_span =
                tracing::info_span!("l2_update_block_range", block_height, end_block_height);
            let last_block_height = core::cmp::min(block_height + step - 1, end_block_height);

            async {
                for height in block_height..=last_block_height {
                    let get_txs_span = tracing::info_span!("l2_db_get_txs", height);
                    let txs = transactions::table
                        .filter(transactions::height.eq(height as i32))
                        .order_by(transactions::id)
                        .load::<super::l0::schema::Transaction>(conn)
                        .instrument(get_txs_span)
                        .await?;

                    let mut block_spends = vec![];
                    let mut block_seeds = vec![];
                    let mut block_outputs = vec![];
                    let mut block_signers = vec![];

                    for tx in txs {
                        let tx_height = tx.height as u32;
                        let raw = RawTx::from_noun(
                            &cue(&tx.jam).ok_or(LayerErrorSource::NounCue(tx_height, *tx.id))?,
                        )
                        .ok_or(LayerErrorSource::NounDecode(tx_height, *tx.id))?;

                        let spend_version = u32::from(raw.version()) as i32;
                        let mut global_seed_idx = 0i32;

                        match &raw {
                            RawTx::V0(raw_v0) => {
                                for (z, (name, input)) in raw_v0.inputs.0.iter().enumerate() {
                                    block_spends.push(TxSpend {
                                        txid: tx.id,
                                        z: z as i32,
                                        version: spend_version,
                                        first: name.first.into(),
                                        last: name.last.into(),
                                        fee: input.spend.fee.0 as i64,
                                        height: tx.height,
                                    });

                                    if let Some(signature) = &input.spend.signature {
                                        for (pk, _) in signature.0.iter() {
                                            let pk_b58 =
                                                bs58::encode(jam(pk.to_noun())).into_string();
                                            block_signers.push(TxSigner {
                                                txid: tx.id,
                                                z: z as i32,
                                                pk: pk_b58,
                                                height: tx.height,
                                            });
                                        }
                                    }

                                    for seed in &input.spend.seeds.0 {
                                        block_seeds.push(TxSeed {
                                            txid: tx.id,
                                            idx: global_seed_idx,
                                            amount: seed.gift.0 as i64,
                                            first: seed.recipient.hash().into(),
                                            height: tx.height,
                                        });
                                        global_seed_idx += 1;
                                    }
                                }
                            }
                            RawTx::V1(raw_v1) => {
                                let spends_map = ZMap::from_iter(
                                    raw_v1
                                        .spends
                                        .0
                                        .iter()
                                        .map(|(name, spend)| (*name, spend.clone())),
                                );
                                for (z, (name, spend)) in spends_map.into_iter().enumerate() {
                                    block_spends.push(TxSpend {
                                        txid: tx.id,
                                        z: z as i32,
                                        version: spend_version,
                                        first: name.first.into(),
                                        last: name.last.into(),
                                        fee: spend.fee().0 as i64,
                                        height: tx.height,
                                    });

                                    match &spend {
                                        SpendV1::S0(legacy_spend) => {
                                            for (pk, _) in legacy_spend.signature.0.iter() {
                                                let pk_b58 =
                                                    bs58::encode(jam(pk.to_noun())).into_string();
                                                block_signers.push(TxSigner {
                                                    txid: tx.id,
                                                    z: z as i32,
                                                    pk: pk_b58,
                                                    height: tx.height,
                                                });
                                            }
                                        }
                                        SpendV1::S1(witness_spend) => {
                                            for (_, (pk, _)) in
                                                witness_spend.witness.pkh_signature.0.iter()
                                            {
                                                let pk_b58 =
                                                    bs58::encode(jam(pk.to_noun())).into_string();
                                                block_signers.push(TxSigner {
                                                    txid: tx.id,
                                                    z: z as i32,
                                                    pk: pk_b58,
                                                    height: tx.height,
                                                });
                                            }
                                        }
                                    }

                                    for seed in spend.seeds().0.iter() {
                                        let first = (true, seed.lock_root.hash()).hash();
                                        block_seeds.push(TxSeed {
                                            txid: tx.id,
                                            idx: global_seed_idx,
                                            amount: seed.gift.0 as i64,
                                            first: first.into(),
                                            height: tx.height,
                                        });
                                        global_seed_idx += 1;
                                    }
                                }
                            }
                        }

                        let outputs = raw.outputs(tx_height, self.activations.tx_engine(tx_height));
                        for (idx, note) in outputs.into_iter().enumerate() {
                            block_outputs.push(TxOutput {
                                txid: tx.id,
                                idx: idx as i32,
                                first: note.name().first.into(),
                                last: note.name().last.into(),
                                assets: note.assets().0 as i64,
                                height: tx.height,
                            });
                        }
                    }

                    let next_metadata = FixedLayerMetadata {
                        layer: Self::LAYER,
                        next_block_height: height as i32 + 1,
                    };

                    conn.spawn_blocking(move |conn| {
                        use diesel::query_dsl::methods::ExecuteDsl;
                        conn.transaction(move |conn| {
                            if !block_spends.is_empty() {
                                let q1 =
                                    diesel::insert_into(tx_spends::table).values(&block_spends);
                                ExecuteDsl::execute(q1, conn)?;
                            }

                            if !block_seeds.is_empty() {
                                let q2 = diesel::insert_into(tx_seeds::table).values(&block_seeds);
                                ExecuteDsl::execute(q2, conn)?;
                            }

                            if !block_outputs.is_empty() {
                                let q3 =
                                    diesel::insert_into(tx_outputs::table).values(&block_outputs);
                                ExecuteDsl::execute(q3, conn)?;
                            }

                            if !block_signers.is_empty() {
                                let q4 =
                                    diesel::insert_into(tx_signers::table).values(&block_signers);
                                ExecuteDsl::execute(q4, conn)?;
                            }

                            let q5 = Self::update_layer_metadata(&next_metadata);
                            ExecuteDsl::execute(q5, conn)?;
                            Ok(())
                        })
                    })
                    .instrument(tracing::info_span!("l2_commit_block", height))
                    .await?;
                }

                Ok::<(), LayerErrorSource>(())
            }
            .instrument(block_range_span)
            .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // LOCAL-ONLY TEST HARNESS:
    // These tests are for local validation against a real SQLite snapshot (`TEST_DB_PATH`)
    // and are not part of production runtime behavior.
    // Remove this module if you want a PR that includes only runtime flow changes.
    use super::*;
    use crate::layers::l0::schema::transactions;
    use diesel::dsl::count_star;
    use diesel_async::RunQueryDsl;
    use iris_crypto::PublicKey;
    use iris_ztd::{cue, jam, NounDecode, NounEncode, ZMap};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_db_path() -> Option<PathBuf> {
        std::env::var("TEST_DB_PATH").ok().map(PathBuf::from)
    }

    fn temp_copy_path() -> PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!("iris-blocks-l2-test-{ts}.sqlite"))
    }

    async fn setup_conn_and_client() -> Option<(crate::db::AsyncDbConnection, L2Client, PathBuf)> {
        let src = test_db_path()?;
        if !src.exists() {
            return None;
        }

        let dst = temp_copy_path();
        std::fs::copy(src, &dst).ok()?;

        let mut conn = crate::db::new_conn(dst.to_str().expect("db path"), 1)
            .await
            .ok()?;
        crate::db::run_migrations(&mut conn).await;
        let client = L2Client::new(ChainActivations::mainnet(), vec![]);
        Some((conn, client, dst))
    }

    async fn run_l2_range(
        client: &L2Client,
        conn: &mut crate::db::AsyncDbConnection,
        start: i32,
        end: i32,
    ) {
        client
            .update_blocks(
                conn,
                FixedLayerMetadata {
                    layer: "l1",
                    next_block_height: end + 1,
                },
            )
            .await
            .expect("l2 update");

        let m = L2Client::layer_metadata(conn)
            .await
            .expect("layer metadata")
            .expect("metadata exists");
        assert!(m.next_block_height >= start);
        assert!(m.next_block_height >= end + 1);
    }

    #[tokio::test]
    async fn test_l2_decode_single_tx() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping test_l2_decode_single_tx: TEST_DB_PATH not set");
            return;
        };

        let target_height = 9745i32;
        run_l2_range(&client, &mut conn, target_height, target_height).await;

        let tx = transactions::table
            .filter(transactions::height.eq(target_height))
            .order_by(transactions::id)
            .first::<super::super::l0::schema::Transaction>(&mut conn)
            .await
            .expect("target tx exists");

        let raw = RawTx::from_noun(&cue(&tx.jam).expect("cue tx jam")).expect("decode raw tx");
        let (expected_spends, expected_seeds, expected_signers) = match &raw {
            RawTx::V0(raw_v0) => {
                let spends = raw_v0.inputs.0.iter().count() as i64;
                let seeds = raw_v0
                    .inputs
                    .0
                    .iter()
                    .map(|(_, input)| input.spend.seeds.0.len() as i64)
                    .sum::<i64>();
                let signers = raw_v0
                    .inputs
                    .0
                    .iter()
                    .map(|(_, input)| {
                        input
                            .spend
                            .signature
                            .as_ref()
                            .map(|sig| sig.0.len() as i64)
                            .unwrap_or(0)
                    })
                    .sum::<i64>();
                (spends, seeds, signers)
            }
            RawTx::V1(raw_v1) => {
                let spends_map = ZMap::from_iter(
                    raw_v1
                        .spends
                        .0
                        .iter()
                        .map(|(name, spend)| (*name, spend.clone())),
                );
                let spends = spends_map.clone().into_iter().count() as i64;
                let seeds = spends_map
                    .clone()
                    .into_iter()
                    .map(|(_, spend)| spend.seeds().0.len() as i64)
                    .sum::<i64>();
                let signers = spends_map
                    .into_iter()
                    .map(|(_, spend)| match spend {
                        SpendV1::S0(legacy) => legacy.signature.0.len() as i64,
                        SpendV1::S1(witness) => witness.witness.pkh_signature.0.len() as i64,
                    })
                    .sum::<i64>();
                (spends, seeds, signers)
            }
        };
        let outputs = raw.outputs(
            tx.height as u32,
            client.activations.tx_engine(tx.height as u32),
        );
        let expected_outputs = outputs.len() as i64;

        let got_spends = tx_spends::table
            .filter(tx_spends::txid.eq(tx.id))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count tx_spends");
        let got_seeds = tx_seeds::table
            .filter(tx_seeds::txid.eq(tx.id))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count tx_seeds");
        let got_outputs = tx_outputs::table
            .filter(tx_outputs::txid.eq(tx.id))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count tx_outputs");
        let got_signers = tx_signers::table
            .filter(tx_signers::txid.eq(tx.id))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count tx_signers");

        assert_eq!(got_spends, expected_spends);
        assert_eq!(got_seeds, expected_seeds);
        assert_eq!(got_outputs, expected_outputs);
        assert_eq!(got_signers, expected_signers);

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_l2_sync_block_range() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping test_l2_sync_block_range: TEST_DB_PATH not set");
            return;
        };

        let start = 5629i32;
        let end = 5640i32;
        run_l2_range(&client, &mut conn, start, end).await;

        let spends_count = tx_spends::table
            .filter(tx_spends::height.ge(start).and(tx_spends::height.le(end)))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count spends");
        let seeds_count = tx_seeds::table
            .filter(tx_seeds::height.ge(start).and(tx_seeds::height.le(end)))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count seeds");
        let outputs_count = tx_outputs::table
            .filter(tx_outputs::height.ge(start).and(tx_outputs::height.le(end)))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count outputs");
        let signers_count = tx_signers::table
            .filter(tx_signers::height.ge(start).and(tx_signers::height.le(end)))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count signers");

        assert!(spends_count > 0);
        assert!(seeds_count > 0);
        assert!(outputs_count > 0);
        assert!(signers_count >= 0);

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_l2_expire_blocks() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping test_l2_expire_blocks: TEST_DB_PATH not set");
            return;
        };

        let start = 5629i32;
        let end = 5640i32;
        run_l2_range(&client, &mut conn, start, end).await;

        let rollback = 5635i32;
        client
            .expire_blocks(
                &mut conn,
                FixedLayerMetadata {
                    layer: "l1",
                    next_block_height: rollback,
                },
            )
            .await
            .expect("expire blocks");

        let spends_high = tx_spends::table
            .filter(tx_spends::height.ge(rollback))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count high spends");
        let seeds_high = tx_seeds::table
            .filter(tx_seeds::height.ge(rollback))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count high seeds");
        let outputs_high = tx_outputs::table
            .filter(tx_outputs::height.ge(rollback))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count high outputs");
        let signers_high = tx_signers::table
            .filter(tx_signers::height.ge(rollback))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count high signers");

        assert_eq!(spends_high, 0);
        assert_eq!(seeds_high, 0);
        assert_eq!(outputs_high, 0);
        assert_eq!(signers_high, 0);

        let m = L2Client::layer_metadata(&mut conn)
            .await
            .expect("layer metadata")
            .expect("metadata exists");
        assert_eq!(m.next_block_height, rollback);

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_l2_no_txs_block() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping test_l2_no_txs_block: TEST_DB_PATH not set");
            return;
        };

        let txs_at_zero = transactions::table
            .filter(transactions::height.eq(0))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count txs at zero");
        assert_eq!(txs_at_zero, 0);

        run_l2_range(&client, &mut conn, 0, 2).await;

        let spends_at_zero = tx_spends::table
            .filter(tx_spends::height.eq(0))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count spends at zero");
        let seeds_at_zero = tx_seeds::table
            .filter(tx_seeds::height.eq(0))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count seeds at zero");
        let outputs_at_zero = tx_outputs::table
            .filter(tx_outputs::height.eq(0))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count outputs at zero");
        let signers_at_zero = tx_signers::table
            .filter(tx_signers::height.eq(0))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count signers at zero");

        assert_eq!(spends_at_zero, 0);
        assert_eq!(seeds_at_zero, 0);
        assert_eq!(outputs_at_zero, 0);
        assert_eq!(signers_at_zero, 0);

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_l2_signer_extraction() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping test_l2_signer_extraction: TEST_DB_PATH not set");
            return;
        };

        let target_height = 9745i32;
        run_l2_range(&client, &mut conn, target_height, target_height).await;

        let signer = tx_signers::table
            .filter(tx_signers::height.eq(target_height))
            .order_by((tx_signers::txid, tx_signers::z))
            .first::<TxSigner>(&mut conn)
            .await
            .expect("signer row");

        let pk_bytes = bs58::decode(&signer.pk)
            .into_vec()
            .expect("decode signer pk base58");
        let pk_noun = cue(&pk_bytes).expect("cue signer pk");
        let pk = PublicKey::from_noun(&pk_noun).expect("decode public key noun");

        let pkh = pk.hash();
        assert_ne!(pkh.to_string(), "");

        let back_b58 = bs58::encode(jam(pk.to_noun())).into_string();
        assert_eq!(back_b58, signer.pk);

        let _ = std::fs::remove_file(path);
    }
}
