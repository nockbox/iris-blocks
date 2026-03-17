pub mod schema;

use super::{l0::schema::transactions, layer::*, shared_schema::*};
use crate::chain_activations::ChainActivations;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use iris_nockchain_types::{
    v1::{Lock, NoteData, SpendCondition, SpendV1},
    Note, Tx,
};
use iris_ztd::{cue, jam, Hashable, MerkleProof, NounDecode, NounEncode};
use log::*;
use schema::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::watch;
use tracing::Instrument;

pub struct L2Client {
    #[allow(dead_code)]
    activations: ChainActivations,
    deps: Vec<Arc<dyn LayerDependency>>,
    stats_tx: watch::Sender<Option<<Self as LayerBase>::Stats>>,
    stats_rx: watch::Receiver<Option<<Self as LayerBase>::Stats>>,
}

/// Per-block buffers for all L2 sub-layer outputs.
/// Data is collected in-memory per block and inserted in one DB transaction.
struct L2BlockBuffers {
    // L2.1
    spends: Vec<TxSpend>,
    seeds: Vec<TxSeed>,
    outputs: Vec<TxOutput>,
    signers: Vec<TxSigner>,
    // L2.2
    name_to_lock: Vec<NameToLock>,
    pkh_to_pk: Vec<PkhToPk>,
    // L2.3
    lock_trees: Vec<LockTree>,
    spend_conditions: Vec<SpendConditionRow>,
}

impl L2BlockBuffers {
    fn new() -> Self {
        Self {
            spends: vec![],
            seeds: vec![],
            outputs: vec![],
            signers: vec![],
            name_to_lock: vec![],
            pkh_to_pk: vec![],
            lock_trees: vec![],
            spend_conditions: vec![],
        }
    }
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

    fn checked_u64_to_i64(value: u64, field: &'static str) -> Result<i64, LayerErrorSource> {
        i64::try_from(value).map_err(|_| {
            LayerErrorSource::OtherError(format!("numeric value out of range for {field}: {value}"))
        })
    }

    fn checked_u32_to_i32(value: u32, field: &'static str) -> Result<i32, LayerErrorSource> {
        i32::try_from(value).map_err(|_| {
            LayerErrorSource::OtherError(format!("numeric value out of range for {field}: {value}"))
        })
    }

    fn checked_usize_to_i32(value: usize, field: &'static str) -> Result<i32, LayerErrorSource> {
        i32::try_from(value).map_err(|_| {
            LayerErrorSource::OtherError(format!("numeric value out of range for {field}: {value}"))
        })
    }

    /// Parse note_data `%lock`/`%pkh` entry and return a normalized lock.
    fn lock_from_note_data(note_data: &NoteData) -> Option<Lock> {
        let lock_key = "lock".to_string();
        let pkh_key = "pkh".to_string();
        let lock_noun = note_data
            .0
            .get(&lock_key)
            .or_else(|| note_data.0.get(&pkh_key))?;

        // Common case: single spend-condition payload decodes directly as Lock.
        if let Some(lock) = Lock::from_noun(lock_noun) {
            return Some(lock);
        }
        // Explicitly require v0-tagged %lock encoding when represented as a pair.
        if let Some((0u64, lock)) = <(u64, Lock)>::from_noun(lock_noun) {
            return Some(lock);
        }
        if let Some((0u64, lock, 0u64)) = <(u64, Lock, u64)>::from_noun(lock_noun) {
            return Some(lock);
        }
        None
    }

    fn spend_conditions_from_lock(lock: &Lock) -> Vec<(u64, SpendCondition)> {
        let count = 1usize << (lock.height() - 1);
        let mut out = vec![];
        for idx in 0..count {
            let sp = lock[idx].clone();
            // We only need the Merkle axis for each leaf; generating a proof is
            // the most direct way to derive that axis from the lock structure.
            let lmp = MerkleProof::prove_hashable(&sp, idx);
            out.push((lmp.axis, sp));
        }
        out
    }

    /// L2.1: Collect transaction internals (spends, seeds, outputs, signers).
    fn collect_tx_internals(
        tx: &super::l0::schema::Transaction,
        vtx: &Tx,
        spend_version: i32,
        bufs: &mut L2BlockBuffers,
    ) -> Result<(), LayerErrorSource> {
        match vtx {
            Tx::V0(v0) => {
                for (z, (name, input)) in v0.raw.inputs.0.iter().enumerate() {
                    bufs.spends.push(TxSpend {
                        txid: tx.id,
                        z: Self::checked_usize_to_i32(z, "tx_spends.z")?,
                        version: spend_version,
                        first: name.first.into(),
                        last: name.last.into(),
                        fee: Self::checked_u64_to_i64(input.spend.fee.0, "tx_spends.fee")?,
                        height: tx.height,
                    });

                    if let Some(signature) = &input.spend.signature {
                        for (pk, _) in signature.0.iter() {
                            bufs.signers.push(TxSigner {
                                txid: tx.id,
                                z: Self::checked_usize_to_i32(z, "tx_signers.z")?,
                                pk: pk.clone().into(),
                                height: tx.height,
                            });
                        }
                    }

                    let z_i32 = Self::checked_usize_to_i32(z, "tx_seeds.z")?;
                    for (idx, seed) in input.spend.seeds.0.iter().enumerate() {
                        bufs.seeds.push(TxSeed {
                            txid: tx.id,
                            z: z_i32,
                            idx: idx as i32,
                            amount: Self::checked_u64_to_i64(seed.gift.0, "tx_seeds.amount")?,
                            first: seed.recipient.hash().into(),
                            height: tx.height,
                        });
                    }
                }
            }
            Tx::V1(v1) => {
                for (z, (name, spend)) in v1.raw.spends.0.iter().enumerate() {
                    bufs.spends.push(TxSpend {
                        txid: tx.id,
                        z: Self::checked_usize_to_i32(z, "tx_spends.z")?,
                        version: spend_version,
                        first: name.first.into(),
                        last: name.last.into(),
                        fee: Self::checked_u64_to_i64(spend.fee().0, "tx_spends.fee")?,
                        height: tx.height,
                    });

                    match spend {
                        SpendV1::S0(legacy_spend) => {
                            for (pk, _) in legacy_spend.signature.0.iter() {
                                bufs.signers.push(TxSigner {
                                    txid: tx.id,
                                    z: Self::checked_usize_to_i32(z, "tx_signers.z")?,
                                    pk: pk.clone().into(),
                                    height: tx.height,
                                });
                            }
                        }
                        SpendV1::S1(witness_spend) => {
                            for (_, (pk, _)) in witness_spend.witness.pkh_signature.0.iter() {
                                bufs.signers.push(TxSigner {
                                    txid: tx.id,
                                    z: Self::checked_usize_to_i32(z, "tx_signers.z")?,
                                    pk: pk.clone().into(),
                                    height: tx.height,
                                });
                            }
                        }
                    }

                    let z_i32 = Self::checked_usize_to_i32(z, "tx_seeds.z")?;
                    for (idx, seed) in spend.seeds().0.iter().enumerate() {
                        let first = (true, seed.lock_root.hash()).hash();
                        bufs.seeds.push(TxSeed {
                            txid: tx.id,
                            z: z_i32,
                            idx: idx as i32,
                            amount: Self::checked_u64_to_i64(seed.gift.0, "tx_seeds.amount")?,
                            first: first.into(),
                            height: tx.height,
                        });
                    }
                }
            }
        }

        let outputs = vtx.outputs().notes();
        for (idx, note) in outputs.into_iter().enumerate() {
            bufs.outputs.push(TxOutput {
                txid: tx.id,
                idx: Self::checked_usize_to_i32(idx, "tx_outputs.idx")?,
                first: note.name().first.into(),
                last: note.name().last.into(),
                assets: Self::checked_u64_to_i64(note.assets().0, "tx_outputs.assets")?,
                height: tx.height,
            });
        }

        Ok(())
    }

    /// L2.2: Collect hash reversals (name_to_lock, pkh_to_pk).
    fn collect_hash_reversals(
        tx: &super::l0::schema::Transaction,
        vtx: &Tx,
        block_id: DbDigest,
        bufs: &mut L2BlockBuffers,
    ) {
        match vtx {
            Tx::V0(v0) => {
                // V0: extract pk→pkh from legacy signatures
                for (_, input) in v0.raw.inputs.0.iter() {
                    if let Some(signature) = &input.spend.signature {
                        for (pk, _) in signature.0.iter() {
                            let pkh = pk.hash();
                            bufs.pkh_to_pk.push(PkhToPk {
                                pkh: pkh.into(),
                                pk: pk.clone().into(),
                                height: tx.height,
                                block_id,
                            });
                        }
                    }
                }
            }
            Tx::V1(v1) => {
                for (_, spend) in v1.raw.spends.0.iter() {
                    match spend {
                        SpendV1::S0(legacy_spend) => {
                            for (pk, _) in legacy_spend.signature.0.iter() {
                                let pkh = pk.hash();
                                bufs.pkh_to_pk.push(PkhToPk {
                                    pkh: pkh.into(),
                                    pk: pk.clone().into(),
                                    height: tx.height,
                                    block_id,
                                });
                            }
                        }
                        SpendV1::S1(witness_spend) => {
                            let lock_proof = &witness_spend.witness.lock_merkle_proof;
                            let root: DbDigest = lock_proof.proof().root.into();

                            // Reverse-map signer PKH to revealed PK from witness signatures.
                            for (pkh, (pk, _)) in &witness_spend.witness.pkh_signature.0 {
                                bufs.pkh_to_pk.push(PkhToPk {
                                    pkh: (*pkh).into(),
                                    pk: pk.clone().into(),
                                    height: tx.height,
                                    block_id,
                                });
                            }
                            let _ = root; // Root-to-name mapping is written in collect_spend_conditions.
                        }
                    }

                    // Reverse-map seed lock roots to synthetic first names.
                    for seed in spend.seeds().0.iter() {
                        let root_digest = seed.lock_root.hash();
                        let first = (true, root_digest).hash();
                        bufs.name_to_lock.push(NameToLock {
                            first: first.into(),
                            root: root_digest.into(),
                            height: tx.height,
                            block_id,
                        });
                    }
                }
            }
        }
    }

    /// L2.3: Collect spend conditions (lock_tree, spend_conditions) from V1 witness spends.
    fn collect_spend_conditions(
        tx: &super::l0::schema::Transaction,
        vtx: &Tx,
        block_id: DbDigest,
        bufs: &mut L2BlockBuffers,
    ) -> Result<(), LayerErrorSource> {
        let Tx::V1(v1) = vtx else { return Ok(()) };

        for (z, (name, spend)) in v1.raw.spends.0.iter().enumerate() {
            let SpendV1::S1(witness_spend) = spend else {
                continue;
            };

            let lock_proof = &witness_spend.witness.lock_merkle_proof;
            let root: DbDigest = lock_proof.proof().root.into();
            let sc = lock_proof.spend_condition();
            let sc_hash: DbDigest = sc.hash().into();

            // Record input name -> lock root mapping for witness spends.
            let input_first: DbDigest = name.first.into();
            bufs.name_to_lock.push(NameToLock {
                first: input_first,
                root,
                height: tx.height,
                block_id,
            });

            // Persist Merkle proof branch hashes needed to re-materialize lock trees.
            for (axis, digest) in lock_proof
                .proof()
                .visible_hashes(lock_proof.axis(), &*sc_hash)
                .ok_or_else(|| {
                    LayerErrorSource::OtherError(format!("invalid merkle proof on root {root}"))
                })?
            {
                bufs.lock_trees.push(LockTree {
                    root,
                    height: tx.height,
                    axis: axis as i32,
                    hash: digest.into(),
                });
            }

            // Persist the revealed spend condition payload.
            bufs.spend_conditions.push(SpendConditionRow {
                hash: sc_hash,
                txid: tx.id,
                z: Some(z as i32),
                height: tx.height,
                jam: jam(sc.to_noun()),
            });
        }

        // Parse V1 output note_data (`%lock`/`%pkh`) and verify that the derived
        // lock root matches the output first name before indexing derived rows.
        for out in vtx.outputs().notes() {
            let Note::V1(v1_note) = out else { continue };
            let Some(lock) = Self::lock_from_note_data(&v1_note.note_data) else {
                continue;
            };
            let lock_root = lock.hash();
            let expected_first = (true, lock_root).hash();
            let actual_first = v1_note.name.first;
            if expected_first != actual_first {
                warn!(
                    "v1 output note_data lock-root/name mismatch: txid={} expected_first={} actual_first={}",
                    tx.id, expected_first, actual_first
                );
                continue;
            }

            for (axis, sc) in Self::spend_conditions_from_lock(&lock) {
                let hash = sc.hash();

                bufs.spend_conditions.push(SpendConditionRow {
                    hash: hash.into(),
                    txid: tx.id,
                    z: None,
                    height: tx.height,
                    jam: jam(sc.to_noun()),
                });

                bufs.lock_trees.push(LockTree {
                    root: lock_root.into(),
                    height: tx.height,
                    axis: axis as i32,
                    hash: hash.into(),
                });
            }
        }

        Ok(())
    }
}

impl LayerBase for L2Client {
    const ACCEPT_LAYERS: &'static [&'static str] = &["l1"];
    const LAYER: &'static str = "l2";
    type Stats = L2Stats;
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

        // L2.3
        trace!("Dropping spend_conditions");
        diesel::delete(spend_conditions::table)
            .filter(spend_conditions::height.ge(metadata.next_block_height))
            .execute(conn)
            .await?;
        trace!("Dropping lock_tree");
        diesel::delete(lock_tree::table)
            .filter(lock_tree::height.ge(metadata.next_block_height))
            .execute(conn)
            .await?;

        // L2.2
        trace!("Dropping pkh_to_pk");
        diesel::delete(pkh_to_pk::table)
            .filter(pkh_to_pk::height.ge(metadata.next_block_height))
            .execute(conn)
            .await?;
        trace!("Dropping name_to_lock");
        diesel::delete(name_to_lock::table)
            .filter(name_to_lock::height.ge(metadata.next_block_height))
            .execute(conn)
            .await?;

        // L2.1
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
                next_block_height: start_block_height as i32,
            },
        )
        .await?;

        let step = 100u32;
        let last_block_height = core::cmp::min(start_block_height + step - 1, end_block_height);

        trace!("Syncing L2 from {start_block_height} to {last_block_height}");
        let mut cur_metadata = FixedLayerMetadata {
            layer: Self::LAYER,
            next_block_height: start_block_height as i32,
        };

        let block_range_span = tracing::info_span!(
            "l2_update_block_range",
            start_block_height,
            last_block_height
        );

        async {
            for height in start_block_height..=last_block_height {
                let get_txs_span = tracing::info_span!("l2_db_get_txs", height);
                let txs = transactions::table
                    .filter(transactions::height.eq(height as i32))
                    .order_by(transactions::id)
                    .load::<super::l0::schema::Transaction>(conn)
                    .instrument(get_txs_span)
                    .await?;

                // Get block_id for L2.2/L2.3
                let block_id: DbDigest = if !txs.is_empty() {
                    txs[0].block_id
                } else {
                    use super::l0::schema::blocks;
                    blocks::table
                        .filter(blocks::height.eq(height as i32))
                        .select(blocks::id)
                        .first::<DbDigest>(conn)
                        .await
                        .unwrap_or(DbDigest(iris_ztd::Digest::from_bytes(&[0; 32])))
                };

                let mut bufs = L2BlockBuffers::new();

                for tx in txs {
                    let tx_height = u32::try_from(tx.height).map_err(|_| {
                        LayerErrorSource::OtherError(format!(
                            "negative block height in transactions.height: {}",
                            tx.height
                        ))
                    })?;
                    let vtx = Tx::from_noun(
                        &cue(&tx.jam).ok_or(LayerErrorSource::NounCue(tx_height, *tx.id))?,
                    )
                    .ok_or(LayerErrorSource::NounDecode(tx_height, *tx.id))?;
                    let spend_version =
                        Self::checked_u32_to_i32(u32::from(vtx.version()), "tx_spends.version")?;

                    // L2.1: transaction internals
                    Self::collect_tx_internals(&tx, &vtx, spend_version, &mut bufs)?;
                    // L2.2: hash reversals
                    Self::collect_hash_reversals(&tx, &vtx, block_id, &mut bufs);
                    // L2.3: spend conditions
                    Self::collect_spend_conditions(&tx, &vtx, block_id, &mut bufs)?;

                    crate::rt::yield_now().await;
                }

                cur_metadata = FixedLayerMetadata {
                    layer: Self::LAYER,
                    next_block_height: height as i32 + 1,
                };
                let next_metadata = cur_metadata;

                conn.spawn_blocking(move |conn| {
                    use diesel::query_dsl::methods::ExecuteDsl;
                    conn.transaction(move |conn| {
                        // L2.1
                        if !bufs.spends.is_empty() {
                            ExecuteDsl::execute(
                                diesel::insert_into(tx_spends::table).values(&bufs.spends),
                                conn,
                            )?;
                        }
                        if !bufs.seeds.is_empty() {
                            ExecuteDsl::execute(
                                diesel::insert_into(tx_seeds::table).values(&bufs.seeds),
                                conn,
                            )?;
                        }
                        if !bufs.outputs.is_empty() {
                            ExecuteDsl::execute(
                                diesel::insert_into(tx_outputs::table).values(&bufs.outputs),
                                conn,
                            )?;
                        }
                        if !bufs.signers.is_empty() {
                            ExecuteDsl::execute(
                                diesel::insert_into(tx_signers::table).values(&bufs.signers),
                                conn,
                            )?;
                        }
                        // L2.2
                        if !bufs.name_to_lock.is_empty() {
                            ExecuteDsl::execute(
                                diesel::insert_or_ignore_into(name_to_lock::table)
                                    .values(&bufs.name_to_lock),
                                conn,
                            )?;
                        }
                        if !bufs.pkh_to_pk.is_empty() {
                            ExecuteDsl::execute(
                                diesel::insert_or_ignore_into(pkh_to_pk::table)
                                    .values(&bufs.pkh_to_pk),
                                conn,
                            )?;
                        }
                        // L2.3
                        if !bufs.lock_trees.is_empty() {
                            ExecuteDsl::execute(
                                diesel::insert_or_ignore_into(lock_tree::table)
                                    .values(&bufs.lock_trees),
                                conn,
                            )?;
                        }
                        if !bufs.spend_conditions.is_empty() {
                            ExecuteDsl::execute(
                                diesel::insert_or_ignore_into(spend_conditions::table)
                                    .values(&bufs.spend_conditions),
                                conn,
                            )?;
                        }

                        ExecuteDsl::execute(Self::update_layer_metadata(&next_metadata), conn)?;
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

        let mapped_names = name_to_lock::table.count().get_result::<i64>(conn).await? as u64;
        let mapped_pkhs = pkh_to_pk::table.count().get_result::<i64>(conn).await? as u64;
        let spend_conds = spend_conditions::table
            .count()
            .get_result::<i64>(conn)
            .await? as u64;
        let lock_nodes = lock_tree::table.count().get_result::<i64>(conn).await? as u64;

        self.stats_tx
            .send(Some(L2Stats {
                mapped_names,
                mapped_pkhs,
                spend_conditions: spend_conds,
                lock_nodes,
            }))
            .unwrap();

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
pub struct L2Stats {
    pub mapped_names: u64,
    pub mapped_pkhs: u64,
    pub spend_conditions: u64,
    pub lock_nodes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layers::l0::schema::transactions;
    use diesel::dsl::count_star;
    use diesel_async::RunQueryDsl;
    use iris_crypto::PublicKey;
    use iris_nockchain_types::RawTx;
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

        let mut conn = crate::db::new_conn(dst.to_str().expect("db path"))
            .await
            .ok()?;
        crate::db::run_migrations(&mut conn).await.ok()?;
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

        let pk_bytes = bs58::decode(signer.pk.to_string())
            .into_vec()
            .expect("decode signer pk base58");
        let pk_noun = cue(&pk_bytes).expect("cue signer pk");
        let pk = PublicKey::from_noun(&pk_noun).expect("decode public key noun");

        let pkh = pk.hash();
        assert_ne!(pkh.to_string(), "");

        let back_b58 = bs58::encode(jam(pk.to_noun())).into_string();
        assert_eq!(back_b58, signer.pk.to_string());

        let _ = std::fs::remove_file(path);
    }
}
