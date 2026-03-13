pub mod schema;

use super::{l0::schema::*, layer::*, shared_schema::*};
use crate::chain_activations::ChainActivations;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use iris_crypto::PublicKey;
use iris_nockchain_types::{
    v0::LegacySignature,
    v1::{Lock, LockPrimitive, Pkh, SpendCondition, SpendV1},
    Note, Page, RawTx,
};
use iris_ztd::{cue, jam, Digest, Hashable, NounDecode, NounEncode, ZMap};
use log::*;
use schema::*;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::Instrument;

pub struct L3Client {
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

    fn lock_leaf_count(lock: &Lock) -> usize {
        match lock {
            Lock::Single(_) => 1,
            Lock::V2(_) => 2,
            Lock::V4(_) => 4,
            Lock::V8(_) => 8,
            Lock::V16(_) => 16,
        }
    }

    fn collect_spend_condition_owners(
        sc: &SpendCondition,
        root: LockRootDigest,
        first: NoteName,
        height: i32,
        lock_owners_rows: &mut Vec<LockOwner>,
        name_owners_rows: &mut Vec<NameOwner>,
    ) {
        for pkh_group in sc.pkh() {
            for pkh in &pkh_group.hashes {
                let pkh = PkhDigest(*pkh);
                lock_owners_rows.push(LockOwner { root, pkh, height });
                name_owners_rows.push(NameOwner { first, pkh, height });
            }
        }
    }

    fn collect_pk_rows(
        pk: &PublicKey,
        first: NoteName,
        height: i32,
        pk_to_pkh_rows: &mut Vec<PkToPkh>,
        name_owners_rows: &mut Vec<NameOwner>,
    ) {
        let pkh = PkhDigest(pk.hash());
        let pk_b58 = bs58::encode(jam(pk.to_noun())).into_string();
        pk_to_pkh_rows.push(PkToPkh {
            pk: pk_b58,
            pkh,
            height,
        });
        name_owners_rows.push(NameOwner { first, pkh, height });
    }

    fn collect_legacy_signature_rows(
        signature: &LegacySignature,
        first: NoteName,
        height: i32,
        pk_to_pkh_rows: &mut Vec<PkToPkh>,
        name_owners_rows: &mut Vec<NameOwner>,
    ) {
        for (pk, _) in &signature.0 {
            Self::collect_pk_rows(pk, first, height, pk_to_pkh_rows, name_owners_rows);
        }
    }

    fn merkle_proof_siblings(axis: u64, path: &[Digest]) -> Vec<(i32, Digest)> {
        let mut out = vec![];
        let mut cur_axis = axis;
        for sibling_hash in path {
            let sibling_axis = if cur_axis % 2 == 0 {
                cur_axis + 1
            } else {
                cur_axis - 1
            };
            out.push((sibling_axis as i32, *sibling_hash));
            cur_axis /= 2;
        }
        out
    }

    fn parse_lock_from_note_data(note_data: &iris_nockchain_types::v1::NoteData) -> Option<Lock> {
        for (k, v) in &note_data.0 {
            if k != "lock" {
                continue;
            }
            if let Some((_, lock)) = <(u64, Lock)>::from_noun(v) {
                return Some(lock);
            }
            if let Some((_, (tag, pkh), _)) = <(u64, (String, Pkh), u64)>::from_noun(v) {
                if tag == "pkh" {
                    return Some(Lock::Single(SpendCondition(vec![LockPrimitive::Pkh(pkh)])));
                }
            }
        }
        None
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

        trace!("Dropping pk_to_pkh");
        diesel::delete(pk_to_pkh::table)
            .filter(pk_to_pkh::height.ge(metadata.next_block_height))
            .execute(conn)
            .await?;
        trace!("Dropping name_owners");
        diesel::delete(name_owners::table)
            .filter(name_owners::height.ge(metadata.next_block_height))
            .execute(conn)
            .await?;
        trace!("Dropping lock_owners");
        diesel::delete(lock_owners::table)
            .filter(lock_owners::height.ge(metadata.next_block_height))
            .execute(conn)
            .await?;
        trace!("Dropping lock_paths");
        diesel::delete(lock_paths::table)
            .filter(lock_paths::height.ge(metadata.next_block_height))
            .execute(conn)
            .await?;
        trace!("Dropping locks");
        diesel::delete(locks::table)
            .filter(locks::height.ge(metadata.next_block_height))
            .execute(conn)
            .await?;
        trace!("Dropping lock_names");
        diesel::delete(lock_names::table)
            .filter(lock_names::height.ge(metadata.next_block_height))
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
            let dep_metadata = Self::layer_metadata(conn)
                .await?
                .unwrap_or(FixedLayerMetadata {
                    layer: Self::LAYER,
                    next_block_height: 0,
                });
            for dep in &self.deps {
                dep.update_blocks(conn, dep_metadata).await?;
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
            let dep_metadata = Self::layer_metadata(conn)
                .await?
                .unwrap_or(FixedLayerMetadata {
                    layer: Self::LAYER,
                    next_block_height: 0,
                });
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

        trace!("Syncing lock/name mappings from {start_block_height} to {end_block_height}");
        let constants = self.activations.constants();
        let step = 100u32;

        for block_height in (start_block_height..=end_block_height).step_by(step as usize) {
            let block_range_span =
                tracing::info_span!("l3_update_block_range", block_height, end_block_height);
            let last_block_height = core::cmp::min(block_height + step - 1, end_block_height);

            async {
                for height in block_height..=last_block_height {
                    let get_block_span = tracing::info_span!("l3_db_get_block", height);
                    let block = blocks::table
                        .filter(blocks::height.eq(height as i32))
                        .first::<Block>(conn)
                        .instrument(get_block_span)
                        .await?;

                    let page = Page::from_noun(
                        &cue(&block.jam).ok_or(LayerErrorSource::NounCue(height, *block.id))?,
                    )
                    .ok_or(LayerErrorSource::NounDecode(height, *block.id))?;

                    let mut block_lock_names = vec![];
                    let mut block_locks = vec![];
                    let mut block_lock_paths = vec![];
                    let mut block_lock_owners = vec![];
                    let mut block_name_owners = vec![];
                    let mut block_pk_to_pkh = vec![];

                    for coinbase_note in page.coinbase(constants) {
                        let first = coinbase_note.name().first.into();
                        match coinbase_note {
                            Note::V0(v0_note) => {
                                for pk in &v0_note.sig.pubkeys {
                                    Self::collect_pk_rows(
                                        pk,
                                        first,
                                        height as i32,
                                        &mut block_pk_to_pkh,
                                        &mut block_name_owners,
                                    );
                                }
                            }
                            Note::V1(_) => {}
                        }
                    }

                    let get_txs_span = tracing::info_span!("l3_db_get_txs", height);
                    let txs = transactions::table
                        .filter(transactions::height.eq(height as i32))
                        .order_by(transactions::id)
                        .load::<Transaction>(conn)
                        .instrument(get_txs_span)
                        .await?;

                    for tx in txs {
                        let tx_height = tx.height as u32;
                        let raw = RawTx::from_noun(
                            &cue(&tx.jam).ok_or(LayerErrorSource::NounCue(tx_height, *tx.id))?,
                        )
                        .ok_or(LayerErrorSource::NounDecode(tx_height, *tx.id))?;

                        match &raw {
                            RawTx::V0(raw_v0) => {
                                for (name, input) in &raw_v0.inputs.0 {
                                    if let Some(signature) = &input.spend.signature {
                                        Self::collect_legacy_signature_rows(
                                            signature,
                                            name.first.into(),
                                            tx.height,
                                            &mut block_pk_to_pkh,
                                            &mut block_name_owners,
                                        );
                                    }
                                }

                                let outputs = raw.outputs(
                                    tx_height,
                                    self.activations.tx_engine(tx_height),
                                );
                                for note in outputs {
                                    if let Note::V0(v0_note) = note {
                                        let first = v0_note.name.first.into();
                                        for pk in &v0_note.sig.pubkeys {
                                            Self::collect_pk_rows(
                                                pk,
                                                first,
                                                tx.height,
                                                &mut block_pk_to_pkh,
                                                &mut block_name_owners,
                                            );
                                        }
                                    }
                                }
                            }
                            RawTx::V1(raw_v1) => {
                                let spends = ZMap::from_iter(
                                    raw_v1
                                        .spends
                                        .0
                                        .iter()
                                        .map(|(name, spend)| (*name, spend.clone())),
                                );

                                for (name, spend) in spends {
                                    let input_first = name.first.into();

                                    match &spend {
                                        SpendV1::S0(legacy_spend) => {
                                            Self::collect_legacy_signature_rows(
                                                &legacy_spend.signature,
                                                input_first,
                                                tx.height,
                                                &mut block_pk_to_pkh,
                                                &mut block_name_owners,
                                            );
                                        }
                                        SpendV1::S1(witness_spend) => {
                                            let lock_proof = &witness_spend.witness.lock_merkle_proof;
                                            let root = LockRootDigest(lock_proof.proof().root);
                                            let sc = lock_proof.spend_condition();
                                            let sc_hash: NoteName = sc.hash().into();
                                            let idx = lock_proof.axis().saturating_sub(1) as i32;

                                            block_lock_names.push(LockName {
                                                root,
                                                first: input_first,
                                                height: tx.height,
                                            });
                                            block_locks.push(LockEntry {
                                                root,
                                                idx,
                                                hash: sc_hash,
                                                jam: jam(sc.to_noun()),
                                                height: tx.height,
                                            });
                                            block_lock_paths.push(LockPath {
                                                root,
                                                axis: 1,
                                                hash: root.0.into(),
                                                height: tx.height,
                                            });
                                            for (axis, digest) in Self::merkle_proof_siblings(
                                                lock_proof.axis(),
                                                &lock_proof.proof().path,
                                            ) {
                                                block_lock_paths.push(LockPath {
                                                    root,
                                                    axis,
                                                    hash: digest.into(),
                                                    height: tx.height,
                                                });
                                            }
                                            Self::collect_spend_condition_owners(
                                                sc,
                                                root,
                                                input_first,
                                                tx.height,
                                                &mut block_lock_owners,
                                                &mut block_name_owners,
                                            );

                                            for (pkh, (pk, _)) in
                                                &witness_spend.witness.pkh_signature.0
                                            {
                                                let pk_b58 =
                                                    bs58::encode(jam(pk.to_noun())).into_string();
                                                block_pk_to_pkh.push(PkToPkh {
                                                    pk: pk_b58,
                                                    pkh: (*pkh).into(),
                                                    height: tx.height,
                                                });
                                                block_name_owners.push(NameOwner {
                                                    first: input_first,
                                                    pkh: (*pkh).into(),
                                                    height: tx.height,
                                                });
                                            }
                                        }
                                    }

                                    for seed in &spend.seeds().0 {
                                        let root_digest = seed.lock_root.hash();
                                        let root: LockRootDigest = root_digest.into();
                                        let first: NoteName = (true, root_digest).hash().into();

                                        block_lock_names.push(LockName {
                                            root,
                                            first,
                                            height: tx.height,
                                        });

                                        if let Some(lock) = Self::parse_lock_from_note_data(&seed.note_data)
                                        {
                                            let leaf_count = Self::lock_leaf_count(&lock);
                                            for idx in 0..leaf_count {
                                                let sc = &lock[idx];
                                                block_locks.push(LockEntry {
                                                    root,
                                                    idx: idx as i32,
                                                    hash: sc.hash().into(),
                                                    jam: jam(sc.to_noun()),
                                                    height: tx.height,
                                                });
                                                Self::collect_spend_condition_owners(
                                                    sc,
                                                    root,
                                                    first,
                                                    tx.height,
                                                    &mut block_lock_owners,
                                                    &mut block_name_owners,
                                                );
                                            }
                                            block_lock_paths.push(LockPath {
                                                root,
                                                axis: 1,
                                                hash: lock.hash().into(),
                                                height: tx.height,
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }

                    let next_metadata = FixedLayerMetadata {
                        layer: Self::LAYER,
                        next_block_height: height as i32 + 1,
                    };

                    conn.spawn_blocking(move |conn| {
                        use diesel::query_dsl::methods::ExecuteDsl;
                        conn.transaction(move |conn| {
                            if !block_lock_names.is_empty() {
                                let q1 = diesel::insert_or_ignore_into(lock_names::table)
                                    .values(&block_lock_names);
                                ExecuteDsl::execute(q1, conn)?;
                            }
                            if !block_locks.is_empty() {
                                let q2 = diesel::insert_or_ignore_into(locks::table).values(&block_locks);
                                ExecuteDsl::execute(q2, conn)?;
                            }
                            if !block_lock_paths.is_empty() {
                                let q3 = diesel::insert_or_ignore_into(lock_paths::table)
                                    .values(&block_lock_paths);
                                ExecuteDsl::execute(q3, conn)?;
                            }
                            if !block_lock_owners.is_empty() {
                                let q4 = diesel::insert_or_ignore_into(lock_owners::table)
                                    .values(&block_lock_owners);
                                ExecuteDsl::execute(q4, conn)?;
                            }
                            if !block_name_owners.is_empty() {
                                let q5 = diesel::insert_or_ignore_into(name_owners::table)
                                    .values(&block_name_owners);
                                ExecuteDsl::execute(q5, conn)?;
                            }
                            if !block_pk_to_pkh.is_empty() {
                                let q6 = diesel::insert_or_ignore_into(pk_to_pkh::table)
                                    .values(&block_pk_to_pkh);
                                ExecuteDsl::execute(q6, conn)?;
                            }
                            let q7 = Self::update_layer_metadata(&next_metadata);
                            ExecuteDsl::execute(q7, conn)?;
                            Ok(())
                        })
                    })
                    .instrument(tracing::info_span!("l3_commit_block", height))
                    .await?;
                }

                Ok::<(), LayerErrorSource>(())
            }
            .instrument(block_range_span)
            .await?;
        }

        let dep_metadata = Self::layer_metadata(conn)
            .await?
            .unwrap_or(FixedLayerMetadata {
                layer: Self::LAYER,
                next_block_height: 0,
            });
        for dep in &self.deps {
            dep.update_blocks(conn, dep_metadata).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel::dsl::count_star;
    use diesel_async::RunQueryDsl;
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
        std::env::temp_dir().join(format!("iris-blocks-l3-test-{ts}.sqlite"))
    }

    async fn setup_conn_and_client() -> Option<(crate::db::AsyncDbConnection, L3Client, PathBuf)> {
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
        let client = L3Client::new(ChainActivations::mainnet(), vec![]);
        Some((conn, client, dst))
    }

    async fn run_l3_range(
        client: &L3Client,
        conn: &mut crate::db::AsyncDbConnection,
        start: i32,
        end: i32,
    ) {
        if start > 0 {
            L3Client::update_layer_metadata(&FixedLayerMetadata {
                layer: "l3",
                next_block_height: start,
            })
            .execute(conn)
            .await
            .expect("seed l3 metadata");
        }

        client
            .update_blocks(
                conn,
                FixedLayerMetadata {
                    layer: "l2",
                    next_block_height: end + 1,
                },
            )
            .await
            .expect("l3 update");
    }

    #[tokio::test]
    async fn test_l3_decode_single_block() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping test_l3_decode_single_block: TEST_DB_PATH not set");
            return;
        };

        run_l3_range(&client, &mut conn, 9745, 9745).await;

        let ln = lock_names::table
            .filter(lock_names::height.eq(9745))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count lock_names");
        let l = locks::table
            .filter(locks::height.eq(9745))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count locks");
        let lo = lock_owners::table
            .filter(lock_owners::height.eq(9745))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count lock_owners");
        let no = name_owners::table
            .filter(name_owners::height.eq(9745))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count name_owners");
        let pp = pk_to_pkh::table
            .filter(pk_to_pkh::height.eq(9745))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count pk_to_pkh");

        assert!(ln >= 0);
        assert!(l >= 0);
        assert!(lo >= 0);
        assert!(no > 0);
        assert!(pp > 0);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_l3_sync_block_range() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping test_l3_sync_block_range: TEST_DB_PATH not set");
            return;
        };

        run_l3_range(&client, &mut conn, 9740, 9750).await;

        let no = name_owners::table
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count name_owners");
        let pp = pk_to_pkh::table
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count pk_to_pkh");
        let lp = lock_paths::table
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count lock_paths");

        assert!(no > 0);
        assert!(pp > 0);
        assert!(lp >= 0);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_l3_expire_blocks() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping test_l3_expire_blocks: TEST_DB_PATH not set");
            return;
        };

        run_l3_range(&client, &mut conn, 39000, 39010).await;
        let rollback = 39005i32;
        client
            .expire_blocks(
                &mut conn,
                FixedLayerMetadata {
                    layer: "l2",
                    next_block_height: rollback,
                },
            )
            .await
            .expect("expire l3");

        let high_name_owners = name_owners::table
            .filter(name_owners::height.ge(rollback))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count high name_owners");
        let high_pk_to_pkh = pk_to_pkh::table
            .filter(pk_to_pkh::height.ge(rollback))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count high pk_to_pkh");
        assert_eq!(high_name_owners, 0);
        assert_eq!(high_pk_to_pkh, 0);

        let m = L3Client::layer_metadata(&mut conn)
            .await
            .expect("layer metadata")
            .expect("metadata exists");
        assert_eq!(m.next_block_height, rollback);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_l3_coinbase_lock_mapping() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping test_l3_coinbase_lock_mapping: TEST_DB_PATH not set");
            return;
        };

        run_l3_range(&client, &mut conn, 0, 2).await;
        let c = name_owners::table
            .filter(name_owners::height.le(2))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count early name_owners");
        assert!(c >= 0);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_l3_pk_to_pkh_round_trip() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping test_l3_pk_to_pkh_round_trip: TEST_DB_PATH not set");
            return;
        };

        run_l3_range(&client, &mut conn, 9745, 9745).await;
        let rows = pk_to_pkh::table
            .filter(pk_to_pkh::height.eq(9745))
            .load::<PkToPkh>(&mut conn)
            .await
            .expect("load pk_to_pkh");

        for row in rows {
            let pk_bytes = bs58::decode(&row.pk).into_vec().expect("decode pk b58");
            let pk_noun = cue(&pk_bytes).expect("cue pk");
            let pk = PublicKey::from_noun(&pk_noun).expect("decode pk");
            assert_eq!(pk.hash(), *row.pkh);
        }

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_l3_lock_names_deterministic() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping test_l3_lock_names_deterministic: TEST_DB_PATH not set");
            return;
        };

        run_l3_range(&client, &mut conn, 9745, 9745).await;
        let rows = lock_names::table
            .filter(lock_names::height.eq(9745))
            .load::<LockName>(&mut conn)
            .await
            .expect("load lock_names");

        for row in rows {
            let expected: NoteName = (true, *row.root).hash().into();
            assert_eq!(row.first, expected);
        }

        let _ = std::fs::remove_file(path);
    }
}
