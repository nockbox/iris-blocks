pub mod schema;

use super::{
    l0::schema::blocks,
    l1::schema::notes,
    l2::schema::{lock_tree, name_to_lock, spend_conditions, SpendConditionRow},
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
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::watch;
use tracing::Instrument;

pub struct L4Client {
    activations: ChainActivations,
    deps: Vec<Arc<dyn LayerDependency>>,
    stats_tx: watch::Sender<Option<<Self as LayerBase>::Stats>>,
    stats_rx: watch::Receiver<Option<<Self as LayerBase>::Stats>>,
}

impl L4Client {
    pub fn new(activations: ChainActivations, deps: Vec<Arc<dyn LayerDependency>>) -> Self {
        let (stats_tx, stats_rx) = Self::verify_dependencies(&deps).unwrap();
        Self {
            activations,
            deps,
            stats_tx,
            stats_rx,
        }
    }

    /// Classify owner identity from a decoded spend condition.
    fn resolve_owner_from_spend_condition(sc: &SpendCondition) -> (String, String) {
        let pkhs: BTreeSet<_> = sc.pkh().flat_map(|v| v.hashes.iter()).collect();
        if pkhs.len() == 1 {
            if let Some(pkh) = pkhs.iter().next() {
                return ("pkh".to_string(), pkh.to_string());
            } else {
                ("musig".to_string(), sc.hash().to_string())
            }
        } else if pkhs.len() > 1 {
            ("musig".to_string(), sc.hash().to_string())
        } else {
            // No signer set was recoverable from the spend condition.
            ("lock".to_string(), sc.hash().to_string())
        }
    }

    /// Resolve a V0 note's owner by decoding its JAM and extracting signature keys.
    fn resolve_v0_owner(note_jam: &[u8]) -> Result<(String, String), LayerErrorSource> {
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
            // Multi-key V0 note: store by lock hash, not by a single key.
            Ok(("musig".to_string(), note.sig.hash().to_string()))
        }
    }

    /// Resolve owner for a lock root by decoding a revealed spend condition.
    /// For single lock roots, classify to `pkh`/`musig` when signer data is present;
    /// otherwise keep `lock` to represent unresolved ownership.
    async fn resolve_owner_from_root_spend_condition(
        conn: &mut crate::db::AsyncDbConnection,
        root: DbDigest,
        height: i32,
    ) -> Result<(String, String), LayerErrorSource> {
        let sc_row = spend_conditions::table
            .filter(spend_conditions::hash.eq(root))
            .filter(spend_conditions::height.le(height))
            .first::<SpendConditionRow>(conn)
            .await
            .optional()?;

        let Some(sc_row) = sc_row else {
            return Err(LayerErrorSource::OtherError(format!(
                "missing revealed spend_condition for root={root}"
            )));
        };

        let sc = SpendCondition::from_noun(&cue(&sc_row.jam).ok_or_else(|| {
            LayerErrorSource::OtherError(format!("failed to cue spend condition for root={root}"))
        })?)
        .ok_or_else(|| {
            LayerErrorSource::OtherError(format!(
                "failed to decode spend condition for root={root}"
            ))
        })?;

        let (owner_type, owner) = Self::resolve_owner_from_spend_condition(&sc);
        if owner_type == "lock" {
            debug!("single lock_tree row for root={root} has no signers. SC: {sc:?}");
        }
        Ok((owner_type, owner))
    }

    /// Build V1 coinbase first-name ownership map from a block page.
    /// V0 pages can be skipped because V0 ownership is resolved from notes.
    fn coinbase_recipients(
        page: &Page,
        constants: iris_nockchain_types::BlockchainConstants,
    ) -> BTreeMap<DbDigest, (String, String)> {
        let mut map = BTreeMap::new();
        if let Page::V1(p) = page {
            let notes = page.coinbase(constants);
            for (note, (pkh, _)) in notes.iter().zip(p.coinbase.0.iter()) {
                let first = DbDigest(note.name().first);
                map.insert(first, ("pkh".to_string(), pkh.to_string()));
            }
        }
        map
    }
}

impl LayerBase for L4Client {
    const ACCEPT_LAYERS: &'static [&'static str] = &["l3"];
    const LAYER: &'static str = "l4";
    type Stats = L4Stats;
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

        diesel::delete(name_info::table)
            .filter(name_info::height.ge(metadata.next_block_height))
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

        let constants = self.activations.constants();
        let step = 100u32;
        let last_block_height = core::cmp::min(start_block_height + step - 1, end_block_height);

        trace!("Syncing name_info from {start_block_height} to {last_block_height}");
        let mut cur_metadata = FixedLayerMetadata {
            layer: Self::LAYER,
            next_block_height: start_block_height as i32,
        };

        let block_range_span = tracing::info_span!(
            "l4_update_block_range",
            start_block_height,
            last_block_height
        );

        async {
            for height in start_block_height..=last_block_height {
                let h = height as i32;
                let block_started = Instant::now();
                let mut block_name_info = vec![];
                let mut inserted_firsts: BTreeSet<DbDigest> = BTreeSet::new();
                let block_jam: Vec<u8> = blocks::table
                    .filter(blocks::height.eq(h))
                    .select(blocks::jam)
                    .first::<Vec<u8>>(conn)
                    .await
                    .map_err(|_| {
                        LayerErrorSource::OtherError(format!(
                            "missing block jam for coinbase mapping at height={h}"
                        ))
                    })?;
                let page = Page::from_noun(
                    &cue(&block_jam).ok_or_else(|| {
                        LayerErrorSource::OtherError(format!("failed to cue block at height {h}"))
                    })?,
                )
                .ok_or_else(|| {
                    LayerErrorSource::OtherError(format!(
                        "failed to decode block page at height {h}"
                    ))
                })?;
                let coinbase_owner_map = Self::coinbase_recipients(&page, constants);

                // Phase 1: resolve ownership for roots whose spend conditions
                // became visible at this height.
                let revealed_sc_hashes = spend_conditions::table
                    .filter(spend_conditions::height.eq(h))
                    .select(spend_conditions::hash)
                    .load::<DbDigest>(conn)
                    .await?;

                let mut revealed_root_count = 0usize;
                for sc_hash in revealed_sc_hashes {
                    let sc_roots = lock_tree::table
                        .filter(lock_tree::height.eq(h))
                        .filter(lock_tree::hash.eq(sc_hash))
                        .select(lock_tree::root)
                        .load::<DbDigest>(conn)
                        .await?;
                    let roots = sc_roots.into_iter().collect::<BTreeSet<_>>();

                    for root in roots {
                        let root_lock_row_count = lock_tree::table
                            .filter(lock_tree::root.eq(root))
                            .filter(lock_tree::height.le(h))
                            .count()
                            .get_result::<i64>(conn)
                            .await?;

                        let (owner_type, owner) = if root_lock_row_count > 1 {
                            ("lock".to_string(), root.to_string())
                        } else if root_lock_row_count == 1 {
                            Self::resolve_owner_from_root_spend_condition(conn, root, h).await?
                        } else {
                            return Err(LayerErrorSource::OtherError(format!(
                                "missing lock_tree rows for root={root} at height<={h}"
                            )));
                        };

                        let firsts = name_to_lock::table
                            .filter(name_to_lock::root.eq(root))
                            .filter(name_to_lock::height.le(h))
                            .select(name_to_lock::first)
                            .load::<DbDigest>(conn)
                            .await?
                            .into_iter()
                            .collect::<BTreeSet<_>>();

                        for first in firsts {
                            if inserted_firsts.insert(first) {
                                block_name_info.push(NameInfo {
                                    first,
                                    height: h,
                                    version: 1,
                                    owner_type: owner_type.clone(),
                                    owner: owner.clone(),
                                });
                                revealed_root_count += 1;
                            }
                        }
                    }
                }

                // Phase 2: backfill ownership for notes created at this height
                // that still have no `name_info` row.
                let new_firsts = notes::table
                    .filter(notes::created_height.eq(h))
                    .select(notes::first)
                    .distinct()
                    .load::<DbDigest>(conn)
                    .await?;
                let mut created_note_count = 0usize;

                for first in new_firsts {
                    if inserted_firsts.contains(&first) {
                        continue;
                    }

                    let has_name_info = name_info::table
                        .filter(name_info::first.eq(first))
                        .filter(name_info::height.le(h))
                        .select(name_info::height)
                        .first::<i32>(conn)
                        .await
                        .optional()?
                        .is_some();
                    if has_name_info {
                        continue;
                    }

                    let (version, note_jam, is_coinbase) = notes::table
                        .filter(notes::first.eq(first))
                        .filter(notes::created_height.le(h))
                        .order(notes::created_height.asc())
                        .then_order_by(notes::last.asc())
                        .select((notes::version, notes::jam, notes::coinbase))
                        .first::<(i32, Vec<u8>, bool)>(conn)
                        .await
                        .map_err(|_| {
                            LayerErrorSource::OtherError(format!(
                                "missing note jam for first={first} at height={h}"
                            ))
                        })?;

                    let (owner_type, owner) = if is_coinbase {
                        coinbase_owner_map
                            .get(&first)
                            .cloned()
                            .ok_or_else(|| {
                                LayerErrorSource::OtherError(format!(
                                    "coinbase owner missing for first={first} at height={h}"
                                ))
                            })?
                    } else if version == 0 {
                        Self::resolve_v0_owner(&note_jam)?
                    } else {
                        let root = name_to_lock::table
                            .filter(name_to_lock::first.eq(first))
                            .filter(name_to_lock::height.le(h))
                            .order(name_to_lock::height.asc())
                            .select(name_to_lock::root)
                            .first::<DbDigest>(conn)
                            .await
                            .map_err(|_| {
                                LayerErrorSource::OtherError(format!(
                                    "missing name_to_lock root for v1 first={first}"
                                ))
                            })?;

                        let has_revealed_sc = spend_conditions::table
                            .filter(spend_conditions::hash.eq(root))
                            .filter(spend_conditions::height.le(h))
                            .select(spend_conditions::hash)
                            .first::<DbDigest>(conn)
                            .await
                            .optional()?
                            .is_some();
                        if has_revealed_sc {
                            return Err(LayerErrorSource::OtherError(format!(
                                "v1 first={first} has revealed spend_condition for root={root} during note processing"
                            )));
                        }

                        // Before defaulting to `lock`, check V1 coinbase mapping.
                        // Coinbase first names can be resolved directly to `pkh`.
                        if let Some((owner_type, owner)) = coinbase_owner_map.get(&first).cloned()
                        {
                            if owner_type == "pkh" {
                                (owner_type, owner)
                            } else {
                                ("lock".to_string(), root.to_string())
                            }
                        } else {
                            ("lock".to_string(), root.to_string())
                        }
                    };

                    if inserted_firsts.insert(first) {
                        block_name_info.push(NameInfo {
                            first,
                            height: h,
                            version,
                            owner_type,
                            owner,
                        });
                        created_note_count += 1;
                    }
                }

                cur_metadata = FixedLayerMetadata {
                    layer: Self::LAYER,
                    next_block_height: h + 1,
                };
                let next_metadata = cur_metadata;
                let inserted_name_info_rows = block_name_info.len();

                conn.spawn_blocking(move |conn| {
                    use diesel::query_dsl::methods::ExecuteDsl;
                    conn.transaction(move |conn| {
                        if !block_name_info.is_empty() {
                            ExecuteDsl::execute(
                                diesel::insert_into(name_info::table)
                                    .values(&block_name_info)
                                    .on_conflict_do_nothing(),
                                conn,
                            )?;
                        }
                        ExecuteDsl::execute(Self::update_layer_metadata(&next_metadata), conn)?;
                        Ok(())
                    })
                })
                .instrument(tracing::info_span!("l4_commit_block", height))
                .await?;

                tracing::debug!(
                    block_height = h,
                    revealed_root_count,
                    created_note_count,
                    inserted_name_info_rows,
                    elapsed_ms = block_started.elapsed().as_millis() as u64,
                    "l4 block derivation profile"
                );

                crate::rt::yield_now().await;
            }

            Ok::<(), LayerErrorSource>(())
        }
        .instrument(block_range_span)
        .await?;

        let ni_count = name_info::table.count().get_result::<i64>(conn).await? as u64;

        self.stats_tx
            .send(Some(L4Stats {
                credit_info: ni_count,
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
pub struct L4Stats {
    pub credit_info: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel::sql_query;
    use diesel::sql_types::{BigInt, Text};
    use diesel_async::RunQueryDsl;
    use std::collections::BTreeSet;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(diesel::QueryableByName)]
    struct RecipientTypeRow {
        #[diesel(sql_type = Text)]
        owner_type: String,
    }

    #[derive(diesel::QueryableByName)]
    struct CountRow {
        #[diesel(sql_type = BigInt)]
        count: i64,
    }

    fn test_db_path() -> Option<PathBuf> {
        std::env::var("TEST_DB_PATH").ok().map(PathBuf::from)
    }

    fn temp_copy_path() -> PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!("iris-blocks-l4-test-{ts}.sqlite"))
    }

    async fn setup_conn_and_client() -> Option<(crate::db::AsyncDbConnection, L4Client, PathBuf)> {
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
        let client = L4Client::new(ChainActivations::mainnet(), vec![]);
        Some((conn, client, dst))
    }

    async fn run_l4_range(client: &L4Client, conn: &mut crate::db::AsyncDbConnection, end: i32) {
        client
            .update_blocks(
                conn,
                FixedLayerMetadata {
                    layer: "l3",
                    next_block_height: end + 1,
                },
            )
            .await
            .expect("l4 update");
    }

    #[tokio::test]
    async fn recipient_types_are_in_expected_domain() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping recipient_types_are_in_expected_domain: TEST_DB_PATH not set");
            return;
        };
        run_l4_range(&client, &mut conn, 5650).await;

        let rows = sql_query("SELECT DISTINCT owner_type FROM name_info")
            .load::<RecipientTypeRow>(&mut conn)
            .await
            .expect("recipient types");
        let got = rows
            .into_iter()
            .map(|r| r.owner_type)
            .collect::<BTreeSet<_>>();
        let allowed = ["pkh", "pk", "musig", "lock"]
            .into_iter()
            .map(str::to_string)
            .collect::<BTreeSet<_>>();
        assert!(
            got.is_subset(&allowed),
            "unexpected recipient_type values: {got:?}"
        );

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn lock_rows_are_only_for_unresolved_v1_recipients() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!(
                "Skipping lock_rows_are_only_for_unresolved_v1_recipients: TEST_DB_PATH not set"
            );
            return;
        };
        run_l4_range(&client, &mut conn, 5650).await;

        let row = sql_query(
            "SELECT COUNT(*) AS count
             FROM name_info ni
             JOIN notes n ON n.first = ni.first
             JOIN name_to_lock ntl ON ntl.first = ni.first
             LEFT JOIN spend_conditions sc ON sc.hash = ntl.root
             WHERE ni.owner_type = 'lock'
               AND n.version >= 1
               AND sc.hash IS NOT NULL",
        )
        .get_result::<CountRow>(&mut conn)
        .await
        .expect("lock unresolved check");
        assert_eq!(row.count, 0, "resolved V1 rows should not be typed as lock");

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn v1_coinbase_rows_are_not_typed_as_lock() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping v1_coinbase_rows_are_not_typed_as_lock: TEST_DB_PATH not set");
            return;
        };
        run_l4_range(&client, &mut conn, 5650).await;

        let row = sql_query(
            "SELECT COUNT(*) AS count
             FROM notes n
             JOIN name_info ni ON ni.first = n.first
             WHERE n.coinbase = 1
               AND n.version >= 1
               AND ni.owner_type = 'lock'",
        )
        .get_result::<CountRow>(&mut conn)
        .await
        .expect("v1 coinbase lock typing check");
        assert_eq!(row.count, 0, "v1 coinbase rows should resolve as pkh");

        let _ = std::fs::remove_file(path);
    }
}
