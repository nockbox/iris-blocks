pub mod schema;

use super::{
    l1::schema::{notes, Note as L1Note},
    l2::schema::{tx_outputs, tx_signers, tx_spends, TxOutput, TxSigner, TxSpend},
    l3::schema::{lock_names, name_owners, pk_to_pkh, LockName, NameOwner, PkToPkh},
    layer::*,
    shared_schema::*,
};
use crate::chain_activations::ChainActivations;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use log::*;
use schema::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::Instrument;

pub struct L4Client {
    activations: ChainActivations,
    deps: Vec<Arc<dyn LayerDependency>>,
}

#[derive(Debug, Clone)]
struct ResolvedRecipient {
    recipient_type: String,
    recipient: String,
}

impl L4Client {
    pub fn new(activations: ChainActivations, deps: Vec<Arc<dyn LayerDependency>>) -> Self {
        Self::verify_dependencies(&deps).unwrap();
        Self { activations, deps }
    }

    async fn resolve_recipients(
        conn: &mut crate::db::AsyncDbConnection,
        firsts: &[NoteName],
    ) -> Result<HashMap<iris_ztd::Digest, ResolvedRecipient>, LayerErrorSource> {
        let mut uniq_firsts = vec![];
        let mut seen = HashSet::new();
        for first in firsts {
            if seen.insert(*first) {
                uniq_firsts.push(*first);
            }
        }
        if uniq_firsts.is_empty() {
            return Ok(HashMap::new());
        }

        let owner_rows = name_owners::table
            .filter(name_owners::first.eq_any(&uniq_firsts))
            .load::<NameOwner>(conn)
            .await?;
        let mut owners_by_first: HashMap<iris_ztd::Digest, HashSet<iris_ztd::Digest>> =
            HashMap::new();
        for row in owner_rows {
            owners_by_first
                .entry(*row.first)
                .or_default()
                .insert(*row.pkh);
        }

        let single_pkhs: Vec<PkhDigest> = owners_by_first
            .iter()
            .filter_map(|(_, s)| {
                if s.len() == 1 {
                    s.iter().next().copied().map(PkhDigest::from)
                } else {
                    None
                }
            })
            .collect();
        let pk_rows = if single_pkhs.is_empty() {
            vec![]
        } else {
            pk_to_pkh::table
                .filter(pk_to_pkh::pkh.eq_any(single_pkhs))
                .load::<PkToPkh>(conn)
                .await?
        };
        let mut pk_by_pkh: HashMap<iris_ztd::Digest, String> = HashMap::new();
        for row in pk_rows {
            pk_by_pkh.entry(*row.pkh).or_insert(row.pk);
        }

        let unresolved_firsts: Vec<NoteName> = uniq_firsts
            .iter()
            .copied()
            .filter(|first| owners_by_first.get(&first.0).map(|s| s.len()).unwrap_or(0) != 1)
            .collect();
        let lock_rows = if unresolved_firsts.is_empty() {
            vec![]
        } else {
            lock_names::table
                .filter(lock_names::first.eq_any(unresolved_firsts))
                .load::<LockName>(conn)
                .await?
        };
        let mut root_by_first: HashMap<iris_ztd::Digest, LockRootDigest> = HashMap::new();
        for row in lock_rows {
            root_by_first.entry(*row.first).or_insert(row.root);
        }

        let mut resolved = HashMap::new();
        for first in uniq_firsts {
            let entry = if let Some(set) = owners_by_first.get(&first.0) {
                if set.len() == 1 {
                    let pkh = *set.iter().next().expect("single owner exists");
                    if let Some(pk) = pk_by_pkh.get(&pkh) {
                        ResolvedRecipient {
                            recipient_type: "pk".to_string(),
                            recipient: pk.clone(),
                        }
                    } else {
                        ResolvedRecipient {
                            recipient_type: "pkh".to_string(),
                            recipient: pkh.to_string(),
                        }
                    }
                } else if let Some(root) = root_by_first.get(&first.0) {
                    ResolvedRecipient {
                        recipient_type: "lock".to_string(),
                        recipient: (**root).to_string(),
                    }
                } else {
                    ResolvedRecipient {
                        recipient_type: "lock".to_string(),
                        recipient: (*first).to_string(),
                    }
                }
            } else if let Some(root) = root_by_first.get(&first.0) {
                ResolvedRecipient {
                    recipient_type: "lock".to_string(),
                    recipient: (**root).to_string(),
                }
            } else {
                ResolvedRecipient {
                    recipient_type: "lock".to_string(),
                    recipient: (*first).to_string(),
                }
            };
            resolved.insert(first.0, entry);
        }

        Ok(resolved)
    }
}

impl LayerBase for L4Client {
    const ACCEPT_LAYERS: &'static [&'static str] = &["l3"];
    const LAYER: &'static str = "l4";
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

        trace!("Dropping coinbase_credits");
        diesel::delete(coinbase_credits::table)
            .filter(coinbase_credits::height.ge(metadata.next_block_height))
            .execute(conn)
            .await?;
        trace!("Dropping credits");
        diesel::delete(credits::table)
            .filter(credits::height.ge(metadata.next_block_height))
            .execute(conn)
            .await?;
        trace!("Dropping debits");
        diesel::delete(debits::table)
            .filter(debits::height.ge(metadata.next_block_height))
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
        let _ = &self.activations;

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

        trace!("Syncing accounting from {start_block_height} to {end_block_height}");
        let step = 100u32;

        for block_height in (start_block_height..=end_block_height).step_by(step as usize) {
            let block_range_span =
                tracing::info_span!("l4_update_block_range", block_height, end_block_height);
            let last_block_height = core::cmp::min(block_height + step - 1, end_block_height);

            async {
                for height in block_height..=last_block_height {
                    let get_inputs_span = tracing::info_span!("l4_db_get_inputs", height);
                    let (spends, signers, outputs) = async {
                        let spends = tx_spends::table
                            .filter(tx_spends::height.eq(height as i32))
                            .load::<TxSpend>(conn)
                            .await?;
                        let signers = tx_signers::table
                            .filter(tx_signers::height.eq(height as i32))
                            .load::<TxSigner>(conn)
                            .await?;
                        let outputs = tx_outputs::table
                            .filter(tx_outputs::height.eq(height as i32))
                            .load::<TxOutput>(conn)
                            .await?;
                        Ok::<_, LayerErrorSource>((spends, signers, outputs))
                    }
                    .instrument(get_inputs_span)
                    .await?;

                    let v0_txids: HashSet<iris_ztd::Digest> = spends
                        .iter()
                        .filter(|s| s.version == 0)
                        .map(|s| *s.txid)
                        .collect();

                    let mut debit_rows: Vec<Debit> = vec![];
                    if !spends.is_empty() && !signers.is_empty() {
                        let mut firsts: Vec<NoteName> = vec![];
                        let mut lasts: Vec<NoteName> = vec![];
                        let mut seen_note = HashSet::new();
                        for s in &spends {
                            if seen_note.insert((*s.first, *s.last)) {
                                firsts.push(s.first);
                                lasts.push(s.last);
                            }
                        }

                        let note_rows = notes::table
                            .filter(notes::first.eq_any(&firsts))
                            .filter(notes::last.eq_any(&lasts))
                            .load::<L1Note>(conn)
                            .await?;
                        let note_assets: HashMap<(iris_ztd::Digest, iris_ztd::Digest), i64> =
                            note_rows
                                .into_iter()
                                .map(|n| ((*n.first, *n.last), n.assets))
                                .collect();

                        let spend_by_txz: HashMap<(iris_ztd::Digest, i32), &TxSpend> =
                            spends.iter().map(|s| ((*s.txid, s.z), s)).collect();

                        let mut signer_counts_by_txz: HashMap<(iris_ztd::Digest, i32), usize> =
                            HashMap::new();
                        let mut signer_spends: HashMap<(iris_ztd::Digest, String), Vec<i32>> =
                            HashMap::new();
                        for signer in &signers {
                            *signer_counts_by_txz.entry((*signer.txid, signer.z)).or_default() += 1;
                            signer_spends
                                .entry((*signer.txid, signer.pk.clone()))
                                .or_default()
                                .push(signer.z);
                        }

                        for ((txid, pk), zs) in signer_spends {
                            let mut amount = 0i64;
                            let mut fee = 0i64;
                            let mut sole_owner = true;
                            let mut seen_z = HashSet::new();

                            for z in zs {
                                if !seen_z.insert(z) {
                                    continue;
                                }
                                let Some(spend) = spend_by_txz.get(&(txid, z)).copied() else {
                                    warn!("Missing spend row for tx {} z {}", txid, z);
                                    continue;
                                };

                                fee += spend.fee;
                                if signer_counts_by_txz
                                    .get(&(txid, z))
                                    .copied()
                                    .unwrap_or_default()
                                    != 1
                                {
                                    sole_owner = false;
                                }

                                if let Some(v) = note_assets.get(&(*spend.first, *spend.last)) {
                                    amount += *v;
                                } else {
                                    warn!(
                                        "Missing note assets for input ({}, {})",
                                        *spend.first, *spend.last
                                    );
                                }
                            }

                            if amount > 0 || fee > 0 {
                                debit_rows.push(Debit {
                                    txid: txid.into(),
                                    pk,
                                    sole_owner,
                                    amount,
                                    fee,
                                    height: height as i32,
                                });
                            }
                        }
                    }

                    let mut credit_rows: Vec<Credit> = vec![];
                    let mut coinbase_credit_rows: Vec<CoinbaseCredit> = vec![];
                    if !outputs.is_empty() {
                        let output_firsts: Vec<NoteName> = outputs.iter().map(|o| o.first).collect();
                        let resolved_by_first = Self::resolve_recipients(conn, &output_firsts).await?;

                        for output in outputs {
                            let mut resolved = resolved_by_first.get(&*output.first).cloned().unwrap_or(
                                ResolvedRecipient {
                                    recipient_type: "lock".to_string(),
                                    recipient: (*output.first).to_string(),
                                },
                            );

                            if v0_txids.contains(&*output.txid) && resolved.recipient_type == "pk" {
                                resolved.recipient_type = "v0pk".to_string();
                            }

                            credit_rows.push(Credit {
                                txid: output.txid,
                                idx: output.idx,
                                recipient_type: resolved.recipient_type,
                                recipient: resolved.recipient,
                                amount: output.assets,
                                height: output.height,
                            });
                        }
                    }

                    let cb_notes: Vec<L1Note> = notes::table
                        .filter(notes::coinbase.eq(true))
                        .filter(notes::created_height.eq(height as i32))
                        .order_by((notes::first, notes::last))
                        .load::<L1Note>(conn)
                        .await?;
                    if !cb_notes.is_empty() {
                        let cb_firsts: Vec<NoteName> = cb_notes.iter().map(|n| n.first).collect();
                        let resolved_by_first = Self::resolve_recipients(conn, &cb_firsts).await?;

                        for (idx, note) in cb_notes.into_iter().enumerate() {
                            let mut resolved =
                                resolved_by_first
                                    .get(&*note.first)
                                    .cloned()
                                    .unwrap_or(ResolvedRecipient {
                                        recipient_type: "lock".to_string(),
                                        recipient: (*note.first).to_string(),
                                    });

                            if note.version == 0 && resolved.recipient_type == "pk" {
                                resolved.recipient_type = "v0pk".to_string();
                            }

                            coinbase_credit_rows.push(CoinbaseCredit {
                                block_id: note.created_bid,
                                idx: idx as i32,
                                recipient_type: resolved.recipient_type,
                                recipient: resolved.recipient,
                                amount: note.assets,
                                height: note.created_height,
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
                            if !debit_rows.is_empty() {
                                let q1 = diesel::insert_or_ignore_into(debits::table).values(&debit_rows);
                                ExecuteDsl::execute(q1, conn)?;
                            }
                            if !credit_rows.is_empty() {
                                let q2 = diesel::insert_or_ignore_into(credits::table).values(&credit_rows);
                                ExecuteDsl::execute(q2, conn)?;
                            }
                            if !coinbase_credit_rows.is_empty() {
                                let q3 = diesel::insert_or_ignore_into(coinbase_credits::table)
                                    .values(&coinbase_credit_rows);
                                ExecuteDsl::execute(q3, conn)?;
                            }
                            let q4 = Self::update_layer_metadata(&next_metadata);
                            ExecuteDsl::execute(q4, conn)?;
                            Ok(())
                        })
                    })
                    .instrument(tracing::info_span!("l4_commit_block", height))
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
    use crate::layers::{l2::L2Client, l3::L3Client};
    use diesel::dsl::count_star;
    use diesel_async::RunQueryDsl;
    use std::collections::HashMap;
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
        std::env::temp_dir().join(format!("iris-blocks-l4-test-{ts}.sqlite"))
    }

    async fn setup_conn_and_client() -> Option<(crate::db::AsyncDbConnection, L4Client, PathBuf)> {
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
        let client = L4Client::new(ChainActivations::mainnet(), vec![]);
        Some((conn, client, dst))
    }

    async fn run_l4_range(
        client: &L4Client,
        conn: &mut crate::db::AsyncDbConnection,
        start: i32,
        end: i32,
    ) {
        let activations = ChainActivations::mainnet();
        let l2 = L2Client::new(activations.clone(), vec![]);
        let l3 = L3Client::new(activations, vec![]);

        L2Client::update_layer_metadata(&FixedLayerMetadata {
            layer: "l2",
            next_block_height: start,
        })
        .execute(conn)
        .await
        .expect("seed l2 metadata");
        L3Client::update_layer_metadata(&FixedLayerMetadata {
            layer: "l3",
            next_block_height: start,
        })
        .execute(conn)
        .await
        .expect("seed l3 metadata");
        L4Client::update_layer_metadata(&FixedLayerMetadata {
            layer: "l4",
            next_block_height: start,
        })
        .execute(conn)
        .await
        .expect("seed l4 metadata");

        l2.update_blocks(
            conn,
            FixedLayerMetadata {
                layer: "l1",
                next_block_height: end + 1,
            },
        )
        .await
        .expect("l2 update");
        l3.update_blocks(
            conn,
            FixedLayerMetadata {
                layer: "l2",
                next_block_height: end + 1,
            },
        )
        .await
        .expect("l3 update");
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
    async fn test_l4_single_tx_debits() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping test_l4_single_tx_debits: TEST_DB_PATH not set");
            return;
        };

        let target_height = 9745;
        run_l4_range(&client, &mut conn, target_height, target_height).await;

        let rows = debits::table
            .filter(debits::height.eq(target_height))
            .load::<Debit>(&mut conn)
            .await
            .expect("load debits");
        assert!(!rows.is_empty());
        for row in rows {
            assert!(row.amount > 0);
            assert!(row.fee >= 0);

            let spends_for_tx = tx_spends::table
                .filter(tx_spends::txid.eq(row.txid))
                .load::<TxSpend>(&mut conn)
                .await
                .expect("load spends for tx");

            let mut expected_sole_owner = true;
            for spend in spends_for_tx {
                let c = tx_signers::table
                    .filter(
                        tx_signers::txid
                            .eq(row.txid)
                            .and(tx_signers::z.eq(spend.z)),
                    )
                    .select(count_star())
                    .first::<i64>(&mut conn)
                    .await
                    .expect("count signers for spend");
                if c != 1 {
                    expected_sole_owner = false;
                }
            }
            assert_eq!(row.sole_owner, expected_sole_owner);
        }

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_l4_single_tx_credits() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping test_l4_single_tx_credits: TEST_DB_PATH not set");
            return;
        };

        let target_height = 9745;
        run_l4_range(&client, &mut conn, target_height, target_height).await;

        let outputs = tx_outputs::table
            .filter(tx_outputs::height.eq(target_height))
            .load::<TxOutput>(&mut conn)
            .await
            .expect("load tx_outputs");
        let output_map: HashMap<(iris_ztd::Digest, i32), i64> =
            outputs.into_iter().map(|o| ((*o.txid, o.idx), o.assets)).collect();

        let rows = credits::table
            .filter(credits::height.eq(target_height))
            .load::<Credit>(&mut conn)
            .await
            .expect("load credits");
        assert!(!rows.is_empty());

        for row in rows {
            assert!(matches!(row.recipient_type.as_str(), "pk" | "v0pk" | "pkh" | "lock"));
            let expected_assets = output_map
                .get(&(*row.txid, row.idx))
                .copied()
                .expect("credit row has matching output");
            assert_eq!(row.amount, expected_assets);
        }

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_l4_sync_block_range() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping test_l4_sync_block_range: TEST_DB_PATH not set");
            return;
        };

        run_l4_range(&client, &mut conn, 9740, 9750).await;

        let debits_count = debits::table
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count debits");
        let credits_count = credits::table
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count credits");

        assert!(debits_count > 0);
        assert!(credits_count > 0);

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_l4_expire_blocks() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping test_l4_expire_blocks: TEST_DB_PATH not set");
            return;
        };

        run_l4_range(&client, &mut conn, 39000, 39010).await;
        let rollback = 39005i32;
        client
            .expire_blocks(
                &mut conn,
                FixedLayerMetadata {
                    layer: "l3",
                    next_block_height: rollback,
                },
            )
            .await
            .expect("expire l4");

        let high_debits = debits::table
            .filter(debits::height.ge(rollback))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count high debits");
        let high_credits = credits::table
            .filter(credits::height.ge(rollback))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count high credits");
        let high_coinbase = coinbase_credits::table
            .filter(coinbase_credits::height.ge(rollback))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count high coinbase credits");
        assert_eq!(high_debits, 0);
        assert_eq!(high_credits, 0);
        assert_eq!(high_coinbase, 0);

        let m = L4Client::layer_metadata(&mut conn)
            .await
            .expect("layer metadata")
            .expect("metadata exists");
        assert_eq!(m.next_block_height, rollback);

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_l4_debit_credit_balance() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping test_l4_debit_credit_balance: TEST_DB_PATH not set");
            return;
        };

        let target_height = 9745;
        run_l4_range(&client, &mut conn, target_height, target_height).await;

        let rows = debits::table
            .filter(debits::height.eq(target_height))
            .load::<Debit>(&mut conn)
            .await
            .expect("load debits");
        let mut by_tx: HashMap<iris_ztd::Digest, Vec<Debit>> = HashMap::new();
        for row in rows {
            by_tx.entry(*row.txid).or_default().push(row);
        }

        let selected_tx = by_tx
            .iter()
            .find_map(|(txid, ds)| {
                if !ds.is_empty() && ds.iter().all(|d| d.sole_owner) {
                    Some(*txid)
                } else {
                    None
                }
            })
            .expect("at least one all-sole-owner tx exists");

        let selected_debits = by_tx.get(&selected_tx).expect("debits for tx");
        let debit_amount_sum: i64 = selected_debits.iter().map(|d| d.amount).sum();
        let fee_sum: i64 = selected_debits.iter().map(|d| d.fee).sum();

        let credit_amount_sum = credits::table
            .filter(credits::txid.eq(TxId(selected_tx)))
            .select(credits::amount)
            .load::<i64>(&mut conn)
            .await
            .expect("load credits for tx")
            .into_iter()
            .sum::<i64>();

        assert_eq!(debit_amount_sum, credit_amount_sum + fee_sum);

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_l4_recipient_type_valid() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping test_l4_recipient_type_valid: TEST_DB_PATH not set");
            return;
        };

        run_l4_range(&client, &mut conn, 9740, 9750).await;

        let rows = credits::table
            .load::<Credit>(&mut conn)
            .await
            .expect("load credits");
        assert!(!rows.is_empty());
        for row in rows {
            assert!(matches!(row.recipient_type.as_str(), "pk" | "v0pk" | "pkh" | "lock"));
            assert!(!row.recipient.is_empty());
        }

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_l4_coinbase_credits_exist() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping test_l4_coinbase_credits_exist: TEST_DB_PATH not set");
            return;
        };

        run_l4_range(&client, &mut conn, 0, 5).await;

        let rows = coinbase_credits::table
            .filter(coinbase_credits::height.le(5))
            .load::<CoinbaseCredit>(&mut conn)
            .await
            .expect("load coinbase credits");
        assert!(!rows.is_empty());
        for row in rows {
            assert!(row.amount > 0);
            assert!(matches!(row.recipient_type.as_str(), "pk" | "v0pk" | "pkh" | "lock"));
            assert!(!row.recipient.is_empty());
        }

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_l4_coinbase_credits_expire() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping test_l4_coinbase_credits_expire: TEST_DB_PATH not set");
            return;
        };

        run_l4_range(&client, &mut conn, 0, 10).await;
        let rollback = 5i32;
        client
            .expire_blocks(
                &mut conn,
                FixedLayerMetadata {
                    layer: "l3",
                    next_block_height: rollback,
                },
            )
            .await
            .expect("expire l4");

        let high = coinbase_credits::table
            .filter(coinbase_credits::height.ge(rollback))
            .select(count_star())
            .first::<i64>(&mut conn)
            .await
            .expect("count high coinbase credits");
        assert_eq!(high, 0);

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_l4_full_balance() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping test_l4_full_balance: TEST_DB_PATH not set");
            return;
        };

        run_l4_range(&client, &mut conn, 5629, 9750).await;

        let pk_types = vec!["pk", "v0pk"];

        let candidate_pk = coinbase_credits::table
            .inner_join(debits::table.on(
                coinbase_credits::recipient
                    .eq(debits::pk)
                    .and(coinbase_credits::recipient_type.eq_any(&pk_types))
                    .and(debits::sole_owner.eq(true)),
            ))
            .select(coinbase_credits::recipient)
            .first::<String>(&mut conn)
            .await;

        let Ok(pk) = candidate_pk else {
            eprintln!("Skipping test_l4_full_balance: no pk found in both coinbase_credits and debits");
            let _ = std::fs::remove_file(path);
            return;
        };

        let tx_received = credits::table
            .filter(credits::recipient_type.eq_any(&pk_types).and(credits::recipient.eq(&pk)))
            .select(credits::amount)
            .load::<i64>(&mut conn)
            .await
            .expect("load tx credits")
            .into_iter()
            .sum::<i64>();
        let coinbase_received = coinbase_credits::table
            .filter(
                coinbase_credits::recipient_type
                    .eq_any(&pk_types)
                    .and(coinbase_credits::recipient.eq(&pk)),
            )
            .select(coinbase_credits::amount)
            .load::<i64>(&mut conn)
            .await
            .expect("load coinbase credits")
            .into_iter()
            .sum::<i64>();
        let spent_amount = debits::table
            .filter(debits::pk.eq(&pk).and(debits::sole_owner.eq(true)))
            .load::<Debit>(&mut conn)
            .await
            .expect("load debits")
            .into_iter()
            .map(|d| d.amount)
            .sum::<i64>();

        let balance = tx_received + coinbase_received - spent_amount;
        assert!(balance >= 0);

        let _ = std::fs::remove_file(path);
    }
}
