pub mod schema;

use super::{l0::schema::blocks, l1::schema::notes, layer::*, shared_schema::*};
use crate::chain_activations::ChainActivations;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use log::*;
use schema::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::watch;
use tracing::Instrument;

pub struct L3Client {
    #[allow(dead_code)]
    activations: ChainActivations,
    deps: Vec<Arc<dyn LayerDependency>>,
    stats_tx: watch::Sender<Option<<Self as LayerBase>::Stats>>,
    stats_rx: watch::Receiver<Option<<Self as LayerBase>::Stats>>,
}

impl L3Client {
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

impl LayerBase for L3Client {
    const ACCEPT_LAYERS: &'static [&'static str] = &["l2"];
    const LAYER: &'static str = "l3";
    type Stats = L3Stats;
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

        trace!("Syncing credits/debits from {start_block_height} to {last_block_height}");
        let mut cur_metadata = FixedLayerMetadata {
            layer: Self::LAYER,
            next_block_height: start_block_height as i32,
        };

        let block_range_span = tracing::info_span!(
            "l3_update_block_range",
            start_block_height,
            last_block_height
        );

        async {
            for height in start_block_height..=last_block_height {
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

                let mut credit_map: std::collections::BTreeMap<(Option<DbDigest>, DbDigest), i64> =
                    std::collections::BTreeMap::new();
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
                let mut debit_map: std::collections::BTreeMap<(Option<DbDigest>, DbDigest), i64> =
                    std::collections::BTreeMap::new();
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

                // Keep both credits and debits even when they share (txid, first):
                // same-first outputs represent real refund/change and must remain
                // visible so received - spent tracks note-balance evolution exactly.

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

        let credit_count = credits::table.count().get_result::<i64>(conn).await? as u64;
        let debit_count = debits::table.count().get_result::<i64>(conn).await? as u64;

        self.stats_tx
            .send(Some(L3Stats {
                credits: credit_count,
                debits: debit_count,
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
pub struct L3Stats {
    pub credits: u64,
    pub debits: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel::sql_query;
    use diesel::sql_types::BigInt;
    use diesel_async::RunQueryDsl;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(diesel::QueryableByName)]
    struct CountRow {
        #[diesel(sql_type = BigInt)]
        count: i64,
    }

    #[derive(diesel::QueryableByName)]
    struct SumRow {
        #[diesel(sql_type = BigInt)]
        sum_nicks: i64,
    }

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
        let mut conn = crate::db::new_conn(dst.to_str().expect("db path"))
            .await
            .ok()?;
        crate::db::run_migrations(&mut conn).await.ok()?;
        let client = L3Client::new(ChainActivations::mainnet(), vec![]);
        Some((conn, client, dst))
    }

    #[tokio::test]
    async fn l3_generates_credits_and_debits_without_refund_double_count() {
        let Some((mut conn, client, path)) = setup_conn_and_client().await else {
            eprintln!("Skipping l3 test: TEST_DB_PATH not set");
            return;
        };

        client
            .update_blocks(
                &mut conn,
                FixedLayerMetadata {
                    layer: "l2",
                    next_block_height: 5651,
                },
            )
            .await
            .expect("l3 update");

        let credits = sql_query("SELECT COUNT(*) AS count FROM credits")
            .get_result::<CountRow>(&mut conn)
            .await
            .expect("credits count")
            .count;
        let debits = sql_query("SELECT COUNT(*) AS count FROM debits")
            .get_result::<CountRow>(&mut conn)
            .await
            .expect("debits count")
            .count;
        assert!(credits > 0);
        assert!(debits >= 0);

        let note_created = sql_query("SELECT COALESCE(SUM(assets), 0) AS sum_nicks FROM notes")
            .get_result::<SumRow>(&mut conn)
            .await
            .expect("note created sum")
            .sum_nicks;
        let note_spent = sql_query(
            "SELECT COALESCE(SUM(assets), 0) AS sum_nicks
             FROM notes
             WHERE spent_txid IS NOT NULL",
        )
        .get_result::<SumRow>(&mut conn)
        .await
        .expect("note spent sum")
        .sum_nicks;
        let l3_credits = sql_query("SELECT COALESCE(SUM(amount), 0) AS sum_nicks FROM credits")
            .get_result::<SumRow>(&mut conn)
            .await
            .expect("l3 credits sum")
            .sum_nicks;
        let l3_debits = sql_query("SELECT COALESCE(SUM(amount), 0) AS sum_nicks FROM debits")
            .get_result::<SumRow>(&mut conn)
            .await
            .expect("l3 debits sum")
            .sum_nicks;
        assert_eq!(l3_credits, note_created);
        assert_eq!(l3_debits, note_spent);

        let _ = std::fs::remove_file(path);
    }
}
