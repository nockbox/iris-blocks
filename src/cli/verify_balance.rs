use crate::chain_activations::ChainActivations;
use crate::layers::l0::{L0Client, L0Config};
use crate::layers::l1::L1Client;
use crate::layers::layer::LayerBase;
use crate::layers::layer::LayerDependency;
use clap::Parser;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use iris_grpc_proto::client::{BalanceRequest, PublicNockchainGrpcClient};
use iris_grpc_proto::pb::private::v1::nock_app_service_client::NockAppServiceClient;
use iris_nockchain_types::{Name, Note};
use iris_ztd::{cue, Digest, NounDecode};
use log::*;
use rand::prelude::*;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};
use tonic::transport::{Channel, Uri};

use crate::db;
use crate::layers::l1::schema::notes;
use crate::layers::shared_schema::{layer_metadata, DbDigest};

#[derive(Debug, Parser)]
pub struct VerifyBalanceArgs {
    /// Public gRPC endpoint URI (e.g. http://[::1]:50051)
    #[arg(short, long)]
    pub connect: Uri,
    /// Private gRPC endpoint URI (e.g. http://[::1]:50051)
    #[arg(short, long)]
    pub sync_connect: Option<Uri>,
    /// Single first-name hash (base58) to look up
    #[arg(long)]
    pub first_name: Option<String>,
    #[arg(long)]
    pub sample_random: Option<u64>,
}

async fn check_first_name(
    conn: Arc<Mutex<db::AsyncDbConnection>>,
    mut grpc: PublicNockchainGrpcClient,
    first_name: String,
    l0_stats: Option<
        &mut watch::Receiver<Option<<L0Client<NockAppServiceClient<Channel>> as LayerBase>::Stats>>,
    >,
) -> Result<(), Box<dyn std::error::Error>> {
    let balance_update = grpc
        .wallet_get_balance(&BalanceRequest::FirstName(first_name.clone()))
        .await?;

    let height = balance_update.height;
    eprintln!(
        "Remote balance at height {height}, block_id={}, {} notes",
        balance_update.block_id,
        balance_update.notes.0.len()
    );

    let mut conn = conn.lock().await;
    let l1_meta = layer_metadata::table
        .filter(layer_metadata::layer.eq("l1"))
        .select(layer_metadata::next_block_height)
        .first::<i32>(&mut conn)
        .await
        .map_err(|e| format!("Failed to read l1 layer_metadata: {e}"))?;
    core::mem::drop(conn);

    if (l1_meta as u32) <= height {
        if let Some(l0_stats) = l0_stats {
            debug!("l1 height = ({l1_meta}) is not above remote height ({height}). Syncing.");
            l0_stats
                .wait_for(|v| v.as_ref().map(|v| v.next_block_height).unwrap_or(0) >= height)
                .await
                .unwrap();
        } else {
            return Err(format!(
                "l1 next_block_height ({l1_meta}) is not above remote height ({height}). \
                     Sync further before verifying."
            )
            .into());
        }
    }
    eprintln!("l1 next_block_height={l1_meta}, covers remote height {height}");

    let h = height as i32;
    let mut conn = conn.lock().await;
    let db_rows: Vec<(DbDigest, DbDigest, Vec<u8>)> = notes::table
        .filter(notes::first.eq(DbDigest::from(Digest::try_from(&*first_name).unwrap())))
        .filter(notes::created_height.le(h))
        .filter(notes::spent_height.is_null().or(notes::spent_height.gt(h)))
        .select((notes::first, notes::last, notes::jam))
        .load(&mut conn)
        .await?;
    core::mem::drop(conn);

    eprintln!("Local DB has {} notes at height {height}", db_rows.len());

    let mut local_notes: Vec<(Name, Note)> = db_rows
        .into_iter()
        .map(|(first, last, jam_bytes)| {
            let name = Name::new(first.0, last.0);
            let noun =
                cue(&jam_bytes).unwrap_or_else(|| panic!("Failed to cue note jam for {name}"));
            let note: Note = NounDecode::from_noun(&noun)
                .unwrap_or_else(|| panic!("Failed to decode note for {name}: {noun:?}"));
            (name, note)
        })
        .collect();
    local_notes.sort_by_key(|a| a.0);

    let mut remote_notes: Vec<(Name, Note)> = balance_update.notes.0.into_iter().collect();
    remote_notes.sort_by_key(|a| a.0);

    if local_notes.len() != remote_notes.len() {
        eprintln!(
            "MISMATCH: local has {} notes, remote has {}",
            local_notes.len(),
            remote_notes.len()
        );

        // Show which names are only local or only remote.
        let local_names: std::collections::BTreeSet<_> =
            local_notes.iter().map(|(n, _)| *n).collect();
        let remote_names: std::collections::BTreeSet<_> =
            remote_notes.iter().map(|(n, _)| *n).collect();

        let mut cnt = 0;
        for name in local_names.difference(&remote_names) {
            if cnt > 10 {
                eprintln!("  ...");
                break;
            }
            eprintln!("  only local:  {name}");
            cnt += 1;
        }
        let mut cnt = 0;
        for name in remote_names.difference(&local_names) {
            if cnt > 10 {
                eprintln!("  ...");
                break;
            }
            eprintln!("  only remote: {name}");
            cnt += 1;
        }
    }

    assert!(
        local_notes == remote_notes,
        "Balance mismatch between local DB and remote gRPC at height {height} for {first_name}",
    );

    eprintln!(
        "✓ Balances match for {first_name} ({} notes at height {height})",
        local_notes.len()
    );

    Ok(())
}

impl VerifyBalanceArgs {
    pub async fn run(self, db_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let conn = Arc::new(Mutex::new(db::new_conn(db_path).await?));

        let mut l0_stats = if let Some(sync_connect) = self.sync_connect {
            let activations = ChainActivations::mainnet();
            let l1 = Arc::new(L1Client::new(activations.clone()));
            let scry = Some(NockAppServiceClient::new(
                Channel::builder(sync_connect).connect().await?,
            ));

            let mut cfg = L0Config::default();
            cfg.store_pow = false;
            cfg.block_range_config.block_range_scry_no_pow = true;
            cfg.verify_outputs = true;

            let (client, _query_tx) = L0Client::new(
                conn.clone(),
                scry,
                cfg,
                activations,
                vec![l1 as Arc<dyn LayerDependency>],
            );
            let stats = client.stats_handle();
            tokio::spawn(client.run());
            Some(stats)
        } else {
            None
        };

        let grpc = PublicNockchainGrpcClient::connect(self.connect.to_string()).await?;

        if let Some(first_name) = self.first_name {
            check_first_name(conn, grpc, first_name, l0_stats.as_mut()).await?;
        } else if let Some(sample_random) = self.sample_random {
            let mut db = conn.lock().await;
            let mut first_names: Vec<DbDigest> = notes::table
                .select(notes::first)
                .distinct()
                .load(&mut db)
                .await?;
            core::mem::drop(db);

            first_names.shuffle(&mut rand::thread_rng());

            for first_name in first_names.iter().take(sample_random as usize) {
                check_first_name(
                    conn.clone(),
                    grpc.clone(),
                    first_name.to_string(),
                    l0_stats.as_mut(),
                )
                .await?;
            }
        }

        Ok(())
    }
}
