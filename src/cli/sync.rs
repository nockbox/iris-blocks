use clap::Parser;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use std::net::SocketAddr;
use tonic::transport::{Channel, Uri};

use crate::{
    chain_activations::ChainActivations,
    db,
    layers::{
        l0::{L0Client, L0Config},
        l1::L1Client,
        l2::L2Client,
        l3::L3Client,
        l4::L4Client,
        layer::{LayerDependency, LayerExt},
        shared_schema::layer_metadata,
    },
};
use iris_grpc_proto::pb::private::v1::nock_app_service_client::NockAppServiceClient;
use std::sync::Arc;

#[derive(Debug, Parser, Clone)]
pub struct SyncArgs {
    #[arg(short, long, default_value = "[::1]:50051")]
    pub bind: SocketAddr,
    #[arg(short, long)]
    pub connect: Option<Uri>,
    #[arg(short, long, default_value = "false")]
    pub run_migrations: bool,
    /// Reset next_block_height to 0 for the given layer (l1–l4) and exit.
    #[arg(long, value_name = "LAYER")]
    pub rederive_layer: Option<String>,
    #[command(flatten)]
    pub l0: L0Config,
}

impl SyncArgs {
    pub async fn run(self, db_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let _addr = self.bind;
        let mut conn = db::new_conn(db_path).await?;

        if self.run_migrations {
            db::run_migrations(&mut conn).await;
            eprintln!("Migrations run.");
        }

        if let Some(layer) = self.rederive_layer {
            diesel::update(layer_metadata::table)
                .filter(layer_metadata::layer.eq(&layer))
                .set(layer_metadata::next_block_height.eq(0))
                .execute(&mut conn)
                .await?;
            eprintln!("Reset {layer} next_block_height to 0.");
            return Ok(());
        }

        let activations = ChainActivations::mainnet();
        let l4_client = Arc::new(L4Client::new(activations.clone(), vec![]));
        let l3_deps: Vec<Arc<dyn LayerDependency>> = vec![l4_client.clone()];
        let l3_client = Arc::new(L3Client::new(activations.clone(), l3_deps));
        let l2_deps: Vec<Arc<dyn LayerDependency>> = vec![l3_client.clone()];
        let l2_client = Arc::new(L2Client::new(activations.clone(), l2_deps));
        let l1_deps: Vec<Arc<dyn LayerDependency>> = vec![l2_client.clone()];
        let l1_client = Arc::new(L1Client::new(activations.clone(), l1_deps));

        let connect: Uri = match self.connect {
            Some(uri) => uri,
            None => {
                eprintln!("No connection URI provided. Syncing upper layers once.");
                let l0_metadata =
                    L0Client::<NockAppServiceClient<Channel>>::layer_metadata(&mut conn)
                        .await?
                        .unwrap();
                l1_client
                    .update_blocks(&mut conn, l0_metadata)
                    .await
                    .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
                return Ok(());
            }
        };
        let scry = Some(NockAppServiceClient::new(
            Channel::builder(connect).connect().await?,
        ));
        let l0_deps: Vec<Arc<dyn LayerDependency>> = vec![l1_client.clone()];
        let (client, _query_tx) = L0Client::new(conn, scry, self.l0, activations, l0_deps);
        client.run().await;

        Ok(())
    }
}
