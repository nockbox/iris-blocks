use clap::Parser;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use std::io;
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

const DERIVABLE_LAYERS: &[&str] = &["l1", "l2", "l3", "l4"];

fn validate_layer_name(layer: &str) -> Result<(), Box<dyn std::error::Error>> {
    if DERIVABLE_LAYERS.iter().any(|valid| *valid == layer) {
        return Ok(());
    }
    Err(Box::new(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("invalid layer '{layer}', expected one of: l1, l2, l3, l4"),
    )))
}

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
    /// Remove a layer by reverting migrations from l4 down to the given layer,
    /// then re-running up migrations. This drops and recreates tables.
    #[arg(long, value_name = "LAYER")]
    pub remove_layer: Option<String>,
    #[arg(long, default_value = "false")]
    pub disable_l4: bool,
    #[command(flatten)]
    pub l0: L0Config,
}

impl SyncArgs {
    pub async fn run(self, db_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let _addr = self.bind;
        let mut conn = db::new_conn(db_path).await?;

        if self.run_migrations {
            db::run_migrations(&mut conn).await?;
            eprintln!("Migrations run.");
        }

        if let Some(layer) = self.remove_layer {
            validate_layer_name(&layer)?;
            db::remove_layers_down_to(&mut conn, &layer).await?;
            eprintln!("Reverted migrations down to {layer}.");
            if !self.run_migrations {
                return Ok(());
            } else {
                db::run_migrations(&mut conn).await?;
                eprintln!("Re-applied migrations.");
            }
        }

        if let Some(layer) = self.rederive_layer {
            validate_layer_name(&layer)?;
            diesel::update(layer_metadata::table)
                .filter(layer_metadata::layer.eq(&layer))
                .set(layer_metadata::next_block_height.eq(0))
                .execute(&mut conn)
                .await?;
            eprintln!("Reset {layer} next_block_height to 0.");
            return Ok(());
        }

        let activations = ChainActivations::mainnet();
        let l4_client = if self.disable_l4 {
            None
        } else {
            Some(Arc::new(L4Client::new(activations.clone(), vec![])))
        };
        let l3_deps: Vec<Arc<dyn LayerDependency>> = l4_client
            .as_ref()
            .map(|c| vec![c.clone() as Arc<dyn LayerDependency>])
            .unwrap_or_default();
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
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::NotFound,
                                "missing l0 layer metadata; run sync with a connection first",
                            )
                        })?;
                while l1_client
                    .update_blocks(&mut conn, l0_metadata)
                    .await
                    .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?
                {
                    log::trace!("More blocks available, looping");
                }
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

#[cfg(test)]
mod tests {
    use super::validate_layer_name;

    #[test]
    fn validate_layer_accepts_known_layers() {
        for layer in ["l1", "l2", "l3", "l4"] {
            validate_layer_name(layer).expect("valid layer");
        }
    }

    #[test]
    fn validate_layer_rejects_unknown_layers() {
        assert!(validate_layer_name("l0").is_err());
        assert!(validate_layer_name("foo").is_err());
    }
}
