use crate::chain_activations::ChainActivations;
use crate::db;
use crate::layers::l0::{L0Client, L0Config, L0Stats};
use crate::layers::l1::{L1Client, L1Stats};
use crate::layers::l2::{L2Client, L2Stats};
use crate::layers::l3::{L3Client, L3Stats};
use crate::layers::l4::{L4Client, L4Stats};
use crate::layers::layer::*;
use futures::channel::oneshot;
use futures::prelude::*;
use iris_grpc_proto::pb::private::v1::nock_app_service_client::NockAppServiceClient;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::watch;
use tonic_web_wasm_client::Client;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

type WasmScryClient = NockAppServiceClient<Client>;

use std::sync::OnceLock;

use tracing_subscriber::{layer::SubscriberExt, reload, util::SubscriberInitExt, EnvFilter};

static LOG_FILTER_HANDLE: OnceLock<reload::Handle<EnvFilter, tracing_subscriber::Registry>> =
    OnceLock::new();

#[wasm_bindgen(js_name = "setLogging")]
pub fn set_logging(spec: Option<String>) -> Result<(), String> {
    if let Some(handle) = LOG_FILTER_HANDLE.get() {
        if let Some(spec) = spec {
            let new_filter =
                EnvFilter::try_new(&spec).map_err(|e| format!("invalid filter `{spec}`: {e}"))?;
            handle
                .reload(new_filter)
                .map_err(|e| format!("failed to reload filter: {e}"))?;
            tracing::info!(%spec, "log filter updated");
        }
        return Ok(());
    }

    let filter = EnvFilter::new(spec.unwrap_or_else(|| "info,iris_blocks=debug".to_string()));

    let (reloadable_filter, handle) = reload::Layer::new(filter);

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(true)
        .with_target(true)
        .with_level(true)
        .without_time()
        .with_writer(tracing_subscriber_wasm::MakeConsoleWriter::default());

    let _ = tracing_subscriber::registry()
        .with(reloadable_filter)
        .with(fmt_layer)
        .try_init();

    let _ = LOG_FILTER_HANDLE.set(handle);

    tracing::info!("tracing initialized");
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, tsify::Tsify)]
#[tsify(from_wasm_abi)]
pub struct BlockExporterConfig {
    pub layers: Vec<String>,
    pub db_connect: String,
    pub db_run_migrations: bool,
    pub remove_layer: Option<String>,
    pub private_grpc_connect: Option<String>,
    pub scry_no_pow: bool,
    pub verify_outputs: bool,
}

#[wasm_bindgen]
pub struct BlockExporter {
    stop_handle: oneshot::Sender<()>,
    finished_handle: oneshot::Receiver<()>,
    l0_stats: Option<watch::Receiver<Option<L0Stats>>>,
    l1_stats: Option<watch::Receiver<Option<L1Stats>>>,
    l2_stats: Option<watch::Receiver<Option<L2Stats>>>,
    l3_stats: Option<watch::Receiver<Option<L3Stats>>>,
    l4_stats: Option<watch::Receiver<Option<L4Stats>>>,
    query_tx: crate::layers::l0::DbQueryHandle<WasmScryClient>,
}

#[wasm_bindgen]
impl BlockExporter {
    #[wasm_bindgen(constructor)]
    pub async fn new(
        config: BlockExporterConfig,
        db_bytes: Option<Vec<u8>>,
    ) -> Result<Self, JsValue> {
        Self::new_impl(config, db_bytes)
            .await
            .map_err(|e| JsValue::from(e.to_string()))
    }

    async fn new_impl(
        config: BlockExporterConfig,
        db_bytes: Option<Vec<u8>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        log::info!("Starting block exporter");

        let mut enable_l0 = false;
        let mut enable_l1 = false;
        let mut enable_l2 = false;
        let mut enable_l3 = false;
        let mut enable_l4 = false;

        for layer in &config.layers {
            match &**layer {
                "l0" => enable_l0 = true,
                "l1" => enable_l1 = true,
                "l2" => enable_l2 = true,
                "l3" => enable_l3 = true,
                "l4" => enable_l4 = true,
                _ => return Err("Invalid layer".into()),
            }
        }

        if !enable_l0 {
            return Err("l0 is required".into());
        }

        // Validate layer dependencies: l4 requires l3, l3 requires l2, l2 requires l1
        if enable_l4 && !enable_l3 {
            return Err("l4 requires l3".into());
        }
        if enable_l3 && !enable_l2 {
            return Err("l3 requires l2".into());
        }
        if enable_l2 && !enable_l1 {
            return Err("l2 requires l1".into());
        }

        let scry = if let Some(connect) = config.private_grpc_connect {
            Some(NockAppServiceClient::new(Client::new(connect)))
        } else {
            None
        };

        let mut conn = db::new_conn(&config.db_connect).await?;

        if let Some(db_bytes) = db_bytes {
            conn.spawn_blocking(move |conn| -> Result<(), diesel::result::Error> {
                crate::sqlite_raw::deserialize_db(conn, &db_bytes).map_err(|e| {
                    diesel::result::Error::DatabaseError(
                        diesel::result::DatabaseErrorKind::Unknown,
                        Box::new(e),
                    )
                })
            })
            .await?;
        }

        if let Some(ref layer) = config.remove_layer {
            db::remove_layers_down_to(&mut conn, layer).await?;
        }

        if config.db_run_migrations {
            db::run_migrations(&mut conn).await?;
        }

        let activations = ChainActivations::mainnet();

        // Build layer chain bottom-up: l4 → l3 → l2 → l1 → l0
        let (l4_client, l4_stats) = if enable_l4 {
            let client = L4Client::new(activations.clone(), vec![]);
            let stats = client.stats_handle();
            (Some(Arc::new(client)), Some(stats))
        } else {
            (None, None)
        };

        let (l3_client, l3_stats) = if enable_l3 {
            let deps: Vec<Arc<dyn LayerDependency>> = if let Some(ref l4) = l4_client {
                vec![l4.clone()]
            } else {
                vec![]
            };
            let client = L3Client::new(activations.clone(), deps);
            let stats = client.stats_handle();
            (Some(Arc::new(client)), Some(stats))
        } else {
            (None, None)
        };

        let (l2_client, l2_stats) = if enable_l2 {
            let deps: Vec<Arc<dyn LayerDependency>> = if let Some(ref l3) = l3_client {
                vec![l3.clone()]
            } else {
                vec![]
            };
            let client = L2Client::new(activations.clone(), deps);
            let stats = client.stats_handle();
            (Some(Arc::new(client)), Some(stats))
        } else {
            (None, None)
        };

        let (l1_client, l1_stats) = if enable_l1 {
            let deps: Vec<Arc<dyn LayerDependency>> = if let Some(ref l2) = l2_client {
                vec![l2.clone()]
            } else {
                vec![]
            };
            let client = L1Client::new(activations.clone(), deps);
            let stats = client.stats_handle();
            (Some(client), Some(stats))
        } else {
            (None, None)
        };

        let l0_deps: Vec<Arc<dyn LayerDependency>> = if let Some(l1_client) = l1_client {
            vec![Arc::new(l1_client)]
        } else {
            vec![]
        };

        let mut cfg = L0Config::default();
        cfg.store_pow = false;
        cfg.block_range_config.block_range_scry_no_pow = config.scry_no_pow;
        cfg.verify_outputs = config.verify_outputs;
        let (l0_client, query_tx) = L0Client::new(conn, scry, cfg, activations.clone(), l0_deps);
        let l0_stats = Some(l0_client.stats_handle());

        let (stop_handle, stop_receiver) = oneshot::channel();
        let (finished_sender, finished_handle) = oneshot::channel();

        spawn_local(async move {
            futures::select! {
                _ = stop_receiver.fuse() => (),
                _ = l0_client.run().fuse() => (),
            };
            finished_sender.send(()).ok();
        });

        Ok(Self {
            stop_handle,
            finished_handle,
            l0_stats,
            l1_stats,
            l2_stats,
            l3_stats,
            l4_stats,
            query_tx,
        })
    }

    #[wasm_bindgen]
    pub async fn stop(self) {
        self.stop_handle.send(()).ok();
        self.finished_handle.await.unwrap();
    }

    #[wasm_bindgen(js_name = "nextL0Stats")]
    pub async fn next_l0_stats(&self) -> Option<L0Stats> {
        let mut changes = self.l0_stats.clone()?;
        let _ = changes.changed().await;
        let r = *changes.borrow();
        r
    }

    #[wasm_bindgen(js_name = "nextL1Stats")]
    pub async fn next_l1_stats(&self) -> Option<L1Stats> {
        let mut changes = self.l1_stats.clone()?;
        let _ = changes.changed().await;
        let r = *changes.borrow();
        r
    }

    #[wasm_bindgen(js_name = "nextL2Stats")]
    pub async fn next_l2_stats(&self) -> Option<L2Stats> {
        let mut changes = self.l2_stats.clone()?;
        let _ = changes.changed().await;
        let r = *changes.borrow();
        r
    }

    #[wasm_bindgen(js_name = "nextL3Stats")]
    pub async fn next_l3_stats(&self) -> Option<L3Stats> {
        let mut changes = self.l3_stats.clone()?;
        let _ = changes.changed().await;
        let r = *changes.borrow();
        r
    }

    #[wasm_bindgen(js_name = "nextL4Stats")]
    pub async fn next_l4_stats(&self) -> Option<L4Stats> {
        let mut changes = self.l4_stats.clone()?;
        let _ = changes.changed().await;
        let r = *changes.borrow();
        r
    }

    #[wasm_bindgen]
    pub async fn query(&self, sql: String) -> Result<String, JsValue> {
        let res = self
            .query_tx
            .query(sql)
            .await
            .map_err(|e| JsValue::from_str(&e))?;
        Ok(serde_json::to_string(&res).map_err(|e| JsValue::from_str(&e.to_string()))?)
    }

    #[wasm_bindgen(js_name = "exportDb")]
    pub async fn export_db(&self) -> Result<Vec<u8>, JsValue> {
        let res = self
            .query_tx
            .export()
            .await
            .map_err(|e| JsValue::from_str(&e))?;
        Ok(res)
    }

    #[wasm_bindgen(js_name = "updateRpc")]
    pub fn update_rpc(&self, url: Option<String>) -> Result<(), JsValue> {
        let client = url.map(|u| NockAppServiceClient::new(Client::new(u)));
        self.query_tx
            .update_rpc(client)
            .map_err(|e| JsValue::from_str(&e))
    }
}
