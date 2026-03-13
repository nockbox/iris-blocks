use crate::chain_activations::ChainActivations;
use crate::db;
use crate::layers::l0::{L0Client, L0Config, L0Stats};
use crate::layers::l1::{L1Client, L1Stats};
use crate::layers::layer::*;
use futures::channel::oneshot;
use futures::prelude::*;
use iris_grpc_proto::pb::private::v1::nock_app_service_client::NockAppServiceClient;
use std::sync::Arc;
use tokio::sync::watch;
use tonic_web_wasm_client::Client;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

use std::sync::OnceLock;

use tracing_subscriber::{
    layer::SubscriberExt, reload, util::SubscriberInitExt, EnvFilter,
};

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

#[wasm_bindgen]
pub struct BlockExporter {
    stop_handle: oneshot::Sender<()>,
    finished_handle: oneshot::Receiver<()>,
    l0_stats: Option<watch::Receiver<Option<L0Stats>>>,
    l1_stats: Option<watch::Receiver<Option<L1Stats>>>,
    query_tx: crate::layers::l0::DbQueryHandle,
}

#[wasm_bindgen]
impl BlockExporter {
    #[wasm_bindgen(constructor)]
    pub async fn new(
        layers: Vec<String>,
        db_connect: String,
        db_run_migrations: bool,
        private_grpc_connecct: Option<String>,
        scry_no_pow: bool,
    ) -> Result<Self, JsValue> {
        Self::new_impl(
            layers,
            db_connect,
            db_run_migrations,
            private_grpc_connecct,
            scry_no_pow,
        )
        .await
        .map_err(|e| JsValue::from(e.to_string()))
    }

    async fn new_impl(
        layers: Vec<String>,
        db_connect: String,
        db_run_migrations: bool,
        private_grpc_connecct: Option<String>,
        scry_no_pow: bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        log::info!("Starting block exporter");

        let mut enable_l0 = false;
        let mut enable_l1 = false;

        for layer in layers {
            match &*layer {
                "l0" => enable_l0 = true,
                "l1" => enable_l1 = true,
                _ => return Err("Invalid layer".into()),
            }
        }

        if !enable_l0 {
            return Err("l0 is required".into());
        }

        let scry = if let Some(connect) = private_grpc_connecct {
            Some(NockAppServiceClient::new(Client::new(connect)))
        } else {
            None
        };

        let mut conn = db::new_conn(&db_connect).await?;

        if db_run_migrations {
            db::run_migrations(&mut conn).await;
        }

        let activations = ChainActivations::mainnet();

        let (l1_client, l1_stats) = if enable_l1 {
            let l1_client = L1Client::new(activations.clone(), vec![]);
            let l1_stats = l1_client.stats_handle();
            (Some(l1_client), Some(l1_stats))
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
        cfg.block_range_config.block_range_scry_no_pow = scry_no_pow;
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

    #[wasm_bindgen]
    pub async fn query(&self, sql: String) -> Result<String, JsValue> {
        let res = self
            .query_tx
            .query(sql)
            .await
            .map_err(|e| JsValue::from_str(&e))?;
        Ok(serde_json::to_string(&res).map_err(|e| JsValue::from_str(&e.to_string()))?)
    }
}
