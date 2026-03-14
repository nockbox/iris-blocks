use super::shared_schema;
use crate::rt::{RtBound, RtSync};
use core::future::Future;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use iris_ztd::Digest;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;

#[derive(Debug, Error)]
pub enum LayerErrorSource {
    #[error(transparent)]
    DieselError(#[from] diesel::result::Error),
    #[error(transparent)]
    TonicError(#[from] tonic::Status),
    #[error("Other error: {0}")]
    OtherError(String),
    #[error(transparent)]
    Layer(Box<LayerError>),
    #[error("Noun decode error block {0} digest {1}")]
    NounDecode(u32, Digest),
    #[error("Noun cue error on block {0} digest {1}")]
    NounCue(u32, Digest),
}

impl From<LayerError> for LayerErrorSource {
    fn from(e: LayerError) -> Self {
        Self::Layer(Box::new(e))
    }
}

#[derive(Debug, Error)]
pub struct LayerError {
    layer: &'static str,
    #[source]
    source: LayerErrorSource,
}

impl core::fmt::Display for LayerError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Layer '{}' error: {}", self.layer, self.source)
    }
}

pub trait LayerBase {
    const ACCEPT_LAYERS: &'static [&'static str];
    const LAYER: &'static str;
    type Stats;
    fn stats_handle(&self) -> watch::Receiver<Option<Self::Stats>>;
}

pub trait Layer {
    fn accepts_layers(&self) -> &'static [&'static str];
    fn layer(&self) -> &'static str;
}

impl<T: ?Sized + LayerBase> Layer for T {
    fn accepts_layers(&self) -> &'static [&'static str] {
        Self::ACCEPT_LAYERS
    }
    fn layer(&self) -> &'static str {
        Self::LAYER
    }
}

pub struct VerifyDependenciesError(&'static str, Vec<&'static str>);

impl core::fmt::Display for VerifyDependenciesError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "The following dependents don't accept {} layer: {}",
            self.0,
            self.1.join(", ")
        )
    }
}

impl core::fmt::Debug for VerifyDependenciesError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self}")
    }
}

impl std::error::Error for VerifyDependenciesError {}

pub trait LayerExt: LayerBase {
    fn verify_dependencies(
        deps: &[Arc<dyn LayerDependency>],
    ) -> Result<
        (
            watch::Sender<Option<Self::Stats>>,
            watch::Receiver<Option<Self::Stats>>,
        ),
        VerifyDependenciesError,
    > {
        let mismatch = deps
            .iter()
            .filter(|dep| !dep.accepts_layers().contains(&Self::LAYER))
            .map(|dep| dep.layer())
            .collect::<Vec<_>>();
        if mismatch.is_empty() {
            Ok(watch::channel(None))
        } else {
            Err(VerifyDependenciesError(Self::LAYER, mismatch))
        }
    }

    #[allow(async_fn_in_trait)]
    async fn layer_metadata(
        conn: &mut crate::db::AsyncDbConnection,
    ) -> Result<Option<shared_schema::FixedLayerMetadata>, diesel::result::Error> {
        let metadata = shared_schema::layer_metadata::table
            .select(shared_schema::LayerMetadata::as_select())
            .filter(shared_schema::layer_metadata::layer.eq(Self::LAYER))
            .load::<shared_schema::LayerMetadata>(conn)
            .await?
            .pop();
        Ok(metadata.map(|v| shared_schema::FixedLayerMetadata {
            layer: Self::LAYER,
            next_block_height: v.next_block_height,
        }))
    }

    fn update_layer_metadata(
        metadata: &shared_schema::FixedLayerMetadata,
    ) -> impl diesel::query_builder::AsQuery
           + diesel::query_builder::QueryId
           + diesel::query_builder::QueryFragment<crate::db::Db>
           + diesel::query_dsl::methods::ExecuteDsl<crate::db::DbConnection>
           + diesel_async::RunQueryDsl<crate::db::AsyncDbConnection> {
        diesel::insert_into(shared_schema::layer_metadata::table)
            .values(metadata)
            .on_conflict(shared_schema::layer_metadata::layer)
            .do_update()
            .set(metadata)
    }
}

impl<T: ?Sized + LayerBase> LayerExt for T {}

pub trait LayerImpl: Layer {
    fn expire_blocks_impl<'a>(
        &'a self,
        conn: &'a mut crate::db::AsyncDbConnection,
        metadata: shared_schema::FixedLayerMetadata,
    ) -> impl Future<Output = Result<(), LayerErrorSource>> + RtBound + 'a;
    fn update_blocks_impl<'a>(
        &'a self,
        conn: &'a mut crate::db::AsyncDbConnection,
        metadata: shared_schema::FixedLayerMetadata,
    ) -> impl Future<Output = Result<bool, LayerErrorSource>> + RtBound + 'a;
}

#[cfg_attr(feature = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(feature = "wasm"), async_trait::async_trait)]
pub trait LayerDependency: Layer + RtBound + RtSync {
    async fn expire_blocks(
        &self,
        conn: &mut crate::db::AsyncDbConnection,
        metadata: shared_schema::FixedLayerMetadata,
    ) -> Result<(), LayerError>;
    async fn update_blocks(
        &self,
        conn: &mut crate::db::AsyncDbConnection,
        metadata: shared_schema::FixedLayerMetadata,
    ) -> Result<bool, LayerError>;
}

#[cfg_attr(feature = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(feature = "wasm"), async_trait::async_trait)]
impl<T: ?Sized + LayerImpl + RtBound + RtSync> LayerDependency for T {
    #[tracing::instrument(skip_all)]
    async fn expire_blocks(
        &self,
        conn: &mut crate::db::AsyncDbConnection,
        metadata: shared_schema::FixedLayerMetadata,
    ) -> Result<(), LayerError> {
        self.expire_blocks_impl(conn, metadata)
            .await
            .map_err(|e| LayerError {
                layer: self.layer(),
                source: e,
            })
    }
    #[tracing::instrument(skip_all)]
    async fn update_blocks(
        &self,
        conn: &mut crate::db::AsyncDbConnection,
        metadata: shared_schema::FixedLayerMetadata,
    ) -> Result<bool, LayerError> {
        self.update_blocks_impl(conn, metadata)
            .await
            .map_err(|e| LayerError {
                layer: self.layer(),
                source: e,
            })
    }
}
