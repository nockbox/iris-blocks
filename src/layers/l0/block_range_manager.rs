use crate::scry::{ScryError, Scryable};
use clap::Parser;
use futures::future::{BoxFuture, FutureExt};
use iris_nockchain_types::{BlockHeight, Page, Tx};
use iris_ztd::{Digest, ZMap};
use log::*;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Poll, Waker};

#[derive(Debug, Copy, Clone, Parser)]
pub struct BlockRangeConfig {
    #[arg(long, default_value_t = 10)]
    pub block_range_step: u64,
    #[arg(long, default_value_t = 3)]
    pub block_range_peek_ahead: u64,
    #[arg(long, default_value_t = false)]
    pub block_range_scry_no_pow: bool,
}

pub type ScryBlocksResult =
    Result<Option<Option<Vec<(BlockHeight, Digest, Page, ZMap<Digest, Tx>)>>>, ScryError>;

enum Prefetch {
    Pending(BoxFuture<'static, ScryBlocksResult>),
    Ready(ScryBlocksResult),
}

struct SharedState {
    tasks: std::collections::BTreeMap<u64, Prefetch>,
    driver_waker: Option<Waker>,
}

#[derive(Clone)]
pub struct BlockRangeManager<S> {
    config: BlockRangeConfig,
    shared: Arc<Mutex<SharedState>>,
    client: S,
}

impl<S: Scryable> BlockRangeManager<S> {
    pub fn new(client: S, config: BlockRangeConfig) -> Self {
        let shared = Arc::new(Mutex::new(SharedState {
            tasks: std::collections::BTreeMap::new(),
            driver_waker: None,
        }));

        let shared_clone = shared.clone();
        tokio::spawn(futures::future::poll_fn(move |cx| {
            let mut state = shared_clone.lock().unwrap();
            state.driver_waker = Some(cx.waker().clone());

            for (bh, prefetch) in state.tasks.iter_mut() {
                let res = if let Prefetch::Pending(fut) = prefetch {
                    if let Poll::Ready(r) = fut.as_mut().poll(cx) {
                        trace!("Fetched height {bh}");
                        Some(r)
                    } else {
                        None
                    }
                } else {
                    None
                };

                if let Some(r) = res {
                    *prefetch = Prefetch::Ready(r);
                }
            }

            Poll::Pending::<()>
        }));

        Self {
            config,
            shared,
            client,
        }
    }

    #[tracing::instrument(skip_all)]
    pub async fn scry_blocks(&mut self, next_height_start: u64) -> ScryBlocksResult {
        let valid_starts: Vec<u64> = (1..=self.config.block_range_peek_ahead)
            .map(|i| next_height_start + i * self.config.block_range_step)
            .collect();

        let scry_root = if self.config.block_range_scry_no_pow {
            "heaviest-chain-blocks-range-no-pow"
        } else {
            "heaviest-chain-blocks-range"
        };

        trace!("scry_root={scry_root}");

        let target_prefetch = {
            let mut state = self.shared.lock().unwrap();
            let mut dirty = false;

            let target = state.tasks.remove(&next_height_start);

            state.tasks.retain(|k, _| {
                let valid = valid_starts.contains(k);
                if !valid {
                    trace!("Aborting scry on {k}");
                    dirty = true;
                }
                valid
            });

            for &start in &valid_starts {
                if let std::collections::btree_map::Entry::Vacant(e) = state.tasks.entry(start) {
                    let mut c = self.client.clone();
                    let end = start + self.config.block_range_step - 1;
                    let fut =
                        async move { c.remote_scry((scry_root, start, end, ())).await }.boxed();
                    e.insert(Prefetch::Pending(fut));
                    dirty = true;
                }
            }

            if dirty {
                if let Some(w) = state.driver_waker.take() {
                    w.wake();
                }
            }

            target
        };

        match target_prefetch {
            Some(Prefetch::Ready(res)) => res,
            Some(Prefetch::Pending(fut)) => fut.await,
            None => {
                let end = next_height_start + self.config.block_range_step - 1;
                self.client
                    .remote_scry((scry_root, next_height_start, end, ()))
                    .await
            }
        }
    }
}
