use std::{
    env,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{bail, Result};
use bitcoin::Block;
use once_cell::sync::Lazy;
use protoblock::{
    BlockProtocol, BlocksFetcher, ProtocolFuture, ProtocolPreProcessFuture, Telemetry,
};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing_subscriber::EnvFilter;

pub static REGTEST_GUARD: Lazy<tokio::sync::Mutex<()>> = Lazy::new(|| tokio::sync::Mutex::new(()));

static TRACING_SUBSCRIBER: Lazy<()> = Lazy::new(|| {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();
});

pub fn init_tracing() {
    Lazy::force(&TRACING_SUBSCRIBER);
}

pub fn regtest_tests_enabled() -> bool {
    match env::var("PROTOBLOCK_RUN_REGTESTS") {
        Ok(value) => {
            let normalized = value.trim().to_ascii_lowercase();
            matches!(normalized.as_str(), "1" | "true" | "yes")
        }
        Err(_) => false,
    }
}

#[derive(Default)]
pub struct RecordingProtocol {
    processed: Vec<u64>,
    rollbacks: Vec<u64>,
}

impl RecordingProtocol {
    pub fn processed(&self) -> &[u64] {
        &self.processed
    }

    pub fn rollback_points(&self) -> &[u64] {
        &self.rollbacks
    }
}

impl BlockProtocol for RecordingProtocol {
    type PreProcessed = u64;

    fn pre_process(
        &self,
        _block: Block,
        height: u64,
    ) -> ProtocolPreProcessFuture<Self::PreProcessed> {
        Box::pin(async move { Ok(height) })
    }

    fn process<'a>(&'a mut self, data: Self::PreProcessed, height: u64) -> ProtocolFuture<'a> {
        Box::pin(async move {
            assert_eq!(data, height, "pre-process pipeline must echo the height");
            self.processed.push(height);
            Ok(())
        })
    }

    fn rollback<'a>(&'a mut self, block_height: u64) -> ProtocolFuture<'a> {
        Box::pin(async move {
            self.rollbacks.push(block_height);
            self.processed.retain(|&existing| existing <= block_height);
            Ok(())
        })
    }

    fn shutdown<'a>(&'a mut self) -> ProtocolFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Default)]
pub struct SharedProtocolState {
    pub processed: Vec<u64>,
}

#[derive(Clone)]
pub struct SharedRecordingProtocol {
    state: Arc<Mutex<SharedProtocolState>>,
}

impl SharedRecordingProtocol {
    pub fn new() -> (Self, Arc<Mutex<SharedProtocolState>>) {
        let state = Arc::new(Mutex::new(SharedProtocolState::default()));
        (
            Self {
                state: state.clone(),
            },
            state,
        )
    }
}

impl BlockProtocol for SharedRecordingProtocol {
    type PreProcessed = u64;

    fn pre_process(
        &self,
        _block: Block,
        height: u64,
    ) -> ProtocolPreProcessFuture<Self::PreProcessed> {
        Box::pin(async move { Ok(height) })
    }

    fn process<'a>(&'a mut self, data: Self::PreProcessed, height: u64) -> ProtocolFuture<'a> {
        let state = self.state.clone();
        Box::pin(async move {
            assert_eq!(data, height, "pre-process pipeline must echo the height");
            let mut guard = state.lock().await;
            guard.processed.push(height);
            Ok(())
        })
    }

    fn rollback<'a>(&'a mut self, block_height: u64) -> ProtocolFuture<'a> {
        let state = self.state.clone();
        Box::pin(async move {
            let mut guard = state.lock().await;
            guard.processed.retain(|&existing| existing <= block_height);
            Ok(())
        })
    }

    fn shutdown<'a>(&'a mut self) -> ProtocolFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

pub async fn wait_for_height<P: BlockProtocol>(
    fetcher: &BlocksFetcher<P>,
    target: u64,
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();
    loop {
        let current = fetcher.last_confirmed_height();
        if let Some(height) = current {
            if height >= target {
                return Ok(());
            }
        }
        if start.elapsed() > timeout {
            let reported = current
                .map(|height| height.to_string())
                .unwrap_or_else(|| "<none>".to_owned());
            bail!(
                "fetcher did not reach height {target} within {:?} (last confirmed: {reported})",
                timeout
            );
        }
        sleep(Duration::from_millis(50)).await;
    }
}

pub async fn wait_for_height_at_most<P: BlockProtocol>(
    fetcher: &BlocksFetcher<P>,
    target: u64,
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();
    loop {
        if let Some(height) = fetcher.last_confirmed_height() {
            if height <= target {
                return Ok(());
            }
        }

        if start.elapsed() > timeout {
            bail!(
                "fetcher did not roll back to <= {target} within {:?}",
                timeout
            );
        }

        sleep(Duration::from_millis(50)).await;
    }
}

pub async fn wait_for_worker_pool_size(
    telemetry: &Arc<Telemetry>,
    expected: usize,
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();
    loop {
        let current = telemetry.worker_pool_size();
        if current == expected {
            return Ok(());
        }

        if start.elapsed() > timeout {
            bail!(
                "worker pool did not reach size {expected} within {:?} (size: {current}, transitions: {})",
                timeout,
                telemetry.worker_pool_transitions()
            );
        }

        sleep(Duration::from_millis(50)).await;
    }
}

pub async fn wait_for_processed_len(
    state: &Arc<Mutex<SharedProtocolState>>,
    target: usize,
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();
    loop {
        {
            let guard = state.lock().await;
            if guard.processed.len() >= target {
                return Ok(());
            }
        }

        if start.elapsed() > timeout {
            bail!(
                "protocol did not record {target} blocks within {:?}",
                timeout
            );
        }

        sleep(Duration::from_millis(50)).await;
    }
}

pub fn assert_is_contiguous(heights: &[u64]) {
    for window in heights.windows(2) {
        if let [lhs, rhs] = window {
            assert_eq!(rhs, &(lhs + 1), "heights must increase monotonically");
        }
    }
}
