use crate::preprocessors::ordered_queue::OrderedBlockQueue;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tokio::{select, time};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

/// Default interval used by the metrics reporter task.
pub const DEFAULT_METRICS_INTERVAL: Duration = Duration::from_secs(5);

static TRACING_INIT: OnceLock<()> = OnceLock::new();

/// Installs a basic tracing subscriber (if one is not already active).
///
/// The subscriber honours `RUST_LOG` if it is present, otherwise it falls back to `info`.
/// Calling this function multiple times is harmless.
pub fn init_tracing() {
    if TRACING_INIT.get().is_some() {
        return;
    }

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(true)
        .try_init();

    let _ = TRACING_INIT.set(());
}

/// Lightweight rolling counters used to derive runtime metrics.
#[derive(Default, Debug)]
pub struct Telemetry {
    processed_blocks: AtomicU64,
    rpc_errors: AtomicU64,
    rpc_timeouts: AtomicU64,
    worker_pool_transitions: AtomicU64,
    worker_pool_size: AtomicUsize,
    payload_splits: AtomicU64,
}

impl Telemetry {
    pub fn record_processed_blocks(&self, count: u64) {
        if count == 0 {
            return;
        }
        self.processed_blocks.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_rpc_error(&self) {
        self.rpc_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_rpc_timeout(&self) {
        self.rpc_timeouts.fetch_add(1, Ordering::Relaxed);
        self.rpc_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> TelemetrySnapshot {
        TelemetrySnapshot {
            processed_blocks: self.processed_blocks.load(Ordering::Relaxed),
            rpc_errors: self.rpc_errors.load(Ordering::Relaxed),
            rpc_timeouts: self.rpc_timeouts.load(Ordering::Relaxed),
            payload_splits: self.payload_splits.load(Ordering::Relaxed),
        }
    }

    pub fn processed_blocks(&self) -> u64 {
        self.processed_blocks.load(Ordering::Relaxed)
    }

    pub fn rpc_errors(&self) -> u64 {
        self.rpc_errors.load(Ordering::Relaxed)
    }

    pub fn rpc_timeouts(&self) -> u64 {
        self.rpc_timeouts.load(Ordering::Relaxed)
    }

    pub fn record_worker_pool_size(&self, workers: usize) {
        self.worker_pool_size.store(workers, Ordering::Relaxed);
        self.worker_pool_transitions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_payload_split(&self) {
        self.payload_splits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn worker_pool_size(&self) -> usize {
        self.worker_pool_size.load(Ordering::Relaxed)
    }

    pub fn worker_pool_transitions(&self) -> u64 {
        self.worker_pool_transitions.load(Ordering::Relaxed)
    }

    pub fn payload_splits(&self) -> u64 {
        self.payload_splits.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct TelemetrySnapshot {
    pub processed_blocks: u64,
    pub rpc_errors: u64,
    pub rpc_timeouts: u64,
    pub payload_splits: u64,
}

/// Spawns a background task that periodically logs throughput, queue blocks/bytes, and RPC errors.
pub fn spawn_metrics_reporter<T: Send + 'static>(
    telemetry: Arc<Telemetry>,
    queue: Arc<OrderedBlockQueue<T>>,
    shutdown: CancellationToken,
    interval: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut last_snapshot = telemetry.snapshot();
        let mut last_tick = Instant::now();

        loop {
            select! {
                _ = shutdown.cancelled() => {
                    tracing::info!(target: "protoblock::metrics", "metrics reporter shutting down");
                    break;
                }
                _ = ticker.tick() => {
                    let current_snapshot = telemetry.snapshot();
                    let processed_delta = current_snapshot
                        .processed_blocks
                        .saturating_sub(last_snapshot.processed_blocks);
                    let elapsed = last_tick.elapsed().as_secs_f64();
                    let throughput = if elapsed <= f64::EPSILON {
                        0.0
                    } else {
                        processed_delta as f64 / elapsed
                    };
                    let queue_blocks = queue.len().await;
                    let queue_bytes = queue.bytes().await;

                    tracing::info!(
                        target: "protoblock::metrics",
                        throughput = format!("{throughput:.2}"),
                        processed = current_snapshot.processed_blocks,
                        queue_blocks,
                        queue_bytes,
                        rpc_errors = current_snapshot.rpc_errors,
                        rpc_timeouts = current_snapshot.rpc_timeouts,
                        payload_splits = current_snapshot.payload_splits,
                        "runtime metrics snapshot"
                    );

                    last_snapshot = current_snapshot;
                    last_tick = Instant::now();
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::preprocessors::block::PreProcessedBlock;
    use bitcoin::hashes::Hash;
    use bitcoin::BlockHash;
    use tokio::time::timeout;

    fn dummy_hash(seed: u8) -> BlockHash {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        BlockHash::from_slice(&bytes).expect("valid hash")
    }

    #[tokio::test]
    async fn telemetry_records_counters() {
        let telemetry = Telemetry::default();
        telemetry.record_processed_blocks(3);
        telemetry.record_rpc_error();
        telemetry.record_rpc_timeout();
        assert_eq!(telemetry.worker_pool_size(), 0);
        assert_eq!(telemetry.worker_pool_transitions(), 0);
        telemetry.record_worker_pool_size(4);
        telemetry.record_worker_pool_size(1);
        telemetry.record_payload_split();

        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.processed_blocks, 3);
        assert_eq!(snapshot.rpc_errors, 2);
        assert_eq!(snapshot.rpc_timeouts, 1);
        assert_eq!(snapshot.payload_splits, 1);
        assert_eq!(telemetry.worker_pool_size(), 1);
        assert_eq!(telemetry.worker_pool_transitions(), 2);
    }

    #[tokio::test]
    async fn metrics_reporter_logs_until_shutdown() {
        let telemetry = Arc::new(Telemetry::default());
        telemetry.record_processed_blocks(10);
        let queue = Arc::new(OrderedBlockQueue::new());
        queue.reset_expected(0).await;
        queue
            .push(
                PreProcessedBlock::new(0, dummy_hash(1), dummy_hash(0), ()),
                1,
            )
            .await;

        let shutdown = CancellationToken::new();
        let handle = spawn_metrics_reporter(
            telemetry,
            queue,
            shutdown.clone(),
            Duration::from_millis(10),
        );

        shutdown.cancel();
        timeout(Duration::from_secs(1), handle)
            .await
            .expect("reporter should stop promptly")
            .expect("task should not panic");
    }
}
