//! Tip tracking and tip-wait coordination for `BlocksFetcher`.

use super::fetcher::ProcessingHandles;
use super::worker_pool::{broadcast_control_signal, start_workers_subset};
use crate::preprocessors::worker::{FetcherEvent, FetcherEventSender, WorkerControl};
use crate::rpc::AsyncRpcClient;
use crate::runtime::protocol::BlockProtocol;
use anyhow::Result;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

/// Tracks the latest blockchain tip height that workers should respect before
/// issuing RPC requests. The value is refreshed periodically by a dedicated
/// task so workers can avoid requesting blocks that do not exist yet.
#[derive(Debug)]
pub struct BlockchainTip {
    value: AtomicU64,
    ready: AtomicBool,
}

impl BlockchainTip {
    pub fn new(initial: Option<u64>) -> Self {
        Self {
            value: AtomicU64::new(initial.unwrap_or(0)),
            ready: AtomicBool::new(initial.is_some()),
        }
    }

    pub fn update(&self, height: u64) {
        self.value.store(height, Ordering::SeqCst);
        self.ready.store(true, Ordering::SeqCst);
    }

    pub fn current(&self) -> Option<u64> {
        if self.ready.load(Ordering::SeqCst) {
            Some(self.value.load(Ordering::SeqCst))
        } else {
            None
        }
    }
}

pub(crate) struct TipTracker;

impl TipTracker {
    pub(crate) fn spawn_refresh_loop(
        rpc_client: Arc<AsyncRpcClient>,
        blockchain_tip: Arc<BlockchainTip>,
        refresh_interval: Duration,
        shutdown: CancellationToken,
        event_tx: FetcherEventSender,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(refresh_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut last_tip = blockchain_tip.current();

            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        break;
                    }
                    _ = ticker.tick() => {
                        match rpc_client.get_blockchain_tip().await {
                            Ok(tip) => {
                                let previous = last_tip;
                                blockchain_tip.update(tip);
                                if let Some(prev) = previous {
                                    if tip < prev {
                                        if let Err(err) = event_tx.send(FetcherEvent::TipRegressed { new_tip: tip }).await {
                                            tracing::warn!(error = %err, new_tip = tip, previous_tip = prev, "failed to publish tip regression event");
                                        }
                                    }
                                }
                                last_tip = Some(tip);
                            }
                            Err(err) => {
                                tracing::warn!(error = %err, "failed to refresh blockchain tip");
                            }
                        }
                    }
                }
            }

            tracing::info!("blockchain tip refresher stopped");
        })
    }

    pub(crate) async fn maybe_adjust_workers_for_tip<P: BlockProtocol>(
        handles: &ProcessingHandles<P>,
        next_expected: u64,
    ) -> Result<()> {
        let Some(tip) = handles.blockchain_tip.current() else {
            return Ok(());
        };

        let processed_height = next_expected.saturating_sub(1);
        let pending_blocks = handles.queue.len().await;
        let pending_bytes = handles.queue.bytes().await;
        if pending_blocks == 0 && processed_height >= tip {
            return Self::enter_tip_wait_mode(
                handles,
                next_expected,
                pending_blocks,
                pending_bytes,
            )
            .await;
        }

        Ok(())
    }

    /// Tip-wait mode intentionally sticks until the fetcher restarts. Running a
    /// single worker near the chain tip avoids churn and is documented in
    /// `docs/architecture.md`; if stronger expansion heuristics are
    /// needed they should be implemented explicitly rather than silently
    /// undoing this downshift.
    async fn enter_tip_wait_mode<P: BlockProtocol>(
        handles: &ProcessingHandles<P>,
        next_expected: u64,
        pending_blocks: usize,
        pending_bytes: usize,
    ) -> Result<()> {
        let senders = handles.control_channels();
        let activity = &handles.activity;
        let tip_wait_mode = &handles.tip_wait_mode;
        let active_workers = &handles.active_workers;
        let telemetry = &handles.telemetry;

        if tip_wait_mode.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        tracing::info!(
            next_expected,
            queue_blocks = pending_blocks,
            queue_bytes = pending_bytes,
            "caught up to blockchain tip; reducing worker concurrency"
        );
        broadcast_control_signal(senders, WorkerControl::Stop).await;
        activity.wait_until_idle().await;
        active_workers.store(1, Ordering::SeqCst);
        telemetry.record_worker_pool_size(1);
        start_workers_subset(senders, next_expected, Some(1)).await
    }
}
