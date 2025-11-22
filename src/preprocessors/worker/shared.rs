use crate::preprocessors::ordered_queue::OrderedBlockQueue;
use crate::processor::tip::BlockchainTip;
use crate::rpc::payload::{RpcPayloadLimits, RpcPayloadStats};
use crate::runtime::protocol::BlockProtocol;
use crate::runtime::telemetry::Telemetry;
use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};

use super::types::FetcherEventSender;

pub struct WorkerShared<P: BlockProtocol> {
    pub(super) protocol: Arc<RwLock<P>>,
    pub(super) queue: Arc<OrderedBlockQueue<P::PreProcessed>>,
    pub(super) telemetry: Arc<Telemetry>,
    pub(super) activity: Arc<WorkerActivityTracker>,
    pub(super) generation: Arc<AtomicU64>,
    pub(super) blockchain_tip: Arc<BlockchainTip>,
    pub(super) active_workers: Arc<AtomicUsize>,
    pub(super) event_tx: FetcherEventSender,
    pub(super) max_batch_size_mb: usize,
    pub(super) payload_limits: RpcPayloadLimits,
    pub(super) payload_stats: Arc<RpcPayloadStats>,
}

pub struct WorkerSharedParams<P: BlockProtocol> {
    pub protocol: Arc<RwLock<P>>,
    pub queue: Arc<OrderedBlockQueue<P::PreProcessed>>,
    pub telemetry: Arc<Telemetry>,
    pub activity: Arc<WorkerActivityTracker>,
    pub generation: Arc<AtomicU64>,
    pub blockchain_tip: Arc<BlockchainTip>,
    pub active_workers: Arc<AtomicUsize>,
    pub event_tx: FetcherEventSender,
    pub max_batch_size_mb: usize,
    pub payload_limits: RpcPayloadLimits,
    pub payload_stats: Arc<RpcPayloadStats>,
}

impl<P: BlockProtocol> WorkerShared<P> {
    pub fn new(params: WorkerSharedParams<P>) -> Self {
        Self {
            protocol: params.protocol,
            queue: params.queue,
            telemetry: params.telemetry,
            activity: params.activity,
            generation: params.generation,
            blockchain_tip: params.blockchain_tip,
            active_workers: params.active_workers,
            event_tx: params.event_tx,
            max_batch_size_mb: params.max_batch_size_mb,
            payload_limits: params.payload_limits,
            payload_stats: params.payload_stats,
        }
    }
}

impl<P: BlockProtocol> Clone for WorkerShared<P> {
    fn clone(&self) -> Self {
        Self {
            protocol: Arc::clone(&self.protocol),
            queue: Arc::clone(&self.queue),
            telemetry: Arc::clone(&self.telemetry),
            activity: Arc::clone(&self.activity),
            generation: Arc::clone(&self.generation),
            blockchain_tip: Arc::clone(&self.blockchain_tip),
            active_workers: Arc::clone(&self.active_workers),
            event_tx: self.event_tx.clone(),
            max_batch_size_mb: self.max_batch_size_mb,
            payload_limits: self.payload_limits,
            payload_stats: Arc::clone(&self.payload_stats),
        }
    }
}

#[derive(Debug, Default)]
pub struct WorkerActivityTracker {
    active_batches: AtomicUsize,
    notify: Notify,
}

impl WorkerActivityTracker {
    pub fn new() -> Self {
        Self {
            active_batches: AtomicUsize::new(0),
            notify: Notify::new(),
        }
    }

    pub fn enter(self: &Arc<Self>) -> WorkerActivityGuard {
        self.active_batches.fetch_add(1, Ordering::SeqCst);
        WorkerActivityGuard {
            tracker: Arc::clone(self),
            active: true,
        }
    }

    pub async fn wait_until_idle(&self) {
        self.wait_until_idle_with(|| async {}).await;
    }

    pub async fn wait_until_idle_with<F, Fut>(&self, mut on_wait: F)
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = ()>,
    {
        loop {
            if self.active_batches.load(Ordering::SeqCst) == 0 {
                return;
            }

            let notified = self.notify.notified();
            on_wait().await;

            if self.active_batches.load(Ordering::SeqCst) == 0 {
                return;
            }
            notified.await;
        }
    }

    fn release(&self) {
        if self.active_batches.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.notify.notify_waiters();
        } else {
            self.notify.notify_one();
        }
    }
}

pub struct WorkerActivityGuard {
    tracker: Arc<WorkerActivityTracker>,
    active: bool,
}

impl Drop for WorkerActivityGuard {
    fn drop(&mut self) {
        if self.active {
            self.tracker.release();
            self.active = false;
        }
    }
}
