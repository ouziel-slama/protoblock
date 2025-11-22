//! Worker pool orchestration for `BlocksFetcher`.
//!
//! This module owns worker creation, control fan-out, pool sizing helpers, and
//! the signaling surface used by reorg/tip-handling code.

use crate::preprocessors::ordered_queue::OrderedBlockQueue;
use crate::preprocessors::worker::{
    worker_control_channel, FetcherEventSender, Worker, WorkerActivityTracker, WorkerControl,
    WorkerControlSender, WorkerShared, WorkerSharedParams,
};
use crate::processor::tip::BlockchainTip;
use crate::rpc::circuit_breaker::RpcCircuitBreaker;
use crate::rpc::payload::{RpcPayloadLimits, RpcPayloadStats};
use crate::runtime::config::FetcherConfig;
use crate::runtime::fatal::FatalErrorHandler;
use crate::runtime::protocol::BlockProtocol;
use crate::runtime::telemetry::Telemetry;
use anyhow::{Context, Result};
use futures::FutureExt;
use std::{
    any::Any,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::{watch, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub(crate) struct WorkerPool<P: BlockProtocol> {
    max_workers: usize,
    max_batch_size_mb: usize,
    protocol: Arc<RwLock<P>>,
    queue: Arc<OrderedBlockQueue<P::PreProcessed>>,
    telemetry: Arc<Telemetry>,
    generation: Arc<AtomicU64>,
    blockchain_tip: Arc<BlockchainTip>,
    activity: Arc<WorkerActivityTracker>,
    active_workers: Arc<AtomicUsize>,
    tip_wait_mode: Arc<AtomicBool>,
    workers: Vec<JoinHandle<()>>,
    control_txs: Vec<WorkerControlSender>,
    workers_done_tx: Option<watch::Sender<bool>>,
    payload_limits: RpcPayloadLimits,
    payload_stats: Arc<RpcPayloadStats>,
}

pub(crate) struct WorkerPoolParams<P: BlockProtocol> {
    pub max_workers: usize,
    pub max_batch_size_mb: usize,
    pub protocol: Arc<RwLock<P>>,
    pub queue: Arc<OrderedBlockQueue<P::PreProcessed>>,
    pub telemetry: Arc<Telemetry>,
    pub generation: Arc<AtomicU64>,
    pub blockchain_tip: Arc<BlockchainTip>,
    pub payload_limits: RpcPayloadLimits,
}

impl<P: BlockProtocol> WorkerPool<P> {
    pub(crate) fn new(params: WorkerPoolParams<P>) -> Self {
        let max_workers = params.max_workers.max(1);
        Self {
            max_workers,
            max_batch_size_mb: params.max_batch_size_mb,
            protocol: params.protocol,
            queue: params.queue,
            telemetry: params.telemetry,
            generation: params.generation,
            blockchain_tip: params.blockchain_tip,
            activity: Arc::new(WorkerActivityTracker::new()),
            active_workers: Arc::new(AtomicUsize::new(max_workers)),
            tip_wait_mode: Arc::new(AtomicBool::new(false)),
            workers: Vec::new(),
            control_txs: Vec::new(),
            workers_done_tx: None,
            payload_limits: params.payload_limits,
            payload_stats: Arc::new(RpcPayloadStats::default()),
        }
    }

    pub(crate) fn handles(&self) -> &Vec<JoinHandle<()>> {
        &self.workers
    }

    pub(crate) fn control_channels(&self) -> &[WorkerControlSender] {
        &self.control_txs
    }

    pub(crate) fn activity(&self) -> Arc<WorkerActivityTracker> {
        self.activity.clone()
    }

    pub(crate) fn active_workers(&self) -> Arc<AtomicUsize> {
        self.active_workers.clone()
    }

    pub(crate) fn tip_wait_mode(&self) -> Arc<AtomicBool> {
        self.tip_wait_mode.clone()
    }

    pub(crate) fn max_workers(&self) -> usize {
        self.max_workers
    }

    pub(crate) fn configure_initial_state(&self, workers: usize, tip_wait: bool) {
        self.tip_wait_mode.store(tip_wait, Ordering::SeqCst);
        self.update_active_workers(workers);
    }

    fn update_active_workers(&self, workers: usize) {
        let clamped = workers.max(1).min(self.max_workers);
        self.active_workers.store(clamped, Ordering::SeqCst);
        self.telemetry.record_worker_pool_size(clamped);
    }

    pub(crate) fn launch(
        &mut self,
        config: &FetcherConfig,
        run_token: CancellationToken,
        root_shutdown: CancellationToken,
        fatal_handler: Arc<FatalErrorHandler>,
        breaker: Arc<RpcCircuitBreaker>,
        event_tx: FetcherEventSender,
    ) -> Result<watch::Receiver<bool>> {
        self.workers.clear();
        self.control_txs.clear();

        let (workers_done_tx, workers_done_rx) = watch::channel(false);
        self.workers_done_tx = Some(workers_done_tx.clone());

        let remaining_workers = Arc::new(AtomicUsize::new(self.max_workers));
        let shared = WorkerShared::new(WorkerSharedParams {
            protocol: self.protocol.clone(),
            queue: self.queue.clone(),
            telemetry: self.telemetry.clone(),
            activity: self.activity.clone(),
            generation: self.generation.clone(),
            blockchain_tip: self.blockchain_tip.clone(),
            active_workers: self.active_workers.clone(),
            event_tx: event_tx.clone(),
            max_batch_size_mb: self.max_batch_size_mb,
            payload_limits: self.payload_limits,
            payload_stats: self.payload_stats.clone(),
        });

        for worker_id in 0..self.max_workers {
            let (tx, rx) = worker_control_channel(8);
            let worker = Worker::new(
                worker_id,
                config,
                breaker.clone(),
                rx,
                run_token.clone(),
                fatal_handler.clone(),
                shared.clone(),
            )?;

            let workers_done = workers_done_tx.clone();
            let remaining_workers = remaining_workers.clone();
            let worker_shutdown = run_token.clone();
            let root_shutdown = root_shutdown.clone();
            let fatal_handler = fatal_handler.clone();

            let handle = tokio::spawn(async move {
                let result = std::panic::AssertUnwindSafe(worker.run())
                    .catch_unwind()
                    .await;

                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        tracing::error!(
                            worker = worker_id,
                            error = %err,
                            "worker task exited with error"
                        );
                        let context = format!("worker {worker_id} exited with error");
                        let err = err.context(context.clone());
                        fatal_handler.trigger_external(context.as_str(), err);
                        worker_shutdown.cancel();
                        root_shutdown.cancel();
                    }
                    Err(panic_payload) => {
                        let panic_msg = panic_message(panic_payload.as_ref());
                        tracing::error!(
                            worker = worker_id,
                            panic = %panic_msg,
                            "worker task panicked"
                        );
                        let context = format!("worker {worker_id} panicked");
                        let panic_error =
                            anyhow::anyhow!("worker {worker_id} panicked: {panic_msg}");
                        fatal_handler.trigger_external(context.as_str(), panic_error);
                        worker_shutdown.cancel();
                        root_shutdown.cancel();
                    }
                }

                if remaining_workers.fetch_sub(1, Ordering::SeqCst) == 1 {
                    let _ = workers_done.send(true);
                }
            });

            self.control_txs.push(tx);
            self.workers.push(handle);
        }

        if self.workers.is_empty() {
            let _ = workers_done_tx.send(true);
        }

        Ok(workers_done_rx)
    }

    pub(crate) async fn start_at(&self, base_height: u64) -> Result<()> {
        let desired = self.active_workers.load(Ordering::SeqCst).max(1);
        let limit = desired.min(self.control_txs.len());
        self.start_subset(base_height, Some(limit)).await
    }

    pub(crate) async fn shutdown(&mut self) -> Vec<JoinHandle<()>> {
        self.broadcast_control(WorkerControl::Shutdown).await;
        self.control_txs.clear();
        std::mem::take(&mut self.workers)
    }

    pub(crate) fn notify_all_workers_done(&mut self) {
        if let Some(tx) = self.workers_done_tx.take() {
            let _ = tx.send(true);
        }
    }

    async fn start_subset(&self, base_height: u64, limit: Option<usize>) -> Result<()> {
        start_workers_subset(&self.control_txs, base_height, limit).await
    }

    async fn broadcast_control(&self, message: WorkerControl) {
        broadcast_control_signal(&self.control_txs, message).await;
    }

    pub(crate) fn determine_initial_state(
        start_height: u64,
        tip: Option<u64>,
        max_workers: usize,
    ) -> (usize, bool) {
        let max_workers = max_workers.max(1);
        if max_workers == 1 {
            return (1, false);
        }

        if let Some(tip_height) = tip {
            if tip_height < start_height {
                return (1, true);
            }
            let available = tip_height.saturating_sub(start_height).saturating_add(1);
            let threshold = (max_workers as u64).saturating_mul(3);
            if available < threshold {
                return (1, true);
            }
        }

        (max_workers, false)
    }
}

fn panic_message(panic: &(dyn Any + Send)) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

pub(crate) async fn broadcast_control_signal(
    senders: &[WorkerControlSender],
    message: WorkerControl,
) {
    for (worker_id, tx) in senders.iter().enumerate() {
        if let Err(err) = tx.send(message.clone()).await {
            tracing::warn!(
                worker = worker_id,
                error = %err,
                "failed to deliver control signal to worker"
            );
        }
    }
}

pub(crate) async fn start_workers_subset(
    senders: &[WorkerControlSender],
    base_height: u64,
    limit: Option<usize>,
) -> Result<()> {
    let total = limit.unwrap_or(senders.len()).min(senders.len());
    for (worker_id, tx) in senders.iter().enumerate().take(total) {
        let start_height = base_height.saturating_add(worker_id as u64);
        tx.send(WorkerControl::Start(start_height))
            .await
            .with_context(|| {
                format!("failed to start worker {worker_id} at height {start_height}")
            })?;
    }
    Ok(())
}
