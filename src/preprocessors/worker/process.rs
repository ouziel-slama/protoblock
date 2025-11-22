use crate::preprocessors::batch::BatchSizer;
use crate::preprocessors::block::PreProcessedBlock;
use crate::preprocessors::ordered_queue::OrderedBlockQueue;
use crate::processor::tip::BlockchainTip;
use crate::rpc::circuit_breaker::RpcCircuitBreaker;
use crate::rpc::payload::{RpcPayloadLimits, RpcPayloadSample, RpcPayloadStats};
use crate::rpc::{extract_hashes, hex_to_block, AsyncRpcClient, BlockBatchClient};
use crate::runtime::config::FetcherConfig;
use crate::runtime::fatal::FatalErrorHandler;
use crate::runtime::hooks::HookDecision;
use crate::runtime::protocol::BlockProtocol;
use crate::runtime::telemetry::Telemetry;
use anyhow::Result;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

use super::shared::{WorkerActivityTracker, WorkerShared};
use super::types::{
    FetchWaitOutcome, FetcherEventSender, PendingBatch, PlanResult, ProcessingOutcome, ReadyBatch,
    WorkerControlReceiver,
};

pub struct Worker<P: BlockProtocol> {
    pub id: usize,
    pub(super) rpc_client: Arc<dyn BlockBatchClient>,
    pub(super) protocol: Arc<RwLock<P>>,
    pub(super) control_rx: WorkerControlReceiver,
    pub(super) queue: Arc<OrderedBlockQueue<P::PreProcessed>>,
    pub(super) batch_sizer: BatchSizer,
    pub(super) shutdown: CancellationToken,
    pub(super) fatal_handler: Arc<FatalErrorHandler>,
    pub(super) telemetry: Arc<Telemetry>,
    pub(super) activity: Arc<WorkerActivityTracker>,
    pub(super) generation: Arc<AtomicU64>,
    pub(super) blockchain_tip: Arc<BlockchainTip>,
    pub(super) active_workers: Arc<AtomicUsize>,
    pub(super) event_tx: FetcherEventSender,
    pub(super) tip_idle_backoff: Duration,
    pub(super) rpc_payload_limits: RpcPayloadLimits,
    pub(super) rpc_payload_stats: Arc<RpcPayloadStats>,
}

impl<P: BlockProtocol> Worker<P> {
    pub fn new(
        id: usize,
        config: &FetcherConfig,
        breaker: Arc<RpcCircuitBreaker>,
        control_rx: WorkerControlReceiver,
        shutdown: CancellationToken,
        fatal_handler: Arc<FatalErrorHandler>,
        shared: WorkerShared<P>,
    ) -> Result<Self> {
        let rpc_client: Arc<dyn BlockBatchClient> =
            Arc::new(AsyncRpcClient::from_config_with_breaker(config, breaker)?);
        Ok(Self::with_rpc_client(
            id,
            rpc_client,
            control_rx,
            shutdown,
            fatal_handler,
            shared,
            config.tip_idle_backoff(),
        ))
    }

    pub(crate) fn with_rpc_client(
        id: usize,
        rpc_client: Arc<dyn BlockBatchClient>,
        control_rx: WorkerControlReceiver,
        shutdown: CancellationToken,
        fatal_handler: Arc<FatalErrorHandler>,
        shared: WorkerShared<P>,
        tip_idle_backoff: Duration,
    ) -> Self {
        let WorkerShared {
            protocol,
            queue,
            telemetry,
            activity,
            generation,
            blockchain_tip,
            active_workers,
            event_tx,
            max_batch_size_mb,
            payload_limits,
            payload_stats,
        } = shared;

        Self {
            id,
            rpc_client,
            protocol,
            control_rx,
            queue,
            batch_sizer: BatchSizer::new(max_batch_size_mb),
            shutdown,
            fatal_handler,
            telemetry,
            activity,
            generation,
            blockchain_tip,
            active_workers,
            event_tx,
            tip_idle_backoff,
            rpc_payload_limits: payload_limits,
            rpc_payload_stats: payload_stats,
        }
    }

    #[tracing::instrument(name = "worker", skip_all, fields(worker = self.id))]
    pub async fn run(mut self) -> Result<()> {
        tracing::info!(worker = self.id, "worker task started");

        let mut current_height: Option<u64> = None;
        let mut consecutive_failures = 0usize;
        let shutdown = self.shutdown.clone();
        let mut pending_fetch: Option<PendingBatch> = None;
        let mut ready_batch: Option<ReadyBatch> = None;

        loop {
            if shutdown.is_cancelled() {
                tracing::info!(worker = self.id, "shutdown requested; exiting worker loop");
                if let Some(batch) = pending_fetch.take() {
                    batch.abort();
                }
                break;
            }

            if ready_batch.is_none() {
                if !self
                    .ensure_pending_fetch(&mut current_height, &mut pending_fetch, &shutdown)
                    .await?
                {
                    break;
                }

                match self
                    .wait_for_pending_fetch(
                        &mut pending_fetch,
                        &shutdown,
                        &mut consecutive_failures,
                    )
                    .await?
                {
                    FetchWaitOutcome::Ready(batch) => {
                        ready_batch = Some(batch);
                    }
                    FetchWaitOutcome::Restart(height) => {
                        current_height = Some(height);
                        continue;
                    }
                    FetchWaitOutcome::Start(height) => {
                        tracing::info!(
                            worker = self.id,
                            start = height,
                            "received start override while waiting for batch"
                        );
                        current_height = Some(height);
                        continue;
                    }
                    FetchWaitOutcome::Stop => {
                        tracing::debug!(worker = self.id, "received stop signal while fetching");
                        current_height = None;
                        continue;
                    }
                    FetchWaitOutcome::Shutdown => {
                        tracing::info!(worker = self.id, "received shutdown signal");
                        break;
                    }
                }
            }

            let Some(batch) = ready_batch.take() else {
                continue;
            };

            let next_start_height = batch.context.next_start_height();
            current_height = Some(next_start_height);

            if pending_fetch.is_none() {
                if let PlanResult::Ready(plan) = self.build_batch_plan(next_start_height) {
                    pending_fetch = Some(self.spawn_pending_batch(plan));
                }
            }

            let batch_epoch = batch.context.epoch;
            let requested = batch.context.requested;
            let start_height = batch.context.start_height;
            let blocks = batch.blocks;

            let outcome = self.process_blocks(batch_epoch, blocks).await?;

            match outcome {
                ProcessingOutcome::Completed(total_bytes) => {
                    self.batch_sizer.adjust(total_bytes);
                    let queue_blocks = self.queue.len().await;
                    let queue_bytes = self.queue.bytes().await;
                    tracing::info!(
                        worker = self.id,
                        requested_blocks = requested,
                        preprocessed_bytes = total_bytes,
                        queue_blocks,
                        queue_bytes,
                        "pre-processed batch processed"
                    );
                }
                ProcessingOutcome::Cancelled => {
                    tracing::info!(
                        worker = self.id,
                        "pre-process hook cancelled; exiting worker loop"
                    );
                    if let Some(batch) = pending_fetch.take() {
                        batch.abort();
                    }
                    break;
                }
                ProcessingOutcome::Stale => {
                    tracing::info!(
                        worker = self.id,
                        start_height,
                        batch_epoch,
                        "discarded stale batch after generation change"
                    );
                    if let Some(batch) = pending_fetch.take() {
                        batch.abort();
                    }
                    current_height = None;
                    continue;
                }
            }
        }

        tracing::info!(worker = self.id, "worker task exited");
        Ok(())
    }

    pub(super) async fn process_blocks(
        &self,
        batch_epoch: u64,
        blocks: Vec<(u64, String)>,
    ) -> Result<ProcessingOutcome> {
        let mut total_bytes = 0usize;

        for (height, hex_block) in blocks {
            let current_epoch = self.generation.load(Ordering::SeqCst);
            if current_epoch != batch_epoch {
                tracing::debug!(
                    worker = self.id,
                    height,
                    batch_epoch,
                    current_epoch,
                    "abandoning batch produced under stale generation"
                );
                return Ok(ProcessingOutcome::Stale);
            }

            let _activity_guard = self.activity.enter();

            let block = hex_to_block(&hex_block)?;
            let (block_hash, prev_hash) = extract_hashes(&block);
            let pre_process_future = {
                let protocol = self.protocol.read().await;
                protocol.pre_process(block, height)
            };
            tokio::pin!(pre_process_future);
            let hook = tokio::select! {
                result = &mut pre_process_future => HookDecision::Finished(result),
                _ = self.shutdown.cancelled() => HookDecision::Cancelled,
            };
            let data = match hook {
                HookDecision::Finished(Ok(value)) => value,
                HookDecision::Finished(Err(error)) => {
                    return Err(self.fatal_handler.trigger(error));
                }
                HookDecision::Cancelled => {
                    return Ok(ProcessingOutcome::Cancelled);
                }
            };
            let pre_processed = PreProcessedBlock::new(height, block_hash, prev_hash, data);
            let queue_bytes = pre_processed.queue_bytes();
            total_bytes = total_bytes.saturating_add(queue_bytes);
            self.queue.push(pre_processed, queue_bytes).await;
        }

        Ok(ProcessingOutcome::Completed(total_bytes))
    }

    pub(super) fn apply_payload_limits(&self, desired_blocks: usize) -> (usize, bool) {
        if desired_blocks == 0 {
            return (0, false);
        }
        let estimate = self.rpc_payload_stats.estimate();
        let allowed = estimate.clamp(desired_blocks, &self.rpc_payload_limits);
        (allowed, allowed < desired_blocks)
    }

    pub(super) fn record_payload_sample(&self, blocks: &[(u64, String)]) {
        if let Some(sample) = RpcPayloadSample::from_blocks(blocks) {
            self.rpc_payload_stats.record(sample);
        }
    }

    pub(super) fn record_oversized_payload_hint(&self, block_count: usize) {
        self.rpc_payload_stats
            .record_oversized_hint(block_count, &self.rpc_payload_limits);
    }
}
