//! Block fetching pipeline orchestration.
//!
//! `BlocksFetcher` now composes smaller modules so each concern is owned by the
//! component that knows it best:
//! - `worker_pool` manages worker creation, control hooks, and pool sizing.
//! - `reorg` encapsulates fork detection and rollback mechanics.
//! - `reorg` exposes helpers like `ReorgWindow` used during reorg handling.
//! - `tip` keeps track of the blockchain tip, refresh cadence, and the tip-wait
//!   state machine.
//! - `lifecycle` wires run-scoped cancellation, telemetry reporters, and fatal
//!   error propagation.
//!
//! The struct defined below orchestrates these pieces so callers still interact
//! with a single `BlocksFetcher` API while implementation details live in the
//! focused submodules.

use super::lifecycle::{LifecycleHandles, LifecycleSpawnParams};
use super::reorg::ReorgWindow;
use super::reorg::{seed_reorg_window, ReorgManager};
use super::tip::{BlockchainTip, TipTracker};
use super::worker_pool::{WorkerPool, WorkerPoolParams};

use crate::preprocessors::ordered_queue::{OrderedBlockQueue, BYTES_PER_MEGABYTE};
use crate::preprocessors::worker::{FetcherEvent, WorkerActivityTracker, WorkerControlSender};
use crate::rpc::circuit_breaker::RpcCircuitBreaker;
use crate::rpc::payload::RpcPayloadLimits;
use crate::rpc::AsyncRpcClient;
use crate::runtime::config::FetcherConfig;
use crate::runtime::fatal::FatalErrorHandler;
use crate::runtime::progress::ProgressTracker;
use crate::runtime::protocol::BlockProtocol;
use crate::runtime::telemetry::Telemetry;
use anyhow::{bail, Context, Result};
use futures::future::join_all;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::{mpsc, watch, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub struct BlocksFetcher<P: BlockProtocol> {
    config: FetcherConfig,
    protocol: Arc<RwLock<P>>,
    queue: Arc<OrderedBlockQueue<P::PreProcessed>>,
    processing_handle: Option<JoinHandle<Result<()>>>,
    breaker: Arc<RpcCircuitBreaker>,
    running: bool,
    shutdown_root: CancellationToken,
    progress: Arc<ProgressTracker>,
    telemetry: Arc<Telemetry>,
    worker_pool: WorkerPool<P>,
    lifecycle: Option<LifecycleHandles>,
    generation: Arc<AtomicU64>,
    blockchain_tip: Arc<BlockchainTip>,
}

pub(crate) enum BlockAction {
    Processed,
    Cancelled,
}

pub(crate) struct ProcessingHandles<P: BlockProtocol> {
    pub(super) protocol: Arc<RwLock<P>>,
    pub(super) queue: Arc<OrderedBlockQueue<P::PreProcessed>>,
    pub(super) rpc_client: Arc<AsyncRpcClient>,
    pub(super) control_txs: Vec<WorkerControlSender>,
    pub(super) progress: Arc<ProgressTracker>,
    pub(super) activity: Arc<WorkerActivityTracker>,
    pub(super) generation: Arc<AtomicU64>,
    pub(super) fatal_handler: Arc<FatalErrorHandler>,
    pub(super) telemetry: Arc<Telemetry>,
    pub(super) blockchain_tip: Arc<BlockchainTip>,
    pub(super) active_workers: Arc<AtomicUsize>,
    pub(super) tip_wait_mode: Arc<AtomicBool>,
    pub(super) max_workers: usize,
}

impl<P: BlockProtocol> ProcessingHandles<P> {
    pub(super) fn control_channels(&self) -> &[WorkerControlSender] {
        &self.control_txs
    }
}

impl<P: BlockProtocol> Clone for ProcessingHandles<P> {
    fn clone(&self) -> Self {
        Self {
            protocol: Arc::clone(&self.protocol),
            queue: Arc::clone(&self.queue),
            rpc_client: Arc::clone(&self.rpc_client),
            control_txs: self.control_txs.clone(),
            progress: Arc::clone(&self.progress),
            activity: Arc::clone(&self.activity),
            generation: Arc::clone(&self.generation),
            fatal_handler: Arc::clone(&self.fatal_handler),
            telemetry: Arc::clone(&self.telemetry),
            blockchain_tip: Arc::clone(&self.blockchain_tip),
            active_workers: Arc::clone(&self.active_workers),
            tip_wait_mode: Arc::clone(&self.tip_wait_mode),
            max_workers: self.max_workers,
        }
    }
}

impl<P: BlockProtocol> BlocksFetcher<P> {
    /// Creates a new block fetcher with the given configuration and protocol.
    ///
    /// The fetcher creates its own root cancellation token. Use [`Self::with_cancellation_token`]
    /// if you need to integrate with an existing shutdown mechanism.
    pub fn new(config: FetcherConfig, protocol: P) -> Self {
        Self::with_cancellation_token(config, protocol, CancellationToken::new())
    }

    /// Creates a new block fetcher with the given configuration, protocol, and shutdown token.
    ///
    /// The shutdown token is used to derive per-run cancellation tokens for workers and the
    /// processing loop.
    pub fn with_cancellation_token(
        config: FetcherConfig,
        protocol: P,
        shutdown_token: CancellationToken,
    ) -> Self {
        let max_workers = config.thread_count().max(1);
        let progress = Arc::new(ProgressTracker::new(config.start_height()));
        let queue_max_bytes = config
            .queue_max_size_mb()
            .saturating_mul(BYTES_PER_MEGABYTE);
        let queue = Arc::new(OrderedBlockQueue::with_capacity(queue_max_bytes));
        let protocol = Arc::new(RwLock::new(protocol));
        let telemetry = Arc::new(Telemetry::default());
        let generation = Arc::new(AtomicU64::new(0));
        let blockchain_tip = Arc::new(BlockchainTip::new(None));
        let worker_pool = WorkerPool::new(WorkerPoolParams {
            max_workers,
            max_batch_size_mb: config.max_batch_size_mb(),
            protocol: protocol.clone(),
            queue: queue.clone(),
            telemetry: telemetry.clone(),
            generation: generation.clone(),
            blockchain_tip: blockchain_tip.clone(),
            payload_limits: RpcPayloadLimits::new(
                config.rpc_max_request_body_bytes(),
                config.rpc_max_response_body_bytes(),
            ),
        });
        Self {
            queue,
            protocol,
            processing_handle: None,
            breaker: Arc::new(RpcCircuitBreaker::default()),
            running: false,
            shutdown_root: shutdown_token,
            progress,
            telemetry,
            config,
            worker_pool,
            lifecycle: None,
            generation,
            blockchain_tip,
        }
    }

    /// Returns a reference to the fetcher's configuration.
    pub fn config(&self) -> &FetcherConfig {
        &self.config
    }

    /// Returns a reference to the ordered block queue.
    pub fn queue(&self) -> &Arc<OrderedBlockQueue<P::PreProcessed>> {
        &self.queue
    }

    /// Returns a reference to the protocol instance wrapped in an `RwLock`.
    pub fn protocol(&self) -> &Arc<RwLock<P>> {
        &self.protocol
    }

    /// Returns a reference to the worker task handles.
    pub fn workers(&self) -> &Vec<JoinHandle<()>> {
        self.worker_pool.handles()
    }

    /// Returns the last height confirmed by the processing loop.
    ///
    /// Returns `None` if no blocks have been processed yet.
    pub fn last_confirmed_height(&self) -> Option<u64> {
        self.progress.last_confirmed()
    }

    /// Returns a clone of the telemetry handle for observability.
    pub fn telemetry(&self) -> Arc<Telemetry> {
        self.telemetry.clone()
    }

    /// Replaces the root shutdown token used to derive per-run cancellation tokens.
    /// This must only be called while the fetcher is idle (i.e. between `stop` and `start`).
    pub fn replace_shutdown_root(&mut self, shutdown: CancellationToken) {
        debug_assert!(
            !self.running,
            "shutdown token should not change while the fetcher is running"
        );
        self.shutdown_root = shutdown;
    }

    /// Starts the fetcher pipeline from the configured start height.
    pub async fn start(&mut self) -> Result<()> {
        self.start_from(self.config.start_height()).await
    }

    /// Starts the fetcher pipeline from the specified height.
    ///
    /// Returns an error if the fetcher is already running.
    pub async fn start_from(&mut self, start_height: u64) -> Result<()> {
        if self.running {
            bail!("fetcher already running");
        }

        debug_assert!(
            self.config.validate().is_ok(),
            "FetcherConfig should have been validated at construction time"
        );

        let max_workers = self.worker_pool.max_workers();
        self.generation.store(0, Ordering::SeqCst);
        let rpc_client = Arc::new(
            AsyncRpcClient::from_config_with_breaker(&self.config, self.breaker.clone())
                .context("failed to build RPC client")?,
        );
        let event_capacity = self.config.thread_count().max(1).saturating_mul(4).max(8);
        let (event_tx, event_rx) = mpsc::channel::<FetcherEvent>(event_capacity);

        tracing::info!(
            start_height,
            threads = self.config.thread_count(),
            "starting block fetcher"
        );

        self.queue.clear().await;
        self.queue.reset_expected(start_height).await;
        self.progress.reset(start_height);

        if let Err(err) = Self::refresh_tip_once(&rpc_client, &self.blockchain_tip).await {
            tracing::info!(error = %err, "failed to fetch initial blockchain tip; continuing without guard until refresh succeeds");
        }
        let tip_hint = self.blockchain_tip.current();
        let lifecycle = LifecycleHandles::spawn::<P>(LifecycleSpawnParams {
            shutdown_root: &self.shutdown_root,
            telemetry: self.telemetry.clone(),
            queue: self.queue.clone(),
            metrics_interval: self.config.metrics_interval(),
            tip_refresh_interval: self.config.tip_refresh_interval(),
            rpc_client: rpc_client.clone(),
            blockchain_tip: self.blockchain_tip.clone(),
            event_tx: event_tx.clone(),
        });
        let fatal_handler = lifecycle.fatal_handler();
        let run_token = lifecycle.run_token.clone();
        let mut lifecycle_guard = Some(lifecycle);

        let initial_reorg_window = match seed_reorg_window(
            &self.config,
            start_height,
            &rpc_client,
            &self.blockchain_tip,
            tip_hint,
            run_token.clone(),
        )
        .await
        .context("failed to seed reorg window")
        {
            Ok(window) => window,
            Err(err) => {
                Self::abort_lifecycle(&mut lifecycle_guard).await;
                return Err(err);
            }
        };

        let current_tip = self.blockchain_tip.current();
        let (initial_workers, start_in_tip_wait) =
            WorkerPool::<P>::determine_initial_state(start_height, current_tip, max_workers);
        self.worker_pool
            .configure_initial_state(initial_workers, start_in_tip_wait);
        if start_in_tip_wait {
            tracing::info!(
                start_height,
                tip = ?current_tip,
                "starting near tip; limiting worker pool to a single thread"
            );
        }

        let workers_done_rx = match self.worker_pool.launch(
            &self.config,
            run_token.clone(),
            self.shutdown_root.clone(),
            fatal_handler.clone(),
            self.breaker.clone(),
            event_tx.clone(),
        ) {
            Ok(rx) => rx,
            Err(err) => {
                Self::abort_lifecycle(&mut lifecycle_guard).await;
                return Err(err);
            }
        };

        if let Err(err) = self.worker_pool.start_at(start_height).await {
            tracing::error!(error = %err, "failed to start workers; shutting down partial tasks");
            let handles = self.worker_pool.shutdown().await;
            let _ = join_all(handles).await;
            self.worker_pool.notify_all_workers_done();
            Self::abort_lifecycle(&mut lifecycle_guard).await;
            return Err(err);
        }

        let processing_handles = ProcessingHandles {
            protocol: self.protocol.clone(),
            queue: self.queue.clone(),
            rpc_client: rpc_client.clone(),
            control_txs: self.worker_pool.control_channels().to_vec(),
            progress: self.progress.clone(),
            activity: self.worker_pool.activity(),
            generation: self.generation.clone(),
            fatal_handler: fatal_handler.clone(),
            telemetry: self.telemetry.clone(),
            blockchain_tip: self.blockchain_tip.clone(),
            active_workers: self.worker_pool.active_workers(),
            tip_wait_mode: self.worker_pool.tip_wait_mode(),
            max_workers,
        };
        let processing_handle = Self::spawn_processing_loop(
            processing_handles,
            run_token.clone(),
            workers_done_rx,
            event_rx,
            start_height,
            initial_reorg_window,
        );
        self.processing_handle = Some(processing_handle);
        self.lifecycle = lifecycle_guard;
        self.running = true;

        Ok(())
    }

    /// Stops the fetcher pipeline gracefully.
    ///
    /// Cancels workers, drains the processing loop, and invokes the protocol's shutdown hook.
    /// Returns any error encountered during shutdown.
    pub async fn stop(&mut self) -> Result<()> {
        if !self.running {
            return Ok(());
        }

        tracing::info!("stopping block fetcher");

        let lifecycle_error = self.lifecycle.as_ref().and_then(|handles| handles.error());
        if let Some(handles) = &self.lifecycle {
            handles.run_token.cancel();
        }

        let worker_handles = self.worker_pool.shutdown().await;
        let results = join_all(worker_handles).await;
        for (idx, result) in results.into_iter().enumerate() {
            if let Err(err) = result {
                tracing::warn!(worker = idx, error = %err, "worker task terminated unexpectedly");
            }
        }
        tracing::debug!("block fetcher stop: worker tasks joined");

        self.worker_pool.notify_all_workers_done();
        tracing::debug!("block fetcher stop: signaled workers_done");

        let mut pipeline_error: Option<anyhow::Error> = None;
        if let Some(handle) = self.processing_handle.take() {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    tracing::error!(error = %err, "processing loop exited with error");
                    pipeline_error = Some(err);
                }
                Err(err) => {
                    tracing::error!(error = %err, "failed to join processing loop task");
                    pipeline_error = Some(err.into());
                }
            }
        }
        tracing::debug!("block fetcher stop: processing loop joined");

        if let Some(handles) = self.lifecycle.take() {
            handles.shutdown().await;
        }

        {
            let mut protocol = self.protocol.write().await;
            protocol
                .shutdown()
                .await
                .context("failed to shutdown protocol")?;
        }

        self.running = false;
        self.queue.clear().await;

        let final_error = pipeline_error.or(lifecycle_error);

        if let Some(err) = final_error {
            return Err(err).context("block processing pipeline aborted");
        }

        Ok(())
    }

    async fn refresh_tip_once(
        rpc_client: &Arc<AsyncRpcClient>,
        blockchain_tip: &Arc<BlockchainTip>,
    ) -> Result<()> {
        let tip = rpc_client.get_blockchain_tip().await?;
        blockchain_tip.update(tip);
        Ok(())
    }

    fn spawn_processing_loop(
        handles: ProcessingHandles<P>,
        shutdown: CancellationToken,
        workers_done_rx: watch::Receiver<bool>,
        event_rx: mpsc::Receiver<FetcherEvent>,
        start_height: u64,
        reorg_window: ReorgWindow,
    ) -> JoinHandle<Result<()>> {
        tokio::spawn(async move {
            let mut workers_done_rx = workers_done_rx;
            let mut event_rx = event_rx;
            let mut reorg_window = reorg_window;
            let reorg_manager = ReorgManager::new(handles.clone());

            let queue_handle = handles.queue.clone();
            let mut next_expected = start_height;
            let mut dropping = false;
            let mut workers_done = *workers_done_rx.borrow();
            let mut events_closed = false;

            loop {
                if dropping && workers_done {
                    break;
                }

                tokio::select! {
                    block = queue_handle.pop_next() => {
                        if dropping {
                            tracing::trace!(
                                height = block.height(),
                                "dropping block after shutdown request"
                            );
                            continue;
                        }

                        match reorg_manager
                            .handle_block(
                                block,
                                &mut reorg_window,
                                &mut next_expected,
                                &shutdown,
                            )
                            .await
                        {
                            Ok(BlockAction::Processed) => {
                                TipTracker::maybe_adjust_workers_for_tip(
                                    &handles,
                                    next_expected,
                                )
                                .await?;
                            }
                            Ok(BlockAction::Cancelled) => {
                                dropping = true;
                                let pending = queue_handle.len().await;
                                let pending_bytes = queue_handle.bytes().await;
                                if pending > 0 {
                                    tracing::info!(
                                        pending_blocks = pending,
                                        pending_bytes,
                                        "dropping buffered blocks after cancellation"
                                    );
                                }
                                queue_handle.clear().await;
                                if workers_done && queue_handle.len().await == 0 {
                                    break;
                                }
                            }
                            Err(err) => {
                                tracing::error!(error = %err, "processing loop exiting due to error");
                                return Err(err);
                            }
                        }
                    }
                    event = event_rx.recv(), if !dropping && !events_closed => {
                        match event {
                            Some(event) => {
                                reorg_manager
                                    .handle_event(
                                        event,
                                        &mut reorg_window,
                                        &mut next_expected,
                                        &shutdown,
                                    )
                                    .await?;
                            }
                            None => {
                                events_closed = true;
                                tracing::debug!("fetcher event channel closed");
                            }
                        }
                    }
                    _ = shutdown.cancelled(), if !dropping => {
                        let pending = queue_handle.len().await;
                        let pending_bytes = queue_handle.bytes().await;
                        tracing::info!(
                            pending_blocks = pending,
                            pending_bytes,
                            "processing loop received shutdown signal; dropping queued blocks"
                        );
                        dropping = true;
                        queue_handle.clear().await;
                        if workers_done {
                            break;
                        }
                    }
                    changed = workers_done_rx.changed(), if !workers_done => {
                        match changed {
                            Ok(_) => {
                                workers_done = *workers_done_rx.borrow();
                                if dropping && workers_done {
                                    break;
                                }
                            }
                            Err(_) => {
                                tracing::warn!("workers done channel closed unexpectedly");
                                break;
                            }
                        }
                    }
                }
            }

            tracing::info!("processing loop stopped");
            Ok(())
        })
    }

    async fn abort_lifecycle(handles: &mut Option<LifecycleHandles>) {
        if let Some(lifecycle) = handles.take() {
            lifecycle.run_token.cancel();
            lifecycle.shutdown().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::processor::reorg;
    use bitcoin::hashes::Hash as _;
    use bitcoin::BlockHash;

    fn dummy_hash(seed: u8) -> BlockHash {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        BlockHash::from_slice(&bytes).expect("valid hash")
    }

    #[test]
    fn locate_common_ancestor_returns_latest_match() {
        let entries = vec![
            (10, dummy_hash(1)),
            (11, dummy_hash(2)),
            (12, dummy_hash(3)),
            (13, dummy_hash(4)),
        ];
        let canonical = vec![dummy_hash(1), dummy_hash(2), dummy_hash(30), dummy_hash(40)];

        let fork = reorg::locate_common_ancestor(&entries, &canonical);
        assert_eq!(fork, Some(11));
    }

    #[test]
    fn locate_common_ancestor_returns_none_when_no_match() {
        let entries = vec![(5, dummy_hash(5)), (6, dummy_hash(6)), (7, dummy_hash(7))];
        let canonical = vec![dummy_hash(50), dummy_hash(51), dummy_hash(52)];

        let fork = reorg::locate_common_ancestor(&entries, &canonical);
        assert_eq!(fork, None);
    }
}
