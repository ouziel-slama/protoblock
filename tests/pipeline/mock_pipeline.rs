use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use crate::support::{
    helpers::{
        assert_is_contiguous, init_tracing, wait_for_height, wait_for_height_at_most,
        wait_for_worker_pool_size, RecordingProtocol,
    },
    mock_rpc::{MockChain, MockRpcServer},
};
use anyhow::{bail, Result};
use bitcoin::Block;
use protoblock::{
    BlockProtocol, BlocksFetcher, FetcherConfig, ProtocolFuture, ProtocolPreProcessFuture,
};
use tokio::sync::Notify;
use tokio::time::sleep;

struct PauseHandle {
    gate: Arc<Notify>,
    entered: Arc<Notify>,
    entered_flag: Arc<AtomicBool>,
}

impl PauseHandle {
    fn new() -> Self {
        Self {
            gate: Arc::new(Notify::new()),
            entered: Arc::new(Notify::new()),
            entered_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    fn gate(&self) -> Arc<Notify> {
        self.gate.clone()
    }

    fn entered(&self) -> Arc<Notify> {
        self.entered.clone()
    }

    fn entered_flag(&self) -> Arc<AtomicBool> {
        self.entered_flag.clone()
    }

    fn release(&self) {
        self.gate.notify_waiters();
    }

    async fn wait_until_paused(&self, timeout: Duration) -> Result<()> {
        if self.entered_flag.load(Ordering::SeqCst) {
            return Ok(());
        }

        if tokio::time::timeout(timeout, self.entered.notified())
            .await
            .is_err()
        {
            bail!("pause gate was not entered within {:?}", timeout);
        }
        Ok(())
    }
}

impl Drop for PauseHandle {
    fn drop(&mut self) {
        self.entered_flag.store(true, Ordering::SeqCst);
        self.gate.notify_waiters();
        self.entered.notify_waiters();
    }
}

struct HeavyRecordingProtocol {
    processed: Vec<u64>,
    rollbacks: Vec<u64>,
    payload_bytes: usize,
    pause_height: u64,
    pause_gate: Arc<Notify>,
    pause_entered: Arc<Notify>,
    pause_flag: Arc<AtomicBool>,
    pause_waited: bool,
    pause_announced: bool,
    process_delay: Duration,
}

impl HeavyRecordingProtocol {
    fn new(
        payload_bytes: usize,
        pause_height: u64,
        pause_gate: Arc<Notify>,
        pause_entered: Arc<Notify>,
        pause_flag: Arc<AtomicBool>,
        process_delay: Duration,
    ) -> Self {
        assert!(payload_bytes > 0, "payload_bytes must be greater than zero");
        Self {
            processed: Vec::new(),
            rollbacks: Vec::new(),
            payload_bytes,
            pause_height,
            pause_gate,
            pause_entered,
            pause_flag,
            pause_waited: false,
            pause_announced: false,
            process_delay,
        }
    }

    fn processed(&self) -> &[u64] {
        &self.processed
    }

    fn rollback_points(&self) -> &[u64] {
        &self.rollbacks
    }
}

impl BlockProtocol for HeavyRecordingProtocol {
    type PreProcessed = Vec<u8>;

    fn pre_process(
        &self,
        _block: Block,
        height: u64,
    ) -> ProtocolPreProcessFuture<Self::PreProcessed> {
        let bytes = self.payload_bytes;
        Box::pin(async move { Ok(vec![height as u8; bytes]) })
    }

    fn process<'a>(&'a mut self, data: Self::PreProcessed, height: u64) -> ProtocolFuture<'a> {
        let should_pause = height >= self.pause_height && !self.pause_waited;
        let gate = if should_pause {
            Some(self.pause_gate.clone())
        } else {
            None
        };
        if should_pause {
            self.pause_waited = true;
            if !self.pause_announced {
                self.pause_flag.store(true, Ordering::SeqCst);
                self.pause_entered.notify_waiters();
                self.pause_announced = true;
            }
        }
        let delay = self.process_delay;
        Box::pin(async move {
            drop(data);
            if let Some(waiter) = gate {
                waiter.notified().await;
            }
            if !delay.is_zero() {
                sleep(delay).await;
            }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mock_pipeline_processes_blocks() -> Result<()> {
    init_tracing();
    let chain = MockChain::new(64);
    let server = MockRpcServer::start(chain).await?;

    let config = FetcherConfig::builder()
        .rpc_url(server.url())
        .rpc_user("user")
        .rpc_password("pass")
        .thread_count(2)
        .max_batch_size_mb(2)
        .reorg_window_size(8)
        .start_height(0)
        .build()?;

    let mut fetcher = BlocksFetcher::new(config, RecordingProtocol::default());
    fetcher.start().await?;

    wait_for_height(&fetcher, 20, Duration::from_secs(5)).await?;
    fetcher.stop().await?;
    server.shutdown().await;

    let protocol = fetcher.protocol().write().await;
    assert!(
        protocol.rollback_points().is_empty(),
        "mock run should not reorg"
    );
    assert!(
        protocol.processed().len() >= 21,
        "expected at least 21 processed blocks, got {}",
        protocol.processed().len()
    );
    assert_is_contiguous(protocol.processed());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_block_queue_preserves_order() -> Result<()> {
    init_tracing();
    let chain = MockChain::new(80);
    let server = MockRpcServer::start(chain).await?;

    let config = FetcherConfig::builder()
        .rpc_url(server.url())
        .rpc_user("user")
        .rpc_password("pass")
        .thread_count(3)
        .max_batch_size_mb(2)
        .reorg_window_size(8)
        .start_height(0)
        .build()?;

    let mut fetcher = BlocksFetcher::new(config, RecordingProtocol::default());
    fetcher.start().await?;
    wait_for_height(&fetcher, 40, Duration::from_secs(6)).await?;
    fetcher.stop().await?;
    server.shutdown().await;

    let protocol = fetcher.protocol().write().await;
    assert!(
        protocol.processed().len() >= 41,
        "queue should have yielded at least 41 blocks"
    );
    assert_is_contiguous(protocol.processed());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mock_pipeline_handles_reorg() -> Result<()> {
    init_tracing();
    let chain = MockChain::new(160);
    chain.set_tip_limit(48);
    let server = MockRpcServer::start(chain.clone()).await?;

    let config = FetcherConfig::builder()
        .rpc_url(server.url())
        .rpc_user("user")
        .rpc_password("pass")
        .thread_count(3)
        .max_batch_size_mb(2)
        .reorg_window_size(32)
        .start_height(0)
        .build()?;

    let mut fetcher = BlocksFetcher::new(config, RecordingProtocol::default());
    fetcher.start().await?;
    wait_for_height(&fetcher, 24, Duration::from_secs(6)).await?;

    let fork_height = 20;
    let new_suffix_len = 140;
    chain.force_reorg(fork_height, new_suffix_len)?;
    chain.set_tip_limit(140);

    wait_for_height(&fetcher, 70, Duration::from_secs(12)).await?;
    fetcher.stop().await?;
    server.shutdown().await;

    let protocol = fetcher.protocol().write().await;
    assert!(
        protocol.rollback_points().contains(&fork_height),
        "expected rollback at height {fork_height}, got {:?}",
        protocol.rollback_points()
    );
    let final_height = protocol.processed().last().copied().unwrap_or_default();
    assert!(
        final_height >= 70,
        "mock pipeline should catch up past height 70"
    );
    assert_is_contiguous(protocol.processed());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mock_pipeline_enters_tip_wait_mode() -> Result<()> {
    init_tracing();
    let chain = MockChain::new(96);
    let initial_tip = 24;
    chain.set_tip_limit(initial_tip);
    let server = MockRpcServer::start(chain.clone()).await?;

    let config = FetcherConfig::builder()
        .rpc_url(server.url())
        .rpc_user("user")
        .rpc_password("pass")
        .thread_count(3)
        .max_batch_size_mb(1)
        .reorg_window_size(12)
        .queue_max_size_mb(16)
        .start_height(0)
        .build()?;

    let mut fetcher = BlocksFetcher::new(config, RecordingProtocol::default());
    fetcher.start().await?;
    let telemetry = fetcher.telemetry();

    wait_for_height(&fetcher, initial_tip, Duration::from_secs(10)).await?;
    wait_for_worker_pool_size(&telemetry, 1, Duration::from_secs(5)).await?;

    assert_eq!(
        telemetry.worker_pool_size(),
        1,
        "fetcher should downshift to a single worker near tip"
    );
    assert!(
        telemetry.worker_pool_transitions() >= 2,
        "expected at least 2 worker pool transitions, observed {}",
        telemetry.worker_pool_transitions()
    );

    let resumed_tip = chain.advance_tip_by(6);
    wait_for_height(&fetcher, resumed_tip, Duration::from_secs(20)).await?;
    assert_eq!(
        telemetry.worker_pool_size(),
        1,
        "tip-wait mode should continue to run exactly one worker"
    );

    fetcher.stop().await?;
    server.shutdown().await;

    let protocol = fetcher.protocol().write().await;
    let processed_tip = protocol.processed().last().copied().unwrap_or_default();
    assert!(
        processed_tip >= resumed_tip,
        "protocol should keep processing blocks after downshift (last: {processed_tip}, target: {resumed_tip})"
    );
    assert_is_contiguous(protocol.processed());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn mock_pipeline_recovers_when_queue_saturates_during_reorg() -> Result<()> {
    init_tracing();
    let chain = MockChain::new(256);
    let server = MockRpcServer::start(chain.clone()).await?;

    const PAUSE_HEIGHT: u64 = 8;
    const FORK_HEIGHT: u64 = 3;
    const NEW_SUFFIX_LEN: u64 = 96;
    const PAYLOAD_BYTES: usize = 64 * 1024;
    const PROCESS_DELAY_MS: u64 = 20;

    let pause_handle = PauseHandle::new();
    let protocol = HeavyRecordingProtocol::new(
        PAYLOAD_BYTES,
        PAUSE_HEIGHT,
        pause_handle.gate(),
        pause_handle.entered(),
        pause_handle.entered_flag(),
        Duration::from_millis(PROCESS_DELAY_MS),
    );

    let config = FetcherConfig::builder()
        .rpc_url(server.url())
        .rpc_user("user")
        .rpc_password("pass")
        .thread_count(4)
        .max_batch_size_mb(2)
        .queue_max_size_mb(2)
        .reorg_window_size(64)
        .start_height(0)
        .build()?;
    let mut fetcher = BlocksFetcher::new(config, protocol);
    fetcher.start().await?;

    let pre_reorg_target = FORK_HEIGHT.saturating_add(1);
    wait_for_height(&fetcher, pre_reorg_target, Duration::from_secs(5)).await?;

    pause_handle
        .wait_until_paused(Duration::from_secs(10))
        .await?;

    sleep(Duration::from_millis(200)).await;
    let queue = fetcher.queue().clone();
    let backlog = queue.len().await;
    assert!(
        backlog >= 12,
        "expected queue backlog to reach at least 12 blocks, observed {backlog}"
    );

    chain.force_reorg(FORK_HEIGHT, NEW_SUFFIX_LEN)?;
    chain.set_tip_limit(FORK_HEIGHT);
    pause_handle.release();
    wait_for_height_at_most(&fetcher, FORK_HEIGHT, Duration::from_secs(10)).await?;
    chain.set_tip_limit(FORK_HEIGHT + NEW_SUFFIX_LEN);

    fetcher.stop().await?;
    server.shutdown().await;

    let protocol = fetcher.protocol().write().await;
    assert!(
        protocol.rollback_points().contains(&FORK_HEIGHT),
        "expected rollback at height {FORK_HEIGHT}, got {:?}",
        protocol.rollback_points()
    );
    assert_is_contiguous(protocol.processed());

    Ok(())
}
