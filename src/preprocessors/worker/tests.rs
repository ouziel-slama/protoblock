use super::types::ProcessingOutcome;
use super::*;
use crate::preprocessors::ordered_queue::OrderedBlockQueue;
use crate::processor::tip::BlockchainTip;
use crate::rpc::payload::{RpcPayloadLimits, RpcPayloadStats};
use crate::rpc::{BlockBatchClient, RpcError};
use crate::runtime::fatal::FatalErrorHandler;
use crate::runtime::protocol::{
    BlockProtocol, ProtocolError, ProtocolFuture, ProtocolPreProcessFuture,
};
use crate::runtime::telemetry::Telemetry;
use anyhow::{anyhow, Result};
use bitcoin::Block;
use futures::future::{self, BoxFuture};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{mpsc, Mutex as AsyncMutex, Notify, RwLock};
use tokio::time::{timeout, Duration};
use tokio_util::sync::CancellationToken;

const GENESIS_BLOCK_HEX: &str = "0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c0101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4d04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73ffffffff0100f2052a01000000434104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac00000000";

#[test]
fn calculates_worker_height_pattern() {
    let heights = Worker::<TestProtocol>::calculate_heights(1, 3, 5, 1);
    assert_eq!(heights, vec![1, 6, 11]);
}

#[tokio::test]
async fn worker_processes_batches_and_adjusts_size() -> Result<()> {
    let queue = Arc::new(OrderedBlockQueue::new());
    queue.reset_expected(1).await;

    let slow_protocol = SlowProtocol::new();
    let gate = slow_protocol.release_handle();
    let started = slow_protocol.first_block_started();
    let protocol = Arc::new(RwLock::new(slow_protocol));
    let mock_client = Arc::new(MockRpcClient::new(vec![
        vec![(1, GENESIS_BLOCK_HEX.to_string())],
        vec![
            (6, GENESIS_BLOCK_HEX.to_string()),
            (11, GENESIS_BLOCK_HEX.to_string()),
        ],
    ]));

    let (tx, rx) = worker_control_channel(4);
    let shutdown = CancellationToken::new();
    let fatal_handler = Arc::new(FatalErrorHandler::new(shutdown.clone(), shutdown.clone()));
    let activity = Arc::new(WorkerActivityTracker::new());
    let generation = Arc::new(AtomicU64::new(0));
    let (event_tx, _event_rx) = mpsc::channel(4);
    let shared = WorkerShared::new(WorkerSharedParams {
        protocol,
        queue: queue.clone(),
        telemetry: Arc::new(Telemetry::default()),
        activity: activity.clone(),
        generation,
        blockchain_tip: Arc::new(BlockchainTip::new(Some(20))),
        active_workers: Arc::new(AtomicUsize::new(5)),
        event_tx,
        max_batch_size_mb: 8,
        payload_limits: RpcPayloadLimits::new(usize::MAX / 2, usize::MAX / 2),
        payload_stats: Arc::new(RpcPayloadStats::default()),
    });
    let worker = Worker::with_rpc_client(
        0,
        mock_client.clone(),
        rx,
        shutdown,
        fatal_handler,
        shared,
        Duration::from_millis(5),
    );

    let handle = tokio::spawn(async move {
        worker.run().await.unwrap();
    });

    tx.send(WorkerControl::Start(1)).await.unwrap();

    started.notified().await;

    timeout(Duration::from_millis(500), async {
        loop {
            if mock_client.as_ref().request_count().await >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("double buffering should issue a second RPC while processing the first batch");

    gate.notify_waiters();

    timeout(Duration::from_millis(500), async {
        loop {
            if mock_client.as_ref().request_count().await >= 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("worker should perform a third request after processing completes");

    tx.send(WorkerControl::Shutdown).await.unwrap();
    handle.await.unwrap();

    let requests = mock_client.requests().await;
    assert!(
        requests.len() >= 2,
        "expected at least two RPC requests, got {:?}",
        requests
    );
    assert_eq!(requests[0], vec![1]);
    let mut fetched: Vec<u64> = requests.iter().skip(1).flatten().copied().collect();
    fetched.sort_unstable();
    fetched.dedup();
    assert!(
        fetched.contains(&6) && fetched.contains(&11),
        "expected heights 6 and 11 to be requested, got {:?}",
        requests
    );

    queue.reset_expected(6).await;
    assert!(queue.try_pop_next().await.is_some());

    Ok(())
}

#[tokio::test]
async fn worker_clamps_batches_to_payload_limits() -> Result<()> {
    let queue = Arc::new(OrderedBlockQueue::new());
    queue.reset_expected(1).await;

    let protocol = Arc::new(RwLock::new(TestProtocol::default()));
    let mock_client = Arc::new(MockRpcClient::new(vec![
        vec![(1, GENESIS_BLOCK_HEX.to_string())],
        vec![(6, GENESIS_BLOCK_HEX.to_string())],
    ]));
    let (tx, rx) = worker_control_channel(4);
    let shutdown = CancellationToken::new();
    let fatal_handler = Arc::new(FatalErrorHandler::new(shutdown.clone(), shutdown.clone()));
    let activity = Arc::new(WorkerActivityTracker::new());
    let generation = Arc::new(AtomicU64::new(0));
    let (event_tx, _event_rx) = mpsc::channel(4);
    let shared = WorkerShared::new(WorkerSharedParams {
        protocol,
        queue: queue.clone(),
        telemetry: Arc::new(Telemetry::default()),
        activity,
        generation,
        blockchain_tip: Arc::new(BlockchainTip::new(Some(20))),
        active_workers: Arc::new(AtomicUsize::new(2)),
        event_tx,
        max_batch_size_mb: 32,
        payload_limits: RpcPayloadLimits::new(512, 512),
        payload_stats: Arc::new(RpcPayloadStats::default()),
    });
    let worker = Worker::with_rpc_client(
        0,
        mock_client.clone(),
        rx,
        shutdown.clone(),
        fatal_handler,
        shared,
        Duration::from_millis(5),
    );

    let handle = tokio::spawn(async move { worker.run().await.unwrap() });
    tx.send(WorkerControl::Start(1)).await.unwrap();

    timeout(Duration::from_millis(200), async {
        loop {
            if mock_client.request_count().await >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("worker should submit multiple requests even when clamped");

    shutdown.cancel();
    tx.send(WorkerControl::Shutdown).await.ok();
    handle.await.unwrap();

    let requests = mock_client.requests().await;
    assert!(
        requests.iter().all(|batch| batch.len() == 1),
        "payload limits should restrict batches to a single height, got {requests:?}"
    );
    Ok(())
}

#[tokio::test]
async fn worker_escalates_unshrinkable_oversize() -> Result<()> {
    let queue = Arc::new(OrderedBlockQueue::new());
    queue.reset_expected(1).await;

    let protocol = Arc::new(RwLock::new(TestProtocol::default()));
    let mock_client: Arc<dyn BlockBatchClient> = Arc::new(OversizeErrorRpcClient::new());
    let (tx, rx) = worker_control_channel(4);
    let shutdown = CancellationToken::new();
    let fatal_handler = Arc::new(FatalErrorHandler::new(shutdown.clone(), shutdown.clone()));
    let activity = Arc::new(WorkerActivityTracker::new());
    let generation = Arc::new(AtomicU64::new(0));
    let (event_tx, _event_rx) = mpsc::channel(4);
    let shared = WorkerShared::new(WorkerSharedParams {
        protocol,
        queue,
        telemetry: Arc::new(Telemetry::default()),
        activity,
        generation,
        blockchain_tip: Arc::new(BlockchainTip::new(Some(10))),
        active_workers: Arc::new(AtomicUsize::new(1)),
        event_tx,
        max_batch_size_mb: 4,
        payload_limits: RpcPayloadLimits::new(512 * 1024, 512 * 1024),
        payload_stats: Arc::new(RpcPayloadStats::default()),
    });
    let worker = Worker::with_rpc_client(
        0,
        mock_client,
        rx,
        shutdown.clone(),
        fatal_handler.clone(),
        shared,
        Duration::from_millis(5),
    );

    let handle = tokio::spawn(async move { worker.run().await });
    tx.send(WorkerControl::Start(1)).await.unwrap();

    let result = handle.await.expect("worker task panicked");
    assert!(
        result.is_err(),
        "worker should exit with fatal error when batches cannot shrink further"
    );
    let captured = fatal_handler
        .error()
        .expect("fatal handler should capture oversize error");
    let message = format!("{captured}");
    assert!(
        message.contains("PROTOBLOCK_MAX_RESPONSE_MB")
            || message.contains("PROTOBLOCK_MAX_REQUEST_MB"),
        "fatal message should mention payload env vars; got {message}"
    );
    assert!(
        shutdown.is_cancelled(),
        "fatal handler should cancel the worker shutdown token"
    );

    Ok(())
}

#[tokio::test]
async fn activity_tracker_waits_for_idle() -> Result<()> {
    let tracker = Arc::new(WorkerActivityTracker::new());
    let guard = tracker.enter();
    let wait = tokio::spawn({
        let tracker = tracker.clone();
        async move {
            tracker.wait_until_idle().await;
        }
    });

    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(!wait.is_finished(), "tracker should still be busy");

    drop(guard);
    timeout(Duration::from_millis(200), wait)
        .await
        .expect("waiter should finish")
        .expect("task runs successfully");
    Ok(())
}

struct MockRpcClient {
    requests: AsyncMutex<Vec<Vec<u64>>>,
    responses: AsyncMutex<VecDeque<Vec<(u64, String)>>>,
}

impl MockRpcClient {
    fn new(responses: Vec<Vec<(u64, String)>>) -> Self {
        Self {
            requests: AsyncMutex::new(Vec::new()),
            responses: AsyncMutex::new(VecDeque::from(responses)),
        }
    }

    async fn request_count(&self) -> usize {
        self.requests.lock().await.len()
    }

    async fn requests(&self) -> Vec<Vec<u64>> {
        self.requests.lock().await.clone()
    }
}

impl BlockBatchClient for MockRpcClient {
    fn batch_get_blocks<'a>(
        &'a self,
        heights: &'a [u64],
    ) -> BoxFuture<'a, Result<Vec<(u64, String)>>> {
        let heights_vec = heights.to_vec();
        Box::pin(async move {
            self.requests.lock().await.push(heights_vec);
            let mut responses = self.responses.lock().await;
            responses
                .pop_front()
                .ok_or_else(|| anyhow!("no mock response available"))
        })
    }
}

enum ControlledResponse {
    Immediate(Vec<(u64, String)>),
    Pending,
}

struct ControlledRpcClient {
    requests: AsyncMutex<Vec<Vec<u64>>>,
    responses: AsyncMutex<VecDeque<ControlledResponse>>,
}

impl ControlledRpcClient {
    fn new(responses: Vec<ControlledResponse>) -> Self {
        Self {
            requests: AsyncMutex::new(Vec::new()),
            responses: AsyncMutex::new(VecDeque::from(responses)),
        }
    }

    async fn request_count(&self) -> usize {
        self.requests.lock().await.len()
    }
}

impl BlockBatchClient for ControlledRpcClient {
    fn batch_get_blocks<'a>(
        &'a self,
        heights: &'a [u64],
    ) -> BoxFuture<'a, Result<Vec<(u64, String)>>> {
        let heights_vec = heights.to_vec();
        Box::pin(async move {
            self.requests.lock().await.push(heights_vec);
            let mut responses = self.responses.lock().await;
            match responses.pop_front().expect("no mock response available") {
                ControlledResponse::Immediate(blocks) => Ok(blocks),
                ControlledResponse::Pending => {
                    future::pending::<Result<Vec<(u64, String)>>>().await
                }
            }
        })
    }
}

struct OversizeErrorRpcClient {
    requests: AsyncMutex<Vec<Vec<u64>>>,
}

impl OversizeErrorRpcClient {
    fn new() -> Self {
        Self {
            requests: AsyncMutex::new(Vec::new()),
        }
    }
}

impl BlockBatchClient for OversizeErrorRpcClient {
    fn batch_get_blocks<'a>(
        &'a self,
        heights: &'a [u64],
    ) -> BoxFuture<'a, Result<Vec<(u64, String)>>> {
        let heights_vec = heights.to_vec();
        Box::pin(async move {
            self.requests.lock().await.push(heights_vec);
            Err(RpcError::ResponseTooLarge { method: "getblock" }.into())
        })
    }
}

struct SleepyRpcClient {
    requests: AsyncMutex<Vec<Vec<u64>>>,
    responses: AsyncMutex<VecDeque<Vec<(u64, String)>>>,
    delay: Duration,
}

impl SleepyRpcClient {
    fn new(delay: Duration, responses: Vec<Vec<(u64, String)>>) -> Self {
        Self {
            requests: AsyncMutex::new(Vec::new()),
            responses: AsyncMutex::new(VecDeque::from(responses)),
            delay,
        }
    }
}

impl BlockBatchClient for SleepyRpcClient {
    fn batch_get_blocks<'a>(
        &'a self,
        heights: &'a [u64],
    ) -> BoxFuture<'a, Result<Vec<(u64, String)>>> {
        let heights_vec = heights.to_vec();
        let delay = self.delay;
        Box::pin(async move {
            self.requests.lock().await.push(heights_vec);
            tokio::time::sleep(delay).await;
            let mut responses = self.responses.lock().await;
            responses
                .pop_front()
                .ok_or_else(|| anyhow!("no mock response available"))
        })
    }
}

#[derive(Default)]
struct TestProtocol {
    processed: Mutex<Vec<(u64, String)>>,
}

impl BlockProtocol for TestProtocol {
    type PreProcessed = String;

    fn pre_process(
        &self,
        _block: Block,
        height: u64,
    ) -> ProtocolPreProcessFuture<Self::PreProcessed> {
        Box::pin(async move { Ok(format!("processed-{height}")) })
    }

    fn process<'a>(&'a mut self, data: Self::PreProcessed, height: u64) -> ProtocolFuture<'a> {
        Box::pin(async move {
            self.processed.lock().unwrap().push((height, data));
            Ok(())
        })
    }

    fn rollback<'a>(&'a mut self, _: u64) -> ProtocolFuture<'a> {
        Box::pin(future::ready(Ok(())))
    }

    fn shutdown<'a>(&'a mut self) -> ProtocolFuture<'a> {
        Box::pin(future::ready(Ok(())))
    }
}

struct SlowProtocol {
    processed: Mutex<Vec<(u64, String)>>,
    release: Arc<Notify>,
    first_ready: Arc<Notify>,
    first_seen: AtomicBool,
}

impl SlowProtocol {
    fn new() -> Self {
        Self {
            processed: Mutex::new(Vec::new()),
            release: Arc::new(Notify::new()),
            first_ready: Arc::new(Notify::new()),
            first_seen: AtomicBool::new(false),
        }
    }

    fn release_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.release)
    }

    fn first_block_started(&self) -> Arc<Notify> {
        Arc::clone(&self.first_ready)
    }
}

impl BlockProtocol for SlowProtocol {
    type PreProcessed = String;

    fn pre_process(
        &self,
        _block: Block,
        height: u64,
    ) -> ProtocolPreProcessFuture<Self::PreProcessed> {
        let should_wait = !self.first_seen.swap(true, Ordering::SeqCst);
        let release = self.release.clone();
        let ready = self.first_ready.clone();
        Box::pin(async move {
            if should_wait {
                ready.notify_one();
                release.notified().await;
            }
            Ok(format!("processed-{height}"))
        })
    }

    fn process<'a>(&'a mut self, data: Self::PreProcessed, height: u64) -> ProtocolFuture<'a> {
        Box::pin(async move {
            self.processed.lock().unwrap().push((height, data));
            Ok(())
        })
    }

    fn rollback<'a>(&'a mut self, _: u64) -> ProtocolFuture<'a> {
        Box::pin(future::ready(Ok(())))
    }

    fn shutdown<'a>(&'a mut self) -> ProtocolFuture<'a> {
        Box::pin(future::ready(Ok(())))
    }
}

#[derive(Default)]
struct NeverProtocol;

impl BlockProtocol for NeverProtocol {
    type PreProcessed = String;

    fn pre_process(
        &self,
        _block: Block,
        _height: u64,
    ) -> ProtocolPreProcessFuture<Self::PreProcessed> {
        Box::pin(async move { future::pending::<Result<String, ProtocolError>>().await })
    }

    fn process<'a>(&'a mut self, _data: Self::PreProcessed, _height: u64) -> ProtocolFuture<'a> {
        Box::pin(future::ready(Ok(())))
    }

    fn rollback<'a>(&'a mut self, _: u64) -> ProtocolFuture<'a> {
        Box::pin(future::ready(Ok(())))
    }

    fn shutdown<'a>(&'a mut self) -> ProtocolFuture<'a> {
        Box::pin(future::ready(Ok(())))
    }
}

struct HeavyProtocol {
    processed: Mutex<Vec<(u64, String)>>,
    delay: Duration,
}

impl HeavyProtocol {
    fn new(delay: Duration) -> Self {
        Self {
            processed: Mutex::new(Vec::new()),
            delay,
        }
    }
}

impl BlockProtocol for HeavyProtocol {
    type PreProcessed = String;

    fn pre_process(
        &self,
        _block: Block,
        height: u64,
    ) -> ProtocolPreProcessFuture<Self::PreProcessed> {
        let delay = self.delay;
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            Ok(format!("processed-{height}"))
        })
    }

    fn process<'a>(&'a mut self, data: Self::PreProcessed, height: u64) -> ProtocolFuture<'a> {
        Box::pin(async move {
            self.processed.lock().unwrap().push((height, data));
            Ok(())
        })
    }

    fn rollback<'a>(&'a mut self, _: u64) -> ProtocolFuture<'a> {
        Box::pin(future::ready(Ok(())))
    }

    fn shutdown<'a>(&'a mut self) -> ProtocolFuture<'a> {
        Box::pin(future::ready(Ok(())))
    }
}

#[tokio::test]
async fn worker_tip_idle_backoff_is_configurable() -> Result<()> {
    let fast = measure_tip_resume_latency(Duration::from_millis(20)).await?;
    let slow = measure_tip_resume_latency(Duration::from_millis(80)).await?;

    assert!(
        fast < slow,
        "shorter backoff should resume fetching sooner (fast: {:?}, slow: {:?})",
        fast,
        slow
    );
    Ok(())
}

async fn measure_tip_resume_latency(backoff: Duration) -> Result<Duration> {
    let queue = Arc::new(OrderedBlockQueue::new());
    queue.reset_expected(1).await;

    let protocol = Arc::new(RwLock::new(TestProtocol::default()));
    let mock_client = Arc::new(MockRpcClient::new(vec![vec![(
        1,
        GENESIS_BLOCK_HEX.to_string(),
    )]]));
    let (tx, rx) = worker_control_channel(4);
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let fatal_handler = Arc::new(FatalErrorHandler::new(shutdown.clone(), shutdown));
    let activity = Arc::new(WorkerActivityTracker::new());
    let generation = Arc::new(AtomicU64::new(0));
    let (event_tx, event_rx) = mpsc::channel(4);
    drop(event_rx);
    let blockchain_tip = Arc::new(BlockchainTip::new(Some(0)));
    let shared = WorkerShared::new(WorkerSharedParams {
        protocol,
        queue,
        telemetry: Arc::new(Telemetry::default()),
        activity,
        generation,
        blockchain_tip: blockchain_tip.clone(),
        active_workers: Arc::new(AtomicUsize::new(1)),
        event_tx,
        max_batch_size_mb: 8,
        payload_limits: RpcPayloadLimits::new(usize::MAX / 2, usize::MAX / 2),
        payload_stats: Arc::new(RpcPayloadStats::default()),
    });
    let worker = Worker::with_rpc_client(
        0,
        mock_client.clone(),
        rx,
        worker_shutdown.clone(),
        fatal_handler,
        shared,
        backoff,
    );

    let handle = tokio::spawn(async move { worker.run().await.unwrap() });

    tx.send(WorkerControl::Start(1)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    let resume_start = std::time::Instant::now();
    blockchain_tip.update(10);

    timeout(Duration::from_millis(500), async {
        loop {
            if mock_client.request_count().await >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("worker should fetch once tip advances");

    let elapsed = resume_start.elapsed();

    worker_shutdown.cancel();
    let _ = tx.send(WorkerControl::Shutdown).await;
    handle.await.unwrap();
    Ok(elapsed)
}

#[tokio::test]
async fn worker_shutdown_aborts_prefetched_batch() -> Result<()> {
    let queue = Arc::new(OrderedBlockQueue::new());
    queue.reset_expected(1).await;

    let protocol = Arc::new(RwLock::new(NeverProtocol));
    let mock_client: Arc<ControlledRpcClient> = Arc::new(ControlledRpcClient::new(vec![
        ControlledResponse::Immediate(vec![(1, GENESIS_BLOCK_HEX.to_string())]),
        ControlledResponse::Pending,
    ]));

    let (tx, rx) = worker_control_channel(4);
    let shutdown = CancellationToken::new();
    let fatal_handler = Arc::new(FatalErrorHandler::new(shutdown.clone(), shutdown.clone()));
    let activity = Arc::new(WorkerActivityTracker::new());
    let generation = Arc::new(AtomicU64::new(0));
    let (event_tx, _event_rx) = mpsc::channel(4);
    let shared = WorkerShared::new(WorkerSharedParams {
        protocol,
        queue,
        telemetry: Arc::new(Telemetry::default()),
        activity,
        generation,
        blockchain_tip: Arc::new(BlockchainTip::new(Some(20))),
        active_workers: Arc::new(AtomicUsize::new(5)),
        event_tx,
        max_batch_size_mb: 8,
        payload_limits: RpcPayloadLimits::new(usize::MAX / 2, usize::MAX / 2),
        payload_stats: Arc::new(RpcPayloadStats::default()),
    });
    let worker_client: Arc<dyn BlockBatchClient> = mock_client.clone();
    let worker = Worker::with_rpc_client(
        0,
        worker_client,
        rx,
        shutdown.clone(),
        fatal_handler,
        shared,
        Duration::from_millis(5),
    );

    let handle = tokio::spawn(async move {
        worker.run().await.unwrap();
    });

    tx.send(WorkerControl::Start(1)).await.unwrap();

    timeout(Duration::from_millis(500), async {
        loop {
            if mock_client.as_ref().request_count().await >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("second RPC should start before shutdown");

    shutdown.cancel();
    let _ = tx.send(WorkerControl::Shutdown).await;

    timeout(Duration::from_millis(500), handle)
        .await
        .expect("worker should exit after aborting prefetched batch")
        .unwrap();

    Ok(())
}

#[tokio::test]
async fn double_buffering_reduces_elapsed_time() -> Result<()> {
    let queue = Arc::new(OrderedBlockQueue::new());
    queue.reset_expected(1).await;

    let protocol = Arc::new(RwLock::new(HeavyProtocol::new(Duration::from_millis(40))));
    let mock_client = Arc::new(SleepyRpcClient::new(
        Duration::from_millis(40),
        vec![
            vec![(1, GENESIS_BLOCK_HEX.to_string())],
            vec![
                (6, GENESIS_BLOCK_HEX.to_string()),
                (11, GENESIS_BLOCK_HEX.to_string()),
            ],
        ],
    ));

    let (tx, rx) = worker_control_channel(4);
    let shutdown = CancellationToken::new();
    let fatal_handler = Arc::new(FatalErrorHandler::new(shutdown.clone(), shutdown.clone()));
    let activity = Arc::new(WorkerActivityTracker::new());
    let generation = Arc::new(AtomicU64::new(0));
    let (event_tx, _event_rx) = mpsc::channel(4);
    let shared = WorkerShared::new(WorkerSharedParams {
        protocol,
        queue: queue.clone(),
        telemetry: Arc::new(Telemetry::default()),
        activity,
        generation,
        blockchain_tip: Arc::new(BlockchainTip::new(Some(20))),
        active_workers: Arc::new(AtomicUsize::new(5)),
        event_tx,
        max_batch_size_mb: 8,
        payload_limits: RpcPayloadLimits::new(usize::MAX / 2, usize::MAX / 2),
        payload_stats: Arc::new(RpcPayloadStats::default()),
    });
    let worker = Worker::with_rpc_client(
        0,
        mock_client,
        rx,
        shutdown.clone(),
        fatal_handler,
        shared,
        Duration::from_millis(5),
    );

    let handle = tokio::spawn(async move {
        worker.run().await.unwrap();
    });

    let start = Instant::now();
    tx.send(WorkerControl::Start(1)).await.unwrap();

    timeout(Duration::from_secs(2), async {
        loop {
            if queue.len().await >= 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("worker should pre-process three blocks");
    let elapsed = start.elapsed();

    let _ = tx.send(WorkerControl::Shutdown).await;
    shutdown.cancel();
    handle.await.unwrap();

    assert!(
        elapsed < Duration::from_millis(225),
        "double buffering should complete two batches quickly (elapsed: {:?})",
        elapsed
    );

    Ok(())
}

#[tokio::test]
async fn process_blocks_accounts_for_preprocessed_bytes() -> Result<()> {
    let queue = Arc::new(OrderedBlockQueue::new());
    queue.reset_expected(0).await;

    let protocol = Arc::new(RwLock::new(InflatingProtocol::new(4 * 1_024)));
    let mock_client = Arc::new(MockRpcClient::new(Vec::new()));
    let (tx, rx) = worker_control_channel(1);
    drop(tx);
    let shutdown = CancellationToken::new();
    let fatal_handler = Arc::new(FatalErrorHandler::new(shutdown.clone(), shutdown.clone()));
    let activity = Arc::new(WorkerActivityTracker::new());
    let generation = Arc::new(AtomicU64::new(0));
    let (event_tx, event_rx) = mpsc::channel(1);
    drop(event_rx);
    let shared = WorkerShared::new(WorkerSharedParams {
        protocol,
        queue: queue.clone(),
        telemetry: Arc::new(Telemetry::default()),
        activity,
        generation: generation.clone(),
        blockchain_tip: Arc::new(BlockchainTip::new(Some(10))),
        active_workers: Arc::new(AtomicUsize::new(1)),
        event_tx,
        max_batch_size_mb: 8,
        payload_limits: RpcPayloadLimits::new(usize::MAX / 2, usize::MAX / 2),
        payload_stats: Arc::new(RpcPayloadStats::default()),
    });
    let worker = Worker::with_rpc_client(
        0,
        mock_client,
        rx,
        shutdown,
        fatal_handler,
        shared,
        Duration::from_millis(5),
    );

    let outcome = worker
        .process_blocks(0, vec![(0, GENESIS_BLOCK_HEX.to_string())])
        .await?;

    match outcome {
        ProcessingOutcome::Completed(total_bytes) => {
            assert!(
                total_bytes >= 4 * 1_024,
                "pre-processed payload should dominate reported bytes"
            );
            assert_eq!(
                queue.bytes().await,
                total_bytes,
                "queue accounting should match reported batch bytes"
            );
        }
        other => panic!("expected completed outcome, got {:?}", other),
    }

    Ok(())
}

struct InflatingProtocol {
    payload_size: usize,
}

impl InflatingProtocol {
    fn new(payload_size: usize) -> Self {
        Self { payload_size }
    }
}

impl BlockProtocol for InflatingProtocol {
    type PreProcessed = Vec<u8>;

    fn pre_process(
        &self,
        _block: Block,
        _height: u64,
    ) -> ProtocolPreProcessFuture<Self::PreProcessed> {
        let len = self.payload_size;
        Box::pin(async move { Ok(vec![0u8; len]) })
    }

    fn process<'a>(&'a mut self, _data: Self::PreProcessed, _height: u64) -> ProtocolFuture<'a> {
        Box::pin(future::ready(Ok(())))
    }

    fn rollback<'a>(&'a mut self, _: u64) -> ProtocolFuture<'a> {
        Box::pin(future::ready(Ok(())))
    }

    fn shutdown<'a>(&'a mut self) -> ProtocolFuture<'a> {
        Box::pin(future::ready(Ok(())))
    }
}
