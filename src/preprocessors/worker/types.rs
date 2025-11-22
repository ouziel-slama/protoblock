use crate::rpc::BlockBatchClient;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Duration;

pub(super) const DEFAULT_HEIGHT_STEP: usize = 1;
const WORKER_INITIAL_BACKOFF_MS: u64 = 250;
const WORKER_MAX_BACKOFF_MS: u64 = 2_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerControl {
    Start(u64),
    Stop,
    Shutdown,
}

#[derive(Debug, Clone)]
pub enum FetcherEvent {
    TipRegressed {
        new_tip: u64,
    },
    HeightOutOfRange {
        worker_id: usize,
        missing_height: u64,
        available_tip: u64,
    },
}

pub type FetcherEventSender = mpsc::Sender<FetcherEvent>;
pub type WorkerControlSender = mpsc::Sender<WorkerControl>;
pub type WorkerControlReceiver = mpsc::Receiver<WorkerControl>;

pub fn worker_control_channel(capacity: usize) -> (WorkerControlSender, WorkerControlReceiver) {
    mpsc::channel(capacity)
}

#[derive(Debug)]
pub(super) enum ProcessingOutcome {
    Completed(usize),
    Cancelled,
    Stale,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct BatchContext {
    pub(super) start_height: u64,
    pub(super) stride: u64,
    pub(super) requested: usize,
    pub(super) epoch: u64,
}

impl BatchContext {
    pub(super) fn next_start_height(&self) -> u64 {
        let processed = self.requested as u64;
        let step = self.stride.saturating_mul(processed);
        self.start_height.saturating_add(step)
    }
}

#[derive(Debug)]
pub(super) struct BatchPlan {
    pub(super) context: BatchContext,
    pub(super) heights: Vec<u64>,
}

pub(super) struct PendingBatch {
    pub(super) context: BatchContext,
    pub(super) heights: Arc<[u64]>,
    pub(super) handle: JoinHandle<Result<Vec<(u64, String)>>>,
}

impl PendingBatch {
    pub(super) fn new(plan: BatchPlan, rpc_client: Arc<dyn BlockBatchClient>) -> Self {
        let heights: Arc<[u64]> = Arc::from(plan.heights);
        let heights_for_task = Arc::clone(&heights);
        let handle =
            tokio::spawn(async move { rpc_client.batch_get_blocks(&heights_for_task).await });

        Self {
            context: plan.context,
            heights,
            handle,
        }
    }

    pub(super) fn abort(self) {
        self.handle.abort();
    }
}

pub(super) struct ReadyBatch {
    pub(super) context: BatchContext,
    pub(super) blocks: Vec<(u64, String)>,
}

pub(super) enum PlanResult {
    Ready(BatchPlan),
    AwaitTip,
    Exhausted,
}

pub(super) enum FetchWaitOutcome {
    Ready(ReadyBatch),
    Restart(u64),
    Start(u64),
    Stop,
    Shutdown,
}

pub(super) enum IdleDecision {
    Retry,
    Start(u64),
    Stop,
    Shutdown,
}

pub(super) fn retry_delay(attempt: usize) -> Duration {
    if attempt == 0 {
        return Duration::from_millis(WORKER_INITIAL_BACKOFF_MS);
    }

    let exponent = attempt.saturating_sub(1).min(8) as u32;
    let mut delay_ms = WORKER_INITIAL_BACKOFF_MS.saturating_mul(1u64 << exponent);
    if delay_ms > WORKER_MAX_BACKOFF_MS {
        delay_ms = WORKER_MAX_BACKOFF_MS;
    }

    Duration::from_millis(delay_ms)
}

pub(super) fn worker_stride(thread_count: usize, step: usize) -> u64 {
    let stride = thread_count.max(1) as u64;
    let step = step.max(1) as u64;
    stride.saturating_mul(step)
}
