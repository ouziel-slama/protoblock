use super::block::PreProcessedBlock;
use std::collections::HashMap;
use tokio::sync::{Mutex, Notify};

pub const BYTES_PER_MEGABYTE: usize = 1_048_576;

struct QueueEntry<T> {
    block: PreProcessedBlock<T>,
    size_bytes: usize,
}

struct QueueState<T> {
    next_expected: u64,
    blocks: HashMap<u64, QueueEntry<T>>,
    total_bytes: usize,
}

impl<T> QueueState<T> {
    fn new(next_expected: u64) -> Self {
        Self {
            next_expected,
            blocks: HashMap::new(),
            total_bytes: 0,
        }
    }
}

/// Async queue that only releases blocks in the expected order.
pub struct OrderedBlockQueue<T> {
    state: Mutex<QueueState<T>>,
    notify: Notify,
    max_bytes: usize,
}

impl<T> OrderedBlockQueue<T> {
    pub fn new() -> Self {
        Self::with_start_and_capacity(0, usize::MAX)
    }

    pub fn with_start(next_expected: u64) -> Self {
        Self::with_start_and_capacity(next_expected, usize::MAX)
    }

    pub fn with_capacity(max_bytes: usize) -> Self {
        Self::with_start_and_capacity(0, max_bytes)
    }

    pub fn with_start_and_capacity(next_expected: u64, max_bytes: usize) -> Self {
        assert!(max_bytes > 0, "max_bytes must be greater than zero");
        Self {
            state: Mutex::new(QueueState::new(next_expected)),
            notify: Notify::new(),
            max_bytes,
        }
    }

    pub async fn push(&self, block: PreProcessedBlock<T>, size_bytes: usize) {
        let mut pending_block = Some(block);
        loop {
            let notified = self.notify.notified();
            let mut state = self.state.lock().await;
            let height = pending_block
                .as_ref()
                .expect("pending block should exist before enqueue")
                .height();
            let prospective_bytes = state.total_bytes.saturating_add(size_bytes);
            let queue_empty = state.blocks.is_empty();
            let is_next_expected = height == state.next_expected;
            if prospective_bytes <= self.max_bytes || queue_empty || is_next_expected {
                let block = pending_block
                    .take()
                    .expect("block should only be enqueued once");
                state
                    .blocks
                    .insert(height, QueueEntry { block, size_bytes });
                state.total_bytes = prospective_bytes;
                drop(state);
                self.notify.notify_waiters();
                return;
            }
            drop(state);
            notified.await;
        }
    }

    pub async fn pop_next(&self) -> PreProcessedBlock<T> {
        loop {
            if let Some(block) = self.try_pop_next().await {
                self.notify.notify_waiters();
                return block;
            }
            #[cfg(test)]
            {
                test_hooks::pause_in_gap().await;
            }
            let notified = self.notify.notified();
            if let Some(block) = self.try_pop_next().await {
                self.notify.notify_waiters();
                return block;
            }
            notified.await;
        }
    }

    pub async fn try_pop_next(&self) -> Option<PreProcessedBlock<T>> {
        let mut state = self.state.lock().await;
        let expected = state.next_expected;
        let block = state.blocks.remove(&expected);
        if let Some(entry) = block {
            state.next_expected += 1;
            state.total_bytes = state.total_bytes.saturating_sub(entry.size_bytes);
            Some(entry.block)
        } else {
            None
        }
    }

    pub async fn clear(&self) {
        let mut state = self.state.lock().await;
        state.blocks.clear();
        state.total_bytes = 0;
        drop(state);
        self.notify.notify_waiters();
    }

    pub async fn reset_expected(&self, height: u64) {
        let mut state = self.state.lock().await;
        let previous_expected = state.next_expected;
        state.next_expected = height;

        if height < previous_expected {
            // Rewinding to an earlier height: drop any future blocks that were derived from a
            // now-stale branch so callers can safely replay from `height`.
            let mut removed_bytes = 0usize;
            state.blocks.retain(|&existing_height, entry| {
                let keep = existing_height < height;
                if !keep {
                    removed_bytes = removed_bytes.saturating_add(entry.size_bytes);
                }
                keep
            });
            state.total_bytes = state.total_bytes.saturating_sub(removed_bytes);
        } else {
            // Fast-forwarding: discard already-obsolete entries below the new starting point so we
            // only keep blocks that are at or beyond the requested height.
            let mut removed_bytes = 0usize;
            state.blocks.retain(|&existing_height, entry| {
                let keep = existing_height >= height;
                if !keep {
                    removed_bytes = removed_bytes.saturating_add(entry.size_bytes);
                }
                keep
            });
            state.total_bytes = state.total_bytes.saturating_sub(removed_bytes);
        }

        drop(state);
        self.notify.notify_waiters();
    }

    pub async fn len(&self) -> usize {
        self.state.lock().await.blocks.len()
    }

    pub async fn bytes(&self) -> usize {
        self.state.lock().await.total_bytes
    }

    pub async fn is_empty(&self) -> bool {
        self.state.lock().await.blocks.is_empty()
    }

    pub async fn has_ready_block(&self) -> bool {
        let state = self.state.lock().await;
        state.blocks.contains_key(&state.next_expected)
    }

    pub async fn next_expected(&self) -> u64 {
        self.state.lock().await.next_expected
    }
}

impl<T> Default for OrderedBlockQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
pub(super) mod test_hooks {
    use once_cell::sync::Lazy;
    use std::sync::{Arc, Mutex};
    use tokio::sync::{oneshot, Notify};

    #[derive(Clone)]
    pub struct GapProbe {
        pub entered_signal: Arc<Mutex<Option<oneshot::Sender<()>>>>,
        pub resume: Arc<Notify>,
    }

    static GAP_PROBE: Lazy<Mutex<Option<GapProbe>>> = Lazy::new(|| Mutex::new(None));

    pub struct GapProbeGuard;

    impl Drop for GapProbeGuard {
        fn drop(&mut self) {
            GAP_PROBE.lock().unwrap().take();
        }
    }

    pub fn install_gap_probe(probe: GapProbe) -> GapProbeGuard {
        *GAP_PROBE.lock().unwrap() = Some(probe);
        GapProbeGuard
    }

    pub async fn pause_in_gap() {
        let probe = { GAP_PROBE.lock().unwrap().clone() };

        if let Some(probe) = probe {
            if let Some(sender) = probe.entered_signal.lock().unwrap().take() {
                let _ = sender.send(());
            }
            probe.resume.notified().await;

            // Ensure the probe only pauses a single gap so other tests are not impacted.
            let mut guard = GAP_PROBE.lock().unwrap();
            let same_probe = guard
                .as_ref()
                .map(|current| {
                    Arc::ptr_eq(&current.entered_signal, &probe.entered_signal)
                        && Arc::ptr_eq(&current.resume, &probe.resume)
                })
                .unwrap_or(false);

            if same_probe {
                guard.take();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::preprocessors::block::PreProcessedBlock;
    use bitcoin::hashes::Hash;
    use bitcoin::BlockHash;
    use std::sync::{Arc, Mutex};
    use tokio::sync::oneshot;
    use tokio::time::{sleep, timeout, Duration};

    fn dummy_hash(seed: u8) -> BlockHash {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        BlockHash::from_slice(&bytes).expect("valid hash")
    }

    fn make_block(height: u64) -> PreProcessedBlock<String> {
        PreProcessedBlock::new(
            height,
            dummy_hash(height as u8),
            dummy_hash((height + 1) as u8),
            format!("data-{height}"),
        )
    }

    const TEST_BLOCK_BYTES: usize = 64;

    #[tokio::test]
    async fn pop_next_returns_in_order() {
        let queue = OrderedBlockQueue::new();
        queue.reset_expected(10).await;

        queue.push(make_block(12), TEST_BLOCK_BYTES).await;
        queue.push(make_block(11), TEST_BLOCK_BYTES).await;
        queue.push(make_block(10), TEST_BLOCK_BYTES).await;

        assert_eq!(queue.pop_next().await.height(), 10);
        assert_eq!(queue.pop_next().await.height(), 11);
        assert_eq!(queue.pop_next().await.height(), 12);
    }

    #[tokio::test]
    async fn try_pop_next_non_blocking() {
        let queue = OrderedBlockQueue::with_start(5);
        assert!(queue.try_pop_next().await.is_none());

        queue.push(make_block(5), TEST_BLOCK_BYTES).await;
        assert!(queue.try_pop_next().await.is_some());
        assert!(queue.try_pop_next().await.is_none());
    }

    #[tokio::test]
    async fn pop_next_blocks_until_ready() {
        let queue = Arc::new(OrderedBlockQueue::new());
        let cloned = queue.clone();

        let pop_future = tokio::spawn(async move { cloned.pop_next().await.height() });

        sleep(Duration::from_millis(25)).await;
        assert!(!pop_future.is_finished());

        queue.push(make_block(0), TEST_BLOCK_BYTES).await;

        let height = timeout(Duration::from_millis(250), pop_future)
            .await
            .expect("pop should finish")
            .expect("task should not fail");

        assert_eq!(height, 0);
    }

    #[tokio::test]
    async fn reset_expected_drops_future_blocks_when_rewinding() {
        let queue = OrderedBlockQueue::with_start(5);
        queue.push(make_block(5), TEST_BLOCK_BYTES).await;
        queue.push(make_block(6), TEST_BLOCK_BYTES).await;
        queue.push(make_block(10), TEST_BLOCK_BYTES).await;

        queue.reset_expected(3).await;

        assert_eq!(queue.len().await, 0, "future entries should be dropped");
        assert_eq!(queue.bytes().await, 0, "bytes should drop after rewind");
        assert!(
            queue.try_pop_next().await.is_none(),
            "queue should be empty"
        );
    }

    #[tokio::test]
    async fn reset_expected_keeps_future_blocks_when_fast_forwarding() {
        let queue = OrderedBlockQueue::with_start(0);
        queue.push(make_block(2), TEST_BLOCK_BYTES).await;
        queue.push(make_block(6), TEST_BLOCK_BYTES).await;

        queue.reset_expected(6).await;

        assert_eq!(
            queue.bytes().await,
            TEST_BLOCK_BYTES,
            "byte usage should match retained block"
        );
        let next = queue
            .try_pop_next()
            .await
            .expect("block at height 6 should remain");
        assert_eq!(next.height(), 6);
        assert_eq!(
            queue.len().await,
            0,
            "older entries should have been dropped"
        );
        assert_eq!(
            queue.bytes().await,
            0,
            "bytes should drop after draining retained block"
        );
    }

    #[tokio::test]
    async fn bytes_reflect_pending_payload() {
        let queue = OrderedBlockQueue::with_start(0);
        queue
            .push(make_block(0), TEST_BLOCK_BYTES.saturating_mul(2))
            .await;
        queue
            .push(make_block(1), TEST_BLOCK_BYTES.saturating_mul(3))
            .await;

        assert_eq!(queue.bytes().await, TEST_BLOCK_BYTES * 5);

        let _ = queue.pop_next().await;
        assert_eq!(queue.bytes().await, TEST_BLOCK_BYTES * 3);
    }

    #[tokio::test]
    async fn push_waits_when_queue_is_full() {
        let queue = Arc::new(OrderedBlockQueue::with_capacity(BYTES_PER_MEGABYTE));
        queue.reset_expected(0).await;

        queue.push(make_block(0), BYTES_PER_MEGABYTE).await;

        let cloned = queue.clone();
        let push_future = tokio::spawn(async move {
            cloned.push(make_block(1), BYTES_PER_MEGABYTE).await;
        });

        sleep(Duration::from_millis(25)).await;
        assert!(
            !push_future.is_finished(),
            "producer should wait while the queue is full"
        );

        assert_eq!(queue.pop_next().await.height(), 0);
        push_future.await.expect("push task should not panic");
        assert_eq!(queue.pop_next().await.height(), 1);
    }

    #[tokio::test]
    async fn next_expected_block_bypasses_capacity_limit() {
        let capacity = TEST_BLOCK_BYTES * 2;
        let queue = Arc::new(OrderedBlockQueue::with_capacity(capacity));
        queue.reset_expected(0).await;

        queue.push(make_block(1), TEST_BLOCK_BYTES).await;
        queue.push(make_block(2), TEST_BLOCK_BYTES).await;
        assert_eq!(queue.bytes().await, capacity);

        let cloned = queue.clone();
        let push_future = tokio::spawn(async move {
            cloned.push(make_block(0), TEST_BLOCK_BYTES).await;
        });

        timeout(Duration::from_millis(250), push_future)
            .await
            .expect("next-expected push should bypass byte cap")
            .expect("push task should not panic");

        assert_eq!(queue.bytes().await, capacity + TEST_BLOCK_BYTES);
        assert_eq!(queue.pop_next().await.height(), 0);
        assert_eq!(queue.bytes().await, capacity);
    }

    #[tokio::test]
    async fn queue_budget_tracks_preprocessed_payload() {
        let capacity = 1_024usize;
        let queue = Arc::new(OrderedBlockQueue::with_capacity(capacity));
        queue.reset_expected(0).await;

        let oversized_data = "x".repeat(capacity.saturating_mul(2));
        let block = PreProcessedBlock::new(0, dummy_hash(1), dummy_hash(2), oversized_data.clone());
        let block_bytes = block.queue_bytes();
        assert!(
            block_bytes > capacity,
            "pre-processed payload should exceed queue capacity"
        );
        queue.push(block, block_bytes).await;

        let cloned = queue.clone();
        let push_future = tokio::spawn(async move {
            let next = PreProcessedBlock::new(1, dummy_hash(3), dummy_hash(4), oversized_data);
            let next_bytes = next.queue_bytes();
            cloned.push(next, next_bytes).await;
        });

        sleep(Duration::from_millis(25)).await;
        assert!(
            !push_future.is_finished(),
            "queue should exert backpressure while full"
        );

        assert_eq!(queue.pop_next().await.height(), 0);
        push_future
            .await
            .expect("push task should resume once capacity frees");
        assert_eq!(queue.pop_next().await.height(), 1);
    }

    #[tokio::test]
    async fn oversized_block_still_fits_when_queue_is_empty() {
        let queue = OrderedBlockQueue::with_capacity(BYTES_PER_MEGABYTE);
        queue.reset_expected(0).await;

        let oversized = BYTES_PER_MEGABYTE.saturating_mul(2);
        queue.push(make_block(0), oversized).await;
        assert_eq!(queue.bytes().await, oversized);

        let block = queue.pop_next().await;
        assert_eq!(block.height(), 0);
        assert_eq!(queue.bytes().await, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pop_next_rechecks_after_registering_waiter() {
        let queue = Arc::new(OrderedBlockQueue::new());
        queue.reset_expected(0).await;

        let resume = Arc::new(Notify::new());
        let (entered_tx, entered_rx) = oneshot::channel();
        let _probe_guard = super::test_hooks::install_gap_probe(super::test_hooks::GapProbe {
            entered_signal: Arc::new(Mutex::new(Some(entered_tx))),
            resume: resume.clone(),
        });

        let cloned = queue.clone();
        let pop_future = tokio::spawn(async move { cloned.pop_next().await.height() });

        entered_rx
            .await
            .expect("gap probe should signal waiter registration");
        queue.push(make_block(0), TEST_BLOCK_BYTES).await;
        resume.notify_waiters();

        let height = timeout(Duration::from_millis(250), pop_future)
            .await
            .expect("pop should finish")
            .expect("task should not fail");
        assert_eq!(height, 0);
    }
}
