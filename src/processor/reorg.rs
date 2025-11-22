//! Reorg coordination for `BlocksFetcher`.

use super::backoff::{retry_with_backoff, RetryBackoff, RetryDisposition};
use super::fetcher::{BlockAction, ProcessingHandles};
use super::worker_pool::{broadcast_control_signal, start_workers_subset};
use crate::preprocessors::block::PreProcessedBlock;
use crate::preprocessors::worker::{FetcherEvent, WorkerControl};
use crate::processor::tip::BlockchainTip;
use crate::rpc::{AsyncRpcClient, RpcError};
use crate::runtime::config::FetcherConfig;
use crate::runtime::hooks::HookDecision;
use crate::runtime::protocol::{BlockProtocol, ProtocolError, ProtocolStage};
use anyhow::{anyhow, Context, Result};
use bitcoin::BlockHash;
use std::collections::VecDeque;
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

// Reorg-related RPC requests share the same retry/backoff helper to keep timings,
// cancellation, and logging consistent across seeding and recovery paths.
const REORG_RECOVERY_INITIAL_BACKOFF_MS: u64 = 250;
const REORG_RECOVERY_MAX_BACKOFF_MS: u64 = 2_000;
const REORG_SEED_MAX_ATTEMPTS: usize = 12;

/// Sliding window of block hashes used during reorg detection.
#[derive(Debug, Clone)]
pub struct ReorgWindow {
    limit: usize,
    items: VecDeque<(u64, BlockHash)>,
}

impl ReorgWindow {
    pub fn new(limit: usize) -> Self {
        Self {
            limit: limit.max(1),
            items: VecDeque::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn push(&mut self, height: u64, hash: BlockHash) {
        self.items.push_back((height, hash));
        if self.items.len() > self.limit {
            self.items.pop_front();
        }
    }

    pub fn find_hash(&self, hash: &BlockHash) -> Option<u64> {
        self.items
            .iter()
            .rev()
            .find(|(_, existing_hash)| existing_hash == hash)
            .map(|(height, _)| *height)
    }

    pub fn clear(&mut self) {
        self.items.clear();
    }

    /// Removes entries whose height is greater than the provided value while keeping
    /// the older portion of the window intact.
    pub fn truncate_after(&mut self, height: u64) {
        while matches!(self.items.back(), Some((existing_height, _)) if *existing_height > height) {
            self.items.pop_back();
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &(u64, BlockHash)> {
        self.items.iter()
    }
}

pub(crate) struct ReorgManager<P: BlockProtocol> {
    handles: ProcessingHandles<P>,
}

impl<P: BlockProtocol> ReorgManager<P> {
    pub(crate) fn new(handles: ProcessingHandles<P>) -> Self {
        Self { handles }
    }

    pub(crate) async fn handle_block(
        &self,
        block: PreProcessedBlock<P::PreProcessed>,
        reorg_window: &mut ReorgWindow,
        next_expected: &mut u64,
        shutdown: &CancellationToken,
    ) -> Result<BlockAction> {
        if Self::is_reorg(reorg_window, &block) {
            self.handle_reorg(block, reorg_window, next_expected, shutdown)
                .await?;
            return Ok(BlockAction::Processed);
        }

        let height = block.height();
        let hash = *block.block_hash();
        let data = block.into_inner();

        {
            let mut guard = self.handles.protocol.write().await;
            match tokio::select! {
                result = guard.process(data, height) => HookDecision::Finished(result),
                _ = shutdown.cancelled() => HookDecision::Cancelled,
            } {
                HookDecision::Finished(Ok(())) => {}
                HookDecision::Finished(Err(error)) => {
                    return Err(self.handles.fatal_handler.trigger(error));
                }
                HookDecision::Cancelled => {
                    return Ok(BlockAction::Cancelled);
                }
            }
        }

        reorg_window.push(height, hash);
        *next_expected = height.saturating_add(1);
        self.handles.progress.mark_confirmed(height);
        self.handles.telemetry.record_processed_blocks(1);

        Ok(BlockAction::Processed)
    }

    pub(crate) async fn handle_event(
        &self,
        event: FetcherEvent,
        reorg_window: &mut ReorgWindow,
        next_expected: &mut u64,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        match event {
            FetcherEvent::HeightOutOfRange { available_tip, .. } => {
                self.handle_tip_regression_if_needed(
                    available_tip,
                    reorg_window,
                    next_expected,
                    true,
                    shutdown,
                )
                .await?;
            }
            FetcherEvent::TipRegressed { new_tip } => {
                self.handle_tip_regression_if_needed(
                    new_tip,
                    reorg_window,
                    next_expected,
                    false,
                    shutdown,
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn handle_tip_regression_if_needed(
        &self,
        available_tip: u64,
        reorg_window: &mut ReorgWindow,
        next_expected: &mut u64,
        refresh_tip: bool,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        let Some(last_confirmed) = self.handles.progress.last_confirmed() else {
            return Ok(());
        };

        let mut effective_tip = available_tip;

        if last_confirmed <= effective_tip && refresh_tip {
            match tokio::select! {
                _ = shutdown.cancelled() => Err(anyhow!("tip refresh cancelled")),
                result = self.handles.rpc_client.get_blockchain_tip() => result,
            } {
                Ok(latest_tip) => {
                    self.handles.blockchain_tip.update(latest_tip);
                    effective_tip = latest_tip;
                    tracing::debug!(
                        last_confirmed,
                        observed_tip = available_tip,
                        latest_tip,
                        "refreshed blockchain tip after height miss"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        last_confirmed,
                        observed_tip = available_tip,
                        error = %err,
                        "failed to refresh blockchain tip after height miss"
                    );
                }
            }
        }

        if last_confirmed <= effective_tip {
            return Ok(());
        }

        self.recover_from_reorg(
            reorg_window,
            next_expected,
            ReorgCause::TipRegression {
                available_tip: effective_tip,
                last_confirmed,
            },
            shutdown,
        )
        .await
    }

    async fn handle_reorg(
        &self,
        conflicting_block: PreProcessedBlock<P::PreProcessed>,
        reorg_window: &mut ReorgWindow,
        next_expected: &mut u64,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        let cause = ReorgCause::ConflictingBlock {
            height: conflicting_block.height(),
            prev_hash: *conflicting_block.previous_hash(),
        };
        self.recover_from_reorg(reorg_window, next_expected, cause, shutdown)
            .await
    }

    async fn recover_from_reorg(
        &self,
        reorg_window: &mut ReorgWindow,
        next_expected: &mut u64,
        cause: ReorgCause,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        let fork_height = match find_canonical_fork_height_with_retry(
            reorg_window,
            self.handles.rpc_client.as_ref(),
            shutdown,
        )
        .await
        .context("failed to locate canonical ancestor for reorg")?
        {
            Some(height) => height,
            None => {
                let detail = match &cause {
                    ReorgCause::ConflictingBlock { height, .. } => {
                        format!("conflicting_height={height}")
                    }
                    ReorgCause::TipRegression {
                        available_tip,
                        last_confirmed,
                    } => format!("available_tip={available_tip}, last_confirmed={last_confirmed}"),
                };
                let rollback_error = ProtocolError::new(
                    ProtocolStage::Rollback,
                    anyhow!(
                        "unable to locate canonical ancestor within reorg window (window_size={}, {})",
                        reorg_window.len(),
                        detail,
                    ),
                );
                return Err(self.handles.fatal_handler.trigger(rollback_error));
            }
        };

        let reorg_depth = reorg_window
            .iter()
            .filter(|(height, _)| *height > fork_height)
            .count();
        let queue_blocks = self.handles.queue.len().await;
        let queue_bytes = self.handles.queue.bytes().await;

        match &cause {
            ReorgCause::ConflictingBlock { height, prev_hash } => {
                tracing::warn!(
                    height,
                    fork_height,
                    prev = %prev_hash,
                    reorg_depth,
                    queue_blocks,
                    queue_bytes,
                    "reorg detected; restarting workers from canonical ancestor"
                );
            }
            ReorgCause::TipRegression {
                available_tip,
                last_confirmed,
            } => {
                tracing::warn!(
                    available_tip,
                    last_confirmed,
                    fork_height,
                    reorg_depth,
                    queue_blocks,
                    queue_bytes,
                    "tip regressed below confirmed height; revalidating canonical chain"
                );
            }
        }

        let new_generation = self
            .handles
            .generation
            .fetch_add(1, Ordering::SeqCst)
            .wrapping_add(1);
        tracing::debug!(
            generation = new_generation,
            "bumped worker generation due to reorg"
        );

        broadcast_control_signal(self.handles.control_channels(), WorkerControl::Stop).await;
        self.handles
            .activity
            .wait_until_idle_with(|| async {
                self.handles.queue.clear().await;
            })
            .await;

        self.handles.queue.clear().await;
        let resume_height = fork_height.saturating_add(1);
        self.handles.queue.reset_expected(resume_height).await;

        {
            let mut guard = self.handles.protocol.write().await;
            if let Err(error) = guard.rollback(fork_height).await {
                return Err(self.handles.fatal_handler.trigger(error));
            }
        }

        reorg_window.truncate_after(fork_height);
        *next_expected = resume_height;
        self.handles.progress.mark_confirmed(fork_height);
        if self.handles.tip_wait_mode.load(Ordering::SeqCst) {
            self.handles.active_workers.store(1, Ordering::SeqCst);
            self.handles.telemetry.record_worker_pool_size(1);
            start_workers_subset(self.handles.control_channels(), resume_height, Some(1)).await?;
        } else {
            let restored = self.handles.max_workers.max(1);
            self.handles
                .active_workers
                .store(restored, Ordering::SeqCst);
            self.handles.telemetry.record_worker_pool_size(restored);
            start_workers_subset(self.handles.control_channels(), resume_height, None).await?;
        }

        tracing::info!(
            resume_height,
            fork_height,
            reorg_depth,
            "reorg recovery complete"
        );

        Ok(())
    }

    fn is_reorg(reorg_window: &ReorgWindow, block: &PreProcessedBlock<P::PreProcessed>) -> bool {
        if reorg_window.is_empty() {
            return false;
        }

        match reorg_window.iter().last() {
            Some((_, last_hash)) => block.previous_hash() != last_hash,
            None => false,
        }
    }
}

pub(crate) async fn seed_reorg_window(
    config: &FetcherConfig,
    start_height: u64,
    rpc_client: &Arc<AsyncRpcClient>,
    blockchain_tip: &Arc<BlockchainTip>,
    tip_hint: Option<u64>,
    shutdown: CancellationToken,
) -> Result<ReorgWindow> {
    let mut window = ReorgWindow::new(config.reorg_window_size());
    if start_height == 0 {
        return Ok(window);
    }

    let max_entries = window.limit();
    if max_entries == 0 {
        return Ok(window);
    }

    let mut tip_hint = tip_hint;
    'seed: loop {
        let heights = compute_reorg_seed_heights(start_height, max_entries, tip_hint);
        if heights.is_empty() {
            return Ok(window);
        }

        let mut attempts_used = 0;
        let fetch_result = retry_with_backoff(
            RetryBackoff::new(
                Duration::from_millis(REORG_RECOVERY_INITIAL_BACKOFF_MS),
                Duration::from_millis(REORG_RECOVERY_MAX_BACKOFF_MS),
            )
            .with_max_attempts(REORG_SEED_MAX_ATTEMPTS)
            .with_cancellation(&shutdown),
            |attempt| {
                attempts_used = attempt;
                let rpc_client = Arc::clone(rpc_client);
                let shutdown = shutdown.clone();
                let heights = heights.clone();
                async move {
                    tokio::select! {
                        _ = shutdown.cancelled() => Err(anyhow::anyhow!("reorg window seeding cancelled")),
                        result = rpc_client.batch_get_block_hashes(&heights) => result,
                    }
                }
            },
            |attempt, backoff, err, will_retry| {
                if will_retry {
                    tracing::warn!(
                        attempt,
                        max_attempts = REORG_SEED_MAX_ATTEMPTS,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %err,
                        start_height,
                        "failed to seed reorg window; retrying"
                    );
                } else {
                    tracing::error!(
                        attempt,
                        max_attempts = REORG_SEED_MAX_ATTEMPTS,
                        error = %err,
                        start_height,
                        "failed to seed reorg window; giving up"
                    );
                }
            },
            |_, err| {
                if let Some(rpc_err) = err.downcast_ref::<RpcError>() {
                    if matches!(rpc_err, RpcError::HeightOutOfRange { .. }) {
                        return RetryDisposition::Abort;
                    }
                }
                RetryDisposition::Retry
            },
        )
        .await;

        match fetch_result {
            Ok(hashes) => {
                for (height, hash) in heights.iter().copied().zip(hashes.into_iter()) {
                    window.push(height, hash);
                }

                tracing::info!(
                    start_height,
                    seeded = window.len(),
                    attempts = attempts_used,
                    "reorg window seeded with historical hashes"
                );

                return Ok(window);
            }
            Err(err) => {
                if let Some(RpcError::HeightOutOfRange { height }) = err.downcast_ref::<RpcError>()
                {
                    let available_tip = height.saturating_sub(1);
                    if tip_hint == Some(available_tip) {
                        return Err(err).context("failed to fetch historical block hashes");
                    }

                    tracing::info!(
                        start_height,
                        missing_height = *height,
                        available_tip,
                        "reorg window seeding requested future block; clamping to node tip"
                    );

                    tip_hint = Some(available_tip);
                    blockchain_tip.update(available_tip);
                    continue 'seed;
                }

                return Err(err)
                    .context("failed to fetch historical block hashes after repeated attempts");
            }
        }
    }
}

pub(crate) fn compute_reorg_seed_heights(
    start_height: u64,
    max_entries: usize,
    tip_hint: Option<u64>,
) -> Vec<u64> {
    if start_height == 0 || max_entries == 0 {
        return Vec::new();
    }

    let highest_requested = start_height.saturating_sub(1);
    let highest_available = tip_hint
        .map(|tip| tip.min(highest_requested))
        .unwrap_or(highest_requested);

    let available = highest_available.saturating_add(1);
    if available == 0 {
        return Vec::new();
    }

    let desired_entries = std::cmp::min(max_entries as u64, available);
    if desired_entries == 0 {
        return Vec::new();
    }

    let lowest = highest_available.saturating_sub(desired_entries.saturating_sub(1));
    (lowest..=highest_available).collect()
}

async fn find_canonical_fork_height(
    reorg_window: &ReorgWindow,
    rpc_client: &AsyncRpcClient,
) -> Result<Option<u64>> {
    if reorg_window.is_empty() {
        return Ok(None);
    }

    let mut entries: Vec<(u64, BlockHash)> = reorg_window.iter().cloned().collect();
    let mut heights: Vec<u64> = entries.iter().map(|(height, _)| *height).collect();

    while !heights.is_empty() {
        match rpc_client.batch_get_block_hashes(&heights).await {
            Ok(canonical_hashes) => return Ok(locate_common_ancestor(&entries, &canonical_hashes)),
            Err(err) => {
                if let Some(RpcError::HeightOutOfRange { height }) = err.downcast_ref::<RpcError>()
                {
                    let missing_height = *height;
                    while matches!(heights.last(), Some(current) if *current >= missing_height) {
                        heights.pop();
                        entries.pop();
                    }

                    if heights.is_empty() {
                        return Ok(None);
                    }

                    tracing::info!(
                        missing_height,
                        available_tip = missing_height.saturating_sub(1),
                        next_request = heights.last().copied(),
                        "canonical hash lookup exceeded chain tip; trimming request window"
                    );
                    continue;
                }

                return Err(err).context("failed to fetch canonical hashes for reorg recovery");
            }
        }
    }

    Ok(None)
}

async fn find_canonical_fork_height_with_retry(
    reorg_window: &ReorgWindow,
    rpc_client: &AsyncRpcClient,
    shutdown: &CancellationToken,
) -> Result<Option<u64>> {
    retry_with_backoff(
        RetryBackoff::new(
            Duration::from_millis(REORG_RECOVERY_INITIAL_BACKOFF_MS),
            Duration::from_millis(REORG_RECOVERY_MAX_BACKOFF_MS),
        )
        .with_cancellation(shutdown),
        |_attempt| {
            let shutdown = shutdown.clone();
            async move {
                tokio::select! {
                    _ = shutdown.cancelled() => Err(anyhow!("canonical fork search cancelled")),
                    result = find_canonical_fork_height(reorg_window, rpc_client) => result,
                }
            }
        },
        |attempt, backoff, err, _| {
            tracing::warn!(
                attempt,
                backoff_ms = backoff.as_millis() as u64,
                error = %err,
                "failed to locate canonical ancestor; retrying"
            );
        },
        |_, _| RetryDisposition::Retry,
    )
    .await
}

pub(crate) fn locate_common_ancestor(
    entries: &[(u64, BlockHash)],
    canonical_hashes: &[BlockHash],
) -> Option<u64> {
    if entries.len() != canonical_hashes.len() {
        return None;
    }

    for idx in (0..entries.len()).rev() {
        if entries[idx].1 == canonical_hashes[idx] {
            return Some(entries[idx].0);
        }
    }

    None
}

#[derive(Debug)]
pub(crate) enum ReorgCause {
    ConflictingBlock {
        height: u64,
        prev_hash: BlockHash,
    },
    TipRegression {
        available_tip: u64,
        last_confirmed: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hashes::Hash;

    fn dummy_hash(seed: u8) -> BlockHash {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        BlockHash::from_slice(&bytes).expect("valid hash")
    }

    #[test]
    fn reorg_window_respects_limit() {
        let mut window = ReorgWindow::new(2);
        window.push(10, dummy_hash(1));
        window.push(11, dummy_hash(2));
        window.push(12, dummy_hash(3));

        assert_eq!(window.len(), 2);
        assert_eq!(
            window.iter().cloned().collect::<Vec<_>>(),
            vec![(11, dummy_hash(2)), (12, dummy_hash(3))]
        );
    }

    #[test]
    fn reorg_window_finds_hash_and_returns_height() {
        let mut window = ReorgWindow::new(4);
        window.push(5, dummy_hash(5));
        window.push(6, dummy_hash(6));

        assert_eq!(window.find_hash(&dummy_hash(5)), Some(5));
        assert_eq!(window.find_hash(&dummy_hash(42)), None);
    }

    #[test]
    fn reorg_window_clear_empties_window() {
        let mut window = ReorgWindow::new(3);
        window.push(1, dummy_hash(1));
        window.push(2, dummy_hash(2));
        assert!(!window.is_empty());

        window.clear();
        assert!(window.is_empty());
    }

    #[test]
    fn reorg_window_truncate_after_preserves_prefix() {
        let mut window = ReorgWindow::new(5);
        window.push(10, dummy_hash(10));
        window.push(11, dummy_hash(11));
        window.push(12, dummy_hash(12));

        window.truncate_after(10);
        assert_eq!(
            window.iter().cloned().collect::<Vec<_>>(),
            vec![(10, dummy_hash(10))]
        );

        window.push(13, dummy_hash(13));
        window.truncate_after(13);
        assert_eq!(
            window.iter().cloned().collect::<Vec<_>>(),
            vec![(10, dummy_hash(10)), (13, dummy_hash(13))]
        );
    }
}
