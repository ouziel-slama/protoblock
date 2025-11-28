use crate::rpc::payload::estimate_request_body_bytes;
use crate::rpc::RpcError;
use anyhow::{anyhow, Result};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use super::process::Worker;
use super::types::{
    retry_delay, worker_stride, BatchContext, BatchPlan, FetchWaitOutcome, FetcherEvent,
    IdleDecision, PendingBatch, PlanResult, ReadyBatch, WorkerControl, DEFAULT_HEIGHT_STEP,
};

impl<P: crate::runtime::protocol::BlockProtocol> Worker<P> {
    #[must_use]
    pub fn calculate_heights(
        start: u64,
        count: usize,
        thread_count: usize,
        step: usize,
    ) -> Vec<u64> {
        if count == 0 {
            return Vec::new();
        }

        let stride = worker_stride(thread_count, step);
        let mut heights = Vec::with_capacity(count);
        let mut current = start;

        for _ in 0..count {
            heights.push(current);
            current = current.saturating_add(stride);
        }

        heights
    }

    pub(crate) fn filter_heights(&self, heights: &[u64]) -> Vec<u64> {
        match self.blockchain_tip.current() {
            Some(tip) => heights
                .iter()
                .copied()
                .take_while(|height| *height <= tip)
                .collect(),
            None => heights.to_vec(),
        }
    }

    pub(super) fn build_batch_plan(&self, start_height: u64) -> PlanResult {
        let batch_size = self.batch_sizer.get_size();
        let (payload_safe_size, clamped) = self.apply_payload_limits(batch_size);
        let planned_size = payload_safe_size.max(1);
        if clamped {
            tracing::debug!(
                worker = self.id,
                requested_blocks = batch_size,
                limited_blocks = planned_size,
                "RPC payload limits reduced planned batch size"
            );
            self.telemetry.record_payload_split();
        }
        let active_threads = self.active_workers.load(Ordering::SeqCst).max(1);
        let stride = worker_stride(active_threads, DEFAULT_HEIGHT_STEP);
        let heights = Self::calculate_heights(
            start_height,
            planned_size,
            active_threads,
            DEFAULT_HEIGHT_STEP,
        );

        if heights.is_empty() {
            return PlanResult::Exhausted;
        }

        let heights_to_fetch = self.filter_heights(&heights);
        if heights_to_fetch.is_empty() {
            return PlanResult::AwaitTip;
        }

        let context = BatchContext {
            start_height,
            stride,
            requested: heights_to_fetch.len(),
            epoch: self.generation.load(Ordering::SeqCst),
        };

        PlanResult::Ready(BatchPlan {
            context,
            heights: heights_to_fetch,
        })
    }

    pub(super) fn spawn_pending_batch(&self, plan: BatchPlan) -> PendingBatch {
        PendingBatch::new(plan, Arc::clone(&self.rpc_client))
    }

    async fn idle_wait(
        &mut self,
        start_height: u64,
        shutdown: &CancellationToken,
    ) -> Result<IdleDecision> {
        let decision = tokio::select! {
            _ = sleep(self.tip_idle_backoff) => IdleDecision::Retry,
            control = self.control_rx.recv() => match control {
                Some(WorkerControl::Start(height)) => IdleDecision::Start(height),
                Some(WorkerControl::Stop) => IdleDecision::Stop,
                Some(WorkerControl::Shutdown) | None => IdleDecision::Shutdown,
            },
            _ = shutdown.cancelled() => IdleDecision::Shutdown,
        };

        if matches!(decision, IdleDecision::Retry) {
            tracing::trace!(
                worker = self.id,
                start_height,
                "retrying idle fetch after backoff"
            );
        }

        Ok(decision)
    }

    async fn report_missing_height(&self, missing_height: u64) {
        let available_tip = missing_height.saturating_sub(1);
        self.blockchain_tip.update(available_tip);
        if let Err(err) = self
            .event_tx
            .send(FetcherEvent::HeightOutOfRange {
                worker_id: self.id,
                missing_height,
                available_tip,
            })
            .await
        {
            tracing::debug!(
                worker = self.id,
                error = %err,
                missing_height,
                available_tip,
                "failed to publish height-out-of-range event"
            );
        }
    }

    async fn wait_for_start_signal(&mut self, shutdown: &CancellationToken) -> Result<Option<u64>> {
        loop {
            tokio::select! {
                control = self.control_rx.recv() => {
                    match control {
                        Some(WorkerControl::Start(height)) => return Ok(Some(height)),
                        Some(WorkerControl::Stop) => {
                            tracing::debug!(worker = self.id, "ignoring stop while idle");
                        }
                        Some(WorkerControl::Shutdown) | None => return Ok(None),
                    }
                }
                _ = shutdown.cancelled() => {
                    return Ok(None);
                }
            }
        }
    }

    pub(super) async fn ensure_pending_fetch(
        &mut self,
        current_height: &mut Option<u64>,
        pending_fetch: &mut Option<PendingBatch>,
        shutdown: &CancellationToken,
    ) -> Result<bool> {
        loop {
            if pending_fetch.is_some() {
                return Ok(true);
            }

            let start_height = match current_height {
                Some(height) => *height,
                None => match self.wait_for_start_signal(shutdown).await? {
                    Some(height) => {
                        *current_height = Some(height);
                        height
                    }
                    None => {
                        tracing::info!(worker = self.id, "control channel closed; stopping");
                        return Ok(false);
                    }
                },
            };

            match self.build_batch_plan(start_height) {
                PlanResult::Ready(plan) => {
                    pending_fetch.replace(self.spawn_pending_batch(plan));
                    return Ok(true);
                }
                PlanResult::AwaitTip => {
                    tracing::debug!(
                        worker = self.id,
                        start_height,
                        "no blocks available up to current tip; waiting before retry"
                    );
                    match self.idle_wait(start_height, shutdown).await? {
                        IdleDecision::Retry => {
                            *current_height = Some(start_height);
                        }
                        IdleDecision::Start(height) => {
                            *current_height = Some(height);
                        }
                        IdleDecision::Stop => {
                            *current_height = None;
                        }
                        IdleDecision::Shutdown => {
                            return Ok(false);
                        }
                    }
                }
                PlanResult::Exhausted => {
                    tracing::warn!(
                        worker = self.id,
                        "calculated empty batch; shutting worker down"
                    );
                    return Ok(false);
                }
            }
        }
    }

    pub(super) async fn wait_for_pending_fetch(
        &mut self,
        pending_fetch: &mut Option<PendingBatch>,
        shutdown: &CancellationToken,
        consecutive_failures: &mut usize,
    ) -> Result<FetchWaitOutcome> {
        let Some(pending) = pending_fetch.as_mut() else {
            return Ok(FetchWaitOutcome::Shutdown);
        };

        tokio::select! {
            result = &mut pending.handle => {
                let pending_batch = pending_fetch.take().expect("pending batch must exist");
                match result {
                    Ok(Ok(blocks)) => {
                        if blocks.is_empty() {
                            tracing::info!(
                                worker = self.id,
                                start_height = pending_batch.context.start_height,
                                "prefetched batch returned empty; backing off briefly"
                            );
                            sleep(self.tip_idle_backoff).await;
                            Ok(FetchWaitOutcome::Restart(pending_batch.context.start_height))
                        } else {
                            self.record_payload_sample(&blocks);
                            let first_height = pending_batch.heights.first().copied();
                            let last_height = pending_batch.heights.last().copied();
                            tracing::debug!(
                                worker = self.id,
                                first_height,
                                last_height,
                                requested = pending_batch.heights.len(),
                                "prefetched batch completed"
                            );

                            *consecutive_failures = 0;
                            Ok(FetchWaitOutcome::Ready(ReadyBatch {
                                context: pending_batch.context,
                                blocks,
                            }))
                        }
                    }
                    Ok(Err(error)) => {
                        let start_height = pending_batch.context.start_height;
                        if let Some(RpcError::HeightOutOfRange { height }) =
                            error.downcast_ref::<RpcError>()
                        {
                            let missing_height = *height;
                            self.report_missing_height(missing_height).await;
                            self.batch_sizer.shrink_on_failure();
                            *consecutive_failures = 0;
                            tracing::info!(
                                worker = self.id,
                                start_height,
                                missing_height,
                                "caught up to chain tip; backing off briefly"
                            );
                            sleep(self.tip_idle_backoff).await;
                            Ok(FetchWaitOutcome::Restart(start_height))
                        } else if matches!(
                            error.downcast_ref::<RpcError>(),
                            Some(RpcError::ResponseTooLarge { .. })
                        ) {
                            let requested = pending_batch.heights.len();
                            let request_estimate_bytes =
                                estimate_request_body_bytes(requested);
                            let request_budget_bytes = self.rpc_payload_limits.request_budget();
                            let response_budget_bytes = self.rpc_payload_limits.response_budget();
                            let request_limit_bytes =
                                self.rpc_payload_limits.max_request_body_bytes();
                            let response_limit_bytes =
                                self.rpc_payload_limits.max_response_body_bytes();
                            let single_request_estimate_bytes = estimate_request_body_bytes(1);
                            let request_budget_unreachable =
                                single_request_estimate_bytes > request_budget_bytes;
                            let unshrinkable = requested <= 1 || request_budget_unreachable;

                            if unshrinkable {
                                let fatal_context = format!(
                                    "worker {} cannot fetch height {} within RPC payload limits",
                                    self.id, start_height
                                );
                                let err = error.context(format!(
                                    "RPC batch exceeded HTTP size limits with {requested} \
                                     block(s) starting at height {start_height}; \
                                     request_estimate={request_estimate_bytes}B, \
                                     single_request_estimate={single_request_estimate_bytes}B, \
                                     safe_request_budget={request_budget_bytes}B \
                                     (PROTOBLOCK_MAX_REQUEST_MB -> {request_limit_bytes}B), \
                                     safe_response_budget={response_budget_bytes}B \
                                     (PROTOBLOCK_MAX_RESPONSE_MB -> {response_limit_bytes}B). \
                                     Increase the PROTOBLOCK_MAX_* environment variables to \
                                     continue."
                                ));
                                let fatal = self
                                    .fatal_handler
                                    .trigger_external(fatal_context.as_str(), err);
                                return Err(fatal);
                            }

                            self.batch_sizer.shrink_for_oversize();
                            self.record_oversized_payload_hint(requested);
                            *consecutive_failures = 0;
                            tracing::info!(
                                worker = self.id,
                                start_height,
                                requested,
                                request_estimate_bytes,
                                single_request_estimate_bytes,
                                request_budget_bytes,
                                response_budget_bytes,
                                request_limit_bytes,
                                response_limit_bytes,
                                "RPC batch exceeded HTTP size limits; shrinking before retry"
                            );
                            let delay = retry_delay(0);
                            sleep(delay).await;
                            Ok(FetchWaitOutcome::Restart(start_height))
                        } else {
                            *consecutive_failures = consecutive_failures.saturating_add(1);
                            self.batch_sizer.shrink_on_failure();
                            if let Some(rpc_error) = error.downcast_ref::<RpcError>() {
                                match rpc_error {
                                    RpcError::Timeout { .. } => self.telemetry.record_rpc_timeout(),
                                    RpcError::CircuitOpen => self.telemetry.record_rpc_error(),
                                    RpcError::HeightOutOfRange { .. } => {}
                                    RpcError::ResponseTooLarge { .. } => {}
                                }
                            } else {
                                self.telemetry.record_rpc_error();
                            }
                            let delay = retry_delay(*consecutive_failures);
                            tracing::warn!(
                                worker = self.id,
                                attempts = *consecutive_failures,
                                backoff_ms = delay.as_millis() as u64,
                                error = %error,
                                "RPC batch failed; backing off before retry"
                            );
                            sleep(delay).await;
                            Ok(FetchWaitOutcome::Restart(start_height))
                        }
                    }
                    Err(join_err) => Err(anyhow!(
                        "batch fetch task ended unexpectedly: {join_err}"
                    )),
                }
            }
            control = self.control_rx.recv() => {
                let pending_batch = pending_fetch.take().expect("pending batch must exist");
                pending_batch.abort();
                match control {
                    Some(WorkerControl::Start(height)) => Ok(FetchWaitOutcome::Start(height)),
                    Some(WorkerControl::Stop) => Ok(FetchWaitOutcome::Stop),
                    Some(WorkerControl::Shutdown) | None => Ok(FetchWaitOutcome::Shutdown),
                }
            }
            _ = shutdown.cancelled() => {
                let pending_batch = pending_fetch.take().expect("pending batch must exist");
                pending_batch.abort();
                Ok(FetchWaitOutcome::Shutdown)
            }
        }
    }
}
