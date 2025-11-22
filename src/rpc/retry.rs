//! Shared retry context, logging helpers, and canned message templates used by
//! the RPC client to keep instrumentation consistent across operations.

use crate::rpc::circuit_breaker::CircuitState;
use anyhow::Error;
use std::time::Duration;

macro_rules! log_with_retry_ctx {
    ($level:ident, $ctx:expr, $($rest:tt)*) => {{
        if let Some(heights) = $ctx.heights {
            tracing::$level!(
                start_height = heights.start,
                end_height = heights.end,
                $($rest)*
            );
        } else {
            tracing::$level!($($rest)*);
        }
    }};
}

pub(crate) use log_with_retry_ctx;

#[derive(Clone, Copy)]
struct RetryHeights {
    start: u64,
    end: u64,
}

impl RetryHeights {
    fn from_slice(heights: &[u64]) -> Option<Self> {
        heights.first().map(|start| Self {
            start: *start,
            end: heights.last().copied().unwrap_or(*start),
        })
    }
}

/// Logging labels that describe how a particular RPC operation should report
/// circuit-breaker state, retry attempts, and exhaustion.
#[derive(Clone, Copy)]
pub(crate) struct RetryMessages {
    pub(crate) permit: &'static str,
    pub(crate) circuit_open: &'static str,
    pub(crate) timeout: &'static str,
    pub(crate) retry: &'static str,
    pub(crate) exhausted: &'static str,
    pub(crate) exhausted_timeout: Option<&'static str>,
    pub(crate) tip: Option<&'static str>,
    pub(crate) oversized: Option<&'static str>,
    pub(crate) retry_includes_timeout_flag: bool,
    pub(crate) retry_after_timeout: bool,
    pub(crate) timeout_includes_backoff: bool,
    pub(crate) timeout_on_exhaustion: bool,
}

/// Context passed into `retry_with_breaker` so callers can attach optional
/// height metadata and reuse consistent log messaging.
#[derive(Clone, Copy)]
pub(crate) struct RetryContext<'a> {
    heights: Option<RetryHeights>,
    messages: &'a RetryMessages,
}

impl<'a> RetryContext<'a> {
    pub(crate) fn new(messages: &'a RetryMessages) -> Self {
        Self {
            heights: None,
            messages,
        }
    }

    pub(crate) fn with_heights(messages: &'a RetryMessages, heights: &[u64]) -> Self {
        Self {
            heights: RetryHeights::from_slice(heights),
            messages,
        }
    }

    pub(crate) fn log_permit(&self, state: CircuitState) {
        log_with_retry_ctx!(
            trace,
            self,
            breaker_state = ?state,
            "{}",
            self.messages.permit
        );
    }

    pub(crate) fn log_circuit_open(&self) {
        log_with_retry_ctx!(warn, self, "{}", self.messages.circuit_open);
    }

    pub(crate) fn log_tip(&self, attempt: usize, height: u64) {
        if let Some(message) = self.messages.tip {
            log_with_retry_ctx!(info, self, attempt, missing_height = height, "{}", message);
        }
    }

    pub(crate) fn log_timeout(&self, attempt: usize, method: Option<&str>, backoff: Duration) {
        let backoff_ms = Self::duration_to_millis(backoff);
        if self.messages.timeout_includes_backoff {
            if let Some(method) = method {
                log_with_retry_ctx!(
                    warn,
                    self,
                    attempt,
                    method = method,
                    backoff_ms = backoff_ms,
                    "{}",
                    self.messages.timeout
                );
            } else {
                log_with_retry_ctx!(
                    warn,
                    self,
                    attempt,
                    backoff_ms = backoff_ms,
                    "{}",
                    self.messages.timeout
                );
            }
        } else if let Some(method) = method {
            log_with_retry_ctx!(
                warn,
                self,
                attempt,
                method = method,
                "{}",
                self.messages.timeout
            );
        } else {
            log_with_retry_ctx!(warn, self, attempt, "{}", self.messages.timeout);
        }
    }

    pub(crate) fn log_retry(&self, attempt: usize, backoff: Duration, err: &Error, timeout: bool) {
        let backoff_ms = Self::duration_to_millis(backoff);
        if self.messages.retry_includes_timeout_flag {
            log_with_retry_ctx!(
                warn,
                self,
                attempt,
                backoff_ms = backoff_ms,
                error = %err,
                timeout = timeout,
                "{}",
                self.messages.retry
            );
        } else {
            log_with_retry_ctx!(
                warn,
                self,
                attempt,
                backoff_ms = backoff_ms,
                error = %err,
                "{}",
                self.messages.retry
            );
        }
    }

    pub(crate) fn log_exhausted(&self, attempt: usize, err: &Error, timeout: bool) {
        let message = if timeout {
            self.messages
                .exhausted_timeout
                .unwrap_or(self.messages.exhausted)
        } else {
            self.messages.exhausted
        };
        log_with_retry_ctx!(error, self, attempt, error = %err, "{}", message);
    }

    fn duration_to_millis(backoff: Duration) -> u64 {
        backoff.as_millis().min(u128::from(u64::MAX)) as u64
    }

    pub(crate) fn timeout_on_exhaustion(&self) -> bool {
        self.messages.timeout_on_exhaustion
    }

    pub(crate) fn retry_after_timeout(&self) -> bool {
        self.messages.retry_after_timeout
    }

    pub(crate) fn log_oversized(&self, attempt: usize, method: &str) {
        if let Some(message) = self.messages.oversized {
            log_with_retry_ctx!(warn, self, attempt, method = method, "{}", message);
        }
    }
}

pub(crate) const BATCH_GET_BLOCKS_RETRY: RetryMessages = RetryMessages {
    permit: "circuit breaker permit acquired",
    circuit_open: "RPC circuit breaker open; rejecting batch",
    timeout: "RPC batch timed out; will retry",
    retry: "batch_get_blocks failed; retrying",
    exhausted: "batch_get_blocks exhausted retries",
    exhausted_timeout: None,
    tip: Some("batch_get_blocks reached chain tip"),
    oversized: Some("RPC batch exceeded HTTP size limit; shrinking request"),
    retry_includes_timeout_flag: true,
    retry_after_timeout: true,
    timeout_includes_backoff: false,
    timeout_on_exhaustion: true,
};

pub(crate) const FETCH_HASHES_RETRY: RetryMessages = RetryMessages {
    permit: "circuit breaker permit acquired for getblockhash batch",
    circuit_open: "RPC circuit breaker open; rejecting getblockhash batch",
    timeout: "getblockhash batch timed out; will retry",
    retry: "getblockhash batch failed; retrying",
    exhausted: "getblockhash batch exhausted retries",
    exhausted_timeout: None,
    tip: Some("getblockhash batch reached chain tip"),
    oversized: None,
    retry_includes_timeout_flag: true,
    retry_after_timeout: true,
    timeout_includes_backoff: false,
    timeout_on_exhaustion: true,
};

pub(crate) const GET_TIP_RETRY: RetryMessages = RetryMessages {
    permit: "circuit breaker permit acquired",
    circuit_open: "RPC circuit breaker open; rejecting getblockcount request",
    timeout: "getblockcount timed out; retrying",
    retry: "getblockcount failed; retrying",
    exhausted: "getblockcount exhausted retries",
    exhausted_timeout: Some("getblockcount exhausted retries after timeout"),
    tip: None,
    oversized: None,
    retry_includes_timeout_flag: false,
    retry_after_timeout: false,
    timeout_includes_backoff: true,
    timeout_on_exhaustion: false,
};
