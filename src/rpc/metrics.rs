//! Lightweight counters tracking RPC successes, failures, and latency so the
//! client can expose aggregated snapshots without leaking implementation
//! details to downstream consumers.

use crate::rpc::circuit_breaker::CircuitState;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Debug, Default)]
pub(crate) struct RpcMetrics {
    total_requests: AtomicU64,
    total_errors: AtomicU64,
    total_latency_ns: AtomicU64,
    total_timeouts: AtomicU64,
}

impl RpcMetrics {
    pub(crate) fn record_success(&self, latency: Duration) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ns
            .fetch_add(latency.as_nanos() as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_failure(&self, latency: Duration) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.total_errors.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ns
            .fetch_add(latency.as_nanos() as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_timeout(&self, latency: Duration) {
        self.record_failure(latency);
        self.total_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> RpcMetricsSnapshot {
        let total_requests = self.total_requests.load(Ordering::Relaxed);
        let total_errors = self.total_errors.load(Ordering::Relaxed);
        let total_latency_ns = self.total_latency_ns.load(Ordering::Relaxed);

        let average_latency_ms = if total_requests == 0 {
            0.0
        } else {
            (total_latency_ns as f64 / total_requests as f64) / 1_000_000.0
        };

        let error_rate = if total_requests == 0 {
            0.0
        } else {
            total_errors as f64 / total_requests as f64
        };

        RpcMetricsSnapshot {
            total_requests,
            total_errors,
            average_latency_ms,
            error_rate,
            total_timeouts: self.total_timeouts.load(Ordering::Relaxed),
            breaker_state: CircuitState::Closed,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct RpcMetricsSnapshot {
    pub total_requests: u64,
    pub total_errors: u64,
    pub average_latency_ms: f64,
    pub error_rate: f64,
    pub total_timeouts: u64,
    pub breaker_state: CircuitState,
}
