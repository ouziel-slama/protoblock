//! JSON-RPC client plumbing: authentication, circuit breaker, batching,
//! metrics, retry policy, and helper utilities.

pub mod auth;
pub mod circuit_breaker;
pub mod client;
pub mod helpers;
pub mod metrics;
pub mod options;
pub mod payload;
pub mod retry;

pub use circuit_breaker::{
    CircuitBreakerError, CircuitBreakerSnapshot, CircuitState, RpcCircuitBreaker,
};
pub use client::{AsyncRpcClient, BlockBatchClient, RpcError};
pub use helpers::{extract_hashes, hex_to_block};
pub use metrics::RpcMetricsSnapshot;
pub use options::RpcClientOptions;
