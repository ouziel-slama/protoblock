//! Configurable knobs for the RPC client along with validation helpers so
//! callers can reason about timeouts, concurrency, and retry/backoff limits.

use anyhow::{bail, Result};
use std::time::Duration;

pub const DEFAULT_HTTP_BODY_LIMIT_BYTES: usize = 10 * 1024 * 1024;
const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 256;
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 10;
const DEFAULT_MAX_ATTEMPTS: usize = 5;
const DEFAULT_INITIAL_BACKOFF_MS: u64 = 200;
const DEFAULT_MAX_BACKOFF_MS: u64 = 2_000;

#[derive(Debug, Clone)]
pub struct RpcClientOptions {
    pub request_timeout: Duration,
    pub max_concurrent_requests: usize,
    pub max_attempts: usize,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub max_request_body_bytes: usize,
    pub max_response_body_bytes: usize,
}

impl Default for RpcClientOptions {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(DEFAULT_REQUEST_TIMEOUT_SECS),
            max_concurrent_requests: DEFAULT_MAX_CONCURRENT_REQUESTS,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            initial_backoff: Duration::from_millis(DEFAULT_INITIAL_BACKOFF_MS),
            max_backoff: Duration::from_millis(DEFAULT_MAX_BACKOFF_MS),
            max_request_body_bytes: DEFAULT_HTTP_BODY_LIMIT_BYTES,
            max_response_body_bytes: DEFAULT_HTTP_BODY_LIMIT_BYTES,
        }
    }
}

impl RpcClientOptions {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.request_timeout.is_zero() {
            bail!("request_timeout must be greater than 0");
        }
        if self.max_concurrent_requests == 0 {
            bail!("max_concurrent_requests must be greater than 0");
        }
        if self.max_attempts == 0 {
            bail!("max_attempts must be greater than 0");
        }
        if self.initial_backoff.is_zero() {
            bail!("initial_backoff must be greater than 0");
        }
        if self.max_request_body_bytes == 0 {
            bail!("max_request_body_bytes must be greater than 0");
        }
        if self.max_response_body_bytes == 0 {
            bail!("max_response_body_bytes must be greater than 0");
        }
        Ok(())
    }
}
