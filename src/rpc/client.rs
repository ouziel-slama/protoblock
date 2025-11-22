//! RPC client implementation and reusable abstractions for fetching Bitcoin
//! blocks from Bitcoin Core nodes via JSON-RPC. Houses the `AsyncRpcClient`,
//! error types, and the `BlockBatchClient` trait consumed by workers.

use crate::rpc::auth::build_auth_headers;
use crate::rpc::circuit_breaker::{CircuitBreakerError, RpcCircuitBreaker};
use crate::rpc::metrics::{RpcMetrics, RpcMetricsSnapshot};
use crate::rpc::options::RpcClientOptions;
use crate::rpc::retry::{RetryContext, BATCH_GET_BLOCKS_RETRY, FETCH_HASHES_RETRY, GET_TIP_RETRY};
use crate::runtime::config::FetcherConfig;
use anyhow::{anyhow, bail, Context, Result};
use bitcoin::BlockHash;
use futures::future::BoxFuture;
use jsonrpsee::core::{
    client::{ClientT, Error as JsonRpcError},
    http_helpers::HttpError,
    params::BatchRequestBuilder,
};
use jsonrpsee::http_client::transport::Error as HttpTransportError;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use jsonrpsee::rpc_params;
use jsonrpsee::types::ErrorObject;
use serde::de::DeserializeOwned;
use std::{fmt, future::Future, str::FromStr, sync::Arc, time::Duration};
use tokio::time::{sleep, timeout, Instant};

#[derive(Debug)]
pub enum RpcError {
    Timeout { method: &'static str },
    CircuitOpen,
    HeightOutOfRange { height: u64 },
    ResponseTooLarge { method: &'static str },
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcError::Timeout { method } => write!(f, "rpc method {method} timed out"),
            RpcError::CircuitOpen => write!(f, "rpc circuit breaker is open"),
            RpcError::HeightOutOfRange { height } => {
                write!(f, "requested height {height} is above the current tip")
            }
            RpcError::ResponseTooLarge { method } => {
                write!(f, "rpc {method} response exceeded HTTP size limits")
            }
        }
    }
}

impl std::error::Error for RpcError {}

pub trait BlockBatchClient: Send + Sync {
    fn batch_get_blocks<'a>(
        &'a self,
        heights: &'a [u64],
    ) -> BoxFuture<'a, Result<Vec<(u64, String)>>>;
}

#[derive(Debug, Clone)]
pub struct AsyncRpcClient {
    rpc_url: Arc<String>,
    rpc_user: Arc<String>,
    rpc_password: Arc<String>,
    client: HttpClient,
    options: RpcClientOptions,
    metrics: Arc<RpcMetrics>,
    breaker: Arc<RpcCircuitBreaker>,
}

impl BlockBatchClient for AsyncRpcClient {
    fn batch_get_blocks<'a>(
        &'a self,
        heights: &'a [u64],
    ) -> BoxFuture<'a, Result<Vec<(u64, String)>>> {
        Box::pin(self.batch_get_blocks(heights))
    }
}

impl AsyncRpcClient {
    pub fn new(
        url: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self> {
        Self::with_options(url, user, password, RpcClientOptions::default())
    }

    pub fn with_options(
        url: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
        options: RpcClientOptions,
    ) -> Result<Self> {
        Self::with_options_and_breaker(
            url,
            user,
            password,
            options,
            Arc::new(RpcCircuitBreaker::default()),
        )
    }

    pub fn with_options_and_breaker(
        url: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
        options: RpcClientOptions,
        breaker: Arc<RpcCircuitBreaker>,
    ) -> Result<Self> {
        options.validate()?;

        let rpc_url = url.into();
        let rpc_user = user.into();
        let rpc_password = password.into();

        let headers = build_auth_headers(&rpc_user, &rpc_password)?;
        let max_request_body_size = options.max_request_body_bytes.min(u32::MAX as usize) as u32;
        let max_response_body_size = options.max_response_body_bytes.min(u32::MAX as usize) as u32;

        let client = HttpClientBuilder::default()
            .set_headers(headers)
            .request_timeout(options.request_timeout)
            .max_concurrent_requests(options.max_concurrent_requests)
            .max_request_size(max_request_body_size)
            .max_response_size(max_response_body_size)
            .build(&rpc_url)
            .map_err(|err| anyhow!("failed to build RPC client: {err}"))?;

        Ok(Self {
            rpc_url: Arc::new(rpc_url),
            rpc_user: Arc::new(rpc_user),
            rpc_password: Arc::new(rpc_password),
            client,
            options,
            metrics: Arc::new(RpcMetrics::default()),
            breaker,
        })
    }

    pub fn from_config(config: &FetcherConfig) -> Result<Self> {
        Self::from_config_with_breaker(config, Arc::new(RpcCircuitBreaker::default()))
    }

    pub fn from_config_with_breaker(
        config: &FetcherConfig,
        breaker: Arc<RpcCircuitBreaker>,
    ) -> Result<Self> {
        config.validate()?;
        let options = RpcClientOptions {
            max_concurrent_requests: std::cmp::max(32, config.thread_count().saturating_mul(4)),
            request_timeout: config.rpc_timeout(),
            max_request_body_bytes: config.rpc_max_request_body_bytes(),
            max_response_body_bytes: config.rpc_max_response_body_bytes(),
            ..RpcClientOptions::default()
        };
        Self::with_options_and_breaker(
            config.rpc_url().to_owned(),
            config.rpc_user().to_owned(),
            config.rpc_password().to_owned(),
            options,
            breaker,
        )
    }

    pub fn endpoint(&self) -> &str {
        &self.rpc_url
    }

    pub fn credentials(&self) -> (&str, &str) {
        (self.rpc_user.as_str(), self.rpc_password.as_str())
    }

    pub fn metrics(&self) -> RpcMetricsSnapshot {
        let mut snapshot = self.metrics.snapshot();
        snapshot.breaker_state = self.breaker.snapshot().state;
        snapshot
    }

    pub async fn batch_get_blocks(&self, heights: &[u64]) -> Result<Vec<(u64, String)>> {
        if heights.is_empty() {
            return Ok(Vec::new());
        }

        let context = RetryContext::with_heights(&BATCH_GET_BLOCKS_RETRY, heights);
        self.retry_with_breaker(
            context,
            || async { self.perform_batch(heights).await },
            |attempt, blocks: &Vec<(u64, String)>| {
                tracing::debug!(
                    attempt,
                    blocks = blocks.len(),
                    "batch_get_blocks completed successfully"
                );
            },
        )
        .await
    }

    pub async fn batch_get_block_hashes(&self, heights: &[u64]) -> Result<Vec<BlockHash>> {
        if heights.is_empty() {
            return Ok(Vec::new());
        }

        let raw_hashes = self.fetch_hashes_with_breaker(heights).await?;

        if raw_hashes.len() != heights.len() {
            bail!(
                "RPC returned mismatched hash count (expected {}, got {})",
                heights.len(),
                raw_hashes.len()
            );
        }

        let mut hashes = Vec::with_capacity(raw_hashes.len());

        for (idx, hash_hex) in raw_hashes.into_iter().enumerate() {
            let height = heights.get(idx).copied().unwrap_or_default();
            let hash = BlockHash::from_str(&hash_hex)
                .with_context(|| format!("failed to parse block hash for height {height}"))?;
            hashes.push(hash);
        }

        Ok(hashes)
    }

    /// Shared retry/backoff loop that wraps RPC operations with breaker gating, metrics,
    /// exponential backoff, and consistent logging.
    async fn retry_with_breaker<T, F, Fut, S>(
        &self,
        context: RetryContext<'_>,
        mut operation: F,
        mut on_success: S,
    ) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
        S: FnMut(usize, &T),
    {
        let mut attempt = 0;

        loop {
            match self.breaker.before_request() {
                Ok(state) => context.log_permit(state),
                Err(CircuitBreakerError::CircuitOpen) => {
                    context.log_circuit_open();
                    return Err(RpcError::CircuitOpen.into());
                }
            }

            attempt += 1;
            let start = Instant::now();

            match operation().await {
                Ok(value) => {
                    self.metrics.record_success(start.elapsed());
                    self.breaker.record_success();
                    on_success(attempt, &value);
                    return Ok(value);
                }
                Err(err) => {
                    let elapsed = start.elapsed();
                    if let Some(rpc_error) = err.downcast_ref::<RpcError>() {
                        match rpc_error {
                            RpcError::HeightOutOfRange { height } => {
                                self.metrics.record_success(elapsed);
                                self.breaker.record_success();
                                context.log_tip(attempt, *height);
                                return Err(err);
                            }
                            RpcError::Timeout { method } => {
                                self.metrics.record_timeout(elapsed);
                                self.breaker.record_failure();
                                let will_retry = attempt < self.options.max_attempts;
                                let backoff = self.backoff_delay(attempt);
                                if will_retry || context.timeout_on_exhaustion() {
                                    context.log_timeout(attempt, Some(*method), backoff);
                                }
                                if !will_retry {
                                    context.log_exhausted(attempt, &err, true);
                                    return Err(err);
                                }
                                if context.retry_after_timeout() {
                                    context.log_retry(attempt, backoff, &err, true);
                                }
                                sleep(backoff).await;
                                continue;
                            }
                            RpcError::ResponseTooLarge { method } => {
                                self.metrics.record_failure(elapsed);
                                self.breaker.record_failure();
                                context.log_oversized(attempt, method);
                                return Err(err);
                            }
                            _ => {}
                        }
                    }

                    self.metrics.record_failure(elapsed);
                    self.breaker.record_failure();

                    if attempt >= self.options.max_attempts {
                        context.log_exhausted(attempt, &err, false);
                        return Err(err);
                    }

                    let backoff = self.backoff_delay(attempt);
                    context.log_retry(attempt, backoff, &err, false);
                    sleep(backoff).await;
                }
            }
        }
    }

    async fn perform_batch(&self, heights: &[u64]) -> Result<Vec<(u64, String)>> {
        let hashes = self.fetch_hashes_once(heights).await?;
        let blocks = self.batch_get_raw_blocks(&hashes).await?;

        if blocks.len() != heights.len() {
            bail!(
                "RPC returned mismatched block count (expected {}, got {})",
                heights.len(),
                blocks.len()
            );
        }

        Ok(heights.iter().copied().zip(blocks.into_iter()).collect())
    }

    async fn fetch_hashes_with_breaker(&self, heights: &[u64]) -> Result<Vec<String>> {
        let context = RetryContext::with_heights(&FETCH_HASHES_RETRY, heights);
        let start_height = heights.first().copied().unwrap_or_default();
        let end_height = heights.last().copied().unwrap_or(start_height);

        self.retry_with_breaker(
            context,
            || async { self.fetch_hashes_once(heights).await },
            move |attempt, hashes: &Vec<String>| {
                tracing::debug!(
                    attempt,
                    count = hashes.len(),
                    start_height,
                    end_height,
                    "getblockhash batch completed successfully"
                );
            },
        )
        .await
    }

    async fn fetch_hashes_once(&self, heights: &[u64]) -> Result<Vec<String>> {
        let mut batch = BatchRequestBuilder::new();

        for height in heights {
            batch
                .insert("getblockhash", rpc_params![height])
                .context("failed to serialize getblockhash params")?;
        }

        self.execute_batch(batch, "getblockhash", Some(heights))
            .await
    }

    async fn batch_get_raw_blocks(&self, hashes: &[String]) -> Result<Vec<String>> {
        let mut batch = BatchRequestBuilder::new();

        for hash in hashes {
            batch
                .insert("getblock", rpc_params![hash, 0u64])
                .context("failed to serialize getblock params")?;
        }

        self.execute_batch(batch, "getblock", None).await
    }

    async fn execute_batch<'a, R>(
        &self,
        batch: BatchRequestBuilder<'a>,
        label: &'static str,
        context: Option<&[u64]>,
    ) -> Result<Vec<R>>
    where
        R: DeserializeOwned + fmt::Debug + 'static,
    {
        let response = timeout(
            self.options.request_timeout,
            self.client.batch_request(batch),
        )
        .await
        .map_err(|_| RpcError::Timeout { method: label })?
        .map_err(|err| map_rpc_error(label, err))?;

        let mut values = Vec::with_capacity(response.len());
        for (idx, entry) in response.into_iter().enumerate() {
            match entry {
                Ok(value) => values.push(value),
                Err(err) => {
                    if let Some(ctx) = context {
                        if let Some(height) = ctx.get(idx) {
                            if err.code() == -8 {
                                return Err(RpcError::HeightOutOfRange { height: *height }.into());
                            }
                        }
                    }
                    return Err(map_rpc_batch_error(label, &err));
                }
            }
        }

        tracing::debug!(
            method = label,
            count = values.len(),
            "batch RPC call completed"
        );

        Ok(values)
    }

    fn backoff_delay(&self, attempt: usize) -> Duration {
        if attempt <= 1 {
            return self.options.initial_backoff;
        }

        let exponent = attempt.saturating_sub(1) as u32;
        let multiplier = 1u32.checked_shl(exponent).unwrap_or(u32::MAX);
        let mut delay = self.options.initial_backoff.saturating_mul(multiplier);

        if delay > self.options.max_backoff {
            delay = self.options.max_backoff;
        }

        delay
    }

    pub async fn get_blockchain_tip(&self) -> Result<u64> {
        const METHOD: &str = "getblockcount";

        self.retry_with_breaker(
            RetryContext::new(&GET_TIP_RETRY),
            || async {
                timeout(
                    self.options.request_timeout,
                    self.client.request(METHOD, rpc_params![]),
                )
                .await
                .map_err(|_| RpcError::Timeout { method: METHOD })?
                .map_err(|err| map_rpc_error(METHOD, err))
            },
            |attempt, height: &u64| {
                tracing::debug!(attempt, tip = *height, "refreshed blockchain tip");
            },
        )
        .await
    }
}

fn map_rpc_error(label: &'static str, err: JsonRpcError) -> anyhow::Error {
    if response_too_large(&err) {
        return RpcError::ResponseTooLarge { method: label }.into();
    }
    anyhow!("rpc {label} call failed: {err}")
}

fn map_rpc_batch_error(label: &str, err: &ErrorObject<'_>) -> anyhow::Error {
    if let Some(data) = err.data() {
        anyhow!(
            "rpc {label} call failed (code={}, message={}, data={})",
            err.code(),
            err.message(),
            data.get()
        )
    } else {
        anyhow!(
            "rpc {label} call failed (code={}, message={})",
            err.code(),
            err.message()
        )
    }
}

fn response_too_large(err: &JsonRpcError) -> bool {
    match err {
        JsonRpcError::Transport(inner) => {
            if let Some(transport_err) = inner.downcast_ref::<HttpTransportError>() {
                match transport_err {
                    HttpTransportError::Http(http_err) => matches!(http_err, HttpError::TooLarge),
                    HttpTransportError::RequestTooLarge => true,
                    _ => false,
                }
            } else {
                false
            }
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::circuit_breaker::RpcCircuitBreaker;
    use crate::rpc::retry::{RetryContext, BATCH_GET_BLOCKS_RETRY, GET_TIP_RETRY};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    fn test_client(breaker: Arc<RpcCircuitBreaker>) -> AsyncRpcClient {
        let options = RpcClientOptions {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            request_timeout: Duration::from_millis(5),
            ..RpcClientOptions::default()
        };

        AsyncRpcClient::with_options_and_breaker(
            "http://127.0.0.1:8332",
            "user",
            "pass",
            options,
            breaker,
        )
        .expect("test RPC client must build")
    }

    #[tokio::test]
    async fn retry_with_breaker_retries_timeouts() {
        let breaker = Arc::new(RpcCircuitBreaker::new(5, Duration::from_secs(5), 1));
        let client = test_client(breaker);
        let heights = vec![100u64, 101u64];
        let first_height = heights[0];
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_op = attempts.clone();

        let blocks = client
            .retry_with_breaker(
                RetryContext::with_heights(&BATCH_GET_BLOCKS_RETRY, &heights),
                move || {
                    let attempts_for_future = attempts_for_op.clone();
                    async move {
                        let current = attempts_for_future.fetch_add(1, Ordering::SeqCst);
                        if current == 0 {
                            Err(RpcError::Timeout { method: "getblock" }.into())
                        } else {
                            Ok(vec![(first_height, "deadbeef".to_string())])
                        }
                    }
                },
                |_, _| {},
            )
            .await
            .expect("second attempt should succeed");

        assert_eq!(blocks.len(), 1);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(client.metrics().total_timeouts, 1);
    }

    #[tokio::test]
    async fn retry_with_breaker_respects_open_breaker() {
        let breaker = Arc::new(RpcCircuitBreaker::new(1, Duration::from_secs(60), 1));
        let client = test_client(breaker.clone());

        breaker.before_request().unwrap();
        breaker.record_failure();

        let executions = Arc::new(AtomicUsize::new(0));
        let executions_for_op = executions.clone();

        let err = client
            .retry_with_breaker(
                RetryContext::new(&GET_TIP_RETRY),
                move || {
                    let executions_for_future = executions_for_op.clone();
                    async move {
                        executions_for_future.fetch_add(1, Ordering::SeqCst);
                        Ok(0u64)
                    }
                },
                |_, _| {},
            )
            .await
            .expect_err("breaker is open and should prevent calls");

        assert_eq!(executions.load(Ordering::SeqCst), 0);
        assert!(matches!(
            err.downcast_ref::<RpcError>(),
            Some(RpcError::CircuitOpen)
        ));
    }

    #[test]
    fn map_error_detects_http_too_large() {
        let transport_error = HttpTransportError::Http(HttpError::TooLarge);
        let err = JsonRpcError::Transport(anyhow::Error::new(transport_error));
        let mapped = map_rpc_error("getblock", err);
        match mapped.downcast_ref::<RpcError>() {
            Some(RpcError::ResponseTooLarge { method }) => assert_eq!(*method, "getblock"),
            _ => panic!("expected ResponseTooLarge error"),
        }
    }

    #[test]
    fn map_error_detects_request_too_large() {
        let transport_error = HttpTransportError::RequestTooLarge;
        let err = JsonRpcError::Transport(anyhow::Error::new(transport_error));
        let mapped = map_rpc_error("getblock", err);
        match mapped.downcast_ref::<RpcError>() {
            Some(RpcError::ResponseTooLarge { method }) => assert_eq!(*method, "getblock"),
            _ => panic!("expected ResponseTooLarge error"),
        }
    }
}
