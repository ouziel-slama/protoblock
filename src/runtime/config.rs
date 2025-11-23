use crate::rpc::options::DEFAULT_HTTP_BODY_LIMIT_BYTES;
use crate::rpc::payload::{estimate_request_body_bytes, single_block_response_body_bytes};
use crate::runtime::telemetry;
use anyhow::{bail, Context, Result};
use std::time::Duration;

const DEFAULT_RPC_TIMEOUT_SECS: u64 = 10;
const DEFAULT_QUEUE_MAX_SIZE_MB: usize = 200;
const DEFAULT_TIP_IDLE_BACKOFF_SECS: u64 = 10;
const DEFAULT_TIP_REFRESH_INTERVAL_SECS: u64 = 10;
const BITCOIN_MAX_BLOCK_BYTES: usize = 4 * 1024 * 1024;

/// Runtime configuration for the block fetcher pipeline.
///
/// All instances must be constructed via [`FetcherConfig::builder`] or [`FetcherConfig::new`]
/// so invariants are validated before any consumer observes the values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetcherConfig {
    rpc_url: String,
    rpc_user: String,
    rpc_password: String,
    thread_count: usize,
    max_batch_size_mb: usize,
    reorg_window_size: usize,
    start_height: u64,
    rpc_timeout: Duration,
    metrics_interval: Duration,
    queue_max_size_mb: usize,
    tip_idle_backoff: Duration,
    tip_refresh_interval: Duration,
    rpc_max_request_body_bytes: usize,
    rpc_max_response_body_bytes: usize,
}

pub struct FetcherConfigParams {
    pub rpc_url: String,
    pub rpc_user: String,
    pub rpc_password: String,
    pub thread_count: usize,
    pub max_batch_size_mb: usize,
    pub reorg_window_size: usize,
    pub start_height: u64,
    pub rpc_timeout: Duration,
    pub metrics_interval: Duration,
    pub queue_max_size_mb: usize,
    pub tip_idle_backoff: Duration,
    pub tip_refresh_interval: Duration,
    pub rpc_max_request_body_bytes: usize,
    pub rpc_max_response_body_bytes: usize,
}

impl FetcherConfig {
    /// Returns a builder to incrementally construct and validate a configuration.
    pub fn builder() -> FetcherConfigBuilder {
        FetcherConfigBuilder::default()
    }

    /// Constructs a configuration directly from the provided values.
    ///
    /// Prefer [`FetcherConfig::builder`] for ergonomics when many values use defaults.
    /// Callers that already have concrete runtime parameters can use this method to enforce
    /// validation without going through the builder.
    pub fn new(params: FetcherConfigParams) -> Result<Self> {
        let FetcherConfigParams {
            rpc_url,
            rpc_user,
            rpc_password,
            thread_count,
            max_batch_size_mb,
            reorg_window_size,
            start_height,
            rpc_timeout,
            metrics_interval,
            queue_max_size_mb,
            tip_idle_backoff,
            tip_refresh_interval,
            rpc_max_request_body_bytes,
            rpc_max_response_body_bytes,
        } = params;

        let config = Self {
            rpc_url: trimmed_string(rpc_url),
            rpc_user: trimmed_string(rpc_user),
            rpc_password: trimmed_string(rpc_password),
            thread_count,
            max_batch_size_mb,
            reorg_window_size,
            start_height,
            rpc_timeout,
            metrics_interval,
            queue_max_size_mb,
            tip_idle_backoff,
            tip_refresh_interval,
            rpc_max_request_body_bytes,
            rpc_max_response_body_bytes,
        };

        config.validate()?;
        Ok(config)
    }

    /// Full RPC URL (including scheme) configured for the fetcher.
    pub fn rpc_url(&self) -> &str {
        &self.rpc_url
    }

    /// RPC username.
    pub fn rpc_user(&self) -> &str {
        &self.rpc_user
    }

    /// RPC password.
    pub fn rpc_password(&self) -> &str {
        &self.rpc_password
    }

    /// Number of worker threads configured for the fetcher.
    pub fn thread_count(&self) -> usize {
        self.thread_count
    }

    /// Maximum megabytes per batch that workers target.
    pub fn max_batch_size_mb(&self) -> usize {
        self.max_batch_size_mb
    }

    /// Number of blocks tracked in the reorg window.
    pub fn reorg_window_size(&self) -> usize {
        self.reorg_window_size
    }

    /// Starting height requested when the pipeline boots.
    pub fn start_height(&self) -> u64 {
        self.start_height
    }

    /// Per-RPC timeout applied to the JSON-RPC client.
    pub fn rpc_timeout(&self) -> Duration {
        self.rpc_timeout
    }

    /// Interval used by the telemetry reporter.
    pub fn metrics_interval(&self) -> Duration {
        self.metrics_interval
    }

    /// Maximum megabytes of queued block data allowed in the ordered queue.
    pub fn queue_max_size_mb(&self) -> usize {
        self.queue_max_size_mb
    }

    /// Idle backoff used when workers poll near the blockchain tip.
    pub fn tip_idle_backoff(&self) -> Duration {
        self.tip_idle_backoff
    }

    /// Interval between background tip refresh RPC calls.
    pub fn tip_refresh_interval(&self) -> Duration {
        self.tip_refresh_interval
    }

    /// Maximum allowed HTTP request body bytes for RPC calls.
    pub fn rpc_max_request_body_bytes(&self) -> usize {
        self.rpc_max_request_body_bytes
    }

    /// Maximum allowed HTTP response body bytes for RPC calls.
    pub fn rpc_max_response_body_bytes(&self) -> usize {
        self.rpc_max_response_body_bytes
    }

    /// Performs validation on an existing configuration instance.
    pub fn validate(&self) -> Result<()> {
        validate_url(&self.rpc_url)?;
        ensure_not_empty(&self.rpc_user, "rpc_user")?;
        ensure_not_empty(&self.rpc_password, "rpc_password")?;

        if self.thread_count == 0 {
            bail!("thread_count must be greater than 0");
        }

        if self.max_batch_size_mb == 0 {
            bail!("max_batch_size_mb must be greater than 0");
        }

        if self.reorg_window_size == 0 {
            bail!("reorg_window_size must be greater than 0");
        }

        if self.rpc_timeout.is_zero() {
            bail!("rpc_timeout must be greater than 0");
        }

        if self.metrics_interval.is_zero() {
            bail!("metrics_interval must be greater than 0");
        }

        if self.queue_max_size_mb == 0 {
            bail!("queue_max_size_mb must be greater than 0");
        }

        if self.tip_idle_backoff.is_zero() {
            bail!("tip_idle_backoff must be greater than 0");
        }

        if self.tip_refresh_interval.is_zero() {
            bail!("tip_refresh_interval must be greater than 0");
        }

        if self.rpc_max_request_body_bytes == 0 {
            bail!("rpc_max_request_body_bytes must be greater than 0");
        }

        if self.rpc_max_response_body_bytes == 0 {
            bail!("rpc_max_response_body_bytes must be greater than 0");
        }

        let min_request_bytes = estimate_request_body_bytes(1);
        if self.rpc_max_request_body_bytes < min_request_bytes {
            bail!(
                "rpc_max_request_body_bytes ({}) must be at least {} bytes to fit one getblock \
                 request; increase PROTOBLOCK_MAX_REQUEST_MB",
                self.rpc_max_request_body_bytes,
                min_request_bytes,
            );
        }

        let max_block_hex_bytes = BITCOIN_MAX_BLOCK_BYTES.saturating_mul(2);
        let min_response_bytes = single_block_response_body_bytes(max_block_hex_bytes);
        if self.rpc_max_response_body_bytes < min_response_bytes {
            let min_response_mib = min_response_bytes as f64 / (1024.0 * 1024.0);
            bail!(
                "rpc_max_response_body_bytes ({}) must be at least {} bytes (~{:.2} MiB) to fit a \
                 full Bitcoin block; increase PROTOBLOCK_MAX_RESPONSE_MB",
                self.rpc_max_response_body_bytes,
                min_response_bytes,
                min_response_mib,
            );
        }

        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct FetcherConfigBuilder {
    rpc_url: Option<String>,
    rpc_user: Option<String>,
    rpc_password: Option<String>,
    thread_count: Option<usize>,
    max_batch_size_mb: Option<usize>,
    reorg_window_size: Option<usize>,
    start_height: Option<u64>,
    rpc_timeout: Option<Duration>,
    metrics_interval: Option<Duration>,
    queue_max_size_mb: Option<usize>,
    tip_idle_backoff: Option<Duration>,
    tip_refresh_interval: Option<Duration>,
    rpc_max_request_body_bytes: Option<usize>,
    rpc_max_response_body_bytes: Option<usize>,
}

impl FetcherConfigBuilder {
    pub fn rpc_url(mut self, url: impl Into<String>) -> Self {
        self.rpc_url = Some(url.into());
        self
    }

    pub fn rpc_user(mut self, user: impl Into<String>) -> Self {
        self.rpc_user = Some(user.into());
        self
    }

    pub fn rpc_password(mut self, password: impl Into<String>) -> Self {
        self.rpc_password = Some(password.into());
        self
    }

    pub fn thread_count(mut self, count: usize) -> Self {
        self.thread_count = Some(count);
        self
    }

    pub fn max_batch_size_mb(mut self, max_mb: usize) -> Self {
        self.max_batch_size_mb = Some(max_mb);
        self
    }

    pub fn reorg_window_size(mut self, window: usize) -> Self {
        self.reorg_window_size = Some(window);
        self
    }

    pub fn start_height(mut self, height: u64) -> Self {
        self.start_height = Some(height);
        self
    }

    pub fn rpc_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_timeout = Some(timeout);
        self
    }

    pub fn metrics_interval(mut self, interval: Duration) -> Self {
        self.metrics_interval = Some(interval);
        self
    }

    pub fn queue_max_size_mb(mut self, megabytes: usize) -> Self {
        self.queue_max_size_mb = Some(megabytes);
        self
    }

    pub fn tip_idle_backoff(mut self, backoff: Duration) -> Self {
        self.tip_idle_backoff = Some(backoff);
        self
    }

    pub fn tip_refresh_interval(mut self, interval: Duration) -> Self {
        self.tip_refresh_interval = Some(interval);
        self
    }

    pub fn rpc_max_request_body_bytes(mut self, bytes: usize) -> Self {
        self.rpc_max_request_body_bytes = Some(bytes);
        self
    }

    pub fn rpc_max_response_body_bytes(mut self, bytes: usize) -> Self {
        self.rpc_max_response_body_bytes = Some(bytes);
        self
    }

    pub fn build(self) -> Result<FetcherConfig> {
        let params = FetcherConfigParams {
            rpc_url: self.rpc_url.context("rpc_url is required")?,
            rpc_user: self.rpc_user.context("rpc_user is required")?,
            rpc_password: self.rpc_password.context("rpc_password is required")?,
            thread_count: self.thread_count.context("thread_count is required")?,
            max_batch_size_mb: self
                .max_batch_size_mb
                .context("max_batch_size_mb is required")?,
            reorg_window_size: self
                .reorg_window_size
                .context("reorg_window_size is required")?,
            start_height: self.start_height.context("start_height is required")?,
            rpc_timeout: self
                .rpc_timeout
                .unwrap_or_else(|| Duration::from_secs(DEFAULT_RPC_TIMEOUT_SECS)),
            metrics_interval: self
                .metrics_interval
                .unwrap_or(telemetry::DEFAULT_METRICS_INTERVAL),
            queue_max_size_mb: self.queue_max_size_mb.unwrap_or(DEFAULT_QUEUE_MAX_SIZE_MB),
            tip_idle_backoff: self
                .tip_idle_backoff
                .unwrap_or_else(|| Duration::from_secs(DEFAULT_TIP_IDLE_BACKOFF_SECS)),
            tip_refresh_interval: self
                .tip_refresh_interval
                .unwrap_or_else(|| Duration::from_secs(DEFAULT_TIP_REFRESH_INTERVAL_SECS)),
            rpc_max_request_body_bytes: self
                .rpc_max_request_body_bytes
                .unwrap_or(DEFAULT_HTTP_BODY_LIMIT_BYTES),
            rpc_max_response_body_bytes: self
                .rpc_max_response_body_bytes
                .unwrap_or(DEFAULT_HTTP_BODY_LIMIT_BYTES),
        };

        FetcherConfig::new(params)
    }
}

fn trimmed_string(value: String) -> String {
    value.trim().to_owned()
}

fn ensure_not_empty(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field} cannot be empty");
    }
    Ok(())
}

fn validate_url(url: &str) -> Result<()> {
    let url = url.trim();
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        bail!("rpc_url must start with http:// or https://");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::telemetry;
    use std::time::Duration;

    fn base_builder() -> FetcherConfigBuilder {
        FetcherConfig::builder()
            .rpc_url("http://localhost:8332")
            .rpc_user("user")
            .rpc_password("pass")
            .thread_count(4)
            .max_batch_size_mb(4)
            .reorg_window_size(7)
    }

    #[test]
    fn builder_produces_valid_config() {
        let config = base_builder().start_height(0).build().unwrap();
        assert_eq!(config.thread_count(), 4);
        assert_eq!(config.start_height(), 0);
        assert_eq!(config.queue_max_size_mb(), DEFAULT_QUEUE_MAX_SIZE_MB);
        assert_eq!(
            config.rpc_timeout(),
            Duration::from_secs(DEFAULT_RPC_TIMEOUT_SECS)
        );
        assert_eq!(
            config.metrics_interval(),
            telemetry::DEFAULT_METRICS_INTERVAL
        );
        assert_eq!(
            config.tip_idle_backoff(),
            Duration::from_secs(DEFAULT_TIP_IDLE_BACKOFF_SECS)
        );
        assert_eq!(
            config.tip_refresh_interval(),
            Duration::from_secs(DEFAULT_TIP_REFRESH_INTERVAL_SECS)
        );
        assert_eq!(
            config.rpc_max_request_body_bytes(),
            DEFAULT_HTTP_BODY_LIMIT_BYTES
        );
        assert_eq!(
            config.rpc_max_response_body_bytes(),
            DEFAULT_HTTP_BODY_LIMIT_BYTES
        );
    }

    #[test]
    fn metrics_interval_can_be_overridden() {
        let interval = Duration::from_secs(30);
        let queue_max_size_mb = 1024;
        let request_override = estimate_request_body_bytes(8);
        let max_block_hex_bytes = BITCOIN_MAX_BLOCK_BYTES * 2;
        let min_response_bytes = single_block_response_body_bytes(max_block_hex_bytes);
        let response_override = min_response_bytes.saturating_add(1_048_576);
        let config = base_builder()
            .metrics_interval(interval)
            .queue_max_size_mb(queue_max_size_mb)
            .rpc_max_request_body_bytes(request_override)
            .rpc_max_response_body_bytes(response_override)
            .start_height(0)
            .build()
            .expect("config should build");
        assert_eq!(config.metrics_interval(), interval);
        assert_eq!(config.queue_max_size_mb(), queue_max_size_mb);
        assert_eq!(config.rpc_max_request_body_bytes(), request_override);
        assert_eq!(config.rpc_max_response_body_bytes(), response_override);
    }

    #[test]
    fn tip_idle_backoff_can_be_overridden() {
        let backoff = Duration::from_secs(2);
        let config = base_builder()
            .tip_idle_backoff(backoff)
            .start_height(0)
            .build()
            .expect("config should build");
        assert_eq!(config.tip_idle_backoff(), backoff);
    }

    #[test]
    fn tip_refresh_interval_can_be_overridden() {
        let interval = Duration::from_secs(3);
        let config = base_builder()
            .tip_refresh_interval(interval)
            .start_height(0)
            .build()
            .expect("config should build");
        assert_eq!(config.tip_refresh_interval(), interval);
    }

    #[test]
    fn missing_required_fields_error() {
        let err = FetcherConfig::builder()
            .rpc_user("user")
            .rpc_password("pass")
            .thread_count(1)
            .max_batch_size_mb(1)
            .reorg_window_size(7)
            .start_height(0)
            .build()
            .unwrap_err();

        assert!(
            format!("{err}").contains("rpc_url"),
            "error should mention missing rpc_url"
        );
    }

    #[test]
    fn start_height_is_required() {
        let err = base_builder().build().unwrap_err();
        assert!(
            format!("{err}").contains("start_height"),
            "error should mention missing start_height"
        );
    }

    #[test]
    fn validation_catches_invalid_values() {
        let err = base_builder()
            .rpc_url("ftp://invalid")
            .start_height(10)
            .build()
            .unwrap_err();

        assert!(
            format!("{err}").contains("http:// or https://"),
            "error should mention URL scheme"
        );

        let err = base_builder()
            .thread_count(0)
            .start_height(10)
            .build()
            .unwrap_err();
        assert!(
            format!("{err}").contains("thread_count"),
            "error should mention thread count"
        );

        let err = base_builder()
            .rpc_timeout(Duration::from_secs(0))
            .start_height(10)
            .build()
            .unwrap_err();
        assert!(
            format!("{err}").contains("rpc_timeout"),
            "error should mention rpc_timeout"
        );

        let err = base_builder()
            .metrics_interval(Duration::from_secs(0))
            .start_height(10)
            .build()
            .unwrap_err();
        assert!(
            format!("{err}").contains("metrics_interval"),
            "error should mention metrics_interval"
        );

        let err = base_builder()
            .queue_max_size_mb(0)
            .start_height(10)
            .build()
            .unwrap_err();
        assert!(
            format!("{err}").contains("queue_max_size_mb"),
            "error should mention queue_max_size_mb"
        );

        let err = base_builder()
            .tip_idle_backoff(Duration::from_secs(0))
            .start_height(10)
            .build()
            .unwrap_err();
        assert!(
            format!("{err}").contains("tip_idle_backoff"),
            "error should mention tip_idle_backoff"
        );

        let err = base_builder()
            .tip_refresh_interval(Duration::from_secs(0))
            .start_height(10)
            .build()
            .unwrap_err();
        assert!(
            format!("{err}").contains("tip_refresh_interval"),
            "error should mention tip_refresh_interval"
        );

        let err = base_builder()
            .rpc_max_request_body_bytes(0)
            .start_height(10)
            .build()
            .unwrap_err();
        assert!(
            format!("{err}").contains("rpc_max_request_body_bytes"),
            "error should mention rpc_max_request_body_bytes"
        );

        let err = base_builder()
            .rpc_max_response_body_bytes(0)
            .start_height(10)
            .build()
            .unwrap_err();
        assert!(
            format!("{err}").contains("rpc_max_response_body_bytes"),
            "error should mention rpc_max_response_body_bytes"
        );
    }

    #[test]
    fn rpc_payload_limits_must_fit_single_block() {
        let min_request_bytes = estimate_request_body_bytes(1);
        let max_block_hex_bytes = BITCOIN_MAX_BLOCK_BYTES * 2;
        let min_response_bytes = single_block_response_body_bytes(max_block_hex_bytes);

        let err = base_builder()
            .rpc_max_request_body_bytes(min_request_bytes.saturating_sub(1))
            .rpc_max_response_body_bytes(min_response_bytes)
            .start_height(10)
            .build()
            .unwrap_err();
        assert!(
            format!("{err}").contains("PROTOBLOCK_MAX_REQUEST_MB"),
            "error should mention request env var"
        );

        let err = base_builder()
            .rpc_max_request_body_bytes(min_request_bytes)
            .rpc_max_response_body_bytes(min_response_bytes.saturating_sub(1))
            .start_height(10)
            .build()
            .unwrap_err();
        assert!(
            format!("{err}").contains("PROTOBLOCK_MAX_RESPONSE_MB"),
            "error should mention response env var"
        );
    }

    #[test]
    fn direct_constructor_runs_validation() {
        let err = FetcherConfig::new(FetcherConfigParams {
            rpc_url: "http://localhost:8332".into(),
            rpc_user: "user".into(),
            rpc_password: "pass".into(),
            thread_count: 0,
            max_batch_size_mb: 4,
            reorg_window_size: 7,
            start_height: 0,
            rpc_timeout: Duration::from_secs(DEFAULT_RPC_TIMEOUT_SECS),
            metrics_interval: telemetry::DEFAULT_METRICS_INTERVAL,
            queue_max_size_mb: DEFAULT_QUEUE_MAX_SIZE_MB,
            tip_idle_backoff: Duration::from_secs(DEFAULT_TIP_IDLE_BACKOFF_SECS),
            tip_refresh_interval: Duration::from_secs(DEFAULT_TIP_REFRESH_INTERVAL_SECS),
            rpc_max_request_body_bytes: DEFAULT_HTTP_BODY_LIMIT_BYTES,
            rpc_max_response_body_bytes: DEFAULT_HTTP_BODY_LIMIT_BYTES,
        })
        .unwrap_err();

        assert!(
            format!("{err}").contains("thread_count"),
            "error should mention invalid thread_count"
        );
    }
}
