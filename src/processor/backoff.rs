use anyhow::{anyhow, Result};
use std::time::Duration;
use tokio::task::yield_now;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
pub(crate) struct RetryBackoff<'a> {
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub max_attempts: Option<usize>,
    pub cancellation: Option<&'a CancellationToken>,
}

impl<'a> RetryBackoff<'a> {
    pub(crate) fn new(initial_delay: Duration, max_delay: Duration) -> Self {
        Self {
            initial_delay,
            max_delay,
            max_attempts: None,
            cancellation: None,
        }
    }

    pub(crate) fn with_max_attempts(mut self, max_attempts: usize) -> Self {
        self.max_attempts = Some(max_attempts);
        self
    }

    pub(crate) fn with_cancellation(mut self, token: &'a CancellationToken) -> Self {
        self.cancellation = Some(token);
        self
    }
}

pub(crate) enum RetryDisposition {
    Retry,
    Abort,
}

pub(crate) async fn retry_with_backoff<'a, T, F, Fut, L, C>(
    config: RetryBackoff<'a>,
    mut operation: F,
    mut on_retry: L,
    mut classify_error: C,
) -> Result<T>
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
    L: FnMut(usize, Duration, &anyhow::Error, bool),
    C: FnMut(usize, &anyhow::Error) -> RetryDisposition,
{
    let mut attempt = 0;
    let mut backoff = config.initial_delay;

    loop {
        attempt += 1;

        if let Some(token) = config.cancellation {
            if token.is_cancelled() {
                return Err(anyhow!("retry cancelled"));
            }
        }

        match operation(attempt).await {
            Ok(value) => return Ok(value),
            Err(err) => match classify_error(attempt, &err) {
                RetryDisposition::Abort => return Err(err),
                RetryDisposition::Retry => {
                    let exhausted = config
                        .max_attempts
                        .map(|max| attempt >= max)
                        .unwrap_or(false);

                    on_retry(attempt, backoff, &err, !exhausted);

                    if exhausted {
                        return Err(err);
                    }

                    sleep_with_cancellation(backoff, config.cancellation).await?;
                    backoff = next_backoff(backoff, config.max_delay);
                }
            },
        }
    }
}

async fn sleep_with_cancellation(
    delay: Duration,
    cancellation: Option<&CancellationToken>,
) -> Result<()> {
    if delay.is_zero() {
        yield_now().await;
        return Ok(());
    }

    if let Some(token) = cancellation {
        tokio::select! {
            _ = token.cancelled() => Err(anyhow!("retry cancelled")),
            _ = sleep(delay) => Ok(()),
        }
    } else {
        sleep(delay).await;
        Ok(())
    }
}

fn next_backoff(current: Duration, max_backoff: Duration) -> Duration {
    if current.is_zero() {
        return max_backoff.min(Duration::from_millis(1));
    }

    let mut next = current.saturating_mul(2);
    if next > max_backoff {
        next = max_backoff;
    }
    next
}
