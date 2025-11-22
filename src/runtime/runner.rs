use crate::{BlockProtocol, BlocksFetcher, FetcherConfig};
use anyhow::Result;
use tokio::signal;
use tokio_util::sync::CancellationToken;

/// Coordinates the block fetcher lifecycle and handles OS signals for graceful shutdowns.
pub struct Runner<P: BlockProtocol> {
    fetcher: BlocksFetcher<P>,
    shutdown: CancellationToken,
    started: bool,
}

impl<P: BlockProtocol> Runner<P> {
    /// Creates a new runner and wires a root [`CancellationToken`] that propagates
    /// through the entire pipeline (fetcher, workers, queue, reorg processing).
    pub fn new(config: FetcherConfig, protocol: P) -> Self {
        let shutdown = CancellationToken::new();
        let fetcher = BlocksFetcher::with_cancellation_token(config, protocol, shutdown.clone());
        Self {
            fetcher,
            shutdown,
            started: false,
        }
    }

    /// Returns a clone of the root shutdown token so external callers can integrate
    /// with their own signal handlers or cancellation strategies.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Starts the underlying fetcher pipeline.
    pub async fn start(&mut self) -> Result<()> {
        if self.started {
            return Ok(());
        }

        self.fetcher.start().await?;
        self.started = true;
        Ok(())
    }

    /// Stops the pipeline gracefully by cancelling the root token and delegating to the fetcher.
    pub async fn stop(&mut self) -> Result<()> {
        if !self.started {
            return Ok(());
        }

        self.shutdown.cancel();
        self.fetcher.stop().await?;
        self.started = false;
        self.reinitialize_shutdown_token();
        Ok(())
    }

    /// Runs until a Ctrl-C (SIGINT) is received or the shutdown token is cancelled elsewhere.
    pub async fn run_until_ctrl_c(&mut self) -> Result<()> {
        self.start().await?;
        tracing::info!("runner started; waiting for Ctrl-C (SIGINT) to initiate shutdown");

        tokio::select! {
            _ = signal::ctrl_c() => {
                tracing::info!("Ctrl-C received; shutting down runner");
            }
            _ = self.shutdown.cancelled() => {
                tracing::info!("runner shutdown token cancelled");
            }
        }

        self.shutdown.cancel();
        self.fetcher.stop().await?;
        self.started = false;
        self.reinitialize_shutdown_token();
        Ok(())
    }

    fn reinitialize_shutdown_token(&mut self) {
        self.shutdown = CancellationToken::new();
        self.fetcher.replace_shutdown_root(self.shutdown.clone());
    }
}
