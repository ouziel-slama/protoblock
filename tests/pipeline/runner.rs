use std::time::Duration;

use crate::support::{
    helpers::{
        assert_is_contiguous, init_tracing, wait_for_processed_len, RecordingProtocol,
        SharedRecordingProtocol,
    },
    mock_rpc::{MockChain, MockRpcServer},
};
use anyhow::{Context, Result};
use protoblock::{BlocksFetcher, FetcherConfig, Runner};
use tokio::time::{sleep, timeout};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_failure_cancels_pipeline() -> Result<()> {
    init_tracing();
    let chain = MockChain::new(16);
    chain.corrupt_block_hex(0, "zzzz")?;
    let server = MockRpcServer::start(chain.clone()).await?;

    let config = FetcherConfig::builder()
        .rpc_url(server.url())
        .rpc_user("user")
        .rpc_password("pass")
        .thread_count(2)
        .max_batch_size_mb(1)
        .reorg_window_size(4)
        .start_height(0)
        .build()?;

    let mut fetcher = BlocksFetcher::new(config, RecordingProtocol::default());
    fetcher.start().await?;
    sleep(Duration::from_millis(250)).await;

    let err = fetcher
        .stop()
        .await
        .expect_err("worker failure should surface from stop");
    let message = format!("{err:#}");
    assert!(
        message.contains("block processing pipeline aborted"),
        "expected worker failure to cancel pipeline, got {message}"
    );
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runner_exits_on_worker_failure() -> Result<()> {
    init_tracing();
    let chain = MockChain::new(12);
    chain.corrupt_block_hex(0, "zzzz")?;
    let server = MockRpcServer::start(chain.clone()).await?;

    let config = FetcherConfig::builder()
        .rpc_url(server.url())
        .rpc_user("user")
        .rpc_password("pass")
        .thread_count(2)
        .max_batch_size_mb(1)
        .reorg_window_size(4)
        .start_height(0)
        .build()?;

    let mut runner = Runner::new(config, RecordingProtocol::default());
    let outcome = timeout(Duration::from_secs(5), runner.run_until_ctrl_c())
        .await
        .context("runner should stop after worker failure")?;

    let err = outcome.expect_err("worker failure should abort runner");
    let message = format!("{err:#}");
    assert!(
        message.contains("block processing pipeline aborted"),
        "runner did not propagate worker failure, got {message}"
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runner_can_restart_after_stop() -> Result<()> {
    init_tracing();
    let chain = MockChain::new(48);
    let server = MockRpcServer::start(chain).await?;

    let config = FetcherConfig::builder()
        .rpc_url(server.url())
        .rpc_user("user")
        .rpc_password("pass")
        .thread_count(2)
        .max_batch_size_mb(1)
        .reorg_window_size(8)
        .start_height(0)
        .build()?;

    let (protocol, state) = SharedRecordingProtocol::new();
    let mut runner = Runner::new(config, protocol);

    runner.start().await?;
    wait_for_processed_len(&state, 10, Duration::from_secs(5)).await?;
    runner.stop().await?;

    runner.start().await?;
    wait_for_processed_len(&state, 20, Duration::from_secs(5)).await?;
    runner.stop().await?;
    server.shutdown().await;

    let guard = state.lock().await;
    assert!(
        guard.processed.len() >= 21,
        "runner restart should continue processing blocks, got {}",
        guard.processed.len()
    );
    assert_is_contiguous(&guard.processed);

    Ok(())
}
