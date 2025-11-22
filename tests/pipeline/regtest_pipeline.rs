use std::time::Duration;

use crate::support::{
    helpers::{
        assert_is_contiguous, init_tracing, regtest_tests_enabled, wait_for_height,
        wait_for_height_at_most, RecordingProtocol, REGTEST_GUARD,
    },
    regtest::RegtestNode,
};
use anyhow::Result;
use protoblock::{BlocksFetcher, FetcherConfig};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn regtest_fetcher_processes_real_blocks() -> Result<()> {
    init_tracing();
    if !regtest_tests_enabled() {
        tracing::info!(
            "skipping regtest_fetcher_processes_real_blocks (set PROTOBLOCK_RUN_REGTESTS=1)"
        );
        return Ok(());
    }
    let _guard = REGTEST_GUARD.lock().await;
    let node = RegtestNode::start().await?;
    node.mine_blocks(35).await?;
    let chain_tip = node.best_height().await?;
    let target_height = chain_tip.saturating_sub(2);

    let config = FetcherConfig::builder()
        .rpc_url(node.rpc_url())
        .rpc_user(node.rpc_user())
        .rpc_password(node.rpc_password())
        .thread_count(2)
        .max_batch_size_mb(3)
        .reorg_window_size(8)
        .start_height(0)
        .build()?;

    let mut fetcher = BlocksFetcher::new(config, RecordingProtocol::default());
    fetcher.start().await?;
    wait_for_height(&fetcher, target_height, Duration::from_secs(30)).await?;
    fetcher.stop().await?;
    node.shutdown().await?;

    let protocol = fetcher.protocol().write().await;
    assert!(
        protocol.rollback_points().is_empty(),
        "regtest run should not reorg"
    );
    assert!(
        protocol.processed().last().copied().unwrap_or_default() >= target_height,
        "expected to reach at least height {target_height} after regtest sync"
    );
    assert_is_contiguous(protocol.processed());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn regtest_fetcher_handles_reorgs() -> Result<()> {
    init_tracing();
    if !regtest_tests_enabled() {
        tracing::info!("skipping regtest_fetcher_handles_reorgs (set PROTOBLOCK_RUN_REGTESTS=1)");
        return Ok(());
    }
    let _guard = REGTEST_GUARD.lock().await;
    let node = RegtestNode::start().await?;
    node.mine_blocks(45).await?;
    let initial_tip = node.best_height().await?;

    let config = FetcherConfig::builder()
        .rpc_url(node.rpc_url())
        .rpc_user(node.rpc_user())
        .rpc_password(node.rpc_password())
        .thread_count(2)
        .max_batch_size_mb(3)
        .reorg_window_size(16)
        .start_height(0)
        .build()?;

    let mut fetcher = BlocksFetcher::new(config, RecordingProtocol::default());
    fetcher.start().await?;
    let initial_target = initial_tip.saturating_sub(2);
    wait_for_height(&fetcher, initial_target, Duration::from_secs(30)).await?;
    let pre_reorg_height = fetcher
        .last_confirmed_height()
        .expect("regtest pipeline should have processed at least one block");
    tracing::info!(pre_reorg_height, "captured pre-reorg height");

    let reorg_depth = 3usize;
    node.invalidate_tip(reorg_depth).await?;
    node.mine_blocks((reorg_depth + 2) as u64).await?;
    let final_height = node.best_height().await?;
    let target_height = final_height
        .saturating_sub(1)
        .max(pre_reorg_height.saturating_add(1));
    tracing::info!(final_height, target_height, "post-reorg height targets");
    wait_for_height(&fetcher, target_height, Duration::from_secs(30)).await?;

    fetcher.stop().await?;
    node.shutdown().await?;

    let protocol = fetcher.protocol().write().await;
    assert!(
        !protocol.rollback_points().is_empty(),
        "reorg test should record at least one rollback"
    );
    let last_rollback = *protocol.rollback_points().last().unwrap();
    assert!(
        last_rollback <= final_height,
        "rollback height {last_rollback} should not exceed final height {final_height}"
    );
    assert!(
        protocol.processed().last().copied().unwrap_or_default() >= target_height,
        "pipeline should catch up close to the new tip (target {target_height})"
    );
    assert_is_contiguous(protocol.processed());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn regtest_fetcher_rolls_back_on_tip_regression_without_growth() -> Result<()> {
    init_tracing();
    if !regtest_tests_enabled() {
        tracing::info!("skipping tip regression test (set PROTOBLOCK_RUN_REGTESTS=1)");
        return Ok(());
    }
    let _guard = REGTEST_GUARD.lock().await;
    let node = RegtestNode::start().await?;
    node.mine_blocks(40).await?;
    let initial_tip = node.best_height().await?;

    let config = FetcherConfig::builder()
        .rpc_url(node.rpc_url())
        .rpc_user(node.rpc_user())
        .rpc_password(node.rpc_password())
        .thread_count(2)
        .max_batch_size_mb(3)
        .reorg_window_size(16)
        .start_height(0)
        .build()?;

    let mut fetcher = BlocksFetcher::new(config, RecordingProtocol::default());
    fetcher.start().await?;
    wait_for_height(
        &fetcher,
        initial_tip.saturating_sub(2),
        Duration::from_secs(30),
    )
    .await?;
    let pre_reorg_height = fetcher
        .last_confirmed_height()
        .expect("pipeline should have confirmed blocks");

    let reorg_depth = 4usize;
    node.invalidate_tip(reorg_depth).await?;
    let regressed_tip = node.best_height().await?;
    tracing::info!(
        reorg_depth,
        regressed_tip,
        pre_reorg_height,
        "tip regressed after invalidate"
    );

    wait_for_height_at_most(&fetcher, regressed_tip, Duration::from_secs(20)).await?;

    node.mine_blocks((reorg_depth + 2) as u64).await?;
    let final_tip = node.best_height().await?;
    wait_for_height(
        &fetcher,
        final_tip.saturating_sub(1),
        Duration::from_secs(30),
    )
    .await?;

    fetcher.stop().await?;
    node.shutdown().await?;

    let protocol = fetcher.protocol().write().await;
    let rollback_points = protocol.rollback_points();
    assert!(
        !rollback_points.is_empty(),
        "tip regression should trigger rollback"
    );
    let last_rollback = *rollback_points.last().unwrap();
    assert!(
        last_rollback <= pre_reorg_height,
        "rollback height {last_rollback} should not exceed pre-reorg height {pre_reorg_height}"
    );
    assert!(
        protocol.processed().last().copied().unwrap_or_default() >= final_tip.saturating_sub(1),
        "pipeline should catch up after tip regression"
    );
    assert_is_contiguous(protocol.processed());

    Ok(())
}
