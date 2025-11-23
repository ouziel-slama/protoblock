# Protoblock

[![Tests](https://github.com/ouziel-slama/protoblock/actions/workflows/tests.yml/badge.svg)](https://github.com/ouziel-slama/protoblock/actions/workflows/tests.yml)
[![Coverage](https://github.com/ouziel-slama/protoblock/actions/workflows/coverage.yml/badge.svg)](https://github.com/ouziel-slama/protoblock/actions/workflows/coverage.yml)
[![Format](https://github.com/ouziel-slama/protoblock/actions/workflows/fmt.yml/badge.svg)](https://github.com/ouziel-slama/protoblock/actions/workflows/fmt.yml)
[![Clippy](https://github.com/ouziel-slama/protoblock/actions/workflows/clippy.yml/badge.svg)](https://github.com/ouziel-slama/protoblock/actions/workflows/clippy.yml)
[![Publish Crate](https://github.com/ouziel-slama/protoblock/actions/workflows/publish.yml/badge.svg)](https://github.com/ouziel-slama/protoblock/actions/workflows/publish.yml)

Protoblock is an asynchronous Bitcoin block ingestion pipeline. It multiplexes JSON-RPC batch calls across a pool of workers, pre-processes blocks with user-defined logic, and feeds an ordered queue that guarantees strictly increasing heights before executing stateful `BlockProtocol` hooks. The crate focuses on correctness under reorgs, graceful shutdown, and observability so downstream consumers can build reliable backfills or live processors on top of it.

## Installation

1. **Requirements**
   - Rust 1.79+ with the `tokio` multi-threaded runtime available.
   - Access to a Bitcoin Core node with RPC enabled (`getblockhash` and `getblock` permissions).
   - `bitcoind` should have `-txindex=1` if you plan to access historical blocks arbitrarily.

2. **Add the dependency**
   - When the crate is published on crates.io:
     ```toml
     protoblock = "0.1"
     ```
   - While developing in-tree, point to the local path or Git repository:
     ```toml
     protoblock = { path = "../protoblock" }
     # or
     protoblock = { git = "https://github.com/your-org/protoblock", rev = "<commit>" }
     ```

3. **Build & test**
   ```bash
   cargo check
   cargo test --all                     # unit + mock integration tests
   PROTOBLOCK_RUN_REGTESTS=1 cargo test regtest  # opt-in real bitcoind tests
   ```
   The regtest suite requires a local `bitcoind` binary on your `PATH`. Leave `PROTOBLOCK_RUN_REGTESTS` unset (the default) to skip those heavier tests.

4. **Optional tooling**
   - Enable tracing by setting `RUST_LOG=info` (or a more specific target) before running your binary.

## Quick Start

Below is a minimal example that prints block metadata while keeping the heavy lifting inside Protoblock.

```rust
use anyhow::Result;
use bitcoin::Block;
use protoblock::{
    BlockProtocol, FetcherConfig, ProtocolFuture, ProtocolPreProcessFuture, Runner,
};

struct LoggingProtocol;

impl BlockProtocol for LoggingProtocol {
    type PreProcessed = Block;

    fn pre_process(
        &self,
        block: Block,
        height: u64,
    ) -> ProtocolPreProcessFuture<Self::PreProcessed> {
        Box::pin(async move {
            tracing::info!(height, txs = block.txdata.len(), "downloaded block");
            Ok(block)
        })
    }

    fn process<'a>(
        &'a mut self,
        block: Self::PreProcessed,
        height: u64,
    ) -> ProtocolFuture<'a> {
        Box::pin(async move {
            // Apply state changes serially (write to DB, forward to Kafka, etc.)
            tracing::info!(height, hash = %block.block_hash(), "committing block");
            Ok(())
        })
    }

    fn rollback<'a>(&'a mut self, keep_height: u64) -> ProtocolFuture<'a> {
        Box::pin(async move {
            tracing::warn!(
                keep_height,
                "chain reorg rollback requested; purge heights above this value"
            );
            Ok(())
        })
    }

    fn shutdown<'a>(&'a mut self) -> ProtocolFuture<'a> {
        Box::pin(async move {
            tracing::info!("protocol shutdown hook invoked");
            Ok(())
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    protoblock::init_tracing();

    let config = FetcherConfig::builder()
        .rpc_url("http://127.0.0.1:8332")
        .rpc_user("rpcuser")
        .rpc_password("rpcpass")
        .thread_count(4)
        .max_batch_size_mb(4)
        .reorg_window_size(12)
        .queue_max_size_mb(4_096)
        .start_height(0)
        .build()?;

    let protocol = LoggingProtocol;
    let mut runner = Runner::new(config, protocol);
    runner.run_until_ctrl_c().await?;
    Ok(())
}
```

Run the example binary, mine a few blocks on your regtest node, and observe Protoblock streaming them in order with backpressure and retry handling baked in.

## Examples

`examples/rpc_speed.rs` spins up a no-op protocol so you can measure raw JSON-RPC throughput and worker scheduling overhead. It renders a live progress bar (via `indicatif`) whose length follows the current blockchain tip, and it runs until you send Ctrl-C.

```bash
export PROTOBLOCK_RPC_URL="http://127.0.0.1:8332"   # optional (default http://localhost:8332)
export PROTOBLOCK_RPC_USER="rpcuser"                # optional (default rpc)
export PROTOBLOCK_RPC_PASSWORD="rpcpass"            # optional (default rpc)
export PROTOBLOCK_START_HEIGHT=830000               # optional (default 0)
export PROTOBLOCK_THREAD_COUNT=8                    # optional (default 4)
export PROTOBLOCK_MAX_BATCH_MB=8                    # optional (default 4)
export PROTOBLOCK_QUEUE_MB=8192                     # optional (default 200)
export PROTOBLOCK_REORG_WINDOW=24                   # optional (default 12)
export PROTOBLOCK_MAX_REQUEST_MB=10                 # optional (default 10)
export PROTOBLOCK_MAX_RESPONSE_MB=10                # optional (default 10)
cargo run --example rpc_speed
```

Adjust the optional knobs to reflect your node’s capabilities. The processor and pre-processor intentionally discard every block so the run isolates RPC performance, and the example will keep streaming until you terminate it.

## Architecture Overview

- **Runner** wires graceful shutdown, signal handling, and owns the root `CancellationToken`.
- **BlocksFetcher** owns the worker pool, ordered queue, progress tracking, telemetry, and the main processing loop that applies `BlockProtocol::process`.
- **Workers** fetch batches of blocks concurrently, call the asynchronous `pre_process` hook, and enqueue `PreProcessedBlock` items. Each worker now maintains a double-buffer: it launches the next `batch_get_blocks` call as soon as a batch is in hand, lets that RPC run while `pre_process` executes, and aborts the prefetched future if shutdown/stop/generation changes occur. Batch sizes still grow/shrink automatically based on the observed megabytes per batch.
- **RPC Module (`src/rpc/`)** splits authentication, options, retry instrumentation, metrics, helpers, and the `AsyncRpcClient` into focused submodules while re-exporting the same public surface (`crate::rpc::*`). This keeps transport code isolated, mockable via the `BlockBatchClient` trait, and ready for future transports.
- **OrderedBlockQueue** releases blocks strictly in-order and enforces a configurable byte budget so workers apply backpressure instead of growing memory unbounded, ensuring the sequential `process` stage never observes a height gap even though workers fetch out-of-order.
- **QueueByteSize** is a lightweight trait (re-exported from `protoblock::QueueByteSize`) implemented by every protocol’s `PreProcessed` type. Workers call `PreProcessedBlock::queue_bytes()` so telemetry, `BatchSizer`, and queue backpressure track the *post*-`pre_process` footprint instead of the raw RPC payload size.
- **ReorgWindow** tracks recent hashes so the processing loop can detect when a new block contradicts the stored chain, invoke `rollback`, and re-seed the queue safely. The loop passes the fork height (the last height that must remain), so `rollback` implementations must purge any state for heights greater than the provided value.
- **RpcCircuitBreaker** wraps the JSON-RPC client to avoid hammering a failing node. When the breaker is Open, workers pause and allow the node to recover before resuming via Half-Open probes.
- **Telemetry & Metrics Reporter** expose throughput, queue blocks/bytes, and RPC error rates through tracing logs at a configurable interval via `runtime::telemetry::Telemetry`.
- **runtime::fatal::FatalErrorHandler** and `runtime::progress::ProgressTracker` guarantee that fatal protocol errors stop the pipeline, bubble up through `BlocksFetcher::stop`/`Runner::run_until_ctrl_c`, and leave restart decisions to the operator-provided `start_height` (no persistence is performed for you). The tracker reports `start_height - 1` immediately so hosts can observe which heights are already durable after a restart.
- **Shutdown Semantics** intentionally favor quick termination: when the shared cancellation token flips, workers stop fetching immediately and any blocks left in `OrderedBlockQueue` are dropped instead of drained.
- **Tip wait mode** automatically reduces the worker pool to a single task when the pipeline catches up to the current Bitcoin tip. This is acceptable because Bitcoin produces roughly one block every ~10 minutes—there is no realistic scenario where dozens of new blocks appear instantly—so a single worker can comfortably keep pace until a restart re-expands the pool. Control how aggressively that idle worker polls for the next block via `FetcherConfig::tip_idle_backoff`, and independently tune the background tip refresh cadence via `FetcherConfig::tip_refresh_interval`.
- **WorkerPool / TipTracker / ReorgManager / LifecycleHandles** split responsibilities that used to sit inside `BlocksFetcher`. The worker pool manages concurrency, `TipTracker` decides when to downshift near tip, `ReorgManager` handles RPC lookups and rollbacks, and `LifecycleHandles` bundles per-run resources (fatal handler, child cancellation token, metrics reporter, tip refresher). This keeps each module focused and easier to extend.

Refer to `docs/architecture.md` for a deep dive into each component and how they interact.

## Configuration

`FetcherConfig` centralizes every runtime parameter. Build it via `FetcherConfig::builder()` (or call `FetcherConfig::new(FetcherConfigParams { .. })` if you already have concrete values) to benefit from validation and sensible defaults. The struct’s fields are intentionally private so every instance is validated before it is used.

| Field | Description | Notes |
| --- | --- | --- |
| `rpc_url` | Full HTTP URL of the Bitcoin Core JSON-RPC endpoint. | Must start with `http://` or `https://`. |
| `rpc_user` / `rpc_password` | RPC credentials. | Required; empty strings are rejected. |
| `thread_count` | Number of worker tasks fetching blocks. | Must be ≥ 1; also influences stride between heights. |
| `max_batch_size_mb` | Target megabytes per batch before workers increase/decrease the block count. | Controls throughput vs. memory pressure. |
| `reorg_window_size` | Size of the sliding window (in blocks) used to detect and roll back chain reorganizations. | Choose ≥ the maximum depth you want to tolerate. |
| `queue_max_size_mb` | Maximum megabytes of queued `PreProcessedBlock` data allowed in the ordered queue. | Defaults to 200 MB; workers block when the byte budget is exhausted (backpressure). |
| `start_height` | First height to request when the pipeline starts. | Required; Protoblock always restarts from this height. |
| `rpc_timeout` | Per-RPC timeout duration. | Defaults to 10 seconds if unset. |
| `metrics_interval` | Interval between telemetry snapshots logged to `protoblock::metrics`. | Defaults to 5 seconds; tune to align with your logging/observability cadence. |
| `tip_idle_backoff` | How long workers wait before retrying when they reach the tip or receive `HeightOutOfRange`. | Defaults to 10 seconds; lower values poll for new blocks more aggressively at the cost of extra RPC chatter. |
| `tip_refresh_interval` | Frequency of background `getblockcount` polling performed by the tip tracker. | Defaults to 10 seconds; lower values update the shared tip cache faster without affecting worker idle backoff. |
| `rpc_max_request_body_bytes` | Maximum JSON-RPC HTTP request body size permitted by the client. | Defaults to 10 MB (jsonrpsee default); set this to match your node/frontend proxy limits. |
| `rpc_max_response_body_bytes` | Maximum JSON-RPC HTTP response body size permitted by the client. | Defaults to 10 MB; workers proactively split batches so responses remain under this ceiling. |

Tips:
- Replace any `FetcherConfig { .. }` literals with `FetcherConfig::builder()` or `FetcherConfig::new(FetcherConfigParams { .. })`; direct struct construction is no longer supported.
- Reuse `Runner::cancellation_token()` if your host application already has a shutdown signal source.
- Monitor the tracing target `protoblock::metrics` to observe queue blocks, queue bytes, throughput, and RPC failures in real time.

#### Upgrading existing configs

If you previously instantiated `FetcherConfig` via a struct literal, switch to the builder pattern or `FetcherConfig::new(FetcherConfigParams { .. })`. Both paths perform validation before returning, eliminating the window where an invalid configuration could reach the fetcher.

## Documentation Set

- `docs/architecture.md` — details the internal pipeline and concurrency model.
- `docs/usage.md` — walks through configuration, protocol implementation, and recommended operational practices.

Both documents complement this README and complete the Phase 7 documentation checklist.

## License

Protoblock is dual-licensed under the MIT License (`LICENSE-MIT`) or the Apache License, Version 2.0 (`LICENSE-APACHE`), matching the approach used in `rollblock`. You may choose either license to use, copy, modify, and distribute this project.

