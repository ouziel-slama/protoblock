# Protoblock Usage Guide

This document complements the README by walking through everyday tasks required to embed Protoblock inside an application or service.

## 1. Prerequisites

- **Runtime**: Tokio multi-threaded runtime. Most host binaries should annotate `#[tokio::main(flavor = "multi_thread")]`.
- **Bitcoin Core node**: Enable RPC (`-server`, `-rpcuser`, `-rpcpassword`) and, ideally, `-txindex=1` for random block access. The node must allow batch `getblockhash`/`getblock` calls from the Protoblock host.
- **Tracing/logging**: Either rely on `protoblock::init_tracing()` or integrate with your own tracing subscriber before the pipeline starts.
- **Start height management**: Plan for how your orchestrator will remember the height you want to resume from; Protoblock requires you to supply `start_height` manually at every launch.

## 2. Building a Configuration

Use the builder for clarity and validation:

```rust
use protoblock::FetcherConfig;
use std::time::Duration;

let config = FetcherConfig::builder()
    .rpc_url("http://127.0.0.1:8332")
    .rpc_user("user")
    .rpc_password("pass")
    .thread_count(6)
    .max_batch_size_mb(8)
    .reorg_window_size(24)
    .queue_max_size_mb(4_096)
    .start_height(840_000)
    .rpc_timeout(Duration::from_secs(15))
    .tip_idle_backoff(Duration::from_secs(5))
    .tip_refresh_interval(Duration::from_secs(8))
    .build()?;
```

If you already have concrete values (for example, parsed CLI flags), construct a `FetcherConfigParams` literal and pass it to `FetcherConfig::new(...)`—the params struct mirrors the configuration fields and performs the same validation as the builder.

> `FetcherConfig` cannot be instantiated via a struct literal; using the builder or `FetcherConfig::new(FetcherConfigParams { .. })` guarantees that validation runs before the fetcher observes the values.

Tips:

- Size `thread_count` based on the number of concurrent RPC requests your node can tolerate. Protoblock automatically scales `max_concurrent_requests` to `max(32, thread_count * 4)`.
- Pick a `reorg_window_size` that exceeds the largest reorg you are willing to roll back. For most Bitcoin workloads, 12–24 blocks is sufficient.
- Protoblock does **not** persist progress; supply the exact `start_height` you wish to replay from on every restart.
- Tune `tip_idle_backoff` to control how frequently the pipeline probes for the next block once it reaches the chain tip (shorter waits mean faster detection, longer waits reduce RPC noise).
- Adjust `tip_refresh_interval` independently to decide how often the shared tip cache is refreshed, even while the pipeline is still catching up.
- Align `rpc_max_request_body_bytes` and `rpc_max_response_body_bytes` with your node or proxy HTTP limits so workers can split batches before jsonrpsee enforces the ceiling.

## 3. Implementing `BlockProtocol`

`BlockProtocol` is how you plug custom logic into the pipeline. Protoblock itself does not persist progress, so if you need durability you can add it inside your protocol implementation. A typical implementation:

```rust
use anyhow::Result;
use bitcoin::Block;
use protoblock::{BlockProtocol, ProtocolFuture, ProtocolPreProcessFuture};

pub struct Writer {
    storage: sled::Db,
    highest_persisted: u64,
}

impl BlockProtocol for Writer {
    type PreProcessed = Vec<u8>; // Serialized records

    fn pre_process(
        &self,
        block: Block,
        height: u64,
    ) -> ProtocolPreProcessFuture<Self::PreProcessed> {
        Box::pin(async move {
            // Perform CPU or IO heavy work concurrently (e.g., extract tx metadata)
            let encoded = serde_json::to_vec(&block)?;
            Ok(encoded)
        })
    }

    fn process<'a>(
        &'a mut self,
        data: Self::PreProcessed,
        height: u64,
    ) -> ProtocolFuture<'a> {
        Box::pin(async move {
            self.storage.insert(height.to_be_bytes(), data)?;
            self.storage.flush()?;
            self.highest_persisted = height;
            Ok(())
        })
    }

    fn rollback<'a>(&'a mut self, keep_height: u64) -> ProtocolFuture<'a> {
        Box::pin(async move {
            // Protoblock passes the fork height (the last height that must remain),
            // so delete everything strictly greater than `keep_height`.
            while self.highest_persisted > keep_height {
                self.storage
                    .remove(self.highest_persisted.to_be_bytes())?;
                self.highest_persisted -= 1;
            }
            Ok(())
        })
    }

    fn shutdown<'a>(&'a mut self) -> ProtocolFuture<'a> {
        Box::pin(async move {
            self.storage.flush()?;
            Ok(())
        })
    }
}
```

`type PreProcessed` must also implement `protoblock::QueueByteSize` so the queue can charge the correct number of bytes per block. Built-in collections (`Vec<u8>`, `String`, `bytes::Bytes`, tuples, `Option`, etc.) already implement the trait. For custom payloads, compose the usage from individual fields:

```rust
use protoblock::QueueByteSize;

#[derive(Clone)]
struct Snapshot {
    proof: Vec<u8>,
    summary: String,
}

impl QueueByteSize for Snapshot {
    fn queue_bytes(&self) -> usize {
        self.proof.queue_bytes().saturating_add(self.summary.queue_bytes())
    }
}
```

> **Rollback contract**: The runtime invokes `rollback` exactly once per reorg with the fork height (the highest block that must remain confirmed). Your implementation must treat that number as an inclusive lower bound and delete or undo every side effect for heights greater than `keep_height`.

Guidelines:

- `pre_process` runs in the worker tasks and should avoid holding mutable state. Use it for expensive parsing or DB lookups that can happen in parallel.
- Because workers release the read lock immediately after creating the future, `pre_process` must return a `'static` future (clone any `Arc`s or config you need inside the async block instead of borrowing `self` across awaits).
- `process` runs on a single task to guarantee order. Keep it idempotent if possible—Protoblock may re-run it after a rollback.
- `rollback` receives the fork height (the last height that must remain) and must remove any side effects for heights greater than that value.
- `shutdown` is your chance to flush caches, close connections, or release resources gracefully.

> **Lifecycle contract**: `BlocksFetcher::stop`/`Runner::stop` always invoke `BlockProtocol::shutdown`, even if you plan to call `start` again on the same runner. Make `shutdown` idempotent and restart-safe (e.g., reopen database handles lazily on the next `pre_process`/`process` call), or construct a fresh `Runner` with a brand-new protocol instance each time you restart.

## 4. Starting the Pipeline

```rust
use anyhow::Result;
use protoblock::{init_tracing, Runner};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let config = /* build config */;
    let protocol = Writer { /* ... */ };

    let mut runner = Runner::new(config, protocol);
    runner.run_until_ctrl_c().await?;
    Ok(())
}
```

Alternate flows:

- Call `runner.start().await?` and `runner.stop().await?` manually if you integrate with an existing service runtime.
- Clone `runner.cancellation_token()` and hook it up to your orchestrator (e.g., signal from an HTTP admin endpoint).

## 5. Monitoring & Telemetry

- Enable `RUST_LOG=info,protoblock::metrics=trace` to see batch sizes, queue blocks/bytes, throughput, and RPC error counts.
- Call `BlocksFetcher::telemetry()` (via your own handle to the fetcher) to poll cumulative counters from `runtime::telemetry::Telemetry` for dashboards.
- Inspect `AsyncRpcClient::metrics()` if you need detailed RPC statistics (total requests, error rate, breaker state).
- Tune the noise level by setting `FetcherConfig::metrics_interval` (defaults to 5 seconds) so snapshots line up with your observability pipeline.

## 6. Operational Recommendations

- **Graceful shutdown**: Always prefer cancelling the root token (Ctrl-C/Kill SIGINT, or your orchestrator’s signal). Protoblock stops immediately—workers halt and any remaining queue entries are dropped—before calling `protocol.shutdown`, so pick a shutdown window where losing in-flight blocks is acceptable.
- **Restarts**: Because progress is in-memory only, plan for your orchestration layer to pass the correct `start_height` when relaunching.
- **Backpressure**: Watch both queue block count and `queue_bytes`. If the byte metric rides near the configured `queue_max_size_mb`, workers are being throttled because `process` is slower than `pre_process`. Lower `thread_count`/`max_batch_size_mb`, shrink `queue_max_size_mb`, or optimize downstream storage.
- **RPC hygiene**: Put Protoblock behind a dedicated Bitcoin Core node if possible. The adaptive batcher and circuit breaker are friendly to shared nodes, but isolation avoids noisy neighbors.
- **Testing**: Leverage the regtest pipeline examples in `tests/pipeline/mock_pipeline.rs` and `tests/pipeline/regtest_pipeline.rs`—spin up a regtest `bitcoind`, mine blocks, and assert your protocol behavior under reorgs and restarts. Set `PROTOBLOCK_RUN_REGTESTS=1` when running `cargo test` if you want to execute the regtest suite; leave it unset to skip those heavier cases.

## 7. Troubleshooting Checklist

- **Stuck at height N**: Check logs for `HeightOutOfRange` (you might be at the tip) or `RpcError::Timeout`. Adjust `rpc_timeout` or inspect node health.
- **Reorg loops**: Increase `reorg_window_size` and verify that `rollback` truly rewinds side effects. Persistent divergence often indicates downstream storage still has conflicting rows.
- **High latency**: Reduce `max_batch_size_mb` or `thread_count` to avoid overwhelming the RPC server/network.
- **Restarts**: Double-check that your chosen `start_height` matches the point you want to resume from; Protoblock will reprocess everything from that height forward.

With these practices, you can confidently run Protoblock in development, staging, or production environments while keeping control over throughput, durability, and observability.

