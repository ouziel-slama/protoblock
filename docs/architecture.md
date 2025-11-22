# Protoblock Architecture

## Goals

Protoblock is designed as a reusable ingestion engine that can saturate a Bitcoin Core JSON-RPC node while maintaining strict block ordering, safe rollback on chain reorganizations, and observable runtime behavior. The crate offers:

- **Pipeline separation** between concurrent data acquisition (`pre_process`) and sequential mutation (`process`).
- **Deterministic ordering** so downstream consumers can reason about committed heights.
- **Backpressure and fault tolerance** via adaptive batch sizing, RPC circuit breaking, and cancellation tokens.
- **Graceful lifecycle management** exposed through a `Runner` type that integrates with host applications.

## Data Flow Overview

```
             ┌────────────┐
     config  │            │  protocol impl
────────────▶│   Runner   │◀────────────────────┐
             │            │                     │
             └──────┬─────┘                     │
                    │ start() / stop()          │
             ┌──────▼─────┐                     │
             │ Blocks     │     telemetry        │
             │ Fetcher    │──────────────────────┘
             └──────┬─────┘
                    │
          ┌─────────▼─────────┐   batch RPC, pre_process
          │ Worker pool (N)   │──────────────────────────┐
          └─────────┬─────────┘                          │
                    │ PreProcessedBlock<T>               │
             ┌──────▼─────┐                              │
             │ Ordered    │ queue.pop_next()             │
             │ BlockQueue │                              │
             └──────┬─────┘                              │
                    │                                    │
             ┌──────▼─────┐   process / rollback / flush ▼
             │ Protocol   │──────────────────────────────┘
```

## Key Components

### Runner

- Owns the root `CancellationToken`, wires Ctrl-C handling (`run_until_ctrl_c`), and calls into `BlocksFetcher`.
- Allows embedding applications to compose their own shutdown strategies by cloning `Runner::cancellation_token()`.

### BlocksFetcher

- Builds shared infrastructure: `OrderedBlockQueue`, `runtime::telemetry::Telemetry`, `runtime::progress::ProgressTracker`, `RpcCircuitBreaker`, and worker channels. Construction now happens exactly once; per-run state lives in the lifecycle module.
- Delegates concurrency to `worker_pool::WorkerPool`, tip tracking to `tip::TipTracker`, and reorg recovery to `processor::reorg::ReorgManager`. `BlocksFetcher` simply wires the pieces together and forwards signals between them.
- Starts the sequential processing loop and records run-scoped handles (processing, metrics, tip-refresher) inside `lifecycle::LifecycleHandles`.
- Exposes `start_from`, `stop`, and `last_confirmed_height` (returns `Option<u64>`; `None` means no block has finished processing yet) to external callers.

### Worker Tasks

- Each worker owns an `AsyncRpcClient` configured from `FetcherConfig` (including timeouts and credentials) and a `BatchSizer`.
- Workers are started with distinct base heights: worker *i* begins at `start_height + i`. Within a batch they call `Worker::calculate_heights`, which steps forward by the currently active worker count (normally `thread_count`, but it can drop to 1 when the pipeline is idle). With 5 active workers the first worker touches heights `0,5,10,…`, the second `1,6,11,…`, etc.
- After each batch completes, the worker advances its base height by `active_workers * batch_size`, ensuring the next batch continues the same interleaved sequence without overlapping other workers. When only a single worker is running the stride collapses to sequential heights.
- Every iteration clamps the candidate heights to the shared `BlockchainTip`, which is refreshed out-of-band. If the tip is unknown or stale, workers optimistically probe future heights and rely on `height out of range` RPC errors to clamp themselves (they back off for 10 seconds before retrying) while the refresher catches up.
- Workers now run in a double-buffered loop: as soon as a batch is fetched they spawn the next `batch_get_blocks` future and let it execute while `pre_process` runs. Prefetched batches are dropped when a generation change marks the current batch as stale, and the in-flight RPC is aborted if shutdown/stop is requested so the worker cannot hang while exiting.
- For every batch:
  1. Fetch block hashes (`getblockhash`) and raw blocks (`getblock`) via JSON-RPC batches.
  2. Transform them into `bitcoin::Block`, create the asynchronous `pre_process` future, release the read lock immediately, and capture the returned data inside `PreProcessedBlock`.
  3. Measure each block’s `queue_bytes()` (powered by the `QueueByteSize` trait), feed that into `BatchSizer`, telemetry, and logging, then push the item into the shared `OrderedBlockQueue`.
- Handles transient failures with exponential backoff (`retry_delay`), consults the `RpcCircuitBreaker`, and honours shutdown signals mid-batch.

### OrderedBlockQueue

- Backed by a mutex-protected `HashMap<u64, PreProcessedBlock<T>>` and a `tokio::sync::Notify`.
- Enforces a configurable `queue_max_size_mb` so workers exert real backpressure: once the byte budget is exhausted, `push` waits for `process` to catch up instead of letting memory grow without bound.
- Computes that budget using the `QueueByteSize` trait via `PreProcessedBlock::queue_bytes()`, which counts both block metadata and the bytes returned by `pre_process` so telemetry reflects reality.
- Only releases the `next_expected` height, blocking waiters until the gap is filled. Workers can therefore fetch out-of-order without risking protocol invariants.
- Supports `reset_expected` so reorg handling can drop future blocks and resume from an earlier height.

### Processing Loop

- Runs inside `BlocksFetcher::spawn_processing_loop`.
- Pops the next block from the queue, checks for reorgs, and calls `BlockProtocol::process` sequentially (holding a write lock on the protocol implementation).
- Uses a `watch::Receiver<bool>` to detect when workers have stopped; once shutdown is requested, it switches into cancellation mode and exits quickly while discarding any remaining queue items (fast shutdown is preferred over draining).

### Tip Tracking & Idle Mode

- `tip::TipTracker` owns the background refresh loop (calling `getblockcount` every `FetcherConfig::tip_refresh_interval`) and keeps `BlockchainTip` up to date for both workers and the processing loop.
- During startup `TipTracker::maybe_adjust_workers_for_tip` compares `start_height` with the freshest tip. If fewer than `3 * thread_count` blocks are outstanding (or the tip hasn’t advanced beyond `start_height` yet), it immediately enters tip-wait mode and only worker `0` is started; otherwise it boots the entire pool.
- When the processing loop finishes the block whose height equals the latest tip and the queue is empty—even if this happens immediately after startup—the tracker instructs all fetch workers to stop, waits for their in-flight batches to drain via the worker pool, shrinks the worker stride to a single slot, and restarts only worker `0`. That lone worker polls for new blocks at tip without wasting bandwidth. The worker backoff (`tip_idle_backoff`) and tip refresh cadence (`tip_refresh_interval`) are independent knobs so operators can tune RPC usage for each phase separately.
- Once this one-worker mode is engaged it remains active until the fetcher is restarted. New blocks are still ingested (the solo worker keeps polling), but the rest of the pool stays idle; this matches the operational expectation that steady-state tip tracking does not require parallelism. Because we target Bitcoin, where block production averages one block every ~10 minutes and there are no sudden surges of dozens of blocks at once, relying on a single worker near tip is safe. Reorg recovery respects the current mode: if the fetcher is already idling it restarts just the single worker, otherwise it restarts the full pool.

### Reorg Handling

- `processor::reorg::ReorgManager` owns the `ReorgWindow`, RPC lookups, and rollback wiring. The processing loop now forwards `FetcherEvent`s and `PreProcessedBlock`s to the manager, which decides whether to continue, rewind, or refresh the tip.
- On startup, Protoblock fetches the `reorg_window_size` hashes preceding `start_height` in a single batched RPC call so reorgs can be detected immediately, even on the very first block processed.
- When a new block’s `previous_hash` is unknown inside the window, the manager triggers a rollback:
  1. Compute the fork height (the highest height that must remain) and invoke `protocol.rollback` once with that value so the protocol can purge everything above it.
  2. Reset the queue’s expected height.
  3. Ask the worker pool to restart the appropriate number of workers (respecting tip-wait mode).
- This design avoids double-processing blocks and ensures downstream state mirrors the canonical chain. All retries (`find_canonical_fork_height_with_retry`) now live in the module, keeping `BlocksFetcher` free of low-level RPC details.

### Lifecycle & Telemetry

- `lifecycle::LifecycleHandles` centralizes per-run resources such as the derived cancellation token, fatal handler, metrics reporter, and tip refresh loop. `BlocksFetcher` stores the handles and simply asks them to spawn/shutdown when `start_from`/`stop` run.
- The lifecycle module produces small, testable units: a constructor (`spawn`) that wires child tokens + tasks, `fatal_handler()` accessor for reuse elsewhere, and `shutdown` to await background work at the end of a run.

### Progress Tracking & Fatal Errors

- `runtime::progress::ProgressTracker` stores the last confirmed height in-memory and initializes to `start_height - 1` (or `None` at genesis) so observers immediately know which heights are already durable after a restart. Protoblock still relies on the operator to supply the desired `start_height`; no automatic checkpoint reload exists.
- Any fatal `ProtocolError` (from `pre_process`, `process`, `rollback`, or `shutdown`) cancels the root token **and** propagates up through `BlocksFetcher::stop` / `Runner::run_until_ctrl_c`, giving host applications an explicit error instead of forcing them to scrape logs via the shared `runtime::fatal::FatalErrorHandler`. Persistence (if desired) must be implemented by the host application.

### RPC Client & Circuit Breaker

- `AsyncRpcClient` wraps `jsonrpsee` and exposes `batch_get_blocks`. It automatically retries failed batches, classifies timeouts, and records metrics for observability.
- `RpcCircuitBreaker` transitions between Closed → Open → Half-Open when too many consecutive failures occur. Workers consult it before every RPC attempt and back off while the breaker is Open.
- The `src/rpc/` module shards responsibilities across `client`, `options`, `metrics`, `retry`, `auth`, and `helpers` submodules while re-exporting the legacy `crate::rpc::*` API. The `BlockBatchClient` trait now lives alongside the client so alternative transports can plug in without depending on worker internals.

### Telemetry & Tracing

- `runtime::telemetry::Telemetry` keeps rolling counters for processed blocks, RPC errors, and timeouts. The metrics reporter task logs throughput, queue blocks/bytes, and error counters on the interval configured via `FetcherConfig::metrics_interval` (defaults to `DEFAULT_METRICS_INTERVAL`). Queue bytes now correspond to the true size of the pre-processed payloads retained in memory.
- `runtime::telemetry::init_tracing` installs a global tracing subscriber honoring `RUST_LOG`, making it easy to integrate with existing logging setups.

## Concurrency & Cancellation Model

- Every subsystem receives a `CancellationToken`. Workers stop fetching new batches when cancellation is requested but finish the in-flight batch to keep state consistent.
- The processing loop listens on the same token; once it flips, the loop acknowledges cancellation and terminates, dropping any queued-but-not-yet-processed blocks so shutdown can complete promptly.
- The root token back-propagates to user code via `Runner::cancellation_token`, enabling unified orchestration (e.g., hooking into a service manager or Kubernetes lifecycle hooks).

## Error Handling Strategy

1. **RPC failures** trigger retries with exponential backoff. Persistent failures eventually trip the circuit breaker.
2. **Protocol hook errors** are treated as fatal: the `runtime::fatal::FatalErrorHandler` cancels the pipeline and surfaces the error so the operator can restart from a deliberate `start_height`.
3. **Channel closures / worker panics** are surfaced to the logs and cause the fetcher to stop cleanly, ensuring `protocol.shutdown` still runs.
4. **Validation issues** (bad config, malformed URLs, zero values) are caught up-front by `FetcherConfig::builder` / `FetcherConfig::new` and propagate as `anyhow::Error` results.

## Extensibility Notes

- You can instrument additional metrics by wrapping the `runtime::telemetry::Telemetry` struct or subscribing to tracing spans emitted by workers (`tracing::instrument`).
- Custom RPC strategies (e.g., alternative transports) can implement the `BlockBatchClient` trait used by workers.
- External controllers can pause/resume ingestion by sending `WorkerControl::Stop`/`Start` through the exposed channels if the API is extended to surface them.

This document should give you enough context to reason about performance characteristics, add new features (such as host-implemented persistence backends), or debug production incidents with confidence. Refer to `docs/usage.md` for hands-on guidance on configuring and embedding Protoblock in your application.

