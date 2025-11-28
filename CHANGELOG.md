# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2025-11-28

### Fixed
- fix cargo.toml

## [0.1.1] - 2025-11-28

### Fixed
- docs.rs build now succeeds by dropping the unstable `doc_auto_cfg` gate from `src/lib.rs`

### Changed
- Oversized batch and RPC retry logs are downgraded from `warn` to `info` to reduce noise during normal resizing
- Ordered queue gap test hooks carry a queue identifier so pause probes only affect the intended queue under test

## [0.1.0] - 2025-11-23

### Added

#### Core Features
- **Asynchronous Bitcoin block ingestion pipeline** with multiplexed JSON-RPC batch calls across a worker pool
- **BlockProtocol trait** for user-defined block processing with `pre_process`, `process`, `rollback`, and `shutdown` hooks
- **Runner** for lifecycle management with graceful shutdown and signal handling (Ctrl-C support)
- **BlocksFetcher** orchestrating the worker pool, ordered queue, and main processing loop

#### Worker Pool & Concurrency
- **Multi-threaded worker pool** for concurrent block fetching with configurable `thread_count`
- **Double-buffer prefetching** in workers to minimize RPC idle time by launching the next batch request while preprocessing
- **Automatic batch sizing** that grows/shrinks based on observed megabytes per batch
- **Tip wait mode** that reduces worker pool to a single task when catching up to Bitcoin tip

#### Queue & Backpressure
- **OrderedBlockQueue** guaranteeing strictly increasing block heights despite out-of-order worker fetches
- **Queue byte budget** with configurable `queue_max_size_mb` to enforce backpressure and prevent unbounded memory growth
- **QueueByteSize trait** for tracking post-`pre_process` memory footprint

#### Reorg Handling
- **ReorgWindow** tracking recent block hashes to detect chain reorganizations
- **Automatic rollback** via `BlockProtocol::rollback` when fork is detected
- **Configurable reorg window size** to control maximum tolerated reorg depth

#### RPC & Networking
- **RpcCircuitBreaker** to avoid hammering failing Bitcoin nodes with automatic recovery probing
- **Configurable RPC timeout** with retry instrumentation
- **HTTP client** with Basic authentication support for Bitcoin Core RPC
- **Batch request/response size limits** (`rpc_max_request_body_bytes`, `rpc_max_response_body_bytes`)

#### Observability & Telemetry
- **Built-in tracing** with `init_tracing()` helper for structured logging
- **Metrics reporter** exposing throughput, queue blocks/bytes, and RPC error rates
- **Configurable metrics interval** for telemetry snapshot logging
- **ProgressTracker** reporting pipeline progress and durable heights

#### Configuration
- **FetcherConfig builder** with validation for all runtime parameters
- **Comprehensive configuration options** including:
  - RPC connection settings (URL, user, password)
  - Worker pool size and batch sizing
  - Queue limits and backpressure thresholds
  - Reorg window size
  - Timeout and retry settings
  - Tip polling and refresh intervals

#### Error Handling
- **FatalErrorHandler** for stopping pipeline on unrecoverable protocol errors
- **Graceful error propagation** from workers through `BlocksFetcher` to `Runner`
- **Fast shutdown semantics** with immediate worker termination on cancellation

#### Examples & Documentation
- **`rpc_speed.rs` example** for measuring raw JSON-RPC throughput with live progress bar
- **Quick start example** demonstrating `LoggingProtocol` implementation
- **Comprehensive README** with architecture overview, configuration guide, and usage examples
- **`docs/architecture.md`** detailing internal pipeline and concurrency model
- **`docs/usage.md`** covering configuration, protocol implementation, and operational practices

#### Testing
- **Unit tests** for core components
- **Mock integration tests** with simulated RPC client
- **Regtest suite** for end-to-end testing against real Bitcoin Core (opt-in via `PROTOBLOCK_RUN_REGTESTS=1`)
- **CI/CD workflows** for tests, coverage, formatting, clippy, and publishing

### Technical Details
- Rust 1.79+ required
- Built on Tokio async runtime
- Uses `jsonrpsee` for RPC client
- Bitcoin block parsing via `bitcoin` crate
- Dual-licensed under MIT OR Apache-2.0
 
[0.1.1]: https://github.com/ouziel-slama/protoblock/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/ouziel-slama/protoblock/releases/tag/v0.1.0

