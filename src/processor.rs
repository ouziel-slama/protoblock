//! Processor orchestration covering fetch loops, lifecycle management, reorg
//! handling, and worker pool coordination.

pub mod backoff;
pub mod fetcher;
pub mod lifecycle;
pub mod reorg;
pub mod tip;
pub mod worker_pool;
