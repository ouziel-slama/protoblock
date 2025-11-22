//! Worker module split across focused submodules:
//! - `types`: shared enums/aliases for worker control & batching
//! - `shared`: state shared across workers (queue, telemetry, activity)
//! - `process`: worker struct plus run/process logic
//! - `batch`: helper impls for planning/fetching batches
//! - `tests`: worker integration/unit tests

mod batch;
mod process;
mod shared;
mod types;

#[cfg(test)]
mod tests;

pub use process::Worker;
pub use shared::{WorkerActivityTracker, WorkerShared, WorkerSharedParams};
pub use types::{
    worker_control_channel, FetcherEvent, FetcherEventSender, WorkerControl, WorkerControlReceiver,
    WorkerControlSender,
};
