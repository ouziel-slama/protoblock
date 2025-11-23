//! Runtime glue that wires configs, hooks, progress tracking, telemetry, and
//! runner orchestration.

pub mod config;
pub mod fatal;
pub mod hooks;
pub mod progress;
pub mod protocol;
pub mod runner;
pub mod telemetry;
