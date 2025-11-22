use crate::preprocessors::sized_queue::QueueByteSize;
use anyhow::Error as AnyError;
use bitcoin::Block;
use core::future::Future;
use core::pin::Pin;

pub type ProtocolFuture<'a> = Pin<Box<dyn Future<Output = Result<(), ProtocolError>> + Send + 'a>>;
pub type ProtocolPreProcessFuture<T> =
    Pin<Box<dyn Future<Output = Result<T, ProtocolError>> + Send + 'static>>;

/// Enumerates the execution stages of the [`BlockProtocol`] hooks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolStage {
    PreProcess,
    Process,
    Rollback,
    Shutdown,
}

/// Error surfaced by protocol hooks. Every instance is considered fatal.
#[derive(Debug)]
pub struct ProtocolError {
    stage: ProtocolStage,
    source: AnyError,
}

impl ProtocolError {
    pub fn new(stage: ProtocolStage, source: AnyError) -> Self {
        Self { stage, source }
    }

    pub fn stage(&self) -> ProtocolStage {
        self.stage
    }

    pub fn into_source(self) -> AnyError {
        self.source
    }
}

impl core::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{:?} protocol error: {}", self.stage, self.source)
    }
}

impl std::error::Error for ProtocolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// Trait implemented by downstream consumers of fetched Bitcoin blocks.
pub trait BlockProtocol: Send + Sync + 'static {
    type PreProcessed: QueueByteSize + Send + 'static;

    /// Executed by worker tasks. Always async so it can perform I/O such as DB access.
    fn pre_process(
        &self,
        block: Block,
        height: u64,
    ) -> ProtocolPreProcessFuture<Self::PreProcessed>;

    /// Executed sequentially by the main task to apply the pre-processed data.
    fn process<'a>(&'a mut self, data: Self::PreProcessed, height: u64) -> ProtocolFuture<'a>;

    /// Invoked to rollback state during a chain reorganization.
    fn rollback<'a>(&'a mut self, block_height: u64) -> ProtocolFuture<'a>;

    /// Called once during shutdown to allow graceful cleanup (flush buffers, close DBs, etc.).
    fn shutdown<'a>(&'a mut self) -> ProtocolFuture<'a>;
}
