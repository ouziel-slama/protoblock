//! Lifecycle orchestration for `BlocksFetcher`.

use super::tip::TipTracker;
use crate::preprocessors::ordered_queue::OrderedBlockQueue;
use crate::preprocessors::worker::FetcherEventSender;
use crate::processor::tip::BlockchainTip;
use crate::rpc::AsyncRpcClient;
use crate::runtime::fatal::FatalErrorHandler;
use crate::runtime::protocol::BlockProtocol;
use crate::runtime::telemetry::{self, Telemetry};
use anyhow::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub(crate) struct LifecycleHandles {
    pub run_token: CancellationToken,
    fatal_handler: Arc<FatalErrorHandler>,
    metrics_handle: Option<JoinHandle<()>>,
    tip_handle: Option<JoinHandle<()>>,
}

pub(crate) struct LifecycleSpawnParams<'a, P: BlockProtocol> {
    pub shutdown_root: &'a CancellationToken,
    pub telemetry: Arc<Telemetry>,
    pub queue: Arc<OrderedBlockQueue<P::PreProcessed>>,
    pub metrics_interval: Duration,
    pub tip_refresh_interval: Duration,
    pub rpc_client: Arc<AsyncRpcClient>,
    pub blockchain_tip: Arc<BlockchainTip>,
    pub event_tx: FetcherEventSender,
}

impl LifecycleHandles {
    pub(crate) fn spawn<P: BlockProtocol>(params: LifecycleSpawnParams<'_, P>) -> Self {
        let LifecycleSpawnParams {
            shutdown_root,
            telemetry,
            queue,
            metrics_interval,
            tip_refresh_interval,
            rpc_client,
            blockchain_tip,
            event_tx,
        } = params;

        let run_token = shutdown_root.child_token();
        let fatal_handler = Arc::new(FatalErrorHandler::new(
            shutdown_root.clone(),
            run_token.clone(),
        ));
        let metrics_handle = telemetry::spawn_metrics_reporter(
            telemetry,
            queue,
            run_token.clone(),
            metrics_interval,
        );
        let tip_handle = TipTracker::spawn_refresh_loop(
            rpc_client,
            blockchain_tip,
            tip_refresh_interval,
            run_token.clone(),
            event_tx,
        );

        Self {
            run_token,
            fatal_handler,
            metrics_handle: Some(metrics_handle),
            tip_handle: Some(tip_handle),
        }
    }

    pub(crate) fn fatal_handler(&self) -> Arc<FatalErrorHandler> {
        self.fatal_handler.clone()
    }

    pub(crate) fn error(&self) -> Option<Error> {
        self.fatal_handler.error()
    }

    pub(crate) async fn shutdown(mut self) {
        if let Some(handle) = self.metrics_handle.take() {
            if let Err(err) = handle.await {
                tracing::warn!(error = %err, "metrics reporter task panicked");
            }
        }

        if let Some(handle) = self.tip_handle.take() {
            if let Err(err) = handle.await {
                tracing::warn!(error = %err, "blockchain tip refresher task panicked");
            }
        }
    }
}
