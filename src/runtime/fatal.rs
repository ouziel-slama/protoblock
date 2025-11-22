use crate::runtime::protocol::ProtocolError;
use anyhow::Error as AnyError;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct FatalErrorHandler {
    inner: Arc<FatalInner>,
}

struct FatalInner {
    triggered: AtomicBool,
    root_shutdown: CancellationToken,
    run_shutdown: CancellationToken,
    captured_error: Mutex<Option<CapturedFatalError>>,
    notify: Notify,
}

#[derive(Clone)]
struct CapturedFatalError {
    inner: Arc<AnyError>,
}

impl CapturedFatalError {
    fn new(inner: AnyError) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl fmt::Debug for CapturedFatalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("CapturedFatalError")
            .field(&self.inner)
            .finish()
    }
}

impl fmt::Display for CapturedFatalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.inner.as_ref(), f)
    }
}

impl std::error::Error for CapturedFatalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.inner.as_ref().as_ref())
    }
}

impl FatalErrorHandler {
    pub fn new(root_shutdown: CancellationToken, run_shutdown: CancellationToken) -> Self {
        Self {
            inner: Arc::new(FatalInner {
                triggered: AtomicBool::new(false),
                root_shutdown,
                run_shutdown,
                captured_error: Mutex::new(None),
                notify: Notify::new(),
            }),
        }
    }

    pub fn trigger(&self, error: ProtocolError) -> AnyError {
        let stage = error.stage();

        if self.inner.triggered.swap(true, Ordering::SeqCst) {
            return error.into();
        }

        tracing::error!(
            stage = ?stage,
            error = %error,
            "fatal protocol error; initiating shutdown"
        );

        self.capture_error(CapturedFatalError::new(error.into()))
    }

    pub fn trigger_external(&self, context: &str, error: AnyError) -> AnyError {
        if self.inner.triggered.swap(true, Ordering::SeqCst) {
            return error;
        }

        tracing::error!(
            context,
            error = %error,
            "fatal pipeline error; initiating shutdown"
        );

        self.capture_error(CapturedFatalError::new(error))
    }

    fn capture_error(&self, error: CapturedFatalError) -> AnyError {
        {
            let mut slot = self.inner.captured_error.lock().unwrap();
            if slot.is_none() {
                *slot = Some(error.clone());
            }
        }

        self.inner.run_shutdown.cancel();
        self.inner.root_shutdown.cancel();
        self.inner.notify.notify_waiters();

        error.into()
    }

    pub fn error(&self) -> Option<AnyError> {
        self.inner
            .captured_error
            .lock()
            .unwrap()
            .as_ref()
            .map(|error| error.clone().into())
    }
}
