use crate::runtime::protocol::ProtocolError;

/// Outcome of a protocol hook that can be interrupted by shutdown signals.
pub(crate) enum HookDecision<T> {
    Finished(Result<T, ProtocolError>),
    Cancelled,
}
