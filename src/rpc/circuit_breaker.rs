use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Represents the current state of the RPC circuit breaker.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

/// Snapshot of breaker internals for observability.
#[derive(Debug, Copy, Clone)]
pub struct CircuitBreakerSnapshot {
    pub state: CircuitState,
    pub consecutive_failures: usize,
    pub opened_at: Option<Instant>,
}

#[derive(Debug)]
struct BreakerState {
    state: CircuitState,
    consecutive_failures: usize,
    opened_at: Option<Instant>,
    half_open_in_flight: usize,
}

/// Error returned when the breaker refuses to allow an RPC attempt.
#[derive(Debug)]
pub enum CircuitBreakerError {
    CircuitOpen,
}

impl std::fmt::Display for CircuitBreakerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitBreakerError::CircuitOpen => write!(f, "RPC circuit breaker is open"),
        }
    }
}

impl std::error::Error for CircuitBreakerError {}

/// Simple circuit breaker with Closed/Open/Half-Open transitions.
#[derive(Debug, Clone)]
pub struct RpcCircuitBreaker {
    state: Arc<Mutex<BreakerState>>,
    failure_threshold: usize,
    cooldown: Duration,
    half_open_sample: usize,
}

impl Default for RpcCircuitBreaker {
    fn default() -> Self {
        Self::new(5, Duration::from_secs(30), 1)
    }
}

impl RpcCircuitBreaker {
    pub fn new(failure_threshold: usize, cooldown: Duration, half_open_sample: usize) -> Self {
        let threshold = failure_threshold.max(1);
        let cooldown = if cooldown.is_zero() {
            Duration::from_secs(1)
        } else {
            cooldown
        };
        let half_open_sample = half_open_sample.max(1);

        Self {
            state: Arc::new(Mutex::new(BreakerState {
                state: CircuitState::Closed,
                consecutive_failures: 0,
                opened_at: None,
                half_open_in_flight: 0,
            })),
            failure_threshold: threshold,
            cooldown,
            half_open_sample,
        }
    }

    /// Returns current breaker parameters.
    pub fn snapshot(&self) -> CircuitBreakerSnapshot {
        let guard = self.state.lock().expect("circuit breaker mutex poisoned");
        CircuitBreakerSnapshot {
            state: guard.state,
            consecutive_failures: guard.consecutive_failures,
            opened_at: guard.opened_at,
        }
    }

    /// Checks whether a new RPC attempt is allowed and reserves a Half-Open slot if needed.
    pub fn before_request(&self) -> Result<CircuitState, CircuitBreakerError> {
        let mut state = self.state.lock().expect("circuit breaker mutex poisoned");

        if state.state == CircuitState::Open {
            if let Some(opened_at) = state.opened_at {
                if opened_at.elapsed() >= self.cooldown {
                    self.transition(&mut state, CircuitState::HalfOpen);
                    state.half_open_in_flight = 0;
                } else {
                    return Err(CircuitBreakerError::CircuitOpen);
                }
            } else {
                return Err(CircuitBreakerError::CircuitOpen);
            }
        }

        if matches!(state.state, CircuitState::HalfOpen) {
            if state.half_open_in_flight >= self.half_open_sample {
                return Err(CircuitBreakerError::CircuitOpen);
            }
            state.half_open_in_flight += 1;
        }

        Ok(state.state)
    }

    /// Records a successful RPC call and resets counters when appropriate.
    pub fn record_success(&self) {
        let mut state = self.state.lock().expect("circuit breaker mutex poisoned");
        self.release_half_open_slot(&mut state);
        state.consecutive_failures = 0;

        if matches!(state.state, CircuitState::HalfOpen) {
            state.opened_at = None;
            self.transition(&mut state, CircuitState::Closed);
        }
    }

    /// Records a failed RPC call, potentially opening the circuit.
    pub fn record_failure(&self) {
        let mut state = self.state.lock().expect("circuit breaker mutex poisoned");
        self.release_half_open_slot(&mut state);

        state.consecutive_failures = state.consecutive_failures.saturating_add(1);

        if matches!(state.state, CircuitState::HalfOpen) {
            state.opened_at = Some(Instant::now());
            state.half_open_in_flight = 0;
            self.transition(&mut state, CircuitState::Open);
            return;
        }

        if state.consecutive_failures >= self.failure_threshold
            && !matches!(state.state, CircuitState::Open)
        {
            state.opened_at = Some(Instant::now());
            state.half_open_in_flight = 0;
            self.transition(&mut state, CircuitState::Open);
        }
    }

    fn release_half_open_slot(&self, state: &mut BreakerState) {
        if matches!(state.state, CircuitState::HalfOpen) && state.half_open_in_flight > 0 {
            state.half_open_in_flight -= 1;
        }
    }

    fn transition(&self, state: &mut BreakerState, next: CircuitState) {
        if state.state != next {
            tracing::warn!(
                previous = ?state.state,
                next = ?next,
                consecutive_failures = state.consecutive_failures,
                "rpc circuit breaker state changed"
            );
            state.state = next;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn opens_after_threshold_and_recovers() {
        let breaker = RpcCircuitBreaker::new(2, Duration::from_millis(5), 1);

        breaker.before_request().unwrap();
        breaker.record_failure();
        assert_eq!(breaker.snapshot().state, CircuitState::Closed);

        breaker.before_request().unwrap();
        breaker.record_failure();
        assert_eq!(breaker.snapshot().state, CircuitState::Open);

        assert!(breaker.before_request().is_err());

        thread::sleep(Duration::from_millis(6));
        assert_eq!(breaker.before_request().unwrap(), CircuitState::HalfOpen);

        breaker.record_success();
        assert_eq!(breaker.snapshot().state, CircuitState::Closed);
    }

    #[test]
    fn half_open_failure_reopens() {
        let breaker = RpcCircuitBreaker::new(1, Duration::from_millis(5), 1);

        breaker.before_request().unwrap();
        breaker.record_failure();
        assert_eq!(breaker.snapshot().state, CircuitState::Open);

        thread::sleep(Duration::from_millis(6));
        breaker.before_request().unwrap();
        breaker.record_failure();
        assert_eq!(breaker.snapshot().state, CircuitState::Open);
    }

    #[test]
    fn enforces_half_open_sample_limit() {
        let breaker = RpcCircuitBreaker::new(1, Duration::from_millis(5), 1);

        breaker.before_request().unwrap();
        breaker.record_failure();

        thread::sleep(Duration::from_millis(6));
        breaker.before_request().unwrap();
        assert!(breaker.before_request().is_err());
        breaker.record_success();
        thread::sleep(Duration::from_millis(1));
        breaker.before_request().unwrap();
    }
}
