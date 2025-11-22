use std::sync::atomic::{AtomicU64, Ordering};

const UNINITIALIZED: u64 = u64::MAX;

/// Tracks the last fully confirmed block height in memory.
#[derive(Debug)]
pub struct ProgressTracker {
    last_confirmed: AtomicU64,
}

impl ProgressTracker {
    pub fn new(initial_height: u64) -> Self {
        Self {
            last_confirmed: AtomicU64::new(Self::initial_value(initial_height)),
        }
    }

    pub fn reset(&self, start_height: u64) {
        self.last_confirmed
            .store(Self::initial_value(start_height), Ordering::SeqCst);
    }

    pub fn mark_confirmed(&self, height: u64) {
        self.last_confirmed.store(height, Ordering::SeqCst);
    }

    pub fn last_confirmed(&self) -> Option<u64> {
        match self.last_confirmed.load(Ordering::SeqCst) {
            UNINITIALIZED => None,
            value => Some(value),
        }
    }

    fn initial_value(start_height: u64) -> u64 {
        if start_height == 0 {
            UNINITIALIZED
        } else {
            start_height.saturating_sub(1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initializes_with_previous_height_when_start_gt_zero() {
        let tracker = ProgressTracker::new(100);
        assert_eq!(tracker.last_confirmed(), Some(99));

        tracker.mark_confirmed(120);
        assert_eq!(tracker.last_confirmed(), Some(120));
    }

    #[test]
    fn reset_respects_new_start_height() {
        let tracker = ProgressTracker::new(5);
        tracker.mark_confirmed(10);
        tracker.reset(20);

        assert_eq!(tracker.last_confirmed(), Some(19));
        tracker.reset(0);
        assert_eq!(tracker.last_confirmed(), None);
    }

    #[test]
    fn zero_start_height_reports_none() {
        let tracker = ProgressTracker::new(0);
        assert_eq!(tracker.last_confirmed(), None);

        tracker.reset(0);
        assert_eq!(tracker.last_confirmed(), None);
    }
}
