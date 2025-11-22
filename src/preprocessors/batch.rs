const MAX_BLOCKS_PER_BATCH: usize = 1_024;
const BYTES_PER_MB: usize = 1024 * 1024;

/// Adaptive batch sizing helper used by workers.
#[derive(Debug, Clone)]
pub struct BatchSizer {
    max_batch_size_mb: usize,
    current_batch_size: usize,
}

impl BatchSizer {
    pub fn new(max_batch_size_mb: usize) -> Self {
        Self {
            max_batch_size_mb: max_batch_size_mb.max(1),
            current_batch_size: 1,
        }
    }

    pub fn adjust(&mut self, total_bytes: usize) {
        let max_bytes = self.max_batch_size_mb.saturating_mul(BYTES_PER_MB);

        if total_bytes < max_bytes {
            self.current_batch_size = (self.current_batch_size + 1).min(MAX_BLOCKS_PER_BATCH);
        } else if total_bytes > max_bytes {
            self.current_batch_size = self.current_batch_size.saturating_sub(1).max(1);
        }
    }

    pub fn get_size(&self) -> usize {
        self.current_batch_size
    }

    pub fn shrink_on_failure(&mut self) {
        if self.current_batch_size > 1 {
            self.current_batch_size = (self.current_batch_size / 2).max(1);
        }
    }

    pub fn shrink_for_oversize(&mut self) {
        if self.current_batch_size > 1 {
            let quarter = self.current_batch_size / 4;
            self.current_batch_size = quarter.max(1);
        }
    }

    pub fn max_batch_size_mb(&self) -> usize {
        self.max_batch_size_mb
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MB: usize = BYTES_PER_MB;

    #[test]
    fn increases_when_under_limit() {
        let mut sizer = BatchSizer::new(3);
        sizer.adjust(2 * MB);
        assert_eq!(sizer.get_size(), 2);
    }

    #[test]
    fn decreases_when_over_limit() {
        let mut sizer = BatchSizer::new(3);
        sizer.adjust(2 * MB); // -> 2
        sizer.adjust(2 * MB); // -> 3
        sizer.adjust(4 * MB); // over limit, drop to 2
        assert_eq!(sizer.get_size(), 2);
    }

    #[test]
    fn never_drops_below_one() {
        let mut sizer = BatchSizer::new(1);
        sizer.adjust(4 * MB); // stays at 1
        assert_eq!(sizer.get_size(), 1);
    }

    #[test]
    fn oscillates_around_limit() {
        let mut sizer = BatchSizer::new(3);
        sizer.adjust(MB); // 2
        sizer.adjust(2 * MB); // 3
        sizer.adjust(3 * MB); // stays 3
        sizer.adjust(4 * MB); // 2
        sizer.adjust(2 * MB); // 3

        assert_eq!(sizer.get_size(), 3);
    }

    #[test]
    fn shrink_on_failure_halves_batch_size() {
        let mut sizer = BatchSizer::new(3);
        while sizer.get_size() <= 10 {
            sizer.adjust(MB / 2); // grow quickly
        }
        assert!(sizer.get_size() > 10);
        let previous = sizer.get_size();
        sizer.shrink_on_failure();
        assert!(sizer.get_size() <= previous / 2);
    }

    #[test]
    fn shrink_for_oversize_quarters_batch() {
        let mut sizer = BatchSizer::new(4);
        for _ in 0..16 {
            sizer.adjust(MB / 2);
        }
        let before = sizer.get_size();
        assert!(before > 4);
        sizer.shrink_for_oversize();
        assert!(sizer.get_size() <= before / 4);
        sizer.shrink_for_oversize();
        assert_eq!(sizer.get_size(), 1);
        sizer.shrink_for_oversize();
        assert_eq!(sizer.get_size(), 1);
    }

    #[test]
    fn reacts_to_sub_megabyte_overages() {
        let mut sizer = BatchSizer::new(1);
        sizer.adjust(MB / 2); // grow to 2
        assert_eq!(sizer.get_size(), 2);

        sizer.adjust(MB + MB / 2); // 1.5 MB should shrink back to 1
        assert_eq!(sizer.get_size(), 1);
    }

    #[test]
    fn handles_large_max_batch_size_without_overflow() {
        let mut sizer = BatchSizer::new(usize::MAX / MB + 1);
        sizer.adjust(usize::MAX - 1);
        assert_eq!(sizer.get_size(), 2);

        sizer.adjust(usize::MAX);
        assert_eq!(sizer.get_size(), 2);
    }
}
