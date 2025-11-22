use std::collections::VecDeque;
use std::sync::Mutex;

const DEFAULT_SAMPLE_WINDOW: usize = 256;
const DEFAULT_REQUEST_PER_BLOCK_BYTES: usize = 256;
const DEFAULT_RESPONSE_PER_BLOCK_BYTES: usize = 4 * 1024 * 1024;
const SAFETY_NUMERATOR: usize = 9;
const SAFETY_DENOMINATOR: usize = 10;
const REQUEST_BATCH_OVERHEAD: usize = 128;
const RESPONSE_BATCH_OVERHEAD: usize = 256;
const REQUEST_ENTRY_OVERHEAD: usize = 96;
const RESPONSE_ENTRY_OVERHEAD: usize = 96;
const BLOCK_HASH_HEX_CHARS: usize = 64;
const QUOTE_BYTES: usize = 2;
const OVERSIZE_HINT_WEIGHT: usize = 16;

/// Upper bounds applied to RPC payload planning logic.
#[derive(Clone, Copy, Debug)]
pub struct RpcPayloadLimits {
    max_request_body_bytes: usize,
    max_response_body_bytes: usize,
}

impl RpcPayloadLimits {
    pub fn new(max_request_body_bytes: usize, max_response_body_bytes: usize) -> Self {
        Self {
            max_request_body_bytes: max_request_body_bytes.max(1),
            max_response_body_bytes: max_response_body_bytes.max(1),
        }
    }

    pub fn request_budget(&self) -> usize {
        apply_margin(self.max_request_body_bytes)
    }

    pub fn response_budget(&self) -> usize {
        apply_margin(self.max_response_body_bytes)
    }

    pub fn max_request_body_bytes(&self) -> usize {
        self.max_request_body_bytes
    }

    pub fn max_response_body_bytes(&self) -> usize {
        self.max_response_body_bytes
    }
}

/// Rolling statistics derived from recent payload measurements.
pub struct RpcPayloadStats {
    inner: Mutex<PayloadStatsInner>,
    fallback_request_per_block: usize,
    fallback_response_per_block: usize,
}

impl RpcPayloadStats {
    pub fn new(
        max_samples: usize,
        fallback_request_per_block: usize,
        fallback_response_per_block: usize,
    ) -> Self {
        Self {
            inner: Mutex::new(PayloadStatsInner::new(max_samples.max(1))),
            fallback_request_per_block: fallback_request_per_block.max(1),
            fallback_response_per_block: fallback_response_per_block.max(1),
        }
    }

    pub fn record(&self, sample: RpcPayloadSample) {
        if sample.block_count == 0 {
            return;
        }

        let per_block_request = div_rounding_up(sample.request_bytes, sample.block_count).max(1);
        let per_block_response = div_rounding_up(sample.response_bytes, sample.block_count).max(1);

        let mut inner = self.inner.lock().expect("payload stats mutex poisoned");
        inner.push(per_block_request, per_block_response);
    }

    pub fn record_oversized_hint(&self, block_count: usize, limits: &RpcPayloadLimits) {
        if block_count == 0 {
            return;
        }

        let safe_blocks = block_count.saturating_sub(1).max(1);
        let per_block_request = div_rounding_up(limits.request_budget(), safe_blocks).max(1);
        let per_block_response = div_rounding_up(limits.response_budget(), safe_blocks).max(1);

        let mut inner = self.inner.lock().expect("payload stats mutex poisoned");
        let repeats = OVERSIZE_HINT_WEIGHT.min(inner.max_samples);
        for _ in 0..repeats {
            inner.push(per_block_request, per_block_response);
        }
    }

    pub fn estimate(&self) -> PayloadEstimate {
        let inner = self.inner.lock().expect("payload stats mutex poisoned");
        let request = inner
            .percentile(&inner.per_block_request, 0.95)
            .unwrap_or(self.fallback_request_per_block);
        let response = inner
            .percentile(&inner.per_block_response, 0.95)
            .unwrap_or(self.fallback_response_per_block);

        PayloadEstimate {
            request_per_block: request.max(1),
            response_per_block: response.max(1),
        }
    }
}

impl Default for RpcPayloadStats {
    fn default() -> Self {
        Self::new(
            DEFAULT_SAMPLE_WINDOW,
            DEFAULT_REQUEST_PER_BLOCK_BYTES,
            DEFAULT_RESPONSE_PER_BLOCK_BYTES,
        )
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PayloadEstimate {
    pub request_per_block: usize,
    pub response_per_block: usize,
}

impl PayloadEstimate {
    pub fn clamp(&self, desired_blocks: usize, limits: &RpcPayloadLimits) -> usize {
        if desired_blocks == 0 {
            return 0;
        }

        let request_budget = limits.request_budget();
        let response_budget = limits.response_budget();

        let request_capacity = request_budget
            .checked_div(self.request_per_block.max(1))
            .unwrap_or(0);
        let response_capacity = response_budget
            .checked_div(self.response_per_block.max(1))
            .unwrap_or(0);

        desired_blocks
            .min(request_capacity.max(1))
            .min(response_capacity.max(1))
            .max(1)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RpcPayloadSample {
    pub start_height: u64,
    pub end_height: u64,
    pub block_count: usize,
    pub request_bytes: usize,
    pub response_bytes: usize,
}

impl RpcPayloadSample {
    pub fn from_blocks(blocks: &[(u64, String)]) -> Option<Self> {
        if blocks.is_empty() {
            return None;
        }

        let block_count = blocks.len();
        let start_height = blocks.first().map(|(h, _)| *h).unwrap_or_default();
        let end_height = blocks.last().map(|(h, _)| *h).unwrap_or(start_height);
        let response_bytes =
            measure_response_body_bytes(blocks.iter().map(|(_, hex)| hex.as_str()));
        let request_bytes = estimate_request_body_bytes(block_count);

        Some(Self {
            start_height,
            end_height,
            block_count,
            request_bytes,
            response_bytes,
        })
    }
}

/// Conservatively estimates the HTTP body size for a batch request.
pub fn estimate_request_body_bytes(block_count: usize) -> usize {
    if block_count == 0 {
        return 0;
    }

    let per_entry = REQUEST_ENTRY_OVERHEAD.saturating_add(BLOCK_HASH_HEX_CHARS + QUOTE_BYTES);
    REQUEST_BATCH_OVERHEAD.saturating_add(block_count.saturating_mul(per_entry))
}

/// Measures the HTTP body size for a JSON-RPC batch response given the returned hex strings.
pub fn measure_response_body_bytes<'a, I>(blocks: I) -> usize
where
    I: Iterator<Item = &'a str>,
{
    let mut total = RESPONSE_BATCH_OVERHEAD;
    for hex in blocks {
        total = total
            .saturating_add(RESPONSE_ENTRY_OVERHEAD)
            .saturating_add(hex.len());
    }
    total
}

/// Estimates the HTTP body size for a single JSON-RPC `getblock` response given the length
/// of the encoded hex payload.
pub const fn single_block_response_body_bytes(hex_len: usize) -> usize {
    RESPONSE_BATCH_OVERHEAD
        .saturating_add(RESPONSE_ENTRY_OVERHEAD)
        .saturating_add(hex_len)
}

struct PayloadStatsInner {
    per_block_request: VecDeque<usize>,
    per_block_response: VecDeque<usize>,
    max_samples: usize,
}

impl PayloadStatsInner {
    fn new(max_samples: usize) -> Self {
        Self {
            per_block_request: VecDeque::with_capacity(max_samples),
            per_block_response: VecDeque::with_capacity(max_samples),
            max_samples,
        }
    }

    fn push(&mut self, request: usize, response: usize) {
        if self.per_block_request.len() >= self.max_samples {
            self.per_block_request.pop_front();
        }
        if self.per_block_response.len() >= self.max_samples {
            self.per_block_response.pop_front();
        }

        self.per_block_request.push_back(request.max(1));
        self.per_block_response.push_back(response.max(1));
    }

    fn percentile(&self, values: &VecDeque<usize>, percentile: f64) -> Option<usize> {
        if values.is_empty() {
            return None;
        }
        let mut sorted: Vec<usize> = values.iter().copied().collect();
        sorted.sort_unstable();
        let clamped = percentile.clamp(0.0, 1.0);
        let idx = (clamped * (sorted.len() - 1) as f64).round() as usize;
        sorted.get(idx).copied()
    }
}

fn div_rounding_up(total: usize, divisor: usize) -> usize {
    if divisor == 0 {
        return 0;
    }
    let remainder = total % divisor;
    if remainder == 0 {
        total / divisor
    } else {
        total / divisor + 1
    }
}

fn apply_margin(value: usize) -> usize {
    let reduced = value
        .saturating_mul(SAFETY_NUMERATOR)
        .checked_div(SAFETY_DENOMINATOR.max(1))
        .unwrap_or(value);
    reduced.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_estimates_payloads() {
        let stats = RpcPayloadStats::new(4, 10, 100);
        let sample = RpcPayloadSample {
            start_height: 10,
            end_height: 12,
            block_count: 3,
            request_bytes: 300,
            response_bytes: 3_000,
        };
        stats.record(sample);

        let estimate = stats.estimate();
        assert_eq!(estimate.request_per_block, 100);
        assert_eq!(estimate.response_per_block, 1_000);
    }

    #[test]
    fn clamp_respects_limits() {
        let limits = RpcPayloadLimits::new(1_000, 2_000);
        let estimate = PayloadEstimate {
            request_per_block: 400,
            response_per_block: 700,
        };
        let clamped = estimate.clamp(10, &limits);
        assert!(clamped <= 2, "expected clamp to reduce batch size");
    }

    #[test]
    fn request_estimation_accounts_for_entries() {
        let bytes = estimate_request_body_bytes(2);
        let per_entry = REQUEST_ENTRY_OVERHEAD + BLOCK_HASH_HEX_CHARS + QUOTE_BYTES;
        assert_eq!(bytes, REQUEST_BATCH_OVERHEAD + per_entry * 2);
    }

    #[test]
    fn response_measurement_counts_hex_length() {
        let bytes = measure_response_body_bytes(["abc", "defgh"].iter().copied());
        assert_eq!(
            bytes,
            RESPONSE_BATCH_OVERHEAD + RESPONSE_ENTRY_OVERHEAD * 2 + 3 + 5
        );
    }

    #[test]
    fn single_block_response_estimate_matches_measurement() {
        let hex_len = 512;
        let hex = "0".repeat(hex_len);
        let measured = measure_response_body_bytes(std::iter::once(hex.as_str()));
        let estimated = single_block_response_body_bytes(hex_len);
        assert_eq!(estimated, measured);
    }

    #[test]
    fn oversized_hint_reduces_capacity() {
        let limits = RpcPayloadLimits::new(10_000, 10_000);
        let stats = RpcPayloadStats::new(32, 64, 64);

        let sample = RpcPayloadSample {
            start_height: 0,
            end_height: 0,
            block_count: 4,
            request_bytes: 256,
            response_bytes: 256,
        };

        for _ in 0..32 {
            stats.record(sample);
        }

        let before = stats.estimate();
        assert!(before.response_per_block <= 64);

        stats.record_oversized_hint(50, &limits);
        let after = stats.estimate();
        let min_expected = div_rounding_up(limits.response_budget(), 49);
        assert!(
            after.response_per_block >= min_expected,
            "oversize hint should force per-block estimate upwards"
        );
    }
}
