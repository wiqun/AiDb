//! 命令, 用户写批次和 `propose` 共用的耗时直方图.
//! 桶边界留在代码里. 每个桶记自己的次数, `sum_us` 是观测值之和.

use std::sync::atomic::{AtomicU64, Ordering};

/// 有限桶上界, 单位微秒. 第 16 桶是 `+Inf`.
pub const COMMAND_DURATION_BOUNDS_US: [u64; 15] = [
    100, 200, 500, 1_000, 2_000, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 2_500_000, 10_000_000,
];

const NUM_BUCKETS: usize = COMMAND_DURATION_BOUNDS_US.len() + 1;

/// 每个桶自己的次数, 不是累计.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LatencySnapshot {
    pub buckets: [u64; NUM_BUCKETS],
    pub count: u64,
    pub sum_us: u64,
}

#[derive(Debug)]
pub struct LatencyHistogram {
    buckets: [AtomicU64; NUM_BUCKETS],
    count: AtomicU64,
    sum_us: AtomicU64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
        }
    }
}

impl LatencyHistogram {
    /// 记一次真实耗时. 只给命中的那一个桶加 1.
    pub fn record(&self, duration_us: u64) {
        let idx = match COMMAND_DURATION_BOUNDS_US.binary_search(&duration_us) {
            Ok(i) => i,
            Err(i) => i,
        };
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(duration_us, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> LatencySnapshot {
        let mut buckets = [0u64; NUM_BUCKETS];
        for (i, slot) in self.buckets.iter().enumerate() {
            buckets[i] = slot.load(Ordering::Relaxed);
        }
        LatencySnapshot {
            buckets,
            count: self.count.load(Ordering::Relaxed),
            sum_us: self.sum_us.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::LatencyHistogram;
    use crate::statistics::render_histogram;

    #[test]
    fn latency_histogram_sum_is_the_observed_total() {
        let histogram = LatencyHistogram::default();
        histogram.record(150);
        histogram.record(3_000);
        let snap = histogram.snapshot();
        assert_eq!(snap.count, 2);
        assert_eq!(snap.sum_us, 3_150);
        let text = render_histogram("aikv_command_duration_seconds", "", &snap);
        assert!(
            text.contains("le=\"0.0002\"} 1"),
            "cumulative at 200us should be 1: {text}"
        );
        assert!(
            text.contains("le=\"0.005\"} 2"),
            "cumulative at 5ms should be 2: {text}"
        );
        assert!(
            text.contains("le=\"+Inf\"} 2"),
            "inf bucket should be 2: {text}"
        );
        assert!(text.contains("0.00315"), "sum seconds: {text}");
        assert!(
            text.contains("_count 2") || text.contains("_count 2\n"),
            "{text}"
        );
    }
}
