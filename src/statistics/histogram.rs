//! 无锁固定分桶直方图实现.

use std::sync::atomic::{AtomicU64, Ordering};

/// 9 个直方图分桶上界 (微秒), 与 OTel 现有的 9 个边界完全对齐 (0.0001s ~ 1.0s)
pub const HISTOGRAM_BOUNDS_US: [u64; 9] = [
    100,       // 0.1 ms (0.0001 s)
    500,       // 0.5 ms (0.0005 s)
    1_000,     // 1.0 ms (0.001 s)
    5_000,     // 5.0 ms (0.005 s)
    10_000,    // 10 ms  (0.01 s)
    50_000,    // 50 ms  (0.05 s)
    100_000,   // 100 ms (0.1 s)
    500_000,   // 500 ms (0.5 s)
    1_000_000, // 1.0 s  (1.0 s)
];

/// 固定分桶数量 (9 个有限区间 + 1 个溢出桶)
pub const NUM_HISTOGRAM_BUCKETS: usize = HISTOGRAM_BOUNDS_US.len() + 1; // 10

/// 桶 0~8 的中心点代表值 (秒, 用于 OTel 差分重放)
pub const BUCKET_MID_POINTS_SECS: [f64; 9] = [
    0.00005, // (0 + 100us) / 2
    0.00030, // (100us + 500us) / 2
    0.00075, // (500us + 1ms) / 2
    0.00300, // (1ms + 5ms) / 2
    0.00750, // (5ms + 10ms) / 2
    0.03000, // (10ms + 50ms) / 2
    0.07500, // (50ms + 100ms) / 2
    0.30000, // (100ms + 500ms) / 2
    0.75000, // (500ms + 1s) / 2
];

/// 桶 9 (> 1.0 s) 溢出桶代表值 (秒), 严格大于 1.0s 确保精准落入 OTel 的 +Inf 桶
pub const OVERFLOW_BUCKET_VALUE_SECS: f64 = 2.0;

/// 无锁固定分桶原子直方图
#[derive(Debug)]
pub struct AtomicHistogram {
    pub buckets: [AtomicU64; NUM_HISTOGRAM_BUCKETS],
    pub sum_us: AtomicU64,
    pub count: AtomicU64,
}

impl Default for AtomicHistogram {
    fn default() -> Self {
        Self {
            buckets: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
            sum_us: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }
}

impl AtomicHistogram {
    /// 记录一次耗时 (微秒). 二分定位桶索引并原子无锁累加.
    #[inline]
    pub fn record(&self, duration_us: u64) {
        let idx = match HISTOGRAM_BOUNDS_US.binary_search(&duration_us) {
            Ok(i) => i,
            Err(i) => i,
        };
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(duration_us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// 重置直方图为零状态
    pub fn reset(&self) {
        for b in &self.buckets {
            b.store(0, Ordering::Relaxed);
        }
        self.sum_us.store(0, Ordering::Relaxed);
        self.count.store(0, Ordering::Relaxed);
    }

    /// 导出当前直方图数值快照 (10 桶数组 + sum_us)
    pub fn snapshot(&self) -> ([u64; NUM_HISTOGRAM_BUCKETS], u64) {
        let mut b = [0u64; NUM_HISTOGRAM_BUCKETS];
        for (i, slot) in self.buckets.iter().enumerate() {
            b[i] = slot.load(Ordering::Relaxed);
        }
        (b, self.sum_us.load(Ordering::Relaxed))
    }
}
