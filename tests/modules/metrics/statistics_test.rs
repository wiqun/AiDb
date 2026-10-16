use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

use aidb::statistics::{
    AtomicHistogram, BackupOp, CompactionPhase, DbOp, Statistics, WriteStallKind,
    HISTOGRAM_BOUNDS_US, NUM_HISTOGRAM_BUCKETS,
};

#[test]
fn test_atomic_histogram_bucket_boundaries_and_overflow() {
    let hist = AtomicHistogram::default();
    assert_eq!(HISTOGRAM_BOUNDS_US.len(), 9);
    assert_eq!(NUM_HISTOGRAM_BUCKETS, 10);

    // 1. 50 us -> 落在 < 100 us 的桶 0 (Err(0))
    hist.record(50);
    // 2. 100 us -> 严格命中上界 100 us, 落在桶 0 (Ok(0))
    hist.record(100);
    // 3. 500 us -> 严格命中上界 500 us, 落在桶 1 (Ok(1))
    hist.record(500);
    // 4. 1_000_000 us (1s) -> 严格命中非溢出上界 1s, 落在桶 8 (Ok(8))
    hist.record(1_000_000);
    // 5. 1_500_000 us (1.5s) -> 超过 1s 上界, 落在溢出桶 9 (Err(9))
    hist.record(1_500_000);

    assert_eq!(hist.buckets[0].load(Ordering::Relaxed), 2);
    assert_eq!(hist.buckets[1].load(Ordering::Relaxed), 1);
    for i in 2..8 {
        assert_eq!(hist.buckets[i].load(Ordering::Relaxed), 0);
    }
    assert_eq!(hist.buckets[8].load(Ordering::Relaxed), 1);
    assert_eq!(hist.buckets[9].load(Ordering::Relaxed), 1);

    assert_eq!(hist.count.load(Ordering::Relaxed), 5);
    assert_eq!(
        hist.sum_us.load(Ordering::Relaxed),
        50 + 100 + 500 + 1_000_000 + 1_500_000
    );
}

#[test]
fn test_atomic_histogram_concurrent_record() {
    let hist = Arc::new(AtomicHistogram::default());
    let mut handles = Vec::new();

    for _ in 0..10 {
        let h = Arc::clone(&hist);
        handles.push(thread::spawn(move || {
            for _ in 0..10_000 {
                h.record(200); // 落在桶 1 (100 us ~ 500 us)
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(hist.count.load(Ordering::Relaxed), 100_000);
    assert_eq!(hist.buckets[1].load(Ordering::Relaxed), 100_000);
    assert_eq!(hist.sum_us.load(Ordering::Relaxed), 100_000 * 200);
}

#[test]
fn test_statistics_dynamic_max_levels() {
    let stats_3 = Statistics::new(3);
    assert_eq!(stats_3.sstable_count.len(), 3);
    assert_eq!(stats_3.sstable_size_bytes.len(), 3);
    let snap_3 = stats_3.snapshot();
    assert_eq!(snap_3.sstable_count.len(), 3);
    assert_eq!(snap_3.sstable_size_bytes.len(), 3);

    let stats_10 = Statistics::new(10);
    assert_eq!(stats_10.sstable_count.len(), 10);
    assert_eq!(stats_10.sstable_size_bytes.len(), 10);
    let snap_10 = stats_10.snapshot();
    assert_eq!(snap_10.sstable_count.len(), 10);
    assert_eq!(snap_10.sstable_size_bytes.len(), 10);
}

#[test]
fn test_statistics_reset_behavior() {
    let stats = Statistics::new(7);

    // 注入 Counter
    stats.operations[DbOp::Put as usize].store(42, Ordering::Relaxed);
    stats.compaction_phases[CompactionPhase::Pick as usize].store(5, Ordering::Relaxed);
    stats.backup_total[BackupOp::Create as usize].store(3, Ordering::Relaxed);
    stats.flush_total.store(10, Ordering::Relaxed);
    stats.block_cache_hits.store(100, Ordering::Relaxed);
    stats.block_cache_misses.store(20, Ordering::Relaxed);
    stats.bloom_false_positive.store(2, Ordering::Relaxed);

    #[cfg(feature = "cluster")]
    {
        stats.raft_rpc[0][0].store(99, Ordering::Relaxed);
        stats.raft_log_entries.store(500, Ordering::Relaxed);
        stats.raft_group_fatal.store(1, Ordering::Relaxed);
        stats.raft_group_restart[0].store(4, Ordering::Relaxed);
    }

    // 注入直方图
    stats.operation_durations[DbOp::Put as usize].record(250);
    stats.flush_duration.record(5_000);
    stats.compaction_durations[CompactionPhase::Run as usize].record(15_000);
    stats.backup_duration.record(500_000);
    stats.write_stall_durations[WriteStallKind::MemTableSlowdown as usize].record(2_000);

    // 注入统计极值 Gauge
    stats
        .write_stall_max_duration_us
        .store(8888, Ordering::Relaxed);

    // 注入物理瞬时 Gauge (reset 必须保留)
    stats.wal_size_bytes.store(1024 * 1024, Ordering::Relaxed);
    stats.sequence.store(12345, Ordering::Relaxed);
    stats.memtable_size_bytes[0].store(4096, Ordering::Relaxed);
    stats.sstable_count[0].store(7, Ordering::Relaxed);
    stats.sstable_size_bytes[0].store(65536, Ordering::Relaxed);
    stats.block_cache_size.store(2048, Ordering::Relaxed);
    stats.block_cache_capacity.store(65536, Ordering::Relaxed);
    stats.backup_size_bytes.store(50000, Ordering::Relaxed);

    // 执行 Reset
    stats.reset();

    // 断言 1: Counter 必须全部清零
    assert_eq!(
        stats.operations[DbOp::Put as usize].load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        stats.compaction_phases[CompactionPhase::Pick as usize].load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        stats.backup_total[BackupOp::Create as usize].load(Ordering::Relaxed),
        0
    );
    assert_eq!(stats.flush_total.load(Ordering::Relaxed), 0);
    assert_eq!(stats.block_cache_hits.load(Ordering::Relaxed), 0);
    assert_eq!(stats.block_cache_misses.load(Ordering::Relaxed), 0);
    assert_eq!(stats.bloom_false_positive.load(Ordering::Relaxed), 0);

    #[cfg(feature = "cluster")]
    {
        assert_eq!(stats.raft_rpc[0][0].load(Ordering::Relaxed), 0);
        assert_eq!(stats.raft_log_entries.load(Ordering::Relaxed), 0);
        assert_eq!(stats.raft_group_fatal.load(Ordering::Relaxed), 0);
        assert_eq!(stats.raft_group_restart[0].load(Ordering::Relaxed), 0);
    }

    // 断言 2: 直方图必须全部清零
    assert_eq!(
        stats.operation_durations[DbOp::Put as usize]
            .count
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        stats.operation_durations[DbOp::Put as usize]
            .sum_us
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(stats.flush_duration.count.load(Ordering::Relaxed), 0);
    assert_eq!(
        stats.compaction_durations[CompactionPhase::Run as usize]
            .count
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(stats.backup_duration.count.load(Ordering::Relaxed), 0);
    assert_eq!(
        stats.write_stall_durations[WriteStallKind::MemTableSlowdown as usize]
            .count
            .load(Ordering::Relaxed),
        0
    );

    // 断言 3: 统计极值 Gauge 必须清零
    assert_eq!(stats.write_stall_max_duration_us.load(Ordering::Relaxed), 0);

    // 断言 4: 物理瞬时 Gauge 必须完好保留!
    assert_eq!(stats.wal_size_bytes.load(Ordering::Relaxed), 1024 * 1024);
    assert_eq!(stats.sequence.load(Ordering::Relaxed), 12345);
    assert_eq!(stats.memtable_size_bytes[0].load(Ordering::Relaxed), 4096);
    assert_eq!(stats.sstable_count[0].load(Ordering::Relaxed), 7);
    assert_eq!(stats.sstable_size_bytes[0].load(Ordering::Relaxed), 65536);
    assert_eq!(stats.block_cache_size.load(Ordering::Relaxed), 2048);
    assert_eq!(stats.block_cache_capacity.load(Ordering::Relaxed), 65536);
    assert_eq!(stats.backup_size_bytes.load(Ordering::Relaxed), 50000);

    // 快照也必须与原子字段保持一致
    let snap = stats.snapshot();
    assert_eq!(snap.operations[DbOp::Put as usize], 0);
    assert_eq!(snap.wal_size_bytes, 1024 * 1024);
    assert_eq!(snap.sequence, 12345);
    assert_eq!(snap.write_stall_max_duration_us, 0);
}
