use std::sync::atomic::Ordering;
use std::sync::Arc;

use aidb::metrics::{sync_to_otel, testutil};
use aidb::statistics::{Statistics, WriteStallKind};

#[test]
fn test_otel_export_new_counters_gauges_and_stall() {
    let exporter = testutil::init_in_memory();
    let stats = Arc::new(Statistics::new(7));

    // 注入 Counters 初始值
    stats.wal_written_bytes.store(1000, Ordering::Relaxed);
    stats.flush_written_bytes.store(2000, Ordering::Relaxed);
    stats
        .compaction_written_bytes
        .store(3000, Ordering::Relaxed);
    stats.logical_write_bytes.store(4000, Ordering::Relaxed);
    stats.block_read_bytes.store(5000, Ordering::Relaxed);
    stats.logical_read_bytes.store(6000, Ordering::Relaxed);
    stats.compaction_read_bytes.store(7000, Ordering::Relaxed);
    stats.bloom_useful.store(80, Ordering::Relaxed);

    // 注入 Write Stall
    let stall_kind = WriteStallKind::L0FilesSlowdown;
    stats.write_stall_requests[stall_kind as usize].store(5, Ordering::Relaxed);
    stats
        .write_stall_window_max_us
        .store(50_000, Ordering::Relaxed);

    // 注入 Pending Compaction Bytes
    stats
        .compaction_pending_bytes
        .store(123_456, Ordering::Relaxed);

    // 第一次同步
    sync_to_otel(&stats);

    // 验证 7 个字节 Counters + bloom_useful
    assert_eq!(
        testutil::counter_sum(&exporter, "aidb_wal_written_bytes_total"),
        1000
    );
    assert_eq!(
        testutil::counter_sum(&exporter, "aidb_flush_written_bytes_total"),
        2000
    );
    assert_eq!(
        testutil::counter_sum(&exporter, "aidb_compaction_written_bytes_total"),
        3000
    );
    assert_eq!(
        testutil::counter_sum(&exporter, "aidb_logical_write_bytes_total"),
        4000
    );
    assert_eq!(
        testutil::counter_sum(&exporter, "aidb_block_read_bytes_total"),
        5000
    );
    assert_eq!(
        testutil::counter_sum(&exporter, "aidb_logical_read_bytes_total"),
        6000
    );
    assert_eq!(
        testutil::counter_sum(&exporter, "aidb_compaction_read_bytes_total"),
        7000
    );
    assert_eq!(
        testutil::counter_sum(&exporter, "aidb_bloom_useful_total"),
        80
    );

    assert_eq!(
        testutil::counter_sum(&exporter, "aidb_write_stall_total"),
        5
    );
    assert_eq!(
        testutil::histogram_count(&exporter, "aidb_write_stall_duration_seconds"),
        0
    );

    assert_eq!(
        testutil::gauge_value(&exporter, "aidb_compaction_pending_bytes"),
        123_456.0
    );
    assert_eq!(
        testutil::gauge_value(&exporter, "aidb_write_stall_window_max_us"),
        50_000.0
    );

    // 第二次同步 (无新增增量), 验证差分推进不重复累计
    sync_to_otel(&stats);
    assert_eq!(
        testutil::counter_sum(&exporter, "aidb_wal_written_bytes_total"),
        1000
    );
    assert_eq!(
        testutil::counter_sum(&exporter, "aidb_write_stall_total"),
        5
    );
    assert_eq!(
        testutil::histogram_count(&exporter, "aidb_write_stall_duration_seconds"),
        0
    );

    // 增加增量
    stats.wal_written_bytes.fetch_add(500, Ordering::Relaxed);
    stats.write_stall_requests[stall_kind as usize].fetch_add(2, Ordering::Relaxed);

    sync_to_otel(&stats);
    assert_eq!(
        testutil::counter_sum(&exporter, "aidb_wal_written_bytes_total"),
        1500
    );
    assert_eq!(
        testutil::counter_sum(&exporter, "aidb_write_stall_total"),
        7
    );
    assert_eq!(
        testutil::histogram_count(&exporter, "aidb_write_stall_duration_seconds"),
        0
    );
}
