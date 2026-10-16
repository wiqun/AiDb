use std::sync::atomic::Ordering;
use tempfile::TempDir;

use aidb::config::Options;
use aidb::engine::filter::bloom::bloom_false_positive_count;
use aidb::statistics::DbOp;
use aidb::DB;

#[test]
fn test_read_write_hotpath_atomic_stats() {
    let dir = TempDir::new().unwrap();
    let db = DB::open(dir.path(), Options::default()).unwrap();
    let stats = db.statistics();

    // 1. Put
    db.put(b"k1", b"v1").unwrap();
    // 2. Get
    let v = db.get(b"k1").unwrap();
    assert_eq!(v, Some(b"v1".to_vec()));
    // 3. Delete (内部使用 key_exists, 不调用 get, 避免污染 Get 计数与读放大指标)
    db.delete(b"k1").unwrap();
    // 4. Snapshot
    let _snap = db.snapshot();

    // 验证操作计数
    assert_eq!(
        stats.operations[DbOp::Put as usize].load(Ordering::Relaxed),
        1
    );
    // 仅 1 次显式 get (delete 内部改为 key_exists, 不计入 Get)
    assert_eq!(
        stats.operations[DbOp::Get as usize].load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        stats.operations[DbOp::Delete as usize].load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        stats.operations[DbOp::Snapshot as usize].load(Ordering::Relaxed),
        1
    );

    // 验证耗时直方图
    assert_eq!(
        stats.operation_durations[DbOp::Put as usize]
            .count
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        stats.operation_durations[DbOp::Get as usize]
            .count
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        stats.operation_durations[DbOp::Delete as usize]
            .count
            .load(Ordering::Relaxed),
        1
    );
    // Snapshot 只记计数不记耗时直方图, count 应为 0
    assert_eq!(
        stats.operation_durations[DbOp::Snapshot as usize]
            .count
            .load(Ordering::Relaxed),
        0
    );
}

#[test]
#[serial_test::serial]
fn test_bloom_false_positive_reader_stats_injection() {
    let dir = TempDir::new().unwrap();
    let opts = Options {
        bloom_false_positive_rate: 0.5,
        use_wal: false,
        ..Default::default()
    };
    let db = DB::open(dir.path(), opts).unwrap();
    let stats = db.statistics();

    // 写入一批 key 并 flush 落盘为 SSTable, 确保后续读取经过 SSTable Bloom 过滤器
    for i in 0..500 {
        db.put(format!("key_{i:04x}").as_bytes(), b"v").unwrap();
    }
    db.flush().unwrap();

    let s_before = stats.bloom_false_positive.load(Ordering::Relaxed);
    let g_before = bloom_false_positive_count();

    // 查询大量不存在的 key 触发 Bloom FP
    for i in 0..500 {
        let _ = db.get(format!("absent_{i:04x}").as_bytes());
    }

    let s_delta = stats.bloom_false_positive.load(Ordering::Relaxed) - s_before;
    let g_delta = bloom_false_positive_count() - g_before;

    // 双计数器严格对拍: 证明 SSTableReader 注入的 stats.bloom_false_positive 与全局计数器严格同步
    assert_eq!(
        s_delta, g_delta,
        "stats.bloom_false_positive should strictly track bloom_false_positive_count"
    );
    assert!(
        s_delta >= 1,
        "High FP rate (0.5) over 500 misses should trigger at least 1 false positive"
    );
}

#[test]
fn test_flush_and_wal_atomic_stats() {
    let dir = TempDir::new().unwrap();
    let db = DB::open(dir.path(), Options::default()).unwrap();
    let stats = db.statistics();

    // 写入非空数据确保不会触发 empty table abandon
    db.put(b"flush_k", b"flush_v").unwrap();
    db.flush().unwrap();

    // 断言 flush 计数与耗时
    assert_eq!(stats.flush_total.load(Ordering::Relaxed), 1);
    assert_eq!(stats.flush_duration.count.load(Ordering::Relaxed), 1);

    // 断言 WAL 大小大于 0
    assert!(stats.wal_size_bytes.load(Ordering::Relaxed) > 0);
}
