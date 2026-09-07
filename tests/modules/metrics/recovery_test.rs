use tempfile::TempDir;

use aidb::config::Options;
use aidb::DB;

#[test]
fn test_recovery_metrics_and_reset_retention() {
    let dir = TempDir::new().unwrap();
    let opts = Options {
        block_size: 256,
        sync_wal: true,
        ..Options::for_testing()
    };

    // 1. 首次打开并写入数据，故意不完全 flush，保留未落盘 WAL 尾巴
    {
        let db = DB::open(dir.path(), opts.clone()).unwrap();
        db.put(b"key_1", b"value_1").unwrap();
        db.put(b"key_2", b"value_2").unwrap();
        // flush 产生 SSTable 与 Manifest 变体
        db.flush().unwrap();
        // 再次写入留在 WAL 中
        db.put(b"key_unflushed", b"value_unflushed").unwrap();
        drop(db);
    } // 严格确保 db 在重新打开前被 drop 关闭，释放句柄与文件锁
    std::thread::sleep(std::time::Duration::from_millis(20));

    // 2. 重新打开数据库，触发真实冷启动恢复
    let db = DB::open(dir.path(), opts).unwrap();
    let stats = db.statistics();
    let snap = stats.snapshot();

    // [ASSUMPTION]: 重启后必须发生真实 WAL 回放，各项耗时微秒与回放字节大于 0
    assert!(
        snap.recovery_wal_replay_duration_us > 0,
        "recovery_wal_replay_duration_us must be > 0 (got {})",
        snap.recovery_wal_replay_duration_us
    );
    assert!(
        snap.recovery_wal_replayed_bytes > 0,
        "recovery_wal_replayed_bytes must be > 0 (got {})",
        snap.recovery_wal_replayed_bytes
    );
    assert!(
        snap.recovery_manifest_duration_us > 0,
        "recovery_manifest_duration_us must be > 0 (got {})",
        snap.recovery_manifest_duration_us
    );
    assert!(
        snap.recovery_sstable_open_duration_us > 0,
        "recovery_sstable_open_duration_us must be > 0 (got {})",
        snap.recovery_sstable_open_duration_us
    );

    // 3. 验证 Reset 保留契约：轮间清零不影响恢复事实
    stats.reset();
    let snap_after_reset = stats.snapshot();
    assert_eq!(
        snap_after_reset.recovery_wal_replay_duration_us, snap.recovery_wal_replay_duration_us,
        "recovery_wal_replay_duration_us must survive stats.reset()"
    );
    assert_eq!(
        snap_after_reset.recovery_wal_replayed_bytes, snap.recovery_wal_replayed_bytes,
        "recovery_wal_replayed_bytes must survive stats.reset()"
    );
    assert_eq!(
        snap_after_reset.recovery_manifest_duration_us, snap.recovery_manifest_duration_us,
        "recovery_manifest_duration_us must survive stats.reset()"
    );
    assert_eq!(
        snap_after_reset.recovery_sstable_open_duration_us, snap.recovery_sstable_open_duration_us,
        "recovery_sstable_open_duration_us must survive stats.reset()"
    );
}

#[cfg(feature = "monitoring")]
#[test]
fn test_recovery_metrics_otel_export() {
    let exporter = aidb::metrics::testutil::init_in_memory();
    let dir = TempDir::new().unwrap();
    let opts = Options {
        block_size: 256,
        sync_wal: true,
        ..Options::for_testing()
    };
    {
        let db = DB::open(dir.path(), opts.clone()).unwrap();
        db.put(b"k1", b"v1").unwrap();
        db.flush().unwrap();
        db.put(b"k2", b"v2").unwrap();
        drop(db);
    }
    std::thread::sleep(std::time::Duration::from_millis(20));

    let db = DB::open(dir.path(), opts).unwrap();
    let snap = db.statistics().snapshot();
    aidb::metrics::sync_to_otel(db.statistics().as_ref());

    // 使用 testutil 统一辅助函数断言 4 个恢复 Gauge 指标且数值严格对应
    let wal_dur = aidb::metrics::testutil::gauge_value(
        &exporter,
        "aidb_recovery_wal_replay_duration_seconds",
    );
    assert!(
        wal_dur > 0.0,
        "aidb_recovery_wal_replay_duration_seconds must be exported (> 0)"
    );
    let wal_bytes =
        aidb::metrics::testutil::gauge_value(&exporter, "aidb_recovery_wal_replayed_bytes");
    assert_eq!(wal_bytes, snap.recovery_wal_replayed_bytes as f64);
    let manifest_dur =
        aidb::metrics::testutil::gauge_value(&exporter, "aidb_recovery_manifest_duration_seconds");
    assert!(
        manifest_dur > 0.0,
        "aidb_recovery_manifest_duration_seconds must be exported (> 0)"
    );
    let sst_dur = aidb::metrics::testutil::gauge_value(
        &exporter,
        "aidb_recovery_sstable_open_duration_seconds",
    );
    assert!(
        sst_dur > 0.0,
        "aidb_recovery_sstable_open_duration_seconds must be exported (> 0)"
    );
}
