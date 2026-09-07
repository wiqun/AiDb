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
