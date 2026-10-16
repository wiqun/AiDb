//! @component aidb-engine
//! 空间放大 (SA) 分母测试: live_bytes = Σ 各层 SST properties 的
//! raw_key_size + raw_value_size (未压缩有效 entry 载荷, internal key 开销 +8B/条).
//!
//!   cargo test --test metrics live_bytes -- --test-threads=1

use std::sync::atomic::Ordering;
use tempfile::TempDir;

use aidb::config::Options;
use aidb::DB;

const INTERNAL_KEY_OVERHEAD: u64 = 8;

/// Seam 2: flush 后 live_bytes 精确等于全部 SST 的有效 entry 载荷之和.
#[test]
fn test_live_bytes_after_flush() {
    let dir = TempDir::new().unwrap();
    let db = DB::open(dir.path(), Options::default()).unwrap();
    let stats = db.statistics();

    // 5 个 key: user_key 6B + internal 8B + value 100B = 114B/条
    let keys: Vec<&[u8]> = vec![b"key_01", b"key_02", b"key_03", b"key_04", b"key_05"];
    let val_len = 100u64;
    for k in &keys {
        db.put(k, &[b'v'; 100]).unwrap();
    }

    let live_before_flush = stats.live_bytes.load(Ordering::Relaxed);
    assert_eq!(live_before_flush, 0, "live_bytes only counts SST payload");

    db.flush().unwrap();

    let expected: u64 = (keys.len() as u64) * (6 + INTERNAL_KEY_OVERHEAD + val_len);
    let live_after = stats.live_bytes.load(Ordering::Relaxed);
    assert_eq!(
        live_after, expected,
        "live_bytes must equal Σ (user_key.len + 8 internal + value.len) across all SSTs"
    );
}

/// Seam 2: overwrite 旧版本滞留推高 live_bytes, compaction 合并去重后回落.
#[test]
fn test_live_bytes_converges_after_overwrite_compaction() {
    let dir = TempDir::new().unwrap();
    let opts = Options {
        level0_compaction_trigger: 2,
        ..Default::default()
    };
    let db = DB::open(dir.path(), opts).unwrap();
    let stats = db.statistics();

    // v1: 10 个 key ("ov_NN" 5B), value 200B
    for i in 0..10 {
        db.put(format!("ov_{i:02}").as_bytes(), &[b'a'; 200])
            .unwrap();
    }
    db.flush().unwrap();
    let live_v1 = stats.live_bytes.load(Ordering::Relaxed);
    let expected_v1: u64 = 10 * (5 + INTERNAL_KEY_OVERHEAD + 200);
    assert_eq!(live_v1, expected_v1);

    // v2: 同 key 覆盖为 50B 小值, flush 产生新 SST (旧版本仍滞留)
    for i in 0..10 {
        db.put(format!("ov_{i:02}").as_bytes(), &[b'b'; 50])
            .unwrap();
    }
    db.flush().unwrap();
    let live_stale = stats.live_bytes.load(Ordering::Relaxed);
    assert!(
        live_stale > live_v1,
        "Stale versions in extra SST must inflate live_bytes ({live_stale} > {live_v1})"
    );

    // compaction 去重后: 仅保留每个 key 的最新版本
    db.drain_compactions().unwrap();
    let live_v2 = stats.live_bytes.load(Ordering::Relaxed);
    let expected_v2: u64 = 10 * (5 + INTERNAL_KEY_OVERHEAD + 50);
    assert_eq!(
        live_v2, expected_v2,
        "After compaction, live_bytes must retain only the newest version per key"
    );
}
