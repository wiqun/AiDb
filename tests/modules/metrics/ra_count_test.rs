//! @component aidb-engine
//! 经典读放大 (RA) 口径测试: block_seek_count (含 BlockCache 命中) / logical_read_count
//!
//!   cargo test --test metrics ra_count -- --test-threads=1

use std::sync::atomic::Ordering;
use tempfile::TempDir;

use aidb::config::Options;
use aidb::DB;

/// Seam 1: read_block_cached 的 hit 与 miss 双路径均累加 block_seek_count,
/// 而 block_read_bytes (读穿透字节) 仅 miss 落盘路径累加.
#[test]
fn test_block_seek_count_hit_and_miss() {
    let dir = TempDir::new().unwrap();
    let opts = Options {
        block_size: 256,
        ..Default::default()
    };
    let db = DB::open(dir.path(), opts).unwrap();
    let stats = db.statistics();

    for i in 0..20 {
        db.put(format!("k_{i:02}").as_bytes(), &[b'v'; 128])
            .unwrap();
    }
    db.flush().unwrap();

    let target_key = b"k_10";
    let expected_val = vec![b'v'; 128];

    let seek_0 = stats.block_seek_count.load(Ordering::Relaxed);
    assert_eq!(seek_0, 0);
    assert_eq!(stats.block_read_bytes.load(Ordering::Relaxed), 0);

    // 1. 冷读: 目标 key 位于中间 block, cache miss 真正读盘
    let res = db.get(target_key).unwrap();
    assert_eq!(res, Some(expected_val.clone()));

    let seek_1 = stats.block_seek_count.load(Ordering::Relaxed);
    let disk_read_1 = stats.block_read_bytes.load(Ordering::Relaxed);
    assert!(
        seek_1 > seek_0,
        "Cold read (cache miss) must increment block_seek_count (got {seek_1})"
    );
    assert!(
        disk_read_1 > 0,
        "Cold read must increase block_read_bytes (got {disk_read_1})"
    );

    // 2. 热读: 命中 BlockCache, 触碰计数继续累加, 落盘字节不变
    let res2 = db.get(target_key).unwrap();
    assert_eq!(res2, Some(expected_val));

    let seek_2 = stats.block_seek_count.load(Ordering::Relaxed);
    let disk_read_2 = stats.block_read_bytes.load(Ordering::Relaxed);
    assert!(
        seek_2 > seek_1,
        "Cache hit must still increment block_seek_count (hit path counts)"
    );
    assert_eq!(
        disk_read_2, disk_read_1,
        "Cache hit must NOT increase block_read_bytes (disk penetration bytes)"
    );
}

/// Seam 3: get 命中与未命中各计 1 次逻辑读.
#[test]
fn test_logical_read_count_get_hit_and_miss() {
    let dir = TempDir::new().unwrap();
    let db = DB::open(dir.path(), Options::default()).unwrap();
    let stats = db.statistics();

    db.put(b"hit_key", b"hit_val").unwrap();
    db.flush().unwrap();

    let count_0 = stats.logical_read_count.load(Ordering::Relaxed);
    assert_eq!(count_0, 0);

    // 1. 命中读
    assert_eq!(db.get(b"hit_key").unwrap(), Some(b"hit_val".to_vec()));
    assert_eq!(
        stats.logical_read_count.load(Ordering::Relaxed),
        1,
        "Hit get must count exactly 1 logical read"
    );

    // 2. 未命中读 (bloom 判定 key 不存在, 未触碰任何 data block)
    assert_eq!(db.get(b"absent_key_missing").unwrap(), None);
    assert_eq!(
        stats.logical_read_count.load(Ordering::Relaxed),
        2,
        "Miss get must still count exactly 1 logical read"
    );
}

/// Seam 3: 迭代扫描每产出 1 条 KV 计 1 次逻辑读.
#[test]
fn test_logical_read_count_scan() {
    let dir = TempDir::new().unwrap();
    let db = DB::open(dir.path(), Options::default()).unwrap();
    let stats = db.statistics();

    const N: u64 = 20;
    for i in 0..N {
        db.put(format!("scan_{i:02}").as_bytes(), b"val").unwrap();
    }
    db.flush().unwrap();

    let count_0 = stats.logical_read_count.load(Ordering::Relaxed);

    let mut yielded = 0u64;
    let mut it = db.scan(None, None).unwrap();
    while let Some(Ok((_, _))) = it.next() {
        yielded += 1;
    }
    assert_eq!(yielded, N);

    assert_eq!(
        stats.logical_read_count.load(Ordering::Relaxed),
        count_0 + N,
        "Each yielded KV from scan must count exactly 1 logical read"
    );
}

/// 写路径内部存在性检查 (stats: None) 与 Compaction 不得污染触碰计数.
#[test]
fn test_internal_paths_do_not_pollute_block_seek_count() {
    let dir = TempDir::new().unwrap();
    let opts = Options {
        level0_compaction_trigger: 2,
        ..Default::default()
    };
    let db = DB::open(dir.path(), opts).unwrap();
    let stats = db.statistics();

    for i in 0..20 {
        db.put(format!("k_{i:02}").as_bytes(), b"val1").unwrap();
    }
    db.flush().unwrap();
    for i in 10..30 {
        db.put(format!("k_{i:02}").as_bytes(), b"val2").unwrap();
    }
    db.flush().unwrap();

    assert_eq!(stats.block_seek_count.load(Ordering::Relaxed), 0);

    // 无任何用户读的前提下执行 Compaction
    db.drain_compactions().unwrap();

    assert_eq!(
        stats.block_seek_count.load(Ordering::Relaxed),
        0,
        "Compaction with no user reads must NOT increment block_seek_count"
    );
    assert_eq!(
        stats.logical_read_count.load(Ordering::Relaxed),
        0,
        "Internal write path must NOT increment logical_read_count"
    );

    // 写路径覆盖已存在 key (触发内部 key_exists) 同样不得污染
    db.put(b"k_05", b"val3_updated").unwrap();
    assert_eq!(
        stats.block_seek_count.load(Ordering::Relaxed),
        0,
        "Write path key_exists must NOT increment block_seek_count"
    );
}
