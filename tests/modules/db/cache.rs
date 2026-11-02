//! DB 共享 BlockCache 集成测试 (Phase7.4)
//! @component aidb-engine

use aidb::config::Options;
use aidb::engine::db::DB;
use tempfile::tempdir;

#[test]
fn test_db_shared_block_cache() {
    let dir = tempdir().unwrap();
    let mut opts = Options::for_testing();
    opts.block_cache_size = 512 * 1024;
    opts.memtable_size = 256;

    let db = DB::open(dir.path(), opts).unwrap();
    for i in 0..50 {
        db.put(
            format!("key_{i:03}").as_bytes(),
            format!("val_{i}").as_bytes(),
        )
        .unwrap();
    }

    db.flush().unwrap();
    let stats_after_flush = db.cache_stats();
    assert!(
        stats_after_flush.insertions >= 1,
        "open/read_key_range or flush reads should warm cache: {stats_after_flush:?}"
    );

    db.reset_cache_stats();
    for i in 0..50 {
        let _ = db.get(format!("key_{i:03}").as_bytes()).unwrap();
    }
    let stats_after_reads = db.cache_stats();
    assert!(
        stats_after_reads.hits >= 1,
        "second full read pass should hit cache: {stats_after_reads:?}"
    );
    assert!(
        stats_after_reads.misses < stats_after_reads.lookups,
        "most lookups should be hits after warming: {stats_after_reads:?}"
    );
    assert_eq!(db.block_cache_capacity(), 512 * 1024);
}

#[test]
fn test_db_clear_cache() {
    let dir = tempdir().unwrap();
    let mut opts = Options::for_testing();
    opts.block_cache_size = 256 * 1024;
    opts.row_cache_size = 0;
    opts.memtable_size = 256;

    let db = DB::open(dir.path(), opts).unwrap();
    db.put(b"clear_me", b"value").unwrap();
    db.flush().unwrap();

    assert!(db.get(b"clear_me").unwrap().is_some());
    db.reset_cache_stats();
    assert!(db.get(b"clear_me").unwrap().is_some());
    assert!(db.cache_stats().hits >= 1);

    db.clear_cache();
    db.reset_cache_stats();
    assert!(db.get(b"clear_me").unwrap().is_some());
    let stats = db.cache_stats();
    assert!(
        stats.misses >= 1,
        "after clear_cache, read should miss then re-warm: {stats:?}"
    );
}

#[test]
fn test_db_block_cache_disabled() {
    let dir = tempdir().unwrap();
    let mut opts = Options::for_testing();
    opts.block_cache_size = 0;
    opts.memtable_size = 256;

    let db = DB::open(dir.path(), opts).unwrap();
    assert_eq!(db.block_cache_capacity(), 0);

    db.put(b"k", b"v").unwrap();
    db.flush().unwrap();
    db.reset_cache_stats();

    assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
    let stats = db.cache_stats();
    assert_eq!(stats.insertions, 0, "capacity=0 must not insert");
    assert!(stats.lookups >= 1);
    assert_eq!(stats.hits, 0);
    assert!(stats.misses >= 1);
}
