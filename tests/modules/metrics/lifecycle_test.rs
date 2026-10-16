use std::sync::atomic::Ordering;
use std::sync::Arc;
use tempfile::TempDir;

use aidb::config::Options;
use aidb::engine::cache::BlockCache;
use aidb::statistics::Statistics;
use aidb::{Error, DB};

#[test]
fn test_db_default_statistics_lifecycle() {
    let dir = TempDir::new().unwrap();
    let db = DB::open(dir.path(), Options::default()).unwrap();

    let stats = db.statistics();
    assert_eq!(stats.snapshot().sstable_count.len(), 7);
    assert_eq!(stats.snapshot().sstable_size_bytes.len(), 7);
    assert_eq!(
        stats.block_cache_capacity.load(Ordering::Relaxed),
        64 * 1024 * 1024
    );
}

#[test]
fn test_db_custom_statistics_injection_and_consistency() {
    let dir = TempDir::new().unwrap();

    // 1. 正向用例: max_levels 与 custom_stats 层数一致
    let custom_stats = Arc::new(Statistics::new(4));
    let opts = Options {
        max_levels: 4,
        statistics: Some(Arc::clone(&custom_stats)),
        ..Default::default()
    };

    let db = DB::open(dir.path(), opts).unwrap();
    assert!(Arc::ptr_eq(&db.statistics(), &custom_stats));
    assert_eq!(db.statistics().snapshot().sstable_count.len(), 4);

    // 2. 负向用例: max_levels=7 但注入 len=4 的 statistics -> 必须返回 InvalidArgument
    let dir2 = TempDir::new().unwrap();
    let bad_opts = Options {
        max_levels: 7,
        statistics: Some(Arc::new(Statistics::new(4))),
        ..Default::default()
    };

    let err = DB::open(dir2.path(), bad_opts);
    assert!(
        matches!(err.as_ref().err(), Some(Error::InvalidArgument(_))),
        "Expected InvalidArgument error on mismatched levels, got {:?}",
        err.as_ref().err()
    );
}

#[test]
fn test_memtable_atomic_metrics_write() {
    let dir = TempDir::new().unwrap();
    let db = DB::open(dir.path(), Options::default()).unwrap();
    let stats = db.statistics();

    assert_eq!(stats.memtable_size_bytes[0].load(Ordering::Relaxed), 0);

    // 写入数据后, MemTable active 字节数必须原子增加, frozen 保持为 0
    db.put(b"test_key", b"test_val").unwrap();
    assert!(stats.memtable_size_bytes[0].load(Ordering::Relaxed) > 0);
    assert_eq!(stats.memtable_size_bytes[1].load(Ordering::Relaxed), 0);

    // 执行 flush: active 清零, 冻结表刷盘后从 frozen 扣回, 最终回到 0
    db.flush().unwrap();
    assert_eq!(stats.memtable_size_bytes[0].load(Ordering::Relaxed), 0);
    assert_eq!(stats.memtable_size_bytes[1].load(Ordering::Relaxed), 0);
}

#[test]
fn test_block_cache_atomic_metrics_injection() {
    let stats = Arc::new(Statistics::new(7));
    assert_eq!(stats.block_cache_capacity.load(Ordering::Relaxed), 0);
    assert_eq!(stats.block_cache_hits.load(Ordering::Relaxed), 0);
    assert_eq!(stats.block_cache_misses.load(Ordering::Relaxed), 0);

    // 注入 stats 创建 BlockCache (容量 1MB)
    let cache = BlockCache::new_with_stats(1024 * 1024, Some(Arc::clone(&stats)));
    assert_eq!(
        stats.block_cache_capacity.load(Ordering::Relaxed),
        1024 * 1024
    );

    let key = aidb::engine::cache::CacheKey {
        file_number: 1,
        offset: 0,
    };

    // 查空 key -> 触发 cache miss
    let res = cache.get(key.clone());
    assert!(res.is_none());
    assert_eq!(stats.block_cache_misses.load(Ordering::Relaxed), 1);
    assert_eq!(stats.block_cache_hits.load(Ordering::Relaxed), 0);

    // 插入并读取命中 -> 触发 cache hit
    let dummy_block = bytes::Bytes::from_static(b"123");
    cache.insert(key.clone(), dummy_block);
    let hit_res = cache.get(key);
    assert!(hit_res.is_some());
    assert_eq!(stats.block_cache_hits.load(Ordering::Relaxed), 1);
    assert!(stats.block_cache_size.load(Ordering::Relaxed) > 0);
}
