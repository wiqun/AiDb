//! @component aidb-cache
//! RowCache 分片并发 LRU 缓存与写提交失效测试

use bytes::Bytes;
use std::sync::Arc;
use tempfile::tempdir;

use aidb::config::Options;
use aidb::engine::cache::RowCache;
use aidb::DB;

#[test]
fn test_row_cache_standalone_lru_and_invalidation() {
    let cache = RowCache::new(1024 * 64);
    let key1 = b"user_1001";
    let val1 = Bytes::from_static(b"value_1001");

    assert_eq!(cache.get(key1, 100), None);

    cache.insert(key1, val1.clone(), 50);
    // 可见性检查: snapshot_seq >= 50 时可见
    assert_eq!(cache.get(key1, 50), Some(val1.clone()));
    assert_eq!(cache.get(key1, 60), Some(val1.clone()));
    // snapshot_seq < 50 时不可见 (返回 None)
    assert_eq!(cache.get(key1, 40), None);

    // 失效检查
    cache.invalidate(key1);
    assert_eq!(cache.get(key1, 100), None);
}

#[test]
fn test_db_row_cache_integration_lifecycle() {
    let dir = tempdir().unwrap();
    let mut opts = Options::for_testing();
    opts.row_cache_size = 1024 * 1024;
    let db = DB::open(dir.path(), opts).unwrap();

    let key = b"my_key";
    let val1 = b"my_val_1";
    let val2 = b"my_val_2";

    db.put(key, val1).unwrap();

    // 第一次读: 从 LSM 读取并回填 RowCache
    let r1 = db.get(key).unwrap();
    assert_eq!(r1.as_deref(), Some(val1.as_slice()));

    // 第二次读: 应当命中 RowCache
    let r2 = db.get(key).unwrap();
    assert_eq!(r2.as_deref(), Some(val1.as_slice()));

    let stats = db.row_cache().stats();
    assert!(stats.hits >= 1, "第二次点查必须命中 RowCache");

    // 覆写 key: 触发自动失效
    db.put(key, val2).unwrap();
    let r3 = db.get(key).unwrap();
    assert_eq!(r3.as_deref(), Some(val2.as_slice()));

    // 删除 key: 触发自动失效
    db.delete(key).unwrap();
    assert_eq!(db.get(key).unwrap(), None);
}

#[test]
fn test_row_cache_concurrency_multithread() {
    let cache = Arc::new(RowCache::new(1024 * 128));
    let mut handles = Vec::new();

    for t in 0..16 {
        let c = Arc::clone(&cache);
        handles.push(std::thread::spawn(move || {
            for i in 0..100 {
                let key = format!("k_{t}_{i}").into_bytes();
                let val = Bytes::from(format!("v_{t}_{i}").into_bytes());
                c.insert(&key, val.clone(), 100);
                assert_eq!(c.get(&key, 100), Some(val));
                if i % 2 == 0 {
                    c.invalidate(&key);
                    assert_eq!(c.get(&key, 100), None);
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}
