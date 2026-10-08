use std::sync::atomic::Ordering;
use tempfile::TempDir;

use aidb::config::Options;
use aidb::DB;

#[test]
fn test_user_read_and_logical_bytes() {
    let dir = TempDir::new().unwrap();
    let opts = Options {
        block_size: 256,
        ..Default::default()
    };
    let db = DB::open(dir.path(), opts).unwrap();
    let stats = db.statistics();

    // 写入多个 key 跨越多个 data blocks, 确保中间 block 未被 open 时的 read_key_range 缓存
    for i in 0..20 {
        db.put(format!("k_{i:02}").as_bytes(), &[b'v'; 128])
            .unwrap();
    }
    db.flush().unwrap();

    let target_key = b"k_10";
    let expected_val = vec![b'v'; 128];
    // 点查只计返回的 value 长度. miss 不加 key 长度.
    let kv_len = expected_val.len() as u64;

    let block_read_0 = stats.block_read_bytes.load(Ordering::Relaxed);
    let logical_read_0 = stats.logical_read_bytes.load(Ordering::Relaxed);
    assert_eq!(block_read_0, 0);

    // 1. 冷读: 目标 key 位于中间 block, 触发真正读盘
    let res = db.get(target_key).unwrap();
    assert_eq!(res, Some(expected_val.clone()));

    let block_read_1 = stats.block_read_bytes.load(Ordering::Relaxed);
    let logical_read_1 = stats.logical_read_bytes.load(Ordering::Relaxed);
    assert!(
        block_read_1 > 0,
        "Cold read must increase block_read_bytes (got {block_read_1})"
    );
    assert_eq!(
        logical_read_1,
        logical_read_0 + kv_len,
        "Cold read must increment logical_read_bytes by value length"
    );

    // 2. 热读: 命中 BlockCache, block_read_bytes 不变, logical_read_bytes 继续增加
    let res2 = db.get(target_key).unwrap();
    assert_eq!(res2, Some(expected_val.clone()));

    let block_read_2 = stats.block_read_bytes.load(Ordering::Relaxed);
    let logical_read_2 = stats.logical_read_bytes.load(Ordering::Relaxed);
    assert_eq!(
        block_read_2, block_read_1,
        "Cache hit must NOT increase block_read_bytes"
    );
    assert_eq!(
        logical_read_2,
        logical_read_1 + kv_len,
        "Cache hit must still increment logical_read_bytes"
    );

    // 3. Scan 扫描迭代器累加
    let mut it = db.scan(None, None).unwrap();
    let mut scanned_count = 0;
    while let Some(Ok((k, v))) = it.next() {
        if k == target_key {
            scanned_count += 1;
            assert_eq!(v, expected_val);
        }
    }
    assert_eq!(scanned_count, 1);
    let logical_read_3 = stats.logical_read_bytes.load(Ordering::Relaxed);
    assert!(
        logical_read_3 > logical_read_2,
        "DBIterator::next must accumulate logical_read_bytes"
    );
}

#[test]
fn test_delete_does_not_pollute_logical_read() {
    let dir = TempDir::new().unwrap();
    let db = DB::open(dir.path(), Options::default()).unwrap();
    let stats = db.statistics();

    db.put(b"del_probe_key", b"del_probe_val").unwrap();
    db.flush().unwrap();

    let logical_before = stats.logical_read_bytes.load(Ordering::Relaxed);
    assert_eq!(logical_before, 0);

    // 执行 delete, 内部使用 key_exists 而非 get, 不物化 value 且不累加 logical_read_bytes
    db.delete(b"del_probe_key").unwrap();

    let logical_after = stats.logical_read_bytes.load(Ordering::Relaxed);
    assert_eq!(
        logical_after, 0,
        "DB::delete must strictly NOT pollute logical_read_bytes"
    );
}

#[test]
fn test_bloom_useful() {
    let dir = TempDir::new().unwrap();
    let opts = Options {
        bloom_false_positive_rate: 0.01,
        ..Default::default()
    };
    let db = DB::open(dir.path(), opts).unwrap();
    let stats = db.statistics();

    // 写入并 flush
    for i in 0..50 {
        db.put(format!("key_{i:04}").as_bytes(), b"val").unwrap();
    }
    db.flush().unwrap();

    let bloom_useful_before = stats.bloom_useful.load(Ordering::Relaxed);

    // 查询绝对不存在的 key
    let res = db.get(b"non_existent_key_absent_12345").unwrap();
    assert_eq!(res, None);

    let bloom_useful_after = stats.bloom_useful.load(Ordering::Relaxed);
    assert!(
        bloom_useful_after > bloom_useful_before,
        "Bloom true negative should increment bloom_useful (before={bloom_useful_before}, after={bloom_useful_after})"
    );
}

#[test]
fn test_compaction_cache_isolation() {
    let dir = TempDir::new().unwrap();
    let opts = Options {
        level0_compaction_trigger: 2,
        ..Default::default()
    };
    let db = DB::open(dir.path(), opts).unwrap();
    let stats = db.statistics();

    // 写入两个 SST 并触发 flush
    for i in 0..20 {
        db.put(format!("k_{i:02}").as_bytes(), b"val1").unwrap();
    }
    db.flush().unwrap();

    for i in 10..30 {
        db.put(format!("k_{i:02}").as_bytes(), b"val2").unwrap();
    }
    db.flush().unwrap();

    // 在没有用户读取的前提下执行 Compaction
    let block_read_before = stats.block_read_bytes.load(Ordering::Relaxed);
    assert_eq!(block_read_before, 0);

    db.drain_compactions().unwrap();

    let block_read_after = stats.block_read_bytes.load(Ordering::Relaxed);
    let compaction_read = stats.compaction_read_bytes.load(Ordering::Relaxed);

    assert_eq!(
        block_read_after, 0,
        "Compaction using iter_uncached must NOT increase block_read_bytes"
    );
    assert!(
        compaction_read > 0,
        "Compaction must accumulate compaction_read_bytes"
    );
}

#[test]
fn test_write_path_key_exists_does_not_pollute_read_bytes_or_cache() {
    let dir = TempDir::new().unwrap();
    let opts = Options {
        block_size: 256,
        bloom_false_positive_rate: 0.0, // 关闭 bloom 确保查 SST 一定读 data block
        ..Default::default()
    };
    let db = DB::open(dir.path(), opts).unwrap();
    let stats = db.statistics();

    // 1. 写入 key 并 flush 到 SSTable
    let target_key = b"target_key_for_exists";
    db.put(target_key, b"value_1").unwrap();
    db.flush().unwrap();

    // 清空缓存与指标干扰
    db.clear_cache();
    stats.block_read_bytes.store(0, Ordering::Relaxed);
    assert_eq!(db.block_cache_size(), 0);

    // 2. 正向验证: 写路径 put / delete 已存在的 key (强制写路径 key_exists 查 SST)
    db.put(target_key, b"value_2_updated").unwrap();

    let block_read_after_write = stats.block_read_bytes.load(Ordering::Relaxed);
    let cache_size_after_write = db.block_cache_size();

    assert_eq!(
        block_read_after_write, 0,
        "Write path key_exists must NOT increment block_read_bytes (got {block_read_after_write})"
    );
    assert_eq!(
        cache_size_after_write, 0,
        "Write path key_exists must NOT insert into BlockCache (got {cache_size_after_write})"
    );

    // 3. 反向验证: 用户公开 API db.key_exists 必须正常读盘并进缓存
    // 先 flush 确保最新版本落入 SSTable (而非留在 memtable 中命中), 并清空缓存
    db.flush().unwrap();
    db.clear_cache();
    stats.block_read_bytes.store(0, Ordering::Relaxed);

    let exists = db.key_exists(target_key).unwrap();
    assert!(exists, "target_key should exist in SSTable");

    let block_read_after_user = stats.block_read_bytes.load(Ordering::Relaxed);
    let cache_size_after_user = db.block_cache_size();
    assert!(
        block_read_after_user > 0,
        "User db.key_exists must increment block_read_bytes (got {block_read_after_user})"
    );
    assert!(
        cache_size_after_user > 0,
        "User db.key_exists must populate BlockCache (got {cache_size_after_user})"
    );
}
