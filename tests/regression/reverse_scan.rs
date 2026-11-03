//! 反向遍历 (seek_to_last / prev) 的范围边界与删除语义回归测试.
//! @component aidb-engine

use aidb::config::Options;
use aidb::{DbIterGuard, DB};
use tempfile::tempdir;

fn open_db() -> (tempfile::TempDir, std::sync::Arc<DB>) {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), Options::for_testing()).unwrap();
    (dir, db)
}

/// 从 `seek_to_last` 开始逐步 `prev`, 收集全部 user key (逆序).
fn collect_reverse(it: &mut DbIterGuard) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    it.seek_to_last();
    while it.valid() {
        out.push(it.key().unwrap().to_vec());
        if !it.prev() {
            break;
        }
    }
    out
}

/// 收集正向遍历的全部 user key.
fn collect_forward(it: DbIterGuard) -> Vec<Vec<u8>> {
    it.map(|r| r.unwrap().0).collect()
}

fn keys(list: &[&[u8]]) -> Vec<Vec<u8>> {
    list.iter().map(|k| k.to_vec()).collect()
}

/// bug: `DBIterator::seek_to_last` 忽略 `end_key`, 反向范围扫描会从 `end` 之后的 key 开始返回,
/// 期望: `scan([b, d))` 的 `seek_to_last` 定位到 `c`, 逆序结果为 `[c, b]`, 与内存表或 SST 位置无关.
/// Issue: #85 (LSM 纯内存跳表原生有界正反向扫描)
#[test]
fn reverse_seek_to_last_respects_end_bound_in_memtable() {
    let (_dir, db) = open_db();
    for k in [b"a", b"b", b"c", b"d", b"e"] {
        db.put(k, b"v").unwrap();
    }
    let mut it = db.scan(Some(b"b"), Some(b"d")).unwrap();
    assert_eq!(collect_reverse(&mut it), keys(&[b"c", b"b"]));
}

/// bug: 同上, 数据位于 SST 时 `seek_to_last` 同样忽略 `end_key`.
/// 期望: flush 后 `scan([b, d))` 反向结果仍为 `[c, b]`.
/// Issue: #85 (LSM 纯内存跳表原生有界正反向扫描)
#[test]
fn reverse_seek_to_last_respects_end_bound_in_sst() {
    let (_dir, db) = open_db();
    for k in [b"a", b"b", b"c", b"d", b"e"] {
        db.put(k, b"v").unwrap();
    }
    db.flush().unwrap();
    let mut it = db.scan(Some(b"b"), Some(b"d")).unwrap();
    assert_eq!(collect_reverse(&mut it), keys(&[b"c", b"b"]));
}

/// bug: 反向遍历不遵守 `start` 下界, `prev` 会越过 `start` 返回更小的 key.
/// 期望: `scan([b, e))` 反向只返回 `[d, c, b]`, 不返回 `a`; memtable 与 SST 混合布局下一致.
/// Issue: #85 (LSM 纯内存跳表原生有界正反向扫描)
#[test]
fn reverse_walk_respects_start_bound_across_layers() {
    let (_dir, db) = open_db();
    for k in [b"a", b"b", b"c", b"d", b"e"] {
        db.put(k, b"v").unwrap();
    }
    db.flush().unwrap();
    db.put(b"f", b"v").unwrap();
    let mut it = db.scan(Some(b"b"), Some(b"e")).unwrap();
    assert_eq!(collect_reverse(&mut it), keys(&[b"d", b"c", b"b"]));
}

/// bug: 反向遍历用 "存在更高 seq 的 Delete 即跳过整组" 的近似判断, 删除后重新写入的 key 会被误删.
/// 期望: `put(a) -> flush -> delete(a) -> put(a)` 之后, 反向遍历仍返回 `a` (最新版本为 Put).
#[test]
fn reverse_scan_keeps_key_rewritten_after_delete() {
    let (_dir, db) = open_db();
    db.put(b"a", b"1").unwrap();
    db.flush().unwrap();
    db.delete(b"a").unwrap();
    db.put(b"a", b"2").unwrap();
    db.put(b"b", b"3").unwrap();
    let mut it = db.iter().unwrap();
    assert_eq!(collect_reverse(&mut it), keys(&[b"b", b"a"]));
    let forward = collect_forward(db.iter().unwrap());
    assert_eq!(forward, keys(&[b"a", b"b"]));
}

/// 反向遍历必须只看到快照时刻已存在的版本: 快照之后写入的 key 不可见, 且最新版本的删除仍生效.
#[test]
fn reverse_scan_on_snapshot_ignores_later_writes() {
    let (_dir, db) = open_db();
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.flush().unwrap();
    let snap = db.snapshot().unwrap();
    db.delete(b"a").unwrap();
    db.put(b"c", b"3").unwrap();
    let mut it = snap.scan(None, None).unwrap();
    assert_eq!(collect_reverse(&mut it), keys(&[b"b", b"a"]));
    let mut live = db.scan(None, None).unwrap();
    assert_eq!(collect_reverse(&mut live), keys(&[b"c", b"b"]));
}

/// 场景 (Issue #84): `scan_limited` 纯内存跳表反向有界扫描需处理多版本覆盖与尾部连续墓碑.
/// 期望: 区间截断, `limit` 限制, 删除后重写及超 8 版本跳跃回退的结果与合并迭代器完全一致.
#[test]
fn test_scan_limited_reverse_in_memtable() {
    let (_dir, db) = open_db();
    for k in [b"a", b"b", b"c", b"d", b"e"] {
        db.put(k, b"v").unwrap();
    }
    // 区间 [b, e) limit=2 reverse=true -> [d, c]
    let res = db.scan_limited(Some(b"b"), Some(b"e"), 2, true).unwrap();
    let res_keys: Vec<_> = res.into_iter().map(|(k, _)| k).collect();
    assert_eq!(res_keys, keys(&[b"d", b"c"]));

    // limit=1 -> [d]
    let res = db.scan_limited(Some(b"b"), Some(b"e"), 1, true).unwrap();
    let res_keys: Vec<_> = res.into_iter().map(|(k, _)| k).collect();
    assert_eq!(res_keys, keys(&[b"d"]));

    // 删除 d 后, 期望返回 [c, b]
    db.delete(b"d").unwrap();
    let res = db.scan_limited(Some(b"b"), Some(b"e"), 2, true).unwrap();
    let res_keys: Vec<_> = res.into_iter().map(|(k, _)| k).collect();
    assert_eq!(res_keys, keys(&[b"c", b"b"]));

    // 重新 put d, 期望返回 [d, c]
    db.put(b"d", b"v2").unwrap();
    let res = db.scan_limited(Some(b"b"), Some(b"e"), 2, true).unwrap();
    let res_keys: Vec<_> = res.into_iter().map(|(k, _)| k).collect();
    assert_eq!(res_keys, keys(&[b"d", b"c"]));

    // 单 key 超过 8 个版本 + 尾部大量墓碑, 验证链表回退与跳跃切换一致
    for i in 0..12 {
        db.put(b"c", format!("vc_{i}").as_bytes()).unwrap();
    }
    for i in 0..12 {
        db.put(b"d", format!("vd_{i}").as_bytes()).unwrap();
    }
    db.delete(b"d").unwrap();
    let res = db.scan_limited(Some(b"b"), Some(b"e"), 2, true).unwrap();
    assert_eq!(
        res,
        vec![
            (b"c".to_vec(), b"vc_11".to_vec()),
            (b"b".to_vec(), b"v".to_vec())
        ]
    );
}

/// 场景 (Issue #84): 存在与查询区间不重叠的磁盘 SSTable 时, `scan_limited` 仍应命中纯内存跳表快速路径.
/// 期望: 正确跳过 `m3` 墓碑并逆序返回 `[m2, m1]`.
#[test]
fn test_scan_limited_reverse_with_sst_and_non_overlapping_sst() {
    let (_dir, db) = open_db();
    db.put(b"a_sst", b"1").unwrap();
    db.flush().unwrap();

    for k in [b"m1", b"m2", b"m3"] {
        db.put(k, b"v").unwrap();
    }
    db.delete(b"m3").unwrap();
    let res = db.scan_limited(Some(b"m0"), Some(b"m9"), 2, true).unwrap();
    let res_keys: Vec<_> = res.into_iter().map(|(k, _)| k).collect();
    assert_eq!(res_keys, keys(&[b"m2", b"m1"]));
}
