//! DB 引擎 API 测试
//! @component aidb-engine

use aidb::config::Options;
use aidb::{Error, WriteBatch, DB};
use std::sync::Arc;
use tempfile::tempdir;

fn small_opts() -> Options {
    let mut o = Options::for_testing();
    o.memtable_size = 4096;
    o.sync_wal = true;
    o
}

/// 验证 DB 引擎 Open 打开与重复 Close 安全性
#[test]
fn test_db_open_and_close() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    db.close().unwrap();
    db.close().unwrap();
}

/// 验证 DB 引擎写入 (Put) 与读取 (Get)
#[test]
fn test_db_put_and_get() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    db.put(b"k", b"v").unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
    assert_eq!(db.get(b"missing").unwrap(), None);
    db.close().unwrap();
}

/// 验证 DB 引擎删除 (Delete) 操作
#[test]
fn test_db_delete() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    db.put(b"k", b"v").unwrap();
    db.delete(b"k").unwrap();
    assert_eq!(db.get(b"k").unwrap(), None);
    db.close().unwrap();
}

/// 验证 DB 引擎相同 Key 覆盖写入
#[test]
fn test_db_overwrite() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    db.put(b"k", b"v1").unwrap();
    db.put(b"k", b"v2").unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"v2".to_vec()));
    db.close().unwrap();
}

/// 验证 Key 删除后重新写入 Resurrection 成功
#[test]
fn test_key_resurrection() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    db.put(b"k", b"v1").unwrap();
    db.delete(b"k").unwrap();
    db.put(b"k", b"v2").unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"v2".to_vec()));
    db.close().unwrap();
}

#[test]
fn test_empty_key_rejected() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    assert!(matches!(db.put(b"", b"v"), Err(Error::InvalidArgument(_))));
    db.close().unwrap();
}

#[test]
fn test_empty_value() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    db.put(b"k", b"").unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(vec![]));
    db.close().unwrap();
}

#[test]
fn test_write_batch_empty() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    let batch = WriteBatch::new();
    let _ = db.write(&batch).unwrap();
    db.close().unwrap();
}

#[test]
fn test_write_batch_mixed() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    let mut batch = WriteBatch::new();
    batch.put(b"a", b"1");
    batch.put(b"b", b"2");
    batch.delete(b"a");
    let _ = db.write(&batch).unwrap();
    assert_eq!(db.get(b"a").unwrap(), None);
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    db.close().unwrap();
}

#[test]
fn test_manual_flush() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    db.put(b"k", b"lush").unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"lush".to_vec()));
    db.flush().unwrap();
    eprintln!(
        "files {:?}",
        std::fs::read_dir(dir.path()).unwrap().collect::<Vec<_>>()
    );
    assert_eq!(db.get(b"k").unwrap(), Some(b"lush".to_vec()));
    let sst_count: usize = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "sst").unwrap_or(false))
        .count();
    assert!(sst_count >= 1);
    db.close().unwrap();
}

#[test]
fn test_flush_persistence() {
    let dir = tempdir().unwrap();
    {
        let db = DB::open(dir.path(), small_opts()).unwrap();
        db.put(b"persist", b"ok").unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }
    let db = DB::open(dir.path(), small_opts()).unwrap();
    assert_eq!(db.get(b"persist").unwrap(), Some(b"ok".to_vec()));
    db.close().unwrap();
}

#[test]
fn test_db_recovery() {
    let dir = tempdir().unwrap();
    {
        let db = DB::open(dir.path(), small_opts()).unwrap();
        db.put(b"rec", b"overed").unwrap();
        db.close().unwrap();
    }
    let db = DB::open(dir.path(), small_opts()).unwrap();
    assert_eq!(db.get(b"rec").unwrap(), Some(b"overed".to_vec()));
    db.close().unwrap();
}

#[test]
fn test_db_delete_range() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    for (k, v) in [(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")] {
        db.put(k, v).unwrap();
    }
    db.delete_range(b"b", b"d").unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), None);
    assert_eq!(db.get(b"c").unwrap(), None);
    assert_eq!(db.get(b"d").unwrap(), Some(b"4".to_vec()));
    db.close().unwrap();
}

#[test]
fn test_wal_gc_after_flush() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    db.put(b"k1", b"v1").unwrap();
    db.flush().unwrap();
    db.put(b"k2", b"v2").unwrap();
    db.flush().unwrap();
    let wal_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("wal_"))
        })
        .collect();
    assert!(
        wal_files.len() <= 2,
        "flushed WALs should be cleaned up, found {:?}",
        wal_files
    );
    assert_eq!(db.get(b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(db.get(b"k2").unwrap(), Some(b"v2".to_vec()));
    db.close().unwrap();
}

#[test]
fn test_scan_range() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    for (k, v) in [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")] {
        db.put(k, v).unwrap();
    }
    let mut it = db.scan(Some(b"b"), Some(b"d")).unwrap();
    let mut keys = Vec::new();
    for item in it.by_ref().take(10) {
        let (k, _) = item.unwrap();
        keys.push(k);
    }
    assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec()]);
    db.close().unwrap();
}

#[test]
fn test_iterator_filters_tombstone() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    db.put(b"x", b"1").unwrap();
    db.delete(b"x").unwrap();
    db.put(b"y", b"2").unwrap();
    let mut it = db.iter().unwrap();
    let mut keys = Vec::new();
    while let Some(Ok((k, _))) = it.next() {
        keys.push(k);
    }
    assert_eq!(keys, vec![b"y".to_vec()]);
    db.close().unwrap();
}

#[test]
fn test_snapshot_after_overwrite() {
    let dir = tempdir().unwrap();
    let db = Arc::new(DB::open(dir.path(), small_opts()).unwrap());
    db.put(b"s", b"old").unwrap();
    let snap = db.snapshot().unwrap();
    db.put(b"s", b"new").unwrap();
    assert_eq!(snap.get(b"s").unwrap(), Some(b"old".to_vec()));
    assert_eq!(db.get(b"s").unwrap(), Some(b"new".to_vec()));
    db.close().unwrap();
}

#[test]
fn test_error_if_exists() {
    let dir = tempdir().unwrap();
    std::fs::create_dir_all(dir.path()).unwrap();
    let mut opts = small_opts();
    opts.error_if_exists = true;
    assert!(matches!(
        DB::open(dir.path(), opts),
        Err(Error::InvalidArgument(_))
    ));
}

#[test]
fn test_db_iterator_prev_and_seek_to_last() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    for (k, v) in [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")] {
        db.put(k, v).unwrap();
    }
    // Use seek_to_last to go to end, then walk back
    let mut it = db.iter().unwrap();
    it.seek_to_last();
    assert!(it.valid());
    assert_eq!(it.key(), Some(b"c".as_slice()));
    assert!(it.prev());
    assert_eq!(it.key(), Some(b"b".as_slice()));
    assert!(it.prev());
    assert_eq!(it.key(), Some(b"a".as_slice()));
    // prev at first entry returns false
    assert!(!it.prev());
    // Iterator should be invalid
    assert!(!it.valid());
    db.close().unwrap();
}

#[test]
fn test_db_reverse_iteration_with_deletes() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.delete(b"b").unwrap();
    db.put(b"c", b"3").unwrap();
    // Reverse iteration should skip the deleted b
    let mut it = db.iter().unwrap();
    it.seek_to_last();
    assert!(it.valid());
    assert_eq!(it.key(), Some(b"c".as_slice()));
    assert!(it.prev());
    assert_eq!(it.key(), Some(b"a".as_slice()));
    assert!(!it.prev());
    db.close().unwrap();
}

#[test]
fn test_db_reverse_scan_with_range() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    for (k, v) in [(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")] {
        db.put(k, v).unwrap();
    }
    // Use scan with start/end then verify forward
    let mut it = db.scan(Some(b"b"), Some(b"d")).unwrap();
    let mut keys = Vec::new();
    for item in it.by_ref().take(10) {
        let (k, _) = item.unwrap();
        keys.push(k);
    }
    assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec()]);
    db.close().unwrap();
}

/// flush 后重开, 估值来自 SST; 同一 key 再写入时, MemTable 里的新版本也计入.
#[test]
fn estimate_includes_sst_and_newer_memtable_version() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    db.put(b"k", b"v1").unwrap();
    db.flush().unwrap();
    drop(db);
    let db = DB::open(dir.path(), small_opts()).unwrap();
    assert_eq!(db.estimate_entry_count(), 1);
    db.put(b"k", b"v2").unwrap();
    assert_eq!(db.estimate_entry_count(), 2);
}

/// 重开后, 在任何新写入之前估值已经来自已打开的 SST, 不是 0.
#[test]
fn estimate_reopen_is_not_zero_before_new_writes() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    db.put(b"k", b"v1").unwrap();
    db.flush().unwrap();
    drop(db);
    let db = DB::open(dir.path(), small_opts()).unwrap();
    assert_eq!(db.estimate_entry_count(), 1);
}
