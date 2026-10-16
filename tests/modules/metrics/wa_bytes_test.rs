use std::sync::atomic::Ordering;
use tempfile::TempDir;

use aidb::config::Options;
use aidb::{WriteBatch, DB};

#[test]
fn test_wa_wal_and_logical_bytes() {
    let dir = TempDir::new().unwrap();
    let db = DB::open(dir.path(), Options::default()).unwrap();
    let stats = db.statistics();

    let key = b"my_key_12345";
    let val = b"my_val_67890_abcdef";
    let expected_kv_len = (key.len() + val.len()) as u64;

    // 1. 单键 Put
    db.put(key, val).unwrap();
    let logical_after_put = stats.logical_write_bytes.load(Ordering::Relaxed);
    let wal_after_put = stats.wal_written_bytes.load(Ordering::Relaxed);

    assert_eq!(
        logical_after_put, expected_kv_len,
        "logical_write_bytes should equal key.len() + val.len()"
    );
    assert!(
        wal_after_put >= logical_after_put,
        "wal_written_bytes ({wal_after_put}) should be >= logical ({logical_after_put}) due to record header/trailer"
    );

    // 2. 单键 Delete
    let del_key = b"del_key_abc";
    db.delete(del_key).unwrap();
    let logical_after_del = stats.logical_write_bytes.load(Ordering::Relaxed);
    assert_eq!(
        logical_after_del,
        logical_after_put + del_key.len() as u64,
        "delete should accumulate key.len() to logical_write_bytes"
    );

    // 3. WriteBatch (Put + Delete)
    let mut batch = WriteBatch::new();
    batch.put(b"bk1", b"bv1");
    batch.delete(b"bk2");
    let batch_bytes = (b"bk1".len() + b"bv1".len() + b"bk2".len()) as u64;

    db.write(&batch).unwrap();
    let logical_after_batch = stats.logical_write_bytes.load(Ordering::Relaxed);
    assert_eq!(
        logical_after_batch,
        logical_after_del + batch_bytes,
        "write batch should accumulate each op to logical_write_bytes"
    );
}

#[test]
fn test_wa_flush_written_bytes() {
    let dir = TempDir::new().unwrap();
    let db = DB::open(dir.path(), Options::default()).unwrap();
    let stats = db.statistics();

    // 写入数据确保非空 MemTable
    db.put(b"flush_k1", b"flush_v1").unwrap();
    db.put(b"flush_k2", b"flush_v2").unwrap();

    let flush_before = stats.flush_written_bytes.load(Ordering::Relaxed);
    assert_eq!(flush_before, 0);

    db.flush().unwrap();

    let flush_after = stats.flush_written_bytes.load(Ordering::Relaxed);
    assert!(
        flush_after > 0,
        "flush_written_bytes should be greater than 0 after successful flush"
    );

    // 验证 WA 公式: (wal + flush + compaction) / logical >= 1.0
    let wal = stats.wal_written_bytes.load(Ordering::Relaxed);
    let compaction = stats.compaction_written_bytes.load(Ordering::Relaxed);
    let logical = stats.logical_write_bytes.load(Ordering::Relaxed);

    assert!(logical > 0);
    let physical_write = wal + flush_after + compaction;
    let wa = (physical_write as f64) / (logical as f64);
    assert!(
        wa >= 1.0,
        "WA ratio ({wa}) must be >= 1.0, physical={physical_write}, logical={logical}"
    );
}
