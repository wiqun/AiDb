//! DB P1 测试: flush 边界, 并发, WriteBatch 细分, 背压
//! @component aidb-engine

use aidb::config::Options;
use aidb::{WriteBatch, DB};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

fn tiny_opts() -> Options {
    let mut o = Options::for_testing();
    o.memtable_size = 512;
    o.max_write_buffer_number = 2;
    o.sync_wal = true;
    o.background_compaction = false;
    o
}

fn filler_key(i: u8) -> Vec<u8> {
    vec![b'f', i]
}

#[test]
fn test_auto_flush_on_memtable_full() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), tiny_opts()).unwrap();
    for i in 0..32u8 {
        db.put(&filler_key(i), &[i; 64]).unwrap();
    }
    assert!(
        db.immutable_memtable_count() >= 1 || db.level0_sstable_count() >= 1,
        "memtable full should freeze or flush"
    );
    db.close().unwrap();
}

#[test]
fn test_flush_empty_db() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), tiny_opts()).unwrap();
    db.flush().unwrap();
    assert_eq!(db.level0_sstable_count(), 0);
    db.close().unwrap();
}

#[test]
fn test_multiple_flushes() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), tiny_opts()).unwrap();
    for round in 0..3u8 {
        db.put(&[b'r', round], &[round]).unwrap();
        db.flush().unwrap();
    }
    assert!(db.level0_sstable_count() >= 1);
    for round in 0..3u8 {
        assert_eq!(db.get(&[b'r', round]).unwrap(), Some(vec![round]));
    }
    db.close().unwrap();
}

#[test]
fn test_flush_with_deletes() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), tiny_opts()).unwrap();
    db.put(b"gone", b"x").unwrap();
    db.delete(b"gone").unwrap();
    db.put(b"stay", b"y").unwrap();
    db.flush().unwrap();
    assert_eq!(db.get(b"gone").unwrap(), None);
    assert_eq!(db.get(b"stay").unwrap(), Some(b"y".to_vec()));
    db.close().unwrap();
}

#[test]
fn test_close_triggers_flush() {
    let dir = tempdir().unwrap();
    {
        let db = DB::open(dir.path(), tiny_opts()).unwrap();
        db.put(b"close_me", b"ok").unwrap();
        db.close().unwrap();
    }
    let db = DB::open(dir.path(), tiny_opts()).unwrap();
    assert_eq!(db.get(b"close_me").unwrap(), Some(b"ok".to_vec()));
    db.close().unwrap();
}

#[test]
fn test_write_batch_single_put() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), tiny_opts()).unwrap();
    let mut batch = WriteBatch::new();
    batch.put(b"one", b"1");
    db.write(&batch).unwrap();
    assert_eq!(db.get(b"one").unwrap(), Some(b"1".to_vec()));
    db.close().unwrap();
}

#[test]
fn test_write_batch_multiple_puts() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), tiny_opts()).unwrap();
    let mut batch = WriteBatch::new();
    for i in 0..5u8 {
        batch.put([b'k', i], [i]);
    }
    db.write(&batch).unwrap();
    for i in 0..5u8 {
        assert_eq!(db.get(&[b'k', i]).unwrap(), Some(vec![i]));
    }
    db.close().unwrap();
}

#[test]
fn test_write_batch_delete_only() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), tiny_opts()).unwrap();
    db.put(b"x", b"1").unwrap();
    let mut batch = WriteBatch::new();
    batch.delete(b"x");
    db.write(&batch).unwrap();
    assert_eq!(db.get(b"x").unwrap(), None);
    db.close().unwrap();
}

#[test]
fn test_concurrent_writes_during_freeze() {
    let dir = tempdir().unwrap();
    let db = Arc::new(DB::open(dir.path(), tiny_opts()).unwrap());
    let db2 = Arc::clone(&db);
    let writer = thread::spawn(move || {
        for i in 32..64u8 {
            db2.put(&filler_key(i), &[i; 64]).unwrap();
        }
    });
    for i in 0..32u8 {
        db.put(&filler_key(i), &[i; 64]).unwrap();
    }
    writer.join().unwrap();
    assert_eq!(db.get(&filler_key(40)).unwrap().unwrap()[0], 40);
    db.close().unwrap();
}

#[test]
fn test_multiple_immutable_get() {
    let dir = tempdir().unwrap();
    let mut opts = tiny_opts();
    opts.max_write_buffer_number = 3;
    let db = DB::open(dir.path(), opts).unwrap();
    db.put(b"anchor", b"v0").unwrap();
    for i in 0..48u8 {
        db.put(&filler_key(i), &[i; 80]).unwrap();
    }
    assert_eq!(db.get(b"anchor").unwrap(), Some(b"v0".to_vec()));
    db.close().unwrap();
}

#[test]
fn test_iterator_concurrent_write() {
    let dir = tempdir().unwrap();
    let db = Arc::new(DB::open(dir.path(), tiny_opts()).unwrap());
    for i in 0..8u8 {
        db.put(&[b'i', i], &[i]).unwrap();
    }
    let db2 = Arc::clone(&db);
    let writer = thread::spawn(move || {
        for i in 8..16u8 {
            db2.put(&[b'i', i], &[i]).unwrap();
        }
    });
    let mut seen = 0usize;
    let mut it = db.iter().unwrap();
    while let Some(Ok(_)) = it.next() {
        seen += 1;
        if seen == 4 {
            thread::sleep(Duration::from_millis(1));
        }
    }
    writer.join().unwrap();
    assert!(seen >= 8);
    db.close().unwrap();
}

#[test]
fn test_flush_immutable_concurrent_read() {
    let dir = tempdir().unwrap();
    let db = Arc::new(DB::open(dir.path(), tiny_opts()).unwrap());
    db.put(b"read_me", b"yes").unwrap();
    for i in 0..40u8 {
        db.put(&filler_key(i), &[i; 64]).unwrap();
    }
    let db2 = Arc::clone(&db);
    let reader = thread::spawn(move || {
        for _ in 0..50 {
            assert_eq!(db2.get(b"read_me").unwrap(), Some(b"yes".to_vec()));
        }
    });
    db.flush().unwrap();
    reader.join().unwrap();
    db.close().unwrap();
}

#[test]
fn test_wal_partial_corruption_recovery() {
    let dir = tempdir().unwrap();
    {
        let db = DB::open(dir.path(), tiny_opts()).unwrap();
        db.put(b"good", b"1").unwrap();
        db.put(b"also", b"2").unwrap();
        // 模拟崩溃: 不 close, Drop 仅 sync WAL
    }
    let wal_path = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("wal_") && n.ends_with(".log"))
        })
        .expect("wal file");
    let mut data = std::fs::read(&wal_path).unwrap();
    if data.len() > 32 {
        data.truncate(data.len() - 16);
        std::fs::write(&wal_path, data).unwrap();
    }
    let mut opts = tiny_opts();
    opts.strict_wal_recovery = false;
    let db = DB::open(dir.path(), opts).unwrap();
    assert_eq!(db.get(b"good").unwrap(), Some(b"1".to_vec()));
    db.close().unwrap();
}

#[test]
fn test_flush_backpressure_blocks_third_memtable() {
    let dir = tempdir().unwrap();
    let mut opts = tiny_opts();
    opts.max_write_buffer_number = 2;
    opts.memtable_size = 256;
    let db = DB::open(dir.path(), opts).unwrap();
    for i in 0..80u8 {
        db.put(&filler_key(i), &[i; 48]).unwrap();
    }
    assert!(
        db.immutable_memtable_count() < 2,
        "backpressure should cap immutable memtables below max_write_buffer_number"
    );
    db.flush().unwrap();
    db.close().unwrap();
}

#[test]
fn test_flush_reclaim() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), tiny_opts()).unwrap();
    for i in 0..40u8 {
        db.put(&filler_key(i), &[i; 64]).unwrap();
    }
    assert!(
        db.immutable_memtable_count() >= 1 || db.level0_sstable_count() >= 1,
        "memtable full should freeze or flush"
    );
    db.flush().unwrap();
    assert_eq!(db.immutable_memtable_count(), 0);
    db.close().unwrap();
}

#[test]
fn test_close_during_background_flush() {
    let dir = tempdir().unwrap();
    {
        let mut opts = tiny_opts();
        opts.memtable_size = 256;
        let db = Arc::new(DB::open(dir.path(), opts).unwrap());
        db.put(b"seed", b"1").unwrap();
        let db2 = Arc::clone(&db);
        let writer = thread::spawn(move || {
            for i in 0..120u8 {
                let _ = db2.put(&filler_key(i), &[i; 80]);
            }
        });
        thread::sleep(Duration::from_millis(5));
        db.close().unwrap();
        writer.join().unwrap();
    }
    let db2 = DB::open(dir.path(), tiny_opts()).unwrap();
    assert_eq!(db2.get(b"seed").unwrap(), Some(b"1".to_vec()));
    db2.close().unwrap();
}

#[test]
fn test_sstable_load_skip_corrupted() {
    let dir = tempdir().unwrap();
    {
        let db = DB::open(dir.path(), tiny_opts()).unwrap();
        db.put(b"good", b"1").unwrap();
        db.flush().unwrap();
        db.put(b"bad", b"2").unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }
    let mut sst_paths: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "sst"))
        .collect();
    assert!(sst_paths.len() >= 2);
    // read_dir order is undefined; second flush has the highest file number.
    sst_paths.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
    let mut data = std::fs::read(sst_paths.last().unwrap()).unwrap();
    if data.len() > 20 {
        data[10] ^= 0xff;
        std::fs::write(sst_paths.last().unwrap(), data).unwrap();
    }
    let db = DB::open(dir.path(), tiny_opts()).unwrap();
    assert_eq!(db.get(b"good").unwrap(), Some(b"1".to_vec()));
    db.close().unwrap();
}
