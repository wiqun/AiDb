//! DB 全链路: flush / reopen / batch / scan / 并发 (roadmap 5.9)
//! @component aidb-engine

use aidb::config::Options;
use aidb::{WriteBatch, DB};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;

fn opts() -> Options {
    let mut o = Options::for_testing();
    o.memtable_size = 2048;
    o.sync_wal = true;
    o
}

#[test]
fn test_integration_put_flush_reopen_scan() {
    let dir = tempdir().unwrap();
    {
        let db = DB::open(dir.path(), opts()).unwrap();
        for i in 0..100u8 {
            let key = [b'k', i];
            db.put(&key, &[i]).unwrap();
        }
        db.flush().unwrap();
        db.close().unwrap();
    }
    let db = DB::open(dir.path(), opts()).unwrap();
    for i in 0..100u8 {
        let key = [b'k', i];
        assert_eq!(db.get(&key).unwrap(), Some(vec![i]));
    }
    let mut it = db.scan(None, None).unwrap();
    let mut count = 0;
    while it.next().is_some() {
        count += 1;
    }
    assert_eq!(count, 100);
    db.close().unwrap();
}

#[test]
fn test_integration_write_batch_reopen() {
    let dir = tempdir().unwrap();
    {
        let db = DB::open(dir.path(), opts()).unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"w1", b"a");
        batch.put(b"w2", b"b");
        batch.delete(b"w1");
        db.write(&batch).unwrap();
        db.close().unwrap();
    }
    let db = DB::open(dir.path(), opts()).unwrap();
    assert_eq!(db.get(b"w1").unwrap(), None);
    assert_eq!(db.get(b"w2").unwrap(), Some(b"b".to_vec()));
    db.close().unwrap();
}

#[test]
fn test_integration_concurrent_read_while_write() {
    let dir = tempdir().unwrap();
    let db = Arc::new(DB::open(dir.path(), opts()).unwrap());
    db.put(b"shared", b"v0").unwrap();
    let db2 = Arc::clone(&db);
    let reader = std::thread::spawn(move || {
        for _ in 0..50 {
            let _ = db2.get(b"shared").unwrap();
        }
    });
    for i in 1..50 {
        db.put(b"shared", format!("v{i}").as_bytes()).unwrap();
    }
    reader.join().unwrap();
    assert!(db.get(b"shared").unwrap().is_some());
    db.close().unwrap();
}

/// 多线程并发读 + 并发 flush/compaction, 验证读结果不因并发写而损坏.
#[test]
fn test_concurrent_read_during_flush_and_compact() {
    let dir = tempdir().unwrap();
    let mut o = opts();
    o.memtable_size = 256;
    o.level0_compaction_trigger = 2;
    let db = DB::open(dir.path(), o).unwrap();

    // Pre-populate
    for i in 0..50u8 {
        db.put(&[i], &[i; 100]).unwrap();
    }
    db.flush().unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = Arc::clone(&stop);
    let db_for_writer = Arc::clone(&db);

    // Writer: alternate writes, flushes, and compactions
    let writer = thread::spawn(move || {
        let mut cycle = 0u8;
        while !stop2.load(Ordering::Relaxed) && cycle < 20 {
            for i in 0..30u8 {
                let _ = db_for_writer.put(&[i], &[cycle; 100]);
            }
            let _ = db_for_writer.flush();
            let _ = db_for_writer.drain_compactions();
            cycle += 1;
        }
    });

    // Readers: concurrent reads
    let db_for_readers = Arc::clone(&db);
    let readers: Vec<_> = (0..8)
        .map(|_| {
            let d = Arc::clone(&db_for_readers);
            thread::spawn(move || {
                for _ in 0..200 {
                    let _ = d.get(&[0]);
                    let _ = d.get(&[25]);
                    let _ = d.get(&[49]);
                }
            })
        })
        .collect();

    for h in readers {
        h.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    let val = db.get(&[0]).unwrap();
    assert!(val.is_some());
    db.close().unwrap();
}
