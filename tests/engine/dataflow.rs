//! L2 引擎黑盒 dataflow — put → flush → get 全路径 (Mode A + E)
//! @component aidb-engine

use crate::common::dataflow::capture_spans;
use aidb::config::Options;
use aidb::WriteBatch;
use aidb::DB;
use std::sync::Arc;
use tempfile::tempdir;

fn opts() -> Options {
    let mut o = Options::for_testing();
    o.memtable_size = 2048;
    o.sync_wal = true;
    o.background_compaction = false;
    o
}

#[test]
#[serial_test::serial]
fn test_put_flush_get_lifecycle() {
    // capture_spans 内部已调用 tracing_test_lock()
    let dir = tempdir().unwrap();
    let db = Arc::new(DB::open(dir.path(), opts()).unwrap());

    let put_tree = capture_spans(|| {
        db.put(b"lifecycle", b"ok").unwrap();
    });
    put_tree.assert_ancestor("wal_write", "db_put");
    put_tree.assert_ancestor("mem_put", "db_put");

    let flush_tree = capture_spans(|| {
        db.flush().unwrap();
    });
    assert!(!flush_tree.spans_named("db_flush").is_empty());

    let get_tree = capture_spans(|| {
        assert_eq!(db.get(b"lifecycle").unwrap(), Some(b"ok".to_vec()));
    });
    get_tree.assert_ancestor("sst_seek", "db_get");

    db.close().unwrap();
    drop(db);

    let db2 = DB::open(dir.path(), opts()).unwrap();
    assert_eq!(db2.get(b"lifecycle").unwrap(), Some(b"ok".to_vec()));
    db2.close().unwrap();
}

#[test]
fn test_write_batch_keyspace_net_add() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), opts()).unwrap();

    let mut batch = WriteBatch::new();
    batch.put(b"wb:k1", b"v1");
    batch.put(b"wb:k2", b"v2");
    db.write(&batch).unwrap();
    assert_eq!(scan_key_count(&db), 2);

    let mut overwrite = WriteBatch::new();
    overwrite.put(b"wb:k1", b"v1-new");
    overwrite.delete(b"wb:k2");
    db.write(&overwrite).unwrap();
    assert_eq!(scan_key_count(&db), 1);
    assert_eq!(db.get(b"wb:k1").unwrap(), Some(b"v1-new".to_vec()));

    db.close().unwrap();
}

fn scan_key_count(db: &DB) -> usize {
    db.scan(None, None).unwrap().count()
}

#[test]
fn test_wal_memtable_replay_via_db_open() {
    let dir = tempdir().unwrap();
    {
        let db = DB::open(dir.path(), opts()).unwrap();
        db.put(b"replay", b"wal").unwrap();
        // 崩溃式 Drop, 不经 close
    }
    let db2 = DB::open(dir.path(), opts()).unwrap();
    assert_eq!(db2.get(b"replay").unwrap(), Some(b"wal".to_vec()));
    db2.close().unwrap();
}
