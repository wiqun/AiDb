//! DB 层跨模块 dataflow — put/get/flush/delete 的 span 树 (Mode A)
//! @component aidb-engine

use crate::common::dataflow::capture_spans;
use crate::common::observability::tracing_test_lock;
use aidb::config::Options;
use aidb::statistics::DbOp;
use aidb::DB;
use std::sync::atomic::Ordering;
use tempfile::tempdir;

fn dataflow_opts() -> Options {
    let mut o = Options::for_testing();
    o.memtable_size = 4096;
    o.block_cache_size = 512 * 1024;
    o.bloom_false_positive_rate = 0.01;
    o.sync_wal = true;
    o.background_compaction = false;
    o
}

#[test]
#[serial_test::serial]
fn test_db_put_span_tree() {
    let _lock = tracing_test_lock();
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), dataflow_opts()).unwrap();

    let tree = capture_spans(|| {
        db.put(b"k", b"v").unwrap();
    });

    assert!(tree.spans_named("db_put").is_empty());
    assert!(tree.spans_named("wal_write").is_empty());
    assert!(tree.spans_named("mem_put").is_empty());
    assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
    db.close().unwrap();
}

#[test]
#[serial_test::serial]
fn test_db_get_memtable_span_tree() {
    let _lock = tracing_test_lock();
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), dataflow_opts()).unwrap();
    db.put(b"k", b"v").unwrap();

    let tree = capture_spans(|| {
        assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
    });

    assert!(tree.spans_named("db_get").is_empty());
    assert!(tree.spans_named("mem_search").is_empty());
    db.close().unwrap();
}

#[test]
#[serial_test::serial]
fn test_db_get_sstable_span_tree() {
    let _lock = tracing_test_lock();
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), dataflow_opts()).unwrap();
    db.put(b"sst", b"on_disk").unwrap();
    db.flush().unwrap();

    let tree = capture_spans(|| {
        assert_eq!(db.get(b"sst").unwrap(), Some(b"on_disk".to_vec()));
    });

    assert!(tree.spans_named("db_get").is_empty());
    assert!(tree.spans_named("sst_seek").is_empty());
    db.close().unwrap();
}

#[test]
fn test_db_delete_span_tree() {
    // capture_spans 内部已调用 tracing_test_lock()
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), dataflow_opts()).unwrap();
    db.put(b"k", b"v").unwrap();

    let tree = capture_spans(|| {
        db.delete(b"k").unwrap();
    });

    assert!(tree.spans_named("db_delete").is_empty());
    assert!(tree.spans_named("wal_write").is_empty());
    assert_eq!(db.get(b"k").unwrap(), None);
    db.close().unwrap();
}

#[test]
#[serial_test::serial]
fn test_db_flush_span_tree() {
    let _lock = tracing_test_lock();
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), dataflow_opts()).unwrap();
    db.put(b"f", b"lush").unwrap();

    aidb::observability::set_traces_enabled(true);
    let tree = capture_spans(|| {
        db.flush().unwrap();
    });
    aidb::observability::set_traces_enabled(false);

    assert!(!tree.spans_named("db_flush").is_empty());
    assert!(tree.spans_named("db_flush_sst").is_empty());
    assert!(tree.spans_named("sst_build_add").is_empty());
    db.close().unwrap();
}

#[test]
#[serial_test::serial]
fn test_db_put_event_chain() {
    let _lock = tracing_test_lock();
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), dataflow_opts()).unwrap();

    let tree = capture_spans(|| {
        db.put(b"ev", b"1").unwrap();
    });

    assert!(tree.spans_named("db_put").is_empty());
    let stats = db.statistics();
    assert_eq!(
        stats.operations[DbOp::Put as usize].load(Ordering::Relaxed),
        1
    );
    assert!(stats.wal_written_bytes.load(Ordering::Relaxed) > 0);
    assert!(stats.logical_write_bytes.load(Ordering::Relaxed) > 0);
    db.close().unwrap();
}
