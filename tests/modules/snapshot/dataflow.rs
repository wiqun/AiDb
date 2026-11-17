//! Snapshot 可观测性: 无热路径 Span 契约与快照隔离读校验
//! @component aidb-engine

use tempfile::tempdir;

use crate::common::dataflow::capture_spans_under_lock;
use crate::common::observability::tracing_test_lock;

use super::common::snapshot_opts;
use aidb::DB;

/// 验证 Snapshot 创建不产生热路径 Span 且保持快照隔离语义
#[test]
fn test_snapshot_observability() {
    let _lock = tracing_test_lock();
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), snapshot_opts()).unwrap();
    db.put(b"k", b"v1").unwrap();

    let mut snap_opt = None;
    let caps = capture_spans_under_lock(|| {
        snap_opt = Some(db.snapshot().unwrap());
    });
    assert!(caps.spans_named("db_snapshot").is_empty());

    let snap = snap_opt.unwrap();
    db.put(b"k", b"v2").unwrap();
    assert_eq!(snap.get(b"k").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(db.get(b"k").unwrap(), Some(b"v2".to_vec()));
    drop(snap);
    db.close().unwrap();
}
