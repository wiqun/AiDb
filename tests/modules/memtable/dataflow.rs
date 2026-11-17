//! MemTable 模块级 dataflow — API 状态与无热路径 Span 契约
//! @component aidb-memtable

use crate::common::dataflow::capture_spans_under_lock;
use crate::common::observability::tracing_test_lock;
use aidb::engine::memtable::MemTable;

/// 验证 MemTable 热路径不产生 Span，且读写/删除/冻结状态正确
#[test]
fn test_mem_observability() {
    let _lock = tracing_test_lock();
    let mt = MemTable::new();

    assert!(capture_spans_under_lock(|| mt.put(b"k", b"v", 1).unwrap())
        .spans_named("mem_put")
        .is_empty());
    assert!(capture_spans_under_lock(|| {
        assert!(mt.get_latest(b"k").unwrap().is_some());
    })
    .spans_named("mem_get")
    .is_empty());
    assert!(capture_spans_under_lock(|| mt.delete(b"k", 2).unwrap())
        .spans_named("mem_delete")
        .is_empty());
    assert!(capture_spans_under_lock(|| {
        let _ = mt.freeze(1);
    })
    .spans_named("mem_freeze")
    .is_empty());

    let mt2 = MemTable::new();
    mt2.put(b"k", b"v", 1).unwrap();
    assert!(mt2.get_latest(b"k").unwrap().is_some());
    assert!(mt2.get_latest(b"missing").unwrap().is_none());
    mt2.delete(b"k2", 3).unwrap();
    let frozen = mt2.freeze(1);
    assert_eq!(
        frozen.get_latest(b"k").unwrap().as_deref(),
        Some(b"v".as_slice())
    );
}
