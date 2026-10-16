//! MemTable 模块级 dataflow — API → span / event
//! @component aidb-memtable

use crate::common::dataflow::capture_spans_under_lock;
use crate::common::observability::{capture_events_under_lock, tracing_test_lock};
use aidb::engine::memtable::MemTable;

/// 验证 MemTable API 的 Observability span 与 event 追踪日志顺序
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

    let events = capture_events_under_lock(|| {
        let mt2 = MemTable::new();
        mt2.put(b"k", b"v", 1).unwrap();
        assert!(mt2.get_latest(b"k").unwrap().is_some());
        assert!(mt2.get_latest(b"missing").unwrap().is_none());
        mt2.delete(b"k2", 3).unwrap();
        let _frozen = mt2.freeze(1);
    });
    for needle in [
        "mem.put",
        "mem.get.hit",
        "mem.get.miss",
        "mem.delete",
        "mem.freeze",
    ] {
        assert!(
            events.iter().any(|e| e.contains(needle)),
            "missing event containing {needle:?}, got: {events:?}"
        );
    }
}
