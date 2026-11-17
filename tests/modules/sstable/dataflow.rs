//! SSTable 模块级可观测性 — 无热路径 Span 契约与产物校验
//! @component aidb-sstable

use aidb::config::CompressionType;
use aidb::engine::memtable::{encode_internal_key, ValueType};
use aidb::engine::sstable::{sstable_path, SSTableBuilder, SSTableReader};
use tempfile::tempdir;

use crate::common::dataflow::capture_spans_under_lock;
use crate::common::observability::tracing_test_lock;

/// 验证 SSTable 构建与读取不产生热路径 Span 且产物可正常读回
#[test]
fn test_sst_observability() {
    let _lock = tracing_test_lock();
    let dir = tempdir().unwrap();
    let path = sstable_path(dir.path(), 1, 0);

    let mut b = SSTableBuilder::new(&path, 4096, 16, CompressionType::None, 0.0).unwrap();
    let k = encode_internal_key(b"obs", 1, ValueType::TypePut);
    let caps = capture_spans_under_lock(|| {
        b.add(&k, b"v").unwrap();
    });
    assert!(caps.spans_named("sst_build_add").is_empty());

    let finish_caps = capture_spans_under_lock(|| {
        b.finish().unwrap();
    });
    assert!(finish_caps.spans_named("sst_build_finish").is_empty());

    let r = SSTableReader::open(&path, None).unwrap();
    let seek = encode_internal_key(b"obs", 1, ValueType::TypePut);
    let read_caps = capture_spans_under_lock(|| {
        assert!(r.get(&seek).unwrap().is_some());
    });
    assert!(read_caps.spans_named("sst_seek").is_empty());
    assert!(read_caps.spans_named("sst_block_read").is_empty());
    assert!(std::fs::metadata(&path).unwrap().len() > 0);
}
