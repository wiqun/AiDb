//! Bloom Filter 模块级 dataflow — SST read path bloom_check event
//! @component aidb-filter

use aidb::config::CompressionType;
use aidb::engine::memtable::{encode_internal_key, ValueType};
use aidb::engine::sstable::{sstable_path, SSTableBuilder, SSTableReader};
use tempfile::tempdir;

use crate::common::dataflow::capture_spans_under_lock;
use crate::common::observability::{capture_events_under_lock, tracing_test_lock};

fn build_bloom_sst(dir: &std::path::Path, file_num: u64) -> std::path::PathBuf {
    let path = sstable_path(dir, file_num, 0);
    let mut b = SSTableBuilder::new(&path, 256, 2, CompressionType::None, 0.01).unwrap();
    b.set_expected_keys(1);
    let ik = encode_internal_key(b"present", 1, ValueType::TypePut);
    b.add(&ik, b"v").unwrap();
    b.finish().unwrap();
    path
}

/// 验证 Bloom Filter 可观测性 (hit=false 假阳性过滤事件跟踪)
#[test]
fn test_filter_bloom_observability() {
    let _lock = tracing_test_lock();
    let dir = tempdir().unwrap();
    let path = build_bloom_sst(dir.path(), 1);
    let reader = SSTableReader::open(&path, None).unwrap();
    assert!(reader.has_bloom_filter());

    let present = encode_internal_key(b"present", 1, ValueType::TypePut);
    let caps = capture_spans_under_lock(|| {
        assert!(reader.get(&present).unwrap().is_some());
    });
    assert!(caps.spans_named("sst_seek").is_empty());

    let missing = encode_internal_key(b"absent", u64::MAX, ValueType::TypePut);
    let events = capture_events_under_lock(|| {
        assert_eq!(reader.get(&missing).unwrap(), None);
    });
    assert!(
        events.iter().any(|e| e.contains("hit=false")),
        "bloom negative path should log hit=false, got: {events:?}"
    );
}
