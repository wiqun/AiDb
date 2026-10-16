//! Compaction picker / job / merge 测试.
//! @component aidb-compaction

use aidb::config::CompressionType;
use aidb::config::Options;
use aidb::engine::compaction::{
    user_key_from_internal, CompactionJob, CompactionPicker, MergeIterator, UserKeyRange,
};
use aidb::engine::memtable::extract_value_type;
use aidb::engine::memtable::{encode_internal_key, ValueType};
use aidb::engine::sstable::{sstable_path, SSTableBuilder, SSTableReader};
use std::sync::Arc;
use tempfile::tempdir;

fn sst(
    dir: &std::path::Path,
    num: u64,
    level: usize,
    entries: &[(&[u8], u64, ValueType, &[u8])],
) -> Arc<SSTableReader> {
    let path = sstable_path(dir, num, level);
    let mut b = SSTableBuilder::new(&path, 512, 16, CompressionType::None, 0.0).unwrap();
    for (uk, seq, ty, val) in entries {
        b.add(&encode_internal_key(uk, *seq, *ty), val).unwrap();
    }
    b.finish().unwrap();
    Arc::new(SSTableReader::open(&path, None).unwrap())
}

/// 验证 MergeIterator 处理跨 SSTable 重叠 Key 的版本合并
#[test]
fn test_merge_iterator_overlapping_keys() {
    let dir = tempdir().unwrap();
    let low = sst(dir.path(), 1, 0, &[(b"k", 1, ValueType::TypePut, b"old")]);
    let high = sst(dir.path(), 2, 0, &[(b"k", 2, ValueType::TypePut, b"new")]);
    let mut it = MergeIterator::new(vec![low, high]).unwrap();
    let first = it.next_entry().unwrap().unwrap();
    assert_eq!(first.1, b"new");
}

/// 没有幸存文件就丢掉
#[test]
fn test_compaction_job_removes_tombstone_at_level1() {
    let dir = tempdir().unwrap();
    let input = sst(
        dir.path(),
        1,
        0,
        &[
            (b"a", 1, ValueType::TypePut, b"1"),
            (b"b", 1, ValueType::TypeDelete, b""),
        ],
    );
    let job = CompactionJob::new(
        vec![input],
        vec![],
        1,
        dir.path().to_path_buf(),
        512,
        16,
        CompressionType::None,
        0.0,
    );
    let results = job.run(&[10]).unwrap();
    assert_eq!(results[0].entry_count, 1);
}

#[test]
fn test_pick_level0_trigger() {
    let dir = tempdir().unwrap();
    let mut opts = Options::for_testing();
    opts.level0_compaction_trigger = 2;
    let picker = CompactionPicker::from_options(&opts);
    let mut levels: Vec<Vec<Arc<SSTableReader>>> = vec![Vec::new(); opts.max_levels];
    levels[0].push(sst(
        dir.path(),
        1,
        0,
        &[(b"a", 1, ValueType::TypePut, b"v")],
    ));
    levels[0].push(sst(
        dir.path(),
        2,
        0,
        &[(b"b", 1, ValueType::TypePut, b"v")],
    ));
    assert!(picker.pick_compaction(&levels).is_some());
}

#[test]
fn test_compaction_output_has_bloom() {
    let dir = tempdir().unwrap();
    let input = sst(
        dir.path(),
        1,
        0,
        &[
            (b"a", 1, ValueType::TypePut, b"v1"),
            (b"b", 1, ValueType::TypePut, b"v2"),
        ],
    );
    let job = CompactionJob::new(
        vec![input],
        vec![],
        1,
        dir.path().to_path_buf(),
        512,
        16,
        CompressionType::None,
        0.01,
    );
    let results = job.run(&[20]).unwrap();
    assert_eq!(results[0].entry_count, 2);
    let reader = SSTableReader::open(&results[0].output_path, None).unwrap();
    assert!(reader.has_bloom_filter());
}

#[test]
fn test_level0_output_retains_tombstone() {
    let dir = tempdir().unwrap();
    let input = sst(
        dir.path(),
        1,
        0,
        &[
            (b"a", 1, ValueType::TypePut, b"1"),
            (b"b", 1, ValueType::TypeDelete, b""),
        ],
    );
    let job = CompactionJob::new(
        vec![input],
        vec![],
        0,
        dir.path().to_path_buf(),
        512,
        16,
        CompressionType::None,
        0.0,
    );
    let results = job.run(&[10]).unwrap();
    assert_eq!(results[0].entry_count, 1);
    assert!(!results[0].smallest_key.is_empty());
    assert!(!results[0].largest_key.is_empty());
}

fn level1_job(dir: &std::path::Path, input: Arc<SSTableReader>) -> CompactionJob {
    CompactionJob::new(
        vec![input],
        vec![],
        1,
        dir.to_path_buf(),
        512,
        16,
        CompressionType::None,
        0.0,
    )
}

fn output_markers(path: &std::path::Path) -> Vec<(Vec<u8>, ValueType, Vec<u8>)> {
    let reader = Arc::new(SSTableReader::open(path, None).unwrap());
    let mut it = MergeIterator::new(vec![reader]).unwrap();
    let mut out = Vec::new();
    while let Some((key, value)) = it.next_entry().unwrap() {
        let ty = extract_value_type(&key).unwrap();
        let uk = user_key_from_internal(&key).unwrap().to_vec();
        out.push((uk, ty, value));
    }
    out
}

#[test]
fn delete_marker_stays_when_surviving_file_overlaps() {
    let dir = tempdir().unwrap();
    let input = sst(dir.path(), 1, 0, &[(b"b", 2, ValueType::TypeDelete, b"")]);
    let job = level1_job(dir.path(), input).with_surviving_user_ranges(vec![UserKeyRange {
        start: b"b".to_vec(),
        end: b"m".to_vec(),
    }]);
    let results = job.run(&[10]).unwrap();
    assert_eq!(results[0].entry_count, 1);
    let markers = output_markers(&results[0].output_path);
    assert_eq!(markers.len(), 1);
    assert_eq!(markers[0].0, b"b");
    assert_eq!(markers[0].1, ValueType::TypeDelete);
}

#[test]
fn delete_marker_range_stays_when_only_the_tail_overlaps() {
    let dir = tempdir().unwrap();
    let input = sst(
        dir.path(),
        1,
        0,
        &[(b"b", 1, ValueType::TypeRangeDelete, b"z")],
    );
    let job = level1_job(dir.path(), input).with_surviving_user_ranges(vec![UserKeyRange {
        start: b"m".to_vec(),
        end: b"m".to_vec(),
    }]);
    let results = job.run(&[10]).unwrap();
    assert_eq!(results[0].entry_count, 1);
    let markers = output_markers(&results[0].output_path);
    assert_eq!(markers.len(), 1);
    assert_eq!(markers[0].0, b"b");
    assert_eq!(markers[0].1, ValueType::TypeRangeDelete);
    assert_eq!(markers[0].2, b"z");
}

#[test]
fn delete_marker_drops_when_nothing_survives() {
    let dir = tempdir().unwrap();
    let input = sst(
        dir.path(),
        1,
        0,
        &[
            (b"b", 2, ValueType::TypeDelete, b""),
            (b"b", 1, ValueType::TypeRangeDelete, b"z"),
        ],
    );
    let results = level1_job(dir.path(), input).run(&[10]).unwrap();
    assert_eq!(results[0].entry_count, 0);
}

#[test]
fn delete_marker_stays_when_newer_than_snapshot() {
    let dir = tempdir().unwrap();
    let input = sst(
        dir.path(),
        1,
        0,
        &[
            (b"b", 2, ValueType::TypeDelete, b""),
            (b"b", 1, ValueType::TypePut, b"old"),
        ],
    );
    let job = level1_job(dir.path(), input).with_snapshot_threshold(1);
    let results = job.run(&[10]).unwrap();
    assert_eq!(results[0].entry_count, 2);
    let markers = output_markers(&results[0].output_path);
    assert!(markers
        .iter()
        .any(|(uk, ty, _)| uk == b"b" && *ty == ValueType::TypeDelete));
    assert!(markers
        .iter()
        .any(|(uk, ty, val)| uk == b"b" && *ty == ValueType::TypePut && val == b"old"));
}

#[test]
fn dropped_range_delete_hides_key_in_later_split() {
    let dir = tempdir().unwrap();
    let input = sst(
        dir.path(),
        1,
        0,
        &[
            (b"a", 2, ValueType::TypeRangeDelete, b"z"),
            (b"m", 1, ValueType::TypePut, b"v"),
        ],
    );
    let results = level1_job(dir.path(), input).run(&[10, 11]).unwrap();
    for result in &results {
        if result.entry_count == 0 {
            continue;
        }
        let markers = output_markers(&result.output_path);
        assert!(
            !markers
                .iter()
                .any(|(uk, ty, _)| uk == b"m" && *ty == ValueType::TypePut),
            "later split wrote the covered put"
        );
    }
}

#[test]
fn delete_marker_bottom_keeps_when_snapshot_can_see() {
    let dir = tempdir().unwrap();
    let input = sst(dir.path(), 1, 0, &[(b"b", 4, ValueType::TypeDelete, b"")]);
    let job = level1_job(dir.path(), input).with_snapshot_threshold(4);
    let results = job.run(&[10]).unwrap();
    assert_eq!(results[0].entry_count, 1);
}
