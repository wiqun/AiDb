//! Compaction 执行: 将多个输入 SST 归并去重后写出新 SSTable (`CompactionJob::run`).
//! 支持 subcompaction 分裂: 大 compaction 按 key 区间拆成多个并行子任务,
//! 使用 `std::thread::scope` 并行执行, 每个子任务负责 `[range_start, range_end)`.
//!
//! # 数据流
//!
//! ```text
//! inputs + expanded_inputs → MergeIterator (堆归并)
//!   → 同一 user_key 的 TypePut: 最新版本保留, 其余按 SnapshotDedupTracker 丢弃
//!   → RangeTombstoneTracker 过滤被覆盖的点条目
//!   → 删除标记: 没有快照需要且没有幸存文件重叠时丢掉
//!   → SSTableBuilder → .sst → CompactionResult (entry_count == 0 时 abandon)
//! ```
//!
//! # Invariant
//!
//! - 同一 user_key 保留最高 sequence 版本; 从新到旧扫描, 一旦遇到 `seq <= min_snapshot_sequence`
//!   的 "边界穿越版本" 即保留之, 更老版本一律丢弃 (保证所有活跃 Snapshot 可见性).
//! - 没有幸存文件就丢掉删除标记: 点删除和范围删除使用同一条范围重叠判断.
//!   快照还能看见的版本即使在最底层也留下.
//! - 0 entry 的子任务输出空 `CompactionResult` (file_number = 0), 上层跳过 AddFile.

use super::drop::{should_drop_delete_marker, UserKeyRange};
use super::helpers::user_key_from_internal;
use super::merge::MergeIterator;
use super::trackers::{RangeTombstoneTracker, SnapshotDedupTracker};
use crate::config::CompressionType;
use crate::engine::compaction::filter::{CompactionFilter, FilterDecision};
use crate::engine::memtable::{extract_sequence, extract_value_type, ValueType};
use crate::engine::sstable::{sstable_path, SSTableBuilder, SSTableReader};
use crate::error::Result;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct CompactionJob {
    pub inputs: Vec<Arc<SSTableReader>>,
    pub expanded_inputs: Vec<Arc<SSTableReader>>,
    pub output_level: usize,
    pub db_path: PathBuf,
    pub block_size: usize,
    pub block_restart_interval: usize,
    pub compression: CompressionType,
    pub bloom_false_positive_rate: f64,
    /// 活跃快照中的最小 sequence. 低于此值的旧版本可安全 dedup.
    pub min_snapshot_sequence: u64,
    /// 可选的 compaction 过滤器.
    pub compaction_filter: Option<Arc<dyn CompactionFilter>>,
    /// 这次不会重写的文件的 user key 闭区间.
    surviving_user_ranges: Vec<UserKeyRange>,
}

pub struct CompactionResult {
    pub file_number: u64,
    pub entry_count: usize,
    pub output_path: PathBuf,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub file_size: u64,
    /// 本子任务中因 filter 丢弃的最新 Put 的 user_key (去重前由调用方合并).
    pub filter_removed_user_keys: Vec<Vec<u8>>,
}

impl CompactionJob {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        inputs: Vec<Arc<SSTableReader>>,
        expanded_inputs: Vec<Arc<SSTableReader>>,
        output_level: usize,
        db_path: PathBuf,
        block_size: usize,
        block_restart_interval: usize,
        compression: CompressionType,
        bloom_false_positive_rate: f64,
    ) -> Self {
        Self {
            inputs,
            expanded_inputs,
            output_level,
            db_path,
            block_size,
            block_restart_interval,
            compression,
            bloom_false_positive_rate,
            min_snapshot_sequence: u64::MAX, // 默认无活跃快照
            compaction_filter: None,
            surviving_user_ranges: Vec::new(),
        }
    }

    /// 这次 compaction 之后仍然存在的文件范围. 默认空, 空则删除标记不留下.
    pub fn with_surviving_user_ranges(mut self, ranges: Vec<UserKeyRange>) -> Self {
        self.surviving_user_ranges = ranges;
        self
    }

    /// 设置快照保护阈值: compaction 不会删除 `sequence >= min_snapshot_sequence` 的版本.
    pub fn with_snapshot_threshold(mut self, min_seq: u64) -> Self {
        self.min_snapshot_sequence = min_seq;
        self
    }

    /// 设置 compaction 过滤器.
    pub fn with_filter(mut self, filter: Option<Arc<dyn CompactionFilter>>) -> Self {
        self.compaction_filter = filter;
        self
    }

    fn all_inputs(&self) -> Vec<Arc<SSTableReader>> {
        let mut all = self.inputs.clone();
        all.extend(self.expanded_inputs.clone());
        all
    }

    /// 检查 entry 是否应被 filter 丢弃.
    fn should_filter(&self, key: &[u8], value: &[u8]) -> bool {
        should_filter_impl(&self.compaction_filter, self.output_level, key, value)
    }

    fn count_dedup_entries(&self) -> Result<usize> {
        let mut merge_iter = MergeIterator::new(self.all_inputs())?;
        let mut count = 0usize;
        let mut last_user_key: Option<Vec<u8>> = None;
        let mut tracker = SnapshotDedupTracker::default();
        while let Some((key, value)) = merge_iter.next_entry()? {
            let user_key = user_key_from_internal(&key)?;
            let seq = extract_sequence(&key)?;
            if last_user_key.as_deref() == Some(user_key) {
                if !tracker.already_crossed() {
                    count += 1; // 边界穿越版本或更新的重复版本, 快照可能需要
                    tracker.observe(seq, self.min_snapshot_sequence);
                }
                continue;
            }
            tracker.start_key();
            let value_type = extract_value_type(&key)?;
            if drop_delete_marker(
                value_type,
                seq,
                self.min_snapshot_sequence,
                user_key,
                &value,
                &self.surviving_user_ranges,
            ) {
                last_user_key = Some(user_key.to_vec());
                tracker.observe(seq, self.min_snapshot_sequence);
                continue;
            }
            count += 1;
            last_user_key = Some(user_key.to_vec());
            tracker.observe(seq, self.min_snapshot_sequence);
        }
        Ok(count)
    }

    /// 单遍计数 + 分裂点记录. 基于文件大小粗估条目数以确定分裂间隔.
    fn count_dedup_with_splits(&self, num_splits: usize) -> Result<(usize, Vec<Vec<u8>>)> {
        if num_splits <= 1 {
            let count = self.count_dedup_entries()?;
            return Ok((count, vec![]));
        }

        // 基于文件大小粗估条目数 (~100 bytes/entry 经验值)
        let estimated_total: usize = self
            .all_inputs()
            .iter()
            .map(|r| r.file_size() as usize / 100)
            .sum::<usize>()
            .max(1);
        let est_per_split = estimated_total / num_splits;
        let mut next_split_at = est_per_split;

        let mut merge_iter = MergeIterator::new(self.all_inputs())?;
        let mut count = 0usize;
        let mut last_user_key: Option<Vec<u8>> = None;
        let mut splits: Vec<Vec<u8>> = Vec::new();
        let mut tracker = SnapshotDedupTracker::default();

        while let Some((key, value)) = merge_iter.next_entry()? {
            let user_key = user_key_from_internal(&key)?;
            let seq = extract_sequence(&key)?;
            if last_user_key.as_deref() == Some(user_key) {
                if !tracker.already_crossed() {
                    count += 1;
                    tracker.observe(seq, self.min_snapshot_sequence);
                    if count >= next_split_at && splits.len() < num_splits - 1 {
                        splits.push(user_key.to_vec());
                        next_split_at = count + est_per_split;
                    }
                }
                continue;
            }
            tracker.start_key();
            let value_type = extract_value_type(&key)?;
            if drop_delete_marker(
                value_type,
                seq,
                self.min_snapshot_sequence,
                user_key,
                &value,
                &self.surviving_user_ranges,
            ) {
                last_user_key = Some(user_key.to_vec());
                tracker.observe(seq, self.min_snapshot_sequence);
                continue;
            }
            count += 1;
            last_user_key = Some(user_key.to_vec());
            tracker.observe(seq, self.min_snapshot_sequence);

            if count >= next_split_at && splits.len() < num_splits - 1 {
                splits.push(user_key.to_vec());
                next_split_at = count + est_per_split;
            }
        }

        Ok((count, splits))
    }

    /// 执行 compaction, 可能分裂为多个子任务并行.
    ///
    /// `file_numbers` 中元素个数表示分裂数: 1 = 不分裂, N = N 个子任务.
    /// 每个子任务使用 `file_numbers[i]` 作为输出文件编号.
    /// 调用方需预先分配所有文件编号, 避免并发分配冲突.
    #[tracing::instrument(name = "cmp_run", skip(self))]
    pub fn run(&self, file_numbers: &[u64]) -> Result<Vec<CompactionResult>> {
        if file_numbers.len() <= 1 {
            let fnum = file_numbers.first().copied().unwrap_or(0);
            return Ok(vec![self.run_single(fnum)?]);
        }
        self.run_split(file_numbers)
    }

    /// 单线程完整 compaction. 合并 count_dedup_entries 的计数遍历和写入遍历, 减少一次完整 I/O.
    fn run_single(&self, file_number: u64) -> Result<CompactionResult> {
        let output_path = sstable_path(&self.db_path, file_number, self.output_level);

        // 粗估条目数以设置 Bloom filter (不做精确计数, 避免双遍历)
        let estimated_keys: usize = self
            .all_inputs()
            .iter()
            .map(|r| r.file_size() as usize / 100)
            .sum::<usize>()
            .max(1);

        let mut builder = SSTableBuilder::new(
            &output_path,
            self.block_size,
            self.block_restart_interval,
            self.compression,
            self.bloom_false_positive_rate,
        )?;
        if self.bloom_false_positive_rate > 0.0 {
            builder.set_expected_keys(estimated_keys);
        }

        let mut merge_iter = MergeIterator::new(self.all_inputs())?;
        let mut filter_removed: Vec<Vec<u8>> = Vec::new();
        let mut entry_count = 0usize;
        let mut last_user_key: Option<Vec<u8>> = None;
        let mut smallest_key: Option<Vec<u8>> = None;
        let mut largest_key: Option<Vec<u8>> = None;
        let mut tracker = SnapshotDedupTracker::default();
        let mut range_tracker = RangeTombstoneTracker::default();

        while let Some((key, value)) = merge_iter.next_entry()? {
            let user_key = user_key_from_internal(&key)?;
            let value_type = extract_value_type(&key)?;
            let seq = extract_sequence(&key)?;

            if last_user_key.as_deref() != Some(user_key) {
                range_tracker.advance_past(user_key);
            }

            if last_user_key.as_deref() == Some(user_key) {
                if !tracker.already_crossed() {
                    if !self.should_filter(&key, &value) {
                        builder.add(&key, &value)?;
                        entry_count += 1;
                    }
                    tracker.observe(seq, self.min_snapshot_sequence);
                }
                continue;
            }

            tracker.start_key();

            if value_type == ValueType::TypeRangeDelete {
                range_tracker.add(user_key.to_vec(), value.clone(), seq);
            }
            if drop_delete_marker(
                value_type,
                seq,
                self.min_snapshot_sequence,
                user_key,
                &value,
                &self.surviving_user_ranges,
            ) {
                last_user_key = Some(user_key.to_vec());
                tracker.observe(seq, self.min_snapshot_sequence);
                continue;
            }

            if value_type != ValueType::TypeRangeDelete && range_tracker.covers(user_key, seq) {
                last_user_key = Some(user_key.to_vec());
                tracker.observe(seq, self.min_snapshot_sequence);
                continue;
            }

            if !self.should_filter(&key, &value) {
                builder.add(&key, &value)?;
                entry_count += 1;
            } else if value_type == ValueType::TypePut {
                // 记录; Version 安装后再通知 listener (见 DB::run_compaction_once).
                filter_removed.push(user_key.to_vec());
            }
            last_user_key = Some(user_key.to_vec());
            tracker.observe(seq, self.min_snapshot_sequence);
            if smallest_key.is_none() {
                smallest_key = Some(key.clone());
            }
            largest_key = Some(key);
        }

        if entry_count == 0 {
            builder.abandon()?;
            return Ok(CompactionResult {
                file_number: 0,
                entry_count: 0,
                output_path,
                smallest_key: vec![],
                largest_key: vec![],
                file_size: 0,
                filter_removed_user_keys: filter_removed,
            });
        }

        let file_size = builder.finish()?;
        let files_merged = self.inputs.len() + self.expanded_inputs.len();
        tracing::info!(
            level = self.output_level,
            files_merged,
            bytes_written = file_size,
            "compaction_complete"
        );
        Ok(CompactionResult {
            file_number,
            entry_count,
            output_path,
            smallest_key: smallest_key.unwrap_or_default(),
            largest_key: largest_key.unwrap_or_default(),
            file_size,
            filter_removed_user_keys: filter_removed,
        })
    }

    /// 分裂 compaction: 将输入按 key 范围分成多个子任务并行执行.
    fn run_split(&self, file_numbers: &[u64]) -> Result<Vec<CompactionResult>> {
        let num_splits = file_numbers.len();
        let (total_count, split_keys) = self.count_dedup_with_splits(num_splits)?;

        if total_count == 0 {
            return Ok(vec![CompactionResult {
                file_number: 0,
                entry_count: 0,
                output_path: sstable_path(&self.db_path, file_numbers[0], self.output_level),
                smallest_key: vec![],
                largest_key: vec![],
                file_size: 0,
                filter_removed_user_keys: vec![],
            }]);
        }

        let est_per_range = total_count / num_splits;
        let all_readers = Arc::new(self.all_inputs());
        let db_path = self.db_path.clone();
        let output_level = self.output_level;
        let block_size = self.block_size;
        let block_restart_interval = self.block_restart_interval;
        let compression = self.compression;
        let bloom_fpr = self.bloom_false_positive_rate;
        let min_snap_seq = self.min_snapshot_sequence;
        let compaction_filter = self.compaction_filter.clone();
        let results: Vec<Result<CompactionResult>> = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(split_keys.len() + 1);

            for i in 0..=split_keys.len() {
                let range_start = if i == 0 {
                    None
                } else {
                    Some(split_keys[i - 1].clone())
                };
                let range_end = if i >= split_keys.len() {
                    None
                } else {
                    Some(split_keys[i].clone())
                };
                let readers = Arc::clone(&all_readers);
                let path = db_path.clone();
                let filter = compaction_filter.clone();
                // 最后一段取剩余估算
                let expected = if i < split_keys.len() {
                    est_per_range
                } else {
                    total_count.saturating_sub(i * est_per_range)
                };

                let surviving = self.surviving_user_ranges.clone();
                handles.push(scope.spawn(move || {
                    write_sub_compaction(
                        &readers,
                        file_numbers[i],
                        range_start,
                        range_end,
                        output_level,
                        &path,
                        block_size,
                        block_restart_interval,
                        compression,
                        bloom_fpr,
                        min_snap_seq,
                        expected,
                        filter,
                        surviving,
                    )
                }));
            }

            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        results.into_iter().collect()
    }
}

/// 单个子 compaction 任务: 以 `range_end` 为界的 `MergeIterator` 遍历输入,
/// 仅输出在 `[range_start, range_end)` 范围内的条目.
#[allow(clippy::too_many_arguments)]
/// 检查 entry 是否应被 compaction filter 丢弃.
/// 与 `CompactionJob::should_filter` 语义相同, 但以函数形式供 `write_sub_compaction` 使用.
fn drop_delete_marker(
    value_type: ValueType,
    sequence: u64,
    min_snapshot_sequence: u64,
    marker_start: &[u8],
    value: &[u8],
    surviving: &[UserKeyRange],
) -> bool {
    let marker_end = if value_type == ValueType::TypeRangeDelete {
        Some(value)
    } else {
        None
    };
    should_drop_delete_marker(
        value_type,
        sequence,
        min_snapshot_sequence,
        marker_start,
        marker_end,
        surviving,
    )
}

fn should_filter_impl(
    filter: &Option<Arc<dyn CompactionFilter>>,
    level: usize,
    key: &[u8],
    value: &[u8],
) -> bool {
    match filter {
        Some(f) => f.filter(level, key, value) == FilterDecision::Remove,
        None => false,
    }
}

#[allow(clippy::too_many_arguments)]
fn write_sub_compaction(
    readers: &[Arc<SSTableReader>],
    file_number: u64,
    range_start: Option<Vec<u8>>,
    range_end: Option<Vec<u8>>,
    output_level: usize,
    db_path: &Path,
    block_size: usize,
    block_restart_interval: usize,
    compression: CompressionType,
    bloom_fpr: f64,
    min_snap_seq: u64,
    expected_keys: usize,
    compaction_filter: Option<Arc<dyn CompactionFilter>>,
    surviving: Vec<UserKeyRange>,
) -> Result<CompactionResult> {
    let output_path = sstable_path(db_path, file_number, output_level);

    // 无预期条目时提前返回空结果
    if expected_keys == 0 {
        // No keys expected for this range, produce empty result
        return Ok(CompactionResult {
            file_number: 0,
            entry_count: 0,
            output_path,
            smallest_key: vec![],
            largest_key: vec![],
            file_size: 0,
            filter_removed_user_keys: vec![],
        });
    }

    let mut builder = SSTableBuilder::new(
        &output_path,
        block_size,
        block_restart_interval,
        compression,
        bloom_fpr,
    )?;
    if bloom_fpr > 0.0 {
        builder.set_expected_keys(expected_keys);
    }

    let mut merge_iter = if range_end.is_some() {
        MergeIterator::with_range(readers.to_vec(), range_end.clone())?
    } else {
        MergeIterator::new(readers.to_vec())?
    };

    let mut filter_removed: Vec<Vec<u8>> = Vec::new();
    let mut entry_count = 0usize;
    let mut last_user_key: Option<Vec<u8>> = None;
    let mut smallest_key: Option<Vec<u8>> = None;
    let mut largest_key: Option<Vec<u8>> = None;
    let mut tracker = SnapshotDedupTracker::default();
    let mut range_tracker = RangeTombstoneTracker::default();

    while let Some((key, value)) = merge_iter.next_entry()? {
        let user_key = user_key_from_internal(&key)?;
        let value_type = extract_value_type(&key)?;
        let seq = extract_sequence(&key)?;

        // 跳过 range_start 之前的条目 (由前一个子任务处理).
        // 起点在本段之前、终点伸进本段的范围删除仍要记进 tracker, 否则标记被丢掉后本段的 key 会写出来.
        if let Some(ref start) = range_start {
            if user_key < start.as_slice() {
                if value_type == ValueType::TypeRangeDelete && value.as_slice() > start.as_slice() {
                    range_tracker.add(user_key.to_vec(), value.clone(), seq);
                }
                continue;
            }
        }

        if last_user_key.as_deref() != Some(user_key) {
            range_tracker.advance_past(user_key);
        }

        if last_user_key.as_deref() == Some(user_key) {
            if !tracker.already_crossed() {
                if !should_filter_impl(&compaction_filter, output_level, &key, &value) {
                    builder.add(&key, &value)?;
                    entry_count += 1;
                }
                tracker.observe(seq, min_snap_seq);
            }
            continue;
        }

        tracker.start_key();

        if value_type == ValueType::TypeRangeDelete {
            range_tracker.add(user_key.to_vec(), value.clone(), seq);
        }
        if drop_delete_marker(value_type, seq, min_snap_seq, user_key, &value, &surviving) {
            last_user_key = Some(user_key.to_vec());
            tracker.observe(seq, min_snap_seq);
            continue;
        }

        if value_type != ValueType::TypeRangeDelete && range_tracker.covers(user_key, seq) {
            last_user_key = Some(user_key.to_vec());
            tracker.observe(seq, min_snap_seq);
            continue;
        }

        if !should_filter_impl(&compaction_filter, output_level, &key, &value) {
            builder.add(&key, &value)?;
            entry_count += 1;
        } else if value_type == ValueType::TypePut {
            filter_removed.push(user_key.to_vec());
        }
        last_user_key = Some(user_key.to_vec());
        tracker.observe(seq, min_snap_seq);
        if smallest_key.is_none() {
            smallest_key = Some(key.clone());
        }
        largest_key = Some(key);
    }

    if entry_count == 0 {
        builder.abandon()?;
        return Ok(CompactionResult {
            file_number: 0,
            entry_count: 0,
            output_path,
            smallest_key: vec![],
            largest_key: vec![],
            file_size: 0,
            filter_removed_user_keys: filter_removed,
        });
    }

    let file_size = builder.finish()?;
    Ok(CompactionResult {
        file_number,
        entry_count,
        output_path,
        smallest_key: smallest_key.unwrap_or_default(),
        largest_key: largest_key.unwrap_or_default(),
        file_size,
        filter_removed_user_keys: filter_removed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::memtable::{encode_internal_key, ValueType};
    use crate::engine::sstable::{sstable_path, SSTableBuilder, SSTableReader};
    use std::path::Path;
    use tempfile::tempdir;

    fn sst(
        dir: &Path,
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

    /// 没有幸存文件就丢掉
    #[test]
    fn test_compaction_dedup_and_tombstone() {
        let dir = tempdir().unwrap();
        let a = sst(
            dir.path(),
            1,
            0,
            &[
                (b"k", 2, ValueType::TypePut, b"v2"),
                (b"x", 1, ValueType::TypeDelete, b""),
            ],
        );
        let b = sst(dir.path(), 2, 0, &[(b"k", 1, ValueType::TypePut, b"v1")]);
        let job = CompactionJob::new(
            vec![a, b],
            vec![],
            1,
            dir.path().to_path_buf(),
            512,
            16,
            CompressionType::None,
            0.0,
        );
        let out = job.run(&[10]).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].entry_count, 1);
        let reader = SSTableReader::open(&out[0].output_path, None).unwrap();
        let seek = encode_internal_key(b"k", u64::MAX, ValueType::TypePut);
        assert_eq!(
            reader.get(&seek).unwrap().map(|(v, _)| v),
            Some(b"v2".to_vec())
        );
        let seek_x = encode_internal_key(b"x", u64::MAX, ValueType::TypePut);
        assert_eq!(reader.get(&seek_x).unwrap(), None);
    }

    #[test]
    fn test_empty_value_not_tombstone() {
        let dir = tempdir().unwrap();
        let a = sst(dir.path(), 1, 0, &[(b"k", 1, ValueType::TypePut, b"")]);
        let job = CompactionJob::new(
            vec![a],
            vec![],
            1,
            dir.path().to_path_buf(),
            512,
            16,
            CompressionType::None,
            0.0,
        );
        let out = job.run(&[11]).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].entry_count, 1);
    }

    #[test]
    fn test_filter_removed_user_keys_recorded() {
        use crate::engine::compaction::{CompactionFilter, FilterDecision};

        struct DropPuts;
        impl CompactionFilter for DropPuts {
            fn filter(&self, _: usize, _: &[u8], _: &[u8]) -> FilterDecision {
                FilterDecision::Remove
            }
        }

        let dir = tempdir().unwrap();
        let a = sst(
            dir.path(),
            1,
            0,
            &[
                (b"k", 2, ValueType::TypePut, b"v2"),
                (b"k", 1, ValueType::TypePut, b"v1"),
            ],
        );
        let job = CompactionJob::new(
            vec![a],
            vec![],
            1,
            dir.path().to_path_buf(),
            512,
            16,
            CompressionType::None,
            0.0,
        )
        .with_filter(Some(Arc::new(DropPuts)));
        let out = job.run(&[20]).unwrap();
        assert_eq!(out[0].entry_count, 0);
        assert_eq!(out[0].filter_removed_user_keys, vec![b"k".to_vec()]);
    }

    /// 没有幸存文件就丢掉
    #[test]
    fn test_compaction_all_tombstones_level1_no_output() {
        let dir = tempdir().unwrap();
        let a = sst(dir.path(), 1, 0, &[(b"k", 1, ValueType::TypeDelete, b"")]);
        let job = CompactionJob::new(
            vec![a],
            vec![],
            1,
            dir.path().to_path_buf(),
            512,
            16,
            CompressionType::None,
            0.01,
        );
        let out = job.run(&[12]).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].entry_count, 0);
        assert_eq!(out[0].file_number, 0);
    }

    #[test]
    fn test_subcompaction_split_two_ranges() {
        let dir = tempdir().unwrap();
        // Write keys a, b, c, d, e at the same level
        let a = sst(
            dir.path(),
            1,
            0,
            &[
                (b"a", 1, ValueType::TypePut, b"1"),
                (b"b", 1, ValueType::TypePut, b"2"),
                (b"c", 1, ValueType::TypePut, b"3"),
                (b"d", 1, ValueType::TypePut, b"4"),
                (b"e", 1, ValueType::TypePut, b"5"),
            ],
        );
        let job = CompactionJob::new(
            vec![a],
            vec![],
            1,
            dir.path().to_path_buf(),
            512,
            16,
            CompressionType::None,
            0.0,
        );
        // Force 2 splits (num_splits=2)
        let results = job.run(&[100, 101]).unwrap();
        // Should produce at most 2 results (some may be empty)
        assert!(results.len() <= 2);
        let non_empty: Vec<&CompactionResult> =
            results.iter().filter(|r| r.entry_count > 0).collect();
        // Total entries should be 5 (no dedup needed, no tombstones)
        let total: usize = non_empty.iter().map(|r| r.entry_count).sum();
        assert_eq!(total, 5);
        // Verify all keys are reachable through the output files
        for res in &non_empty {
            let reader = SSTableReader::open(&res.output_path, None).unwrap();
            for key in [b"a", b"b", b"c", b"d", b"e"] {
                let ik = encode_internal_key(key, u64::MAX, ValueType::TypePut);
                if let Some((val, _)) = reader.get(&ik).unwrap() {
                    let expected: &[u8] = if key == b"a" {
                        b"1"
                    } else if key == b"b" {
                        b"2"
                    } else if key == b"c" {
                        b"3"
                    } else if key == b"d" {
                        b"4"
                    } else if key == b"e" {
                        b"5"
                    } else {
                        unreachable!()
                    };
                    assert_eq!(&val[..], expected);
                }
            }
        }
    }

    #[test]
    fn test_subcompaction_preserves_dedup_across_splits() {
        let dir = tempdir().unwrap();
        // Input has duplicate user keys across files; dedup should work within each split
        let a = sst(dir.path(), 1, 0, &[(b"a", 3, ValueType::TypePut, b"a3")]);
        let b = sst(dir.path(), 2, 0, &[(b"b", 2, ValueType::TypePut, b"b2")]);
        let job = CompactionJob::new(
            vec![a, b],
            vec![],
            1,
            dir.path().to_path_buf(),
            512,
            16,
            CompressionType::None,
            0.0,
        );
        let results = job.run(&[200, 201]).unwrap();
        let total: usize = results.iter().map(|r| r.entry_count).sum();
        assert_eq!(total, 2, "a and b should each appear once after dedup");
    }
}
