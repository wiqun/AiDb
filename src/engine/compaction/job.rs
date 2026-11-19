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

use super::drop::{overlaps_surviving, should_drop_delete_marker, UserKeyRange};
use super::helpers::user_key_from_internal;
use super::merge::MergeIterator;
use super::trackers::{RangeTombstoneTracker, SnapshotDedupTracker};
use crate::config::CompressionType;
use crate::engine::compaction::filter::{CompactionFilter, CompactionPointReader, FilterDecision};
use crate::engine::memtable::{
    compare_internal_key, encode_internal_key, extract_sequence, extract_value_type, ValueType,
};
use crate::engine::sstable::{sstable_path, SSTableBuilder, SSTableReader};
use crate::error::Result;
use std::cmp::Ordering;
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
    /// 输出单个 SST 目标大小 (默认 64 MiB).
    target_file_size: u64,
    /// 与祖父层最大允许重叠字节数 (默认 25 * 64 MiB).
    max_compaction_bytes: u64,
    /// 与输出范围重叠的祖父层文件 (按 smallest_key 升序).
    grandparents: Vec<Arc<SSTableReader>>,
}

pub struct CompactionResult {
    pub file_number: u64,
    pub entry_count: usize,
    pub output_path: PathBuf,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub file_size: u64,
    /// 输出 SST 中 `\x00raft/` 前缀条目的 key + value 字节数.
    pub raft_written_bytes: u64,
    pub filter_removed_user_keys: Vec<Vec<u8>>,
}

/// 切分点候选样本上限. 每个样本约 32 字节, 1024 个约 32KB.
/// 样本按输出序号均匀分布, 步长在超限时翻倍, 因此粒度变粗但内存有界.
const SPLIT_SAMPLE_CAP: usize = 1024;

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
            target_file_size: 64 * 1024 * 1024,
            max_compaction_bytes: 25 * 64 * 1024 * 1024,
            grandparents: Vec::new(),
        }
    }

    /// 设置输出单个 SST 目标字节数.
    pub fn with_target_file_size(mut self, target_file_size: u64) -> Self {
        self.target_file_size = target_file_size.max(1);
        self
    }

    /// 设置祖父层最大允许重叠字节数 (`ShouldStopBefore` 阈值).
    pub fn with_max_compaction_bytes(mut self, max_compaction_bytes: u64) -> Self {
        self.max_compaction_bytes = max_compaction_bytes.max(1);
        self
    }

    /// 设置祖父层重叠文件列表 (按 smallest_key 升序).
    pub fn with_grandparents(mut self, grandparents: Vec<Arc<SSTableReader>>) -> Self {
        self.grandparents = grandparents;
        self
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

    /// 写入路径用的 filter + 点查句柄. reader 为 `None` 时等价于旧的纯 `filter`.
    fn filter_ctx<'a>(&'a self, reader: Option<&'a dyn CompactionPointReader>) -> FilterCtx<'a> {
        FilterCtx {
            filter: self.compaction_filter.as_deref(),
            reader,
        }
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

    /// 单遍计数 + 分裂点记录.
    ///
    /// 切分点不能按 "估算条目数 / num_splits" 当间隔放: 条目数只能从 SST 文件
    /// 大小粗估, 平均记录远大于 200 字节时会高估一个数量级, 间隔永远够不到,
    /// `splits` 为空 —— `num_splits` 报 2 却只有 1 个输出文件, 切分静默退化成
    /// 单写手, 还白多一遍计数扫描.
    ///
    /// 改成单遍扫描时按输出序号均匀采样, 扫描结束后总数已知, 再在样本里取最接近
    /// `count * k / num_splits` 的 user key. 步长随样本数翻倍, 内存有界, 且不需要
    /// 额外一遍扫描.
    fn count_dedup_with_splits(&self, num_splits: usize) -> Result<(usize, Vec<Vec<u8>>)> {
        if num_splits <= 1 {
            let count = self.count_dedup_entries()?;
            return Ok((count, vec![]));
        }

        let mut merge_iter = MergeIterator::new(self.all_inputs())?;
        let mut count = 0usize;
        let mut last_user_key: Option<Vec<u8>> = None;
        let mut tracker = SnapshotDedupTracker::default();
        let mut samples: Vec<(usize, Vec<u8>)> = Vec::new();
        let mut stride = 1usize;

        while let Some((key, value)) = merge_iter.next_entry()? {
            let user_key = user_key_from_internal(&key)?;
            let seq = extract_sequence(&key)?;
            let counted = if last_user_key.as_deref() == Some(user_key) {
                let counted = !tracker.already_crossed();
                if counted {
                    tracker.observe(seq, self.min_snapshot_sequence);
                }
                counted
            } else {
                tracker.start_key();
                let value_type = extract_value_type(&key)?;
                let dropped = drop_delete_marker(
                    value_type,
                    seq,
                    self.min_snapshot_sequence,
                    user_key,
                    &value,
                    &self.surviving_user_ranges,
                );
                last_user_key = Some(user_key.to_vec());
                tracker.observe(seq, self.min_snapshot_sequence);
                !dropped
            };
            if !counted {
                continue;
            }
            count += 1;
            if count.is_multiple_of(stride) {
                samples.push((count, user_key.to_vec()));
                if samples.len() > SPLIT_SAMPLE_CAP {
                    stride *= 2;
                    samples.retain(|(i, _)| i.is_multiple_of(stride));
                }
            }
        }

        // 总数已知, 按真实的 k/N 分位取最近样本; 粒度受 stride 限制, 但分布均匀.
        let mut splits: Vec<Vec<u8>> = Vec::with_capacity(num_splits - 1);
        for k in 1..num_splits {
            let target = count * k / num_splits;
            if target == 0 {
                continue;
            }
            let pos = samples.partition_point(|(i, _)| *i < target);
            let Some(pick) = samples.get(pos).or_else(|| samples.last()) else {
                break;
            };
            if splits.last().map(|prev| prev.as_slice()) != Some(pick.1.as_slice()) {
                splits.push(pick.1.clone());
            }
        }

        Ok((count, splits))
    }

    /// 执行 compaction, 可能分裂为多个子任务并行.
    ///
    /// `file_numbers` 中元素个数表示分裂数: 1 = 不分裂, N = N 个子任务.
    /// 每个子任务使用 `file_numbers[i]` 作为输出文件编号.
    /// 调用方需预先分配所有文件编号, 避免并发分配冲突.
    pub fn run(&self, file_numbers: &[u64]) -> Result<Vec<CompactionResult>> {
        self.run_with_reader(file_numbers, None)
    }

    /// 同 [`CompactionJob::run`], 但 filter 可经 `reader` 点查其它 key
    /// (见 [`CompactionFilter::filter_with_reader`]). `reader` 为 `None` 时与 `run` 等价.
    pub fn run_with_reader(
        &self,
        file_numbers: &[u64],
        reader: Option<&dyn CompactionPointReader>,
    ) -> Result<Vec<CompactionResult>> {
        if file_numbers.len() <= 1 {
            let fnum = file_numbers.first().copied().unwrap_or(0);
            return self.run_single_with_allocator(fnum, None, reader);
        }
        self.run_split(file_numbers, reader)
    }

    /// 支持按 `target_file_size` 与 `max_compaction_bytes` 动态切分输出文件的单线程执行入口.
    pub fn run_with_allocator(
        &self,
        first_file_number: u64,
        alloc_file_number: &(dyn Fn() -> u64 + Sync),
        reader: Option<&dyn CompactionPointReader>,
    ) -> Result<Vec<CompactionResult>> {
        self.run_single_with_allocator(first_file_number, Some(alloc_file_number), reader)
    }

    /// 单线程完整 compaction. 支持在 user_key 切换边界按 `target_file_size` 和祖父层重叠切分多个 SST.
    fn run_single_with_allocator(
        &self,
        first_file_number: u64,
        alloc_file_number: Option<&(dyn Fn() -> u64 + Sync)>,
        reader: Option<&dyn CompactionPointReader>,
    ) -> Result<Vec<CompactionResult>> {
        let ctx = self.filter_ctx(reader);

        let total_input_bytes: u64 = self
            .all_inputs()
            .iter()
            .map(|r| r.file_size())
            .sum::<u64>()
            .max(1);
        let raw_estimated_keys: usize = self
            .all_inputs()
            .iter()
            .map(|r| {
                r.properties()
                    .map(|p| p.num_entries as usize)
                    .unwrap_or_else(|| (r.file_size() as usize / 32).max(1))
            })
            .sum::<usize>()
            .max(1);
        let estimated_keys = if total_input_bytes > self.target_file_size {
            ((raw_estimated_keys as u128 * self.target_file_size as u128)
                / total_input_bytes as u128)
                .max(1) as usize
        } else {
            raw_estimated_keys
        };

        let new_builder = |file_number: u64| -> Result<(PathBuf, SSTableBuilder)> {
            let path = sstable_path(&self.db_path, file_number, self.output_level);
            let mut b = SSTableBuilder::new(
                &path,
                self.block_size,
                self.block_restart_interval,
                self.compression,
                self.bloom_false_positive_rate,
            )?;
            if self.bloom_false_positive_rate > 0.0 {
                b.set_expected_keys(estimated_keys);
            }
            Ok((path, b))
        };

        let mut cur_file_number = first_file_number;
        let (mut output_path, mut builder) = new_builder(cur_file_number)?;
        let mut results: Vec<CompactionResult> = Vec::new();
        let mut gp_tracker = GrandparentTracker::new(&self.grandparents, self.max_compaction_bytes);

        let mut merge_iter = MergeIterator::new(self.all_inputs())?;
        let mut filter_removed: Vec<Vec<u8>> = Vec::new();
        let mut entry_count = 0usize;
        let mut last_user_key: Option<Vec<u8>> = None;
        let mut smallest_key: Option<Vec<u8>> = None;
        let mut largest_key: Option<Vec<u8>> = None;
        let mut tracker = SnapshotDedupTracker::default();
        let mut range_tracker = RangeTombstoneTracker::default();
        let mut pending_fragments: Vec<(u64, Vec<u8>)> = Vec::new();

        while let Some((key, value)) = merge_iter.next_entry()? {
            let user_key = user_key_from_internal(&key)?;
            let value_type = extract_value_type(&key)?;
            let seq = extract_sequence(&key)?;

            if last_user_key.as_deref() != Some(user_key) {
                range_tracker.advance_past(user_key);

                if entry_count > 0 {
                    if let Some(alloc) = alloc_file_number {
                        let size_split = builder.estimated_file_size() >= self.target_file_size;
                        let gp_split = gp_tracker.should_stop_before(user_key);
                        if size_split || gp_split {
                            let raft_bytes = builder.raft_written_bytes();
                            let file_size = builder.finish()?;
                            results.push(CompactionResult {
                                file_number: cur_file_number,
                                entry_count,
                                output_path,
                                smallest_key: smallest_key.take().unwrap_or_default(),
                                largest_key: largest_key.take().unwrap_or_default(),
                                file_size,
                                raft_written_bytes: raft_bytes,
                                filter_removed_user_keys: std::mem::take(&mut filter_removed),
                            });
                            pending_fragments = range_tracker.active_past(user_key);
                            cur_file_number = alloc();
                            (output_path, builder) = new_builder(cur_file_number)?;
                            entry_count = 0;
                            gp_tracker.reset_overlap();
                        }
                    }
                } else {
                    gp_tracker.observe_first_key(user_key);
                }
            }

            flush_spanning_fragments(
                &mut builder,
                &mut pending_fragments,
                Some(user_key),
                Some(&key),
                &mut entry_count,
                &mut smallest_key,
                &mut largest_key,
            )?;

            if last_user_key.as_deref() == Some(user_key) {
                if !tracker.already_crossed() {
                    if !should_filter_impl(ctx, self.output_level, &key, &value) {
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

            if value_type != ValueType::TypeRangeDelete
                && range_tracker.covers(user_key, seq, self.min_snapshot_sequence)
            {
                last_user_key = Some(user_key.to_vec());
                tracker.observe(seq, self.min_snapshot_sequence);
                continue;
            }

            if !should_filter_impl(ctx, self.output_level, &key, &value) {
                builder.add(&key, &value)?;
                entry_count += 1;
            } else if value_type == ValueType::TypePut {
                // 记录; Version 安装后再通知 listener (见 DB::run_compaction_once).
                filter_removed.push(user_key.to_vec());
                if let Some(marker) = removal_marker(
                    user_key,
                    seq,
                    self.min_snapshot_sequence,
                    &self.surviving_user_ranges,
                ) {
                    builder.add(&marker, &[])?;
                    entry_count += 1;
                    last_user_key = Some(user_key.to_vec());
                    tracker.observe(seq, self.min_snapshot_sequence);
                    if smallest_key.is_none() {
                        smallest_key = Some(marker.clone());
                    }
                    largest_key = Some(marker);
                    continue;
                }
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
            if results.is_empty() {
                results.push(CompactionResult {
                    file_number: 0,
                    entry_count: 0,
                    output_path,
                    smallest_key: vec![],
                    largest_key: vec![],
                    file_size: 0,
                    raft_written_bytes: 0,
                    filter_removed_user_keys: filter_removed,
                });
            }
            return Ok(results);
        }

        let raft_bytes = builder.raft_written_bytes();
        let file_size = builder.finish()?;
        results.push(CompactionResult {
            file_number: cur_file_number,
            entry_count,
            output_path,
            smallest_key: smallest_key.unwrap_or_default(),
            largest_key: largest_key.unwrap_or_default(),
            file_size,
            raft_written_bytes: raft_bytes,
            filter_removed_user_keys: filter_removed,
        });
        Ok(results)
    }

    /// 分裂 compaction: 将输入按 key 范围分成多个子任务并行执行.
    fn run_split(
        &self,
        file_numbers: &[u64],
        reader: Option<&dyn CompactionPointReader>,
    ) -> Result<Vec<CompactionResult>> {
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
                raft_written_bytes: 0,
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
        let ctx = self.filter_ctx(reader);
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
                        ctx,
                        surviving,
                    )
                }));
            }

            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        results.into_iter().collect()
    }
}

struct GrandparentTracker<'a> {
    grandparents: &'a [Arc<SSTableReader>],
    index: usize,
    seen_key: bool,
    overlapped_bytes: u64,
    max_compaction_bytes: u64,
}

impl<'a> GrandparentTracker<'a> {
    fn new(grandparents: &'a [Arc<SSTableReader>], max_compaction_bytes: u64) -> Self {
        Self {
            grandparents,
            index: 0,
            seen_key: false,
            overlapped_bytes: 0,
            max_compaction_bytes,
        }
    }

    fn observe_first_key(&mut self, user_key: &[u8]) {
        self.advance_past(user_key);
        self.seen_key = true;
    }

    fn reset_overlap(&mut self) {
        self.overlapped_bytes = 0;
        self.seen_key = false;
    }

    fn advance_past(&mut self, user_key: &[u8]) {
        while self.index < self.grandparents.len() {
            let gp = &self.grandparents[self.index];
            let gp_largest = crate::engine::memtable::extract_user_key(gp.largest_key());
            if gp_largest < user_key {
                self.index += 1;
            } else {
                break;
            }
        }
    }

    fn should_stop_before(&mut self, user_key: &[u8]) -> bool {
        if self.grandparents.is_empty() {
            return false;
        }
        self.advance_past(user_key);
        while self.index < self.grandparents.len() {
            let gp = &self.grandparents[self.index];
            let gp_smallest = crate::engine::memtable::extract_user_key(gp.smallest_key());
            if gp_smallest <= user_key {
                if self.seen_key {
                    self.overlapped_bytes = self.overlapped_bytes.saturating_add(gp.file_size());
                }
                self.index += 1;
            } else {
                break;
            }
        }
        self.seen_key = true;
        self.overlapped_bytes > self.max_compaction_bytes
    }
}

/// 单个子 compaction 任务: 以 `range_end` 为界的 `MergeIterator` 遍历输入,
/// 仅输出在 `[range_start, range_end)` 范围内的条目.
#[allow(clippy::too_many_arguments)]
/// 最新 `TypePut` 被 filter 删除后, 是否需要写出同 sequence 的删除标记.
///
/// 更老版本仍可能在快照 (`min_snap_seq != u64::MAX`) 或幸存文件里; 没有标记它们就会重新可见.
/// 返回 `Some(标记的内部键)` 表示需要写出.
fn removal_marker(
    user_key: &[u8],
    seq: u64,
    min_snap_seq: u64,
    surviving: &[UserKeyRange],
) -> Option<Vec<u8>> {
    if min_snap_seq != u64::MAX || overlaps_surviving(user_key, surviving) {
        Some(encode_internal_key(user_key, seq, ValueType::TypeDelete))
    } else {
        None
    }
}

/// 判断删除标记是否可以不写入输出 (见 `should_drop_delete_marker`).
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

fn add_output_entry(
    builder: &mut SSTableBuilder,
    key: &[u8],
    value: &[u8],
    value_type: ValueType,
    user_key: &[u8],
    range_end: Option<&[u8]>,
    entry_count: &mut usize,
) -> Result<()> {
    if value_type == ValueType::TypeRangeDelete {
        let clipped = clip_range_end(value, range_end);
        if clipped.as_slice() > user_key {
            builder.add(key, &clipped)?;
            *entry_count += 1;
        }
        return Ok(());
    }
    builder.add(key, value)?;
    *entry_count += 1;
    Ok(())
}

fn clip_range_end(end: &[u8], range_end: Option<&[u8]>) -> Vec<u8> {
    match range_end {
        Some(bound) if end > bound => bound.to_vec(),
        _ => end.to_vec(),
    }
}

/// 把起点落在本段之前的墓碑写成 `[range_start, end)` 片段.
/// `before` 是当前内部键; 只写出排在它前面的片段, 其余留在 `pending`.
fn flush_spanning_fragments(
    builder: &mut SSTableBuilder,
    pending: &mut Vec<(u64, Vec<u8>)>,
    range_start: Option<&[u8]>,
    before: Option<&[u8]>,
    entry_count: &mut usize,
    smallest_key: &mut Option<Vec<u8>>,
    largest_key: &mut Option<Vec<u8>>,
) -> Result<()> {
    let Some(range_start) = range_start else {
        return Ok(());
    };
    if pending.is_empty() {
        return Ok(());
    }
    pending.sort_by(|left, right| {
        let left_key = encode_internal_key(range_start, left.0, ValueType::TypeRangeDelete);
        let right_key = encode_internal_key(range_start, right.0, ValueType::TypeRangeDelete);
        compare_internal_key(&left_key, &right_key)
    });
    let mut rest = Vec::new();
    for (sequence, end) in pending.drain(..) {
        let internal = encode_internal_key(range_start, sequence, ValueType::TypeRangeDelete);
        if before.is_some_and(|current| compare_internal_key(&internal, current) != Ordering::Less)
        {
            rest.push((sequence, end));
            continue;
        }
        builder.add(&internal, &end)?;
        *entry_count += 1;
        if smallest_key.is_none() {
            *smallest_key = Some(internal.clone());
        }
        *largest_key = Some(internal);
    }
    *pending = rest;
    Ok(())
}

/// filter 与可选点查句柄, 随写入路径下发. 两者都是借用, 因此整体 `Copy`, 可直接 move 进子任务线程.
#[derive(Clone, Copy)]
struct FilterCtx<'a> {
    filter: Option<&'a dyn CompactionFilter>,
    reader: Option<&'a dyn CompactionPointReader>,
}

/// 检查 entry 是否应被 compaction filter 丢弃.
/// 有 reader 时走 `filter_with_reader`, 否则走 `filter`.
fn should_filter_impl(ctx: FilterCtx<'_>, level: usize, key: &[u8], value: &[u8]) -> bool {
    let Some(filter) = ctx.filter else {
        return false;
    };
    let decision = match ctx.reader {
        Some(reader) => filter.filter_with_reader(level, key, value, reader),
        None => filter.filter(level, key, value),
    };
    decision == FilterDecision::Remove
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
    ctx: FilterCtx<'_>,
    surviving: Vec<UserKeyRange>,
) -> Result<CompactionResult> {
    let output_path = sstable_path(db_path, file_number, output_level);

    // 无预期条目时提前返回空结果. 跨段墓碑不在本段的点 key 计数里, 仍要写出片段.
    if expected_keys == 0 && !readers.iter().any(|reader| reader.has_range_tombstones()) {
        return Ok(CompactionResult {
            file_number: 0,
            entry_count: 0,
            output_path,
            smallest_key: vec![],
            largest_key: vec![],
            file_size: 0,
            raft_written_bytes: 0,
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
    let mut pending_fragments: Vec<(u64, Vec<u8>)> = Vec::new();
    while let Some((key, value)) = merge_iter.next_entry()? {
        let user_key = user_key_from_internal(&key)?;
        let value_type = extract_value_type(&key)?;
        let seq = extract_sequence(&key)?;

        // 跳过 range_start 之前的条目 (由前一个子任务处理).
        // 终点伸进本段的范围删除写成 [range_start, min(end, range_end)).
        if let Some(ref start) = range_start {
            if user_key < start.as_slice() {
                if value_type == ValueType::TypeRangeDelete && value.as_slice() > start.as_slice() {
                    let end = clip_range_end(&value, range_end.as_deref());
                    if end.as_slice() > start.as_slice() {
                        pending_fragments.push((seq, end.clone()));
                        range_tracker.add(start.clone(), end, seq);
                    }
                }
                continue;
            }
        }
        flush_spanning_fragments(
            &mut builder,
            &mut pending_fragments,
            range_start.as_deref(),
            Some(&key),
            &mut entry_count,
            &mut smallest_key,
            &mut largest_key,
        )?;

        if last_user_key.as_deref() != Some(user_key) {
            range_tracker.advance_past(user_key);
        }

        if last_user_key.as_deref() == Some(user_key) {
            if !tracker.already_crossed() {
                if !should_filter_impl(ctx, output_level, &key, &value) {
                    add_output_entry(
                        &mut builder,
                        &key,
                        &value,
                        value_type,
                        user_key,
                        range_end.as_deref(),
                        &mut entry_count,
                    )?;
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

        if value_type != ValueType::TypeRangeDelete
            && range_tracker.covers(user_key, seq, min_snap_seq)
        {
            last_user_key = Some(user_key.to_vec());
            tracker.observe(seq, min_snap_seq);
            continue;
        }

        // 最新版本被 filter 删除时, 同 key 的更老版本若仍在快照或幸存文件里, 直接丢弃会让它们重新可见.
        // 这种情况下以同 sequence 写出删除标记, 语义等同于 filter 把该 key 删除.
        let tombstone: Option<Vec<u8>> = if !should_filter_impl(ctx, output_level, &key, &value) {
            add_output_entry(
                &mut builder,
                &key,
                &value,
                value_type,
                user_key,
                range_end.as_deref(),
                &mut entry_count,
            )?;
            None
        } else if value_type == ValueType::TypePut {
            filter_removed.push(user_key.to_vec());
            match removal_marker(user_key, seq, min_snap_seq, &surviving) {
                Some(marker) => {
                    builder.add(&marker, &[])?;
                    entry_count += 1;
                    Some(marker)
                }
                None => None,
            }
        } else {
            None
        };
        last_user_key = Some(user_key.to_vec());
        tracker.observe(seq, min_snap_seq);
        if smallest_key.is_none() {
            smallest_key = Some(tombstone.clone().unwrap_or_else(|| key.clone()));
        }
        largest_key = Some(tombstone.unwrap_or(key));
    }

    flush_spanning_fragments(
        &mut builder,
        &mut pending_fragments,
        range_start.as_deref(),
        None,
        &mut entry_count,
        &mut smallest_key,
        &mut largest_key,
    )?;

    if entry_count == 0 {
        builder.abandon()?;
        return Ok(CompactionResult {
            file_number: 0,
            entry_count: 0,
            output_path,
            smallest_key: vec![],
            largest_key: vec![],
            file_size: 0,
            raft_written_bytes: 0,
            filter_removed_user_keys: filter_removed,
        });
    }

    let raft_bytes = builder.raft_written_bytes();
    let file_size = builder.finish()?;
    Ok(CompactionResult {
        file_number,
        entry_count,
        output_path,
        smallest_key: smallest_key.unwrap_or_default(),
        largest_key: largest_key.unwrap_or_default(),
        file_size,
        raft_written_bytes: raft_bytes,
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
        for res in &non_empty {
            let reader = SSTableReader::open(&res.output_path, None).unwrap();
            for key in [b"a", b"b", b"c", b"d", b"e"] {
                let ik = encode_internal_key(key, u64::MAX, ValueType::TypePut);
                if let Some((val, _)) = reader.get(&ik).unwrap() {
                    let expected: &[u8] = match key {
                        b"a" => b"1".as_slice(),
                        b"b" => b"2".as_slice(),
                        b"c" => b"3".as_slice(),
                        b"d" => b"4".as_slice(),
                        _ => b"5".as_slice(),
                    };
                    assert_eq!(&val[..], expected);
                }
            }
        }
    }

    #[test]
    fn split_truncates_range_delete_and_keeps_both_sides() {
        let dir = tempdir().unwrap();
        let input = sst(
            dir.path(),
            1,
            0,
            &[
                (b"a", 5, ValueType::TypeRangeDelete, b"z"),
                (b"a", 4, ValueType::TypePut, b"va"),
                (b"m", 6, ValueType::TypePut, b"new"),
                (b"m", 4, ValueType::TypePut, b"vm"),
                (b"y", 4, ValueType::TypePut, b"vy"),
                (b"z", 4, ValueType::TypePut, b"vz"),
            ],
        );
        let readers = vec![input];
        let ctx = FilterCtx {
            filter: None,
            reader: None,
        };
        let left = write_sub_compaction(
            &readers,
            10,
            None,
            Some(b"m".to_vec()),
            1,
            dir.path(),
            512,
            16,
            CompressionType::None,
            0.0,
            0,
            8,
            ctx,
            Vec::new(),
        )
        .unwrap();
        let right = write_sub_compaction(
            &readers,
            11,
            Some(b"m".to_vec()),
            None,
            1,
            dir.path(),
            512,
            16,
            CompressionType::None,
            0.0,
            0,
            8,
            ctx,
            Vec::new(),
        )
        .unwrap();
        let outputs = [&left, &right];
        let mut records = Vec::new();
        let mut saw_new_m = false;
        for result in &outputs {
            let reader = SSTableReader::open(&result.output_path, None).unwrap();
            for (start, end, _) in reader.collect_range_tombstones() {
                assert!(
                    !(start.as_slice() == b"a" && end.as_slice() == b"z"),
                    "a split output must not keep the unclipped [a, z)"
                );
                records.push(crate::engine::memtable::RangeTombstoneRecord {
                    start,
                    end,
                    sequence: 5,
                });
            }
            let seek = encode_internal_key(b"m", u64::MAX, ValueType::TypePut);
            if let Some((value, ty)) = reader.get(&seek).unwrap() {
                if ty == ValueType::TypePut {
                    assert_eq!(value, b"new");
                    saw_new_m = true;
                }
            }
        }
        let fragmented = crate::engine::memtable::FragmentedRangeTombstones::from_records(records);
        assert_eq!(fragmented.max_seq(b"a", 10), Some(5));
        assert_eq!(fragmented.max_seq(b"y", 10), Some(5));
        assert_eq!(fragmented.max_seq(b"z", 10), None);
        assert!(saw_new_m, "point newer than the tombstone stays visible");
    }

    /// 大 value 时切分点必须仍然落在真实条目上.
    ///
    /// 条目数只能从 `file_size / 100` 粗估, 平均记录远大于 200 字节时估算值会高估
    /// 一个数量级以上, 切分间隔永远够不到, `splits` 为空, 切分静默退化成单写手:
    /// `num_splits` 报 2 却只有一个输出文件, 还白多一遍计数扫描.
    #[test]
    fn split_points_survive_large_values() {
        let dir = tempdir().unwrap();
        let value = vec![b'v'; 4096];
        let keys: Vec<Vec<u8>> = (0..400).map(|i| format!("k{i:05}").into_bytes()).collect();
        let entries: Vec<(&[u8], u64, ValueType, &[u8])> = keys
            .iter()
            .map(|k| (k.as_slice(), 1u64, ValueType::TypePut, value.as_slice()))
            .collect();
        let input = sst(dir.path(), 1, 0, &entries);
        // 约 1.6MB / 400 条: file_size / 100 ≈ 16000, 高于真实条数 40 倍
        let job = CompactionJob::new(
            vec![input],
            vec![],
            1,
            dir.path().to_path_buf(),
            4096,
            16,
            CompressionType::None,
            0.0,
        );
        let results = job.run(&[100, 101]).unwrap();
        let non_empty = results.iter().filter(|r| r.entry_count > 0).count();
        assert_eq!(
            non_empty, 2,
            "两条子任务都应产出文件, 实际 {non_empty} 个 (切分点落在了估算条目数之外)"
        );
        let total: usize = results.iter().map(|r| r.entry_count).sum();
        assert_eq!(total, 400, "切分前后条目总数必须一致");
    }

    /// 切分 + 活跃快照 pin + 跨段范围墓碑: 这个组合此前没有任何测试覆盖
    /// (`test_subcompaction_split_two_ranges` 有切分无快照, 快照测试又都不切分).
    ///
    /// 断言两件事:
    /// 1. 快照可见的旧版本在两侧子任务里都还在 (pin 住 seq <= min_snapshot_sequence);
    /// 2. 跨段墓碑被切成首尾相接的片段, 拼回去正好覆盖原始区间, 不留缝隙也不重叠.
    #[test]
    fn split_with_snapshot_keeps_pinned_versions_and_stitches_tombstone() {
        let dir = tempdir().unwrap();
        // 10 个 key, 每个两条版本 (seq 4 新 / seq 2 旧); 一条 [k00, zz) 的墓碑盖住全部.
        // 快照 pin 在 seq 3: 每个 key 的 seq 2 版本都是快照唯一可见的版本, 不能丢.
        let mut rows: Vec<(Vec<u8>, u64, ValueType, Vec<u8>)> = Vec::new();
        rows.push((
            b"k00".to_vec(),
            6,
            ValueType::TypeRangeDelete,
            b"zz".to_vec(),
        ));
        for i in 0..10 {
            let key = format!("k{i:02}").into_bytes();
            rows.push((
                key.clone(),
                4,
                ValueType::TypePut,
                format!("n{i:02}").into_bytes(),
            ));
            rows.push((key, 2, ValueType::TypePut, format!("o{i:02}").into_bytes()));
        }
        let entries: Vec<(&[u8], u64, ValueType, &[u8])> = rows
            .iter()
            .map(|(k, s, t, v)| (k.as_slice(), *s, *t, v.as_slice()))
            .collect();
        let input = sst(dir.path(), 1, 0, &entries);

        let job = CompactionJob::new(
            vec![input],
            vec![],
            1,
            dir.path().to_path_buf(),
            512,
            16,
            CompressionType::None,
            0.0,
        )
        .with_snapshot_threshold(3);
        let results = job.run(&[10, 11]).unwrap();
        let non_empty: Vec<&CompactionResult> =
            results.iter().filter(|r| r.entry_count > 0).collect();
        assert_eq!(non_empty.len(), 2, "应切成两段");

        // 1) 快照可见版本必须保留
        let mut seen = 0;
        for result in &non_empty {
            let reader = SSTableReader::open(&result.output_path, None).unwrap();
            for i in 0..10 {
                let key = format!("k{i:02}");
                let seek = encode_internal_key(key.as_bytes(), 3, ValueType::TypePut);
                if let Some((value, ty)) = reader.get(&seek).unwrap() {
                    if ty == ValueType::TypePut {
                        assert_eq!(
                            value,
                            format!("o{i:02}").as_bytes(),
                            "{key} 在 seq 3 快照下应看到 seq 2 的旧值"
                        );
                        seen += 1;
                    }
                }
            }
        }
        assert_eq!(seen, 10, "10 个 key 的快照可见版本都应保留");

        // 2) 墓碑片段首尾相接
        let mut fragments: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for result in &non_empty {
            let reader = SSTableReader::open(&result.output_path, None).unwrap();
            for (start, end, _) in reader.collect_range_tombstones() {
                fragments.push((start, end));
            }
        }
        fragments.sort();
        assert_eq!(fragments.first().unwrap().0, b"k00");
        assert_eq!(fragments.last().unwrap().1, b"zz");
        for pair in fragments.windows(2) {
            assert_eq!(pair[0].1, pair[1].0, "切分后的墓碑片段之间不能有缝隙");
        }
    }

    /// 墓碑覆盖裁剪必须尊重活跃快照.
    ///
    /// `RangeTombstoneTracker::covers` 原先只看 `tombstone.sequence >= point.sequence`,
    /// 不看 `min_snapshot_sequence`. 墓碑 seq 大于最小快照位点时, 存在看不到这次删除的
    /// 快照 (seq 在 `[min_snapshot_sequence, tombstone.sequence)` 之间), 它需要被覆盖的
    /// 最新版本. 一旦被裁掉, 这类快照会读到更旧的版本, 甚至读不到.
    #[test]
    fn range_tombstone_must_not_drop_version_needed_by_snapshot() {
        let dir = tempdir().unwrap();
        let input = sst(
            dir.path(),
            1,
            0,
            &[
                // 墓碑在 seq 6, 盖住 [k00, zz)
                (b"k00", 6, ValueType::TypeRangeDelete, b"zz"),
                (b"k01", 4, ValueType::TypePut, b"n01"),
                (b"k01", 2, ValueType::TypePut, b"o01"),
                // 落点之外, 保证无快照时仍有输出文件可检查
                (b"zzz", 3, ValueType::TypePut, b"live"),
            ],
        );

        // min_snapshot = 2, 但同时有快照在 seq 4: seq 4 的版本必须留下
        let job = CompactionJob::new(
            vec![Arc::clone(&input)],
            vec![],
            1,
            dir.path().to_path_buf(),
            512,
            16,
            CompressionType::None,
            0.0,
        )
        .with_snapshot_threshold(2);
        let results = job.run(&[10]).unwrap();
        let reader = SSTableReader::open(&results[0].output_path, None).unwrap();
        let seek = encode_internal_key(b"k01", 4, ValueType::TypePut);
        assert_eq!(
            reader.get(&seek).unwrap().map(|(value, _)| value),
            Some(b"n01".to_vec()),
            "墓碑 seq 6 大于 min_snapshot_sequence 2 时不能吞掉 seq 4 的版本"
        );

        // 无活跃快照时仍要裁掉被覆盖的点, 不能为了修这个把优化整条关掉
        let job = CompactionJob::new(
            vec![Arc::clone(&input)],
            vec![],
            1,
            dir.path().to_path_buf(),
            512,
            16,
            CompressionType::None,
            0.0,
        );
        let results = job.run(&[11]).unwrap();
        let reader = SSTableReader::open(&results[0].output_path, None).unwrap();
        assert_eq!(
            reader
                .get(&encode_internal_key(b"k01", u64::MAX, ValueType::TypePut))
                .unwrap(),
            None,
            "无快照时墓碑覆盖的点应被裁掉"
        );
        assert_eq!(
            reader
                .get(&encode_internal_key(b"zzz", u64::MAX, ValueType::TypePut))
                .unwrap()
                .map(|(value, _)| value),
            Some(b"live".to_vec()),
            "墓碑区间之外的 key 应原样保留"
        );
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

    /// 父 key 不存在时删除子 key: filter 经 reader 点查父 key.
    /// 期望: 带 reader 的 `run_with_reader` 删除孤儿, 单子任务与多子任务 (线程路径) 都一致.
    #[test]
    fn test_filter_with_reader_sees_reader_in_single_and_split_paths() {
        use crate::engine::compaction::{CompactionFilter, CompactionPointReader, FilterDecision};

        struct ParentReader;
        impl CompactionPointReader for ParentReader {
            fn get(&self, user_key: &[u8]) -> crate::Result<Option<Vec<u8>>> {
                Ok((user_key == b"live").then(|| b"1".to_vec()))
            }
        }

        /// key 形如 "{parent}/x": 父 key 不存在就删除; 无 reader 时保守保留.
        struct DropOrphans;
        impl CompactionFilter for DropOrphans {
            fn filter(&self, _: usize, _: &[u8], _: &[u8]) -> FilterDecision {
                FilterDecision::Keep
            }
            fn filter_with_reader(
                &self,
                _: usize,
                key: &[u8],
                _: &[u8],
                reader: &dyn CompactionPointReader,
            ) -> FilterDecision {
                let user_key = crate::engine::memtable::extract_user_key(key);
                let parent = user_key.split(|b| *b == b'/').next().unwrap_or_default();
                match reader.get(parent) {
                    Ok(None) => FilterDecision::Remove,
                    _ => FilterDecision::Keep,
                }
            }
        }

        let dir = tempdir().unwrap();
        let make = |num: u64| {
            sst(
                dir.path(),
                num,
                0,
                &[
                    (b"dead/x", 2, ValueType::TypePut, b"v"),
                    (b"live/x", 1, ValueType::TypePut, b"v"),
                ],
            )
        };
        let job = CompactionJob::new(
            vec![make(1)],
            vec![],
            1,
            dir.path().to_path_buf(),
            512,
            16,
            CompressionType::None,
            0.0,
        )
        .with_filter(Some(Arc::new(DropOrphans)));

        let without_reader = job.run(&[30]).unwrap();
        assert_eq!(without_reader[0].entry_count, 2);

        let single = job.run_with_reader(&[31], Some(&ParentReader)).unwrap();
        assert_eq!(single[0].entry_count, 1);
        let reader = SSTableReader::open(&single[0].output_path, None).unwrap();
        let seek = encode_internal_key(b"live/x", u64::MAX, ValueType::TypePut);
        assert!(reader.get(&seek).unwrap().is_some());

        let split = job.run_with_reader(&[32, 33], Some(&ParentReader)).unwrap();
        let total: usize = split.iter().map(|r| r.entry_count).sum();
        assert_eq!(total, 1, "线程路径也应删除孤儿 key");
    }

    /// filter 删除 key `k` 的最新版本, 而更深层幸存文件里仍有 `k` 的旧版本.
    /// 现象: 旧实现直接丢弃最新版本, compaction 后旧值重新可见.
    /// 期望: 输出以同 sequence 写出删除标记, 读取得到 None. 单文件与分裂两条路径都要满足.
    #[test]
    fn test_filter_remove_over_surviving_writes_tombstone() {
        use crate::engine::compaction::{CompactionFilter, FilterDecision};

        struct DropAll;
        impl CompactionFilter for DropAll {
            fn filter(&self, _: usize, _: &[u8], _: &[u8]) -> FilterDecision {
                FilterDecision::Remove
            }
        }

        let surviving = vec![UserKeyRange {
            start: b"a".to_vec(),
            end: b"z".to_vec(),
        }];
        let dir = tempdir().unwrap();
        let make_job = |num: u64| {
            let a = sst(dir.path(), num, 0, &[(b"k", 5, ValueType::TypePut, b"new")]);
            CompactionJob::new(
                vec![a],
                vec![],
                1,
                dir.path().to_path_buf(),
                512,
                16,
                CompressionType::None,
                0.0,
            )
            .with_filter(Some(Arc::new(DropAll)))
            .with_surviving_user_ranges(surviving.clone())
        };

        for file_numbers in [vec![40u64], vec![41, 42]] {
            let job = make_job(file_numbers[0] + 100);
            let out = job.run(&file_numbers).unwrap();
            let total: usize = out.iter().map(|r| r.entry_count).sum();
            assert_eq!(total, 1, "{file_numbers:?}: 应只写出一条删除标记");
            let removed: Vec<Vec<u8>> = out
                .iter()
                .flat_map(|r| r.filter_removed_user_keys.clone())
                .collect();
            assert_eq!(removed, vec![b"k".to_vec()], "{file_numbers:?}");
            let written = out.iter().find(|r| r.entry_count == 1).unwrap();
            let reader = SSTableReader::open(&written.output_path, None).unwrap();
            let seek = encode_internal_key(b"k", u64::MAX, ValueType::TypePut);
            let (_, ty) = reader.get(&seek).unwrap().expect("tombstone");
            assert_eq!(ty, ValueType::TypeDelete, "{file_numbers:?}");
        }
    }

    /// 有活跃快照时, 即使没有幸存文件重叠, 最新版本被 filter 删除也要留下删除标记.
    /// 期望: 快照之前的版本仍可读, 快照之后看到删除. 无快照且无幸存重叠时不写标记 (见上一测试的对照).
    #[test]
    fn test_filter_remove_with_snapshot_writes_tombstone() {
        use crate::engine::compaction::{CompactionFilter, FilterDecision};

        struct DropAll;
        impl CompactionFilter for DropAll {
            fn filter(&self, _: usize, _: &[u8], _: &[u8]) -> FilterDecision {
                FilterDecision::Remove
            }
        }

        let dir = tempdir().unwrap();
        let a = sst(dir.path(), 1, 0, &[(b"k", 5, ValueType::TypePut, b"new")]);
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
        .with_filter(Some(Arc::new(DropAll)))
        .with_snapshot_threshold(3);
        let out = job.run(&[50]).unwrap();
        assert_eq!(out[0].entry_count, 1);
        let reader = SSTableReader::open(&out[0].output_path, None).unwrap();
        let seek = encode_internal_key(b"k", u64::MAX, ValueType::TypePut);
        let (_, ty) = reader.get(&seek).unwrap().expect("tombstone");
        assert_eq!(ty, ValueType::TypeDelete);
    }

    #[test]
    fn compaction_job_splits_output_at_target_file_size_without_splitting_user_key_versions() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let dir = tempdir().unwrap();
        let val = vec![b'x'; 256];
        // key "a" has 3 snapshot-protected versions -> ~800B total, must stay in one file even if target is 512B
        // key "b" and "c" push total size well past 512B -> must split at user_key boundaries
        let input = sst(
            dir.path(),
            1,
            0,
            &[
                (b"a", 30, ValueType::TypePut, &val),
                (b"a", 20, ValueType::TypePut, &val),
                (b"a", 10, ValueType::TypePut, &val),
                (b"b", 30, ValueType::TypePut, &val),
                (b"c", 30, ValueType::TypePut, &val),
                (b"d", 30, ValueType::TypePut, &val),
            ],
        );
        let next_num = AtomicU64::new(100);
        let job = CompactionJob::new(
            vec![input],
            vec![],
            1,
            dir.path().to_path_buf(),
            512,
            16,
            CompressionType::None,
            0.0,
        )
        .with_snapshot_threshold(5)
        .with_target_file_size(512);

        let results = job
            .run_with_allocator(1, &|| next_num.fetch_add(1, Ordering::Relaxed), None)
            .unwrap();
        assert!(
            results.len() >= 2,
            "expected multiple output SSTs, got {}",
            results.len()
        );
        // All 3 versions of "a" must stay in the first file (never split across files)
        let first_reader = SSTableReader::open(&results[0].output_path, None).unwrap();
        assert_eq!(
            first_reader
                .get(&encode_internal_key(b"a", 30, ValueType::TypePut))
                .unwrap()
                .unwrap()
                .0,
            val
        );
        assert_eq!(
            first_reader
                .get(&encode_internal_key(b"a", 10, ValueType::TypePut))
                .unwrap()
                .unwrap()
                .0,
            val
        );
        // Second file must NOT contain "a"
        let second_reader = SSTableReader::open(&results[1].output_path, None).unwrap();
        assert!(second_reader
            .get(&encode_internal_key(b"a", 30, ValueType::TypePut))
            .unwrap()
            .is_none());
    }

    #[test]
    fn compaction_job_splits_carry_active_range_tombstone_and_stop_on_grandparent_overlap() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let dir = tempdir().unwrap();
        let val = vec![b'y'; 200];
        let input = sst(
            dir.path(),
            1,
            0,
            &[
                (b"a", 10, ValueType::TypeRangeDelete, b"z"),
                (b"b", 5, ValueType::TypePut, &val),
                (b"m", 5, ValueType::TypePut, &val),
                (b"y", 5, ValueType::TypePut, &val),
            ],
        );
        let gp1 = sst(dir.path(), 2, 2, &[(b"b", 1, ValueType::TypePut, &val)]);
        let gp2 = sst(dir.path(), 3, 2, &[(b"m", 1, ValueType::TypePut, &val)]);
        let next_num = AtomicU64::new(200);
        let job = CompactionJob::new(
            vec![input],
            vec![],
            1,
            dir.path().to_path_buf(),
            512,
            16,
            CompressionType::None,
            0.0,
        )
        .with_snapshot_threshold(5)
        .with_target_file_size(1024 * 1024)
        .with_max_compaction_bytes(100)
        .with_grandparents(vec![gp1, gp2]);

        let results = job
            .run_with_allocator(1, &|| next_num.fetch_add(1, Ordering::Relaxed), None)
            .unwrap();
        assert!(
            results.len() >= 2,
            "expected grandparent overlap to trigger split, got {}",
            results.len()
        );
        // Verify active range tombstone [..., z) was carried into the final split SST
        let last_reader = SSTableReader::open(&results.last().unwrap().output_path, None).unwrap();
        let tombstones = last_reader.collect_range_tombstones();
        assert!(
            tombstones
                .iter()
                .any(|(start, end, seq)| start.as_slice() <= b"y".as_slice()
                    && b"y".as_slice() < end.as_slice()
                    && *seq == 10),
            "expected carried range tombstone covering y at seq 10, got {tombstones:?}"
        );
    }
}
