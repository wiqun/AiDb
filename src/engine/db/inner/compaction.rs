//! Compaction 子模块: 文件 claim、subcompaction 与后台压缩循环.
//!
//! 入口 `run_compaction_once`: pick → claim → trivial move / subcompaction → apply.

use super::{
    sstable_path, update_sstable_metrics, AtomicBool, AtomicOrdering, CompactionJob,
    CompactionTask, Duration, Error, Receiver, Result, SSTableReader, VersionEdit, Weak, DB,
};
use crate::engine::compaction::{user_key_from_internal, CompactionPointReader, UserKeyRange};
use crate::statistics::CompactionPhase;
use std::sync::Arc;

/// 借用 `&DB` 的点查句柄, 只在 `run_compaction_once` 栈上存活.
/// 读取最新可见版本, 不计入 Get 操作统计.
struct DbPointReader<'a> {
    db: &'a DB,
}

impl CompactionPointReader for DbPointReader<'_> {
    fn get(&self, user_key: &[u8]) -> Result<Option<Vec<u8>>> {
        let max_seq = self.db.sequence.load(AtomicOrdering::SeqCst);
        self.db.get_at_sequence_uncounted(user_key, max_seq)
    }
}

impl DB {
    pub(super) fn maybe_trigger_compaction(&self) {
        let levels: Vec<Vec<Arc<SSTableReader>>> = self.sstables.read().iter().cloned().collect();
        if self.compaction_picker.pick_compaction(&levels).is_some() {
            for s in &self.compaction_signals {
                let _ = s.try_send(());
            }
        }
    }

    /// Claim all file numbers in the compaction task. Returns false if any file
    /// is already being compacted by another thread.
    fn try_claim_files(&self, task: &CompactionTask) -> bool {
        let mut guard = self.compacting.lock();
        let mut claimed: Vec<u64> = Vec::new();
        for f in task.inputs.iter().chain(task.expanded_inputs.iter()) {
            if !guard.insert(f.file_number()) {
                // Collision: roll back all claims for this task
                for num in &claimed {
                    guard.remove(num);
                }
                return false;
            }
            claimed.push(f.file_number());
        }
        true
    }

    /// Release all file numbers claimed by the compaction task.
    fn release_files(&self, task: &CompactionTask) {
        let mut guard = self.compacting.lock();
        for f in task.inputs.iter().chain(task.expanded_inputs.iter()) {
            guard.remove(&f.file_number());
        }
    }

    /// 执行一轮 compaction; 返回 true 表示应链式再 pick.
    pub(crate) fn run_compaction_once(&self) -> Result<bool> {
        if self.checkpoint_in_progress.load(AtomicOrdering::Acquire) {
            return Ok(false);
        }
        let pick_start = std::time::Instant::now();
        let levels: Vec<Vec<Arc<SSTableReader>>> = self.sstables.read().iter().cloned().collect();
        let task = match self.compaction_picker.pick_compaction(&levels) {
            Some(t) => t,
            None => return Ok(false),
        };
        let span = crate::observability::optional_span("cmp_run");
        let _enter = span.enter();
        self.stats.compaction_phases[CompactionPhase::Pick as usize]
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.stats.compaction_durations[CompactionPhase::Pick as usize]
            .record(pick_start.elapsed().as_micros() as u64);

        if !self.try_claim_files(&task) {
            return Ok(true);
        }
        self.execute_claimed_task(task)
    }

    /// 对指定 `[start, end)` user key 范围强制执行一轮自顶向下压实 (含 `CompactionFilter` 过滤).
    ///
    /// `start` 为包含下界 (`None` 表示从头), `end` 为不包含上界 (`None` 表示到尾).
    pub fn compact_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<()> {
        if self.checkpoint_in_progress.load(AtomicOrdering::Acquire) {
            return Ok(());
        }
        self.flush()?;
        let max_levels = self.options.max_levels;
        for level in 0..max_levels {
            for attempt in 0..5 {
                let levels: Vec<Vec<Arc<SSTableReader>>> =
                    self.sstables.read().iter().cloned().collect();
                if level >= levels.len() {
                    break;
                }
                let inputs: Vec<Arc<SSTableReader>> = levels[level]
                    .iter()
                    .filter(|sst| sst_overlaps_user_range(sst, start, end))
                    .cloned()
                    .collect();
                if inputs.is_empty() {
                    break;
                }
                let output_level = if level == 0 {
                    let base = self.compaction_picker.base_level(&levels);
                    base.min(levels.len().saturating_sub(1))
                } else if level + 1 < levels.len() {
                    level + 1
                } else {
                    level
                };
                let expanded_inputs = if output_level != level {
                    collect_overlapping_outputs(&inputs, &levels[output_level])
                } else {
                    Vec::new()
                };
                let task = CompactionTask {
                    inputs,
                    level,
                    output_level,
                    expanded_inputs,
                    grandparents: Vec::new(),
                    is_trivial_move: false,
                };
                if self.try_claim_files(&task) {
                    let _ = self.execute_claimed_task(task)?;
                    break;
                }
                if attempt < 4 {
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
        Ok(())
    }

    fn execute_claimed_task(&self, task: CompactionTask) -> Result<bool> {
        struct ClaimGuard<'a> {
            db: &'a DB,
            file_numbers: Vec<u64>,
        }
        impl Drop for ClaimGuard<'_> {
            fn drop(&mut self) {
                let mut guard = self.db.compacting.lock();
                for num in &self.file_numbers {
                    guard.remove(num);
                }
            }
        }
        let file_numbers: Vec<u64> = task
            .inputs
            .iter()
            .chain(task.expanded_inputs.iter())
            .map(|f| f.file_number())
            .collect();
        let _claim_guard = ClaimGuard {
            db: self,
            file_numbers,
        };
        let levels: Vec<Vec<Arc<SSTableReader>>> = self.sstables.read().iter().cloned().collect();

        let level = task.level;
        let output_level = task.output_level;
        let in_files = task.inputs.len() + task.expanded_inputs.len();
        let in_bytes: u64 = task
            .inputs
            .iter()
            .chain(task.expanded_inputs.iter())
            .map(|f| f.file_size())
            .sum();
        tracing::info!(
            target: "cmp",
            db_id = self.db_id(),
            level,
            output_level,
            in_files,
            in_bytes,
            trivial_move = task.is_trivial_move,
            "compaction start"
        );

        // --- TRIVIAL MOVE FAST PATH ---
        if task.is_trivial_move {
            let t_move = std::time::Instant::now();
            let moved = self.run_trivial_move(task)?;
            tracing::info!(
                target: "cmp",
                db_id = self.db_id(),
                level,
                output_level,
                in_files,
                out_files = in_files,
                in_bytes,
                out_bytes = in_bytes,
                duration_ms = t_move.elapsed().as_millis() as u64,
                trivial_move = true,
                "compaction end"
            );
            return Ok(moved);
        }
        // --- END TRIVIAL MOVE ---

        let run_start = std::time::Instant::now();
        // "Pin": 读取 min_snapshot_sequence() 时短暂持有 write_lock, 和
        // snapshot() 内"读 seq + register"共享同一把锁, 二者之间就有了严格
        // 的 happens-before 关系 —— 不会读到一个"snapshot 的 seq 已经确定,
        // 但 register 还没做完"的中间状态. 没有这层同步的话, min_snap_seq
        // 是对 snapshots 列表的独立、无锁读取, 理论上可能和 snapshot() 内部
        // "拿到 seq"与"完成 register"之间的极短窗口交错, 读到一个还没反映
        // 出即将返回的那个 snapshot 的过期阈值.
        //
        // 对于 compaction 开始*之后*才创建的 snapshot 不需要这层保护:
        // 它们的 seq 必然 >= 当前 compaction 输入文件里的最大 sequence
        // (这些文件在 claim 时已经是不可变的已 flush 数据), 而每个 key
        // 的最新版本本来就无条件保留, 所以这类新 snapshot 总能读到正确
        // 版本, 不依赖 min_snap_seq 是否够新.
        let min_snap_seq = {
            let _guard = self.write_lock.lock();
            self.snapshots.min_snapshot_sequence()
        };
        let num_splits = self.compute_subcompaction_splits(&task);
        // Pre-allocate all needed file numbers to avoid concurrent allocation races
        let file_numbers: Vec<u64> = (0..num_splits)
            .map(|_| self.version_set.read().allocate_file_number())
            .collect();
        let consumed: Vec<u64> = task
            .inputs
            .iter()
            .chain(task.expanded_inputs.iter())
            .map(|f| f.file_number())
            .collect();
        let start = surviving_start_level(task.level, task.output_level);
        let surviving = deeper_and_sibling_user_ranges(&levels, start, &consumed);
        let reader = DbPointReader { db: self };
        let alloc = || self.version_set.read().allocate_file_number();
        let job = CompactionJob::new(
            task.inputs.clone(),
            task.expanded_inputs.clone(),
            task.output_level,
            self.path.clone(),
            self.options.block_size,
            self.options.block_restart_interval,
            self.options.compression,
            self.options.bloom_false_positive_rate,
        )
        .with_snapshot_threshold(min_snap_seq)
        .with_surviving_user_ranges(surviving)
        .with_filter(self.compaction_filter.read().clone())
        .with_target_file_size(self.options.target_file_size_base)
        .with_max_compaction_bytes(self.options.max_compaction_bytes)
        .with_grandparents(task.grandparents.clone());

        let results = if file_numbers.len() <= 1 {
            let first_fnum = file_numbers.first().copied().unwrap_or_else(alloc);
            job.run_with_allocator(first_fnum, &alloc, Some(&reader))?
        } else {
            job.run_with_reader(&file_numbers, Some(&reader))?
        };

        let written_bytes: u64 = results.iter().map(|r| r.file_size).sum();
        self.stats
            .compaction_written_bytes
            .fetch_add(written_bytes, AtomicOrdering::Relaxed);
        let raft_written: u64 = results.iter().map(|r| r.raft_written_bytes).sum();
        self.stats
            .raft_compaction_written_bytes
            .fetch_add(raft_written, AtomicOrdering::Relaxed);

        let read_bytes: u64 = task
            .inputs
            .iter()
            .chain(task.expanded_inputs.iter())
            .map(|f| f.file_size())
            .sum();
        self.stats.note_compaction_read(read_bytes);

        self.stats.compaction_phases[CompactionPhase::Run as usize]
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.stats.compaction_durations[CompactionPhase::Run as usize]
            .record(run_start.elapsed().as_micros() as u64);

        let apply_start = std::time::Instant::now();

        {
            let mut sst_guard = self.sstables.write();
            let mut vs_guard = self.version_set.write();

            for result in &results {
                if result.entry_count > 0 {
                    let reader = Arc::new(SSTableReader::open_with_caches(
                        &result.output_path,
                        Some(Arc::clone(&self.block_cache)),
                        Some(Arc::clone(&self.table_cache)),
                        Some(Arc::clone(&self.stats)),
                    )?);
                    if task.output_level == 0 {
                        sst_guard[task.output_level].insert(0, reader);
                    } else {
                        sst_guard[task.output_level].push(reader);
                        sst_guard[task.output_level].sort_by(|a, b| {
                            crate::engine::memtable::extract_user_key(a.smallest_key())
                                .cmp(crate::engine::memtable::extract_user_key(b.smallest_key()))
                        });
                    }
                    vs_guard.apply_edit(&VersionEdit::AddFile {
                        level: task.output_level,
                        file_number: result.file_number,
                        file_size: result.file_size,
                        smallest_key: result.smallest_key.clone(),
                        largest_key: result.largest_key.clone(),
                    })?;
                }
            }

            for input in &task.inputs {
                let num = input.file_number();
                vs_guard.apply_edit(&VersionEdit::DeleteFile {
                    level: task.level,
                    file_number: num,
                })?;
                sst_guard[task.level].retain(|f| f.file_number() != num);
            }
            for expanded in &task.expanded_inputs {
                let num = expanded.file_number();
                vs_guard.apply_edit(&VersionEdit::DeleteFile {
                    level: task.output_level,
                    file_number: num,
                })?;
                sst_guard[task.output_level].retain(|f| f.file_number() != num);
            }
            let l0_output_count = results
                .iter()
                .filter(|r| r.entry_count > 0 && task.output_level == 0)
                .count();
            if task.level == 0 {
                self.l0_sstable_count
                    .fetch_sub(task.inputs.len(), AtomicOrdering::Relaxed);
            }
            if task.output_level == 0 {
                self.l0_sstable_count
                    .fetch_sub(task.expanded_inputs.len(), AtomicOrdering::Relaxed);
                self.l0_sstable_count
                    .fetch_add(l0_output_count, AtomicOrdering::Relaxed);
            }
            self.stats.compaction_phases[CompactionPhase::Apply as usize]
                .fetch_add(1, AtomicOrdering::Relaxed);
            self.stats.compaction_durations[CompactionPhase::Apply as usize]
                .record(apply_start.elapsed().as_micros() as u64);
            self.stats.compaction_last_duration_us.store(
                run_start.elapsed().as_micros() as u64,
                AtomicOrdering::Relaxed,
            );
            self.publish_sst_range_tombstones(&sst_guard);
        }
        self.update_write_stall_state();

        let pending = self.pending_compaction_bytes();
        update_sstable_metrics(&self.sstables.read(), &self.stats, pending);

        // Version 已切换: 再通知 listener, 便于上层用 get==None 安全扣减.
        if let Some(listener) = self.compaction_removal_listener.read().clone() {
            for result in &results {
                for uk in &result.filter_removed_user_keys {
                    listener.on_latest_put_removed(uk);
                }
            }
        }

        self.try_cleanup_wals()?;

        for input in &task.inputs {
            let path = sstable_path(&self.path, input.file_number(), task.level);
            let _ = std::fs::remove_file(path);
        }
        for expanded in &task.expanded_inputs {
            let path = sstable_path(&self.path, expanded.file_number(), task.output_level);
            let _ = std::fs::remove_file(path);
        }

        self.release_files(&task);
        let out_files = results.iter().filter(|r| r.entry_count > 0).count();
        let out_bytes: u64 = results.iter().map(|r| r.file_size).sum();
        tracing::info!(
            target: "cmp",
            db_id = self.db_id(),
            level = task.level,
            output_level = task.output_level,
            in_files,
            out_files,
            in_bytes,
            out_bytes,
            duration_ms = run_start.elapsed().as_millis() as u64,
            trivial_move = false,
            "compaction end"
        );
        Ok(true)
    }

    /// 根据 subcompaction_min_size 和 compaction_threads 计算分裂数.
    fn compute_subcompaction_splits(&self, task: &CompactionTask) -> usize {
        let min_size = self.options.subcompaction_min_size;
        if min_size == 0 || self.options.compaction_threads <= 1 {
            return 1;
        }
        let total_size: u64 = task
            .inputs
            .iter()
            .chain(task.expanded_inputs.iter())
            .map(|r| r.file_size())
            .sum();
        if total_size < min_size {
            return 1;
        }
        let n = (total_size / min_size)
            .min(self.options.compaction_threads as u64)
            .min(self.options.max_sub_compactions as u64);
        n.max(self.options.min_sub_compactions as u64) as usize
    }

    /// Handle a trivial move: promote SST(s) to the next level without rewriting.
    fn run_trivial_move(&self, task: CompactionTask) -> Result<bool> {
        debug_assert!(
            !task.inputs.is_empty(),
            "trivial move requires at least one input"
        );
        let file_number = task.inputs[0].file_number();
        let old_path = sstable_path(&self.path, file_number, task.level);
        let new_path = sstable_path(&self.path, file_number, task.output_level);

        // Rename the SST file to reflect the new level
        std::fs::rename(&old_path, &new_path).map_err(|e| {
            Error::Io(std::io::Error::new(
                e.kind(),
                format!("trivial move rename failed: {old_path:?} -> {new_path:?}: {e}"),
            ))
        })?;

        // Re-open at the new path (file content unchanged, just new level in name)
        let reader = match SSTableReader::open_with_stats(
            &new_path,
            Some(Arc::clone(&self.block_cache)),
            Some(Arc::clone(&self.stats)),
        ) {
            Ok(r) => Arc::new(r),
            Err(e) => {
                // Rollback: move the file back to its original location
                let _ = std::fs::rename(&new_path, &old_path);
                return Err(e);
            }
        };

        {
            let mut sst_guard = self.sstables.write();
            let mut vs_guard = self.version_set.write();

            // Add at new level
            if task.output_level == 0 {
                sst_guard[task.output_level].insert(0, reader.clone());
            } else {
                sst_guard[task.output_level].push(reader.clone());
            }
            let smallest = reader.smallest_key().to_vec();
            let largest = reader.largest_key().to_vec();
            vs_guard.apply_edit(&VersionEdit::AddFile {
                level: task.output_level,
                file_number,
                file_size: reader.file_size(),
                smallest_key: smallest,
                largest_key: largest,
            })?;

            // Delete from old level
            vs_guard.apply_edit(&VersionEdit::DeleteFile {
                level: task.level,
                file_number,
            })?;
            sst_guard[task.level].retain(|f| f.file_number() != file_number);
            if task.level == 0 {
                self.l0_sstable_count.fetch_sub(1, AtomicOrdering::Relaxed);
            }
            if task.output_level == 0 {
                self.l0_sstable_count.fetch_add(1, AtomicOrdering::Relaxed);
            }
            self.publish_sst_range_tombstones(&sst_guard);
        }
        self.update_write_stall_state();

        let pending = self.pending_compaction_bytes();
        update_sstable_metrics(&self.sstables.read(), &self.stats, pending);

        Ok(true)
    }

    /// 测试/诊断: 排空当前可执行的 compaction 任务.
    pub fn drain_compactions(&self) -> Result<()> {
        while self.run_compaction_once()? {}
        Ok(())
    }
}

pub(super) fn compaction_background_loop(
    weak: Weak<DB>,
    shutdown: Arc<AtomicBool>,
    signal: Receiver<()>,
) {
    loop {
        if shutdown.load(AtomicOrdering::Acquire) {
            break;
        }
        let Some(db) = weak.upgrade() else {
            break;
        };
        match db.run_compaction_once() {
            Ok(true) => continue,
            Ok(false) => {
                let _ = signal.recv_timeout(Duration::from_millis(db.options.compaction_poll_ms));
            }
            Err(e) => {
                tracing::error!(target: "cmp", error = %e, "compaction round failed");
                let _ = signal.recv_timeout(Duration::from_millis(db.options.compaction_poll_ms));
            }
        }
    }
}

pub(crate) fn surviving_start_level(source_level: usize, _output_level: usize) -> usize {
    source_level
}

fn sst_overlaps_user_range(sst: &SSTableReader, start: Option<&[u8]>, end: Option<&[u8]>) -> bool {
    let Ok(sst_min) = user_key_from_internal(sst.smallest_key()) else {
        return true;
    };
    let Ok(sst_max) = user_key_from_internal(sst.largest_key()) else {
        return true;
    };
    if let Some(s) = start {
        if sst_max < s {
            return false;
        }
    }
    if let Some(e) = end {
        if sst_min >= e {
            return false;
        }
    }
    true
}

fn collect_overlapping_outputs(
    inputs: &[Arc<SSTableReader>],
    output_files: &[Arc<SSTableReader>],
) -> Vec<Arc<SSTableReader>> {
    let mut min_user: Option<&[u8]> = None;
    let mut max_user: Option<&[u8]> = None;
    for sst in inputs {
        let Ok(s) = user_key_from_internal(sst.smallest_key()) else {
            return output_files.to_vec();
        };
        let Ok(l) = user_key_from_internal(sst.largest_key()) else {
            return output_files.to_vec();
        };
        min_user = Some(match min_user {
            Some(cur) if cur <= s => cur,
            _ => s,
        });
        max_user = Some(match max_user {
            Some(cur) if cur >= l => cur,
            _ => l,
        });
    }
    let (Some(u_min), Some(u_max)) = (min_user, max_user) else {
        return Vec::new();
    };
    output_files
        .iter()
        .filter(|out| {
            let Ok(os) = user_key_from_internal(out.smallest_key()) else {
                return true;
            };
            let Ok(ol) = user_key_from_internal(out.largest_key()) else {
                return true;
            };
            os <= u_max && u_min <= ol
        })
        .cloned()
        .collect()
}

pub(crate) fn deeper_and_sibling_user_ranges(
    levels: &[Vec<Arc<SSTableReader>>],
    output_level: usize,
    consumed: &[u64],
) -> Vec<UserKeyRange> {
    let mut ranges = Vec::new();
    for files in levels.iter().skip(output_level) {
        for file in files {
            if consumed.contains(&file.file_number()) {
                continue;
            }
            let Ok(start) = user_key_from_internal(file.smallest_key()) else {
                continue;
            };
            let Ok(end) = user_key_from_internal(file.largest_key()) else {
                continue;
            };
            ranges.push(UserKeyRange {
                start: start.to_vec(),
                end: end.to_vec(),
            });
        }
    }
    ranges
}

#[cfg(test)]
mod tests {
    use super::{deeper_and_sibling_user_ranges, surviving_start_level};
    use crate::config::CompressionType;
    use crate::engine::memtable::{encode_internal_key, ValueType};
    use crate::engine::sstable::{sstable_path, SSTableBuilder, SSTableReader};
    use std::sync::Arc;
    use std::sync::Mutex;
    use tempfile::tempdir;
    use tracing::Subscriber;

    fn one_key(
        dir: &std::path::Path,
        num: u64,
        level: usize,
        user_key: &[u8],
    ) -> Arc<SSTableReader> {
        let path = sstable_path(dir, num, level);
        let mut b = SSTableBuilder::new(&path, 512, 16, CompressionType::None, 0.0).unwrap();
        b.add(&encode_internal_key(user_key, 1, ValueType::TypePut), b"v")
            .unwrap();
        b.finish().unwrap();
        Arc::new(SSTableReader::open(&path, None).unwrap())
    }

    #[test]
    fn deeper_and_sibling_user_ranges_keeps_unconsumed_output_and_deeper() {
        let dir = tempdir().unwrap();
        let mut levels = vec![Vec::new(), Vec::new(), Vec::new()];
        levels[1].push(one_key(dir.path(), 10, 1, b"a"));
        levels[1].push(one_key(dir.path(), 11, 1, b"m"));
        levels[2].push(one_key(dir.path(), 20, 2, b"z"));
        let ranges = deeper_and_sibling_user_ranges(&levels, 1, &[10]);
        let starts: Vec<Vec<u8>> = ranges.into_iter().map(|r| r.start).collect();
        assert!(starts.contains(&b"m".to_vec()));
        assert!(starts.contains(&b"z".to_vec()));
        assert!(!starts.iter().any(|s| s == b"a"));
    }

    #[test]
    fn source_level_sibling_survives_when_output_is_deeper() {
        let dir = tempdir().unwrap();
        let mut levels = vec![Vec::new(), Vec::new(), Vec::new()];
        levels[1].push(one_key(dir.path(), 10, 1, b"a"));
        levels[1].push(one_key(dir.path(), 11, 1, b"m"));
        levels[2].push(one_key(dir.path(), 20, 2, b"z"));
        let from_source = deeper_and_sibling_user_ranges(&levels, 1, &[10]);
        let from_output = deeper_and_sibling_user_ranges(&levels, 2, &[10]);
        assert!(from_source.iter().any(|r| r.start == b"m"));
        assert!(!from_output.iter().any(|r| r.start == b"m"));
        assert_eq!(surviving_start_level(1, 2), 1);
    }

    #[test]
    fn flush_and_compaction_log_once_without_user_key() {
        use std::sync::Arc;
        use tracing_subscriber::prelude::*;
        use tracing_subscriber::Registry;

        use crate::config::Options;
        use crate::DB;

        let messages = Arc::new(Mutex::new(Vec::new()));
        let layer = LogLayer {
            messages: Arc::clone(&messages),
        };
        let subscriber = Registry::default()
            .with(layer.with_filter(tracing_subscriber::filter::LevelFilter::INFO));
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let key = b"user-key-should-stay-out";
        let _guard = tracing::subscriber::set_default(subscriber);
        db.put(key, b"v").unwrap();
        db.flush().unwrap();
        let flush_logs = messages
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, message, _)| message == "db.flush.complete")
            .count();
        assert_eq!(flush_logs, 1);
        let dumped = messages
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, fields)| fields.clone())
            .collect::<Vec<_>>()
            .join(" ");
        assert!(!dumped.contains("user-key-should-stay-out"));
        db.put(key, b"v2").unwrap();
        db.flush().unwrap();
        db.run_compaction_once().unwrap();
        let start = messages
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, message, _)| message == "compaction start")
            .count();
        let end = messages
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, message, _)| message == "compaction end")
            .count();
        assert_eq!(start, 1);
        assert_eq!(end, 1);
        let dumped = messages
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, fields)| fields.clone())
            .collect::<Vec<_>>()
            .join(" ");
        assert!(!dumped.contains("user-key-should-stay-out"));
        drop(_guard);
        db.close().unwrap();
    }

    struct LogLayer {
        messages: Arc<Mutex<Vec<(tracing::Level, String, String)>>>,
    }

    impl<S: Subscriber> tracing_subscriber::Layer<S> for LogLayer {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut message = String::new();
            let mut fields = String::new();
            event.record(&mut Grab {
                message: &mut message,
                fields: &mut fields,
            });
            self.messages
                .lock()
                .unwrap()
                .push((*event.metadata().level(), message, fields));
        }
    }

    struct Grab<'a> {
        message: &'a mut String,
        fields: &'a mut String,
    }

    impl tracing::field::Visit for Grab<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                *self.message = format!("{value:?}").trim_matches('"').to_string();
            } else {
                self.fields
                    .push_str(&format!("{}={value:?} ", field.name()));
            }
        }

        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            self.fields.push_str(&format!("{}={value} ", field.name()));
        }

        fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
            self.fields.push_str(&format!("{}={value} ", field.name()));
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "message" {
                *self.message = value.to_string();
            } else {
                self.fields.push_str(&format!("{}={value} ", field.name()));
            }
        }
    }

    /// B 层: 走真实 DB 层验证 `min_snapshot_sequence` 确实被接进了 `CompactionJob`.
    ///
    /// A 层 (`with_snapshot_threshold`) 只证明 job 内部逻辑对; 这里补 job 与 DB 之间
    /// 的接线: `run_compaction_once` 是否把活跃快照的最小 sequence 传下去, 压缩后的
    /// 读路径还能否看到被 pin 的旧版本, 以及重开库后从 SST 重建的范围墓碑索引会不会
    /// 让已删区间复活.
    ///
    /// 直接调 `run_compaction_once()` 驱动, 不靠后台压缩 + sleep 轮询, 避免抖动.
    #[test]
    fn compaction_pins_active_snapshot_and_reload_has_no_resurrection() {
        use crate::config::Options;
        use crate::DB;

        let dir = tempdir().unwrap();
        let mut opts = Options::for_testing();
        // 每次 flush 产出独立 L0, 且任务一定够格切分
        opts.memtable_size = 64 * 1024;
        opts.level0_compaction_trigger = 2;
        opts.compaction_threads = 2;
        opts.subcompaction_min_size = 1024;
        opts.background_compaction = false;
        opts.compression = CompressionType::None;
        let db = DB::open(dir.path(), opts).unwrap();

        // 大 value: 平均记录远大于 200 字节, 旧估算 (file_size/100) 会高估一个数量级
        let v1 = vec![b'a'; 2048];
        let v2 = vec![b'b'; 2048];
        for i in 0..200 {
            db.put(format!("s{i:05}").as_bytes(), &v1).unwrap();
        }
        db.flush().unwrap();

        // 快照 pin 在 v1 这一代
        let snap = db.snapshot().unwrap();
        assert_eq!(snap.get(b"s00100").unwrap().as_deref(), Some(v1.as_slice()));

        for i in 0..200 {
            db.put(format!("s{i:05}").as_bytes(), &v2).unwrap();
            db.put(format!("d{i:05}").as_bytes(), &v1).unwrap();
        }
        db.flush().unwrap();
        db.delete_range(b"d", b"e").unwrap();
        db.flush().unwrap();

        let mut rounds = 0;
        while db.run_compaction_once().unwrap() {
            rounds += 1;
            assert!(rounds < 64, "compaction 未收敛");
        }
        assert!(rounds > 0, "压缩根本没跑, 断言无意义");

        // 1) 接线正确: 被 pin 的旧版本没被压掉
        assert_eq!(
            snap.get(b"s00100").unwrap().as_deref(),
            Some(v1.as_slice()),
            "活跃快照的旧版本被压缩丢掉了 (min_snapshot_sequence 没接进 job)"
        );
        // 2) 当前读看到新版本
        assert_eq!(db.get(b"s00100").unwrap().as_deref(), Some(v2.as_slice()));
        // 3) 范围删除生效, 切分/合并不会让已删 key 复活
        for i in 0..200 {
            assert!(
                db.get(format!("d{i:05}").as_bytes()).unwrap().is_none(),
                "已删 key 复活"
            );
        }

        drop(snap);
        drop(db);

        // 4) 重开库: 从 SST 重建范围墓碑索引后仍不能复活
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        for i in 0..200 {
            assert!(
                db.get(format!("d{i:05}").as_bytes()).unwrap().is_none(),
                "重开库后已删 key 复活"
            );
        }
        assert_eq!(db.get(b"s00100").unwrap().as_deref(), Some(v2.as_slice()));
    }
}
