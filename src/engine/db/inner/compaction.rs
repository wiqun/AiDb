//! Compaction 子模块: 文件 claim、subcompaction 与后台压缩循环.
//!
//! 入口 `run_compaction_once`: pick → claim → trivial move / subcompaction → apply.

use super::{
    sstable_path, update_sstable_metrics, AtomicBool, AtomicOrdering, CompactionJob,
    CompactionTask, Duration, Error, Receiver, Result, SSTableReader, VersionEdit, Weak, DB,
};
use crate::engine::compaction::{user_key_from_internal, UserKeyRange};
use crate::statistics::CompactionPhase;
use std::sync::Arc;

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

        // Claim files to prevent overlapping compactions from different threads
        if !self.try_claim_files(&task) {
            return Ok(true);
        }

        let level = task.level;
        tracing::info!(target: "cmp", level, "compaction start");

        // --- TRIVIAL MOVE FAST PATH ---
        if task.is_trivial_move {
            let moved = self.run_trivial_move(task)?;
            tracing::info!(target: "cmp", level, "compaction end");
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
        let results = CompactionJob::new(
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
        .run(&file_numbers)?;

        let written_bytes: u64 = results.iter().map(|r| r.file_size).sum();
        self.stats
            .compaction_written_bytes
            .fetch_add(written_bytes, AtomicOrdering::Relaxed);

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
                    let reader = Arc::new(SSTableReader::open_with_stats(
                        &result.output_path,
                        Some(Arc::clone(&self.block_cache)),
                        Some(Arc::clone(&self.stats)),
                    )?);
                    if task.output_level == 0 {
                        sst_guard[task.output_level].insert(0, reader);
                    } else {
                        sst_guard[task.output_level].push(reader);
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
        }

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
        tracing::info!(target: "cmp", level = task.level, "compaction end");
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
        }

        let pending = self.pending_compaction_bytes();
        update_sstable_metrics(&self.sstables.read(), &self.stats, pending);

        self.release_files(&task);
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
}
