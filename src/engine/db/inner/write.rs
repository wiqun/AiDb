//! 写路径子模块: 单 key 写与 WriteBatch 写入, WAL 落盘与 MemTable 更新.
//!
//! 写顺序不变量 (见 `mod.rs` # Invariant): WAL append 先于 MemTable 写入.
//! 冻结逻辑 (`maybe_freeze` / `wait_for_memtable_slot`) 也在此维护.

use super::{
    Arc, AtomicOrdering, CompactionPicker, Duration, Error, MemTable, OpType, Result, WalEntry,
    WriteBatch, WriteOp, DB, SEQUENCE_LIMIT,
};
use super::write_thread::{
    WriteGroupStatus, Writer, STATE_COMPLETED, STATE_FAILED, STATE_MEMTABLE_DONE,
};
use crate::statistics::{DbOp, WriteStallKind};

impl DB {
    fn alloc_sequence(&self, count: u64) -> Result<u64> {
        let base = self.sequence.fetch_add(count, AtomicOrdering::SeqCst) + 1;
        let last = base.saturating_add(count.saturating_sub(1));
        if last >= SEQUENCE_LIMIT {
            return Err(Error::InvalidState("sequence overflow".into()));
        }
        self.stats.sequence.store(last + 1, AtomicOrdering::Relaxed);
        Ok(base)
    }

    /// Level 0 文件过多时 stall 写入, 等待 compaction 消化.
    /// 仅在 `background_compaction = true` 时生效 (测试模式手动触发 compaction).
    ///
    /// 现在也检查 MemTable 总内存 (F-020):
    /// - Slowdown 阶段始于 60% 的 `max_write_buffer_number * memtable_size`
    /// - Stop 阶段始于 80%, 优于 `wait_for_memtable_slot()` 的硬上限, 形成梯度保护.
    fn record_write_stall(&self, kind: WriteStallKind, elapsed_us: u64) {
        let idx = kind as usize;
        self.stats.write_stall_requests[idx].fetch_add(1, AtomicOrdering::Relaxed);
        self.stats.write_stall_durations[idx].record(elapsed_us);
        self.stats
            .write_stall_max_duration_us
            .fetch_max(elapsed_us, AtomicOrdering::Relaxed);
        self.stats
            .write_stall_window_max_us
            .fetch_max(elapsed_us, AtomicOrdering::Relaxed);
    }

    #[inline(always)]
    fn check_write_stall(&self) {
        if !self.options.background_compaction {
            return;
        }
        if !self.is_write_stall_active() {
            return;
        }
        self.check_write_stall_slow();
    }

    pub(crate) fn update_write_stall_state(&self) {
        if !self.options.background_compaction {
            return;
        }
        let l0 = self.l0_sstable_count.load(AtomicOrdering::Relaxed);
        let imm_count = self.immutable_memtables.read().len();
        let should_stall = l0 >= self.options.level0_slowdown_writes_trigger
            || imm_count >= self.options.max_write_buffer_number.saturating_sub(1);
        self.set_write_stall_active(should_stall);
    }

    fn check_write_stall_slow(&self) {
        // === MemTable 总内存 stall (F-020) — 优先于 L0 检查 ===
        // MemTable 内存问题 (OOM 风险) > L0 文件数问题 (读放大).
        let mt_mem = self
            .approximate_memory_bytes()
            .saturating_sub(self.block_cache_size());
        let mt_limit = (self.options.memtable_size * self.options.max_write_buffer_number) as u64;

        if mt_mem > mt_limit.saturating_mul(4) / 5 {
            // stop: MemTable 总内存超过 80% 量级硬上限, 主动 freeze + flush 释放.
            note_stall_start(WriteStallKind::MemTableStop);
            let start = std::time::Instant::now();
            let mut fail_count = 0u32;
            loop {
                if self.memtable.read().approximate_size() > 0 {
                    let _ = self.freeze_active_if_nonempty();
                }
                if let Err(e) = self.flush_pending() {
                    fail_count += 1;
                    tracing::warn!(target: "db", error = %e, fail_count,
                        "memtable stall flush failed");
                    if fail_count >= 3 {
                        break; // 持久性失败, 避免死循环
                    }
                } else {
                    fail_count = 0;
                }
                if self
                    .approximate_memory_bytes()
                    .saturating_sub(self.block_cache_size())
                    <= mt_limit.saturating_mul(3) / 5
                {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(
                    self.options.write_stall_poll_ms,
                ));
            }
            let elapsed_us = start.elapsed().as_micros() as u64;
            self.record_write_stall(WriteStallKind::MemTableStop, elapsed_us);
            return;
        }

        if mt_mem > mt_limit.saturating_mul(3) / 5 {
            // slowdown: MemTable 总内存超 60% 硬上限, 按超出比例线性 sleep.
            let excess = (mt_mem - mt_limit.saturating_mul(3) / 5) as f64;
            let span = (mt_limit.saturating_mul(4) / 5)
                .saturating_sub(mt_limit.saturating_mul(3) / 5) as f64;
            let sleep_ms = if span > 0.0 {
                (excess / span * self.options.write_stall_slowdown_max_ms as f64) as u64
            } else {
                0
            };
            if sleep_ms > 0 {
                note_stall_start(WriteStallKind::MemTableSlowdown);
                let start = std::time::Instant::now();
                std::thread::sleep(std::time::Duration::from_millis(sleep_ms));
                let elapsed_us = start.elapsed().as_micros() as u64;
                self.record_write_stall(WriteStallKind::MemTableSlowdown, elapsed_us);
            }
        }

        // === L0 文件数 stall (现有逻辑不变) ===
        let l0_count = self.l0_sstable_count.load(AtomicOrdering::Relaxed);
        let opts = &self.options;

        // stop: 轮询等待 until L0 回到 slowdown 阈值以下
        if l0_count >= opts.level0_stop_writes_trigger {
            note_stall_start(WriteStallKind::L0FilesStop);
            let start = std::time::Instant::now();
            while self.sstables.read()[0].len() >= opts.level0_slowdown_writes_trigger {
                std::thread::sleep(std::time::Duration::from_millis(opts.write_stall_poll_ms));
                self.maybe_trigger_compaction();
            }
            let elapsed_us = start.elapsed().as_micros() as u64;
            self.record_write_stall(WriteStallKind::L0FilesStop, elapsed_us);
            return;
        }

        // slowdown: 按超出比例 sleep
        if l0_count > opts.level0_slowdown_writes_trigger {
            let excess = l0_count - opts.level0_slowdown_writes_trigger;
            let cap = opts
                .level0_stop_writes_trigger
                .saturating_sub(opts.level0_slowdown_writes_trigger);
            let sleep_ms = if cap > 0 {
                (excess as f64 / cap as f64 * opts.write_stall_slowdown_max_ms as f64) as u64
            } else {
                0
            };
            if sleep_ms > 0 {
                note_stall_start(WriteStallKind::L0FilesSlowdown);
                let start = std::time::Instant::now();
                std::thread::sleep(std::time::Duration::from_millis(sleep_ms));
                let elapsed_us = start.elapsed().as_micros() as u64;
                self.record_write_stall(WriteStallKind::L0FilesSlowdown, elapsed_us);
            }
        }

        // === L1+ level size stall (F-029) ===
        // 检查 L1 到 L(max-2) 各层大小是否超出目标值.
        // L0 由文件数 stall 处理, 最后一层不限制.
        let picker = CompactionPicker::from_options(opts);
        for level_index in 1..(opts.max_levels.saturating_sub(1)) {
            let actual = CompactionPicker::calculate_level_size(&self.sstables.read()[level_index]);
            let target = picker.target_size_for_level(level_index);
            if target == 0 {
                continue;
            }

            if actual > target.saturating_mul(4) {
                note_stall_start(WriteStallKind::LevelSizeStop);
                let start = std::time::Instant::now();
                // stop: 轮询等待, 主动触发 compaction
                while {
                    let tables = self.sstables.read();
                    CompactionPicker::calculate_level_size(&tables[level_index])
                        > target.saturating_mul(2)
                } {
                    std::thread::sleep(std::time::Duration::from_millis(opts.write_stall_poll_ms));
                    self.maybe_trigger_compaction();
                }
                let elapsed_us = start.elapsed().as_micros() as u64;
                self.record_write_stall(WriteStallKind::LevelSizeStop, elapsed_us);
                return;
            }

            if actual > target.saturating_mul(2) {
                // slowdown: 按超出比例 sleep
                let excess = (actual - target.saturating_mul(2)) as f64;
                let cap = (target.saturating_mul(2)) as f64;
                let sleep_ms = if cap > 0.0 {
                    (excess / cap * opts.write_stall_slowdown_max_ms as f64) as u64
                } else {
                    0
                };
                if sleep_ms > 0 {
                    note_stall_start(WriteStallKind::LevelSizeSlowdown);
                    let start = std::time::Instant::now();
                    std::thread::sleep(std::time::Duration::from_millis(sleep_ms));
                    let elapsed_us = start.elapsed().as_micros() as u64;
                    self.record_write_stall(WriteStallKind::LevelSizeSlowdown, elapsed_us);
                }
                break; // 一次只 stall 最严重的一层
            }
        }
        self.update_write_stall_state();
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let op_start = std::time::Instant::now();
        self.check_not_closed()?;
        Self::validate_user_key(key)?;
        self.check_write_stall();
        self.stats.operations[DbOp::Put as usize].fetch_add(1, AtomicOrdering::Relaxed);
        self.stats
            .logical_write_bytes
            .fetch_add((key.len() + value.len()) as u64, AtomicOrdering::Relaxed);

        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write_internal(&batch, true, self.options.sync_wal)?;

        tracing::debug!(target: "db", "db.put");
        self.stats.operation_durations[DbOp::Put as usize]
            .record(op_start.elapsed().as_micros() as u64);
        self.stats
            .note_engine_write(op_start.elapsed().as_micros() as u64);
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let op_start = std::time::Instant::now();
        self.check_not_closed()?;
        Self::validate_user_key(key)?;
        self.check_write_stall();
        self.stats.operations[DbOp::Delete as usize].fetch_add(1, AtomicOrdering::Relaxed);
        self.stats
            .logical_write_bytes
            .fetch_add(key.len() as u64, AtomicOrdering::Relaxed);

        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write_internal(&batch, true, self.options.sync_wal)?;

        tracing::debug!(target: "db", "db.delete");
        self.stats.operation_durations[DbOp::Delete as usize]
            .record(op_start.elapsed().as_micros() as u64);
        self.stats
            .note_engine_write(op_start.elapsed().as_micros() as u64);
        Ok(())
    }

    pub fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<()> {
        self.check_not_closed()?;
        if start >= end {
            return Ok(());
        }
        self.check_write_stall();
        let mut batch = WriteBatch::new();
        batch.delete_range(start, end);
        self.write_internal(&batch, true, self.options.sync_wal)?;
        Ok(())
    }

    pub fn write(&self, batch: &WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let t0 = std::time::Instant::now();
        let op_start = std::time::Instant::now();
        self.check_not_closed()?;
        self.check_write_stall();
        self.stats.operations[DbOp::WriteBatch as usize].fetch_add(1, AtomicOrdering::Relaxed);

        let mut batch_bytes = 0u64;
        for op in &batch.operations {
            match op {
                WriteOp::Put { key, value } => {
                    Self::validate_user_key(key)?;
                    batch_bytes += (key.len() + value.len()) as u64;
                }
                WriteOp::Delete { key } => {
                    Self::validate_user_key(key)?;
                    batch_bytes += key.len() as u64;
                }
                WriteOp::DeleteRange { start, end } => {
                    batch_bytes += (start.len() + end.len()) as u64;
                }
            }
        }
        self.stats
            .logical_write_bytes
            .fetch_add(batch_bytes, AtomicOrdering::Relaxed);

        self.write_internal(batch, true, self.options.sync_wal)?;

        let total_us = t0.elapsed().as_micros();
        tracing::debug!(
            target: "perf",
            op_count = batch.len(),
            total_us,
            "db_write_done"
        );
        tracing::debug!(target: "db", op_count = batch.len(), "db.write_batch");
        self.stats.operation_durations[DbOp::WriteBatch as usize]
            .record(op_start.elapsed().as_micros() as u64);
        self.stats
            .note_engine_write(op_start.elapsed().as_micros() as u64);
        Ok(())
    }

    /// 写入 WriteBatch 但不经由 DB 自身的 WAL (适用于 StateMachine 写入, 避免双重 WAL 磁盘开销).
    pub fn write_without_wal(&self, batch: &WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let t0 = std::time::Instant::now();
        let op_start = std::time::Instant::now();
        self.check_not_closed()?;
        self.check_write_stall();
        self.stats.operations[DbOp::WriteBatchNoWal as usize].fetch_add(1, AtomicOrdering::Relaxed);
        let mut batch_bytes = 0u64;
        for op in &batch.operations {
            match op {
                WriteOp::Put { key, value } => batch_bytes += (key.len() + value.len()) as u64,
                WriteOp::Delete { key } => batch_bytes += key.len() as u64,
                WriteOp::DeleteRange { start, end } => {
                    batch_bytes += (start.len() + end.len()) as u64;
                }
            }
        }
        self.stats
            .logical_write_bytes
            .fetch_add(batch_bytes, AtomicOrdering::Relaxed);

        self.write_internal(batch, false, false)?;

        let total_us = t0.elapsed().as_micros();
        tracing::debug!(
            target: "perf",
            op_count = batch.len(),
            total_us,
            "db_write_no_wal_done"
        );
        tracing::debug!(target: "db", op_count = batch.len(), "db.write_batch_no_wal");
        self.stats.operation_durations[DbOp::WriteBatchNoWal as usize]
            .record(op_start.elapsed().as_micros() as u64);
        Ok(())
    }

    /// 基于 WriteThread 协作状态机与 Leader Group Commit 的统一内部写入管线 (ADR-006).
    fn write_internal(&self, batch: &WriteBatch, use_wal: bool, sync_wal: bool) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let parker = crossbeam::sync::Parker::new();
        let unparker = parker.unparker().clone();
        let mut writer = Writer::new(batch, unparker, use_wal, sync_wal);

        match self.write_thread.join_batch_group(&mut writer, &parker)? {
            WriteGroupStatus::Leader(leader) => {
                let group = &leader.group;
                let mut total_ops = 0u64;
                for &w_ptr in group {
                    let w = unsafe { &*w_ptr };
                    let b = unsafe { &*w.batch };
                    total_ops += b.len() as u64;
                }

                if total_ops == 0 {
                    for (idx, &w_ptr) in group.iter().enumerate() {
                        if idx > 0 {
                            let w = unsafe { &mut *w_ptr };
                            w.state.store(STATE_COMPLETED, AtomicOrdering::Release);
                            w.unparker.unpark();
                        }
                    }
                    return Ok(());
                }

                let base_seq = match self.alloc_sequence(total_ops) {
                    Ok(seq) => seq,
                    Err(e) => {
                        self.write_thread.fail_group(group, &e.to_string());
                        return Err(e);
                    }
                };

                let mut cur_seq = base_seq;
                for &w_ptr in group {
                    let w = unsafe { &mut *w_ptr };
                    let b = unsafe { &*w.batch };
                    w.seq = cur_seq;
                    cur_seq += b.len() as u64;
                }

                if self.options.use_wal {
                    let mut encoded: Vec<Vec<u8>> =
                        Vec::with_capacity(total_ops as usize + group.len());
                    for &w_ptr in group {
                        let w = unsafe { &*w_ptr };
                        if !w.use_wal || unsafe { (*w.batch).is_empty() } {
                            continue;
                        }
                        let b = unsafe { &*w.batch };
                        if b.len() > 1 {
                            let mut start_buf = Vec::new();
                            WalEntry::encode_to_buf(
                                0,
                                OpType::BatchStart,
                                true,
                                &[],
                                Some(&(b.len() as u32).to_le_bytes()),
                                &mut start_buf,
                            );
                            encoded.push(start_buf);
                        }
                        for (i, op) in b.operations.iter().enumerate() {
                            let seq = w.seq + i as u64;
                            let mut op_buf = Vec::new();
                            match op {
                                WriteOp::Put { key, value } => {
                                    WalEntry::encode_to_buf(
                                        seq,
                                        OpType::TypePut,
                                        true,
                                        key,
                                        Some(value),
                                        &mut op_buf,
                                    );
                                }
                                WriteOp::Delete { key } => {
                                    WalEntry::encode_to_buf(
                                        seq,
                                        OpType::TypeDelete,
                                        false,
                                        key,
                                        None,
                                        &mut op_buf,
                                    );
                                }
                                WriteOp::DeleteRange { start, end } => {
                                    WalEntry::encode_to_buf(
                                        seq,
                                        OpType::TypeDeleteRange,
                                        true,
                                        start,
                                        Some(end),
                                        &mut op_buf,
                                    );
                                }
                            }
                            encoded.push(op_buf);
                        }
                    }
                    if !encoded.is_empty() {
                        let mut wal = self.wal.write();
                        if let Err(e) = wal.append_encoded_write_batch(&encoded, base_seq) {
                            self.write_thread.fail_group(group, &e.to_string());
                            return Err(e);
                        }
                    }
                }

                // Leader 统一将当前 Group 中所有批次顺序写入 MemTable (消除 Followers 线程上下文切换与自旋空转)
                for &w_ptr in group {
                    let w = unsafe { &*w_ptr };
                    let b = unsafe { &*w.batch };
                    self.insert_batch_to_memtable(b, w.seq)?;
                }

                // 原子推进 committed_sequence (短暂持有 write_lock 与 snapshot 注册保持互斥)
                let last_seq = base_seq + total_ops - 1;
                {
                    let _guard = self.write_lock.lock();
                    self.committed_sequence
                        .fetch_max(last_seq, AtomicOrdering::SeqCst);
                }

                // 如果任何一个写入者请求 sync WAL, 则执行 sync
                let need_sync = self.options.sync_wal
                    || group.iter().any(|&w_ptr| unsafe { (*w_ptr).sync });
                if need_sync && self.options.use_wal {
                    let mut wal = self.wal.write();
                    if let Err(e) = wal.sync() {
                        self.write_thread.fail_group(group, &e.to_string());
                        return Err(e);
                    }
                    self.group_commit_synced_seq
                        .store(last_seq, AtomicOrdering::Release);
                }

                // 一次性唤醒全部 Followers 完成写入 (消除二次 park 与二次唤醒开销)
                for (idx, &w_ptr) in group.iter().enumerate() {
                    if idx > 0 {
                        let w = unsafe { &mut *w_ptr };
                        w.state.store(STATE_COMPLETED, AtomicOrdering::Release);
                        w.unparker.unpark();
                    }
                }

                self.maybe_freeze()?;
                Ok(())
            }
            WriteGroupStatus::FollowerMemtable(seq) => {
                let b = unsafe { &*writer.batch };
                self.insert_batch_to_memtable(b, seq)?;
                writer.state.store(STATE_MEMTABLE_DONE, AtomicOrdering::Release);

                while writer.state.load(AtomicOrdering::Acquire) != STATE_COMPLETED {
                    if writer.state.load(AtomicOrdering::Acquire) == STATE_FAILED {
                        let msg = writer
                            .error
                            .clone()
                            .unwrap_or_else(|| "write group failed".to_string());
                        return Err(Error::InvalidState(msg));
                    }
                    parker.park_timeout(Duration::from_micros(50));
                }
                Ok(())
            }
            WriteGroupStatus::Completed => Ok(()),
        }
    }

    fn insert_batch_to_memtable(&self, batch: &WriteBatch, base_seq: u64) -> Result<()> {
        let mt = self.memtable.read();
        for (i, op) in batch.operations.iter().enumerate() {
            let seq = base_seq + i as u64;
            match op {
                WriteOp::Put { key, value } => {
                    mt.put(key, value, seq)?;
                    self.row_cache.invalidate(key);
                }
                WriteOp::Delete { key } => {
                    mt.delete(key, seq)?;
                    self.row_cache.invalidate(key);
                }
                WriteOp::DeleteRange { start, end } => {
                    mt.put_range_delete(start, end, seq)?;
                    self.row_cache.clear();
                }
            }
        }
        Ok(())
    }

    fn maybe_freeze(&self) -> Result<()> {
        if self.memtable.read().approximate_size() < self.options.memtable_size {
            return Ok(());
        }
        self.wait_for_memtable_slot()?;
        let mut mem = self.memtable.write();
        if mem.approximate_size() < self.options.memtable_size {
            return Ok(());
        }
        let flush_seq = self.sequence.load(AtomicOrdering::SeqCst);
        let old = std::mem::take(&mut *mem);
        let frozen = old.freeze(flush_seq);
        self.immutable_memtables.write().push(Arc::new(frozen));
        self.update_write_stall_state();
        Ok(())
    }

    /// 当 Immutable MemTable 数达到 `max_write_buffer_number` 上限时阻塞写入, 并驱动 flush.
    fn wait_for_memtable_slot(&self) -> Result<()> {
        for _ in 0..self.options.memtable_wait_iters {
            if self.immutable_memtables.read().len() + 1 < self.options.max_write_buffer_number {
                return Ok(());
            }
            self.flush_pending()?;
            if self.immutable_memtables.read().len() + 1 < self.options.max_write_buffer_number {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(
                self.options.memtable_wait_interval_ms,
            ));
        }
        Err(Error::Busy(
            "too many immutable memtables waiting flush".into(),
        ))
    }

    pub(super) fn freeze_active_if_nonempty(&self) -> Result<()> {
        let mut mem = self.memtable.write();
        if mem.approximate_size() == 0 {
            return Ok(());
        }
        let flush_seq = self.sequence.load(AtomicOrdering::SeqCst);
        let old = std::mem::replace(
            &mut *mem,
            MemTable::new_with_stats(Some(Arc::clone(&self.stats))),
        );
        self.immutable_memtables.write().push(Arc::new(old.freeze(flush_seq)));
        Ok(())
    }
}

fn note_stall_start(kind: WriteStallKind) {
    let (cause, kind_name) = kind.cause_and_type();
    tracing::warn!(target: "db", cause, kind = kind_name, "write stall start");
}

#[cfg(test)]
mod sequence_tests {
    use super::*;
    use crate::config::Options;
    use crate::engine::memtable::SEQUENCE_LIMIT;
    use crate::error::Error;
    use tempfile::tempdir;
    use tracing::Subscriber;

    #[test]
    fn test_sequence_overflow_on_put() {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        db.sequence
            .store(SEQUENCE_LIMIT - 1, AtomicOrdering::SeqCst);
        assert!(matches!(
            db.put(b"overflow", b"x"),
            Err(Error::InvalidState(_))
        ));
        db.close().unwrap();
    }

    #[test]
    fn write_stall_start_warns_once() {
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::prelude::*;
        use tracing_subscriber::Registry;

        let messages = Arc::new(Mutex::new(Vec::new()));
        let layer = MessageLayer {
            messages: Arc::clone(&messages),
        };
        let subscriber = Registry::default().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            super::note_stall_start(WriteStallKind::L0FilesStop);
        });
        let messages = messages.lock().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].0, tracing::Level::WARN);
        assert_eq!(messages[0].1, "write stall start");
        assert!(messages[0].2.contains("cause=l0"));
        assert!(messages[0].2.contains("kind=stop"));
        assert!(!messages[0].2.contains("key="));
    }

    struct MessageLayer {
        messages: std::sync::Arc<std::sync::Mutex<Vec<(tracing::Level, String, String)>>>,
    }

    impl<S: Subscriber> tracing_subscriber::Layer<S> for MessageLayer {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut fields = String::new();
            let mut message = String::new();
            event.record(&mut FieldGrab {
                message: &mut message,
                fields: &mut fields,
            });
            self.messages
                .lock()
                .unwrap()
                .push((*event.metadata().level(), message, fields));
        }
    }

    struct FieldGrab<'a> {
        message: &'a mut String,
        fields: &'a mut String,
    }

    impl tracing::field::Visit for FieldGrab<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                *self.message = format!("{value:?}").trim_matches('"').to_string();
            } else {
                self.fields
                    .push_str(&format!("{}={value:?} ", field.name()));
            }
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "message" {
                *self.message = value.to_string();
            } else {
                self.fields.push_str(&format!("{}={value} ", field.name()));
            }
        }
    }

    #[test]
    fn test_write_stall_fast_path_active_flag() {
        let temp = tempfile::tempdir().unwrap();
        let opts = crate::config::Options::default();
        let db = DB::open(temp.path(), opts).unwrap();

        // 默认状态下无 stall, 标志应为 false, 写入直接短路
        assert!(!db.is_write_stall_active());

        // 写入普通数据
        db.put(b"k1", b"v1").unwrap();
        assert_eq!(db.get(b"k1").unwrap().unwrap(), b"v1");
        assert!(!db.is_write_stall_active());
    }
}
