//! 写路径子模块: 单 key 写与 WriteBatch 写入, WAL 落盘与 MemTable 更新.
//!
//! 写顺序不变量 (见 `mod.rs` # Invariant): WAL append 先于 MemTable 写入.
//! 冻结逻辑 (`maybe_freeze` / `wait_for_memtable_slot`) 也在此维护.

use super::{
    Arc, AtomicOrdering, CompactionPicker, Duration, Error, MemTable, OpType, Result, WalEntry,
    WriteBatch, WriteOp, DB, SEQUENCE_LIMIT,
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

    fn check_write_stall(&self) {
        if !self.options.background_compaction {
            return;
        }

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

        // 序号分配, WAL 追加, MemTable 插入与 committed 推进处于同一临界区 (与 write() 一致);
        // fsync 等待在锁外, 写入插入即可见.
        let seq;
        {
            let _guard = self.write_lock.lock();
            seq = self.alloc_sequence(1)?;
            self.write_put_to_wal(seq, key, value, false)?;
            self.memtable.read().put(key, value, seq)?;
            self.committed_sequence
                .fetch_max(seq, AtomicOrdering::SeqCst);
        }

        if self.options.sync_wal {
            self.wait_group_commit_sync(seq)?;
        }

        self.maybe_freeze()?;
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

        // 与 put() 相同: 插入与 committed 推进在写锁内完成, fsync 等待在锁外.
        let seq;
        {
            let _guard = self.write_lock.lock();
            seq = self.alloc_sequence(1)?;
            self.write_delete_to_wal(seq, key, false)?;
            self.memtable.read().delete(key, seq)?;
            self.committed_sequence
                .fetch_max(seq, AtomicOrdering::SeqCst);
        }

        if self.options.sync_wal {
            self.wait_group_commit_sync(seq)?;
        }

        self.maybe_freeze()?;
        tracing::debug!(target: "db", "db.delete");
        self.stats.operation_durations[DbOp::Delete as usize]
            .record(op_start.elapsed().as_micros() as u64);
        self.stats
            .note_engine_write(op_start.elapsed().as_micros() as u64);
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
        // 整批先校验再分配序号与写 WAL, 避免非法 key 留下前缀写入.
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
            }
        }
        self.stats
            .logical_write_bytes
            .fetch_add(batch_bytes, AtomicOrdering::Relaxed);

        let n = batch.len() as u64;
        let _guard = self.write_lock.lock();
        let lock_acquired = std::time::Instant::now();
        let base = self.alloc_sequence(n)?;

        if self.options.use_wal {
            let mut wal = self.wal.write();
            let batch_start = WalEntry {
                sequence: 0,
                op_type: OpType::BatchStart,
                has_value: true,
                key: vec![],
                value: Some((n as u32).to_le_bytes().to_vec()),
            };
            let mut encoded: Vec<Vec<u8>> = Vec::with_capacity(1 + batch.operations.len());
            encoded.push(batch_start.encode());
            for (i, op) in batch.operations.iter().enumerate() {
                encoded.push(wal_entry_for_op(op, base + i as u64)?.encode());
            }
            wal.append_encoded_write_batch(&encoded, base)?;
        }

        {
            let mt = self.memtable.read();
            for (i, op) in batch.operations.iter().enumerate() {
                let seq = base + i as u64;
                match op {
                    WriteOp::Put { key, value } => mt.put(key, value, seq)?,
                    WriteOp::Delete { key } => mt.delete(key, seq)?,
                }
            }
        }
        self.committed_sequence.fetch_max(
            base.saturating_add(n.saturating_sub(1)),
            AtomicOrdering::SeqCst,
        );
        // Release the write lock before maybe_freeze to prevent holding both
        // locks simultaneously if a freeze is triggered.
        drop(_guard);

        if self.options.sync_wal {
            let last_seq = base + n - 1;
            self.wait_group_commit_sync(last_seq)?;
        }

        let lock_hold_us = lock_acquired.elapsed().as_micros();
        self.maybe_freeze()?;
        let total_us = t0.elapsed().as_micros();
        tracing::debug!(
            target: "perf",
            op_count = batch.len(),
            total_us,
            lock_hold_us,
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
            }
        }
        self.stats
            .logical_write_bytes
            .fetch_add(batch_bytes, AtomicOrdering::Relaxed);

        let n = batch.len() as u64;
        let _guard = self.write_lock.lock();
        let lock_acquired = std::time::Instant::now();
        let base = self.alloc_sequence(n)?;

        {
            let mt = self.memtable.read();
            for (i, op) in batch.operations.iter().enumerate() {
                let seq = base + i as u64;
                match op {
                    WriteOp::Put { key, value } => {
                        Self::validate_user_key(key)?;
                        mt.put(key, value, seq)?;
                    }
                    WriteOp::Delete { key } => {
                        Self::validate_user_key(key)?;
                        mt.delete(key, seq)?;
                    }
                }
            }
        }
        self.committed_sequence.fetch_max(
            base.saturating_add(n.saturating_sub(1)),
            AtomicOrdering::SeqCst,
        );
        drop(_guard);

        let lock_hold_us = lock_acquired.elapsed().as_micros();
        self.maybe_freeze()?;
        let total_us = t0.elapsed().as_micros();
        tracing::debug!(
            target: "perf",
            op_count = batch.len(),
            total_us,
            lock_hold_us,
            "db_write_no_wal_done"
        );
        tracing::debug!(target: "db", op_count = batch.len(), "db.write_batch_no_wal");
        self.stats.operation_durations[DbOp::WriteBatchNoWal as usize]
            .record(op_start.elapsed().as_micros() as u64);
        Ok(())
    }

    /// 删除 `[start, end)` 半开区间内的全部 user key (RangeTombstone, O(1) 写入).
    pub fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<()> {
        self.check_not_closed()?;
        if start >= end {
            return Ok(());
        }
        self.check_write_stall();

        // 与 put() 相同: RangeTombstone 插入与 committed 推进在写锁内完成, fsync 等待在锁外.
        let seq;
        {
            let _guard = self.write_lock.lock();
            seq = self.alloc_sequence(1)?;
            self.write_delete_range_to_wal(seq, start, end, false)?;
            self.memtable.read().put_range_delete(start, end, seq)?;
            self.committed_sequence
                .fetch_max(seq, AtomicOrdering::SeqCst);
        }

        if self.options.sync_wal {
            self.wait_group_commit_sync(seq)?;
        }

        self.maybe_freeze()?;
        Ok(())
    }

    fn write_put_to_wal(&self, seq: u64, key: &[u8], value: &[u8], sync_now: bool) -> Result<()> {
        if !self.options.use_wal {
            return Ok(());
        }
        let entry = WalEntry {
            sequence: seq,
            op_type: OpType::TypePut,
            has_value: true,
            key: key.to_vec(),
            value: Some(value.to_vec()),
        };
        let mut wal = self.wal.write();
        wal.append(&entry.encode())?;
        wal.note_appended_sequence(seq);
        if self.options.sync_wal && sync_now {
            wal.sync()?;
        }
        Ok(())
    }

    fn write_delete_to_wal(&self, seq: u64, key: &[u8], sync_now: bool) -> Result<()> {
        if !self.options.use_wal {
            return Ok(());
        }
        let entry = WalEntry {
            sequence: seq,
            op_type: OpType::TypeDelete,
            has_value: false,
            key: key.to_vec(),
            value: None,
        };
        let mut wal = self.wal.write();
        wal.append(&entry.encode())?;
        wal.note_appended_sequence(seq);
        if self.options.sync_wal && sync_now {
            wal.sync()?;
        }
        Ok(())
    }

    fn write_delete_range_to_wal(
        &self,
        seq: u64,
        start: &[u8],
        end: &[u8],
        sync_now: bool,
    ) -> Result<()> {
        if !self.options.use_wal {
            return Ok(());
        }
        let entry = WalEntry {
            sequence: seq,
            op_type: OpType::TypeDeleteRange,
            has_value: true,
            key: start.to_vec(),
            value: Some(end.to_vec()),
        };
        let mut wal = self.wal.write();
        wal.append(&entry.encode())?;
        wal.note_appended_sequence(seq);
        if self.options.sync_wal && sync_now {
            wal.sync()?;
        }
        Ok(())
    }

    /// Group commit synchronization.
    ///
    /// When sync_wal is true, multiple concurrent writers cooperate so that
    /// a single fdatasync covers all outstanding WAL records.  The first writer
    /// to acquire group_commit_lock becomes the leader and performs the sync;
    /// subsequent writers double-check synced_seq and return immediately.
    fn wait_group_commit_sync(&self, my_seq: u64) -> Result<()> {
        // Fast path: already covered by a previous sync
        if self.group_commit_synced_seq.load(AtomicOrdering::Acquire) >= my_seq {
            return Ok(());
        }

        let _lock = self.group_commit_lock.lock();

        // Double-check: another leader might have completed while we waited
        if self.group_commit_synced_seq.load(AtomicOrdering::Acquire) >= my_seq {
            return Ok(());
        }

        // Optional batching window
        if self.options.group_commit_batch_us > 0 {
            std::thread::sleep(Duration::from_micros(self.options.group_commit_batch_us));
        }

        // One fdatasync covers all records appended so far
        let synced_seq = {
            let mut wal = self.wal.write();
            let max = wal.max_seq();
            wal.sync()?;
            max
        };

        self.group_commit_synced_seq
            .store(synced_seq, AtomicOrdering::Release);
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
        self.immutable_memtables.write().push(frozen);
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
        self.immutable_memtables.write().push(old.freeze(flush_seq));
        Ok(())
    }
}

fn wal_entry_for_op(op: &WriteOp, seq: u64) -> Result<WalEntry> {
    match op {
        WriteOp::Put { key, value } => Ok(WalEntry {
            sequence: seq,
            op_type: OpType::TypePut,
            has_value: true,
            key: key.clone(),
            value: Some(value.clone()),
        }),
        WriteOp::Delete { key } => Ok(WalEntry {
            sequence: seq,
            op_type: OpType::TypeDelete,
            has_value: false,
            key: key.clone(),
            value: None,
        }),
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
}
