//! 写路径子模块: 单 key 写与 WriteBatch 写入, WAL 落盘与 MemTable 更新.
//!
//! 写顺序不变量 (见 `mod.rs` # Invariant): WAL append 先于 MemTable 写入.
//! 冻结逻辑 (`maybe_freeze` / `wait_for_memtable_slot`) 也在此维护.

use super::write_thread::{
    WriteGroupStatus, Writer, STATE_COMPLETED, STATE_FAILED, STATE_MEMTABLE_DONE,
};
use super::{
    Arc, AtomicOrdering, CompactionPicker, Duration, Error, MemTable, Mutex, OpType, Result,
    WalEntry, WriteBatch, WriteOp, DB, SEQUENCE_LIMIT,
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

    /// Raft apply 等内部高优先写入只保留 OOM 级 MemTableStop 保护, 免除 slowdown 与 L0 stall.
    #[inline(always)]
    fn check_write_stall_for_apply(&self) {
        if !self.options.background_compaction || !self.is_write_stall_active() {
            return;
        }
        let mt_mem = self
            .approximate_memory_bytes()
            .saturating_sub(self.block_cache_size());
        let mt_limit = (self.options.memtable_size * self.options.max_write_buffer_number) as u64;
        if mt_mem > mt_limit.saturating_mul(4) / 5 {
            self.wait_memtable_stop(mt_limit);
        }
    }

    fn apply_smooth_delay(&self, kind: WriteStallKind, ratio: f64) {
        let max_ms = self.options.write_stall_slowdown_max_ms;
        if max_ms == 0 || ratio <= 0.0 {
            return;
        }
        let max_cap_ns = max_ms.saturating_mul(1_000_000);
        let step_ns = ((ratio.clamp(0.0, 1.0) * max_cap_ns as f64) / 20.0)
            .clamp(50_000.0, 5_000_000.0) as u64;
        let now_ns = self.write_delay_epoch.elapsed().as_nanos() as u64;

        let mut target_ns = now_ns;
        let _ = self.write_delay_next_ns.try_update(
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
            |prev| {
                let base = prev.max(now_ns);
                let next = base
                    .saturating_add(step_ns)
                    .min(now_ns.saturating_add(max_cap_ns));
                target_ns = next;
                Some(next)
            },
        );

        let wait_ns = target_ns.saturating_sub(now_ns);
        if wait_ns > 0 {
            self.note_stall_start(kind);
            let start = std::time::Instant::now();
            std::thread::sleep(std::time::Duration::from_nanos(wait_ns));
            self.record_write_stall(kind, start.elapsed().as_micros() as u64);
        }
    }

    fn wait_memtable_stop(&self, mt_limit: u64) {
        self.note_stall_start(WriteStallKind::MemTableStop);
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
        let mt_mem = self
            .approximate_memory_bytes()
            .saturating_sub(self.block_cache_size());
        let mt_limit = (self.options.memtable_size * self.options.max_write_buffer_number) as u64;

        if mt_mem > mt_limit.saturating_mul(4) / 5 {
            self.wait_memtable_stop(mt_limit);
            return;
        }

        if mt_mem > mt_limit.saturating_mul(3) / 5 {
            let excess = (mt_mem - mt_limit.saturating_mul(3) / 5) as f64;
            let span = (mt_limit.saturating_mul(4) / 5)
                .saturating_sub(mt_limit.saturating_mul(3) / 5) as f64;
            if span > 0.0 {
                self.apply_smooth_delay(WriteStallKind::MemTableSlowdown, excess / span);
            }
        }

        // === L0 文件数 stall ===
        let l0_count = self.l0_sstable_count.load(AtomicOrdering::Relaxed);
        let opts = &self.options;

        // stop: 仅阻塞到回落至 stop 阈值以下即解除全停顿, 转由 slowdown 平滑限速
        if l0_count >= opts.level0_stop_writes_trigger {
            self.note_stall_start(WriteStallKind::L0FilesStop);
            let start = std::time::Instant::now();
            while self.sstables.read()[0].len() >= opts.level0_stop_writes_trigger {
                std::thread::sleep(std::time::Duration::from_millis(opts.write_stall_poll_ms));
                self.maybe_trigger_compaction();
            }
            let elapsed_us = start.elapsed().as_micros() as u64;
            self.record_write_stall(WriteStallKind::L0FilesStop, elapsed_us);
            return;
        }

        // slowdown: 令牌桶微秒级平滑限速
        if l0_count > opts.level0_slowdown_writes_trigger {
            let excess = l0_count - opts.level0_slowdown_writes_trigger;
            let cap = opts
                .level0_stop_writes_trigger
                .saturating_sub(opts.level0_slowdown_writes_trigger);
            if cap > 0 {
                self.apply_smooth_delay(
                    WriteStallKind::L0FilesSlowdown,
                    excess as f64 / cap as f64,
                );
            }
        }

        // === L1+ level size stall (F-029) ===
        let picker = CompactionPicker::from_options(opts);
        for level_index in 1..(opts.max_levels.saturating_sub(1)) {
            let actual = CompactionPicker::calculate_level_size(&self.sstables.read()[level_index]);
            let target = picker.target_size_for_level(level_index);
            if target == 0 {
                continue;
            }

            if actual > target.saturating_mul(4) {
                self.note_stall_start(WriteStallKind::LevelSizeStop);
                let start = std::time::Instant::now();
                while {
                    let tables = self.sstables.read();
                    CompactionPicker::calculate_level_size(&tables[level_index])
                        > target.saturating_mul(4)
                } {
                    std::thread::sleep(std::time::Duration::from_millis(opts.write_stall_poll_ms));
                    self.maybe_trigger_compaction();
                }
                let elapsed_us = start.elapsed().as_micros() as u64;
                self.record_write_stall(WriteStallKind::LevelSizeStop, elapsed_us);
                return;
            }

            if actual > target.saturating_mul(2) {
                let excess = (actual - target.saturating_mul(2)) as f64;
                let cap = (target.saturating_mul(2)) as f64;
                if cap > 0.0 {
                    self.apply_smooth_delay(WriteStallKind::LevelSizeSlowdown, excess / cap);
                }
                break;
            }
        }
        self.update_write_stall_state();
        if !self.is_write_stall_active() {
            exit_stall_log(&self.write_stall_state, self.db_id());
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

        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write_internal(&batch, true, self.options.sync_wal)?;

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

        let _total_us = t0.elapsed().as_micros();
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
        self.check_write_stall_for_apply();
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

        let _total_us = t0.elapsed().as_micros();
        self.stats.operation_durations[DbOp::WriteBatchNoWal as usize]
            .record(op_start.elapsed().as_micros() as u64);
        Ok(())
    }

    /// 基于 WriteThread 协作状态机与 Leader Group Commit 的统一内部写入管线 (ADR-006).
    fn write_internal(&self, batch: &WriteBatch, use_wal: bool, sync_wal: bool) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let started = std::time::Instant::now();
        let parker = crossbeam::sync::Parker::new();
        let unparker = parker.unparker().clone();
        let mut writer = Writer::new(batch, unparker, use_wal, sync_wal);

        let join_started = std::time::Instant::now();
        match self.write_thread.join_batch_group(&mut writer, &parker)? {
            WriteGroupStatus::Leader(leader) => {
                let join = join_started.elapsed();
                let group = &leader.group;
                let mut total_ops = 0u64;
                for &w_ptr in group {
                    let w = unsafe { &*w_ptr };
                    let b = unsafe { &*w.batch };
                    total_ops += b.len() as u64;
                }

                if total_ops == 0 {
                    leader.complete();
                    return Ok(());
                }

                let base_seq = match self.alloc_sequence(total_ops) {
                    Ok(seq) => seq,
                    Err(e) => {
                        leader.fail(&e.to_string());
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

                let wal_started = std::time::Instant::now();
                if self.options.use_wal {
                    let mut wal_buf: Vec<u8> = Vec::with_capacity((total_ops as usize) * 96);
                    let mut wal_ranges: Vec<(usize, usize)> =
                        Vec::with_capacity(total_ops as usize + group.len());
                    for &w_ptr in group {
                        let w = unsafe { &*w_ptr };
                        if !w.use_wal || unsafe { (*w.batch).is_empty() } {
                            continue;
                        }
                        let b = unsafe { &*w.batch };
                        if b.len() > 1 {
                            let start = wal_buf.len();
                            WalEntry::encode_to_buf(
                                0,
                                OpType::BatchStart,
                                true,
                                &[],
                                Some(&(b.len() as u32).to_le_bytes()),
                                &mut wal_buf,
                            );
                            wal_ranges.push((start, wal_buf.len()));
                        }
                        for (i, op) in b.operations.iter().enumerate() {
                            let seq = w.seq + i as u64;
                            let start = wal_buf.len();
                            match op {
                                WriteOp::Put { key, value } => {
                                    WalEntry::encode_to_buf(
                                        seq,
                                        OpType::TypePut,
                                        true,
                                        key,
                                        Some(value),
                                        &mut wal_buf,
                                    );
                                }
                                WriteOp::Delete { key } => {
                                    WalEntry::encode_to_buf(
                                        seq,
                                        OpType::TypeDelete,
                                        false,
                                        key,
                                        None,
                                        &mut wal_buf,
                                    );
                                }
                                WriteOp::DeleteRange { start: rs, end } => {
                                    WalEntry::encode_to_buf(
                                        seq,
                                        OpType::TypeDeleteRange,
                                        true,
                                        rs,
                                        Some(end),
                                        &mut wal_buf,
                                    );
                                }
                            }
                            wal_ranges.push((start, wal_buf.len()));
                        }
                    }
                    if !wal_ranges.is_empty() {
                        let mut wal = self.wal.write();
                        if let Err(e) = wal.append_encoded_ranges(&wal_buf, &wal_ranges, base_seq) {
                            leader.fail(&e.to_string());
                            return Err(e);
                        }
                    }
                }

                let wal = wal_started.elapsed();
                let memtable_started = std::time::Instant::now();
                // 广播 STATE_MEMTABLE 让 Followers 与 Leader 并发向无锁 SkipMap 插入各自批次
                if group.len() > 1 {
                    leader.launch_memtable();
                    let w0 = unsafe { &*group[0] };
                    let b0 = unsafe { &*w0.batch };
                    let leader_res = self.insert_batch_to_memtable(b0, w0.seq);
                    let followers_res = leader.wait_memtable_done();
                    if let Err(e) = leader_res.and(followers_res) {
                        leader.fail(&e.to_string());
                        return Err(e);
                    }
                } else {
                    let w0 = unsafe { &*group[0] };
                    let b0 = unsafe { &*w0.batch };
                    if let Err(e) = self.insert_batch_to_memtable(b0, w0.seq) {
                        leader.fail(&e.to_string());
                        return Err(e);
                    }
                }

                // 原子推进 committed_sequence (短暂持有 write_lock 与 snapshot 注册保持互斥)
                let last_seq = base_seq + total_ops - 1;
                {
                    let _guard = self.write_lock.lock();
                    self.committed_sequence
                        .fetch_max(last_seq, AtomicOrdering::SeqCst);
                }

                let memtable = memtable_started.elapsed();
                // 如果任何一个写入者请求 sync WAL, 则执行 sync
                let mut sync = Duration::ZERO;
                let need_sync =
                    self.options.sync_wal || group.iter().any(|&w_ptr| unsafe { (*w_ptr).sync });
                if need_sync && self.options.use_wal {
                    let sync_started = std::time::Instant::now();
                    let mut wal_file = self.wal.write();
                    if let Err(e) = wal_file.sync() {
                        leader.fail(&e.to_string());
                        return Err(e);
                    }
                    sync = sync_started.elapsed();
                    self.group_commit_synced_seq
                        .store(last_seq, AtomicOrdering::Release);
                }

                // 一次性唤醒全部 Followers 完成写入并释放 leader_lock, 再在锁外检查 maybe_freeze
                leader.complete();
                drop(leader);

                let freeze_started = std::time::Instant::now();
                self.maybe_freeze()?;
                note_slow_db_write(
                    use_wal,
                    sync_wal,
                    total_ops,
                    started.elapsed(),
                    join,
                    wal.saturating_add(sync),
                    memtable,
                    freeze_started.elapsed(),
                    Duration::ZERO,
                );
                Ok(())
            }
            WriteGroupStatus::FollowerMemtable(seq) => {
                let join = join_started.elapsed();
                let b = unsafe { &*writer.batch };
                let memtable_started = std::time::Instant::now();
                if let Err(e) = self.insert_batch_to_memtable(b, seq) {
                    writer.error = Some(e.to_string());
                }
                let memtable = memtable_started.elapsed();
                writer
                    .state
                    .store(STATE_MEMTABLE_DONE, AtomicOrdering::Release);

                let wait_started = std::time::Instant::now();
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
                note_slow_db_write(
                    use_wal,
                    sync_wal,
                    batch.len() as u64,
                    started.elapsed(),
                    join,
                    Duration::ZERO,
                    memtable,
                    Duration::ZERO,
                    wait_started.elapsed(),
                );
                Ok(())
            }
            WriteGroupStatus::Completed => Ok(()),
        }
    }

    fn insert_batch_to_memtable(&self, batch: &WriteBatch, base_seq: u64) -> Result<()> {
        let mt = self.memtable.read();
        let before_size = self
            .write_buffer_manager
            .as_ref()
            .map(|_| mt.approximate_size());
        for (i, op) in batch.operations.iter().enumerate() {
            let seq = base_seq + i as u64;
            match op {
                WriteOp::Put { key, value } => {
                    mt.put(key, value, seq)?;
                    self.row_cache.invalidate_ns(self.db_cache_id, key);
                }
                WriteOp::Delete { key } => {
                    mt.delete(key, seq)?;
                    self.row_cache.invalidate_ns(self.db_cache_id, key);
                }
                WriteOp::DeleteRange { start, end } => {
                    mt.put_range_delete(start, end, seq)?;
                    self.row_cache.clear();
                }
            }
        }
        if let (Some(ref wbm), Some(before)) = (&self.write_buffer_manager, before_size) {
            let delta = mt.approximate_size().saturating_sub(before);
            wbm.reserve_mem(delta);
        }
        Ok(())
    }

    fn maybe_freeze(&self) -> Result<()> {
        let wbm_flush = self
            .write_buffer_manager
            .as_ref()
            .is_some_and(|wbm| wbm.should_flush());
        let active_size = self.memtable.read().approximate_size();
        if active_size < self.options.memtable_size && !(wbm_flush && active_size > 0) {
            return Ok(());
        }
        self.wait_for_memtable_slot()?;
        let mut mem = self.memtable.write();
        let active_size = mem.approximate_size();
        let wbm_flush = self
            .write_buffer_manager
            .as_ref()
            .is_some_and(|wbm| wbm.should_flush());
        if active_size < self.options.memtable_size && !(wbm_flush && active_size > 0) {
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
        self.immutable_memtables
            .write()
            .push(Arc::new(old.freeze(flush_seq)));
        Ok(())
    }
    fn note_stall_start(&self, kind: WriteStallKind) {
        let l0_files = self.l0_sstable_count.load(AtomicOrdering::Relaxed);
        let memtable_count = 1 + self.immutable_memtables.read().len();
        enter_stall_log(
            &self.write_stall_state,
            self.db_id(),
            kind,
            l0_files,
            memtable_count,
        );
    }
}

fn enter_stall_log(
    state: &Mutex<Option<(WriteStallKind, std::time::Instant)>>,
    db_id: &str,
    kind: WriteStallKind,
    l0_files: usize,
    memtable_count: usize,
) {
    let mut guard = state.lock();
    if let Some((active, _)) = *guard {
        if active == kind {
            return;
        }
    }
    *guard = Some((kind, std::time::Instant::now()));
    let (cause, kind_name) = kind.cause_and_type();
    tracing::warn!(
        target: "db",
        db_id,
        cause,
        kind = kind_name,
        l0_files,
        memtable_count,
        "write stall start"
    );
}

fn exit_stall_log(state: &Mutex<Option<(WriteStallKind, std::time::Instant)>>, db_id: &str) {
    let prev = state.lock().take();
    if let Some((kind, started)) = prev {
        let (cause, kind_name) = kind.cause_and_type();
        tracing::info!(
            target: "db",
            db_id,
            cause,
            kind = kind_name,
            stall_duration_ms = started.elapsed().as_millis() as u64,
            "write stall end"
        );
    }
}

/// 与慢日志默认阈值一致. 快路径只做几次 `Instant` 读数.
const SLOW_DB_WRITE: Duration = Duration::from_millis(100);

fn db_write_is_slow(total: Duration) -> bool {
    total >= SLOW_DB_WRITE
}

#[allow(clippy::too_many_arguments)]
fn note_slow_db_write(
    use_wal: bool,
    sync_wal: bool,
    ops: u64,
    total: Duration,
    join: Duration,
    wal: Duration,
    memtable: Duration,
    freeze: Duration,
    wait: Duration,
) {
    if !db_write_is_slow(total) {
        return;
    }
    tracing::warn!(
        use_wal,
        sync_wal,
        ops,
        total_us = total.as_micros() as u64,
        join_us = join.as_micros() as u64,
        wal_us = wal.as_micros() as u64,
        memtable_us = memtable.as_micros() as u64,
        freeze_us = freeze.as_micros() as u64,
        wait_us = wait.as_micros() as u64,
        "db_write_slow"
    );
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
        let state = parking_lot::Mutex::new(None);
        tracing::subscriber::with_default(subscriber, || {
            super::enter_stall_log(&state, "testdb", WriteStallKind::L0FilesStop, 12, 2);
            super::enter_stall_log(&state, "testdb", WriteStallKind::L0FilesStop, 12, 2);
            super::exit_stall_log(&state, "testdb");
            super::exit_stall_log(&state, "testdb");
        });
        let messages = messages.lock().unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].0, tracing::Level::WARN);
        assert_eq!(messages[0].1, "write stall start");
        assert!(messages[0].2.contains("cause=l0"));
        assert!(messages[0].2.contains("kind=stop"));
        assert!(!messages[0].2.contains("key="));
        assert_eq!(messages[1].0, tracing::Level::INFO);
        assert_eq!(messages[1].1, "write stall end");
        assert!(messages[1].2.contains("stall_duration_ms="));
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
    fn slow_db_write_matches_slowlog_threshold() {
        assert!(!db_write_is_slow(Duration::from_millis(99)));
        assert!(db_write_is_slow(Duration::from_millis(100)));
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

    #[test]
    fn smooth_delay_caps_single_write_spike() {
        let temp = tempfile::tempdir().unwrap();
        let mut opts = crate::config::Options::for_testing();
        opts.write_stall_slowdown_max_ms = 100;
        let db = DB::open(temp.path(), opts).unwrap();

        let t0 = std::time::Instant::now();
        for _ in 0..5 {
            db.apply_smooth_delay(WriteStallKind::L0FilesSlowdown, 1.0);
        }
        let elapsed = t0.elapsed();
        // 5 次步进约 5 * 5ms = 25ms, 远低于旧版每次直接 sleep 100ms
        assert!(elapsed < Duration::from_millis(80), "elapsed={elapsed:?}");
        assert_eq!(
            db.stats.write_stall_requests[WriteStallKind::L0FilesSlowdown as usize]
                .load(AtomicOrdering::Relaxed),
            5
        );
    }

    #[test]
    fn write_without_wal_bypasses_l0_slowdown_and_stop() {
        let temp = tempfile::tempdir().unwrap();
        let mut opts = crate::config::Options::for_testing();
        opts.background_compaction = true;
        opts.level0_compaction_trigger = 100;
        opts.level0_slowdown_writes_trigger = 1;
        opts.level0_stop_writes_trigger = 2;
        let db = DB::open(temp.path(), opts).unwrap();

        for i in 0..3 {
            let mut b = WriteBatch::new();
            b.put(format!("k{i}").as_bytes(), b"v");
            db.write_without_wal(&b).unwrap();
            db.flush().unwrap();
        }
        assert!(db.is_write_stall_active());

        let mut batch = WriteBatch::new();
        batch.put(b"apply_k", b"apply_v");
        db.write_without_wal(&batch).unwrap();
        assert_eq!(db.get(b"apply_k").unwrap().unwrap(), b"apply_v");
        assert_eq!(
            db.stats.write_stall_requests[WriteStallKind::L0FilesStop as usize]
                .load(AtomicOrdering::Relaxed),
            0
        );
        assert_eq!(
            db.stats.write_stall_requests[WriteStallKind::L0FilesSlowdown as usize]
                .load(AtomicOrdering::Relaxed),
            0
        );
    }
}
