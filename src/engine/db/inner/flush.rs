//! Flush 子模块: 冻结 MemTable 与后台 Flush 调度.
//!
//! `freeze_active_if_nonempty` 在 `write.rs`; 本模块负责把 Immutable MemTable
//! 依序写出 L0 SST, 并 rotate + cleanup WAL.

use super::{
    sstable_path, update_sstable_metrics, AtomicOrdering, MemTable, Result, SSTableBuilder,
    SSTableReader, VersionEdit, DB,
};
use std::sync::Arc;

impl DB {
    pub fn flush(&self) -> Result<()> {
        self.check_not_closed()?;
        self.do_flush_with_span()
    }

    #[tracing::instrument(name = "db_flush", skip(self))]
    fn do_flush_with_span(&self) -> Result<()> {
        self.do_flush()
    }

    pub(super) fn do_flush(&self) -> Result<()> {
        self.freeze_active_if_nonempty()?;
        let _flush_guard = self.flush_lock.lock();
        self.flush_immutable_memtables()?;
        self.rotate_wal()?;
        self.try_cleanup_wals()?;
        self.maybe_trigger_compaction();
        Ok(())
    }

    pub(super) fn flush_pending(&self) -> Result<()> {
        if self.flush_shutdown.load(AtomicOrdering::Acquire) {
            return Ok(());
        }
        let _flush_guard = self.flush_lock.lock();
        if self.flush_shutdown.load(AtomicOrdering::Acquire) {
            return Ok(());
        }
        let flushed = self.flush_immutable_memtables()?;
        if flushed > 0 {
            self.rotate_wal()?;
            self.try_cleanup_wals()?;
            self.maybe_trigger_compaction();
        }
        Ok(())
    }

    fn flush_immutable_memtables(&self) -> Result<usize> {
        let mut flushed = 0usize;
        loop {
            let has_front = !self.immutable_memtables.read().is_empty();
            if !has_front {
                break;
            }
            {
                let imm = self.immutable_memtables.read();
                self.flush_memtable_to_sstable(imm[0].inner())?;
            }
            let removed = self.immutable_memtables.write().remove(0);
            let removed_size = removed.approximate_size() as u64;
            let _ = self.stats.memtable_size_bytes[1].fetch_update(
                AtomicOrdering::Relaxed,
                AtomicOrdering::Relaxed,
                |cur| Some(cur.saturating_sub(removed_size)),
            );
            flushed += 1;
        }
        if self.immutable_memtables.read().is_empty() {
            self.stats.memtable_size_bytes[1].store(0, AtomicOrdering::Relaxed);
        }
        if flushed > 0 {
            self.stats
                .flush_total
                .fetch_add(flushed as u64, AtomicOrdering::Relaxed);
        }
        Ok(flushed)
    }

    #[tracing::instrument(name = "db_flush_sst", skip(self, table))]
    fn flush_memtable_to_sstable(&self, table: &MemTable) -> Result<()> {
        let flush_start = std::time::Instant::now();
        let mut count = 0u64;
        let file_number = self.version_set.read().allocate_file_number();
        let path = sstable_path(&self.path, file_number, 0);
        let key_count = table.rep().inner_map().iter().count();
        let mut builder = SSTableBuilder::new(
            &path,
            self.options.block_size,
            self.options.block_restart_interval,
            self.options.compression,
            self.options.bloom_false_positive_rate,
        )?;
        if self.options.bloom_false_positive_rate > 0.0 {
            builder.set_expected_keys(key_count);
        }
        for entry in table.rep().inner_map().iter() {
            builder.add(entry.key().as_ref(), entry.value().as_ref())?;
            count += 1;
        }
        if count == 0 {
            builder.abandon()?;
            self.stats
                .flush_duration
                .record(flush_start.elapsed().as_micros() as u64);
            self.stats.flush_last_duration_us.store(
                flush_start.elapsed().as_micros() as u64,
                AtomicOrdering::Relaxed,
            );
            return Ok(());
        }
        let file_size = builder.finish()?;
        self.stats
            .flush_written_bytes
            .fetch_add(file_size, AtomicOrdering::Relaxed);
        let reader = Arc::new(SSTableReader::open_with_stats(
            &path,
            Some(Arc::clone(&self.block_cache)),
            Some(Arc::clone(&self.stats)),
        )?);
        {
            let mut tables = self.sstables.write();
            tables[0].insert(0, Arc::clone(&reader));
            self.l0_sstable_count.fetch_add(1, AtomicOrdering::Relaxed);
            let mut vs = self.version_set.write();
            vs.apply_edit(&VersionEdit::AddFile {
                level: 0,
                file_number,
                file_size: reader.file_size(),
                smallest_key: reader.smallest_key().to_vec(),
                largest_key: reader.largest_key().to_vec(),
            })?;
        }
        let pending = self.pending_compaction_bytes();
        update_sstable_metrics(&self.sstables.read(), &self.stats, pending);
        tracing::info!(target: "db", file_number, file_size, "db.flush.complete");
        self.stats
            .flush_duration
            .record(flush_start.elapsed().as_micros() as u64);
        self.stats.flush_last_duration_us.store(
            flush_start.elapsed().as_micros() as u64,
            AtomicOrdering::Relaxed,
        );
        Ok(())
    }

    fn rotate_wal(&self) -> Result<()> {
        let next = self.sequence.load(AtomicOrdering::SeqCst).saturating_add(1);
        self.wal.write().rotate(next)
    }

    pub(super) fn try_cleanup_wals(&self) -> Result<()> {
        let watermark = self.wal_gc_watermark();
        let _ = self.wal.write().cleanup(watermark)?;
        Ok(())
    }

    fn wal_gc_watermark(&self) -> u64 {
        let imm = self.immutable_memtables.read();
        if let Some(min_flush) = imm.iter().map(|m| m.flush_seq()).min() {
            return min_flush;
        }
        drop(imm);
        let mem = self.memtable.read();
        if let Some(min_seq) = super::read::min_sequence_in_memtable(&mem) {
            return min_seq;
        }
        u64::MAX
    }
}
