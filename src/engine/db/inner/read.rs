//! 读路径子模块: get/scan/snapshot 读视图聚合.
//!
//! 读顺序 (见 `mod.rs` # 架构): active MemTable → immutable MemTable (新→旧)
//! → L0 SSTable (新→旧全扫) → L1+ (find_sstable_for_key 按 user_key range 定位).

use super::{
    encode_internal_key_buffered, extract_sequence, AtomicOrdering, DbIterGuard, Error, MemTable,
    PointState, Result, SSTableReader, Snapshot, ValueType, DB,
};
use crate::engine::memtable::{max_covering_range_tombstone_seq, RangeTombstoneRecord};
use crate::statistics::DbOp;
use std::sync::Arc;

impl DB {
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let op_start = std::time::Instant::now();
        self.check_not_closed()?;
        Self::validate_user_key(key)?;
        self.stats.operations[DbOp::Get as usize].fetch_add(1, AtomicOrdering::Relaxed);
        let max_seq = self.sequence.load(AtomicOrdering::SeqCst);
        let r = self.get_at_sequence(key, max_seq)?;
        tracing::debug!(target: "db", found = r.is_some(), "db.get.result");
        self.stats.operation_durations[DbOp::Get as usize]
            .record(op_start.elapsed().as_micros() as u64);
        Ok(r)
    }

    pub(crate) fn get_at_sequence(&self, key: &[u8], max_seq: u64) -> Result<Option<Vec<u8>>> {
        let r = self.get_at_sequence_uncounted(key, max_seq)?;
        let bytes = r.as_ref().map(|v| v.len()).unwrap_or(0);
        self.stats.note_logical_read(bytes);
        Ok(r)
    }

    /// 与 `get_at_sequence` 结果相同, 但不计入逻辑读统计 (compaction 点查使用).
    pub(crate) fn get_at_sequence_uncounted(
        &self,
        key: &[u8],
        max_seq: u64,
    ) -> Result<Option<Vec<u8>>> {
        if self.has_active_range_tombstones() {
            self.get_at_sequence_with_range_tombstones(key, max_seq)
        } else {
            self.get_at_sequence_fast(key, max_seq)
        }
    }

    pub(super) fn has_active_range_tombstones(&self) -> bool {
        if self.memtable.read().has_range_tombstones() {
            return true;
        }
        for imm in self.immutable_memtables.read().iter() {
            if imm.has_range_tombstones() {
                return true;
            }
        }
        for level in self.sstables.read().iter() {
            for reader in level {
                if reader.has_range_tombstones() {
                    return true;
                }
            }
        }
        false
    }

    /// 无 range tombstone 时走旧短路路径 (mem → imm → sst 逐层返回).
    fn get_at_sequence_fast(&self, key: &[u8], max_seq: u64) -> Result<Option<Vec<u8>>> {
        let mut hit: Option<Option<Vec<u8>>> = None;
        let mut err: Option<Error> = None;

        encode_internal_key_buffered(key, max_seq, ValueType::TypePut, |seek_key| {
            match self.memtable.read().search(seek_key) {
                Ok(Some((value, ty))) => {
                    hit = Some(match ty {
                        ValueType::TypePut => Some(value.as_ref().to_vec()),
                        ValueType::TypeDelete | ValueType::TypeRangeDelete => None,
                    });
                    return;
                }
                Ok(None) => {}
                Err(e) => {
                    err = Some(e);
                    return;
                }
            }
            for imm in self.immutable_memtables.read().iter().rev() {
                match imm.search(seek_key) {
                    Ok(Some((value, ty))) => {
                        hit = Some(match ty {
                            ValueType::TypePut => Some(value.as_ref().to_vec()),
                            ValueType::TypeDelete | ValueType::TypeRangeDelete => None,
                        });
                        return;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        err = Some(e);
                        return;
                    }
                }
            }
        });

        if let Some(e) = err {
            return Err(e);
        }
        if let Some(res) = hit {
            return Ok(res);
        }

        self.get_from_sstables(key, max_seq)
    }

    fn max_range_seq_across_indexes(
        &self,
        l0: &[Arc<SSTableReader>],
        l1_plus: &[Vec<Arc<SSTableReader>>],
        key: &[u8],
        max_seq: u64,
    ) -> Option<u64> {
        let mut records = Vec::new();
        if let Ok(found) = self.memtable.read().collect_range_tombstones() {
            push_range_records(&mut records, found);
        }
        for imm in self.immutable_memtables.read().iter() {
            if let Ok(found) = imm.collect_range_tombstones() {
                push_range_records(&mut records, found);
            }
        }
        for reader in l0 {
            push_range_records(&mut records, reader.collect_range_tombstones());
        }
        for level in l1_plus {
            for reader in level {
                push_range_records(&mut records, reader.collect_range_tombstones());
            }
        }
        max_covering_range_tombstone_seq(&records, key, max_seq)
    }

    fn get_from_sstables(&self, key: &[u8], max_seq: u64) -> Result<Option<Vec<u8>>> {
        encode_internal_key_buffered(key, max_seq, ValueType::TypePut, |seek_key| {
            let tables = self.sstables.read();
            // L0 文件按由新到旧排列在 tables[0] (0 = 最新落盘), 顺序遍历
            for reader in &tables[0] {
                if let Some((value, ty)) = reader.get(seek_key)? {
                    return Ok(match ty {
                        ValueType::TypePut => Some(value),
                        ValueType::TypeDelete | ValueType::TypeRangeDelete => None,
                    });
                }
            }
            // L1+ 逐层查找
            for level in tables.iter().skip(1) {
                if let Some(reader) = find_sstable_for_key(level, key) {
                    if let Some((value, ty)) = reader.get(seek_key)? {
                        return Ok(match ty {
                            ValueType::TypePut => Some(value),
                            ValueType::TypeDelete | ValueType::TypeRangeDelete => None,
                        });
                    }
                }
            }
            Ok(None)
        })
    }

    fn get_at_sequence_with_range_tombstones(
        &self,
        key: &[u8],
        max_seq: u64,
    ) -> Result<Option<Vec<u8>>> {
        let mut best_put: Option<(Vec<u8>, u64)> = None;
        let mut best_delete: Option<u64> = None;
        let mut best_range: Option<u64> = None;

        let mut absorb_point = |state: PointState| match state {
            PointState::Put(value, seq) => {
                if best_put.as_ref().is_none_or(|(_, s)| seq > *s) {
                    best_put = Some((value, seq));
                }
            }
            PointState::Delete(seq) => {
                if best_delete.is_none_or(|s| seq > s) {
                    best_delete = Some(seq);
                }
            }
            PointState::Absent => {}
        };

        let mut absorb_range = |seq: Option<u64>| {
            if let Some(s) = seq {
                if best_range.is_none_or(|b| s > b) {
                    best_range = Some(s);
                }
            }
        };

        absorb_point(self.memtable.read().point_state(key, max_seq)?);

        for imm in self.immutable_memtables.read().iter().rev() {
            absorb_point(imm.point_state(key, max_seq)?);
        }

        let l0_readers: Vec<Arc<SSTableReader>>;
        let l1_plus_readers: Vec<Vec<Arc<SSTableReader>>>;
        {
            let tables = self.sstables.read();
            l0_readers = tables[0].clone();
            l1_plus_readers = tables.iter().skip(1).cloned().collect();
        }

        for reader in &l0_readers {
            absorb_point(reader.point_state(key, max_seq)?);
        }
        for level in &l1_plus_readers {
            if let Some(reader) = find_sstable_for_key(level, key) {
                absorb_point(reader.point_state(key, max_seq)?);
            }
        }
        absorb_range(self.max_range_seq_across_indexes(
            &l0_readers,
            &l1_plus_readers,
            key,
            max_seq,
        ));

        let tombstone = match (best_delete, best_range) {
            (Some(d), Some(r)) => Some(d.max(r)),
            (Some(d), None) => Some(d),
            (None, Some(r)) => Some(r),
            (None, None) => None,
        };

        Ok(match (best_put, tombstone) {
            (Some((value, put_seq)), Some(ts)) if put_seq > ts => Some(value),
            (Some((value, _)), None) => Some(value),
            _ => None,
        })
    }

    pub fn snapshot(self: &Arc<Self>) -> Result<Snapshot> {
        self.check_not_closed()?;
        let _guard = self.write_lock.lock();
        // 边界取"已提交"sequence 而非已分配的 `sequence`: 若用已分配值, 快照可能
        // 看到创建时刻尚未插入 MemTable 的写入 (A-005 竞态). committed_sequence
        // 仅在 memtable 插入完成后推进, 锁内读取时所有 <= 该值的写入必已可见.
        let seq = self.committed_sequence.load(AtomicOrdering::SeqCst);
        // register 必须在 write_lock 释放前完成: 否则在"读到 seq"和"注册
        // 保护"之间存在窗口, 一次新写入 (及其可能触发的 compaction) 可以
        // 插进来, 这时 min_snapshot_sequence() 还看不到这个即将返回的
        // snapshot, compaction 可能误判"没有活跃 snapshot 需要 seq 以下的
        // 旧版本"而把它 GC 掉 —— 等 snapshot 真正返回给调用方并读取时,
        // 它所需的旧版本已经不在了. 锁内注册可以保证: 任何在此之后才发生的
        // 写入 (以及它可能触发的 compaction) 一定能在 min_snapshot_sequence()
        // 里看到这个 seq.
        let snapshot_id = self.snapshots.register(seq);
        drop(_guard);
        self.stats.operations[DbOp::Snapshot as usize].fetch_add(1, AtomicOrdering::Relaxed);
        tracing::debug!(target: "db", sequence = seq, id = snapshot_id, "db.snapshot.create");
        Ok(Snapshot::new(Arc::clone(self), seq, snapshot_id))
    }

    pub fn iter(&self) -> Result<DbIterGuard> {
        self.check_not_closed()?;
        // 可见边界取已提交 sequence (与 Snapshot 一致): 写入在写锁内完成 MemTable 插入后才推进
        // committed, 边界内的写入必已可见, 已分配但未插入的写入不会被部分暴露.
        // 惰性迭代器不复制 MemTable, 迭代期间的新写入 (seq 大于边界) 对本迭代器不可见.
        let seq = self.committed_sequence.load(AtomicOrdering::SeqCst);
        let mem = self.memtable.read();
        let imm = self.immutable_memtables.read();
        let sst = self.sstables.read();
        Ok(crate::engine::db::DBIterator::new(
            &mem,
            &imm,
            &sst,
            seq,
            None,
            None,
            Some(Arc::clone(&self.stats)),
        ))
    }

    pub fn scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<DbIterGuard> {
        self.check_not_closed()?;
        // 与 iter() 相同, 使用已提交边界.
        let seq = self.committed_sequence.load(AtomicOrdering::SeqCst);
        let mem = self.memtable.read();
        let imm = self.immutable_memtables.read();
        let sst = self.sstables.read();
        tracing::debug!(target: "db", "db.scan.complete");
        Ok(crate::engine::db::DBIterator::new(
            &mem,
            &imm,
            &sst,
            seq,
            start,
            end,
            Some(Arc::clone(&self.stats)),
        ))
    }
}

pub(super) fn min_sequence_in_memtable(mt: &MemTable) -> Option<u64> {
    let mut min: Option<u64> = None;
    for entry in mt.rep().inner_map().iter() {
        if let Ok(seq) = extract_sequence(entry.key().as_ref()) {
            min = Some(min.map_or(seq, |m| m.min(seq)));
        }
    }
    min
}

fn push_range_records(out: &mut Vec<RangeTombstoneRecord>, records: Vec<(Vec<u8>, Vec<u8>, u64)>) {
    for (start, end, sequence) in records {
        out.push(RangeTombstoneRecord {
            start,
            end,
            sequence,
        });
    }
}

pub(super) fn find_sstable_for_key<'a>(
    level: &'a [Arc<SSTableReader>],
    user_key: &[u8],
) -> Option<&'a Arc<SSTableReader>> {
    level.iter().find(|reader| {
        user_key_in_sstable_range(user_key, reader.smallest_key(), reader.largest_key())
    })
}

/// Level 1+ 文件范围检测: 仅比较 user_key (不用 seek InternalKey, 避免 sequence 干扰).
fn user_key_in_sstable_range(user_key: &[u8], smallest: &[u8], largest: &[u8]) -> bool {
    if smallest.len() < 8 || largest.len() < 8 {
        return true;
    }
    let s_user = &smallest[..smallest.len() - 8];
    let l_user = &largest[..largest.len() - 8];
    user_key >= s_user && user_key <= l_user
}

#[cfg(test)]
mod tests {
    use super::DB;
    use crate::config::{CompressionType, Options};
    use crate::engine::memtable::{encode_internal_key, ValueType};
    use crate::engine::sstable::{sstable_path, SSTableBuilder, SSTableReader};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn one_entry(
        dir: &std::path::Path,
        num: u64,
        user_key: &[u8],
        seq: u64,
        ty: ValueType,
        value: &[u8],
    ) -> Arc<SSTableReader> {
        let path = sstable_path(dir, num, 1);
        let mut b = SSTableBuilder::new(&path, 512, 16, CompressionType::None, 0.0).unwrap();
        b.add(&encode_internal_key(user_key, seq, ty), value)
            .unwrap();
        b.finish().unwrap();
        Arc::new(SSTableReader::open(&path, None).unwrap())
    }

    #[test]
    fn get_and_scan_hide_key_covered_outside_marker_file() {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        db.sequence.store(3, Ordering::SeqCst);
        db.committed_sequence.store(3, Ordering::SeqCst);
        let file_a = one_entry(dir.path(), 1, b"a", 2, ValueType::TypeRangeDelete, b"z");
        let file_b = one_entry(dir.path(), 2, b"m", 1, ValueType::TypePut, b"v");
        let mut levels = vec![Vec::new(); db.options.max_levels];
        levels[1].push(file_a);
        levels[1].push(file_b);
        db.replace_sstables_for_test(levels);
        assert_eq!(db.get(b"m").unwrap(), None);
        let keys: Vec<Vec<u8>> = db
            .scan(None, None)
            .unwrap()
            .map(|item| item.unwrap().0)
            .collect();
        assert!(!keys.iter().any(|k| k == b"m"));
    }

    #[test]
    fn unsorted_level_still_finds_both_keys() {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        db.sequence.store(3, Ordering::SeqCst);
        let file_m = one_entry(dir.path(), 1, b"m", 1, ValueType::TypePut, b"m");
        let file_a = one_entry(dir.path(), 2, b"a", 1, ValueType::TypePut, b"a");
        let mut levels = vec![Vec::new(); db.options.max_levels];
        levels[1].push(file_m);
        levels[1].push(file_a);
        db.replace_sstables_for_test(levels);
        assert_eq!(db.get(b"a").unwrap().as_deref(), Some(b"a".as_slice()));
        assert_eq!(db.get(b"m").unwrap().as_deref(), Some(b"m".as_slice()));
    }

    #[test]
    fn miss_does_not_add_key_length_to_logical_read() {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path(), Options::for_testing()).unwrap();
        let before = db.stats.logical_read_bytes.load(Ordering::Relaxed);
        assert_eq!(db.get(b"missing-key").unwrap(), None);
        let after_miss = db.stats.logical_read_bytes.load(Ordering::Relaxed);
        assert_eq!(after_miss - before, 0);
        db.put(b"k", b"v").unwrap();
        let before_hit = db.stats.logical_read_bytes.load(Ordering::Relaxed);
        assert_eq!(db.get(b"k").unwrap().as_deref(), Some(b"v".as_slice()));
        let after_hit = db.stats.logical_read_bytes.load(Ordering::Relaxed);
        assert_eq!(after_hit - before_hit, 1);
    }
}
