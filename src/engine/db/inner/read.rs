//! 读路径子模块: get/scan/snapshot 读视图聚合.
//!
//! 读顺序 (见 `mod.rs` # 架构): active MemTable → immutable MemTable (新→旧)
//! → L0 SSTable (新→旧全扫) → L1+ (find_sstable_for_key 按 user_key range 定位).

use super::{
    encode_internal_key_buffered, extract_sequence, AtomicOrdering, DbIterGuard, Error, MemTable,
    PinnableSlice, PointState, Result, SSTableReader, Snapshot, ValueType, DB,
};
use crate::engine::memtable::{
    extract_user_key, extract_value_type, max_covering_range_tombstone_seq, InternalKeyBytes,
    InternalKeySlice, RangeTombstoneRecord, SharedSkipMap, K_MAX_SEQUENCE,
};
use crate::statistics::DbOp;
use std::ops::Bound;
use std::sync::Arc;

impl DB {
    /// 零拷贝点查 API (ADR-007): 优先命中 RowCache 或共享底层内存, 避免 to_vec() 堆分配.
    pub fn get_pinned<'a>(&'a self, key: &[u8]) -> Result<Option<PinnableSlice<'a>>> {
        let op_start = std::time::Instant::now();
        self.check_not_closed()?;
        Self::validate_user_key(key)?;
        self.stats.operations[DbOp::Get as usize].fetch_add(1, AtomicOrdering::Relaxed);
        let max_seq = self.sequence.load(AtomicOrdering::SeqCst);

        // 1. 优先查 RowCache (命中时零拷贝借用返回 Shared(Bytes))
        if let Some(val) = self.row_cache.get(key, max_seq) {
            let bytes = val.len();
            self.stats.note_logical_read(bytes);
            self.stats.operation_durations[DbOp::Get as usize]
                .record(op_start.elapsed().as_micros() as u64);
            return Ok(Some(PinnableSlice::Shared(val)));
        }

        // 2. 未命中查 LSM
        let r = self.get_at_sequence(key, max_seq)?;

        // 3. 回填 RowCache 并返回
        if let Some(v) = r {
            let committed = self.committed_sequence.load(AtomicOrdering::Acquire);
            let b = bytes::Bytes::from(v);
            self.row_cache.insert(key, b.clone(), committed);
            self.stats.operation_durations[DbOp::Get as usize]
                .record(op_start.elapsed().as_micros() as u64);
            return Ok(Some(PinnableSlice::Shared(b)));
        }

        self.stats.operation_durations[DbOp::Get as usize]
            .record(op_start.elapsed().as_micros() as u64);
        Ok(None)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.get_pinned(key)?.map(|p| p.into_vec()))
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

    /// 有界范围扫描: 优先命中单层纯内存跳表快路径, 避免构造完整 DBIterator 及墓碑多版本归并放大.
    pub fn scan_limited(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.check_not_closed()?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let seq = self.committed_sequence.load(AtomicOrdering::SeqCst);
        let (is_pure_mem, shared_map) = {
            let mem = self.memtable.read();
            let imm = self.immutable_memtables.read();
            let sst = self.sstables.read();
            let no_sst_overlap = sst.iter().all(|level| {
                level
                    .iter()
                    .all(|r| !crate::engine::db::iterator::sst_overlaps(r, start, end))
            });
            let pure = imm.is_empty() && no_sst_overlap && !mem.has_range_tombstones();
            (pure, mem.rep().shared_map())
        };

        if is_pure_mem {
            if reverse {
                return Ok(scan_memtable_limited_backward(
                    &shared_map,
                    seq,
                    start,
                    end,
                    limit,
                ));
            } else {
                return Ok(scan_memtable_limited_forward(
                    &shared_map,
                    seq,
                    start,
                    end,
                    limit,
                ));
            }
        }

        let mut out = Vec::with_capacity(limit.min(4096));
        if reverse {
            let mut iter = {
                let mem = self.memtable.read();
                let imm = self.immutable_memtables.read();
                let sst = self.sstables.read();
                crate::engine::db::DBIterator::new_unpositioned(
                    &mem,
                    &imm,
                    &sst,
                    seq,
                    start,
                    end,
                    Some(Arc::clone(&self.stats)),
                )
            };
            iter.seek_to_last();
            while out.len() < limit {
                let (Some(key), Some(value)) = (iter.key(), iter.value()) else {
                    break;
                };
                out.push((key.to_vec(), value.to_vec()));
                if !iter.prev() {
                    break;
                }
            }
            return Ok(out);
        }
        let iter = self.scan(start, end)?;
        for item in iter {
            let (key, value) = item?;
            out.push((key, value));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
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

/// 纯 MemTable 单层极速正向有界扫描: 零多路归并抽象、零墓碑多版本堆分配、一路跳表链表前进.
fn scan_memtable_limited_forward(
    map: &SharedSkipMap,
    committed_seq: u64,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    limit: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::with_capacity(limit.min(4096));
    let mut entry = match start {
        Some(s) => encode_internal_key_buffered(s, K_MAX_SEQUENCE, ValueType::TypePut, |seek| {
            map.lower_bound(Bound::Included(InternalKeySlice::from_slice(seek)))
        }),
        None => map.front(),
    };
    let mut last_user_key = Vec::new();
    let mut has_last = false;
    while let Some(e) = entry {
        let internal_key = e.key().as_ref();
        let user_key = extract_user_key(internal_key);
        if end.is_some_and(|end| user_key >= end) {
            break;
        }
        if has_last && last_user_key.as_slice() == user_key {
            entry = e.next();
            continue;
        }
        let Ok(seq) = extract_sequence(internal_key) else {
            entry = e.next();
            continue;
        };
        if seq > committed_seq {
            entry = e.next();
            continue;
        }
        has_last = true;
        last_user_key.clear();
        last_user_key.extend_from_slice(user_key);
        let vtype = extract_value_type(internal_key).unwrap_or(ValueType::TypeDelete);
        if vtype == ValueType::TypePut {
            out.push((user_key.to_vec(), e.value().to_vec()));
            if out.len() >= limit {
                break;
            }
        }
        entry = e.next();
    }
    out
}

type SkipEntry<'a> = crossbeam_skiplist::map::Entry<'a, InternalKeyBytes, Arc<[u8]>>;

/// 沿跳表反向消费 `curr_user_key` 的版本组:
/// - 若组内版本数 <= 8, 直接通过 `prev()` 顺序回退拿到最新可见版本并落到前一个 `user_key` 末尾 (零分配、零跳表搜索).
/// - 若单 key 版本数 > 8, 切换为 `lower_bound` 读最新版本 + `upper_bound` 直接跳过整组.
fn consume_memtable_group_backward<'a>(
    map: &'a SharedSkipMap,
    first_entry: SkipEntry<'a>,
    curr_user_key: &[u8],
    committed_seq: u64,
) -> (Option<Vec<u8>>, Option<SkipEntry<'a>>) {
    let mut best: Option<(u64, ValueType, SkipEntry<'a>)> = None;
    let mut cur = Some(first_entry);
    let mut steps = 0usize;
    while let Some(e) = cur {
        let ikey = e.key().as_ref();
        if extract_user_key(ikey) != curr_user_key {
            let val = best
                .and_then(|(_, vt, win)| (vt == ValueType::TypePut).then(|| win.value().to_vec()));
            return (val, Some(e));
        }
        if let Ok(seq) = extract_sequence(ikey) {
            if seq <= committed_seq {
                let vt = extract_value_type(ikey).unwrap_or(ValueType::TypeDelete);
                let wins = best.as_ref().is_none_or(|(b_seq, _, _)| {
                    seq > *b_seq || (seq == *b_seq && vt == ValueType::TypeDelete)
                });
                if wins {
                    best = Some((seq, vt, e.clone()));
                }
            }
        }
        steps += 1;
        if steps > 8 {
            return jump_memtable_group_backward(map, curr_user_key, committed_seq);
        }
        cur = e.prev();
    }
    let val =
        best.and_then(|(_, vt, win)| (vt == ValueType::TypePut).then(|| win.value().to_vec()));
    (val, None)
}

fn jump_memtable_group_backward<'a>(
    map: &'a SharedSkipMap,
    curr_user_key: &[u8],
    committed_seq: u64,
) -> (Option<Vec<u8>>, Option<SkipEntry<'a>>) {
    encode_internal_key_buffered(curr_user_key, K_MAX_SEQUENCE, ValueType::TypePut, |seek| {
        let seek_slice = InternalKeySlice::from_slice(seek);
        let mut le_opt = map.lower_bound(Bound::Included(seek_slice));
        let mut latest_val = None;
        while let Some(le) = le_opt {
            let l_ikey = le.key().as_ref();
            if extract_user_key(l_ikey) != curr_user_key {
                break;
            }
            if let Ok(seq) = extract_sequence(l_ikey) {
                if seq <= committed_seq {
                    let vtype = extract_value_type(l_ikey).unwrap_or(ValueType::TypeDelete);
                    if vtype == ValueType::TypePut {
                        latest_val = Some(le.value().to_vec());
                    }
                    break;
                }
            }
            le_opt = le.next();
        }
        let prev_entry = map.upper_bound(Bound::Excluded(seek_slice));
        (latest_val, prev_entry)
    })
}

/// 纯 MemTable 单层极速反向有界扫描: 优先链表 `prev()` 零搜索回退, 超长版本组自动切换 `upper_bound` 跳跃.
fn scan_memtable_limited_backward(
    map: &SharedSkipMap,
    committed_seq: u64,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    limit: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::with_capacity(limit.min(4096));
    let mut entry = match end {
        Some(e) => encode_internal_key_buffered(e, K_MAX_SEQUENCE, ValueType::TypePut, |seek| {
            map.upper_bound(Bound::Excluded(InternalKeySlice::from_slice(seek)))
        }),
        None => map.back(),
    };
    let mut curr_key_buf = Vec::new();
    while let Some(e) = entry {
        let curr_user_key = extract_user_key(e.key().as_ref());
        if start.is_some_and(|s| curr_user_key < s) {
            break;
        }
        curr_key_buf.clear();
        curr_key_buf.extend_from_slice(curr_user_key);
        let (latest_val, prev_entry) =
            consume_memtable_group_backward(map, e, &curr_key_buf, committed_seq);
        if let Some(val) = latest_val {
            out.push((curr_key_buf.clone(), val));
            if out.len() >= limit {
                break;
            }
        }
        entry = prev_entry;
    }
    out
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
