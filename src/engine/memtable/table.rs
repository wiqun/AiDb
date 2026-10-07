//! MemTable — 内存写入缓冲: 以 InternalKey 有序的 SkipMap 承载最新写入, 冻结后转为只读.
//!
//! # 结构
//!
//! - `MemTable`: 可变写缓冲; `rep` (SkipMap) 按 InternalKey 有序, `range_index` 单独索引
//!   range tombstone (`[start, end)` + seq), 避免 GET 时扫描全表.
//! - `ImmutableMemTable`: `freeze(self, flush_seq)` 消费可变表得到; 无 put/delete, 仅读;
//!   `flush_seq` = 冻结时刻 sequence, flush 成 SST 时使用.
//!
//! # Invariant
//!
//! - 读路径以 `encode_internal_key(key, max_seq, TypePut)` 构造 seek key, 只返回
//!   `seq <= max_seq` 的版本 (get / point_state / search).
//! - freeze 后不可再写入, 新写入进入新的 active MemTable.

use super::internal_key::{
    check_sequence, encode_internal_key, encode_internal_key_arc, extract_sequence,
    extract_user_key, extract_value_type, ValueType, K_MAX_SEQUENCE,
};
use super::iterator::MemTableIterator;
use super::key_bytes::InternalKeyBytes;
use super::range_tombstone::{max_covering_range_tombstone_seq, RangeTombstoneRecord};
use super::rep::MemTableRep;
use super::skiplist_rep::SkipMapRep;
use crate::error::Result;
use crate::statistics::Statistics;
use parking_lot::RwLock;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// 同 user_key 的 point 状态 (不含 range tombstone).
pub enum PointState {
    Put(Vec<u8>, u64),
    Delete(u64),
    Absent,
}

/// 墓碑记录元组 (start, end, seq)
pub type TombstoneRecord = (Vec<u8>, Vec<u8>, u64);

/// 已冻结、等待 flush 的 MemTable (无 put/delete, 仅读).
pub struct ImmutableMemTable {
    table: MemTable,
    flush_seq: u64,
}

impl ImmutableMemTable {
    pub fn flush_seq(&self) -> u64 {
        self.flush_seq
    }

    pub fn approximate_size(&self) -> usize {
        self.table.approximate_size()
    }

    pub fn get(&self, key: &[u8], snapshot_seq: u64) -> Result<Option<Vec<u8>>> {
        self.table.get(key, snapshot_seq)
    }

    pub fn contains_key(&self, key: &[u8], snapshot_seq: u64) -> Result<bool> {
        self.table.contains_key(key, snapshot_seq)
    }

    pub fn get_latest(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.table.get_latest(key)
    }

    pub fn point_state(&self, key: &[u8], snapshot_seq: u64) -> Result<PointState> {
        self.table.point_state(key, snapshot_seq)
    }

    pub fn max_range_tombstone_seq(&self, user_key: &[u8], max_seq: u64) -> Result<Option<u64>> {
        self.table.max_range_tombstone_seq(user_key, max_seq)
    }

    pub fn has_range_tombstones(&self) -> bool {
        self.table.has_range_tombstones()
    }

    pub(crate) fn collect_range_tombstones(&self) -> Result<Vec<TombstoneRecord>> {
        self.table.collect_range_tombstones()
    }

    pub fn search(&self, seek_key: &[u8]) -> Result<Option<(Arc<[u8]>, ValueType)>> {
        self.table.search(seek_key)
    }

    pub fn iter(&self) -> MemTableIterator<'_> {
        self.table.iter()
    }

    /// 路径遍历: Chunk -> MemTable
    pub(crate) fn inner(&self) -> &MemTable {
        &self.table
    }

    pub fn entry_count(&self) -> u64 {
        self.table.entry_count()
    }
}

/// 可变 MemTable.
pub struct MemTable {
    rep: SkipMapRep,
    /// Range tombstone 索引 — 仅含 delete_range 写入, 避免 GET 扫描全表.
    range_index: RwLock<Vec<RangeTombstoneRecord>>,
    stats: Option<Arc<Statistics>>,
    /// 成功插入的内部记录数, 含覆盖写, 点删除和范围删除. flush 不减少.
    entry_count: AtomicU64,
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

impl MemTable {
    pub fn new() -> Self {
        Self::new_with_stats(None)
    }

    pub fn new_with_stats(stats: Option<Arc<Statistics>>) -> Self {
        Self {
            rep: SkipMapRep::new(),
            range_index: RwLock::new(Vec::new()),
            stats,
            entry_count: AtomicU64::new(0),
        }
    }

    pub fn entry_count(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }

    pub fn has_range_tombstones(&self) -> bool {
        !self.range_index.read().is_empty()
    }

    pub(crate) fn rep(&self) -> &SkipMapRep {
        &self.rep
    }

    pub fn approximate_size(&self) -> usize {
        self.rep.approximate_size()
    }

    pub fn put(&self, key: &[u8], value: &[u8], sequence: u64) -> Result<()> {
        check_sequence(sequence)?;
        let ik = InternalKeyBytes(encode_internal_key_arc(key, sequence, ValueType::TypePut));
        let val = Arc::from(value);
        self.rep.insert(ik, val);
        self.entry_count.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(target: "mem", "mem.put");
        self.sync_active_metric();
        Ok(())
    }

    pub fn delete(&self, key: &[u8], sequence: u64) -> Result<()> {
        check_sequence(sequence)?;
        let ik = InternalKeyBytes(encode_internal_key_arc(
            key,
            sequence,
            ValueType::TypeDelete,
        ));
        self.rep.insert(ik, Arc::from(&[] as &[u8]));
        self.entry_count.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(target: "mem", "mem.delete");
        self.sync_active_metric();
        Ok(())
    }

    /// 写入 range tombstone: InternalKey=start, value=end, 半开区间 `[start, end)`.
    pub fn put_range_delete(&self, start: &[u8], end: &[u8], sequence: u64) -> Result<()> {
        check_sequence(sequence)?;
        let ik = InternalKeyBytes(encode_internal_key_arc(
            start,
            sequence,
            ValueType::TypeRangeDelete,
        ));
        let val = Arc::from(end);
        self.rep.insert(ik, val);
        self.entry_count.fetch_add(1, Ordering::Relaxed);
        self.range_index.write().push(RangeTombstoneRecord {
            start: start.to_vec(),
            end: end.to_vec(),
            sequence,
        });
        tracing::debug!(target: "mem", "mem.put_range_delete");
        self.sync_active_metric();
        Ok(())
    }

    /// 同 user_key 在 `max_seq` 下的 point 状态 (不含 range tombstone 覆盖判定).
    pub fn point_state(&self, key: &[u8], max_seq: u64) -> Result<PointState> {
        let seek_key = encode_internal_key(key, max_seq, ValueType::TypePut);
        let bound = InternalKeyBytes::from_slice(&seek_key);
        let Some(entry) = self.rep.lower_bound(Bound::Included(&bound)) else {
            return Ok(PointState::Absent);
        };
        let ik = entry.key().as_ref();
        if extract_user_key(ik) != key {
            return Ok(PointState::Absent);
        }
        let seq = extract_sequence(ik)?;
        if seq > max_seq {
            return Ok(PointState::Absent);
        }
        match extract_value_type(ik)? {
            ValueType::TypePut => Ok(PointState::Put(entry.value().as_ref().to_vec(), seq)),
            ValueType::TypeDelete => Ok(PointState::Delete(seq)),
            ValueType::TypeRangeDelete => Ok(PointState::Absent),
        }
    }

    pub fn max_range_tombstone_seq(&self, user_key: &[u8], max_seq: u64) -> Result<Option<u64>> {
        Ok(max_covering_range_tombstone_seq(
            &self.range_index.read(),
            user_key,
            max_seq,
        ))
    }

    pub(crate) fn collect_range_tombstones(&self) -> Result<Vec<TombstoneRecord>> {
        Ok(self
            .range_index
            .read()
            .iter()
            .map(|r| (r.start.clone(), r.end.clone(), r.sequence))
            .collect())
    }

    pub fn get(&self, key: &[u8], snapshot_seq: u64) -> Result<Option<Vec<u8>>> {
        let seek_key = encode_internal_key(key, snapshot_seq, ValueType::TypePut);
        match self.search(&seek_key)? {
            Some((value, ValueType::TypePut)) => {
                tracing::debug!(target: "mem", "mem.get.hit");
                Ok(Some(value.as_ref().to_vec()))
            }
            Some((_, ValueType::TypeDelete)) | Some((_, ValueType::TypeRangeDelete)) => {
                tracing::debug!(target: "mem", "mem.get.miss");
                Ok(None)
            }
            None => {
                tracing::debug!(target: "mem", "mem.get.miss");
                Ok(None)
            }
        }
    }

    pub fn search(&self, seek_key: &[u8]) -> Result<Option<(Arc<[u8]>, ValueType)>> {
        let bound = InternalKeyBytes::from_slice(seek_key);
        let Some(entry) = self.rep.lower_bound(Bound::Included(&bound)) else {
            return Ok(None);
        };
        let entry_key = entry.key().as_ref();
        if extract_user_key(entry_key) != extract_user_key(seek_key) {
            return Ok(None);
        }
        let entry_seq = extract_sequence(entry_key)?;
        let seek_seq = extract_sequence(seek_key)?;
        if entry_seq > seek_seq {
            return Ok(None);
        }
        let value_type = extract_value_type(entry_key)?;
        Ok(Some((Arc::clone(entry.value()), value_type)))
    }

    /// 检查 key 是否存在 (不拷贝 value).
    /// 内部调用 `search()`, 仅返回 bool.
    pub fn contains_key(&self, key: &[u8], snapshot_seq: u64) -> Result<bool> {
        let seek_key = encode_internal_key(key, snapshot_seq, ValueType::TypePut);
        match self.search(&seek_key)? {
            Some((_, ValueType::TypePut)) => Ok(true),
            _ => Ok(false),
        }
    }

    pub fn freeze(self, flush_seq: u64) -> ImmutableMemTable {
        tracing::info!(target: "mem", "mem.freeze");
        if let Some(ref s) = self.stats {
            s.memtable_size_bytes[0].store(0, Ordering::Relaxed);
            s.memtable_size_bytes[1].fetch_add(self.approximate_size() as u64, Ordering::Relaxed);
        }
        ImmutableMemTable {
            table: self,
            flush_seq,
        }
    }

    fn sync_active_metric(&self) {
        if let Some(ref s) = self.stats {
            s.memtable_size_bytes[0].store(self.approximate_size() as u64, Ordering::Relaxed);
        }
    }

    pub fn iter(&self) -> MemTableIterator<'_> {
        MemTableIterator::new(self)
    }

    /// 非 snapshot 读: 见所有已写入版本.
    pub fn get_latest(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get(key, K_MAX_SEQUENCE)
    }
}
