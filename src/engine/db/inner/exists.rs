//! 存在性判定子模块: `key_exists` / `key_exists_at_sequence` (不物化 Value).
//!
//! 查序与 `get_at_sequence` 相同: active MemTable → immutable MemTables → SST.
//! 热路径只读 `ValueType` (mem/imm 用 `search`, SST 用 `value_type`), 不走会 `to_vec`
//! 的 `point_state` / `get`.

use super::read::find_sstable_for_key;
use super::{
    encode_internal_key_buffered, AtomicOrdering, Error, PointState, Result, SSTableReader,
    ValueType, DB,
};
use std::sync::Arc;

impl DB {
    /// 完整存在性判定: key 在当前 sequence 下是否可见为 Put (不物化 Value).
    /// 此为用户公开 API，走用户读口径（记入 block_read_bytes，计入 BlockCache）。
    #[tracing::instrument(level = "debug", name = "db_key_exists", skip(self, key))]
    pub fn key_exists(&self, key: &[u8]) -> Result<bool> {
        self.check_not_closed()?;
        Self::validate_user_key(key)?;
        let max_seq = self.sequence.load(AtomicOrdering::SeqCst);
        self.key_exists_internal(key, max_seq, true)
    }

    /// 与 `key_exists` 同一判定, 不记入 BlockCache 和读统计. 给 apply 里的 `PutConditional` 用.
    pub fn key_exists_without_stats(&self, key: &[u8]) -> Result<bool> {
        self.check_not_closed()?;
        Self::validate_user_key(key)?;
        let max_seq = self.sequence.load(AtomicOrdering::SeqCst);
        self.key_exists_internal(key, max_seq, false)
    }

    /// 在给定 `max_seq` 边界下判定 key 是否存在 (不物化 Value).
    #[allow(dead_code)]
    pub(crate) fn key_exists_at_sequence(&self, key: &[u8], max_seq: u64) -> Result<bool> {
        self.key_exists_internal(key, max_seq, true)
    }

    /// 内部统一存在性判定, 由 `record_stats` 参数控制是否经过 BlockCache 与累加 stats.
    pub(crate) fn key_exists_internal(
        &self,
        key: &[u8],
        max_seq: u64,
        record_stats: bool,
    ) -> Result<bool> {
        if self.has_active_range_tombstones() {
            self.key_exists_at_sequence_with_range_tombstones(key, max_seq, record_stats)
        } else {
            self.key_exists_at_sequence_fast(key, max_seq, record_stats)
        }
    }

    /// 无 range tombstone 时: mem → imm → sst, 只看 `ValueType`.
    fn key_exists_at_sequence_fast(
        &self,
        key: &[u8],
        max_seq: u64,
        record_stats: bool,
    ) -> Result<bool> {
        let mut hit: Option<bool> = None;
        let mut err: Option<Error> = None;

        encode_internal_key_buffered(key, max_seq, ValueType::TypePut, |seek_key| {
            match self.memtable.read().search(seek_key) {
                Ok(Some((_, ty))) => {
                    hit = Some(matches!(ty, ValueType::TypePut));
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
                    Ok(Some((_, ty))) => {
                        hit = Some(matches!(ty, ValueType::TypePut));
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
        if let Some(exists) = hit {
            return Ok(exists);
        }

        self.key_exists_from_sstables(key, max_seq, record_stats)
    }

    fn key_exists_from_sstables(
        &self,
        key: &[u8],
        max_seq: u64,
        record_stats: bool,
    ) -> Result<bool> {
        encode_internal_key_buffered(key, max_seq, ValueType::TypePut, |seek_key| {
            let tables = self.sstables.read();
            for reader in &tables[0] {
                if let Some(ty) = reader.value_type_opt(seek_key, record_stats)? {
                    return Ok(matches!(ty, ValueType::TypePut));
                }
            }
            for level in tables.iter().skip(1) {
                if let Some(reader) = find_sstable_for_key(level, key) {
                    if let Some(ty) = reader.value_type_opt(seek_key, record_stats)? {
                        return Ok(matches!(ty, ValueType::TypePut));
                    }
                }
            }
            Ok(false)
        })
    }

    /// range-tombstone 稀有路径: 暂时与 `get_at_sequence_with_range_tombstones` 相同遍历,
    /// 只保留存在性 bool (可丢弃 put value). 不得将完整 `get().is_some()` 当作长期方案.
    fn key_exists_at_sequence_with_range_tombstones(
        &self,
        key: &[u8],
        max_seq: u64,
        record_stats: bool,
    ) -> Result<bool> {
        let mut best_put: Option<u64> = None;
        let mut best_delete: Option<u64> = None;
        let mut best_range: Option<u64> = None;

        let mut absorb_point = |state: PointState| match state {
            PointState::Put(_, seq) => {
                if best_put.is_none_or(|s| seq > s) {
                    best_put = Some(seq);
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
        absorb_range(self.memtable.read().max_range_tombstone_seq(key, max_seq)?);

        for imm in self.immutable_memtables.read().iter().rev() {
            absorb_point(imm.point_state(key, max_seq)?);
            absorb_range(imm.max_range_tombstone_seq(key, max_seq)?);
        }

        let l0_readers: Vec<Arc<SSTableReader>>;
        let l1_plus_readers: Vec<Vec<Arc<SSTableReader>>>;
        {
            let tables = self.sstables.read();
            l0_readers = tables[0].clone();
            l1_plus_readers = tables.iter().skip(1).cloned().collect();
        }

        for reader in &l0_readers {
            absorb_point(reader.point_state_opt(key, max_seq, record_stats)?);
            absorb_range(reader.max_range_tombstone_seq(key, max_seq));
        }
        for level in &l1_plus_readers {
            if let Some(reader) = find_sstable_for_key(level, key) {
                absorb_point(reader.point_state_opt(key, max_seq, record_stats)?);
                absorb_range(reader.max_range_tombstone_seq(key, max_seq));
            }
        }

        let tombstone = match (best_delete, best_range) {
            (Some(d), Some(r)) => Some(d.max(r)),
            (Some(d), None) => Some(d),
            (None, Some(r)) => Some(r),
            (None, None) => None,
        };

        Ok(match (best_put, tombstone) {
            (Some(put_seq), Some(ts)) if put_seq > ts => true,
            (Some(_), None) => true,
            _ => false,
        })
    }
}
