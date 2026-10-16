//! 多路归并迭代器 (compaction 专用): 多个 SST 输入按 InternalKey 顺序做堆归并 (`BinaryHeap`),
//! 供 `CompactionJob` 统一遍历全部输入文件.
//!
//! 排序规则: 先按 user_key 升序, 同一 user_key 内再按完整 InternalKey 排序 (高 sequence 在前),
//! 因此同一 user_key 的多个版本会相邻出现, 由上层 `CompactionJob` 做 dedup.
//!
//! # Invariant
//!
//! - 仅用于 compaction 路径; 读路径 (跨 MemTable + SSTable) 使用 `db::iterator::DBIterator`,
//!   二者勿混用 (见 `docs/modules/02-engine-storage.md`).

use crate::engine::memtable::extract_user_key;
use crate::engine::sstable::{SSTableIterator, SSTableReader};
use crate::error::Result;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

struct MergeEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    iterator_index: usize,
}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for MergeEntry {}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        let self_user = extract_user_key(&self.key);
        let other_user = extract_user_key(&other.key);
        other_user
            .cmp(self_user)
            .then_with(|| other.key.cmp(&self.key))
    }
}

pub struct MergeIterator {
    heap: BinaryHeap<MergeEntry>,
    iterators: Vec<SSTableIterator>,
    range_end: Option<Vec<u8>>,
}

impl MergeIterator {
    pub fn new(readers: Vec<Arc<SSTableReader>>) -> Result<Self> {
        let mut heap = BinaryHeap::new();
        let mut iterators = Vec::with_capacity(readers.len());
        for (idx, reader) in readers.into_iter().enumerate() {
            let iter = reader.iter_uncached();
            if iter.valid() {
                heap.push(MergeEntry {
                    key: iter.key().unwrap_or_default().to_vec(),
                    value: iter.value().unwrap_or_default().to_vec(),
                    iterator_index: idx,
                });
            }
            iterators.push(iter);
        }
        Ok(Self {
            heap,
            iterators,
            range_end: None,
        })
    }

    /// 创建带范围限制的 MergeIterator.
    /// `range_end` 为 None 时行为与 `new()` 一致.
    /// 非 None 时仅 yield key < range_end 的条目 (右开区间).
    pub fn with_range(
        readers: Vec<Arc<SSTableReader>>,
        range_end: Option<Vec<u8>>,
    ) -> Result<Self> {
        let mut heap = BinaryHeap::new();
        let mut iterators = Vec::with_capacity(readers.len());
        for (idx, reader) in readers.into_iter().enumerate() {
            let iter = reader.iter_uncached();
            if iter.valid() {
                let key = iter.key().unwrap_or_default().to_vec();
                if let Some(ref end) = range_end {
                    if key.as_slice() >= end.as_slice() {
                        // 首条已越界, 不加入初始堆, 后续条目只会更大
                        iterators.push(iter);
                        continue;
                    }
                }
                heap.push(MergeEntry {
                    key,
                    value: iter.value().unwrap_or_default().to_vec(),
                    iterator_index: idx,
                });
            }
            iterators.push(iter);
        }
        Ok(Self {
            heap,
            iterators,
            range_end,
        })
    }

    pub fn next_entry(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some(entry) = self.heap.pop() {
            if let Some(ref end) = self.range_end {
                if entry.key.as_slice() >= end.as_slice() {
                    // Past range end: skip without advancing this iterator
                    continue;
                }
            }
            self.advance_iterator(entry.iterator_index)?;
            return Ok(Some((entry.key, entry.value)));
        }
        Ok(None)
    }

    fn advance_iterator(&mut self, index: usize) -> Result<()> {
        if index >= self.iterators.len() {
            return Ok(());
        }
        let iter = &mut self.iterators[index];
        if iter.advance() && iter.valid() {
            self.heap.push(MergeEntry {
                key: iter.key().unwrap_or_default().to_vec(),
                value: iter.value().unwrap_or_default().to_vec(),
                iterator_index: index,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CompressionType;
    use crate::engine::memtable::{encode_internal_key, ValueType};
    use crate::engine::sstable::{sstable_path, SSTableBuilder, SSTableReader};
    use tempfile::tempdir;

    fn sst(
        dir: &std::path::Path,
        num: u64,
        level: usize,
        entries: &[(&[u8], u64, ValueType, &[u8])],
    ) -> Arc<SSTableReader> {
        let path = sstable_path(dir, num, level);
        let mut b = SSTableBuilder::new(&path, 512, 16, CompressionType::None, 0.0).unwrap();
        for (uk, seq, ty, val) in entries {
            let ik = encode_internal_key(uk, *seq, *ty);
            b.add(&ik, val).unwrap();
        }
        b.finish().unwrap();
        Arc::new(SSTableReader::open(&path, None).unwrap())
    }

    fn collect(mut it: MergeIterator) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::new();
        while let Some(e) = it.next_entry().unwrap() {
            out.push(e);
        }
        out
    }

    #[test]
    fn test_merge_two_non_overlapping() {
        let dir = tempdir().unwrap();
        let t1 = sst(
            dir.path(),
            1,
            0,
            &[
                (b"a", 1, ValueType::TypePut, b"1"),
                (b"c", 1, ValueType::TypePut, b"3"),
            ],
        );
        let t2 = sst(
            dir.path(),
            2,
            0,
            &[
                (b"b", 1, ValueType::TypePut, b"2"),
                (b"d", 1, ValueType::TypePut, b"4"),
            ],
        );
        let rows = collect(MergeIterator::new(vec![t1, t2]).unwrap());
        assert_eq!(rows.len(), 4);
        assert_eq!(extract_user_key(&rows[0].0), b"a");
        assert_eq!(extract_user_key(&rows[1].0), b"b");
    }

    #[test]
    fn test_merge_overlapping_high_seq_first() {
        let dir = tempdir().unwrap();
        let low = sst(dir.path(), 1, 0, &[(b"k", 1, ValueType::TypePut, b"old")]);
        let high = sst(dir.path(), 2, 0, &[(b"k", 2, ValueType::TypePut, b"new")]);
        let rows = collect(MergeIterator::new(vec![low, high]).unwrap());
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1, b"new");
        assert_eq!(rows[1].1, b"old");
    }

    #[test]
    fn test_merge_with_range_exclusive_end() {
        let dir = tempdir().unwrap();
        let t1 = sst(
            dir.path(),
            1,
            0,
            &[
                (b"a", 1, ValueType::TypePut, b"1"),
                (b"b", 1, ValueType::TypePut, b"2"),
                (b"c", 1, ValueType::TypePut, b"3"),
                (b"d", 1, ValueType::TypePut, b"4"),
            ],
        );
        // Range [nil, "c") should return only a, b
        let rows = collect(MergeIterator::with_range(vec![t1], Some(b"c".to_vec())).unwrap());

        assert_eq!(rows.len(), 2);
        assert_eq!(extract_user_key(&rows[0].0), b"a");
        assert_eq!(extract_user_key(&rows[1].0), b"b");
    }

    #[test]
    fn test_merge_with_range_none_returns_all() {
        let dir = tempdir().unwrap();
        let t1 = sst(
            dir.path(),
            1,
            0,
            &[
                (b"a", 1, ValueType::TypePut, b"1"),
                (b"b", 1, ValueType::TypePut, b"2"),
            ],
        );
        let rows = collect(MergeIterator::with_range(vec![t1], None).unwrap());
        assert_eq!(rows.len(), 2);
        assert_eq!(extract_user_key(&rows[0].0), b"a");
        assert_eq!(extract_user_key(&rows[1].0), b"b");
    }

    #[test]
    fn test_merge_with_range_empty_result() {
        let dir = tempdir().unwrap();
        let t1 = sst(dir.path(), 1, 0, &[(b"c", 1, ValueType::TypePut, b"1")]);
        // range_end < first key, so nothing returned
        let rows = collect(MergeIterator::with_range(vec![t1], Some(b"a".to_vec())).unwrap());
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_merge_empty_input_table() {
        let dir = tempdir().unwrap();
        let empty_path = sstable_path(dir.path(), 9, 0);
        let b = SSTableBuilder::new(&empty_path, 512, 16, CompressionType::None, 0.0).unwrap();
        b.abandon().unwrap();
        let t1 = sst(dir.path(), 1, 0, &[(b"a", 1, ValueType::TypePut, b"1")]);
        let rows = collect(MergeIterator::new(vec![t1]).unwrap());
        assert_eq!(rows.len(), 1);
    }
}
