//! DB 多路归并迭代器: 跨 active MemTable + immutable MemTable + 各级 SSTable 归并,
//! 每个 user_key 只返回最新可见版本. 拥有各层数据 (无自引用), 供 `DB::iter` / `scan`
//! 与 `Snapshot::iter` / `scan` 使用.
//!
//! # 数据流
//!
//! ```text
//! 层列表: active MemTable → 各 immutable MemTable → L0 SSTable → L1+ 逐文件
//! 归并:   load_next_valid 取各层当前最小 user_key, 在该 key 上按 sequence 取最新
//!         → 最新版本为 TypeDelete / 被 range tombstone 覆盖 → 整组跳过
//!         → 否则保留 TypePut 作为当前条目
//! 逆序:   seek_to_last / prev / load_prev_valid (与正序对称的逻辑)
//! ```
//!
//! # Invariant
//!
//! - `DB::iter` / `scan` 以 `K_MAX_SEQUENCE` 为序列边界 (不过滤到当前 sequence),
//!   与 `get` 的 `sequence.load()` 行为不同; `Snapshot` 则以创建时刻的 sequence 为界.
//! - 本迭代器面向读路径; compaction 归并使用 `compaction::MergeIterator`, 勿混用.

use crate::engine::memtable::{
    encode_internal_key, extract_sequence, extract_user_key, extract_value_type,
    max_covering_range_tombstone_seq, ImmutableMemTable, InternalKeyBytes, MemTable,
    MemTableIterator, RangeTombstoneRecord, ValueType, K_MAX_SEQUENCE,
};
use crate::engine::sstable::{SSTableIterator, SSTableReader};
use crate::error::Result;
use crate::statistics::Statistics;
use std::sync::atomic::Ordering;
use std::sync::Arc;

struct LayerEntry {
    user_key: Vec<u8>,
    value: Vec<u8>,
    sequence: u64,
    value_type: ValueType,
}

enum LayerIterInner {
    MemEntries {
        entries: Vec<(InternalKeyBytes, Arc<[u8]>)>,
        index: usize,
    },
    Sstable(SSTableIterator),
}

struct LayerIter {
    inner: LayerIterInner,
}

impl LayerIter {
    fn from_mem_entries(table: &MemTable) -> Self {
        let mut it = MemTableIterator::new(table);
        it.seek_to_first();
        let mut entries = Vec::new();
        while it.valid() {
            entries.push((it.key_bytes().clone(), it.value_arc().clone()));
            it.next();
        }
        Self {
            inner: LayerIterInner::MemEntries { entries, index: 0 },
        }
    }

    fn from_sstable(reader: &SSTableReader) -> Self {
        let mut it = reader.iter();
        it.seek_to_first();
        Self {
            inner: LayerIterInner::Sstable(it),
        }
    }

    fn valid(&self) -> bool {
        match &self.inner {
            LayerIterInner::MemEntries { entries, index } => *index < entries.len(),
            LayerIterInner::Sstable(it) => it.valid(),
        }
    }

    fn current_entry(&self) -> Option<LayerEntry> {
        let (key, value) = match &self.inner {
            LayerIterInner::MemEntries { entries, index } => {
                let (k, v) = entries.get(*index)?;
                (k.as_ref(), v.as_ref())
            }
            LayerIterInner::Sstable(it) if it.valid() => {
                let k = it.key()?;
                let v = it.value()?;
                (k, v)
            }
            _ => return None,
        };
        Some(LayerEntry {
            user_key: extract_user_key(key).to_vec(),
            value: value.to_vec(),
            sequence: extract_sequence(key).ok()?,
            value_type: extract_value_type(key).ok()?,
        })
    }

    fn advance_past_user_key(&mut self, user_key: &[u8]) {
        loop {
            if !self.valid() {
                return;
            }
            let Some(entry) = self.current_entry() else {
                return;
            };
            if entry.user_key != user_key {
                return;
            }
            self.advance_one();
        }
    }

    fn advance_one(&mut self) {
        match &mut self.inner {
            LayerIterInner::MemEntries { index, .. } => *index += 1,
            LayerIterInner::Sstable(it) => {
                it.advance();
            }
        }
    }

    fn prev_one(&mut self) {
        match &mut self.inner {
            LayerIterInner::MemEntries { index, .. } => {
                if *index > 0 {
                    *index -= 1;
                } else {
                    *index = usize::MAX;
                }
            }
            LayerIterInner::Sstable(it) => {
                it.prev();
            }
        }
    }

    /// 反向跳过指定 user_key 的全部 entry.
    fn prev_past_user_key(&mut self, user_key: &[u8]) {
        while self.valid() {
            let Some(entry) = self.current_entry() else {
                return;
            };
            if entry.user_key != user_key {
                return;
            }
            self.prev_one();
        }
    }

    /// 检查 user_key 在当前层是否有 TypeDelete 标记 (附近 entry 中查找).
    fn has_delete_for_key(&mut self, user_key: &[u8]) -> bool {
        match &mut self.inner {
            LayerIterInner::MemEntries { entries, index } => {
                // Scan backwards from current index
                let mut i = *index;
                while let Some((k, _)) = entries.get(i) {
                    if extract_user_key(k.as_ref()) != user_key {
                        break;
                    }
                    if let Ok(ValueType::TypeDelete) = extract_value_type(k.as_ref()) {
                        return true;
                    }
                    if i == 0 {
                        break;
                    }
                    i -= 1;
                }
                // Scan forward from current index
                let mut i = *index + 1;
                while i < entries.len() {
                    let (k, _) = &entries[i];
                    if extract_user_key(k.as_ref()) != user_key {
                        break;
                    }
                    if let Ok(ValueType::TypeDelete) = extract_value_type(k.as_ref()) {
                        return true;
                    }
                    i += 1;
                }
                false
            }
            LayerIterInner::Sstable(it) => {
                if !it.valid() {
                    return false;
                }
                let cur_key = match it.key() {
                    Some(k) => k.to_vec(),
                    None => return false,
                };
                if extract_user_key(&cur_key) != user_key {
                    return false;
                }
                // Current entry is TypePut; check if a higher-seq version (previous entry)
                // is a TypeDelete
                if let Ok(ValueType::TypeDelete) = extract_value_type(&cur_key) {
                    return true;
                }
                if it.prev() {
                    let prev_key = match it.key() {
                        Some(k) => k.to_vec(),
                        None => return false,
                    };
                    let is_delete = extract_user_key(&prev_key) == user_key
                        && matches!(extract_value_type(&prev_key), Ok(ValueType::TypeDelete));
                    it.advance();
                    if is_delete {
                        return true;
                    }
                }
                false
            }
        }
    }

    fn seek(&mut self, target_user_key: &[u8]) {
        match &mut self.inner {
            LayerIterInner::MemEntries { entries, index } => {
                let seek = encode_internal_key(target_user_key, K_MAX_SEQUENCE, ValueType::TypePut);
                *index = entries
                    .iter()
                    .position(|(k, _)| k.as_ref() >= seek.as_slice())
                    .unwrap_or(entries.len());
            }
            LayerIterInner::Sstable(it) => {
                let seek_key =
                    encode_internal_key(target_user_key, K_MAX_SEQUENCE, ValueType::TypePut);
                it.seek_to_target(&seek_key);
            }
        }
    }

    fn seek_to_last(&mut self) {
        match &mut self.inner {
            LayerIterInner::MemEntries { entries, index } => {
                *index = entries.len().saturating_sub(1);
            }
            LayerIterInner::Sstable(it) => {
                it.seek_to_last();
            }
        }
    }
}

/// 跨 MemTable + SSTable 的合并迭代器.
pub struct DBIterator {
    current: Option<(Vec<u8>, Vec<u8>)>,
    sequence: u64,
    layers: Vec<LayerIter>,
    end_key: Option<Vec<u8>>,
    range_tombstones: Vec<RangeTombstoneRecord>,
    stats: Option<Arc<Statistics>>,
}

fn collect_all_range_tombstones(
    memtable: &MemTable,
    immutables: &[ImmutableMemTable],
    sstables: &[Vec<Arc<SSTableReader>>],
) -> Vec<RangeTombstoneRecord> {
    let mut out = Vec::new();
    if let Ok(v) = memtable.collect_range_tombstones() {
        push_range_records(&mut out, v);
    }
    for imm in immutables {
        if let Ok(v) = imm.collect_range_tombstones() {
            push_range_records(&mut out, v);
        }
    }
    for level in sstables {
        for reader in level {
            push_range_records(&mut out, reader.collect_range_tombstones());
        }
    }
    out
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

impl DBIterator {
    pub fn new(
        memtable: &MemTable,
        immutables: &[ImmutableMemTable],
        sstables: &[Vec<Arc<SSTableReader>>],
        sequence: u64,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        stats: Option<Arc<Statistics>>,
    ) -> Self {
        let mut layers = Vec::new();
        layers.push(LayerIter::from_mem_entries(memtable));
        for imm in immutables {
            layers.push(LayerIter::from_mem_entries(imm.inner()));
        }
        for level in sstables {
            for reader in level {
                layers.push(LayerIter::from_sstable(reader));
            }
        }

        let mut it = Self {
            current: None,
            sequence,
            layers,
            end_key: end.map(|k| k.to_vec()),
            range_tombstones: collect_all_range_tombstones(memtable, immutables, sstables),
            stats,
        };
        if let Some(start) = start {
            it.seek(start);
        } else {
            it.load_next_valid();
        }
        it
    }

    pub fn seek(&mut self, target: &[u8]) {
        for layer in &mut self.layers {
            layer.seek(target);
        }
        self.load_next_valid();
    }

    pub fn seek_to_last(&mut self) {
        for layer in &mut self.layers {
            layer.seek_to_last();
        }
        self.load_prev_valid();
    }

    pub fn prev(&mut self) -> bool {
        let current_key = match self.current.clone() {
            Some((k, _)) => k,
            None => return false,
        };

        // Position each layer before current_key by seeking to current_key
        // then walking backwards past all entries with user_key >= current_key
        for layer in &mut self.layers {
            layer.seek(&current_key);
            while let Some(entry) = layer.current_entry() {
                if entry.user_key < current_key {
                    break;
                }
                layer.prev_one();
            }
        }

        self.load_prev_valid();
        if self.valid() {
            if let Some((ref k, ref v)) = self.current {
                if let Some(ref s) = self.stats {
                    s.logical_read_bytes
                        .fetch_add((k.len() + v.len()) as u64, Ordering::Relaxed);
                    s.logical_read_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        self.valid()
    }

    pub fn valid(&self) -> bool {
        self.current.is_some()
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.current.as_ref().map(|(k, _)| k.as_slice())
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.current.as_ref().map(|(_, v)| v.as_slice())
    }

    fn max_covering_range_seq(&self, user_key: &[u8]) -> Option<u64> {
        max_covering_range_tombstone_seq(&self.range_tombstones, user_key, self.sequence)
    }

    fn load_next_valid(&mut self) {
        loop {
            let min_key = self.find_min_user_key();
            let Some(min_key) = min_key else {
                self.current = None;
                return;
            };

            let mut best: Option<LayerEntry> = None;
            let mut hit_layers = Vec::new();

            for (i, layer) in self.layers.iter().enumerate() {
                let Some(entry) = layer.current_entry() else {
                    continue;
                };
                if entry.user_key != min_key {
                    continue;
                }
                hit_layers.push(i);
                if entry.sequence > self.sequence {
                    continue;
                }
                best = Some(match best {
                    None => entry,
                    Some(b) => pick_newer(b, entry),
                });
            }

            let Some(best) = best else {
                for i in &hit_layers {
                    self.layers[*i].advance_one();
                }
                continue;
            };
            if best.value_type == ValueType::TypeDelete
                || best.value_type == ValueType::TypeRangeDelete
            {
                for i in &hit_layers {
                    self.layers[*i].advance_past_user_key(&min_key);
                }
                continue;
            }
            if self
                .max_covering_range_seq(&min_key)
                .is_some_and(|rs| best.sequence <= rs)
            {
                for i in &hit_layers {
                    self.layers[*i].advance_past_user_key(&min_key);
                }
                continue;
            }
            for i in &hit_layers {
                self.layers[*i].advance_past_user_key(&min_key);
            }
            self.current = Some((best.user_key, best.value));
            return;
        }
    }

    fn find_min_user_key(&self) -> Option<Vec<u8>> {
        let mut min: Option<Vec<u8>> = None;
        for layer in &self.layers {
            let Some(entry) = layer.current_entry() else {
                continue;
            };
            if let Some(ref end) = self.end_key {
                if entry.user_key.as_slice() >= end.as_slice() {
                    continue;
                }
            }
            min = match min {
                None => Some(entry.user_key),
                Some(ref m) if entry.user_key.as_slice() < m.as_slice() => Some(entry.user_key),
                other => other,
            };
        }
        min
    }

    /// 逆序: 找到所有层中最大的 user_key (不过滤 end_key).
    fn raw_find_max_user_key(&self) -> Option<Vec<u8>> {
        let mut max: Option<Vec<u8>> = None;
        for layer in &self.layers {
            let Some(entry) = layer.current_entry() else {
                continue;
            };
            max = match max {
                None => Some(entry.user_key.clone()),
                Some(ref m) if entry.user_key.as_slice() > m.as_slice() => Some(entry.user_key),
                other => other,
            };
        }
        max
    }

    /// 逆序: 在所有层中选出最大可见 user_key (逻辑同 load_next_valid 但方向相反).
    fn load_prev_valid(&mut self) {
        loop {
            // End key 过滤: 各层需要回退到 < end_key 的位置
            if self.end_key.is_some() {
                self.skip_layers_past_end_key();
            }

            let max_key = self.raw_find_max_user_key();
            let Some(max_key) = max_key else {
                self.current = None;
                return;
            };

            // 检查 end_key 边界: 如果 >= end_key 则跳过
            if let Some(ref end) = self.end_key {
                if max_key.as_slice() >= end.as_slice() {
                    for layer in &mut self.layers {
                        if let Some(entry) = layer.current_entry() {
                            if entry.user_key == max_key {
                                layer.prev_one();
                            }
                        }
                    }
                    continue;
                }
            }

            let mut best: Option<LayerEntry> = None;
            let mut hit_layers = Vec::new();

            for i in 0..self.layers.len() {
                let Some(entry) = self.layers[i].current_entry() else {
                    continue;
                };
                if entry.user_key != max_key {
                    continue;
                }
                hit_layers.push(i);
                if entry.sequence > self.sequence {
                    continue;
                }
                best = Some(match best {
                    None => entry,
                    Some(b) => pick_newer(b, entry),
                });
            }

            let Some(best) = best else {
                for i in &hit_layers {
                    self.layers[*i].advance_one();
                }
                continue;
            };

            // Check if any hit layer has a delete for this key (needed for reverse
            // iteration where we might land on a lower-seq Put while a higher-seq
            // Delete exists before the current position).
            let has_delete = if best.value_type == ValueType::TypeDelete {
                true
            } else {
                hit_layers
                    .iter()
                    .any(|i| self.layers[*i].has_delete_for_key(&max_key))
            };

            if has_delete {
                // Reverse flow: skip deleted key by going backward
                for i in &hit_layers {
                    self.layers[*i].prev_past_user_key(&max_key);
                }
                continue;
            }
            if self
                .max_covering_range_seq(&max_key)
                .is_some_and(|rs| best.sequence <= rs)
            {
                for i in &hit_layers {
                    self.layers[*i].prev_past_user_key(&max_key);
                }
                continue;
            }
            for i in &hit_layers {
                self.layers[*i].advance_past_user_key(&max_key);
            }
            self.current = Some((best.user_key, best.value));
            return;
        }
    }

    /// 回退各层到 < end_key 的位置.
    fn skip_layers_past_end_key(&mut self) {
        let end = match &self.end_key {
            Some(e) => e,
            None => return,
        };
        for layer in &mut self.layers {
            while let Some(entry) = layer.current_entry() {
                if entry.user_key.as_slice() >= end.as_slice() {
                    layer.prev_one();
                } else {
                    break;
                }
            }
        }
    }
}

fn pick_newer(a: LayerEntry, b: LayerEntry) -> LayerEntry {
    if b.sequence > a.sequence {
        return b;
    }
    if b.sequence == a.sequence && b.value_type == ValueType::TypeDelete {
        return b;
    }
    a
}

impl Iterator for DBIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.current.clone()?;
        if let Some(ref s) = self.stats {
            s.logical_read_bytes.fetch_add(
                (current.0.len() + current.1.len()) as u64,
                Ordering::Relaxed,
            );
            s.logical_read_count.fetch_add(1, Ordering::Relaxed);
        }
        self.load_next_valid();
        Some(Ok(current))
    }
}

/// 便捷包装 (与 `DB::iter` / `DB::scan` 同型).
pub type DbIterGuard = DBIterator;
