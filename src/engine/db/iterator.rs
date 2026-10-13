//! DB 多路归并迭代器: 跨 active MemTable + immutable MemTable + 与区间相交的 SSTable 归并,
//! 每个 user_key 只返回可见 (`seq <= sequence`) 的最新版本. 拥有各层句柄 (无自引用),
//! 供 `DB::iter` / `scan` 与 `Snapshot::iter` / `scan` 使用.
//!
//! # 数据流
//!
//! ```text
//! 层列表: active MemTable → 各 immutable MemTable → 与 [start, end) 相交的 SSTable
//! 正序:   load_next_valid 取各层当前最小 user_key, 沿组取最新可见版本
//!         → Put 且未被 range tombstone 覆盖则输出; 否则整组跳过
//! 逆序:   load_prev_valid 取各层位置中最大 user_key, 沿组回退取最新可见版本,
//!         再把各层回退到组前; 结果同样需要通过可见性与 tombstone 判定
//! ```
//!
//! # 惰性构造与剪枝
//!
//! - MemTable 层只持有 SkipMap 的 `Arc` 句柄, 按需定位并预读 32 个可见版本, 不复制整表, 迭代期间不持有 MemTable 锁.
//! - SSTable 层仅为与 `[start, end)` 相交的文件创建迭代器 (未定位, 由 seek 决定首块).
//! - range tombstone 仅收集与 `[start, end)` 相交的记录.
//!
//! # Invariant
//!
//! - `DB::iter` / `scan` 的可见边界为创建时刻的 committed sequence, 与 `Snapshot` 语义一致.
//! - `start` 为下界 (含), `end` 为上界 (不含); 逆序遍历不得返回 `< start` 的 key.
//! - 同一迭代器不支持正向与逆向 API 混用后再依赖原方向位置 (逆向返回后位置位于当前 key 之前).
//! - 本迭代器面向读路径; compaction 归并使用 `compaction::MergeIterator`, 勿混用.

use crate::engine::memtable::{
    encode_internal_key, extract_sequence, extract_user_key, extract_value_type,
    max_covering_range_tombstone_seq, range_overlaps, ImmutableMemTable, InternalKeyBytes,
    MemTable, RangeTombstoneRecord, SharedSkipMap, ValueType, K_MAX_SEQUENCE,
};
use crate::engine::sstable::{SSTableIterator, SSTableReader};
use crate::error::Result;
use crate::statistics::Statistics;
use std::ops::Bound;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// 层内当前位置的只读视图 (借用自层内部, 不复制 value).
#[derive(Clone, Copy)]
struct EntryRef<'a> {
    user_key: &'a [u8],
    value: &'a [u8],
    sequence: u64,
    value_type: ValueType,
}

/// 正序归组时的候选版本 (已复制 value, 仅用于输出).
struct Candidate {
    sequence: u64,
    value_type: ValueType,
    value: Vec<u8>,
}

fn bound_key(user_key: &[u8], sequence: u64, value_type: ValueType) -> InternalKeyBytes {
    InternalKeyBytes::from_slice(&encode_internal_key(user_key, sequence, value_type))
}

fn is_visible(internal_key: &[u8], visible_seq: u64) -> bool {
    extract_sequence(internal_key).is_ok_and(|s| s <= visible_seq)
}

/// `(seq, value_type)` 是否应替换当前最优候选: 更大 seq 优先, 同 seq 时 Delete 优先.
fn replaces(best_seq: u64, seq: u64, value_type: ValueType) -> bool {
    seq > best_seq || (seq == best_seq && value_type == ValueType::TypeDelete)
}

/// 内存表层游标每次跳表定位后预读的可见版本数, 摊薄跳表查找成本; 窗口大小与表大小无关.
const MEM_CURSOR_WINDOW: usize = 32;

/// 内存表层游标: 持有 SkipMap 的 `Arc` 句柄, 按需定位并预读一个可见版本窗口.
/// 窗口只含 `seq <= visible_seq` 的版本, 按内部 key 升序存放, `pos` 为当前位置.
struct MemCursor {
    map: SharedSkipMap,
    visible_seq: u64,
    window: Vec<(InternalKeyBytes, Arc<[u8]>)>,
    pos: usize,
}

impl MemCursor {
    fn current_pair(&self) -> Option<&(InternalKeyBytes, Arc<[u8]>)> {
        self.window.get(self.pos)
    }

    /// 从 `lower` 起向后预读 (升序).
    fn fill_forward(&mut self, lower: Bound<InternalKeyBytes>) {
        self.window.clear();
        self.pos = 0;
        let map = Arc::clone(&self.map);
        let mut entry = match &lower {
            Bound::Unbounded => map.front(),
            Bound::Included(k) => map.lower_bound(Bound::Included(k)),
            Bound::Excluded(k) => map.lower_bound(Bound::Excluded(k)),
        };
        while let Some(e) = entry {
            if is_visible(e.key().as_ref(), self.visible_seq) {
                self.window.push((e.key().clone(), e.value().clone()));
                if self.window.len() == MEM_CURSOR_WINDOW {
                    break;
                }
            }
            entry = e.next();
        }
    }

    /// 从 `upper` 起向前预读, 窗口仍按升序存放, `pos` 指向最后一条 (当前位置).
    fn fill_backward(&mut self, upper: Bound<InternalKeyBytes>) {
        self.window.clear();
        let map = Arc::clone(&self.map);
        let mut entry = match &upper {
            Bound::Unbounded => map.back(),
            Bound::Included(k) => map.upper_bound(Bound::Included(k)),
            Bound::Excluded(k) => map.upper_bound(Bound::Excluded(k)),
        };
        let mut collected = Vec::new();
        while let Some(e) = entry {
            if is_visible(e.key().as_ref(), self.visible_seq) {
                collected.push((e.key().clone(), e.value().clone()));
                if collected.len() == MEM_CURSOR_WINDOW {
                    break;
                }
            }
            entry = e.prev();
        }
        collected.reverse();
        self.pos = collected.len().saturating_sub(1);
        self.window = collected;
    }

    fn seek_user_key(&mut self, user_key: &[u8]) {
        let target = bound_key(user_key, K_MAX_SEQUENCE, ValueType::TypePut);
        self.fill_forward(Bound::Included(target));
    }

    fn seek_first(&mut self) {
        self.fill_forward(Bound::Unbounded);
    }

    fn seek_last(&mut self) {
        self.fill_backward(Bound::Unbounded);
    }

    /// 定位到 user_key < `user_key` 的最后一个可见版本.
    fn seek_last_before(&mut self, user_key: &[u8]) {
        let target = bound_key(user_key, K_MAX_SEQUENCE, ValueType::TypePut);
        self.fill_backward(Bound::Excluded(target));
    }

    fn advance(&mut self) {
        if self.pos + 1 < self.window.len() {
            self.pos += 1;
            return;
        }
        let key = match self.current_pair() {
            Some((k, _)) => k.clone(),
            None => return,
        };
        self.fill_forward(Bound::Excluded(key));
    }

    fn retreat(&mut self) {
        if self.pos > 0 {
            self.pos -= 1;
            return;
        }
        let key = match self.current_pair() {
            Some((k, _)) => k.clone(),
            None => return,
        };
        self.fill_backward(Bound::Excluded(key));
    }
}

enum LayerIterInner {
    Mem(MemCursor),
    Sst(SSTableIterator),
}

/// 单层迭代器 (MemTable 或单个 SSTable). `visible_seq` 决定哪些版本可见.
struct LayerIter {
    inner: LayerIterInner,
    visible_seq: u64,
}

impl LayerIter {
    fn from_memtable(map: SharedSkipMap, visible_seq: u64) -> Self {
        Self {
            inner: LayerIterInner::Mem(MemCursor {
                map,
                visible_seq,
                window: Vec::new(),
                pos: 0,
            }),
            visible_seq,
        }
    }

    fn from_sstable(reader: &SSTableReader, visible_seq: u64) -> Self {
        Self {
            inner: LayerIterInner::Sst(reader.iter_unpositioned()),
            visible_seq,
        }
    }

    fn current(&self) -> Option<EntryRef<'_>> {
        let (key, value) = match &self.inner {
            LayerIterInner::Mem(c) => {
                let (k, v) = c.current_pair()?;
                (k.as_ref(), v.as_ref())
            }
            LayerIterInner::Sst(it) => (it.key()?, it.value()?),
        };
        Some(EntryRef {
            user_key: extract_user_key(key),
            value,
            sequence: extract_sequence(key).ok()?,
            value_type: extract_value_type(key).ok()?,
        })
    }

    fn advance_one(&mut self) {
        match &mut self.inner {
            LayerIterInner::Mem(c) => c.advance(),
            LayerIterInner::Sst(it) => {
                it.advance();
            }
        }
    }

    fn retreat_one(&mut self) {
        match &mut self.inner {
            LayerIterInner::Mem(c) => c.retreat(),
            LayerIterInner::Sst(it) => {
                it.prev();
            }
        }
    }

    fn skip_invisible_forward(&mut self) {
        while self
            .current()
            .is_some_and(|e| e.sequence > self.visible_seq)
        {
            self.advance_one();
        }
    }

    fn skip_invisible_backward(&mut self) {
        while self
            .current()
            .is_some_and(|e| e.sequence > self.visible_seq)
        {
            self.retreat_one();
        }
    }

    /// 定位到 >= user_key 的第一个可见版本 (组内最新可见版本).
    fn seek_user_key(&mut self, user_key: &[u8]) {
        match &mut self.inner {
            LayerIterInner::Mem(c) => c.seek_user_key(user_key),
            LayerIterInner::Sst(it) => it.seek_to_target(&encode_internal_key(
                user_key,
                K_MAX_SEQUENCE,
                ValueType::TypePut,
            )),
        }
        self.skip_invisible_forward();
    }

    fn seek_first(&mut self) {
        match &mut self.inner {
            LayerIterInner::Mem(c) => c.seek_first(),
            LayerIterInner::Sst(it) => it.seek_to_first(),
        }
        self.skip_invisible_forward();
    }

    fn seek_last(&mut self) {
        match &mut self.inner {
            LayerIterInner::Mem(c) => c.seek_last(),
            LayerIterInner::Sst(it) => it.seek_to_last(),
        }
        self.skip_invisible_backward();
    }

    /// 定位到 user_key < `user_key` 的最后一个可见版本.
    fn seek_last_before(&mut self, user_key: &[u8]) {
        match &mut self.inner {
            LayerIterInner::Mem(c) => c.seek_last_before(user_key),
            LayerIterInner::Sst(it) => {
                let target = encode_internal_key(user_key, K_MAX_SEQUENCE, ValueType::TypePut);
                it.seek_to_target(&target);
                if it.valid() {
                    it.prev();
                } else {
                    it.seek_to_last();
                }
            }
        }
        self.skip_invisible_backward();
    }
}

/// 跨 MemTable + SSTable 的合并迭代器.
pub struct DBIterator {
    current: Option<(Vec<u8>, Vec<u8>)>,
    sequence: u64,
    layers: Vec<LayerIter>,
    start_key: Option<Vec<u8>>,
    end_key: Option<Vec<u8>>,
    range_tombstones: Vec<RangeTombstoneRecord>,
    stats: Option<Arc<Statistics>>,
}

/// SSTable 的 user key 范围 `[smallest, largest]` 与查询区间 `[start, end)` 是否相交.
/// 空文件或键长异常时保守视为相交.
fn sst_overlaps(reader: &SSTableReader, start: Option<&[u8]>, end: Option<&[u8]>) -> bool {
    let (smallest, largest) = (reader.smallest_key(), reader.largest_key());
    if smallest.len() < 8 || largest.len() < 8 {
        return true;
    }
    let smallest = extract_user_key(smallest);
    let largest = extract_user_key(largest);
    start.is_none_or(|s| largest >= s) && end.is_none_or(|e| smallest < e)
}

fn push_range_records(
    out: &mut Vec<RangeTombstoneRecord>,
    records: Vec<(Vec<u8>, Vec<u8>, u64)>,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
) {
    for (start_key, end_key, sequence) in records {
        if range_overlaps(&start_key, &end_key, start, end) {
            out.push(RangeTombstoneRecord {
                start: start_key,
                end: end_key,
                sequence,
            });
        }
    }
}

fn collect_range_tombstones(
    memtable: &MemTable,
    immutables: &[ImmutableMemTable],
    sstables: &[Vec<Arc<SSTableReader>>],
    start: Option<&[u8]>,
    end: Option<&[u8]>,
) -> Vec<RangeTombstoneRecord> {
    let mut out = Vec::new();
    if let Ok(v) = memtable.collect_range_tombstones() {
        push_range_records(&mut out, v, start, end);
    }
    for imm in immutables {
        if let Ok(v) = imm.collect_range_tombstones() {
            push_range_records(&mut out, v, start, end);
        }
    }
    for level in sstables {
        for reader in level {
            push_range_records(
                &mut out,
                reader.collect_range_tombstones_overlapping(start, end),
                start,
                end,
            );
        }
    }
    out
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
        layers.push(LayerIter::from_memtable(
            memtable.rep().shared_map(),
            sequence,
        ));
        for imm in immutables {
            layers.push(LayerIter::from_memtable(
                imm.inner().rep().shared_map(),
                sequence,
            ));
        }
        for level in sstables {
            for reader in level {
                if sst_overlaps(reader, start, end) {
                    layers.push(LayerIter::from_sstable(reader, sequence));
                }
            }
        }

        let mut it = Self {
            current: None,
            sequence,
            layers,
            start_key: start.map(<[u8]>::to_vec),
            end_key: end.map(<[u8]>::to_vec),
            range_tombstones: collect_range_tombstones(memtable, immutables, sstables, start, end),
            stats,
        };
        match start {
            Some(start) => it.seek(start),
            None => {
                for layer in &mut it.layers {
                    layer.seek_first();
                }
                it.load_next_valid();
            }
        }
        it
    }

    pub fn seek(&mut self, target: &[u8]) {
        for layer in &mut self.layers {
            layer.seek_user_key(target);
        }
        self.load_next_valid();
    }

    pub fn seek_to_last(&mut self) {
        let end = self.end_key.as_deref();
        for layer in &mut self.layers {
            match end {
                Some(end) => layer.seek_last_before(end),
                None => layer.seek_last(),
            }
        }
        self.load_prev_valid();
    }

    pub fn prev(&mut self) -> bool {
        let current_key = match self.current.as_ref() {
            Some((k, _)) => k.clone(),
            None => return false,
        };
        // 各层回退到 current_key 之前的最后一个可见版本 (SST 层可能整体位于 current_key 之前).
        for layer in &mut self.layers {
            layer.seek_last_before(&current_key);
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

    fn is_range_covered(&self, user_key: &[u8], sequence: u64) -> bool {
        max_covering_range_tombstone_seq(&self.range_tombstones, user_key, self.sequence)
            .is_some_and(|rs| sequence <= rs)
    }

    /// 正序: 各层当前位置的最小 user_key (忽略 `>= end` 的位置).
    fn min_layer_user_key(&self) -> Option<Vec<u8>> {
        let end = self.end_key.as_deref();
        let mut min: Option<&[u8]> = None;
        for layer in &self.layers {
            let Some(e) = layer.current() else {
                continue;
            };
            if end.is_some_and(|end| e.user_key >= end) {
                continue;
            }
            if min.is_none_or(|m| e.user_key < m) {
                min = Some(e.user_key);
            }
        }
        min.map(<[u8]>::to_vec)
    }

    /// 逆序: 各层当前位置的最大 user_key.
    fn max_layer_user_key(&self) -> Option<Vec<u8>> {
        let mut max: Option<&[u8]> = None;
        for layer in &self.layers {
            let Some(e) = layer.current() else {
                continue;
            };
            if max.is_none_or(|m| e.user_key > m) {
                max = Some(e.user_key);
            }
        }
        max.map(<[u8]>::to_vec)
    }

    /// 正序沿组消费 `key` 在各层的全部版本, 返回最新可见版本 (仅在胜出时复制 value).
    fn consume_group_forward(&mut self, key: &[u8]) -> Option<Candidate> {
        let visible = self.sequence;
        let mut best: Option<Candidate> = None;
        for layer in &mut self.layers {
            while let Some(e) = layer.current() {
                if e.user_key != key {
                    break;
                }
                let wins = e.sequence <= visible
                    && best
                        .as_ref()
                        .is_none_or(|b| replaces(b.sequence, e.sequence, e.value_type));
                if wins {
                    best = Some(Candidate {
                        sequence: e.sequence,
                        value_type: e.value_type,
                        value: e.value.to_vec(),
                    });
                }
                layer.advance_one();
            }
        }
        best
    }

    fn load_next_valid(&mut self) {
        loop {
            let Some(min_key) = self.min_layer_user_key() else {
                self.current = None;
                return;
            };
            let Some(best) = self.consume_group_forward(&min_key) else {
                continue;
            };
            if best.value_type == ValueType::TypePut
                && !self.is_range_covered(&min_key, best.sequence)
            {
                self.current = Some((min_key, best.value));
                return;
            }
        }
    }

    /// 逆序沿组消费 `key`: 只记录各层组内最新可见版本的 (层号, seq, 类型), 不复制 value.
    /// 返回后各层位置均位于 `key` 组之前.
    fn consume_group_backward(&mut self, key: &[u8]) -> Option<(usize, u64, ValueType)> {
        let visible = self.sequence;
        let mut best: Option<(usize, u64, ValueType)> = None;
        for (idx, layer) in self.layers.iter_mut().enumerate() {
            while let Some(e) = layer.current() {
                if e.user_key != key {
                    break;
                }
                if e.sequence <= visible && best.is_none_or(|(_, s, t)| replaces(s, e.sequence, t))
                {
                    best = Some((idx, e.sequence, e.value_type));
                }
                layer.retreat_one();
            }
        }
        best
    }

    /// 读取第 `idx` 层中 `key` 的最新可见 value, 并把该层恢复到 `key` 组之前.
    fn read_backward_value(&mut self, idx: usize, key: &[u8]) -> Vec<u8> {
        let layer = &mut self.layers[idx];
        layer.seek_user_key(key);
        let value = layer
            .current()
            .map(|e| e.value.to_vec())
            .unwrap_or_default();
        layer.seek_last_before(key);
        value
    }

    fn load_prev_valid(&mut self) {
        loop {
            let Some(max_key) = self.max_layer_user_key() else {
                self.current = None;
                return;
            };
            if self
                .start_key
                .as_deref()
                .is_some_and(|s| max_key.as_slice() < s)
            {
                self.current = None;
                return;
            }
            let picked = self.consume_group_backward(&max_key);
            let value = match picked {
                Some((idx, seq, ValueType::TypePut)) if !self.is_range_covered(&max_key, seq) => {
                    Some(self.read_backward_value(idx, &max_key))
                }
                _ => None,
            };
            if let Some(value) = value {
                self.current = Some((max_key, value));
                return;
            }
        }
    }
}

impl Iterator for DBIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.current.take()?;
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
