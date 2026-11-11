//! Range tombstone 辅助函数 (LevelDB 内联风格).

/// 半开区间 `[start, end)` 是否覆盖 `user_key`.
pub fn range_covers(start: &[u8], end: &[u8], user_key: &[u8]) -> bool {
    start <= user_key && user_key < end
}

/// 半开区间 `[start, end)` 是否与查询区间 `[lo, hi)` 相交 (`None` 表示无界).
pub fn range_overlaps(start: &[u8], end: &[u8], lo: Option<&[u8]>, hi: Option<&[u8]>) -> bool {
    hi.is_none_or(|hi| start < hi) && lo.is_none_or(|lo| end > lo)
}

/// Range tombstone 索引条目 (与 MemTable SkipMap 内联 entry 对应).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeTombstoneRecord {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    pub sequence: u64,
}

/// 在索引中查找覆盖 `user_key` 且 `sequence <= max_seq` 的最大 range tombstone sequence.
pub fn max_covering_range_tombstone_seq(
    records: &[RangeTombstoneRecord],
    user_key: &[u8],
    max_seq: u64,
) -> Option<u64> {
    records
        .iter()
        .filter(|r| r.sequence <= max_seq && range_covers(&r.start, &r.end, user_key))
        .map(|r| r.sequence)
        .max()
}

/// 互不重叠, 按 start 排序的范围墓碑片段. 点查二分到至多一个片段.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FragmentedRangeTombstones {
    fragments: Vec<Fragment>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Fragment {
    start: Vec<u8>,
    end: Vec<u8>,
    /// sequence 降序.
    seqs: Vec<u64>,
}

impl FragmentedRangeTombstones {
    pub fn from_records(records: impl IntoIterator<Item = RangeTombstoneRecord>) -> Self {
        Self {
            fragments: fragment_records(records),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.fragments.is_empty()
    }

    /// 覆盖 `user_key` 且 `sequence <= max_seq` 的最大 sequence.
    pub fn max_seq(&self, user_key: &[u8], max_seq: u64) -> Option<u64> {
        if self.fragments.is_empty() {
            return None;
        }
        let idx = self
            .fragments
            .partition_point(|fragment| fragment.start.as_slice() <= user_key);
        if idx == 0 {
            return None;
        }
        let fragment = &self.fragments[idx - 1];
        if user_key >= fragment.end.as_slice() {
            return None;
        }
        fragment.seqs.iter().copied().find(|seq| *seq <= max_seq)
    }
}

struct BoundaryEvent {
    key: Vec<u8>,
    /// `false` 是开区间终点, 同一 key 上先结束再开始.
    is_start: bool,
    sequence: u64,
}

fn fragment_records(records: impl IntoIterator<Item = RangeTombstoneRecord>) -> Vec<Fragment> {
    let mut events = Vec::new();
    for record in records {
        if record.start.as_slice() >= record.end.as_slice() {
            continue;
        }
        events.push(BoundaryEvent {
            key: record.end,
            is_start: false,
            sequence: record.sequence,
        });
        events.push(BoundaryEvent {
            key: record.start,
            is_start: true,
            sequence: record.sequence,
        });
    }
    events.sort_by(|left, right| {
        left.key
            .cmp(&right.key)
            .then(left.is_start.cmp(&right.is_start))
    });
    sweep_fragments(&events)
}

fn sweep_fragments(events: &[BoundaryEvent]) -> Vec<Fragment> {
    use std::collections::BTreeMap;
    let mut active: BTreeMap<u64, u32> = BTreeMap::new();
    let mut fragments = Vec::new();
    let mut previous: Option<Vec<u8>> = None;
    let mut index = 0;
    while index < events.len() {
        let key = events[index].key.clone();
        if let Some(start) = previous.as_ref() {
            if start != &key && !active.is_empty() {
                fragments.push(Fragment {
                    start: start.clone(),
                    end: key.clone(),
                    seqs: active.keys().copied().rev().collect(),
                });
            }
        }
        while index < events.len() && events[index].key == key {
            apply_boundary(&mut active, &events[index]);
            index += 1;
        }
        previous = Some(key);
    }
    fragments
}

fn apply_boundary(active: &mut std::collections::BTreeMap<u64, u32>, event: &BoundaryEvent) {
    if event.is_start {
        *active.entry(event.sequence).or_insert(0) += 1;
        return;
    }
    let Some(count) = active.get_mut(&event.sequence) else {
        return;
    };
    *count -= 1;
    if *count == 0 {
        active.remove(&event.sequence);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_covering_range_tombstone_seq_from_records() {
        let records = vec![RangeTombstoneRecord {
            start: b"10".to_vec(),
            end: b"50".to_vec(),
            sequence: 5,
        }];
        assert_eq!(
            max_covering_range_tombstone_seq(&records, b"25", 10),
            Some(5)
        );
        assert_eq!(max_covering_range_tombstone_seq(&records, b"60", 10), None);
        assert_eq!(max_covering_range_tombstone_seq(&records, b"25", 4), None);
    }

    #[test]
    fn fragmented_lookup_uses_visible_sequence_and_exclusive_end() {
        let fragmented = FragmentedRangeTombstones::from_records([
            RangeTombstoneRecord {
                start: b"a".to_vec(),
                end: b"m".to_vec(),
                sequence: 10,
            },
            RangeTombstoneRecord {
                start: b"c".to_vec(),
                end: b"z".to_vec(),
                sequence: 5,
            },
        ]);
        assert_eq!(fragmented.max_seq(b"b", 10), Some(10));
        assert_eq!(fragmented.max_seq(b"d", 7), Some(5));
        assert_eq!(fragmented.max_seq(b"m", 10), Some(5));
        assert_eq!(fragmented.max_seq(b"z", 10), None);
        assert_eq!(fragmented.max_seq(b"a", 9), None);
    }
}
