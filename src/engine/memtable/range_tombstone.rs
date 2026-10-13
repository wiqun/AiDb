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
}
