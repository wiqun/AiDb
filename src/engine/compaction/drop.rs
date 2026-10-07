//! 删除标记是否写入 compaction 输出.

use crate::engine::memtable::ValueType;

/// 一个会留下来的文件的 user key 闭区间.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserKeyRange {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

/// 点删除和范围删除共用. 返回 true 表示不写入输出.
pub fn should_drop_delete_marker(
    value_type: ValueType,
    sequence: u64,
    min_snapshot_sequence: u64,
    marker_start: &[u8],
    marker_end: Option<&[u8]>,
    surviving: &[UserKeyRange],
) -> bool {
    if !matches!(
        value_type,
        ValueType::TypeDelete | ValueType::TypeRangeDelete
    ) {
        return false;
    }
    if min_snapshot_sequence != u64::MAX && sequence <= min_snapshot_sequence {
        return false;
    }
    let overlaps = surviving
        .iter()
        .any(|file| marker_overlaps(marker_start, marker_end, file));
    !overlaps
}

fn marker_overlaps(marker_start: &[u8], marker_end: Option<&[u8]>, file: &UserKeyRange) -> bool {
    match marker_end {
        None => file.start.as_slice() <= marker_start && marker_start <= file.end.as_slice(),
        Some(end) if marker_start < end => {
            marker_start <= file.end.as_slice() && file.start.as_slice() < end
        }
        Some(_) => false,
    }
}
