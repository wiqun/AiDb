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
    let _ = sequence;
    // 只要还有快照, 删除标记先留下. 比快照更新的删除若被丢掉,
    // 同 key 更老的版本仍会写出, 当前读会看到已删除的值.
    if min_snapshot_sequence != u64::MAX {
        return false;
    }
    let overlaps = surviving
        .iter()
        .any(|file| marker_overlaps(marker_start, marker_end, file));
    !overlaps
}

/// 点 key 是否落在任一幸存文件的 user key 区间内 (即该 key 的旧版本可能仍在幸存文件里).
pub fn overlaps_surviving(user_key: &[u8], surviving: &[UserKeyRange]) -> bool {
    surviving
        .iter()
        .any(|file| marker_overlaps(user_key, None, file))
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
