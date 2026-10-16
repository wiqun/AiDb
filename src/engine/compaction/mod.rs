//! Compaction: Version 管理, 多路归并, 文件选择与后台整理.

mod drop;
pub mod filter;
mod helpers;
mod job;
mod merge;
mod picker;
mod trackers;
mod version;

pub use drop::UserKeyRange;
pub use filter::{
    CompactionFilter, CompactionPointReader, CompactionRemovalListener, FilterDecision,
};
pub use helpers::{key_ranges_overlap_by_meta_raw, user_key_from_internal};
pub use job::{CompactionJob, CompactionResult};
pub use merge::MergeIterator;
pub use picker::{CompactionPicker, CompactionTask};
pub use version::{
    current_exists, load_sstables_from_version, remove_orphan_sstables,
    scan_version_edits_from_dir, FileMetaData, Version, VersionEdit, VersionSet,
};

pub fn target_size_for_level(level: usize, opts: &crate::config::Options) -> u64 {
    if level == 0 {
        return u64::MAX;
    }
    let base = opts.max_bytes_for_level_base as u64;
    let mult = opts.max_bytes_for_level_multiplier as u64;
    base * mult.pow(level.saturating_sub(1) as u32)
}
