//! 只读指标截面快照定义.

use super::histogram::NUM_HISTOGRAM_BUCKETS;
use super::types::{NUM_BACKUP_OPS, NUM_COMPACTION_PHASES, NUM_DB_OPS, NUM_WRITE_STALL_KINDS};

/// 某一时点的引擎无锁原子统计快照 (纯数值, 只读)
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    // === 存量 Counter 截面 ===
    pub operations: [u64; NUM_DB_OPS],
    pub compaction_phases: [u64; NUM_COMPACTION_PHASES],
    pub flush_total: u64,
    pub block_cache_hits: u64,
    pub block_cache_misses: u64,
    pub bloom_false_positive: u64,
    pub backup_total: [u64; NUM_BACKUP_OPS],
    #[cfg(feature = "cluster")]
    pub raft_rpc: [[u64; 2]; 3],
    #[cfg(feature = "cluster")]
    pub raft_log_entries: u64,
    #[cfg(feature = "cluster")]
    pub raft_group_fatal: u64,
    #[cfg(feature = "cluster")]
    pub raft_group_restart: [u64; 2],

    // === 存量 Histogram 截面 (含桶与真实总耗时 sum_us) ===
    pub operation_duration_buckets: [[u64; NUM_HISTOGRAM_BUCKETS]; NUM_DB_OPS],
    pub operation_duration_sum_us: [u64; NUM_DB_OPS],
    pub flush_duration_buckets: [u64; NUM_HISTOGRAM_BUCKETS],
    pub flush_duration_sum_us: u64,
    pub compaction_duration_buckets: [[u64; NUM_HISTOGRAM_BUCKETS]; NUM_COMPACTION_PHASES],
    pub compaction_duration_sum_us: [u64; NUM_COMPACTION_PHASES],
    pub backup_duration_buckets: [u64; NUM_HISTOGRAM_BUCKETS],
    pub backup_duration_sum_us: u64,

    // === 存量 Gauge 截面 ===
    pub wal_size_bytes: u64,
    pub memtable_size_bytes: [u64; 2],
    pub sstable_count: Vec<u64>,
    pub sstable_size_bytes: Vec<u64>,
    /// SST 有效 entry 载荷字节 (Σ raw_key_size + raw_value_size)
    pub live_bytes: u64,
    pub block_cache_size: u64,
    pub block_cache_capacity: u64,
    pub sequence: u64,
    pub backup_size_bytes: u64,

    // === Phase 2 完整预留字段镜像 (确保基线架构一步到位) ===
    pub wal_written_bytes: u64,
    pub flush_written_bytes: u64,
    pub compaction_written_bytes: u64,
    pub logical_write_bytes: u64,
    pub block_read_bytes: u64,
    pub logical_read_bytes: u64,
    /// data block 触碰次数 (含 BlockCache 命中, 仅用户读路径)
    pub block_seek_count: u64,
    /// 逻辑读次数 (get 命中/未命中各 1, 迭代每产出 1 条 KV 计 1)
    pub logical_read_count: u64,
    pub compaction_read_bytes: u64,
    pub write_stall_requests: [u64; NUM_WRITE_STALL_KINDS],
    pub write_stall_duration_buckets: [[u64; NUM_HISTOGRAM_BUCKETS]; NUM_WRITE_STALL_KINDS],
    pub write_stall_duration_sum_us: [u64; NUM_WRITE_STALL_KINDS],
    pub write_stall_max_duration_us: u64,
    pub compaction_pending_bytes: u64,
    pub bloom_useful: u64,
    pub recovery_wal_replay_duration_us: u64,
    pub recovery_wal_replayed_bytes: u64,
    pub recovery_manifest_duration_us: u64,
    pub recovery_sstable_open_duration_us: u64,
}
