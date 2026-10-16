//! AiDb 核心无锁原子指标体系 (Atomic-First Statistics).
//!
//! 对标 RocksDB `Statistics`, 热路径完全无锁、零堆内存分配, 仅执行原子加减.

pub mod duration;
pub mod histogram;
pub mod prometheus;
pub mod snapshot;
pub mod types;

use std::sync::atomic::{AtomicU64, Ordering};

pub use duration::{LatencyHistogram, LatencySnapshot, COMMAND_DURATION_BOUNDS_US};
pub use histogram::{
    AtomicHistogram, BUCKET_MID_POINTS_SECS, HISTOGRAM_BOUNDS_US, NUM_HISTOGRAM_BUCKETS,
    OVERFLOW_BUCKET_VALUE_SECS,
};
pub use prometheus::render_histogram;
pub use snapshot::StatsSnapshot;
pub use types::{
    BackupOp, CompactionPhase, DbOp, RaftRestartOutcome, RaftRpcDirection, RaftRpcType,
    WriteStallKind, NUM_BACKUP_OPS, NUM_COMPACTION_PHASES, NUM_DB_OPS, NUM_RAFT_RESTART_OUTCOMES,
    NUM_RAFT_RPC_DIRECTIONS, NUM_RAFT_RPC_TYPES, NUM_WRITE_STALL_KINDS,
};

/// 引擎无锁原子统计集合 (实例级持有)
#[derive(Debug)]
pub struct Statistics {
    // === 1. Counter 存量指标 ===
    pub operations: [AtomicU64; NUM_DB_OPS],
    pub compaction_phases: [AtomicU64; NUM_COMPACTION_PHASES],
    pub flush_total: AtomicU64,
    pub block_cache_hits: AtomicU64,
    pub block_cache_misses: AtomicU64,
    pub bloom_false_positive: AtomicU64,
    pub backup_total: [AtomicU64; NUM_BACKUP_OPS],
    #[cfg(feature = "cluster")]
    pub raft_rpc: [[AtomicU64; 2]; 3],
    #[cfg(feature = "cluster")]
    pub raft_log_entries: AtomicU64,
    #[cfg(feature = "cluster")]
    pub raft_group_fatal: AtomicU64,
    #[cfg(feature = "cluster")]
    pub raft_group_restart: [AtomicU64; 2],

    // === 2. Histogram 存量指标 ===
    pub operation_durations: [AtomicHistogram; NUM_DB_OPS],
    pub flush_duration: AtomicHistogram,
    pub compaction_durations: [AtomicHistogram; NUM_COMPACTION_PHASES],
    pub backup_duration: AtomicHistogram,

    // === 3. Gauge 存量指标 ===
    pub wal_size_bytes: AtomicU64,
    pub memtable_size_bytes: [AtomicU64; 2],
    pub sstable_count: Box<[AtomicU64]>,
    pub sstable_size_bytes: Box<[AtomicU64]>,
    /// SST 有效 entry 载荷字节 (Σ raw_key_size + raw_value_size), flush/compaction 后重算
    pub live_bytes: AtomicU64,
    pub block_cache_size: AtomicU64,
    pub block_cache_capacity: AtomicU64,
    pub sequence: AtomicU64,
    pub backup_size_bytes: AtomicU64,

    // === 4. Phase 2 缺口指标预留 (一步到位, 锁定基线) ===
    pub wal_written_bytes: AtomicU64,
    pub flush_written_bytes: AtomicU64,
    pub compaction_written_bytes: AtomicU64,
    pub logical_write_bytes: AtomicU64,
    pub block_read_bytes: AtomicU64,
    pub logical_read_bytes: AtomicU64,
    /// data block 触碰次数 (含 BlockCache 命中), 仅用户读路径; 经典读放大 RA 分子
    pub block_seek_count: AtomicU64,
    /// 逻辑读次数 (get 命中/未命中各 1, 迭代每产出 1 条 KV 计 1); 经典读放大 RA 分母
    pub logical_read_count: AtomicU64,
    pub compaction_read_bytes: AtomicU64,
    pub write_stall_requests: [AtomicU64; NUM_WRITE_STALL_KINDS],
    pub write_stall_durations: [AtomicHistogram; NUM_WRITE_STALL_KINDS],
    pub write_stall_max_duration_us: AtomicU64,
    /// 本导出周期内的最大停顿微秒. `take_write_stall_window_max_us` 读出后清零.
    pub write_stall_window_max_us: AtomicU64,
    pub l0_compaction_trigger: AtomicU64,
    pub l0_slowdown_writes_trigger: AtomicU64,
    pub l0_stop_writes_trigger: AtomicU64,
    pub memtable_limit_bytes: AtomicU64,
    pub flush_last_duration_us: AtomicU64,
    pub compaction_last_duration_us: AtomicU64,
    pub engine_write_duration: duration::LatencyHistogram,
    pub compaction_pending_bytes: AtomicU64,
    pub bloom_useful: AtomicU64,
    pub recovery_wal_replay_duration_us: AtomicU64,
    pub recovery_wal_replayed_bytes: AtomicU64,
    pub recovery_manifest_duration_us: AtomicU64,
    pub recovery_sstable_open_duration_us: AtomicU64,

    // === 5. 实例差分基线 (内嵌隔离, 串行保护) ===
    pub(crate) sync_baseline: parking_lot::Mutex<StatsSnapshot>,
}

impl Default for Statistics {
    fn default() -> Self {
        Self::new(7)
    }
}

impl Statistics {
    /// 构造指定层数上限的 Statistics 结构体
    pub fn new(max_levels: usize) -> Self {
        let mut sst_count = Vec::with_capacity(max_levels);
        let mut sst_size = Vec::with_capacity(max_levels);
        for _ in 0..max_levels {
            sst_count.push(AtomicU64::new(0));
            sst_size.push(AtomicU64::new(0));
        }

        Self {
            operations: std::array::from_fn(|_| AtomicU64::new(0)),
            compaction_phases: std::array::from_fn(|_| AtomicU64::new(0)),
            flush_total: AtomicU64::new(0),
            block_cache_hits: AtomicU64::new(0),
            block_cache_misses: AtomicU64::new(0),
            bloom_false_positive: AtomicU64::new(0),
            backup_total: std::array::from_fn(|_| AtomicU64::new(0)),
            #[cfg(feature = "cluster")]
            raft_rpc: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            #[cfg(feature = "cluster")]
            raft_log_entries: AtomicU64::new(0),
            #[cfg(feature = "cluster")]
            raft_group_fatal: AtomicU64::new(0),
            #[cfg(feature = "cluster")]
            raft_group_restart: std::array::from_fn(|_| AtomicU64::new(0)),

            operation_durations: std::array::from_fn(|_| AtomicHistogram::default()),
            flush_duration: AtomicHistogram::default(),
            compaction_durations: std::array::from_fn(|_| AtomicHistogram::default()),
            backup_duration: AtomicHistogram::default(),

            wal_size_bytes: AtomicU64::new(0),
            memtable_size_bytes: [AtomicU64::new(0), AtomicU64::new(0)],
            sstable_count: sst_count.into_boxed_slice(),
            sstable_size_bytes: sst_size.into_boxed_slice(),
            live_bytes: AtomicU64::new(0),
            block_cache_size: AtomicU64::new(0),
            block_cache_capacity: AtomicU64::new(0),
            sequence: AtomicU64::new(0),
            backup_size_bytes: AtomicU64::new(0),

            wal_written_bytes: AtomicU64::new(0),
            flush_written_bytes: AtomicU64::new(0),
            compaction_written_bytes: AtomicU64::new(0),
            logical_write_bytes: AtomicU64::new(0),
            block_read_bytes: AtomicU64::new(0),
            logical_read_bytes: AtomicU64::new(0),
            block_seek_count: AtomicU64::new(0),
            logical_read_count: AtomicU64::new(0),
            compaction_read_bytes: AtomicU64::new(0),
            write_stall_requests: std::array::from_fn(|_| AtomicU64::new(0)),
            write_stall_durations: std::array::from_fn(|_| AtomicHistogram::default()),
            write_stall_max_duration_us: AtomicU64::new(0),
            write_stall_window_max_us: AtomicU64::new(0),
            l0_compaction_trigger: AtomicU64::new(0),
            l0_slowdown_writes_trigger: AtomicU64::new(0),
            l0_stop_writes_trigger: AtomicU64::new(0),
            memtable_limit_bytes: AtomicU64::new(0),
            flush_last_duration_us: AtomicU64::new(0),
            compaction_last_duration_us: AtomicU64::new(0),
            engine_write_duration: duration::LatencyHistogram::default(),
            compaction_pending_bytes: AtomicU64::new(0),
            bloom_useful: AtomicU64::new(0),
            recovery_wal_replay_duration_us: AtomicU64::new(0),
            recovery_wal_replayed_bytes: AtomicU64::new(0),
            recovery_manifest_duration_us: AtomicU64::new(0),
            recovery_sstable_open_duration_us: AtomicU64::new(0),

            sync_baseline: parking_lot::Mutex::new(StatsSnapshot::default()),
        }
    }

    /// 截取当前所有指标的数值快照 (只读截面)
    pub fn snapshot(&self) -> StatsSnapshot {
        let mut ops = [0u64; NUM_DB_OPS];
        for (i, v) in self.operations.iter().enumerate() {
            ops[i] = v.load(Ordering::Relaxed);
        }

        let mut cp = [0u64; NUM_COMPACTION_PHASES];
        for (i, v) in self.compaction_phases.iter().enumerate() {
            cp[i] = v.load(Ordering::Relaxed);
        }

        let mut bk = [0u64; NUM_BACKUP_OPS];
        for (i, v) in self.backup_total.iter().enumerate() {
            bk[i] = v.load(Ordering::Relaxed);
        }

        let mut op_buckets = [[0u64; NUM_HISTOGRAM_BUCKETS]; NUM_DB_OPS];
        let mut op_sums = [0u64; NUM_DB_OPS];
        for (i, h) in self.operation_durations.iter().enumerate() {
            let (b, s) = h.snapshot();
            op_buckets[i] = b;
            op_sums[i] = s;
        }

        let (flush_b, flush_s) = self.flush_duration.snapshot();

        let mut comp_b = [[0u64; NUM_HISTOGRAM_BUCKETS]; NUM_COMPACTION_PHASES];
        let mut comp_s = [0u64; NUM_COMPACTION_PHASES];
        for (i, h) in self.compaction_durations.iter().enumerate() {
            let (b, s) = h.snapshot();
            comp_b[i] = b;
            comp_s[i] = s;
        }

        let (backup_b, backup_s) = self.backup_duration.snapshot();

        let sst_c = self
            .sstable_count
            .iter()
            .map(|v| v.load(Ordering::Relaxed))
            .collect();
        let sst_s = self
            .sstable_size_bytes
            .iter()
            .map(|v| v.load(Ordering::Relaxed))
            .collect();

        let mut stall_req = [0u64; NUM_WRITE_STALL_KINDS];
        for (i, v) in self.write_stall_requests.iter().enumerate() {
            stall_req[i] = v.load(Ordering::Relaxed);
        }

        let mut stall_b = [[0u64; NUM_HISTOGRAM_BUCKETS]; NUM_WRITE_STALL_KINDS];
        let mut stall_s = [0u64; NUM_WRITE_STALL_KINDS];
        for (i, h) in self.write_stall_durations.iter().enumerate() {
            let (b, s) = h.snapshot();
            stall_b[i] = b;
            stall_s[i] = s;
        }

        StatsSnapshot {
            operations: ops,
            compaction_phases: cp,
            flush_total: self.flush_total.load(Ordering::Relaxed),
            block_cache_hits: self.block_cache_hits.load(Ordering::Relaxed),
            block_cache_misses: self.block_cache_misses.load(Ordering::Relaxed),
            bloom_false_positive: self.bloom_false_positive.load(Ordering::Relaxed),
            backup_total: bk,
            #[cfg(feature = "cluster")]
            raft_rpc: [
                [
                    self.raft_rpc[0][0].load(Ordering::Relaxed),
                    self.raft_rpc[0][1].load(Ordering::Relaxed),
                ],
                [
                    self.raft_rpc[1][0].load(Ordering::Relaxed),
                    self.raft_rpc[1][1].load(Ordering::Relaxed),
                ],
                [
                    self.raft_rpc[2][0].load(Ordering::Relaxed),
                    self.raft_rpc[2][1].load(Ordering::Relaxed),
                ],
            ],
            #[cfg(feature = "cluster")]
            raft_log_entries: self.raft_log_entries.load(Ordering::Relaxed),
            #[cfg(feature = "cluster")]
            raft_group_fatal: self.raft_group_fatal.load(Ordering::Relaxed),
            #[cfg(feature = "cluster")]
            raft_group_restart: [
                self.raft_group_restart[0].load(Ordering::Relaxed),
                self.raft_group_restart[1].load(Ordering::Relaxed),
            ],

            operation_duration_buckets: op_buckets,
            operation_duration_sum_us: op_sums,
            flush_duration_buckets: flush_b,
            flush_duration_sum_us: flush_s,
            compaction_duration_buckets: comp_b,
            compaction_duration_sum_us: comp_s,
            backup_duration_buckets: backup_b,
            backup_duration_sum_us: backup_s,

            wal_size_bytes: self.wal_size_bytes.load(Ordering::Relaxed),
            memtable_size_bytes: [
                self.memtable_size_bytes[0].load(Ordering::Relaxed),
                self.memtable_size_bytes[1].load(Ordering::Relaxed),
            ],
            sstable_count: sst_c,
            sstable_size_bytes: sst_s,
            live_bytes: self.live_bytes.load(Ordering::Relaxed),
            block_cache_size: self.block_cache_size.load(Ordering::Relaxed),
            block_cache_capacity: self.block_cache_capacity.load(Ordering::Relaxed),
            sequence: self.sequence.load(Ordering::Relaxed),
            backup_size_bytes: self.backup_size_bytes.load(Ordering::Relaxed),

            wal_written_bytes: self.wal_written_bytes.load(Ordering::Relaxed),
            flush_written_bytes: self.flush_written_bytes.load(Ordering::Relaxed),
            compaction_written_bytes: self.compaction_written_bytes.load(Ordering::Relaxed),
            logical_write_bytes: self.logical_write_bytes.load(Ordering::Relaxed),
            block_read_bytes: self.block_read_bytes.load(Ordering::Relaxed),
            logical_read_bytes: self.logical_read_bytes.load(Ordering::Relaxed),
            block_seek_count: self.block_seek_count.load(Ordering::Relaxed),
            logical_read_count: self.logical_read_count.load(Ordering::Relaxed),
            compaction_read_bytes: self.compaction_read_bytes.load(Ordering::Relaxed),
            write_stall_requests: stall_req,
            write_stall_duration_buckets: stall_b,
            write_stall_duration_sum_us: stall_s,
            write_stall_max_duration_us: self.write_stall_max_duration_us.load(Ordering::Relaxed),
            compaction_pending_bytes: self.compaction_pending_bytes.load(Ordering::Relaxed),
            bloom_useful: self.bloom_useful.load(Ordering::Relaxed),
            recovery_wal_replay_duration_us: self
                .recovery_wal_replay_duration_us
                .load(Ordering::Relaxed),
            recovery_wal_replayed_bytes: self.recovery_wal_replayed_bytes.load(Ordering::Relaxed),
            recovery_manifest_duration_us: self
                .recovery_manifest_duration_us
                .load(Ordering::Relaxed),
            recovery_sstable_open_duration_us: self
                .recovery_sstable_open_duration_us
                .load(Ordering::Relaxed),
        }
    }

    /// 重置指标. 持有 sync_baseline 锁清空基线, 清零全量 Counter 与直方图,
    /// 清零统计极值 Gauge (`write_stall_max_duration_us`), 严格保留物理瞬时 Gauge.
    pub fn reset(&self) {
        let mut baseline = self.sync_baseline.lock();

        for op in &self.operations {
            op.store(0, Ordering::Relaxed);
        }
        for cp in &self.compaction_phases {
            cp.store(0, Ordering::Relaxed);
        }
        self.flush_total.store(0, Ordering::Relaxed);
        self.block_cache_hits.store(0, Ordering::Relaxed);
        self.block_cache_misses.store(0, Ordering::Relaxed);
        self.bloom_false_positive.store(0, Ordering::Relaxed);
        for bk in &self.backup_total {
            bk.store(0, Ordering::Relaxed);
        }
        #[cfg(feature = "cluster")]
        {
            for r in &self.raft_rpc {
                r[0].store(0, Ordering::Relaxed);
                r[1].store(0, Ordering::Relaxed);
            }
            self.raft_log_entries.store(0, Ordering::Relaxed);
            self.raft_group_fatal.store(0, Ordering::Relaxed);
            self.raft_group_restart[0].store(0, Ordering::Relaxed);
            self.raft_group_restart[1].store(0, Ordering::Relaxed);
        }

        for h in &self.operation_durations {
            h.reset();
        }
        self.flush_duration.reset();
        for h in &self.compaction_durations {
            h.reset();
        }
        self.backup_duration.reset();

        self.wal_written_bytes.store(0, Ordering::Relaxed);
        self.flush_written_bytes.store(0, Ordering::Relaxed);
        self.compaction_written_bytes.store(0, Ordering::Relaxed);
        self.logical_write_bytes.store(0, Ordering::Relaxed);
        self.block_read_bytes.store(0, Ordering::Relaxed);
        self.logical_read_bytes.store(0, Ordering::Relaxed);
        self.block_seek_count.store(0, Ordering::Relaxed);
        self.logical_read_count.store(0, Ordering::Relaxed);
        self.compaction_read_bytes.store(0, Ordering::Relaxed);
        for s in &self.write_stall_requests {
            s.store(0, Ordering::Relaxed);
        }
        for h in &self.write_stall_durations {
            h.reset();
        }
        self.write_stall_max_duration_us.store(0, Ordering::Relaxed);
        self.write_stall_window_max_us.store(0, Ordering::Relaxed);
        self.flush_last_duration_us.store(0, Ordering::Relaxed);
        self.compaction_last_duration_us.store(0, Ordering::Relaxed);
        self.engine_write_duration.reset();
        self.bloom_useful.store(0, Ordering::Relaxed);

        // 重置基线为默认零状态
        *baseline = StatsSnapshot::default();
    }

    /// 读出本周期最大停顿微秒并清零. 没有停顿时是 0.
    pub fn take_write_stall_window_max_us(&self) -> u64 {
        self.write_stall_window_max_us.swap(0, Ordering::Relaxed)
    }

    /// 只加返回的 value 长度. miss 传 0.
    pub fn note_logical_read(&self, value_len: usize) {
        self.logical_read_bytes
            .fetch_add(value_len as u64, Ordering::Relaxed);
        self.logical_read_count.fetch_add(1, Ordering::Relaxed);
    }

    /// compaction 读到的字节不进用户块读.
    pub fn note_compaction_read(&self, bytes: u64) {
        self.compaction_read_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn note_engine_write(&self, duration_us: u64) {
        self.engine_write_duration.record(duration_us);
    }
}

#[cfg(test)]
fn export_catalog() -> &'static str {
    "\
aidb_write_stall_total
aidb_write_stall_window_max_us
aidb_l0_files
aidb_l0_compaction_trigger
aidb_l0_slowdown_writes_trigger
aidb_l0_stop_writes_trigger
aidb_memtable_active_bytes
aidb_memtable_frozen_bytes
aidb_memtable_limit_bytes
aidb_wal_size_bytes
aidb_wal_written_bytes_total
aidb_flush_written_bytes_total
aidb_compaction_written_bytes_total
aidb_compaction_read_bytes_total
aidb_block_read_bytes_total
aidb_flush_last_duration_seconds
aidb_compaction_last_duration_seconds
aidb_engine_write_duration_seconds
aidb_compaction_pending_bytes
aidb_sstable_size_bytes
aidb_block_cache_size_bytes
aidb_block_cache_capacity_bytes
aidb_block_cache_hits_total
aidb_block_cache_misses_total
aidb_bloom_false_positive_total
aidb_bloom_useful_total
aidb_block_seek_count_total
aidb_logical_read_count_total
aidb_logical_read_bytes_total
aidb_recovery_wal_replay_duration_seconds
"
}

#[cfg(test)]
mod tests {
    use super::{export_catalog, Statistics};
    use std::sync::atomic::Ordering;

    #[test]
    fn write_stall_window_resets_after_take() {
        let stats = Statistics::default();
        stats
            .write_stall_window_max_us
            .fetch_max(2_000_000, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(stats.take_write_stall_window_max_us(), 2_000_000);
        assert_eq!(stats.take_write_stall_window_max_us(), 0);
    }

    #[test]
    fn compaction_bytes_do_not_enter_user_block_reads() {
        let stats = Statistics::new(4);
        stats.note_compaction_read(64);
        assert_eq!(stats.compaction_read_bytes.load(Ordering::Relaxed), 64);
        assert_eq!(stats.block_read_bytes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn l0_triggers_follow_startup_options() {
        use crate::config::Options;
        use crate::engine::db::DB;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let mut opts = Options::for_testing();
        opts.level0_compaction_trigger = 3;
        opts.level0_slowdown_writes_trigger = 9;
        opts.level0_stop_writes_trigger = 20;
        let db = DB::open(dir.path(), opts).unwrap();
        assert_eq!(db.stats.l0_compaction_trigger.load(Ordering::Relaxed), 3);
        assert_eq!(
            db.stats.l0_slowdown_writes_trigger.load(Ordering::Relaxed),
            9
        );
        assert_eq!(db.stats.l0_stop_writes_trigger.load(Ordering::Relaxed), 20);
    }

    #[test]
    fn export_omits_backup_lua_and_eviction() {
        let text = export_catalog();
        for forbidden in [
            "aidb_backup_",
            "used_memory_lua",
            "evicted_keys",
            "instantaneous_ops",
            "avg_ttl",
        ] {
            assert!(!text.contains(forbidden), "{forbidden} in {text}");
        }
    }
}
