//! 配置结构 — 单机 LSM 引擎 `Options`, 以及 feature-gated 的集群配置
//! `ClusterConfig` / `MigrationConfig`; 含默认值、预设 (for_testing /
//! for_production 等) 与打开 DB 前的校验 (`Options::validate`).
//!
//! # 分层
//!
//! ```text
//! Options          引擎层: MemTable / SSTable / WAL / Compaction / 运行时调优
//! ClusterConfig    集群: group_count / replication_factor / log 限制 /
//!                   MigrationConfig / snapshot_size_threshold
//! MigrationConfig  迁移: max_batch_size / progress_report_interval /
//!                   max_retries / retry_base_delay_ms / verify_sample_factor
//! ```
//!
//! # Invariant
//!
//! - Raft 模式要求 `use_wal = true` (在 `node.rs` 强制校验, 见
//!   `docs/modules/03-cluster.md`).
//! - `Options::validate` 在 DB 打开前检查 memtable / block / level0 / poll 间隔
//!   等下限, `min_sub_compactions <= max_sub_compactions`.
//! - `ClusterConfig::for_testing()` 用 4 slot、RF=1; 生产默认 256 group、RF=3.
//! - `snapshot_size_threshold: None` = 禁用 size-based snapshot, 仅依赖
//!   `LogsSinceLast` 策略.

use std::sync::Arc;

use crate::statistics::Statistics;

/// 压缩算法
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionType {
    #[default]
    None,
    Snap,
    Lz4,
}

/// DB 打开选项
#[derive(Debug, Clone)]
pub struct Options {
    // === 基本 ===
    /// 目录不存在时自动创建
    pub create_if_missing: bool,
    /// 目录已存在时报错
    pub error_if_exists: bool,
    /// 同时打开的最大文件数 (默认 1000)
    pub max_open_files: usize,

    // === MemTable ===
    /// MemTable 触发 flush 的字节阈值 (默认 64 MiB)
    pub memtable_size: usize,
    /// 最大 MemTable 数量 (达到后冻结当前, 触发 flush)
    pub max_write_buffer_number: usize,
    /// 至少需要合并的不可变 MemTable 数 (用于写放大控制)
    pub min_write_buffer_number_to_merge: usize,

    // === SSTable ===
    /// SSTable Data Block 默认大小 (默认 4 KiB)
    pub block_size: usize,
    /// Block restart 点间隔 (默认 16)
    pub block_restart_interval: usize,
    /// Block cache 容量 (默认 64 MiB)
    pub block_cache_size: usize,
    /// 压缩算法 (Snap/Lz4 需要 `compression` feature, 否则读写均报
    /// `InvalidArgument`; 默认 None 不压缩)
    pub compression: CompressionType,
    /// Bloom Filter 目标假阳性率 (0.0 表示禁用, 默认 0.01)
    pub bloom_false_positive_rate: f64,

    // === WAL ===
    /// 是否启用 WAL (禁用后崩溃不保证数据持久)
    pub use_wal: bool,
    /// 每次写入 Record 后是否调用 fsync (默认 false; 设为 true 保证每条写 crash-safe)
    pub sync_wal: bool,
    /// 遇到 CRC 损坏时返回 Corruption 错误; false 则跳过损坏记录
    pub strict_wal_recovery: bool,
    /// WAL 文件超过此字节数时自动轮转 (默认 64 MiB, 0 = 不自动轮转)
    pub max_wal_size: u64,
    /// Group commit batching window (microseconds).  Leader waits this long before
    /// fsync to collect more concurrent writers.  0 = no additional wait (default).
    pub group_commit_batch_us: u64,

    // === Compaction ===
    /// Level 0 文件数触发 Compaction 的阈值
    pub level0_compaction_trigger: usize,
    /// Level 0 文件数超过此值 → 写入 stall, sleep 等待 compaction (默认 trigger * 2)
    pub level0_slowdown_writes_trigger: usize,
    /// Level 0 文件数超过此值 → 写入停止, 轮询等待 compaction (默认 trigger * 4)
    pub level0_stop_writes_trigger: usize,
    /// Level 1 最大字节数 (默认 256 MiB)
    pub max_bytes_for_level_base: usize,
    /// 每层大小倍率 (默认 10)
    pub max_bytes_for_level_multiplier: usize,
    /// Compaction 后台线程数 (默认 1, 建议 1-4)
    pub compaction_threads: usize,
    /// Subcompaction 分裂阈值 (bytes, 0=禁用, 默认 64MB)
    pub subcompaction_min_size: u64,
    /// MANIFEST 文件上限, 超限触发 rotation (默认 64 MiB)
    pub max_manifest_size: usize,
    /// LSM 最大层级数 (默认 7, Phase6)
    pub max_levels: usize,
    /// 是否启动后台 compaction 线程 (测试可关闭以避免与 drain_compactions 竞态)
    pub background_compaction: bool,
    // === 运行时调优 (Phase 2 新增) ===
    /// Flush 后台线程轮询间隔 (毫秒, 默认 500)
    pub flush_poll_ms: u64,
    /// Compaction 后台线程轮询间隔 (毫秒, 默认 500)
    pub compaction_poll_ms: u64,
    /// Write stall 循环 sleep 间隔 (毫秒, 默认 10)
    pub write_stall_poll_ms: u64,
    /// Slowdown 最大 sleep 时间 (毫秒, 默认 100)
    pub write_stall_slowdown_max_ms: u64,
    /// Memtable 槽等待最大迭代次数 (默认 10_000)
    pub memtable_wait_iters: usize,
    /// Memtable 槽等待轮询间隔 (毫秒, 默认 1)
    pub memtable_wait_interval_ms: u64,
    /// 子压缩最大分裂数 (默认 4)
    pub max_sub_compactions: usize,
    /// 子压缩最小分裂数 (默认 2)
    pub min_sub_compactions: usize,
    /// Compaction 信号通道容量 (默认 64)
    pub compaction_channel_size: usize,
    /// 引擎可观测性原子指标集合 (None 则在 open 时自动创建, 默认 None)
    pub statistics: Option<Arc<Statistics>>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            create_if_missing: true,
            error_if_exists: false,
            max_open_files: 1000,
            memtable_size: 64 * 1024 * 1024,
            max_write_buffer_number: 2,
            min_write_buffer_number_to_merge: 1,
            block_size: 4 * 1024,
            block_restart_interval: 16,
            block_cache_size: 64 * 1024 * 1024,
            // 生产 (启用 compression feature) 默认 Snap 压缩; 无 feature 时默认
            // None, 保证默认构建 (cargo test / 无 feature 部署) 可正常读写 SST.
            // 避免 "默认 Options 声明 Snap 但 feature 未启用" 的不一致 (见 A-001).
            #[cfg(feature = "compression")]
            compression: CompressionType::Snap,
            #[cfg(not(feature = "compression"))]
            compression: CompressionType::None,
            bloom_false_positive_rate: 0.01,
            use_wal: true,
            sync_wal: false,
            strict_wal_recovery: false,
            max_wal_size: 64 * 1024 * 1024,
            group_commit_batch_us: 0,
            level0_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 8,
            level0_stop_writes_trigger: 16,
            max_bytes_for_level_base: 256 * 1024 * 1024,
            max_bytes_for_level_multiplier: 10,
            compaction_threads: 1,
            subcompaction_min_size: 64 * 1024 * 1024,
            max_manifest_size: 64 * 1024 * 1024,
            max_levels: 7,
            background_compaction: true,
            flush_poll_ms: 500,
            compaction_poll_ms: 500,
            write_stall_poll_ms: 10,
            write_stall_slowdown_max_ms: 100,
            memtable_wait_iters: 10_000,
            memtable_wait_interval_ms: 1,
            max_sub_compactions: 4,
            min_sub_compactions: 2,
            compaction_channel_size: 64,
            statistics: None,
        }
    }
}

impl Options {
    /// 默认配置
    pub fn new() -> Self {
        Self::default()
    }

    /// 最小配置: 无压缩, 无 Bloom Filter, 小 MemTable, 适合测试
    pub fn for_testing() -> Self {
        Self {
            create_if_missing: true,
            error_if_exists: false,
            max_open_files: 100,
            memtable_size: 1024 * 1024,
            max_write_buffer_number: 2,
            min_write_buffer_number_to_merge: 1,
            block_size: 512,
            block_restart_interval: 16,
            block_cache_size: 1024 * 1024,
            compression: CompressionType::None,
            bloom_false_positive_rate: 0.0,
            use_wal: true,
            sync_wal: false,
            strict_wal_recovery: true,
            max_wal_size: 0,
            group_commit_batch_us: 0,
            level0_compaction_trigger: 2,
            level0_slowdown_writes_trigger: 4,
            level0_stop_writes_trigger: 8,
            max_bytes_for_level_base: 10 * 1024 * 1024,
            max_bytes_for_level_multiplier: 10,
            compaction_threads: 1,
            subcompaction_min_size: 0,
            max_manifest_size: 1024 * 1024,
            max_levels: 7,
            background_compaction: false,
            flush_poll_ms: 500,
            compaction_poll_ms: 500,
            write_stall_poll_ms: 10,
            write_stall_slowdown_max_ms: 100,
            memtable_wait_iters: 10_000,
            memtable_wait_interval_ms: 1,
            max_sub_compactions: 4,
            min_sub_compactions: 2,
            compaction_channel_size: 64,
            statistics: None,
        }
    }

    /// 高写入吞吐: 大 MemTable + 大 Block + 启用压缩 (有 compression feature 时)
    pub fn for_high_write_throughput() -> Self {
        Self {
            memtable_size: 256 * 1024 * 1024,
            max_write_buffer_number: 4,
            block_size: 16 * 1024,
            #[cfg(feature = "compression")]
            compression: CompressionType::Snap,
            #[cfg(not(feature = "compression"))]
            compression: CompressionType::None,
            ..Self::default()
        }
    }

    /// 校验打开 DB 前的参数 (Phase5).
    pub fn validate(&self) -> crate::error::Result<()> {
        use crate::error::Error;
        if self.memtable_size == 0 {
            return Err(Error::InvalidArgument("memtable_size must be > 0".into()));
        }
        if self.max_write_buffer_number == 0 {
            return Err(Error::InvalidArgument(
                "max_write_buffer_number must be >= 1".into(),
            ));
        }
        if self.block_size < 256 {
            return Err(Error::InvalidArgument("block_size must be >= 256".into()));
        }
        if self.block_restart_interval == 0 {
            return Err(Error::InvalidArgument(
                "block_restart_interval must be >= 1".into(),
            ));
        }
        if self.max_levels < 2 {
            return Err(Error::InvalidArgument("max_levels must be >= 2".into()));
        }
        if self.level0_compaction_trigger == 0 {
            return Err(Error::InvalidArgument(
                "level0_compaction_trigger must be >= 1".into(),
            ));
        }
        if self.flush_poll_ms == 0 {
            return Err(Error::InvalidArgument("flush_poll_ms must be > 0".into()));
        }
        if self.compaction_poll_ms == 0 {
            return Err(Error::InvalidArgument(
                "compaction_poll_ms must be > 0".into(),
            ));
        }
        if self.min_sub_compactions > self.max_sub_compactions {
            return Err(Error::InvalidArgument(
                "min_sub_compactions must be <= max_sub_compactions".into(),
            ));
        }
        Ok(())
    }

    /// 高读取吞吐: 大 Block Cache + 低假阳性率 + 小 Block
    pub fn for_high_read_throughput() -> Self {
        Self {
            block_cache_size: 512 * 1024 * 1024,
            bloom_false_positive_rate: 0.001,
            block_size: 2 * 1024,
            compression: CompressionType::None,
            ..Self::default()
        }
    }
}

// ============================================================
// Cluster 配置 (feature-gated)
// ============================================================

#[cfg(feature = "cluster")]
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// 数据 Group 数量 (默认 256)
    pub group_count: u64,
    /// 副本因子 (默认 3)
    pub replication_factor: u64,
    /// 单 Group 最大 Raft 日志条目数 (默认 1000)
    pub max_log_entries: u64,
    /// 单 Group 最大 Raft 日志字节数 (默认 64 MiB)
    pub max_log_size_bytes: u64,
    /// 迁移配置
    pub migration: MigrationConfig,
    /// 当 leader propose 成功累积的写估算字节数超过此阈值时, 触发 size-based snapshot.
    /// None = 禁用 size-based 触发, 仅依赖 LogsSinceLast (默认).
    pub snapshot_size_threshold: Option<u64>,
}

/// 在线迁移配置
#[cfg(feature = "cluster")]
#[derive(Debug, Clone)]
pub struct MigrationConfig {
    /// 每批最大迁移 key 数 (默认 1000)
    pub max_batch_size: usize,
    /// 进度上报间隔 (key 数, 默认 100)
    pub progress_report_interval: u64,
    /// MetaRaft propose 最大重试次数 (默认 3)
    pub max_retries: u32,
    /// 重试基础延迟 (毫秒, 默认 1000)
    pub retry_base_delay_ms: u64,
    /// 验证采样因子 (默认 1.0)
    pub verify_sample_factor: f64,
}

#[cfg(feature = "cluster")]
impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1000,
            progress_report_interval: 100,
            max_retries: 3,
            retry_base_delay_ms: 1000,
            verify_sample_factor: 1.0,
        }
    }
}

#[cfg(feature = "cluster")]
impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            group_count: 256,
            replication_factor: 3,
            max_log_entries: 1000,
            max_log_size_bytes: 64 * 1024 * 1024,
            migration: MigrationConfig::default(),
            snapshot_size_threshold: None,
        }
    }
}

#[cfg(feature = "cluster")]
impl ClusterConfig {
    /// 生产配置: 16384 槽, 3 副本
    pub fn for_production() -> Self {
        Self::default()
    }

    /// 测试配置: 4 槽, 1 副本
    pub fn for_testing() -> Self {
        Self {
            group_count: 4,
            replication_factor: 1,
            max_log_entries: 100,
            max_log_size_bytes: 1024 * 1024,
            migration: MigrationConfig::default(),
            snapshot_size_threshold: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options_are_sane() {
        let opts = Options::default();
        assert!(opts.create_if_missing);
        assert!(!opts.error_if_exists);
        assert!(opts.use_wal);
        assert!(!opts.sync_wal);
        assert!(!opts.strict_wal_recovery);
        assert_eq!(opts.memtable_size, 64 * 1024 * 1024);
        assert_eq!(opts.block_size, 4 * 1024);
        assert_eq!(opts.block_cache_size, 64 * 1024 * 1024);
        assert_eq!(opts.max_open_files, 1000);
        assert_eq!(opts.max_write_buffer_number, 2);
        assert_eq!(opts.min_write_buffer_number_to_merge, 1);
        assert_eq!(opts.block_restart_interval, 16);
        #[cfg(feature = "compression")]
        assert_eq!(opts.compression, CompressionType::Snap);
        #[cfg(not(feature = "compression"))]
        assert_eq!(opts.compression, CompressionType::None);
        assert!((opts.bloom_false_positive_rate - 0.01).abs() < 1e-6);
        assert_eq!(opts.level0_compaction_trigger, 4);
        assert_eq!(opts.max_bytes_for_level_base, 256 * 1024 * 1024);
        assert_eq!(opts.max_bytes_for_level_multiplier, 10);
        assert_eq!(opts.compaction_threads, 1);
        assert_eq!(opts.subcompaction_min_size, 64 * 1024 * 1024);
        assert_eq!(opts.max_manifest_size, 64 * 1024 * 1024);
        assert_eq!(opts.max_levels, 7);
        assert_eq!(opts.flush_poll_ms, 500);
        assert_eq!(opts.compaction_poll_ms, 500);
        assert_eq!(opts.write_stall_poll_ms, 10);
        assert_eq!(opts.write_stall_slowdown_max_ms, 100);
        assert_eq!(opts.memtable_wait_iters, 10_000);
        assert_eq!(opts.memtable_wait_interval_ms, 1);
        assert_eq!(opts.max_sub_compactions, 4);
        assert_eq!(opts.min_sub_compactions, 2);
        assert_eq!(opts.compaction_channel_size, 64);
    }

    #[test]
    fn for_testing_uses_small_values() {
        let opts = Options::for_testing();
        assert_eq!(opts.memtable_size, 1024 * 1024);
        assert_eq!(opts.block_size, 512);
        assert_eq!(opts.block_cache_size, 1024 * 1024);
        assert_eq!(opts.bloom_false_positive_rate, 0.0);
        assert!(opts.strict_wal_recovery);
        assert_eq!(opts.level0_compaction_trigger, 2);
        assert_eq!(opts.max_bytes_for_level_base, 10 * 1024 * 1024);
        assert_eq!(opts.max_manifest_size, 1024 * 1024);
        assert_eq!(opts.max_open_files, 100);
        assert!(!opts.sync_wal);
        assert_eq!(opts.flush_poll_ms, 500);
        assert_eq!(opts.compaction_poll_ms, 500);
    }

    #[test]
    fn for_high_write_throughput_has_large_memtable() {
        let opts = Options::for_high_write_throughput();
        assert_eq!(opts.memtable_size, 256 * 1024 * 1024);
        #[cfg(feature = "compression")]
        assert_eq!(opts.compression, CompressionType::Snap);
        #[cfg(not(feature = "compression"))]
        assert_eq!(opts.compression, CompressionType::None);
        assert_eq!(opts.block_size, 16 * 1024);
        assert_eq!(opts.max_write_buffer_number, 4);
        // unchanged from default
        assert!(opts.create_if_missing);
        assert_eq!(opts.block_cache_size, 64 * 1024 * 1024);
    }

    #[test]
    fn for_high_read_throughput_has_large_cache() {
        let opts = Options::for_high_read_throughput();
        assert_eq!(opts.block_cache_size, 512 * 1024 * 1024);
        assert!((opts.bloom_false_positive_rate - 0.001).abs() < 1e-6);
        assert_eq!(opts.block_size, 2 * 1024);
        assert_eq!(opts.compression, CompressionType::None);
        // unchanged from default
        assert!(opts.create_if_missing);
        assert_eq!(opts.memtable_size, 64 * 1024 * 1024);
    }

    #[test]
    fn validate_rejects_zero_memtable_size() {
        let mut opts = Options::for_testing();
        opts.memtable_size = 0;
        assert!(opts.validate().is_err());
    }

    #[test]
    fn validate_accepts_for_testing() {
        assert!(Options::for_testing().validate().is_ok());
    }

    #[test]
    fn compression_type_default_is_none() {
        assert_eq!(CompressionType::default(), CompressionType::None);
    }

    #[test]
    fn compression_type_debug_and_clone() {
        let c = CompressionType::Snap;
        let d = c;
        assert_eq!(format!("{:?}", d), "Snap");
    }

    #[test]
    fn validate_rejects_zero_flush_poll_ms() {
        let mut opts = Options::for_testing();
        opts.flush_poll_ms = 0;
        assert!(opts.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_compaction_poll_ms() {
        let mut opts = Options::for_testing();
        opts.compaction_poll_ms = 0;
        assert!(opts.validate().is_err());
    }

    #[test]
    fn validate_rejects_min_sub_larger_than_max_sub() {
        let mut opts = Options::for_testing();
        opts.min_sub_compactions = 5;
        opts.max_sub_compactions = 3;
        assert!(opts.validate().is_err());
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn cluster_config_defaults() {
        let cfg = ClusterConfig::default();
        assert_eq!(cfg.group_count, 256);
        assert_eq!(cfg.replication_factor, 3);
        assert_eq!(cfg.max_log_entries, 1000);
        assert_eq!(cfg.max_log_size_bytes, 64 * 1024 * 1024);
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn cluster_config_for_testing() {
        let cfg = ClusterConfig::for_testing();
        assert_eq!(cfg.group_count, 4);
        assert_eq!(cfg.replication_factor, 1);
        assert_eq!(cfg.max_log_entries, 100);
        assert_eq!(cfg.max_log_size_bytes, 1024 * 1024);
    }
}
