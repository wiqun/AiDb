//! 集中式 OTel Metrics (`monitoring` feature).
//!
//! aidb 无独立 OTLP 出口; 嵌入方 (aikv) 初始化 global `MeterProvider` 后调用 `init()`.

#[cfg(feature = "monitoring")]
use parking_lot::RwLock;
#[cfg(feature = "monitoring")]
use std::sync::Arc;

#[cfg(feature = "monitoring")]
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};
#[cfg(feature = "monitoring")]
use opentelemetry::KeyValue;

#[cfg(feature = "monitoring")]
static METRICS: RwLock<Option<Arc<OtelMetrics>>> = RwLock::new(None);

#[cfg(feature = "monitoring")]
const ATTR_OP: &str = "aidb.operation.name";
#[cfg(feature = "monitoring")]
const ATTR_COMPACTION_PHASE: &str = "aidb.compaction.phase";
#[cfg(feature = "monitoring")]
const ATTR_MEMTABLE_STATE: &str = "aidb.memtable.state";
#[cfg(feature = "monitoring")]
const ATTR_SSTABLE_LEVEL: &str = "aidb.sstable.level";
#[cfg(feature = "monitoring")]
const ATTR_BACKUP_OP: &str = "aidb.backup.operation";
#[cfg(all(feature = "monitoring", feature = "cluster"))]
const ATTR_RAFT_RPC_TYPE: &str = "aidb.raft.rpc.type";
#[cfg(all(feature = "monitoring", feature = "cluster"))]
const ATTR_RAFT_DIRECTION: &str = "aidb.raft.rpc.direction";
#[cfg(all(feature = "monitoring", feature = "cluster"))]
const ATTR_RAFT_RESTART_OUTCOME: &str = "aidb.raft.group.restart.outcome";
#[cfg(feature = "monitoring")]
const ATTR_DB_SYSTEM: &str = "db.system";
#[cfg(feature = "monitoring")]
const ATTR_DB_OPERATION: &str = "db.operation.name";
#[cfg(feature = "monitoring")]
const ATTR_WRITE_STALL_CAUSE: &str = "aidb_write_stall_cause";
#[cfg(feature = "monitoring")]
const ATTR_WRITE_STALL_TYPE: &str = "aidb_write_stall_type";

#[cfg(feature = "monitoring")]
struct OtelMetrics {
    wal_size_bytes: Gauge<f64>,
    memtable_size_bytes: Gauge<f64>,
    sstable_count: Gauge<f64>,
    sstable_size_bytes: Gauge<f64>,
    operations_total: Counter<u64>,
    operation_duration_seconds: Histogram<f64>,
    db_client_operations: Counter<u64>,
    db_client_operation_duration: Histogram<f64>,
    flush_duration_seconds: Histogram<f64>,
    block_cache_size_bytes: Gauge<f64>,
    block_cache_capacity_bytes: Gauge<f64>,
    block_cache_hits_total: Counter<u64>,
    block_cache_misses_total: Counter<u64>,
    bloom_false_positive_total: Counter<u64>,
    flush_total: Counter<u64>,
    sequence: Gauge<f64>,
    total_key_count: Gauge<f64>,
    compaction_total: Counter<u64>,
    compaction_duration_seconds: Histogram<f64>,
    backup_total: Counter<u64>,
    backup_size_bytes: Gauge<f64>,
    backup_duration_seconds: Histogram<f64>,
    #[cfg(feature = "cluster")]
    raft_rpc_total: Counter<u64>,
    #[cfg(feature = "cluster")]
    raft_log_entries_total: Counter<u64>,
    #[cfg(feature = "cluster")]
    raft_group_fatal_total: Counter<u64>,
    #[cfg(feature = "cluster")]
    raft_group_restart_total: Counter<u64>,
    wal_written_bytes_total: Counter<u64>,
    flush_written_bytes_total: Counter<u64>,
    compaction_written_bytes_total: Counter<u64>,
    logical_write_bytes_total: Counter<u64>,
    block_read_bytes_total: Counter<u64>,
    logical_read_bytes_total: Counter<u64>,
    compaction_read_bytes_total: Counter<u64>,
    bloom_useful_total: Counter<u64>,
    write_stall_requests_total: Counter<u64>,
    write_stall_duration_seconds: Histogram<f64>,
    write_stall_max_duration_seconds: Gauge<f64>,
    compaction_pending_bytes: Gauge<f64>,
    recovery_wal_replay_duration_seconds: Gauge<f64>,
    recovery_wal_replayed_bytes: Gauge<f64>,
    recovery_manifest_duration_seconds: Gauge<f64>,
    recovery_sstable_open_duration_seconds: Gauge<f64>,
}

#[cfg(feature = "monitoring")]
impl OtelMetrics {
    fn new(meter: Meter) -> Self {
        Self {
            wal_size_bytes: meter
                .f64_gauge("aidb_wal_size_bytes")
                .with_description("WAL 文件总大小")
                .with_unit("By")
                .build(),
            memtable_size_bytes: meter
                .f64_gauge("aidb_memtable_size_bytes")
                .with_description("MemTable 近似大小 (user_key+value 字节)")
                .with_unit("By")
                .build(),
            sstable_count: meter
                .f64_gauge("aidb_sstable_count")
                .with_description("各层 SSTable 文件数量")
                .with_unit("1")
                .build(),
            sstable_size_bytes: meter
                .f64_gauge("aidb_sstable_size_bytes")
                .with_description("各层 SSTable 文件总大小")
                .with_unit("By")
                .build(),
            operations_total: meter
                .u64_counter("aidb_operations_total")
                .with_description("DB 操作总数")
                .with_unit("1")
                .build(),
            operation_duration_seconds: meter
                .f64_histogram("aidb_operation_duration_seconds")
                .with_description("DB 操作耗时")
                .with_unit("s")
                .with_boundaries(vec![
                    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0,
                ])
                .build(),
            db_client_operations: meter
                .u64_counter("db.client.operations")
                .with_description("DB client operations (OTel semconv)")
                .with_unit("{operation}")
                .build(),
            db_client_operation_duration: meter
                .f64_histogram("db.client.operation.duration")
                .with_description("DB client operation duration (OTel semconv)")
                .with_unit("s")
                .with_boundaries(vec![
                    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0,
                ])
                .build(),
            flush_duration_seconds: meter
                .f64_histogram("aidb_flush_duration_seconds")
                .with_description("MemTable flush 耗时")
                .with_unit("s")
                .with_boundaries(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0])
                .build(),
            block_cache_size_bytes: meter
                .f64_gauge("aidb_block_cache_size_bytes")
                .with_description("Block Cache 当前大小")
                .with_unit("By")
                .build(),
            block_cache_capacity_bytes: meter
                .f64_gauge("aidb_block_cache_capacity_bytes")
                .with_description("Block Cache 配置容量上限")
                .with_unit("By")
                .build(),
            block_cache_hits_total: meter
                .u64_counter("aidb_block_cache_hits_total")
                .with_description("Block Cache 命中次数")
                .with_unit("1")
                .build(),
            block_cache_misses_total: meter
                .u64_counter("aidb_block_cache_misses_total")
                .with_description("Block Cache 未命中次数")
                .with_unit("1")
                .build(),
            bloom_false_positive_total: meter
                .u64_counter("aidb_bloom_false_positive_total")
                .with_description("Bloom Filter 假阳性次数")
                .with_unit("1")
                .build(),
            flush_total: meter
                .u64_counter("aidb_flush_total")
                .with_description("MemTable flush 次数")
                .with_unit("1")
                .build(),
            sequence: meter
                .f64_gauge("aidb_sequence")
                .with_description("当前 DB sequence")
                .with_unit("{sequence}")
                .build(),
            total_key_count: meter
                .f64_gauge("aidb_total_key_count")
                .with_description("近似存活 key 数")
                .with_unit("{key}")
                .build(),
            compaction_total: meter
                .u64_counter("aidb_compaction_total")
                .with_description("Compaction 次数")
                .with_unit("1")
                .build(),
            compaction_duration_seconds: meter
                .f64_histogram("aidb_compaction_duration_seconds")
                .with_description("Compaction 各阶段耗时")
                .with_unit("s")
                .with_boundaries(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0])
                .build(),
            backup_total: meter
                .u64_counter("aidb_backup_total")
                .with_description("备份操作计数")
                .with_unit("1")
                .build(),
            backup_size_bytes: meter
                .f64_gauge("aidb_backup_size_bytes")
                .with_description("备份文件总大小（字节）")
                .with_unit("By")
                .build(),
            backup_duration_seconds: meter
                .f64_histogram("aidb_backup_duration_seconds")
                .with_description("备份操作耗时（秒）")
                .with_unit("s")
                .with_boundaries(vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0])
                .build(),
            #[cfg(feature = "cluster")]
            raft_rpc_total: meter
                .u64_counter("aidb_raft_rpc_total")
                .with_description("Raft RPC 调用次数")
                .with_unit("1")
                .build(),
            #[cfg(feature = "cluster")]
            raft_log_entries_total: meter
                .u64_counter("aidb_raft_log_entries_total")
                .with_description("Raft 日志条目累计数")
                .with_unit("1")
                .build(),
            #[cfg(feature = "cluster")]
            raft_group_fatal_total: meter
                .u64_counter("aidb_raft_group_fatal_total")
                .with_description("检测到 Raft group 进入 Fatal 状态 (apply/存储错误) 的次数")
                .with_unit("1")
                .build(),
            #[cfg(feature = "cluster")]
            raft_group_restart_total: meter
                .u64_counter("aidb_raft_group_restart_total")
                .with_description("Raft group 自愈重启尝试次数 (按结果分类)")
                .with_unit("1")
                .build(),
            wal_written_bytes_total: meter
                .u64_counter("aidb_wal_written_bytes_total")
                .with_description("WAL 写入字节总数")
                .with_unit("By")
                .build(),
            flush_written_bytes_total: meter
                .u64_counter("aidb_flush_written_bytes_total")
                .with_description("Flush 生成 SSTable 写入字节总数")
                .with_unit("By")
                .build(),
            compaction_written_bytes_total: meter
                .u64_counter("aidb_compaction_written_bytes_total")
                .with_description("Compaction 生成 SSTable 写入字节总数")
                .with_unit("By")
                .build(),
            logical_write_bytes_total: meter
                .u64_counter("aidb_logical_write_bytes_total")
                .with_description("用户写入逻辑字节总数 (key + value)")
                .with_unit("By")
                .build(),
            block_read_bytes_total: meter
                .u64_counter("aidb_block_read_bytes_total")
                .with_description("点查与迭代读盘物理字节总数")
                .with_unit("By")
                .build(),
            logical_read_bytes_total: meter
                .u64_counter("aidb_logical_read_bytes_total")
                .with_description("用户点查与迭代消费逻辑字节总数")
                .with_unit("By")
                .build(),
            compaction_read_bytes_total: meter
                .u64_counter("aidb_compaction_read_bytes_total")
                .with_description("Compaction 输入读取字节总数")
                .with_unit("By")
                .build(),
            bloom_useful_total: meter
                .u64_counter("aidb_bloom_useful_total")
                .with_description("Bloom Filter 成功判定 key 不存在 (真阴性 TN) 次数")
                .with_unit("1")
                .build(),
            write_stall_requests_total: meter
                .u64_counter("aidb_write_stall_requests_total")
                .with_description("受写停顿 (Write Stall) 影响的写入请求次数")
                .with_unit("1")
                .build(),
            write_stall_duration_seconds: meter
                .f64_histogram("aidb_write_stall_duration_seconds")
                .with_description("写停顿 (Write Stall) 单次耗时分布")
                .with_unit("s")
                .with_boundaries(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0])
                .build(),
            write_stall_max_duration_seconds: meter
                .f64_gauge("aidb_write_stall_max_duration_seconds")
                .with_description("单次最大写停顿耗时 (秒)")
                .with_unit("s")
                .build(),
            compaction_pending_bytes: meter
                .f64_gauge("aidb_compaction_pending_bytes")
                .with_description("当前待 Compaction 积压量 (字节)")
                .with_unit("By")
                .build(),
            recovery_wal_replay_duration_seconds: meter
                .f64_gauge("aidb_recovery_wal_replay_duration_seconds")
                .with_description("WAL 回放恢复耗时")
                .with_unit("s")
                .build(),
            recovery_wal_replayed_bytes: meter
                .f64_gauge("aidb_recovery_wal_replayed_bytes")
                .with_description("回放的 WAL 物理字节总和")
                .with_unit("By")
                .build(),
            recovery_manifest_duration_seconds: meter
                .f64_gauge("aidb_recovery_manifest_duration_seconds")
                .with_description("Manifest 元数据加载恢复耗时")
                .with_unit("s")
                .build(),
            recovery_sstable_open_duration_seconds: meter
                .f64_gauge("aidb_recovery_sstable_open_duration_seconds")
                .with_description("SSTable Reader 打开恢复耗时")
                .with_unit("s")
                .build(),
        }
    }
}

#[cfg(feature = "monitoring")]
fn metrics() -> Option<Arc<OtelMetrics>> {
    METRICS.read().clone()
}

#[cfg(feature = "monitoring")]
fn db_client_attrs(op: &str) -> [KeyValue; 2] {
    [
        kv_static(ATTR_DB_SYSTEM, "aidb"),
        kv_static(ATTR_DB_OPERATION, op),
    ]
}

#[cfg(feature = "monitoring")]
fn kv_static(label: &str, value: impl Into<String>) -> KeyValue {
    KeyValue::new(label.to_string(), value.into())
}

#[cfg(feature = "monitoring")]
fn kv(label: &str, value: impl Into<String>) -> KeyValue {
    KeyValue::new(label.to_string(), value.into())
}

/// 绑定 OTel Meter (幂等). 通常由 `init()` 在 global provider 就绪后调用.
#[cfg(feature = "monitoring")]
pub fn init_otel(meter: Meter) {
    *METRICS.write() = Some(Arc::new(OtelMetrics::new(meter)));
}

/// 初始化 OTel 指标 (幂等). 需要 global `MeterProvider` 已由嵌入方设置.
pub fn init() {
    #[cfg(feature = "monitoring")]
    {
        if METRICS.read().is_none() {
            let meter = opentelemetry::global::meter("aidb");
            init_otel(meter);
        }
    }
}

/// 将无锁原子 Statistics 差分同步至全局 OTel instruments.
///
/// 锁内全链路串行化:
/// 1. 获取 `stats.sync_baseline` 锁;
/// 2. 若全局 OTel 尚未初始化 (`metrics()` 为 None), 立即返回且不推进基线 (防数据未导出即丢);
/// 3. 获取 `stats.snapshot()` 截面;
/// 4. 遍历各项指标差分与 Gauge 导出至 OTel;
/// 5. 原子推进基线 `*baseline = current`;
#[inline]
fn histogram_bucket_rep_val_secs(b: usize) -> f64 {
    if let Some(&val) = crate::statistics::histogram::BUCKET_MID_POINTS_SECS.get(b) {
        val
    } else {
        crate::statistics::histogram::OVERFLOW_BUCKET_VALUE_SECS
    }
}

/// 将无锁原子 Statistics 差分同步至全局 OTel instruments.
///
/// 锁内全链路串行化:
/// 1. 获取 `stats.sync_baseline` 锁;
/// 2. 若全局 OTel 尚未初始化 (`metrics()` 为 None), 立即返回且不推进基线 (防数据未导出即丢);
/// 3. 获取 `stats.snapshot()` 截面;
/// 4. 遍历各项指标差分与 Gauge 导出至 OTel;
/// 5. 原子推进基线 `*baseline = current`;
/// 6. 释放锁.
#[cfg(feature = "monitoring")]
pub fn sync_to_otel(stats: &crate::statistics::Statistics) {
    use crate::statistics::histogram::NUM_HISTOGRAM_BUCKETS;
    use crate::statistics::types::{BackupOp, CompactionPhase, DbOp};

    let mut baseline = stats.sync_baseline.lock();
    let Some(m) = metrics() else {
        return;
    };
    let current = stats.snapshot();

    // 1. DB Operations Counters & Histograms
    for op in DbOp::ALL {
        let op_idx = op as usize;
        let delta = current.operations[op_idx].saturating_sub(baseline.operations[op_idx]);
        if delta > 0 {
            let attr = kv(ATTR_OP, op.as_str());
            m.operations_total.add(delta, &[attr]);
            m.db_client_operations
                .add(delta, &db_client_attrs(op.as_str()));
        }

        for b in 0..NUM_HISTOGRAM_BUCKETS {
            let b_delta = current.operation_duration_buckets[op_idx][b]
                .saturating_sub(baseline.operation_duration_buckets[op_idx][b]);
            if b_delta > 0 {
                let rep_val = histogram_bucket_rep_val_secs(b);
                let attr = kv(ATTR_OP, op.as_str());
                let client_attrs = db_client_attrs(op.as_str());
                for _ in 0..b_delta {
                    m.operation_duration_seconds
                        .record(rep_val, std::slice::from_ref(&attr));
                    m.db_client_operation_duration
                        .record(rep_val, &client_attrs);
                }
            }
        }
    }

    // 2. Flush Counter & Histogram
    let flush_delta = current.flush_total.saturating_sub(baseline.flush_total);
    if flush_delta > 0 {
        m.flush_total.add(flush_delta, &[]);
    }
    for b in 0..NUM_HISTOGRAM_BUCKETS {
        let b_delta =
            current.flush_duration_buckets[b].saturating_sub(baseline.flush_duration_buckets[b]);
        if b_delta > 0 {
            let rep_val = histogram_bucket_rep_val_secs(b);
            for _ in 0..b_delta {
                m.flush_duration_seconds.record(rep_val, &[]);
            }
        }
    }

    // 3. Compaction Phases Counter & Histograms
    for phase in CompactionPhase::ALL {
        let p_idx = phase as usize;
        let p_delta =
            current.compaction_phases[p_idx].saturating_sub(baseline.compaction_phases[p_idx]);
        if p_delta > 0 {
            m.compaction_total
                .add(p_delta, &[kv(ATTR_COMPACTION_PHASE, phase.as_str())]);
        }

        for b in 0..NUM_HISTOGRAM_BUCKETS {
            let b_delta = current.compaction_duration_buckets[p_idx][b]
                .saturating_sub(baseline.compaction_duration_buckets[p_idx][b]);
            if b_delta > 0 {
                let rep_val = histogram_bucket_rep_val_secs(b);
                let attr = kv(ATTR_COMPACTION_PHASE, phase.as_str());
                for _ in 0..b_delta {
                    m.compaction_duration_seconds
                        .record(rep_val, std::slice::from_ref(&attr));
                }
            }
        }
    }

    // 4. Backup Counters, Histogram & Gauge
    for op in BackupOp::ALL {
        let b_idx = op as usize;
        let delta = current.backup_total[b_idx].saturating_sub(baseline.backup_total[b_idx]);
        if delta > 0 {
            m.backup_total
                .add(delta, &[kv(ATTR_BACKUP_OP, op.as_str())]);
        }
    }
    for b in 0..NUM_HISTOGRAM_BUCKETS {
        let b_delta =
            current.backup_duration_buckets[b].saturating_sub(baseline.backup_duration_buckets[b]);
        if b_delta > 0 {
            let rep_val = histogram_bucket_rep_val_secs(b);
            for _ in 0..b_delta {
                m.backup_duration_seconds.record(rep_val, &[]);
            }
        }
    }
    m.backup_size_bytes
        .record(current.backup_size_bytes as f64, &[]);

    // 5. Block Cache & Bloom Filter
    let hit_delta = current
        .block_cache_hits
        .saturating_sub(baseline.block_cache_hits);
    if hit_delta > 0 {
        m.block_cache_hits_total.add(hit_delta, &[]);
    }
    let miss_delta = current
        .block_cache_misses
        .saturating_sub(baseline.block_cache_misses);
    if miss_delta > 0 {
        m.block_cache_misses_total.add(miss_delta, &[]);
    }
    m.block_cache_size_bytes
        .record(current.block_cache_size as f64, &[]);
    m.block_cache_capacity_bytes
        .record(current.block_cache_capacity as f64, &[]);

    let bloom_delta = current
        .bloom_false_positive
        .saturating_sub(baseline.bloom_false_positive);
    if bloom_delta > 0 {
        m.bloom_false_positive_total.add(bloom_delta, &[]);
    }

    // 6. Gauges (物理绝对值直写)
    m.wal_size_bytes.record(current.wal_size_bytes as f64, &[]);
    m.memtable_size_bytes.record(
        current.memtable_size_bytes[0] as f64,
        &[kv(ATTR_MEMTABLE_STATE, "active")],
    );
    m.memtable_size_bytes.record(
        current.memtable_size_bytes[1] as f64,
        &[kv(ATTR_MEMTABLE_STATE, "frozen")],
    );
    m.sequence.record(current.sequence as f64, &[]);
    m.total_key_count
        .record(current.total_key_count as f64, &[]);

    for (level, (&count, &size)) in current
        .sstable_count
        .iter()
        .zip(current.sstable_size_bytes.iter())
        .enumerate()
    {
        let attrs = [kv(ATTR_SSTABLE_LEVEL, level.to_string())];
        m.sstable_count.record(count as f64, &attrs);
        m.sstable_size_bytes.record(size as f64, &attrs);
    }

    // 7. Cluster Raft 指标差分同步 (feature = "cluster")
    #[cfg(feature = "cluster")]
    {
        const RAFT_RPC_TYPES: [&str; 3] = ["append_entries", "vote", "install_snapshot"];
        const RAFT_DIRECTIONS: [&str; 2] = ["incoming", "outgoing"];
        for (t_idx, &t_name) in RAFT_RPC_TYPES.iter().enumerate() {
            for (d_idx, &d_name) in RAFT_DIRECTIONS.iter().enumerate() {
                let delta =
                    current.raft_rpc[t_idx][d_idx].saturating_sub(baseline.raft_rpc[t_idx][d_idx]);
                if delta > 0 {
                    m.raft_rpc_total.add(
                        delta,
                        &[
                            kv(ATTR_RAFT_RPC_TYPE, t_name),
                            kv(ATTR_RAFT_DIRECTION, d_name),
                        ],
                    );
                }
            }
        }

        let log_delta = current
            .raft_log_entries
            .saturating_sub(baseline.raft_log_entries);
        if log_delta > 0 {
            m.raft_log_entries_total.add(log_delta, &[]);
        }

        let fatal_delta = current
            .raft_group_fatal
            .saturating_sub(baseline.raft_group_fatal);
        if fatal_delta > 0 {
            m.raft_group_fatal_total.add(fatal_delta, &[]);
        }

        const RESTART_OUTCOMES: [&str; 2] = ["success", "failure"];
        for (i, &outcome) in RESTART_OUTCOMES.iter().enumerate() {
            let delta =
                current.raft_group_restart[i].saturating_sub(baseline.raft_group_restart[i]);
            if delta > 0 {
                m.raft_group_restart_total
                    .add(delta, &[kv(ATTR_RAFT_RESTART_OUTCOME, outcome)]);
            }
        }
    }

    // 8. 读写放大、Write Stall 与 Compaction 积压指标
    let wal_written_delta = current
        .wal_written_bytes
        .saturating_sub(baseline.wal_written_bytes);
    if wal_written_delta > 0 {
        m.wal_written_bytes_total.add(wal_written_delta, &[]);
    }
    let flush_written_delta = current
        .flush_written_bytes
        .saturating_sub(baseline.flush_written_bytes);
    if flush_written_delta > 0 {
        m.flush_written_bytes_total.add(flush_written_delta, &[]);
    }
    let comp_written_delta = current
        .compaction_written_bytes
        .saturating_sub(baseline.compaction_written_bytes);
    if comp_written_delta > 0 {
        m.compaction_written_bytes_total
            .add(comp_written_delta, &[]);
    }
    let logical_write_delta = current
        .logical_write_bytes
        .saturating_sub(baseline.logical_write_bytes);
    if logical_write_delta > 0 {
        m.logical_write_bytes_total.add(logical_write_delta, &[]);
    }
    let block_read_delta = current
        .block_read_bytes
        .saturating_sub(baseline.block_read_bytes);
    if block_read_delta > 0 {
        m.block_read_bytes_total.add(block_read_delta, &[]);
    }
    let logical_read_delta = current
        .logical_read_bytes
        .saturating_sub(baseline.logical_read_bytes);
    if logical_read_delta > 0 {
        m.logical_read_bytes_total.add(logical_read_delta, &[]);
    }
    let comp_read_delta = current
        .compaction_read_bytes
        .saturating_sub(baseline.compaction_read_bytes);
    if comp_read_delta > 0 {
        m.compaction_read_bytes_total.add(comp_read_delta, &[]);
    }
    let bloom_useful_delta = current.bloom_useful.saturating_sub(baseline.bloom_useful);
    if bloom_useful_delta > 0 {
        m.bloom_useful_total.add(bloom_useful_delta, &[]);
    }

    for kind in crate::statistics::WriteStallKind::ALL {
        let idx = kind as usize;
        let (cause, stall_type) = kind.cause_and_type();
        let attrs = [
            kv(ATTR_WRITE_STALL_CAUSE, cause),
            kv(ATTR_WRITE_STALL_TYPE, stall_type),
        ];
        let req_delta =
            current.write_stall_requests[idx].saturating_sub(baseline.write_stall_requests[idx]);
        if req_delta > 0 {
            m.write_stall_requests_total.add(req_delta, &attrs);
        }
        for b in 0..NUM_HISTOGRAM_BUCKETS {
            let b_delta = current.write_stall_duration_buckets[idx][b]
                .saturating_sub(baseline.write_stall_duration_buckets[idx][b]);
            if b_delta > 0 {
                let rep_val = histogram_bucket_rep_val_secs(b);
                for _ in 0..b_delta {
                    m.write_stall_duration_seconds.record(rep_val, &attrs);
                }
            }
        }
    }

    m.write_stall_max_duration_seconds.record(
        current.write_stall_max_duration_us as f64 / 1_000_000.0,
        &[],
    );
    m.compaction_pending_bytes
        .record(current.compaction_pending_bytes as f64, &[]);
    m.recovery_wal_replay_duration_seconds.record(
        current.recovery_wal_replay_duration_us as f64 / 1_000_000.0,
        &[],
    );
    m.recovery_wal_replayed_bytes
        .record(current.recovery_wal_replayed_bytes as f64, &[]);
    m.recovery_manifest_duration_seconds.record(
        current.recovery_manifest_duration_us as f64 / 1_000_000.0,
        &[],
    );
    m.recovery_sstable_open_duration_seconds.record(
        current.recovery_sstable_open_duration_us as f64 / 1_000_000.0,
        &[],
    );

    // 9. 推进基线
    *baseline = current;
}

#[cfg(feature = "monitoring")]
pub mod testutil {
    use std::sync::{Arc, OnceLock};

    use opentelemetry::global;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};

    static TEST_EXPORTER: OnceLock<InMemoryMetricExporter> = OnceLock::new();
    static TEST_PROVIDER: OnceLock<Arc<SdkMeterProvider>> = OnceLock::new();

    /// 测试用: 安装 InMemory exporter 并 init aidb metrics.
    pub fn init_in_memory() -> InMemoryMetricExporter {
        if let Some(exporter) = TEST_EXPORTER.get() {
            if let Some(provider) = TEST_PROVIDER.get() {
                let meter = provider.meter("aidb");
                super::init_otel(meter);
            }
            return exporter.clone();
        }
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        global::set_meter_provider(provider.clone());
        let provider = Arc::new(provider);
        let _ = TEST_PROVIDER.set(Arc::clone(&provider));
        let _ = TEST_EXPORTER.set(exporter.clone());
        let meter = provider.meter("aidb");
        super::init_otel(meter);
        exporter
    }

    fn latest_resource_metrics(
        exporter: &InMemoryMetricExporter,
    ) -> Option<Vec<opentelemetry_sdk::metrics::data::ResourceMetrics>> {
        if let Some(provider) = TEST_PROVIDER.get() {
            provider.force_flush().unwrap();
        }
        exporter.get_finished_metrics().ok()
    }

    pub fn counter_sum(exporter: &InMemoryMetricExporter, name: &str) -> u64 {
        let Some(metrics) = latest_resource_metrics(exporter) else {
            return 0;
        };
        let Some(rm) = metrics.last() else {
            return 0;
        };
        let mut total = 0u64;
        for sm in rm.scope_metrics() {
            for m in sm.metrics() {
                if m.name() != name {
                    continue;
                }
                if let AggregatedMetrics::U64(MetricData::Sum(sum)) = m.data() {
                    total += sum.data_points().map(|dp| dp.value()).sum::<u64>();
                }
            }
        }
        total
    }

    pub fn gauge_value(exporter: &InMemoryMetricExporter, name: &str) -> f64 {
        let Some(metrics) = latest_resource_metrics(exporter) else {
            return 0.0;
        };
        let Some(rm) = metrics.last() else {
            return 0.0;
        };
        for sm in rm.scope_metrics() {
            for m in sm.metrics() {
                if m.name() != name {
                    continue;
                }
                if let AggregatedMetrics::F64(MetricData::Gauge(g)) = m.data() {
                    if let Some(dp) = g.data_points().last() {
                        return dp.value();
                    }
                }
            }
        }
        0.0
    }

    pub fn histogram_count(exporter: &InMemoryMetricExporter, name: &str) -> u64 {
        let Some(metrics) = latest_resource_metrics(exporter) else {
            return 0;
        };
        let Some(rm) = metrics.last() else {
            return 0;
        };
        let mut total = 0u64;
        for sm in rm.scope_metrics() {
            for m in sm.metrics() {
                if m.name() != name {
                    continue;
                }
                if let AggregatedMetrics::F64(MetricData::Histogram(h)) = m.data() {
                    total += h.data_points().map(|dp| dp.count()).sum::<u64>();
                }
            }
        }
        total
    }

    pub fn sync_and_get_counter(stats: &crate::statistics::Statistics, metric_name: &str) -> u64 {
        super::sync_to_otel(stats);
        let exporter = init_in_memory();
        counter_sum(&exporter, metric_name)
    }

    pub fn histogram_bucket_counts(
        exporter: &InMemoryMetricExporter,
        name: &str,
    ) -> Option<Vec<u64>> {
        let metrics = latest_resource_metrics(exporter)?;
        let rm = metrics.last()?;
        let mut combined: Option<Vec<u64>> = None;
        for sm in rm.scope_metrics() {
            for m in sm.metrics() {
                if m.name() != name {
                    continue;
                }
                if let AggregatedMetrics::F64(MetricData::Histogram(h)) = m.data() {
                    for dp in h.data_points() {
                        let counts: Vec<u64> = dp.bucket_counts().collect();
                        if let Some(ref mut existing) = combined {
                            if existing.len() < counts.len() {
                                existing.resize(counts.len(), 0);
                            }
                            for (i, &c) in counts.iter().enumerate() {
                                if i < existing.len() {
                                    existing[i] += c;
                                }
                            }
                        } else {
                            combined = Some(counts);
                        }
                    }
                }
            }
        }
        combined
    }
}
