---
name: aidb-metrics
description: AiDb 全量 OTel 指标总表 (仅当前已存在指标). 查指标名, 标签与基数, 类型, 单位, 数据源, 代码位置与典型 PromQL 时读本文; 改埋点前定位调用点也读本文.
---

# AiDb Metrics (指标总表)

## 何时读本文

- 查询 `aidb_*` 指标的名称, 标签与基数, 类型, 单位, 埋点位置与典型 PromQL
- bench / 监控面板选指标, 或改埋点前定位代码
- **不覆盖**: tracing span 索引, `metrics::init` 嵌入机制 → [observability.md](modules/05-observability.md)
- **不覆盖**: 待新增指标 (缺口清单) 与复合指标计算方式 → 工作区根目录 `bench-metrics-audit.md`

## 命名口径

- 指标名统一使用 **Prometheus 渲染名** (下划线形式); OTLP 原生属性为点号形式 (`aidb.operation.name`, `db.client.operations` 等), bench 直查 OTLP 后端 (非经 Collector 转 Prometheus) 时名称不同.
- `db.client.*` 为 OTel semconv 双写指标, 其 Prometheus 渲染名 (是否追加 `_total` / 单位后缀) 取决于 Collector 转换配置, 以实际导出为准.
- 类型统一按 Prometheus 口径标注 (Counter / Gauge / Histogram); 单位为代码中声明的 UCUM 单位.
- 全部指标需 `--features monitoring` 编译; 集群指标额外需 `--features cluster`.
- aidb 为嵌入库, 无独立 OTLP 出口; 核心指标体系采用 Atomic-first 架构, 引擎热路径无锁直写实例级原子字典 (`Arc<Statistics>`), 消除全部全局读锁争用与堆内存分配. aikv 生产导出走导出快照, 不调用 `sync_to_otel`. `sync_to_otel` 只在测试里把差分登记到 `src/metrics.rs` 的旧 meter. 快照不含 `aidb_flush_total` 与 `aidb_compaction_total`.
- 双通道对账契约:
  - 快照直读 (`INFO storage`): 返回无 `_total` 后缀的即时快照值 (如 `aidb_wal_written_bytes`, `aidb_write_stall_requests`), 适合压测前后单轮差分计算.
  - OTel 导出: 返回符合 Prometheus 命名规范的单调 Counter (`_total`), 标签化 Histogram (`_seconds_bucket`) 与 Gauge.
  - 对账一致性: 写停顿次数按 `cause` 与 `kind` 求和. 本导出周期的最大停顿微秒在读出后清零, 不再导出生命周期最大值, 也不再按桶中点重放直方图.
  - 冷启动恢复与线程对账: `INFO storage` 的 5 项恢复指标与 OTel Gauge 对应（耗时微秒与秒换算一致、字节直读 1:1 等价），且 aidb 4 项恢复指标在 `Statistics::reset()` 保护白名单内常驻不被清零; `INFO threads` 渲染 3 项系统指标，OTel 侧线程数为 Gauge 镜像，上下文切换数以差分方式推入 Counter（先 add 后推进基线，崩溃安全）.
- 缓存卫生与读放大隔离 (`iter_uncached`):
  - Compaction 遍历 SSTable 全面改走 `iter_uncached()`, 绕过 BlockCache, 杜绝后台大范围 I/O 冲刷用户热数据并污染缓存命中率.
  - Reader 打开阶段的 `load_range_tombstones()` 同样以未缓存模式读取, 确保只在用户业务读路径发生实际缓存填充与命中统计.
  - 用户点查的逻辑字节只计返回 value 的长度, miss 加 0. Compaction 读字节只进 `aidb_compaction_read_bytes_total`.
- 表格列说明:
  - **标签与基数**: 标注关键标签名, 枚举值范围及预期基数, 便于防范高基数风险.
  - **数据源与代码位置**: 保留准确的更新方式与源码行号, 便于研发定位调用与开销.
  - **说明 (实现陷阱与口径)**: 重点记录采样失真, compaction 污染缓存, 统计盲区等底层技术陷阱.
  - **用途与典型 PromQL**: 提供直接可用的 PromQL 表达式与监控/压测用途.

## 引擎指标

| 指标名 | 类型 | 单位 | 标签与基数 | 数据源与代码位置 | 说明 (实现陷阱与口径) | 用途与典型 PromQL |
| --- | --- | --- | --- | --- | --- | --- |
| `aidb_wal_size_bytes` | Gauge | By | — (基数 1) | WAL 追加与轮转/清理时刷新原子值<br>`stats.wal_size_bytes`; `engine/wal/manager.rs` | WAL 当前文件大小 | 空间放大 (SA) 分子组成; LSM 形态观测:<br>`aidb_wal_size_bytes` |
| `aidb_wal_written_bytes_total` | Counter | By | — (基数 1) | WALManager 写入记录时累加实际落盘字节<br>`stats.wal_written_bytes`; `engine/wal/manager.rs` | WAL 物理写字节 (含 CRC, 长度, 头部与数据) | 写放大 (WA) 分子组成:<br>`rate(aidb_wal_written_bytes_total[1m])` |
| `aidb_memtable_active_bytes` | Gauge | By | — (基数 1) | 活跃 MemTable 近似大小<br>`stats.memtable_size_bytes[0]` | 不含冻结表 | `aidb_memtable_active_bytes` |
| `aidb_memtable_frozen_bytes` | Gauge | By | — (基数 1) | 已冻结 MemTable 近似大小<br>`stats.memtable_size_bytes[1]` | 不含活跃表 | `aidb_memtable_frozen_bytes` |
| `aidb_memtable_limit_bytes` | Gauge | By | — (基数 1) | 打开 DB 时写入 `Options.memtable_size`<br>`stats.memtable_limit_bytes` | 启动后不变 | `aidb_memtable_limit_bytes` |
| `aidb_l0_files` | Gauge | 1 | — (基数 1) | L0 文件数<br>`stats.sstable_count[0]` | 与 `aidb_sstable_count{level="0"}` 同一数字 | `aidb_l0_files` |
| `aidb_l0_compaction_trigger` | Gauge | 1 | — (基数 1) | 打开 DB 时的 `level0_compaction_trigger` | 启动配置, 不是写死的 4 | `aidb_l0_compaction_trigger` |
| `aidb_l0_slowdown_writes_trigger` | Gauge | 1 | — (基数 1) | 打开 DB 时的 `level0_slowdown_writes_trigger` | 启动配置 | `aidb_l0_slowdown_writes_trigger` |
| `aidb_l0_stop_writes_trigger` | Gauge | 1 | — (基数 1) | 打开 DB 时的 `level0_stop_writes_trigger` | 启动配置 | `aidb_l0_stop_writes_trigger` |
| `aidb_sstable_count` | Gauge | 1 | `level` | DB open / flush 完成 / compaction apply 后现算<br>`stats.sstable_count` | 各层 SST 文件数 | `aidb_sstable_count{level="0"}` |
| `aidb_sstable_size_bytes` | Gauge | By | `level` | 同上 | 各层 SST 总大小. 未启用 Raft 时没有 `group` | `sum(aidb_sstable_size_bytes)` |
| `aidb_live_bytes` | Gauge | By | — (基数 1) | DB open / flush 完成 / compaction apply 后 Σ 各 SST Properties 的 `raw_key_size + raw_value_size` 现算<br>`stats.live_bytes`; `engine/db/inner/mod.rs` (`update_sstable_metrics`) | 全部 SST 有效 entry 载荷字节 (未压缩, 含 internal key +8B/条); 旧版本滞留推高, compaction 去重后收敛; reset 保留 | **空间放大 (SA) 分母 (SST 侧)**, 面板补 MemTable:<br>`sum(aidb_live_bytes) + aidb_memtable_active_bytes + aidb_memtable_frozen_bytes` |
| `aidb_operations_total` | Counter | 1 | `aidb_operation_name(6: put\|delete\|write_batch\|write_batch_no_wal\|get\|snapshot)` | 引擎热路径原子直写 + `sync_to_otel` 差分同步<br>`stats.operations` | `stall_stop` 与 `stall_slowdown` 不再导出 | `sum by (aidb_operation_name) (rate(aidb_operations_total[1m]))` |
| `aidb_engine_write_duration_seconds` | Histogram | s | — (基数 1) | 每个用户写批次结束记一次真实耗时<br>`stats.engine_write_duration` | 不含 flush 与 get. 累计桶与观测值之和, 不按桶中点重放 | `histogram_quantile(0.99, rate(aidb_engine_write_duration_seconds_bucket[1m]))` |
| `aidb_logical_write_bytes_total` | Counter | By | — (基数 1) | 写路径原子累加 user_key + value 字节<br>`stats.logical_write_bytes`; `engine/db/inner/write.rs` | 引擎级逻辑写入字节; 集群模式白名单 `\x01sm/` 过滤 Raft log; 主口径建议配合协议级交叉验证 | 写放大 (WA) 分母:<br>`rate(aidb_logical_write_bytes_total[1m])` |
| `aidb_flush_total` | Counter | 1 | — (基数 1) | flush 完成时原子累加<br>`stats.flush_total`; `engine/db/inner/flush.rs` | 内部按轮累加. `INFO storage` 有这个字段. 生产导出快照没有这条序列 | 不作为 Grafana 频率. flush 看字节速率和最近一次耗时:<br>`rate(aidb_flush_written_bytes_total[$rate_window])`<br>`aidb_flush_last_duration_seconds` |
| `aidb_flush_written_bytes_total` | Counter | By | — (基数 1) | MemTable 刷盘 SSTable 完成时原子累加<br>`stats.flush_written_bytes`; `engine/db/inner/flush.rs` | Flush 物理生成 SSTable 字节数 (空 memtable 提前返回不计入) | 写放大 (WA) 分子组成:<br>`rate(aidb_flush_written_bytes_total[1m])` |
| `aidb_flush_last_duration_seconds` | Gauge | s | — (基数 1) | flush 结束时覆盖写入<br>`stats.flush_last_duration_us` | 最近一次耗时, 不是直方图 | `aidb_flush_last_duration_seconds` |
| `aidb_compaction_total` | Counter | 1 | `aidb_compaction_phase(3: pick\|run\|apply)`<br>基数 3 | Compaction 各阶段原子累加<br>`stats.compaction_phases`; `engine/db/inner/compaction.rs` | **trivial move 只记 `pick` 阶段** (无 run/apply). 生产导出快照没有这条序列 | 不作为 Grafana 频次. compaction 看字节速率和最近一次耗时:<br>`rate(aidb_compaction_written_bytes_total[$rate_window])`<br>`aidb_compaction_last_duration_seconds` |
| `aidb_compaction_read_bytes_total` | Counter | By | — (基数 1) | Compaction 开始时一次性累加输入 SST 文件大小<br>`stats.compaction_read_bytes`; `engine/db/inner/compaction.rs` | Compaction 逻辑输入读盘字节 (排除 trivial move, 多个 split 共享输入只计一次) | 后台 Compaction 读 I/O 速率:<br>`rate(aidb_compaction_read_bytes_total[1m])` |
| `aidb_compaction_written_bytes_total` | Counter | By | — (基数 1) | Compaction 生成新 SSTable 完成后累加文件大小<br>`stats.compaction_written_bytes`; `engine/db/inner/compaction.rs` | Compaction 物理输出写盘字节 (排除 trivial move) | 写放大 (WA) 分子组成:<br>`rate(aidb_compaction_written_bytes_total[1m])` |
| `aidb_compaction_last_duration_seconds` | Gauge | s | — (基数 1) | compaction 一轮结束时覆盖写入<br>`stats.compaction_last_duration_us` | 最近一次耗时, 不是直方图 | `aidb_compaction_last_duration_seconds` |
| `aidb_compaction_pending_bytes` | Gauge | By | — (基数 1) | DB open / flush / compaction apply 后通过 `compaction_pending_bytes()` 现算<br>`stats.compaction_pending_bytes`; `engine/compaction/version.rs` | L0 降序最老超额文件大小之和 + L1+ 超过 target_size 的超额字节; 零 I/O 内存计算 | Compaction 积压水位 / Write stall 前兆预警:<br>`aidb_compaction_pending_bytes` |
| `aidb_logical_read_bytes_total` | Counter | By | — (基数 1) | 点查只加返回 value 的长度, miss 加 0<br>`Statistics::note_logical_read` | 不把 key 长度算进 miss | `rate(aidb_logical_read_bytes_total[1m])` |
| `aidb_block_read_bytes_total` | Counter | By | — (基数 1) | 用户读路径 cache miss 穿透读盘时累加数据块与索引块字节<br>`stats.block_read_bytes` | Compaction 读字节走 `note_compaction_read`, 不进这个计数 | `rate(aidb_block_read_bytes_total[1m])` |
| `aidb_block_seek_count_total` | Counter | 1 | — (基数 1) | `read_block_cached` 入口原子累加, cache 命中与 miss 双路径均计 1<br>`stats.block_seek_count`; `engine/sstable/block_io.rs` | **经典读放大 RA 分子**: data block 触碰次数 (含 BlockCache 命中); 纯用户读口径, Compaction 与写路径内部检查不计入 | 经典读放大 (RA) 分子:<br>`rate(aidb_block_seek_count_total[1m])` |
| `aidb_logical_read_count_total` | Counter | 1 | — (基数 1) | 与 `logical_read_bytes` 同点位原子累加: get 命中/未命中各 1, 迭代每产出 1 条 KV 计 1<br>`stats.logical_read_count`; `engine/db/inner/read.rs`, `db/iterator.rs` | **经典读放大 RA 分母**: 用户逻辑读次数 | 经典读放大 (RA) 分母:<br>`rate(aidb_logical_read_count_total[1m])` |
| `aidb_block_cache_size_bytes` | Gauge | By | — (基数 1) | BlockCache 插入/淘汰时更新原子值<br>`stats.block_cache_size`; `engine/cache/block_cache.rs` | 16 分片 LRU 当前占用 | 缓存内存占用 (MB):<br>`aidb_block_cache_size_bytes / 1024 / 1024` |
| `aidb_block_cache_capacity_bytes` | Gauge | By | — (基数 1) | BlockCache 构造时设置<br>`stats.block_cache_capacity`; `block_cache.rs` | 总容量上限 | 缓存水位 (使用率 %):<br>`aidb_block_cache_size_bytes / aidb_block_cache_capacity_bytes * 100` |
| `aidb_block_cache_hits_total` | Counter | 1 | — (基数 1) | BlockCache 读取命中原子累加<br>`stats.block_cache_hits`; `block_cache.rs` | Compaction 改走 `iter_uncached`, 不冲刷用户缓存 | `rate(aidb_block_cache_hits_total[1m]) / (rate(aidb_block_cache_hits_total[1m]) + rate(aidb_block_cache_misses_total[1m]))` |
| `aidb_block_cache_misses_total` | Counter | 1 | — (基数 1) | BlockCache 读取未命中原子累加<br>`stats.block_cache_misses`; `block_cache.rs` | 同上 (纯正用户读未命中口径, 不受 compaction 与写检查污染) | 读缓存未命中率分母项 (同上) |
| `aidb_bloom_false_positive_total` | Counter | 1 | — (基数 1) | Bloom 假阳性穿透判定原子累加<br>`stats.bloom_false_positive`; `engine/sstable/reader.rs` | Bloom 假阳性 (FP) 次数; 可结合 `aidb_bloom_useful_total` (TN) 精确计算 FPR; 活跃 Range Tombstone 下点查走 `point_state()` 无 bloom 检查 | Bloom 假阳性穿透频次:<br>`rate(aidb_bloom_false_positive_total[1m])` |
| `aidb_bloom_useful_total` | Counter | 1 | — (基数 1) | Bloom 检查返回 false 提前返回时原子累加<br>`stats.bloom_useful`; `engine/sstable/reader.rs` | Bloom 真阴性 (TN) 次数 (成功过滤掉磁盘 I/O); 覆盖 seek 与 value_type 路径 | 精确 Bloom 假阳性率 (FPR):<br>`rate(aidb_bloom_false_positive_total[1m]) / (rate(aidb_bloom_false_positive_total[1m]) + rate(aidb_bloom_useful_total[1m]))` |
| `aidb_write_stall_total` | Counter | 1 | `cause(3: memtable\|l0\|level_size)` · `kind(2: slowdown\|stop)`<br>基数 6 | 发生写停顿时累加<br>`stats.write_stall_requests` | 六种停顿. 不再导出停顿直方图 | `sum by (cause, kind) (rate(aidb_write_stall_total[1m]))` |
| `aidb_write_stall_window_max_us` | Gauge | us | — (基数 1) | 导出时 `take_write_stall_window_max_us` 读出并清零 | 本周期最大值. 没有停顿时是 0 | `aidb_write_stall_window_max_us` |
| `aidb_sequence` | Gauge | {sequence} | — (基数 1) | DB open 与写路径原子设置<br>`stats.sequence`; `engine/db/inner/mod.rs`, `write.rs` | 最新已分配的全局 Sequence | 写入速率交叉验证:<br>`rate(aidb_sequence[1m])` |
| `aidb_recovery_wal_replay_duration_seconds` | Gauge | s | — (基数 1) | WAL 重放与解析耗时原子记录<br>`stats.recovery_wal_replay_duration_us`; `engine/db/inner/mod.rs` | 冷启动一次性事件, 每次导出记当前值一次, 不按中点重放 | `aidb_recovery_wal_replay_duration_seconds` |
| `aidb_recovery_wal_replayed_bytes` | Gauge | By | — (基数 1) | WAL 重放物理文件总大小<br>`stats.recovery_wal_replayed_bytes`; `engine/db/inner/mod.rs` | 冷启动一次性事件, 物理重放文件字节量 (Gauge 规范不带 `_total`); `Statistics::reset()` 保护白名单保留 | 冷启动 WAL 恢复带宽分析:<br>`aidb_recovery_wal_replayed_bytes / aidb_recovery_wal_replay_duration_seconds` |
| `aidb_recovery_manifest_duration_seconds` | Gauge | s | — (基数 1) | Manifest 元数据版本恢复耗时原子记录<br>`stats.recovery_manifest_duration_us`; `engine/db/inner/mod.rs` | 冷启动一次性事件 (微秒转秒); 覆盖 `recover` / `open_new` / `bootstrap_from_scan` 三分支; `Statistics::reset()` 保护白名单保留 | 元数据恢复耗时定位:<br>`aidb_recovery_manifest_duration_seconds` |
| `aidb_recovery_sstable_open_duration_seconds` | Gauge | s | — (基数 1) | 全量 SSTable 打开与校验耗时原子记录<br>`stats.recovery_sstable_open_duration_us`; `engine/db/inner/mod.rs` | 冷启动一次性事件 (微秒转秒); `remove_orphan_sstables` 隔离在此耗时之外; `Statistics::reset()` 保护白名单保留 | SST 元数据加载耗时定位:<br>`aidb_recovery_sstable_open_duration_seconds` |

## 协议与进程指标 (aikv 进程)

由嵌入方 `aikv` 维护并经由同一 OTel 导出通道上报.

| 指标名 | 类型 | 单位 | 标签与基数 | 数据源与代码位置 | 说明 (实现陷阱与口径) | 用途与典型 PromQL |
| --- | --- | --- | --- | --- | --- | --- |
| `aikv_recovery_rebuild_counters_duration_seconds` | Gauge | s | — (基数 1) | `KvStorage::rebuild_counters_duration_us` 固定返回 0<br>`server/config.rs` 的 `refresh_runtime_metrics` 仍写入旧 gauge | 全库重扫已去掉. 值保持 0, 不再表示 16 个逻辑库的扫描耗时 | 不要把它读成恢复耗时. 序列若仍出现, 读数是 0 |
| `aikv_process_threads` | Gauge | 1 | — (基数 1) | 周期读取 `/proc/self/status` 的 `Threads:` 字段<br>`ServerMetrics`; `server/metrics.rs` | 进程当前 OS 线程总数; 零 node-exporter 依赖 | 线程数监控 / 线程泄漏与膨胀预警:<br>`aikv_process_threads` |
| `aikv_process_context_switches_total` | Counter | 1 | `aikv_context_switch_type(2: voluntary\|nonvoluntary)`<br>基数 2 | 周期读取 `/proc/self/status` 上下文切换累计值后差分累加<br>`ServerMetrics`; `server/metrics.rs` | 进程级自愿与非自愿上下文切换次数; 先 add 差分后更新基线 (崩溃安全) | 上下文切换速率 (判断 CPU 争用与阻塞):<br>`sum by (aikv_context_switch_type) (rate(aikv_process_context_switches_total[1m]))` |

## OTel semconv 双写指标

与 `aidb_operations_total` 同数据源同步计数. 操作耗时直方图不再导出.

| 指标名 | 类型 | 单位 | 标签与基数 | 数据源与代码位置 | 说明 (实现陷阱与口径) | 用途与典型 PromQL |
| --- | --- | --- | --- | --- | --- | --- |
| `db.client.operations` | Counter | {operation} | `db_system(1: aidb)` · `db_operation_name` | `stats.operations` 经 `sync_to_otel` 同步计数<br>`src/metrics.rs` | 不含 `stall_stop` 与 `stall_slowdown` | `rate(db_client_operations_total[1m])` |

## 集群指标 (`monitoring` + `cluster`)

全部由 client/server 直接 `fetch_add` 写入共享 `Arc<Statistics>` 实例, 集中由 `src/metrics.rs` 的 `sync_to_otel` 差分同步并导出.

| 指标名 | 类型 | 单位 | 标签与基数 | 数据源与代码位置 | 说明 (实现陷阱与口径) | 用途与典型 PromQL |
| --- | --- | --- | --- | --- | --- | --- |
| `aidb_raft_rpc_total` | Counter | 1 | `aidb_raft_rpc_type(3: vote\|append_entries\|install_snapshot)` · `aidb_raft_direction(2: incoming\|outgoing)`<br>基数 6 | Raft RPC 收发原子累加<br>`stats.raft_rpc`; `cluster/network/client.rs`, `cluster/network/server.rs` | 集群内部 RPC 流量统计 | 集群复制流量观测:<br>`sum by (aidb_raft_rpc_type, aidb_raft_direction) (rate(aidb_raft_rpc_total[1m]))` |
| `aidb_raft_log_entries_total` | Counter | 1 | — (基数 1) | AppendEntries 入站 entry 数原子累加<br>`stats.raft_log_entries`; `cluster/network/server.rs` | **仅入站方向** (`entries.len()`); 出站复制量需乘多数派或看对端入站 | Raft 日志复制吞吐速率:<br>`rate(aidb_raft_log_entries_total[1m])` |
| `aidb_raft_group_fatal_total` | Counter | 1 | `aidb_raft_group_id`<br>基数视分片数而定 | Group 进入 OpenRaft Fatal 状态原子累加<br>`stats.raft_group_fatal`; `cluster/multi_raft_node/lifecycle.rs` | Raft 致命不可恢复异常 | 集群致命错误告警:<br>`rate(aidb_raft_group_fatal_total[1m]) > 0` |
| `aidb_raft_group_restart_total` | Counter | 1 | `aidb_raft_group_id` · `aidb_raft_group_restart_outcome(2: success\|failure)`<br>基数 2 × 分片数 | Group 自愈重启结果原子累加<br>`stats.raft_group_restart`; `lifecycle.rs` | 实际取值仅 success/failure (代码注释中的 `skipped_backoff` 未使用) | 自愈成功率观测:<br>`sum by (aidb_raft_group_restart_outcome) (rate(aidb_raft_group_restart_total[1m]))` |
