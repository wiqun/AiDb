---
name: aidb-engine-storage
description: AiDb 持久化层 — SSTable 布局、层级 compaction、布隆过滤器、块缓存、目录 checkpoint. 改 src/engine/{sstable,compaction,filter,cache,checkpoint}、排查 flush/compaction/读放大、MANIFEST/VersionSet 或 Checkpoint::create 时读本文.
---

# AiDb Engine Storage (持久化层)

## 何时读本文

- 改 `engine/sstable`, `compaction`, `filter`, `cache`, `checkpoint`
- 排查 flush→SST、compaction 调度/卡住、get 读放大、Bloom/BlockCache、MANIFEST、目录 Checkpoint
- **不覆盖**: WAL / MemTable / 写路径 / `DB::put` → [engine.md](01-engine.md)
- **衔接**: 全量备份与容灾管理 → [backup.md](04-backup.md)

## 代码地图

| 路径 | 职责 | 入口 |
| --- | --- | --- |
| `engine/sstable/mod.rs` | SSTable 模块根; re-export | — |
| `engine/sstable/block.rs` | Data Block: 前缀压缩 (prefix compression) + 重启点 (restart points) | `BlockBuilder::add`, `Block::iter` |
| `engine/sstable/block_io.rs` | Block trailer (压缩类型 + CRC); cache 读写 | `write_block`, `read_block_cached` |
| `engine/sstable/builder.rs` | InternalKey 有序写盘; `.sst.tmp` → rename | `SSTableBuilder::add`, `finish` |
| `engine/sstable/reader.rs` | Footer → Index → Bloom → Block 点查 | `SSTableReader::open`, `get`, `value_type` |
| `engine/sstable/iterator.rs` | 单文件顺序迭代 | `SSTableIterator` |
| `engine/sstable/index.rs` | Block 最大 InternalKey → `BlockHandle` | `find_block_handle` |
| `engine/sstable/handle.rs` | Block offset + size 编码 (Index/Meta Index 引用) | `BlockHandle::encode/decode` |
| `engine/sstable/footer.rs` | 48B Footer + 8B MAGIC | `Footer::encode/decode` |
| `engine/sstable/filename.rs` | `{num:06}_L{level}.sst` | `sstable_path`, `parse_sstable_filename` |
| `engine/sstable/meta.rs` | Bloom meta 裸块 | `BLOOM_META_NAME`, `write_raw_block` |
| `engine/sstable/properties.rs` | SST 统计属性 (entries / raw key/value size, 24B) | `SstProperties::encode/decode` |
| `engine/compaction/mod.rs` | Compaction 模块根; re-export | — |
| `engine/compaction/version.rs` | `CURRENT` + `MANIFEST-*`; recover/bootstrap | `VersionSet::recover`, `apply_edit` |
| `engine/compaction/picker.rs` | L0/Ln 选层评分; trivial move 判定 | `CompactionPicker::pick_compaction` |
| `engine/compaction/job.rs` | 归并 dedup; subcompaction 多线程分裂 | `CompactionJob::run` |
| `engine/compaction/merge.rs` | 多 SST 堆归并 (compaction 专用) | `MergeIterator` |
| `engine/compaction/helpers.rs` | key range 重叠判定; user_key 提取 | `key_ranges_overlap_by_meta_raw`, `user_key_from_internal` |
| `engine/compaction/filter.rs` | `CompactionFilter` (纯决策) + `CompactionRemovalListener` (最新 Put 被 Remove 时回调) | `FilterDecision`, `CompactionFilter`, `CompactionRemovalListener` |
| `engine/filter/mod.rs` | Filter 模块根; re-export | — |
| `engine/filter/bloom.rs` | SST 级 Bloom Filter (user_key 散列) | `BloomFilter`, `Filter` |
| `engine/cache/mod.rs` | Cache 模块根; re-export | — |
| `engine/cache/block_cache.rs` | 16 分片 LRU Data Block cache | `BlockCache::get/insert` |
| `engine/checkpoint/mod.rs` | 目录一致性硬链接快照 | `Checkpoint::create`, `verify_openable` |

**DB 协调入口** (`engine/db/inner/`):
- `flush.rs::flush_memtable_to_sstable`: MemTable → L0 SSTable (`SSTableBuilder` / `VersionEdit::AddFile`)
- `read.rs::get_from_sstables`: L0 新→旧 + L1+ 范围定位点查
- `compaction.rs::run_compaction_once`: pick → claim → trivial/subcompaction → apply
- `mod.rs::enter_checkpoint` / `leave_checkpoint`: Checkpoint 互斥加锁 + SST pin

公共 re-export (`src/lib.rs`): `BlockCache`, `CacheStats`, `Checkpoint`.

## 关键 invariant (勿破坏)

- **SST 键序**: 文件内 InternalKey **严格递增**; 非排序 key → `Error::InvalidArgument`; 空 SST `finish` 拒绝; restart point 存完整 key (`shared=0`).
- **L0 overlap**: L0 允许多文件 overlap; 新 flush 的文件插入在 `sstables[0]` **头部** (读时新→旧全扫). **L1+** 同层目标文件 range 绝不重叠.
- **L1+ 定位**: `find_sstable_for_key` 用 **user_key range**, 线性扫描, 不按 `smallest_key` 二分. picker 用 meta raw range 扩展 overlap.
- **范围删除覆盖**: `get` 与 `scan` 用打开时装入的范围删除索引调用 `max_covering_range_tombstone_seq`. 索引都空时点查不收集索引. L1+ 点数据仍用 `find_sstable_for_key`, 范围覆盖看每一层每一个文件.
- **Bloom Filter**: 仅索引 user_key; block 内仍按 `seq <= max_seq` 过滤. miss → 免 I/O; decode 失败 → open **降级**为无 filter (记录 warn 日志, 不崩溃).
- **Block CRC 与 Handle 长度**: 校验 **解压后** payload; trailer = `[compressed_data][type:1][crc:4]`. `BlockHandle.size` 必须记录压缩后 payload 的实际长度 (不含 5B trailer). MANIFEST CRC 失败 → `Error::Corruption`.
- **Compaction dedup 与删除标记**: `TypePut` 按 `SnapshotDedupTracker` 保留最新版本和 `seq <= min_snapshot_sequence` 的边界穿越版本, 更老版本丢弃. 点删除和范围删除在没有快照, 且没有幸存文件与标记范围重叠时不写入输出. 只要还有快照, 标记先留下. 幸存文件从源层里这次没重写的文件算起. 点删除的范围是那一个 user key. 范围删除的范围是 `[start, end)`.
- **CompactionRemovalListener**: 仅在 Version 安装成功后回调 `filter_removed_user_keys`; 调用方须持 `Weak<DB>` (禁止 `Arc<DB>` 回指造成泄漏). `DB::close` 在 join compaction 后将 filter/listener 置 `None`.
- **Trivial move**: rename 后若 reader open 失败, 文件自动 rename 回原位置.
- **Checkpoint 互斥**: `checkpoint_in_progress` 阻止 `run_compaction_once`; `pin_sstables` 防止 unlink.
- **MergeIterator 隔离**: compaction 使用 `compaction::MergeIterator`; 读路径使用 `db/iterator::DBIterator`.

## 数据流

### Flush (MemTable → L0)

```mermaid
flowchart LR
    IM[immutable MemTable] --> B[SSTableBuilder]
    B --> TMP[.sst.tmp]
    TMP --> R[rename .sst]
    R --> VS[VersionEdit AddFile L0]
    VS --> M[MANIFEST append]
    M --> SIG[maybe_trigger_compaction]
```

### 点查 (get → SST)

```mermaid
flowchart TD
    G[get_at_sequence] --> L0[L0 逐文件 新→旧]
    L0 --> LN[L1+ user_key 范围二分定位]
    LN --> BF{Bloom Filter?}
    BF -->|miss| X[None / 快速跳过]
    BF -->|hit/none| IDX[Index Block → BlockHandle]
    IDX --> CD{BlockCache?}
    CD -->|hit| SCAN[Block 内 seq ≤ max_seq 查找]
    CD -->|miss| DISK[read_block_from_file + CRC 校验]
    DISK --> SCAN
```

### Compaction

```mermaid
flowchart TD
    CP{checkpoint 进行中?}
    CP -->|yes| RET[return false]
    CP -->|no| P[CompactionPicker 评分选层]
    P -->|none| RET2[return false]
    P -->|task| CLAIM[try_claim_files 锁 key 范围]
    CLAIM -->|collision| RET3[return true 重试]
    CLAIM -->|ok| TM{trivial move?}
    TM -->|yes| RN[rename 提升层级 + VersionEdit]
    TM -->|no| SUB[subcompaction 范围划分]
    SUB --> J[CompactionJob::run 多线程合并]
    J --> MI[MergeIterator 多路归并 dedup]
    MI --> NB[SSTableBuilder 生成新 SST]
    NB --> VS2[AddFile + DeleteFile]
    VS2 --> RM[remove 旧 .sst]
    VS2 --> WALGC[try_cleanup_wals]
```

### Checkpoint (目录一致性快照)

```mermaid
sequenceDiagram
    participant C as Checkpoint
    participant D as DB
    C->>D: flush()
    C->>D: enter_checkpoint()
    C->>D: pin_sstables()
    Note over C: 收集 CURRENT + MANIFEST + WAL + SST
    Note over C: hard_link (跨设备 fallback copy) → tmp → rename dest
    C->>D: leave_checkpoint()
```

## 关键类型与 API

### SSTable 文件物理布局 (自上而下)

```
┌──────────────────────────┐
│ Data Blocks              │  prefix compression + restart points
├──────────────────────────┤
│ Meta Block (Bloom)       │  optional; 裸字节 (无 5B trailer)
├──────────────────────────┤
│ Meta Index Block         │  "bloom" → BlockHandle
├──────────────────────────┤
│ Index Block              │  max_key → BlockHandle
├──────────────────────────┤
│ Footer (48B)             │  meta_index + index handles + magic
└──────────────────────────┘
```

每个 Data/Index block 磁盘布局为 `[payload][type:1][crc:4]` (5B trailer). `payload` 启用压缩时为**压缩后**字节 (Index/Meta Index block 固定不压缩); CRC32 覆盖**解压后**明文.

- 命名规范: `{file_number:06}_L{level}.sst`
- 空 SST: `finish` 拒绝; flush 路径 `count==0` 时 `abandon`.

### VersionSet / MANIFEST

- `CURRENT` 纯文本指针文件, 记录当前活跃的 `MANIFEST-NNNNNN`.
- `VersionEdit`: 记录 `AddFile` / `DeleteFile` 版本增量 (`crc32 + u32 len + postcard(VersionEdit)` + fsync).
- 超出 `max_manifest_size` 时自动触发 `rotate_manifest`.

### Checkpoint API

```rust
// 创建目录级硬链接一致性快照
Checkpoint::create(&db, dest_path)?;

// 冒烟校验快照目录是否可正常打开
Checkpoint::verify_openable(dest_path, Options::default())?;
```

## 常见任务

### 排查 compaction 不前进

1. 检查 L0 文件数 vs `level0_compaction_trigger`.
2. 确认 `background_compaction=true` (单测中可手动调用 `db.drain_compactions()`).
3. 查验是否被 `checkpoint_in_progress` 阻塞或遭遇 `try_claim_files` 范围锁冲突.
4. 设置 `RUST_LOG=aidb=debug` 查看 `cmp_pick`, `cmp_run`, `cmp_apply` 相关 span.

### 排查 get 慢 / 读放大

1. L0 文件过多: 调低 `level0_compaction_trigger` 或等待 compaction 追赶.
2. Bloom 关闭: 检查 `bloom_false_positive_rate` (0.0 表示禁用).
3. BlockCache 未生效: 检查 `block_cache_size` (0 表示禁用).
4. 开启 `--features monitoring` 监控 `aidb_block_cache_hits_total` / `misses_total` 与 `aidb_bloom_false_positive_total`.

### 调优层级容量

`target_size(Ln) = max_bytes_for_level_base × mult^(n-1)` (L1 起算). 默认 base=256MB, mult=10 → L1=256MB, L2≈2.5GB, L3≈25GB. 修改配置仅影响后续 pick 策略, 不影响已有磁盘文件.

## 配置与 feature flags

### Options (engine-storage 相关, 生产默认)

| 项 | 默认 (生产) | 说明 |
| --- | --- | --- |
| `block_size` | 4 KiB | Data Block 切分大小 (字节) |
| `block_restart_interval` | 16 | 前缀压缩 restart point 间隔 |
| `compression` | `Snap` (有 feature) / `None` (无 feature) | Snap/LZ4 块级压缩; 默认构建无 feature 时为 `None` |
| `bloom_false_positive_rate` | 0.01 | Bloom Filter 目标假阳性率 (0.0=禁用) |
| `block_cache_size` | 64 MiB | 16 分片 LRU 缓存容量 (0=禁用) |
| `level0_compaction_trigger` | 4 | L0 文件数达到该阈值触发 Compaction |
| `level0_slowdown_writes_trigger` | 8 | L0 文件数达到该阈值触发写 sleep 降速 |
| `level0_stop_writes_trigger` | 16 | L0 文件数达到该阈值触发写 stall 停写 |
| `max_bytes_for_level_base` | 256 MiB | L1 目标容量上限 |
| `max_bytes_for_level_multiplier` | 10 | 各 Level 容量倍率 |
| `compaction_threads` | 1 | 后台 Compaction 线程数 (建议 1~4) |
| `subcompaction_min_size` | 64 MiB | Subcompaction 多线程分裂阈值 (0=禁用) |
| `max_sub_compactions` / `min_sub_compactions` | 4 / 2 | Subcompaction 最大 / 最小任务分裂数 |
| `compaction_poll_ms` | 500 | 后台 Compaction 线程轮询间隔 (毫秒) |
| `compaction_channel_size` | 64 | Compaction 唤醒通道容量 |
| `max_manifest_size` | 64 MiB | MANIFEST 轮转阈值 |
| `max_levels` | 7 | LSM-Tree 最大层级数 |
| `background_compaction` | true | 是否启动后台 compaction 线程 |

### Feature flags 与环境变量

| Flag / 变量 | 说明 |
| --- | --- |
| `compression` | Cargo feature: 启用 Snap / LZ4 SSTable 块压缩 |
| `monitoring` | Cargo feature: 启用 OTel SST/compaction/cache 统计指标 |
| `AIDB_SKIP_CHECKSUM=1` | 环境变量: 跳过 Block CRC 校验 (仅用于测试/灾难恢复诊断) |

## 测试

```bash
cargo test --test sstable --test compaction --test filter --test cache -- --test-threads=1
cargo test --test engine compaction -- --test-threads=1
cargo test --test regression -- --test-threads=1
cargo test --test db modules::db::checkpoint_consistency -- --test-threads=1
# 压缩特性测试 (Snap / LZ4)
cargo test --features compression --test sstable -- --test-threads=1
```

| 测试集 | 覆盖 |
| --- | --- |
| `tests/sstable.rs` | Data Block 编码、前缀压缩、Index/Footer 编解码、CRC32 校验、Bloom 与 Cache 集成 |
| `tests/compaction.rs` | 选层评分、VersionEdit / MANIFEST 轮转、MergeIterator 多路归并、Tombstone 清理 |
| `tests/filter.rs` | Bloom Filter 编解码与假阳性率校验 |
| `tests/cache.rs` | 16 分片 LRU BlockCache 淘汰机制与命中率统计 |
| `tests/regression.rs` | Bloom 统计回归与空值 Compaction 回归测试 |

## 已知限制

- 与 **aidb-oldmain** 历史磁盘格式不兼容.
- Index Block 与 Meta Index Block 固定不压缩 (`CompressionType::None`).
- Checkpoint 产生全量目录硬链接副本, 跨文件系统/挂载点失败时 fallback 复制整份数据.
- **BlockCache**: 总容量严格均分 16 shard; 单 shard 满时独立 LRU 淘汰, 不跨 shard 借用配额.
- **Subcompaction**: 条目数常以 `file_size / 100` 估算, 极端大 value 场景可能存在分裂偏斜.
