---
name: aidb-design
description: AiDb 核心设计决策与架构权衡 (Why). 解释技术选型理由, 放弃的替代方案 (Trade-offs), YAGNI 刻意不实现, 已知限制与决策总表.
---

# AiDb 设计决策

本文阐述 AiDb 各核心子系统的 **设计决策与架构权衡 (Why)**: 技术选型理由, 放弃的替代方案 (Trade-offs), YAGNI 刻意不实现与已知限制.

## 1. 产品形态与横切取舍

### 为什么是嵌入式 lib, 而非独立网络服务?

AiDb 定位为纯 Rust **嵌入式 lib crate**: 提供纯同步的 `DB` API, 内部不运行网络 listener, 不解析 Redis RESP 协议.
上层分布式 KV 服务 (如 [wiqun/AiKv](https://github.com/wiqun/AiKv)) 负责对外网络暴露, RESP 协议转换与集群运维命令.
通过将存储与共识内核下沉为库, 使得 LSM 存储引擎与 MultiRaft 分布式共识基础设施可高度复用, 且协议层与存储层可独立演进与测试.

### 为什么提供同步 API + Cargo Feature 解耦?

- **纯同步存储 API**: LSM 存储核心路径主要依赖内存操作, 文件 IO 与线程调度, 采用同步 API (`DB::put/get/scan`) 设计极简且锁开销最小; 异步调用方 (如基于 Tokio 的 AiKv) 可通过 `tokio::task::spawn_blocking` 零阻碍桥接.
- **按需 Feature Gate**: 核心 `engine` 模块始终编译且零非必要外部依赖; 全量备份 (`backup`), 分布式共识 (`cluster`), 监控指标 (`monitoring`) 与块压缩 (`compression`) 均通过 Cargo Feature 严格解耦, 避免嵌入方承担不必要的编译体积与外部依赖 (如 tonic/protoc).

### 与 RocksDB / LevelDB: 借鉴什么, 避免什么?

- **借鉴**: LSM 分层存储思想, Leveled Compaction 策略, WAL 物理分片 Record 格式, MemTable 冻结落盘机制, SSTable Block 索引与 Bloom Filter, WriteBatch 原子写及 MVCC 快照读.
- **避免**:
  - 避免 200+ 过于繁复晦涩的配置项 (AiDb 精简为约 25 个核心配置项并提供生产 Preset).
  - 避免 C++ FFI 带来的跨平台编译负担与内存安全黑盒.
  - 避免多 Column Family (CF) 带来的锁竞争与复杂度 (单 Keyspace 极简模型).
  - 避免过度复杂的存储抽象层 (如 Env/FileSystem 的多层包裹).

### 刻意不实现 (YAGNI)

| 刻意不实现的特性 | 核心考量与替代方案 |
| --- | --- |
| **Column Family (多 CF)** | 保持单 Keyspace 极简设计; 专注高性能纯 KV 存储, 上层应用 (AiKv) 对齐 Kvrocks 单库裸 Key 存储模型 |
| **复杂分布式事务 (2PC/XA)** | 专注高性能 KV 原语; 单机提供原子 `WriteBatch` 与 MVCC `Snapshot`, 避免引入分布式事务锁协调器 |
| **ThinReplication (瘦复制)** | 当前采用完整 Raft Log 复制以保证确定性与数据自愈一致性, 暂不引入旁路解耦的瘦复制模型 |
| **内置 HTTP / Prometheus 服务** | 库不自建 HTTP listener; 指标注册为 `aidb_*`, 由宿主进程配置全局 `MeterProvider` 并经 OTLP 统一管道导出 |
| **独立 CLI 管理工具 (`aidb-admin`)** | 不维护独立二进制管理工具; 所有管理与运维能力通过公共 Rust API 或 `examples/` 交付 |

### 磁盘格式与向后兼容策略

AiDb 存储格式专注于高效与健壮性, **不兼容** 早期实验性版本的磁盘数据 (WAL Record 分片语义, InternalKey 编码及 Bloom CRC 已演进). 生产环境禁止跨大版本直接挂载旧格式数据目录.

---

## 2. 存储引擎设计决策

### 为什么废除单核互斥锁, 引入 WriteThread Group Commit?

早期版本在写路径采用单一排他互斥锁, 高并发下存在严重的多线程自旋与上下文切换损耗, 且每个线程独占落盘导致 WAL 频繁产生微小写入与 `fsync` 系统调用.
AiDb 借鉴 RocksDB 写入流水线设计, 引入基于无锁链表的 **WriteThread Group Commit** 架构:
1. **Leader 选举**: 首个进入队列的写入线程自动竞选为 Leader, 后续到达的写入线程作为 Followers 在用户态自旋后进入轻量挂起 (park).
2. **批量合并写 WAL**: Leader 一次性提取就绪队列中的所有写请求, 集中分配单调递增的全局 Sequence, 并在 64KB 用户态缓冲区中单次合并追加 WAL, 彻底平摊系统调用开销.
3. **流水线内存更新**: Leader 在内存中顺序将所有请求写入 MemTable, 消除 Followers 争抢 MemTable 的锁碰撞.
4. **批量原子唤醒**: Leader 持锁短暂推进 `committed_sequence` 并按需 `sync_wal`, 随后一次性唤醒全体 Followers 返回客户端.
*收益*: 写入吞吐量随并发线程数提升近乎线性扩展, 写入长尾延迟显著下降.

### 为什么引入 32 分片 RowCache 与 PinnableSlice?

传统 LSM 存储引擎的点查必须依次遍历 Active MemTable, Immutable MemTables, L0 SSTable 与 L1+ SSTable, 即使开启 BlockCache 也只能缓存已解压缩的 Data Block, 仍需经历二分定位 Restart Point, Block 内键比对与 Value 拷贝开销.
AiDb 引入 **RowCache (32 分片并发 LRU)** 与 **PinnableSlice 零拷贝机制**:
- **32 分片并发 LRU**: 按 UserKey Hash 分片, 细化锁粒度. 点查优先检索 RowCache, 命中后直接返回 `PinnableSlice::Shared(Bytes)`, 实现纳秒级, 零内存拷贝直出.
- **读写一致性联动**: 写入路径 (Put / Delete / DeleteRange / WriteBatch) 与后台 Compaction 合并提交时, 精确计算受影响分片并执行 `invalidate`, 确保缓存数据严格一致.
- **未命中回填**: 点查穿透至 LSM-Tree 成功获取最新数据后, 异步回填至对应分片, 自动建立热点数据行级缓存.

### 为什么提供纯内存跳表原生有界正反向扫描 (`scan_limited`)?

当数据库处于纯内存状态 (`is_pure_mem`, 即尚未触发 Flush 且无 L0 SSTable) 时, 传统范围扫描仍然会走通用 `DBIterator` 复杂的多路归并迭代, 多级堆优先队列构建与虚函数分发, 带来不必要的堆内存分配与性能开销.
AiDb 针对该场景引入 `scan_limited` 快速通道:
- 检测到 `is_pure_mem` 时, 直接绕过 `DBIterator`, 调用 `SkipMapRep` 底层的原生 SkipMap 迭代器 (`scan_memtable_limited_forward` / `scan_memtable_limited_backward`).
- 原生支持正向与逆向扫描, 并在满足 `limit` 数量约束后立即短路返回, 极大降低了轻量嵌入式与只读前缀扫描的延迟.

### 为什么选择 LSM-Tree 而非 B-Tree?

在写密集型的现代存储场景中, LSM-Tree 将随机写入转换为顺序追加写 (WAL + MemTable), 写入延迟极低且对 SSD 磨损最小; 后台通过异步 Leveled Compaction 整理数据并由 Bloom Filter 压制读放大. 相比 B-Tree 随机写导致的大量页分裂与写放大, LSM-Tree 更契合高吞吐写入需求.

### 为什么选择 Leveled Compaction 而非 Tiered/Universal?

- **读放大严格可控**: Leveled Compaction 保证 L1~Ln 各层内部 Key 绝不重叠, 单次点查在每层至多检索一个 SSTable, 读放大为严格的 $O(\log N)$ 级别.
- **空间放大更低**: 相比 Tiered (Size-Tiered) 动辄需要预留 100% 磁盘冗余空间, Leveled Compaction 的空间放大通常控制在 10%~20% 之间.
- **单一策略专注维护**: 放弃维护多种复杂 Compaction 策略, 集中精力将 Leveled Compaction 的选层评分, Key 范围 Claim 锁与 Subcompaction 并行优化做到极致.

### WAL 格式: 为什么使用 32KB Record 分片?

WAL 采用标准的 32KB Block 物理结构, 当单个 `WriteBatch` 跨越 Block 边界或 Value 较大时, 自动切分为 `First` / `Middle` / `Last` 分片 Record 并独立计算 CRC32. 结合逻辑层 `WalEntry::BatchStart`, 确保在进程崩溃或断电重放时能够原子重组完整批次, 彻底杜绝半写损坏.

### MemTable: 为什么选择 Crossbeam SkipMap?

基于 `crossbeam-skiplist` 实现并发无锁 SkipMap, 支持读写并发操作; 当内存达到阈值时以 $O(1)$ 代价切换为不可变 Immutable MemTable, 供后台 Flush 线程无锁读取并落盘, 避免了 `RwLock<BTreeMap>` 在高并发写入下的严重锁竞争.

### SSTable Block: Restart Points 前缀压缩

Block 内部默认每 16 个 Key 设置一个 Restart Point 存储完整 Key, 其余 Key 仅存储与前一个 Key 的前缀重叠长度与差量后缀. 这种设计在显著提升数据压缩率的同时, 允许块内检索通过二分快速定位 Restart Point, 兼顾空间与 CPU 局部性.

### Block Cache: 16 分片 LRU 缓存

为解决单全局互斥锁在多线程高并发点查下的锁瓶颈, BlockCache 内部按 Key 的 Hash 划分为 16 个独立的 LRU 分片 (Shard).
*Trade-off*: 单分片独立淘汰, 当局部访问极度偏斜时可能优先淘汰该分片数据而不向其他分片借用配额, 但换取了近乎线性的并发读取扩展能力.

### Write Stall: 为什么实行阶梯式写背压?

当后台 Flush 或 Compaction 滞后导致 L0 SSTable 文件数堆积时, 读放大将呈线性恶化. AiDb 引入阶梯式 Write Stall 机制: 超过慢速阈值时主动对写入线程进行微秒级延迟 (Sleep), 达到硬上限时暂停写入等待后台清理完成, 彻底避免 L0 无界膨胀引发系统级 OOM 或读崩溃.

### Subcompaction: 大任务并行切分

当单次 Compaction 的输入数据量超过 `subcompaction_min_size` (默认 64MB) 且系统具备多核计算能力时, 通过 `std::thread::scope` 将无重叠的 Key 区间分裂为多个子任务并行执行, 大幅缩短大合并的墙钟耗时.

### MVCC 语义与可见性权衡

- **点查 (`get`)**: 基于获取操作时的全局 SequenceNumber 查询, 保证一致性点查.
- **范围扫描 (`scan`)**: 默认支持快照级可见性或最大序列扫描.
- **批量写入可见性**: 并发 `get` 在 `WriteBatch` 写入 MemTable 的瞬间可能观察到部分更新 (与 LevelDB/RocksDB 一致); 若业务要求绝对隔离性, 需在排他写锁保护下创建一致性 `Snapshot`.

---

## 3. 分布式共识与集群设计决策 (Feature `cluster`)

### 为什么选择 MultiRaft 而非单 Raft / P2P / Paxos?

| 方案 | 一致性 | 水平扩展能力 | 系统复杂度 | 选型结论 |
| --- | --- | --- | --- | --- |
| **单 Raft** | 强一致 | 差 (单 Leader 写入与网络瓶颈) | 低 | 无法满足大规模集群扩展需求 |
| **MultiRaft** | **强一致** | **极高 (多 Group 并发 Leader)** | **中** | **选用: 兼顾强一致与吞吐** |
| **无共识 P2P (Gossip)** | 最终一致 | 高 | 低 | 不满足底层存储强一致性底线 |
| **Paxos** | 强一致 | 中 | 高 | Rust 生态缺乏成熟生产级实现 |

### 为什么选用 OpenRaft 0.10?

OpenRaft 是纯 Rust 生态成熟的 Raft 规范实现, 原生支持 Joint Consensus 安全成员变更, 内置快照传输流与高效的状态机解耦, 深度契合 Tokio 异步网络模型.

### 控制面 (MetaRaft) 与数据面 (MultiRaft) 彻底分离

- **MetaRaft (`group_id = 0`)**: 专门负责集群拓扑管理, 节点加入/退出, 16384 槽位分配表与在线迁移状态机, 操作低频且要求强确定性.
- **MultiRaft (`group_id ≥ 1`)**: 各 Group 独立运行 Raft 日志与状态机, 挂载独立的数据目录 (`data/group_{gid}/`), 负责高吞吐的数据 Propose 与持久化.
- **收益**: 控制面拓扑变更与数据面海量写入在物理上完全隔离, 避免高并发写请求阻塞集群元数据共识.

### 多 Group 共享物理节点

允许 3 节点集群承载 256 个甚至更多 Raft Group, 各 Group 的 Leader 均匀分布在各个物理节点上, 最大化 CPU, 内存与磁盘 IO 的利用率; 单节点宕机仅触发受影响 Group 的重新选主, 集群整体服务不中断.

### 状态机 Key 二进制前缀隔离

- 用户数据键: `\x01sm/{gid}/{user_key}`
- Raft 元数据键: `\x00raft/{gid}/{meta_key}`
通过单字节前缀实现命名空间绝对隔离, 方便状态机扫描, 快照导出与调试定位.

### 为什么选择 gRPC (tonic + prost)?

采用标准 Protobuf 定义 (`proto/raft.proto`), 具备严谨的接口契约, 跨语言生态工具与 HTTP/2 多路复用能力, 相比自研私有二进制协议更易维护与调试.

### 为什么槽位固定为 16384?

完全对齐 Redis Cluster 的槽位拓扑模型 (支持 `{...}` Hash Tag 提取与 CRC16 计算), 使上层服务无需二次转换即可平滑支持 Redis 官方客户端与集群拓扑协议.

---

## 4. 备份与容灾设计决策 (Feature `backup`)

### 为什么基于 Checkpoint 快照?

SSTable 一经生成即为只读不可变文件. `Checkpoint` 在强制 Flush 内存数据后 Pin 住当前活跃的 SSTable 集合, 并通过文件系统硬链接 (Hardlink) 实现近乎零耗时的快照生成, 且在生成过程中协调 Compaction 锁以确保物理一致性.

### 为什么采用临时目录 + 原子 Rename 恢复?

数据恢复时, 先在临时目录 (`restore_tmp_*`) 中完成解压与逐文件 SHA256 校验和验证, 执行 `DB::open` 冒烟自检成功后, 再通过原子 `rename` 切换至目标数据目录. 彻底避免因恢复中途崩溃或断电导致原数据目录损坏.

### Trade-off: 专注单机全量备份

当前内置实现专注单机物理全量备份 (`LocalFileStorage`), 保证单机恢复的绝对可靠与自包含. 增量备份, 跨地域 S3 上传与集群全局备份协调交由上层运维体系调度.

---

## 5. 可观测性设计决策 (Feature `monitoring`)

### 为什么使用 tracing 而非 log crate?

`tracing` 提供结构化 Span 上下文. 只允许 `db_flush`, `cmp_run`, `raft_propose`, 且采样关闭时不创建.

### 库内指标注册而非自建 HTTP Exporter

AiDb 内部仅通过 OpenTelemetry 注册标准 `aidb_*` 指标, 不自建 HTTP Server 或 Prometheus scrape 端口. 遵循「库产出指标, 宿主应用统一导出」的职责分离原则, 由宿主应用配置全局 `MeterProvider` 并通过 OTLP 统一管道输出.

---

## 6. 全景决策总表 (Decision Matrix)

| 决策领域 | 选型方案 | 核心理由 | 放弃方案 / 约束限制 |
| --- | --- | --- | --- |
| **产品形态** | 嵌入式 lib crate | LSM 内核复用, 协议独立演进 | 不自建独立网络服务 |
| **API 范式** | 同步 API + spawn_blocking | 锁模型简单, 零异步开销 | 库层纯异步 API |
| **写入流水线** | WriteThread Group Commit | 消除多线程互斥锁自旋, 64KB 缓冲合并写 WAL | 单全局前台互斥写锁 |
| **存储模型** | LSM-Tree (Leveled) | 顺序写入高效, 读放大可控 | B-Tree 随机写 / Tiered 空间放大 |
| **写缓冲** | Crossbeam SkipMap | 无锁高并发并发读写 | `RwLock<BTreeMap>` 锁竞争 |
| **持久化日志** | 32KB 分片 WAL Record | 支持大 Value, 崩溃原子性 | 裸二进制追加无校验 |
| **行级缓存** | 32-shard LRU RowCache + PinnableSlice | 纳秒级零拷贝直出, 写与合并精确失效联动 | 每次点查全量穿透 SSTable 块查找 |
| **块缓存** | 16-shard LRU Cache | 降低多线程并发锁竞争 | 单全局互斥锁 Cache |
| **内存扫描优化** | 原生跳表有界扫描 (`scan_limited`) | `is_pure_mem` 绕过归并堆分配, 毫秒级直出 | 通用 DBIterator 多路归并构建 |
| **背压控制** | 阶梯式 Write Stall | 防止 L0 文件无限堆积 | 无界追加导致读雪崩 |
| **大合并优化** | Subcompaction 多线程 | 显著缩短长尾合并耗时 | 单线程大合并 |
| **分布式共识** | OpenRaft 0.10 (MultiRaft) | 强一致性, 多 Leader 并行扩展 | 单 Raft 瓶颈 / P2P 弱一致 |
| **状态机 Apply** | `db.write_without_wal` | 消除 Raft Log 之后的第 2 个 WAL 写盘 | 状态机二次写引擎 WAL |
| **集群架构** | MetaRaft / MultiRaft 双层分离 | 元数据与数据读写物理解耦 | 单全局 Raft 队列争用 |
| **节点间通信** | gRPC (tonic + prost) | HTTP/2 多路复用, 类型安全 | 自定义私有二进制协议 |
| **分片拓扑** | 16384 固定槽位 (CRC16) | 兼容 Redis 槽模型与 Hash Tag | 动态分片分裂合并 |
| **全量备份** | Checkpoint 硬链接 + SHA256 | 零拷贝秒级快照, 一致性自检 | 在线物理热拷贝无锁 |
| **可观测性** | `db_flush`, `cmp_run`, `raft_propose` 与宿主 OTel 指标 | 采样关闭时不创建 span, 宿主统一 OTLP 导出 | 热路径 debug span, 库内自建 HTTP scrape 端口 |
