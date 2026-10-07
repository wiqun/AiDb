# AiDb 领域词汇

## 存储引擎

**LSM-Tree (Log-Structured Merge-Tree)**: 自研的 LSM 存储引擎, 写路径顺序追加, 读路径多层合并.

**WAL (Write-Ahead Log)**: 预写日志, 所有写入操作先落 WAL 再更新 MemTable, 用于崩溃恢复.

**MemTable**: 内存中的写入缓冲区, 基于 SkipMap 实现, 支持并发读写.

**SSTable (Sorted String Table)**: 磁盘上不可变的排序键值表, 由 MemTable flush 产生, 只读.

**Block**: SSTable 内部分块, 是 I/O 和缓存的基本单位.

**Block Cache**: 最近访问的 Block 的缓存, 减少读放大.

**Bloom Filter**: 布隆过滤器, 快速判断 key 是否可能在 SSTable 中, 减少不必要的 I/O.

**Manifest**: 记录 SSTable 层级归属和状态的元数据文件.

**Leveled Compaction**: 分层合并策略, 从 L0 逐层向下合并, 控制读放大与写放大.

**Tombstone**: 删除标记, 表示一个 key 已被删除; Compaction 时清理过期 tombstone.

**RangeTombstone**: `delete_range` 写入的范围删除标记, O(1) 写入 (不扫描区间); 读取时覆盖区间内全部 key, Compaction 时清理.

## 共识层

**Raft**: 通过 OpenRaft 0.10 实现的分布式共识协议.

**MetaRaft**: 管理集群元数据的 Raft 组, 处理成员变更、slot 分配等控制面操作, 低频.

**MultiRaft**: 多个数据面 Raft 组, 每个 Group 负责一部分 slot 的数据复制与共识, 高吞吐.

**Group**: MultiRaft 中的一个 Raft 组, 有独立的 Raft 日志和状态机.

**Slot**: 16384 个槽位之一, CRC16 计算; 与 Redis Cluster 兼容. 每个 slot 归属于一个 Group.

**LifecycleManager**: 管理 Group 的生命周期 (创建、启动、停止、销毁).

**Slot 迁移**: 将 slot 从一个 Group 迁移到另一个, 在线操作, 不中断读写.

## 网络层

**RPC**: 节点间的 gRPC 通信, 基于 tonic + prost, 定义在 `proto/raft.proto`.

**AppendEntries**: Raft 日志复制的 RPC 请求.

**Vote**: Raft 选举的 RPC 请求.

**InstallSnapshot**: Raft 快照安装的 RPC 请求.

## 写入路径

**WriteBatch**: 一批原子写入操作, 要么全部成功要么全部失败.

**Snapshot (快照)**: 某一时刻的完整数据视图, 用于崩溃恢复和新节点追赶.

**MVCC**: 多版本并发控制, 通过快照实现隔离读取.

**estimate_entry_count**: 已打开 SSTable 的 `num_entries` 与活跃, 不可变 MemTable 的 `entry_count` 之和. 含旧版本, 点删除, 范围删除. 写路径不为这个数做存在性判断. `DBSIZE` 与 INFO `keys_estimate` 使用这个估值, 不随逻辑 DB 改变.

## 数据路径

**put**: 写入 (插入或更新) 一个 key-value; 返回 `Result<()>`.

**key_exists**: 完整存在性判定 (mem→imm→SST), 热路径只读 `ValueType`, **不**物化 Value. 禁止仅用 MemTable `contains_key` 代替. 用户读口径, 记入 BlockCache 和读统计.

**key_exists_without_stats**: 与 `key_exists` 同一判定, 不记入 BlockCache 和读统计. `PutConditional` 未命中批内 overlay 时走它.

**get**: 读取一个 key 的值, 按 MemTable → SSTable 层级顺序查找.

**delete**: 写入一个 tombstone 标记逻辑删除.

**scan**: 范围扫描, 遍历多个连续 key.

## 集群写回传

**Response::Ok**: 数据面 `Put` / `Delete` / `WriteBatch` / `MigrationWrite` 与控制面成功都返回这个变体. apply 不再为 effects 做存在性判断.
