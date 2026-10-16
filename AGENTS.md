# AiDb — AI 助手操作指南

> **Role & Positioning**: 你是 AiDb 的核心系统开发工程师. AiDb 是纯 Rust 实现的高性能、轻量级**嵌入式 LSM-Tree KV 存储与 MultiRaft 共识库** (lib crate, 无独立网络服务). 上层网络服务 (如 Redis RESP 协议与对外集群命令) 在 **[wiqun/AiKv](https://github.com/wiqun/AiKv)** 实现.

---

## 1. 行为边界与硬约束 (Guardrails & Constraints)

### 🔴 绝对禁止 (Never)
- **禁止网络服务化**: AiDb 不自建 TCP/HTTP listener, 不实现 Redis RESP 协议, 不对外暴露网络端口 (除 Raft 内部 gRPC 节点间通信外).
- **禁止破坏同步 API 契约**: 公共 API (`DB::put/get/scan` 等) 保持**纯同步**; 异步调用方通过 `spawn_blocking` 桥接.
- **禁止允许名单以外的 Span**: 只允许 `db_flush`, `cmp_run`, `raft_propose`. 不得再加 `#[instrument]`. 采样关闭时不组装这些 span 的字段.
- **禁止测试并发污染**: 集成测试与 Raft 测试**严禁多线程并发**, 必须带 `--test-threads=1`.
- **禁止裸 `#[ignore]`**: 耗时或压力测试必须显式标注原因前缀 `#[ignore = "slow: ..."]` 或 `#[ignore = "stress: ..."]`.
- **禁止随意修改底层持久化格式**: WAL Record 格式、SSTable Block 编码、Manifest 版本日志及 `proto/raft.proto` 变更属于高风险操作, 须提前评估兼容性与迁移方案.

### 🟢 必须遵守 (Always)
- **指标前缀规范**: 引擎内部指标统一使用 `aidb_*` 前缀, 经 OpenTelemetry OTLP 统一管道导出, **不进** Redis INFO.
- **测试纪律**: 所有 bug 修复 (`fix:`) 必须在同一 PR 附带可复现回归测试 (入口 [`tests/regression.rs`](tests/regression.rs)), 且测试函数正上方必须有中文 `///` 注释说明 bug 现象、期望与 Issue 编号.
- **测试文件规范**: 新建或修改的集成测试文件顶部必须包含 `//! @component aidb-{domain}` 中文组件标注.
- **文档同步 (强制)**: 修改公共 API、核心行为或模块边界必须同步更新对应 [`docs/modules/`](docs/modules/) 模块文档与根文档; commit 消息修 bug 须带 Issue 引用 (`Fixes #NN`).
- **代码质量门禁**: 提交前必须确保 `cargo fmt --check` 与 `RUSTFLAGS='-D warnings' cargo clippy --all-targets` 零警告通过.

---

## 2. 技术选型与参考基准 (防误猜对照表)

仓库领域术语定义见 [CONTEXT.md](CONTEXT.md).

| 维度 | 本项目选型 | 主流参考 | 核心差异与 AI 行为指引 (Do NOT Guess) |
| --- | --- | --- | --- |
| **LSM 存储引擎** | 自研 WAL + SkipMap + SSTable + Leveled Compaction + BlockCache | RocksDB / mini-lsm | **非 RocksDB API/磁盘格式兼容**; 零 C/C++ FFI 依赖; 严格遵循纯 Rust 安全实现 |
| **分布式共识** | OpenRaft 0.10 (`cluster` feature) | OpenRaft 官方文档 | **不照搬 etcd/raft** 的存储与 API 边界; 网络传输使用 tonic gRPC |
| **MultiRaft 分片** | MetaRaft (控制面) + MultiRaft (数据面) + LifecycleManager | TiKV raftstore | **Slot 固定 16384** (非 TiKV 动态 Region 分裂/merge); 集群拓扑走 MetaRaft 共识 |
| **槽位路由模型** | CRC16 (支持 `{...}` hash tag 提取) 映射到 0..16383 | Redis Cluster Spec | 仅槽位计算算法与 Redis 一致; **MOVED/ASK 重定向与集群运维命令在 AiKv 处理**, 勿在 aidb 臆想 Redis gossip |
| **节点间 RPC** | tonic + prost (`proto/raft.proto`) | gRPC 惯例 | 自定义 Proto 协议 (Vote / AppendEntries / InstallSnapshot), 与 OpenRaft 类型对齐 |

---

## 3. 核心开发与验证命令 (Command-First)

### 本地快速门禁 (推送前必跑)
```bash
export RUSTFLAGS='-D warnings'
cargo fmt --check
cargo clippy --all-targets
cargo test -- --test-threads=1
```

### Feature 专项与全量测试
```bash
# Cluster 特性验证 (需本地 protoc)
cargo clippy --all-targets --features cluster
cargo test --features cluster -- --test-threads=1

# 块压缩特性测试 (Snap / LZ4)
cargo test --features compression --test sstable -- multi_block_read_with -- --test-threads=1

# 慢测与压力测试
cargo test -- --ignored --test-threads=1
```

---

## 4. 任务上下文与模块导航 (Task Routing)

> **文档总览**
- 架构设计详见: [ARCHITECTURE.md](ARCHITECTURE.md);
- 贡献规范见: [CONTRIBUTING.md](CONTRIBUTING.md);
- 完整文档索引见: [docs/README.md](docs/README.md).

修改具体子系统代码时, AI **必须优先查阅**对应的模块文档:

| 开发任务 / 涉及领域 | 优先阅读文档 | 核心关注点 |
| --- | --- | --- |
| **写路径 / WAL / MemTable / 快照** | [docs/modules/01-engine.md](docs/modules/01-engine.md) | `DB` 同步入口、InternalKey 编码、WAL 崩溃重放、Snapshot MVCC |
| **SSTable / Compaction / 缓存 / 过滤** | [docs/modules/02-engine-storage.md](docs/modules/02-engine-storage.md) | Leveled Compaction 评分与 Claim、Bloom 过滤、LRU BlockCache、硬链接快照 |
| **Raft / MultiRaft / 槽位迁移** | [docs/modules/03-cluster.md](docs/modules/03-cluster.md) | MetaRaft 拓扑、MultiRaft Group 生命周期、SlotMigrationManager (F-056-A1 增量追平) |
| **全量备份与恢复** | [docs/modules/04-backup.md](docs/modules/04-backup.md) | `BackupManager`、`RecoveryManager`、逐文件 SHA256 校验和与恢复 |
| **指标注册与链路跟踪** | [docs/modules/05-observability.md](docs/modules/05-observability.md) | `aidb_*` OTel 埋点注册、`db_flush` / `cmp_run` / `raft_propose` span |
