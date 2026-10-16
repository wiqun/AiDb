# AiDb 测试布局

## 分层

| 层级 | Cargo 入口 | 源码 | 说明 |
|------|------------|------|------|
| **L0** | `cargo test --lib` | `src/**` `#[cfg(test)]` | 单元测试; console 一级 **单元测试** (`src/**` 路径映射) |
| **L1** | `tests/{wal,memtable,filter,cache,sstable,db,compaction,snapshot,raft}.rs` | `tests/modules/{mod}/` | 单模块功能 + **模块级** tracing (`dataflow.rs`); 另有 `meta` / `multi_raft` / `metrics` / `cluster_ops` / `span_contract` 等入口, 以 `tests/*.rs` 与 `Cargo.toml` `[[test]]` 为准 |
| **L2** | `tests/pipeline.rs`, `tests/engine.rs` | `tests/pipeline/`, `tests/engine/` | 跨模块 / 引擎黑盒 |
| **L3** | `tests/proptest.rs` | `tests/proptest/` | 随机操作 + 引擎不变式 |
| **L4** | `tests/regression.rs` | `tests/regression/` | 已修 bug 固化 |

L5–L7 (协议兼容 / E2E / bench) 在 aikv 或 `benches/`.

## 测试写法与范围 (硬性)

对新测 / 改测强制执行. 旧测不要求本次回填. 细粒度「每个 API 必测几条」不在本文范围.
测试分类由**路径虚拟映射**生成; 分类不靠 `@suite`.

### 写法

| 位置 | 要求 |
|------|------|
| `tests/` 下新建/改动的集成测文件 | 文件顶 `//! @component aidb-{domain}` + 中文摘要段 (第一段非空) |
| L0 `src/**` 内新建 `#[test]` | 中文 `///`; 模块可用 `//!`; **不要求** `@component` |
| 每个新增/改动的 `#[test]` | 正上方中文 `///`; **禁止**用 `//` 顶替 |
| bug 回归 | `///` 含现象、期望、Issue 编号 (若有) |

- 命名: 描述性 `test_*`
- `#[ignore]`: 必须 `slow:` / `stress:` 前缀; 禁止裸 `#[ignore]`
- 除 `@component` 外不加自定义标签 (`@suite` / `@layer` 等)

### 跨仓边界

| 测什么 | 放哪 |
|--------|------|
| LSM / WAL / MemTable / SSTable / compaction / snapshot / Raft 引擎 | **本仓 aidb** |
| RESP / 命令 / TCP / 集群对外协议 | **aikv** |
| 多进程 / redis-cli / failover | **aikv e2e** |

不确定: 能 `DB::*` / 引擎内复现 → aidb; 需要命令或 TCP → aikv.

### 落点

| 场景 | 落点 |
|------|------|
| 单模块 | L1 `tests/modules/{mod}/` |
| 子系统直连 (不经 `DB::open`) | L2 `pipeline/` |
| `DB` 黑盒 / 崩溃恢复 / compaction 集成 | L2 `engine/` |
| 随机不变式 | L3 `proptest/` |
| 已修 bug 长期固化 | **优先** L4 `regression/` |
| Raft/cluster | `tests/modules/cluster/` 等 (经 raft/meta/… 入口); `--features cluster` |

禁止新建根目录散落 `*_integration.rs`. 集成测推荐 `--test-threads=1`.

## L2 按场景类型

| 子目录 | 入口 | 含义 |
|--------|------|------|
| `pipeline/` | `--test pipeline` | 子系统直连 (如 WAL → MemTable), 不经 `DB::open` |
| `engine/` | `--test engine` | `DB` 公共 API: 全链路、崩溃恢复、compaction 集成、**dataflow** 等 |

新增 L2:

- 能用 `DB::*` → `engine/` 新文件
- 只连部分子系统 → `pipeline/` 新文件
- L3/L4 新增场景 → `proptest/` / `regression/` 新文件, 外层 `#[path]` 挂载
- 勿新建根目录 `*_integration.rs`; 保持少量入口 + 子目录
- **Phase 12 (cluster feature)**: `tests/raft.rs` → `tests/modules/cluster/` (storage/network/node/integration + harness)

`modules/{mod}/dataflow.rs` 是 L1 模块 tracing, 与 L2 `pipeline/` 不同义.

## 常用命令

```bash
cargo test --test wal -- --test-threads=1
cargo test --test memtable -- --test-threads=1
cargo test --test filter -- --test-threads=1
cargo test --test cache -- --test-threads=1
cargo test --test sstable -- --test-threads=1
cargo test --test db -- --test-threads=1
cargo test --test db dataflow -- --test-threads=1
cargo test --test engine dataflow -- --test-threads=1
cargo test --test compaction -- --test-threads=1
cargo test --test snapshot -- --test-threads=1
cargo test --test span_contract -- --test-threads=1  # 只允许 db_flush, cmp_run, raft_propose
cargo test --test pipeline -- --test-threads=1
cargo test --test engine -- --test-threads=1
cargo test --test engine compaction -- --test-threads=1
PROPTEST_CASES=100 cargo test --test proptest -- --test-threads=1
cargo test --test regression -- --test-threads=1

# Phase7.6 bench (criterion 参数已内置, 默认 10K preload)
cargo bench --bench write_bench
cargo bench --bench read_bench
# 更大 read 数据集 (可选)
AIDB_BENCH_PRELOAD=100000 cargo bench --bench read_bench

# Phase 12 Raft (需 cluster feature; 集成测必须单线程)
cargo test --features cluster --test raft -- --test-threads=1
cargo test --features cluster raft_storage -- --test-threads=1
cargo test --features cluster raft_3nodes -- --test-threads=1
```

## 回归测 (L4, 必带)

bugfix **必带** 回归测; 详见 [CONTRIBUTING.md §回归测试](../CONTRIBUTING.md#回归测试-bugfix-必带).

| 场景 | 落点 |
|------|------|
| 单模块 | L1 `tests/modules/{mod}/` |
| DB 级崩溃恢复 | L2 `tests/engine/` |
| 已修 bug 固化 | L4 `tests/regression/` + `regression.rs` 挂载 |

## 慢测与压测 (`#[ignore]`)

前缀: `slow:` / `stress:`. 详见 [CONTRIBUTING.md §慢测与压测](../CONTRIBUTING.md#慢测与压测-ignore).

| 测试 | 标签 | test target | CI job |
|------|------|-------------|--------|
| `test_snapshot_long_hold_heavy_write` | slow | `snapshot` | `test-slow` |
| `test_large_dataset_compaction_stress_10000` | stress | `engine` | `test-slow` |
| `test_bloom_stress` | stress | `regression` | `test-slow` |
| `test_concurrent_write_and_compaction` | stress | `stress` | `test-slow` |
| `test_concurrent_write_with_filter` | stress | `stress` | `test-slow` |

本地: `cargo test -- --ignored --test-threads=1`
