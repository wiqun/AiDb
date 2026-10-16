//! Multi-Raft 数据面编排 — 组合 Router / LifecycleManager / gRPC dispatcher /
//! 每 Group 独立 ShardedStorage, 管理全部数据 Group (gid≥1) 的创建、销毁、
//! 自愈重启与成员对账. 与 `meta_raft_node.rs` 的控制面 (gid=0) 共同构成集群.
//!
//! # 架构
//!
//! ```text
//! lifecycle task (首次立即 tick, 之后默认 1s)

//!   ├─ LifecycleManager::tick -> TickResult (期望拓扑 vs 本地 Group)
//!   ├─ groups_to_create  -> create_group_inner(gid, is_leader, rpc_addr)
//!   │     ├─ ShardedStorage::open -> OpenRaftNode::new (注入 network factory)
//!   │     ├─ is_leader(来自 Meta replicas) -> initialize 单 voter bootstrap
//!   │     └─ dispatcher.register_group + register_node
//!   ├─ groups_to_remove  -> remove_group_inner (shutdown + close + unregister)
//!   ├─ supervise_groups  -> 检测 Fatal -> 指数退避 (2s·2^n, ≤60s) 就地重开
//!   └─ membership drift  -> add_learner_nonblocking + change_membership
//! ```
//!
//! 读写入口: `propose_key` 经 `Router.route_key` 落到目标 group; group 非本地时
//! 经 `remote_leader_client` 转发到该 group 的 leader 节点 (`rpc_addr`).
//!
//! # Invariant
//!
//! - Group ID 约定: `0` = MetaRaft (控制面), 数据 Group ≥ 1 (`DEFAULT_GROUP_ID = 1`).
//! - 自愈重开不传 `init_as_voter`: 该 group 已是集群成员, 只是重载磁盘状态.
//! - 每次 tick 最多处理 1 个 membership drift, 避免批量 joint-consensus 抖动.
//! - Raft 对等通信 / learner 地址一律 `rpc_addr`, 绝不用 `client_addr`
//!   (容器内不可达); MOVED 重定向才用 `client_addr`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use openraft::type_config::async_runtime::WatchReceiver;

use crate::cluster::lifecycle_manager::{LifecycleManager, MetaRaftProvider};
use crate::cluster::meta_types::{default_slot_table, ClusterMeta, SlotMigrationState, SlotTable};
use crate::cluster::network::{RaftNetworkClientFactory, RaftServiceDispatcher};
use crate::cluster::node::OpenRaftNode;
use crate::cluster::router::Router;
use crate::cluster::sharded_storage::ShardedStorage;
use crate::cluster::types::{NodeId, RaftNodeConfig};
use crate::config::Options;
use crate::engine::compaction::{CompactionFilter, CompactionRemovalListener};
use crate::statistics::Statistics;

mod io;
mod lifecycle;
#[cfg(test)]
mod tests;

/// 多 Group Raft 节点管理器
///
/// 组合以下模块:
/// - `Router` — key/slot 路由
/// - `RaftServiceDispatcher` — 统一 gRPC 分发
/// - `ShardedStorage` — 每 Group 独立 DB
/// - `LifecycleManager` — Group 拓扑变化检测
pub struct MultiRaftNode {
    node_id: NodeId,
    groups: Arc<RwLock<HashMap<u64, Arc<OpenRaftNode>>>>,
    router: Arc<Router>,
    storages: Arc<RwLock<HashMap<u64, ShardedStorage>>>,
    grpc_dispatcher: Arc<RaftServiceDispatcher>,
    lifecycle: Arc<LifecycleManager>,
    shutdown_tx: parking_lot::Mutex<Option<watch::Sender<bool>>>,
    server_handle: parking_lot::Mutex<Option<JoinHandle<()>>>,
    /// Override set for group locality checks (used in testing).
    local_group_overrides: Arc<RwLock<HashSet<u64>>>,
    /// Override set for elected-leader checks (used in testing).
    elected_leader_overrides: Arc<RwLock<HashSet<u64>>>,
    /// FIX-0056-A1: 跨节点合并读 (`get_key_from_group_remote`) / tip 远程
    /// fallback (`read_migration_tip`) 用的 gRPC client 工厂. `start_lifecycle_impl`
    /// 启动时会用真实 `rpc_timeout_ms`/`grpc_max_message_size` 替换其内容;
    /// 在此之前保持 `RaftNodeConfig::default()` (此时也不会有本地 group, 无需远程读).
    network_factory: Arc<RwLock<RaftNetworkClientFactory>>,
    stats: Arc<Statistics>,
}

/// 按 group `DB` 构造 `CompactionRemovalListener` (每 group 独立实例).
pub type CompactionRemovalListenerFactory =
    Arc<dyn Fn(Arc<crate::DB>) -> Arc<dyn CompactionRemovalListener> + Send + Sync>;

/// Group 生命周期配置 (用于 start_lifecycle_with_data).
#[derive(Clone)]
pub struct LifecycleConfig {
    pub data_dir: std::path::PathBuf,
    pub raft_node_config: RaftNodeConfig,
    pub options: Options,
    /// 可选的 compaction 过滤器 (如 TTL 过期自动删除).
    pub compaction_filter: Option<Arc<dyn CompactionFilter>>,
    /// 按 group DB 构造 Remove 监听器 (每 group 独立 `Arc<DB>`).
    pub compaction_removal_listener_factory: Option<CompactionRemovalListenerFactory>,
}

/// 单个 group 的自愈重启退避状态.
struct GroupRestartState {
    last_attempt: Option<std::time::Instant>,
    consecutive_failures: u32,
}

/// 计算第 N 次连续失败后, 下一次重启尝试前应等待的退避时长.
///
/// 指数退避 (2s * 2^n), 上限 60s — 既能在偶发单次故障后快速自愈,
/// 也能避免持续性故障 (例如磁盘真的坏了) 时反复重启拖垮节点.
fn group_restart_backoff(consecutive_failures: u32) -> std::time::Duration {
    const BASE: std::time::Duration = std::time::Duration::from_secs(2);
    const MAX: std::time::Duration = std::time::Duration::from_secs(60);
    let exp = consecutive_failures.min(6);
    let millis = BASE.as_millis().saturating_mul(1u128 << exp);
    std::time::Duration::from_millis(millis.min(MAX.as_millis()) as u64)
}

impl MultiRaftNode {
    /// 创建 MultiRaftNode (不启动 gRPC, 不启动 lifecycle task).
    /// 调用 `start()` 启动 gRPC, `start_lifecycle()` 启动后台 task.
    pub fn new(
        node_id: NodeId,
        router: Arc<Router>,
        grpc_dispatcher: Arc<RaftServiceDispatcher>,
    ) -> Self {
        let router_for_lm = Arc::clone(&router);
        let stats = grpc_dispatcher
            .statistics()
            .unwrap_or_else(|| Arc::new(Statistics::default()));
        Self {
            node_id,
            groups: Arc::new(RwLock::new(HashMap::new())),
            router,
            storages: Arc::new(RwLock::new(HashMap::new())),
            grpc_dispatcher,
            lifecycle: Arc::new(LifecycleManager::new(
                node_id,
                router_for_lm,
                Arc::new(NoopMetaRaftProvider),
            )),
            shutdown_tx: parking_lot::Mutex::new(None),
            server_handle: parking_lot::Mutex::new(None),
            local_group_overrides: Arc::new(RwLock::new(HashSet::new())),
            elected_leader_overrides: Arc::new(RwLock::new(HashSet::new())),
            network_factory: Arc::new(RwLock::new(RaftNetworkClientFactory::new_with_stats(
                node_id,
                0,
                RaftNodeConfig::default().rpc_timeout_ms,
                RaftNodeConfig::default().grpc_max_message_size,
                Some(Arc::clone(&stats)),
            ))),
            stats,
        }
    }

    /// 创建 MultiRaftNode 并注入 LifecycleManager (含 MetaRaftProvider).
    pub fn new_with_lifecycle(
        node_id: NodeId,
        router: Arc<Router>,
        grpc_dispatcher: Arc<RaftServiceDispatcher>,
        lifecycle: LifecycleManager,
    ) -> Self {
        let stats = grpc_dispatcher
            .statistics()
            .unwrap_or_else(|| Arc::new(Statistics::default()));
        Self {
            node_id,
            groups: Arc::new(RwLock::new(HashMap::new())),
            router,
            storages: Arc::new(RwLock::new(HashMap::new())),
            grpc_dispatcher,
            lifecycle: Arc::new(lifecycle),
            shutdown_tx: parking_lot::Mutex::new(None),
            server_handle: parking_lot::Mutex::new(None),
            local_group_overrides: Arc::new(RwLock::new(HashSet::new())),
            elected_leader_overrides: Arc::new(RwLock::new(HashSet::new())),
            network_factory: Arc::new(RwLock::new(RaftNetworkClientFactory::new_with_stats(
                node_id,
                0,
                RaftNodeConfig::default().rpc_timeout_ms,
                RaftNodeConfig::default().grpc_max_message_size,
                Some(Arc::clone(&stats)),
            ))),
            stats,
        }
    }

    /// 获取共享 Statistics 实例
    pub fn statistics(&self) -> Arc<Statistics> {
        Arc::clone(&self.stats)
    }

    /// 将共享 Statistics 实例注入到 LifecycleConfig 中 (若未显式指定)
    pub fn inject_shared_statistics(&self, cfg: &mut LifecycleConfig) {
        if cfg.options.statistics.is_none() {
            cfg.options.statistics = Some(Arc::clone(&self.stats));
        }
    }

    /// 节点 ID
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// 注册对端 Raft 节点地址到本节点 network factory 缓存.
    ///
    /// 该缓存是 `remote_leader_client` 在 MetaRaft 元数据缺失时的回退地址来源
    /// (multi_raft_node.rs::remote_leader_client 注释 2). 真实集群由 raft 对等
    /// 通信自动填充; 测试 harness (未接线 lifecycle 的轻量节点) 需显式注册.
    pub fn register_peer_addr(&self, node_id: NodeId, addr: String) {
        self.network_factory.write().add_node(node_id, addr);
    }

    // ---- 访问器 ----

    /// LifecycleManager 引用
    pub fn lifecycle(&self) -> &LifecycleManager {
        &self.lifecycle
    }

    /// Router 引用
    pub fn router(&self) -> &Arc<Router> {
        &self.router
    }

    /// gRPC 调度器引用
    pub fn grpc_dispatcher(&self) -> &Arc<RaftServiceDispatcher> {
        &self.grpc_dispatcher
    }

    /// 检查 Group 是否在本地
    pub fn is_group_local(&self, group_id: u64) -> bool {
        self.groups.read().contains_key(&group_id)
            || self.local_group_overrides.read().contains(&group_id)
    }

    /// Override group locality for testing. Calling this makes `is_group_local`
    /// return `true` for the given group, even if no real Raft group exists.
    pub fn override_group_local(&self, group_id: u64) {
        self.local_group_overrides.write().insert(group_id);
    }

    /// Clear a previous group locality override.
    pub fn clear_group_local_override(&self, group_id: u64) {
        self.local_group_overrides.write().remove(&group_id);
    }

    /// Override elected-leader checks for testing.
    pub fn override_elected_leader(&self, group_id: u64) {
        self.elected_leader_overrides.write().insert(group_id);
    }

    /// Clear a previous elected-leader override.
    pub fn clear_elected_leader_override(&self, group_id: u64) {
        self.elected_leader_overrides.write().remove(&group_id);
    }

    /// 同步检查本节点是否已是 OpenRaft 选出的 group leader (非 MetaRaft 元数据).
    pub fn is_elected_leader_sync(&self, group_id: u64) -> bool {
        if self.elected_leader_overrides.read().contains(&group_id) {
            return true;
        }
        let groups = self.groups.read();
        let Some(node) = groups.get(&group_id) else {
            return false;
        };
        node.raft().metrics().borrow_watched().current_leader == Some(self.node_id)
    }

    /// 本地 Group 的 OpenRaftNode 映射
    pub fn get_groups(&self) -> &Arc<RwLock<HashMap<u64, Arc<OpenRaftNode>>>> {
        &self.groups
    }

    /// 本地 Group 的 ShardedStorage 映射
    pub fn get_storages(&self) -> &Arc<RwLock<HashMap<u64, ShardedStorage>>> {
        &self.storages
    }

    /// 本地承载的全部 group id 列表.
    pub fn local_group_ids(&self) -> Vec<u64> {
        self.groups.read().keys().copied().collect()
    }
}

/// Noop MetaRaftProvider for cases where lifecycle is not yet wired.
struct NoopMetaRaftProvider;

impl MetaRaftProvider for NoopMetaRaftProvider {
    fn get_cluster_meta(&self) -> ClusterMeta {
        ClusterMeta::default()
    }
    fn get_slot_table(&self) -> SlotTable {
        default_slot_table()
    }
    fn get_migration_state(&self) -> Option<SlotMigrationState> {
        None
    }
}
