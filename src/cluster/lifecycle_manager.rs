//! Group 生命周期管理 — 周期性 (默认 1s) 轮询 MetaRaft 元数据, 计算本节点应当
//! 承载的 Group 拓扑, 并同步刷新 Router 缓存. 实际创建/销毁由 `MultiRaftNode`
//! 根据 `TickResult` 执行.
//!
//! # tick 流程
//!
//! ```text
//! tick()
//!   ├─ get_cluster_meta + get_slot_table (MetaRaftProvider)
//!   ├─ Router.refresh_from_data(slot_table, group_nodes, node_addrs, group_leaders)
//!   │     └─ node_addrs 优先 client_addr, fallback rpc_addr (MOVED 用)
//!   ├─ expected = Meta 中 replicas 含本节点 的 group
//!   ├─ to_create / to_remove = expected △ local_groups
//!   └─ TickResult { groups_to_create, groups_to_remove, expected_memberships }
//! ```
//!
//! `expected_memberships` 供 `MultiRaftNode` 做 drift 对账 (期望成员来自 Meta
//! `GroupMeta.replicas`, 实际成员来自 OpenRaft metrics).
//!
//! # Invariant
//!
//! - Router 缓存以 Meta 元数据为准刷新; leader 提取自 `replicas[].is_leader`.
//! - 只计算本节点参与的 group 的期望成员集.
//! - 地址选择: MOVED 用 `client_addr`, 缺失时 fallback `rpc_addr`.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::cluster::meta_types::{ClusterMeta, SlotMigrationState, SlotTable};
use crate::cluster::router::Router;
use crate::cluster::types::NodeId;

/// MetaRaft query interface trait (for decoupling and unit test mocking).
pub trait MetaRaftProvider: Send + Sync {
    fn get_cluster_meta(&self) -> ClusterMeta;
    fn get_slot_table(&self) -> SlotTable;
    fn get_migration_state(&self) -> Option<SlotMigrationState>;
}

/// `tick()` 的返回结果.
#[derive(Debug)]
pub struct TickResult {
    pub groups_to_create: Vec<u64>,
    pub groups_to_remove: Vec<u64>,
    /// group_id -> 期望的副本节点集合 (从 MetaRaft GroupMeta.replicas 提取).
    /// 仅包含本节点参与的 group.
    pub expected_memberships: HashMap<u64, BTreeSet<NodeId>>,
}

/// 期望成员与实际 Raft 成员之间的 drift.
#[derive(Debug)]
pub struct MembershipDrift {
    pub group_id: u64,
    /// 来自 MetaRaft GroupMeta.replicas 的期望成员集合.
    pub expected: BTreeSet<NodeId>,
    /// 来自 Raft membership (OpenRaftNode) 的实际成员集合.
    pub actual: BTreeSet<NodeId>,
    /// 在期望中但不在实际中的节点 (需要添加为 Learner).
    pub to_add: Vec<NodeId>,
    /// 在实际中但不在期望中的节点 (需要从 Raft group 中移除).
    pub to_remove: Vec<NodeId>,
}

/// Group lifecycle manager.
///
/// Periodically polls MetaRaft for group topology changes and determines
/// which groups should be created/removed on this node.
pub struct LifecycleManager {
    node_id: NodeId,
    local_groups: Arc<RwLock<HashSet<u64>>>,
    router: Arc<Router>,
    meta_raft: Arc<dyn MetaRaftProvider>,
    tick_interval: tokio::time::Duration,
}

impl LifecycleManager {
    /// Create a new LifecycleManager.
    pub fn new(node_id: NodeId, router: Arc<Router>, meta_raft: Arc<dyn MetaRaftProvider>) -> Self {
        Self {
            node_id,
            local_groups: Arc::new(RwLock::new(HashSet::new())),
            router,
            meta_raft,
            tick_interval: tokio::time::Duration::from_secs(1),
        }
    }

    /// Set a custom tick interval.
    pub fn with_tick_interval(mut self, interval: tokio::time::Duration) -> Self {
        self.tick_interval = interval;
        self
    }

    /// Execute one tick: query MetaRaft, compute group diff, update local state.
    ///
    /// Returns a [`TickResult`] containing groups to create, groups to remove,
    /// and expected Raft memberships for local groups.
    pub fn tick(&self) -> TickResult {
        let meta = self.meta_raft.get_cluster_meta();
        let new_table = self.meta_raft.get_slot_table();

        // Refresh Router cache with latest topology.
        let mut group_nodes: HashMap<u64, Vec<NodeId>> = HashMap::new();
        let mut node_addrs: HashMap<u64, String> = HashMap::new();

        for (gid, group) in &meta.groups {
            group_nodes.insert(*gid, group.replicas.iter().map(|r| r.node_id).collect());
        }
        for (nid, node) in &meta.nodes {
            // Prefer the client address for MOVED redirects; fall back to RPC address.
            let addr = node
                .client_addr
                .clone()
                .unwrap_or_else(|| node.rpc_addr.clone());
            node_addrs.insert(*nid, addr);
        }
        let group_leaders: HashMap<u64, NodeId> = meta
            .groups
            .iter()
            .filter_map(|(gid, g)| {
                g.replicas
                    .iter()
                    .find(|r| r.is_leader)
                    .map(|r| (*gid, r.node_id))
            })
            .collect();
        self.router
            .refresh_from_data(new_table, group_nodes, node_addrs, group_leaders);

        // Compute which groups this node is expected to host.
        let expected: HashSet<u64> = meta
            .groups
            .iter()
            .filter(|(_, g)| g.replicas.iter().any(|r| r.node_id == self.node_id))
            .map(|(id, _)| *id)
            .collect();

        let current = self.local_groups.read().clone();
        let to_create: Vec<u64> = expected.difference(&current).copied().collect();
        let to_remove: Vec<u64> = current.difference(&expected).copied().collect();

        // Update local_groups set.
        {
            let mut groups = self.local_groups.write();
            for gid in &to_create {
                groups.insert(*gid);
            }
            for gid in &to_remove {
                groups.remove(gid);
            }
        }

        // 提取期望成员信息 (仅限本节点参与的 group).
        let mut expected_memberships: HashMap<u64, BTreeSet<NodeId>> = HashMap::new();
        for (gid, group) in &meta.groups {
            if expected.contains(gid) {
                let members: BTreeSet<NodeId> = group.replicas.iter().map(|r| r.node_id).collect();
                expected_memberships.insert(*gid, members);
            }
        }

        if !to_create.is_empty() {
            tracing::info!(?to_create, "detected groups to create locally");
        }
        if !to_remove.is_empty() {
            tracing::info!(?to_remove, "detected groups to remove locally");
        }

        TickResult {
            groups_to_create: to_create,
            groups_to_remove: to_remove,
            expected_memberships,
        }
    }

    /// Background run loop. Intended to be spawned as a tokio task.
    pub async fn run(&self, mut shutdown_rx: tokio::sync::watch::Receiver<bool>) {
        tracing::info!(node_id = %self.node_id, "lifecycle manager started");
        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.tick_interval) => {
                    let _tick_result = self.tick();
                }
                _ = shutdown_rx.changed() => {
                    tracing::info!("lifecycle manager shutting down");
                    break;
                }
            }
        }
    }

    /// Shared reference to the set of groups currently hosted on this node.
    pub fn local_groups(&self) -> &Arc<RwLock<HashSet<u64>>> {
        &self.local_groups
    }

    /// Reference to the MetaRaft provider (for querying group metadata).
    pub fn meta_raft(&self) -> &Arc<dyn MetaRaftProvider> {
        &self.meta_raft
    }

    /// This node's ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Current tick interval.
    pub fn tick_interval(&self) -> tokio::time::Duration {
        self.tick_interval
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::cluster::meta_types::{
        default_slot_table, ClusterMeta, GroupMeta, ReplicaInfo, SlotMigrationState, SlotTable,
    };
    use crate::cluster::router::Router;

    use super::*;

    struct MockMetaRaft {
        meta: ClusterMeta,
        slots: SlotTable,
        migration: Option<SlotMigrationState>,
    }

    impl MetaRaftProvider for MockMetaRaft {
        fn get_cluster_meta(&self) -> ClusterMeta {
            self.meta.clone()
        }
        fn get_slot_table(&self) -> SlotTable {
            self.slots.clone()
        }
        fn get_migration_state(&self) -> Option<SlotMigrationState> {
            self.migration.clone()
        }
    }

    fn make_group_meta(group_id: u64, node_ids: &[u64]) -> GroupMeta {
        GroupMeta {
            group_id,
            replicas: node_ids
                .iter()
                .enumerate()
                .map(|(i, &nid)| ReplicaInfo {
                    node_id: nid,
                    is_leader: i == 0,
                })
                .collect(),
            slot_ranges: vec![(0, 100)],
            config_version: 1,
        }
    }

    #[test]
    fn tick_returns_expected_memberships_for_local_groups() {
        let node_id = 1u64;
        let mut groups = HashMap::new();
        groups.insert(10, make_group_meta(10, &[1, 2]));
        groups.insert(20, make_group_meta(20, &[3, 4]));
        let meta = ClusterMeta {
            cluster_id: "test".into(),
            nodes: HashMap::new(),
            groups,
            version: 1,
            format_version: 1,
        };
        let mock = Arc::new(MockMetaRaft {
            meta,
            slots: default_slot_table(),
            migration: None,
        });
        let router = Arc::new(Router::new(
            default_slot_table(),
            HashMap::new(),
            HashMap::new(),
        ));
        let mgr: LifecycleManager = LifecycleManager::new(node_id, router, mock);
        let result: TickResult = mgr.tick();
        let members = result.expected_memberships.get(&10).unwrap();
        assert_eq!(members.len(), 2);
        assert!(members.contains(&1));
        assert!(members.contains(&2));
        assert!(!result.expected_memberships.contains_key(&20));
    }

    #[test]
    fn tick_empty_expected_memberships_when_no_groups() {
        let meta = ClusterMeta {
            cluster_id: "test".into(),
            nodes: HashMap::new(),
            groups: HashMap::new(),
            version: 1,
            format_version: 1,
        };
        let mock = Arc::new(MockMetaRaft {
            meta,
            slots: default_slot_table(),
            migration: None,
        });
        let router = Arc::new(Router::new(
            default_slot_table(),
            HashMap::new(),
            HashMap::new(),
        ));
        let mgr = LifecycleManager::new(1, router, mock);
        let result = mgr.tick();
        assert!(result.expected_memberships.is_empty());
    }

    #[test]
    fn tick_handles_empty_replicas() {
        let mut groups = HashMap::new();
        let mut group = make_group_meta(10, &[1]);
        group.replicas = vec![];
        groups.insert(10, group);
        let meta = ClusterMeta {
            cluster_id: "test".into(),
            nodes: HashMap::new(),
            groups,
            version: 1,
            format_version: 1,
        };
        let mock = Arc::new(MockMetaRaft {
            meta,
            slots: default_slot_table(),
            migration: None,
        });
        let router = Arc::new(Router::new(
            default_slot_table(),
            HashMap::new(),
            HashMap::new(),
        ));
        let mgr = LifecycleManager::new(1, router, mock);
        let result = mgr.tick();
        // Empty replicas → no replica matches our node_id → group not in expected set.
        assert!(!result.expected_memberships.contains_key(&10));
    }

    #[test]
    fn tick_preserves_existing_create_remove_logic() {
        let node_id = 1u64;
        let mut groups = HashMap::new();
        groups.insert(10, make_group_meta(10, &[1]));
        let meta = ClusterMeta {
            cluster_id: "test".into(),
            nodes: HashMap::new(),
            groups,
            version: 1,
            format_version: 1,
        };
        let mock = Arc::new(MockMetaRaft {
            meta,
            slots: default_slot_table(),
            migration: None,
        });
        let router = Arc::new(Router::new(
            default_slot_table(),
            HashMap::new(),
            HashMap::new(),
        ));
        let mgr = LifecycleManager::new(node_id, router, mock);
        let result = mgr.tick();
        assert_eq!(result.groups_to_create, vec![10]);
        let result2 = mgr.tick();
        assert!(result2.groups_to_create.is_empty());
    }
}
